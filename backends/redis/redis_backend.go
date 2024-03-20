package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/iancoleman/strcase"
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/internal"
	"github.com/pranavmodx/neoq-sqlite/jobs"
	"github.com/pranavmodx/neoq-sqlite/logging"
	"golang.org/x/exp/slog"
)

// All jobs are placed on the same 'default' queue (until a compelling case is made for using different asynq queues
// for every job)
const defaultAsynqQueue = "default"

// ErrInvalidAddr indicates that the provided address is not a valid redis connection string
var ErrInvalidAddr = errors.New("invalid connecton string: see documentation for valid connection strings")

// RedisBackend is a Redis-backed neoq backend
// nolint: revive
type RedisBackend struct {
	neoq.Neoq
	client       *asynq.Client
	server       *asynq.Server
	inspector    *asynq.Inspector
	mux          *asynq.ServeMux
	config       *neoq.Config
	logger       logging.Logger
	mu           *sync.Mutex // mutext to protect mutating backend state
	taskProvider *memoryTaskConfigProvider
	mgr          *asynq.PeriodicTaskManager
}

type memoryTaskConfigProvider struct {
	mu      *sync.Mutex
	configs []*asynq.PeriodicTaskConfig
}

// newMemoryTaskConfigProvider returns a new asynq MemoryTaskConfigProvider
func newMemoryTaskConfigProvider() (p *memoryTaskConfigProvider) {
	p = &memoryTaskConfigProvider{
		mu:      &sync.Mutex{},
		configs: []*asynq.PeriodicTaskConfig{},
	}
	return
}

// GetConfigs returns this provider's periodic task configurations
func (m *memoryTaskConfigProvider) GetConfigs() (c []*asynq.PeriodicTaskConfig, err error) {
	m.mu.Lock()
	cfgs := m.configs
	m.mu.Unlock()
	return cfgs, nil
}

// addConfig adds a periodic task configuration to this provider's configs
func (m *memoryTaskConfigProvider) addConfig(taskConfig *asynq.PeriodicTaskConfig) {
	m.mu.Lock()
	m.configs = append(m.configs, taskConfig)
	m.mu.Unlock()
}

// Backend is a [neoq.BackendInitializer] that initializes a new Redis-backed neoq backend
func Backend(_ context.Context, opts ...neoq.ConfigOption) (backend neoq.Neoq, err error) {
	b := &RedisBackend{
		config:       neoq.NewConfig(),
		mu:           &sync.Mutex{},
		taskProvider: newMemoryTaskConfigProvider(),
	}

	for _, opt := range opts {
		opt(b.config)
	}

	b.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: b.config.LogLevel}))
	if b.config.ConnectionString == "" {
		err = ErrInvalidAddr
		return
	}

	if b.config.BackendConcurrency <= 0 {
		b.config.BackendConcurrency = runtime.NumCPU()
	}

	clientOpt := asynq.RedisClientOpt{Addr: b.config.ConnectionString}
	if b.config.BackendAuthPassword != "" {
		clientOpt.Password = b.config.BackendAuthPassword
	}
	b.inspector = asynq.NewInspector(clientOpt)
	b.client = asynq.NewClient(clientOpt)
	b.server = asynq.NewServer(
		clientOpt,
		asynq.Config{
			Concurrency:     b.config.BackendConcurrency,
			ShutdownTimeout: b.config.ShutdownTimeout,
		},
	)

	b.mux = asynq.NewServeMux()

	b.mgr, err = asynq.NewPeriodicTaskManager(
		asynq.PeriodicTaskManagerOpts{
			RedisConnOpt:               clientOpt,
			PeriodicTaskConfigProvider: b.taskProvider,
			SyncInterval:               500 * time.Millisecond,
			SchedulerOpts: &asynq.SchedulerOpts{
				PostEnqueueFunc: func(_ *asynq.TaskInfo, err error) {
					if err != nil {
						b.logger.Error("unable to schedule task", slog.Any("error", err))
					}
				},
			},
		})
	if err != nil {
		err = fmt.Errorf("failed to initialize periodic task manager: %w", err)
		log.Fatal(err)
	}

	go func() {
		if err := b.mgr.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := b.server.Run(b.mux); err != nil {
			log.Fatal(err)
		}
	}()

	backend = b

	return backend, err
}

// WithAddr configures neoq to connect to Redis with the given address
func WithAddr(addr string) neoq.ConfigOption {
	return func(c *neoq.Config) {
		c.ConnectionString = addr
	}
}

// WithPassword configures neoq to connect to Redis with the given password
func WithPassword(password string) neoq.ConfigOption {
	return func(c *neoq.Config) {
		c.BackendAuthPassword = password
	}
}

// WithConcurrency configures the number of workers available to process jobs across all queues
func WithConcurrency(concurrency int) neoq.ConfigOption {
	return func(c *neoq.Config) {
		c.BackendConcurrency = concurrency
	}
}

// WithShutdownTimeout specifies the duration to wait to let workers finish their tasks
// before forcing them to abort durning Shutdown()
//
// If unset or zero, default timeout of 8 seconds is used.
func WithShutdownTimeout(timeout time.Duration) neoq.ConfigOption {
	return func(c *neoq.Config) {
		c.ShutdownTimeout = timeout
	}
}

// Enqueue queues jobs to be executed asynchronously
func (b *RedisBackend) Enqueue(ctx context.Context, job *jobs.Job) (jobID string, err error) {
	if job.Queue == "" {
		err = jobs.ErrNoQueueSpecified
		return
	}

	err = jobs.FingerprintJob(job)
	if err != nil {
		return
	}

	var payload []byte
	payload, err = json.Marshal(job.Payload)
	if err != nil {
		return
	}
	task := asynq.NewTask(job.Queue, payload)
	_, err = b.client.EnqueueContext(ctx, task, jobToTaskOptions(job)...)
	if err != nil {
		err = fmt.Errorf("unable to enqueue task: %w", err)
	}

	return job.Fingerprint, err
}

// Start starts processing jobs with the specified queue and handler
func (b *RedisBackend) Start(_ context.Context, h handler.Handler) (err error) {
	h.RecoverCallback = b.config.RecoveryCallback

	b.mux.HandleFunc(h.Queue, func(ctx context.Context, t *asynq.Task) (err error) {
		taskID := t.ResultWriter().TaskID()
		var p map[string]any
		if err = json.Unmarshal(t.Payload(), &p); err != nil {
			b.logger.Info("job has no payload", slog.String("task_id", taskID))
		}

		ti, err := b.inspector.GetTaskInfo(defaultAsynqQueue, taskID)
		if err != nil {
			b.logger.Error("unable to process job", slog.Any("error", err))
			return
		}

		if !ti.Deadline.IsZero() && ti.Deadline.UTC().Before(time.Now().UTC()) {
			err = jobs.ErrJobExceededDeadline
			b.logger.Debug("job deadline is in the past, skipping", slog.String("task_id", taskID))
			return
		}

		if ti.Retried >= ti.MaxRetry {
			err = jobs.ErrJobExceededMaxRetries
			b.logger.Debug("job has exceeded the maximum number of retries, skipping", slog.String("task_id", taskID))
			return
		}

		job := &jobs.Job{
			CreatedAt:  time.Now().UTC(),
			Queue:      h.Queue,
			Payload:    p,
			Deadline:   &ti.Deadline,
			MaxRetries: &ti.MaxRetry,
			RunAfter:   ti.NextProcessAt,
		}

		ctx = withJobContext(ctx, job)
		err = handler.Exec(ctx, h)
		if err != nil {
			b.logger.Error("error handling job", slog.Any("error", err))
		}

		return
	})

	return nil
}

// StartCron starts processing jobs with the specified cron schedule and handler
//
// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
func (b *RedisBackend) StartCron(ctx context.Context, cronSpec string, h handler.Handler) (err error) {
	cd, err := crondescriptor.NewCronDescriptor(cronSpec)
	if err != nil {
		return fmt.Errorf("error creating cron descriptor: %w", err)
	}

	cdStr, err := cd.GetDescription(crondescriptor.Full)
	if err != nil {
		return fmt.Errorf("error getting cron description: %w", err)
	}

	queue := internal.StripNonAlphanum(strcase.ToSnake(*cdStr))
	h.Queue = queue
	h.RecoverCallback = b.config.RecoveryCallback

	err = b.Start(ctx, h)
	if err != nil {
		return
	}

	aCronSpec := toAsynqCronspec(cronSpec)
	c := &asynq.PeriodicTaskConfig{
		Cronspec: aCronSpec,
		Task:     asynq.NewTask(queue, nil),
	}
	b.taskProvider.addConfig(c)

	return
}

// jobToTaskOptions converts jobs.Job to a slice of asynq.Option that corresponds with its settings
func jobToTaskOptions(job *jobs.Job) (opts []asynq.Option) {
	opts = append(opts, asynq.TaskID(job.Fingerprint))

	if !job.RunAfter.IsZero() {
		opts = append(opts, asynq.ProcessAt(job.RunAfter))
	}

	if job.Deadline != nil {
		opts = append(opts, asynq.Deadline(*job.Deadline))
	}

	if job.MaxRetries != nil {
		opts = append(opts, asynq.MaxRetry(*job.MaxRetries))
	}

	return
}

// Asynq does not currently support the seconds field in cron specs. However, it does supports seconds using the
// alternative syntax: @every Xs, where X is the number of seconds between executions
//
// Because of this, when cron specs have six fields (contain seconds), we must convert the seconds field to asynq
// seconds format.
//
// # This implementation is very crude, and unlikely covers the full breadth of the cron spec, panic()ing when necessary
//
// Note: Refactor if asynq merges https://github.com/hibiken/asynq/pull/644
func toAsynqCronspec(cronSpec string) string {
	// nolint: nestif, gomnd
	if strings.Count(cronSpec, "*") == 6 {
		fields := strings.Fields(cronSpec)
		// nolint: gomnd
		if len(fields) == 6 {
			secondsField := fields[0]
			if secondsField == "*" {
				return "@every 1s"
			}

			// Handle cronspec divisor syntax, e.g. */30 is every 30 seconds, or @every 30s
			if strings.Contains(secondsField, "/") {
				seconds := strings.Split(secondsField, "/")[1]
				return fmt.Sprintf("@every %ss", seconds)
			}
		} else {
			panic(fmt.Sprintf("invalid cron spec: %s", cronSpec))
		}
	}

	return cronSpec
}

// SetLogger sets this backend's logger
func (b *RedisBackend) SetLogger(logger logging.Logger) {
	b.logger = logger
}

// Shutdown halts the worker
func (b *RedisBackend) Shutdown(_ context.Context) {
	b.client.Close()
	b.server.Shutdown()
}

// withJobContext creates a new context with the Job set
func withJobContext(ctx context.Context, j *jobs.Job) context.Context {
	return context.WithValue(ctx, internal.JobCtxVarKey, j)
}
