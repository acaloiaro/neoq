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

	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
	"github.com/acaloiaro/neoq/types"
	"github.com/hibiken/asynq"
	"github.com/iancoleman/strcase"
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
	"golang.org/x/exp/slog"
)

var (
	// ErrInvalidAddr indicates that the provided address is not a valid redis connection string
	ErrInvalidAddr = errors.New("invalid connecton string: see documentation for valid connection strings")
)

// RedisBackend is a Redis-backed neoq backend
// nolint: revive
type RedisBackend struct {
	types.Backend
	client       *asynq.Client
	server       *asynq.Server
	mux          *asynq.ServeMux
	config       *config.Config
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

// Backend is a [config.BackendInitializer] that initializes a new Redis-backed neoq backend
func Backend(ctx context.Context, opts ...config.Option) (backend types.Backend, err error) {
	b := &RedisBackend{
		config:       config.New(),
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
				PostEnqueueFunc: func(info *asynq.TaskInfo, err error) {
					if err != nil {
						b.logger.Error("unable to schedule task", err)
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
func WithAddr(addr string) config.Option {
	return func(c *config.Config) {
		c.ConnectionString = addr
	}
}

// WithPassword configures neoq to connect to Redis with the given password
func WithPassword(password string) config.Option {
	return func(c *config.Config) {
		c.BackendAuthPassword = password
	}
}

// WithConcurrency configures the number of workers available to process jobs across all queues
func WithConcurrency(concurrency int) config.Option {
	return func(c *config.Config) {
		c.BackendConcurrency = concurrency
	}
}

// WithShutdownTimeout specifies the duration to wait to let workers finish their tasks
// before forcing them to abort durning Shutdown()
//
// If unset or zero, default timeout of 8 seconds is used.
func WithShutdownTimeout(timeout time.Duration) config.Option {
	return func(c *config.Config) {
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
	if job.RunAfter.IsZero() {
		_, err = b.client.EnqueueContext(ctx, task, asynq.TaskID(job.Fingerprint))
	} else {
		_, err = b.client.EnqueueContext(ctx, task, asynq.TaskID(job.Fingerprint), asynq.ProcessAt(job.RunAfter))
	}

	if err != nil {
		err = fmt.Errorf("unable to enqueue task: %w", err)
	}

	return job.Fingerprint, err
}

// Start starts processing jobs with the specified queue and handler
func (b *RedisBackend) Start(_ context.Context, queue string, h handler.Handler) (err error) {
	b.mux.HandleFunc(queue, func(ctx context.Context, t *asynq.Task) (err error) {
		var p map[string]any
		if err = json.Unmarshal(t.Payload(), &p); err != nil {
			b.logger.Info("job has no payload")
		}

		job := &jobs.Job{
			CreatedAt: time.Now(),
			Queue:     queue,
			Payload:   p,
		}

		ctx = withJobContext(ctx, job)
		err = handler.Exec(ctx, h)
		if err != nil {
			b.logger.Error("error handling job", err)
		}

		return
	})

	return
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

	err = b.Start(ctx, queue, h)
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

// Asynq does not currently support the seconds field in cron specs. However, it does supports seconds using the
// alternative syntax: @every Xs, where X is the number of seconds between executions
//
// Because of this, when cron specs have six fields (contain seconds), we must convert the seconds field to asynq
// seconds format.
//
// # This implementation is very crude, and unlikely covers the full breadth of the cron spec, panic()ing when necessary
//
// TODO: Refactor if asynq merges https://github.com/hibiken/asynq/pull/644
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
func (b *RedisBackend) Shutdown(ctx context.Context) {
	b.client.Close()
	b.server.Shutdown()
}

// withJobContext creates a new context with the Job set
func withJobContext(ctx context.Context, j *jobs.Job) context.Context {
	return context.WithValue(ctx, internal.JobCtxVarKey, j)
}
