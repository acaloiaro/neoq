package memory

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/guregu/null"
	"github.com/iancoleman/strcase"
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/internal"
	"github.com/pranavmodx/neoq-sqlite/jobs"
	"github.com/pranavmodx/neoq-sqlite/logging"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

const (
	defaultMemQueueCapacity = 10000 // default capacity of individual queues
	emptyCapacity           = 0     // queue size at which queues are considered empty
)

// MemBackend is a memory-backed neoq backend
type MemBackend struct {
	neoq.Neoq
	config       *neoq.Config
	logger       logging.Logger
	handlers     *sync.Map // map queue names [string] to queue handlers [Handler]
	fingerprints *sync.Map // map fingerprints [string] to job [Job]
	futureJobs   *sync.Map // map jobIDs [int64] to job [Job]
	queues       *sync.Map // map queue names [string] to queue handler channels [chan Job]
	cron         *cron.Cron
	mu           *sync.Mutex          // mutext to protect mutating state on a pgWorker
	cancelFuncs  []context.CancelFunc // A collection of cancel functions to be called upon Shutdown()
	jobCount     int64                // number of jobs that have been queued since start
	initialized  bool
}

// Backend is a [neoq.BackendInitializer] that initializes a new memory-backed neoq backend
func Backend(_ context.Context, opts ...neoq.ConfigOption) (backend neoq.Neoq, err error) {
	mb := &MemBackend{
		config:       neoq.NewConfig(),
		cron:         cron.New(),
		mu:           &sync.Mutex{},
		queues:       &sync.Map{},
		handlers:     &sync.Map{},
		futureJobs:   &sync.Map{},
		fingerprints: &sync.Map{},
		jobCount:     0,
		cancelFuncs:  []context.CancelFunc{},
	}
	mb.cron.Start()

	for _, opt := range opts {
		opt(mb.config)
	}

	mb.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: mb.config.LogLevel}))
	backend = mb

	return
}

// Enqueue queues jobs to be executed asynchronously
func (m *MemBackend) Enqueue(_ context.Context, job *jobs.Job) (jobID string, err error) {
	var queueChan chan *jobs.Job
	var qc any
	var ok bool

	if job.Queue == "" {
		err = jobs.ErrNoQueueSpecified
		return
	}

	if job.Status == "" {
		job.Status = internal.JobStatusNew
	}

	m.logger.Debug("adding a new job", slog.String("queue", job.Queue))

	if qc, ok = m.queues.Load(job.Queue); !ok {
		return jobs.UnqueuedJobID, fmt.Errorf("%w: %s", handler.ErrNoProcessorForQueue, job.Queue)
	}

	queueChan = qc.(chan *jobs.Job)

	// Make sure RunAfter is set to a non-zero value if not provided by the caller
	// if already set, schedule the future job
	now := time.Now().UTC()
	if job.RunAfter.IsZero() {
		job.RunAfter = now
	}

	if job.Queue == "" {
		err = jobs.ErrNoQueueSpecified
		return
	}

	err = jobs.FingerprintJob(job)
	if err != nil {
		return
	}

	// if the job fingerprint is already known, don't queue the job
	if _, found := m.fingerprints.Load(job.Fingerprint); found {
		return jobs.DuplicateJobID, nil
	}

	m.fingerprints.Store(job.Fingerprint, job)
	m.mu.Lock()
	m.jobCount++
	m.mu.Unlock()

	job.ID = m.jobCount
	jobID = fmt.Sprint(m.jobCount)

	if job.RunAfter.Equal(now) {
		queueChan <- job
	} else {
		m.queueFutureJob(job)
	}

	return jobID, nil
}

// Start starts processing jobs with the specified queue and handler
func (m *MemBackend) Start(ctx context.Context, h handler.Handler) (err error) {
	queueCapacity := h.QueueCapacity
	if queueCapacity == emptyCapacity {
		queueCapacity = defaultMemQueueCapacity
	}

	h.RecoverCallback = m.config.RecoveryCallback

	m.handlers.Store(h.Queue, h)
	m.queues.Store(h.Queue, make(chan *jobs.Job, queueCapacity))

	ctx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	m.cancelFuncs = append(m.cancelFuncs, cancel)
	m.mu.Unlock()

	err = m.start(ctx, h.Queue)
	if err != nil {
		return
	}

	return
}

// StartCron starts processing jobs with the specified cron schedule and handler
//
// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
func (m *MemBackend) StartCron(ctx context.Context, cronSpec string, h handler.Handler) (err error) {
	cd, err := crondescriptor.NewCronDescriptor(cronSpec)
	if err != nil {
		return fmt.Errorf("error creating cron descriptor: %w", err)
	}

	cdStr, err := cd.GetDescription(crondescriptor.Full)
	if err != nil {
		return fmt.Errorf("error getting cron description: %w", err)
	}

	queue := internal.StripNonAlphanum(strcase.ToSnake(*cdStr))

	ctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancelFuncs = append(m.cancelFuncs, cancel)
	m.mu.Unlock()
	h.Queue = queue
	h.RecoverCallback = m.config.RecoveryCallback

	err = m.Start(ctx, h)
	if err != nil {
		return fmt.Errorf("error processing queue '%s': %w", queue, err)
	}

	if err := m.cron.AddFunc(cronSpec, func() {
		_, _ = m.Enqueue(ctx, &jobs.Job{Queue: queue})
	}); err != nil {
		return fmt.Errorf("error adding cron: %w", err)
	}

	return err
}

// SetLogger sets this backend's logger
func (m *MemBackend) SetLogger(logger logging.Logger) {
	m.logger = logger
}

// Shutdown halts the worker
func (m *MemBackend) Shutdown(_ context.Context) {
	for _, f := range m.cancelFuncs {
		f()
	}

	m.cancelFuncs = nil
}

// start starts a processor that handles new incoming jobs and future jobs
// nolint: cyclop
func (m *MemBackend) start(ctx context.Context, queue string) (err error) {
	var queueChan chan *jobs.Job
	var qc any
	var ht any
	var h handler.Handler
	var ok bool

	if ht, ok = m.handlers.Load(queue); !ok {
		err = fmt.Errorf("%w: %s", handler.ErrNoHandlerForQueue, queue)
		m.logger.Error("error loading handler for queue", slog.String("queue", queue))
		return
	}

	if qc, ok = m.queues.Load(queue); !ok {
		m.logger.Error("error loading channel for queue", slog.String("queue", queue), slog.Any("error", handler.ErrNoHandlerForQueue))
		return err
	}

	go func() { m.scheduleFutureJobs(ctx) }()

	h = ht.(handler.Handler)
	queueChan = qc.(chan *jobs.Job)

	for i := 0; i < h.Concurrency; i++ {
		go func() {
			var err error
			var job *jobs.Job
			for {
				select {
				case job = <-queueChan:
					err = m.handleJob(ctx, job, h)
					job.Status = internal.JobStatusProcessed
				case <-ctx.Done():
					err = ctx.Err()
				}

				if err != nil {
					if errors.Is(err, context.Canceled) ||
						errors.Is(err, jobs.ErrJobExceededDeadline) ||
						errors.Is(err, jobs.ErrJobExceededMaxRetries) {
						return
					}

					m.logger.Error("job failed", slog.Int64("job_id", job.ID), slog.Any("error", err))

					runAfter := internal.CalculateBackoff(job.Retries)
					job.RunAfter = runAfter
					job.Status = internal.JobStatusFailed
					m.queueFutureJob(job)
				}

				m.fingerprints.Delete(job.Fingerprint)
			}
		}()
	}

	return nil
}

func (m *MemBackend) scheduleFutureJobs(ctx context.Context) {
	// check for new future jobs on an interval
	ticker := time.NewTicker(m.config.JobCheckInterval)

	// if the queues list is non-empty, then we've already started, and this function is a no-op
	m.mu.Lock()
	if !m.initialized {
		m.initialized = true
		m.mu.Unlock()
	} else {
		m.mu.Unlock()
		return
	}

	for {
		// loop over list of future jobs, scheduling goroutines to wait for jobs that are due within the next 30 seconds
		m.futureJobs.Range(func(_, v any) bool {
			job := v.(*jobs.Job)
			var queueChan chan *jobs.Job

			timeUntilRunAfter := time.Until(job.RunAfter)
			if timeUntilRunAfter <= m.config.FutureJobWindow {
				m.removeFutureJob(job.ID)
				m.logger.Debug(
					"dequeued job",
					slog.String("queue", job.Queue),
					slog.Int("retries", job.Retries),
					slog.String("next_run", timeUntilRunAfter.String()),
					slog.Int64("job_id", job.ID),
				)
				go func(j *jobs.Job) {
					scheduleCh := time.After(timeUntilRunAfter)
					<-scheduleCh
					m.logger.Debug("loading job for queue", slog.String("queue", j.Queue))
					if qc, ok := m.queues.Load(j.Queue); ok {
						queueChan = qc.(chan *jobs.Job)
						queueChan <- j
					} else {
						m.logger.Error(fmt.Sprintf("no queue processor for queue '%s'", j.Queue), handler.ErrNoHandlerForQueue)
					}
				}(job)
			}

			return true
		})
		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (m *MemBackend) handleJob(ctx context.Context, job *jobs.Job, h handler.Handler) (err error) {
	ctx = withJobContext(ctx, job)

	m.logger.Debug(
		"handling job",
		slog.String("status", job.Status),
		slog.Int("retries", job.Retries),
		slog.Int64("job_id", job.ID),
	)

	if job.Status != internal.JobStatusNew {
		job.Retries++
	}

	if job.Deadline != nil && job.Deadline.UTC().Before(time.Now().UTC()) {
		m.logger.Debug(
			"job deadline is in the past, skipping",
			slog.Time("deadline", *job.Deadline),
			slog.Int64("job_id", job.ID),
		)
		err = jobs.ErrJobExceededDeadline
		return
	}

	err = handler.Exec(ctx, h)
	if err != nil {
		job.Error = null.StringFrom(err.Error())
	}

	if err != nil {
		job.Error = null.StringFrom(err.Error())
		job.Status = internal.JobStatusFailed
	}

	if job.MaxRetries != nil && job.Retries >= *job.MaxRetries {
		m.logger.Debug(
			"job exceeded max retries",
			slog.Int("retries", job.Retries),
			slog.Int("max_retries", *job.MaxRetries),
			slog.Int64("job_id", job.ID),
		)
		err = jobs.ErrJobExceededMaxRetries
	}

	return err
}

// queueFutureJob queues a future job for eventual execution
func (m *MemBackend) queueFutureJob(job *jobs.Job) {
	m.fingerprints.Store(job.Fingerprint, job)
	m.futureJobs.Store(job.ID, job)
}

// removeFutureJob removes a future job from the in-memory list of jobs that will execute in the future
func (m *MemBackend) removeFutureJob(jobID int64) {
	if j, ok := m.futureJobs.Load(jobID); ok {
		job := j.(*jobs.Job)
		m.fingerprints.Delete(job.Fingerprint)
		m.futureJobs.Delete(job.ID)
	}
}

// withJobContext creates a new context with the Job set
func withJobContext(ctx context.Context, j *jobs.Job) context.Context {
	return context.WithValue(ctx, internal.JobCtxVarKey, j)
}
