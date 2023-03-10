package memory

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
	"github.com/acaloiaro/neoq/types"
	"github.com/guregu/null"
	"github.com/iancoleman/strcase"                          // TODO factor out
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor" // TODO factor out
	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

const (
	defaultMemQueueCapacity = 10000 // the default capacity of individual queues
	emptyCapacity           = 0
)

// MemBackend is a memory-backed neoq backend
type MemBackend struct {
	types.Backend
	config       *config.Config
	logger       logging.Logger
	handlers     *sync.Map // map queue names [string] to queue handlers [Handler]
	fingerprints *sync.Map // map fingerprints [string] to job [Job]
	futureJobs   *sync.Map // map jobIDs [int64] to job [Job]
	queues       *sync.Map // map queue names [string] to queue handler channels [chan Job]
	cron         *cron.Cron
	mu           *sync.Mutex          // mutext to protect mutating state on a pgWorker
	cancelFuncs  []context.CancelFunc // A collection of cancel functions to be called upon Shutdown()
	jobCount     int64                // number of jobs that have been queued since start
}

// Backend is a [config.BackendInitializer] that initializes a new memory-backed neoq backend
func Backend(ctx context.Context, opts ...config.Option) (backend types.Backend, err error) {
	mb := &MemBackend{
		config:       config.New(),
		cron:         cron.New(),
		mu:           &sync.Mutex{},
		queues:       &sync.Map{},
		handlers:     &sync.Map{},
		futureJobs:   &sync.Map{},
		fingerprints: &sync.Map{},
		logger:       slog.New(slog.NewTextHandler(os.Stdout)),
		jobCount:     0,
		cancelFuncs:  []context.CancelFunc{},
	}
	mb.cron.Start()

	for _, opt := range opts {
		opt(mb.config)
	}

	backend = mb

	return
}

// Enqueue queues jobs to be executed asynchronously
func (m *MemBackend) Enqueue(ctx context.Context, job *jobs.Job) (jobID int64, err error) {
	var queueChan chan *jobs.Job
	var qc any
	var ok bool

	if qc, ok = m.queues.Load(job.Queue); !ok {
		return jobs.UnqueuedJobID, fmt.Errorf("%w: %s", handler.ErrNoProcessorForQueue, job.Queue)
	}

	queueChan = qc.(chan *jobs.Job)

	// Make sure RunAfter is set to a non-zero value if not provided by the caller
	// if already set, schedule the future job
	now := time.Now()
	if job.RunAfter.IsZero() {
		job.RunAfter = now
	}

	if job.Queue == "" {
		err = errors.New("this job does not specify a Queue. Please specify a queue")

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
	jobID = m.jobCount

	if job.RunAfter.Equal(now) {
		queueChan <- job
	} else {
		m.queueFutureJob(job)
	}

	return jobID, nil
}

// Start starts processing jobs with the specified queue and handler
func (m *MemBackend) Start(ctx context.Context, queue string, h handler.Handler) (err error) {
	var queueCapacity = h.QueueCapacity
	if queueCapacity == emptyCapacity {
		queueCapacity = defaultMemQueueCapacity
	}

	m.handlers.Store(queue, h)
	m.queues.Store(queue, make(chan *jobs.Job, queueCapacity))

	ctx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	m.cancelFuncs = append(m.cancelFuncs, cancel)
	m.mu.Unlock()

	err = m.start(ctx, queue)
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

	err = m.Start(ctx, queue, h)
	if err != nil {
		return fmt.Errorf("error processing queue '%s': %w", queue, err)
	}

	if err := m.cron.AddFunc(cronSpec, func() {
		_, _ = m.Enqueue(ctx, &jobs.Job{Queue: queue})
	}); err != nil {
		return fmt.Errorf("error adding cron: %w", err)
	}

	return
}

// SetLogger sets this backend's logger
func (m *MemBackend) SetLogger(logger logging.Logger) {
	m.logger = logger
}

// Shutdown halts the worker
func (m *MemBackend) Shutdown(ctx context.Context) {
	for _, f := range m.cancelFuncs {
		f()
	}

	m.cancelFuncs = nil
}

// start starts a processor that handles new incoming jobs and future jobs
func (m *MemBackend) start(ctx context.Context, queue string) (err error) {
	var queueChan chan *jobs.Job
	var qc any
	var ht any
	var h handler.Handler
	var ok bool

	if ht, ok = m.handlers.Load(queue); !ok {
		return fmt.Errorf("%w: %s", handler.ErrNoHandlerForQueue, queue)
	}

	if qc, ok = m.queues.Load(queue); !ok {
		return fmt.Errorf("%w: %s", handler.ErrNoProcessorForQueue, queue)
	}

	go func() { m.scheduleFutureJobs(ctx, queue) }()

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
				case <-ctx.Done():
					return
				}

				if err != nil {
					m.logger.Error("job failed", err, "job_id", job.ID)
					runAfter := internal.CalculateBackoff(job.Retries)
					job.RunAfter = runAfter
					m.queueFutureJob(job)
				}

				m.fingerprints.Delete(job.Fingerprint)
			}
		}()
	}

	return nil
}

func (m *MemBackend) scheduleFutureJobs(ctx context.Context, queue string) {
	// check for new future jobs on an interval
	ticker := time.NewTicker(m.config.JobCheckInterval)

	for {
		// loop over list of future jobs, scheduling goroutines to wait for jobs that are due within the next 30 seconds
		m.futureJobs.Range(func(_, v any) bool {
			job := v.(*jobs.Job)
			var queueChan chan *jobs.Job

			timeUntilRunAfter := time.Until(job.RunAfter)
			if timeUntilRunAfter <= m.config.FutureJobWindow {
				m.removeFutureJob(job.ID)
				go func(j *jobs.Job) {
					scheduleCh := time.After(timeUntilRunAfter)
					<-scheduleCh
					if qc, ok := m.queues.Load(queue); ok {
						queueChan = qc.(chan *jobs.Job)
						queueChan <- j
					} else {
						m.logger.Error(fmt.Sprintf("no queue processor for queue '%s'", queue), errors.New("no queue processor configured"))
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
	ctxv := handler.CtxVars{Job: job}
	hctx := handler.WithContext(ctx, ctxv)

	// check if the job is being retried and increment retry count accordingly
	if job.Status != internal.JobStatusNew {
		job.Retries++
	}

	// execute the queue handler of this job
	err = handler.Exec(hctx, h)
	if err != nil {
		job.Error = null.StringFrom(err.Error())
	}

	return
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
