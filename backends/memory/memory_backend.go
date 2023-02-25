package memory

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	nctx "github.com/acaloiaro/neoq/context"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"

	"github.com/guregu/null"
	"github.com/iancoleman/strcase"
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

const (
	// TODO make MemBackend queue capacity configurable
	defaultMemQueueCapacity = 10000 // the default capacity of individual queues
	emptyCapacity           = 0
)

// MemBackend is a memory-backed neoq backend
type MemBackend struct {
	cancelFuncs      []context.CancelFunc // A collection of cancel functions to be called upon Shutdown()
	cron             *cron.Cron
	logger           logging.Logger
	jobCheckInterval time.Duration // the duration of time between checking for future jobs to schedule
	mu               *sync.Mutex   // mutext to protect mutating state on a pgWorker
	jobCount         int64         // number of jobs that have been queued since start
	handlers         sync.Map      // map queue names [string] to queue handlers [Handler]
	fingerprints     sync.Map      // map fingerprints [string] to their jobs [Job]
	futureJobs       sync.Map      // map jobIDs [int64] to [Jobs]
	queues           sync.Map      // map queue names [string] to queue handler channels [chan Job]
}

// MemConfigOption is a function that sets optional PgBackend configuration
type MemConfigOption func(p *MemBackend)

func NewMemBackend(opts ...MemConfigOption) (m *MemBackend, err error) {
	m = &MemBackend{
		cron:             cron.New(),
		mu:               &sync.Mutex{},
		queues:           sync.Map{},
		handlers:         sync.Map{},
		futureJobs:       sync.Map{},
		fingerprints:     sync.Map{},
		logger:           slog.New(slog.NewTextHandler(os.Stdout)),
		jobCount:         0,
		cancelFuncs:      []context.CancelFunc{},
		jobCheckInterval: internal.DefaultJobCheckInterval,
	}
	m.cron.Start()

	for _, opt := range opts {
		opt(m)
	}

	return
}

// Enqueue queues jobs to be executed asynchronously
func (m *MemBackend) Enqueue(ctx context.Context, job jobs.Job) (jobID int64, err error) {
	var queueChan chan jobs.Job
	var qc any
	var ok bool

	if qc, ok = m.queues.Load(job.Queue); !ok {
		return internal.UnqueuedJobID, fmt.Errorf("queue has no listeners: %s", job.Queue)
	}

	queueChan = qc.(chan jobs.Job)

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

	err = jobs.FingerprintJob(&job)
	if err != nil {
		return
	}

	// if the job fingerprint is already known, don't queue the job
	if _, found := m.fingerprints.Load(job.Fingerprint); found {
		return internal.DuplicateJobID, nil
	}

	m.fingerprints.Store(job.Fingerprint, job)
	m.mu.Lock()
	m.jobCount++
	m.mu.Unlock()

	job.ID = m.jobCount
	jobID = m.jobCount

	// notify listeners that a new job has arrived if it's not a future job
	if job.RunAfter == now {
		queueChan <- job
	} else {
		m.queueFutureJob(job)
	}

	return
}

// Listen listens for jobs on a queue and processes them with the given handler
func (m *MemBackend) Listen(ctx context.Context, queue string, h handler.Handler) (err error) {
	var queueCapacity = h.QueueCapacity
	if queueCapacity == emptyCapacity {
		queueCapacity = defaultMemQueueCapacity
	}

	m.handlers.Store(queue, h)
	m.queues.Store(queue, make(chan jobs.Job, queueCapacity))

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

// ListenCron listens for jobs on a cron schedule and handles them with the provided handler
//
// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
func (m *MemBackend) ListenCron(ctx context.Context, cronSpec string, h handler.Handler) (err error) {
	cd, err := crondescriptor.NewCronDescriptor(cronSpec)
	if err != nil {
		return err
	}

	cdStr, err := cd.GetDescription(crondescriptor.Full)
	if err != nil {
		return err
	}

	queue := internal.StripNonAlphanum(strcase.ToSnake(*cdStr))

	ctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancelFuncs = append(m.cancelFuncs, cancel)
	m.mu.Unlock()

	err = m.Listen(ctx, queue, h)
	if err != nil {
		return
	}

	m.cron.AddFunc(cronSpec, func() {
		m.Enqueue(ctx, jobs.Job{Queue: queue})
	})

	return
}

func (m *MemBackend) SetConfigOption(option string, value any) {}

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

// start starts a queue listener, processes pending job, and fires up goroutines to process future jobs
func (m *MemBackend) start(ctx context.Context, queue string) (err error) {
	var queueChan chan jobs.Job
	var qc any
	var ht any
	var h handler.Handler
	var ok bool
	if ht, ok = m.handlers.Load(queue); !ok {
		return fmt.Errorf("no handler for queue: %s", queue)
	}

	if qc, ok = m.queues.Load(queue); !ok {
		return fmt.Errorf("no listener configured for qeuue: %s", queue)
	}

	go func() { m.scheduleFutureJobs(ctx, queue) }()

	h = ht.(handler.Handler)
	queueChan = qc.(chan jobs.Job)

	for i := 0; i < h.Concurrency; i++ {
		go func() {
			var err error
			var job jobs.Job

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
	return
}

func (m *MemBackend) scheduleFutureJobs(ctx context.Context, queue string) {
	// check for new future jobs on an interval
	// TODO make the future jobs check interval configurable in MemBackend
	ticker := time.NewTicker(m.jobCheckInterval)

	for {
		// loop over list of future jobs, scheduling goroutines to wait for jobs that are due within the next 30 seconds
		m.futureJobs.Range(func(k, v any) bool {
			job := v.(jobs.Job)
			var queueChan chan jobs.Job

			timeUntilRunAfter := time.Until(job.RunAfter)
			if timeUntilRunAfter <= internal.DefaultFutureJobWindow {
				m.removeFutureJob(job.ID)
				go func(j jobs.Job) {
					scheduleCh := time.After(timeUntilRunAfter)
					<-scheduleCh
					if qc, ok := m.queues.Load(queue); ok {
						queueChan = qc.(chan jobs.Job)
						queueChan <- j
					} else {
						m.logger.Error(fmt.Sprintf("no listen channel configured for queue: %s", queue), errors.New("no listener configured"))
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

func (m *MemBackend) handleJob(ctx context.Context, job jobs.Job, h handler.Handler) (err error) {
	ctxv := nctx.HandlerCtxVars{Job: &job}
	hctx := handler.WithHandlerContext(ctx, ctxv)

	// check if the job is being retried and increment retry count accordingly
	if job.Status != internal.JobStatusNew {
		job.Retries = job.Retries + 1
	}

	// execute the queue handler of this job
	err = handler.ExecHandler(hctx, h)
	if err != nil {
		job.Error = null.StringFrom(err.Error())
	}

	return
}

// queueFutureJob queues a future job for eventual execution
func (m *MemBackend) queueFutureJob(job jobs.Job) {
	m.fingerprints.Store(job.Fingerprint, job)
	m.futureJobs.Store(job.ID, job)
}

// removeFutureJob removes a future job from the in-memory list of jobs that will execute in the future
func (m *MemBackend) removeFutureJob(jobID int64) {
	if j, ok := m.futureJobs.Load(jobID); ok {
		job := j.(jobs.Job)
		m.fingerprints.Delete(job.Fingerprint)
		m.futureJobs.Delete(job.ID)
	}
}
