package neoq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/guregu/null"
	"github.com/iancoleman/strcase"
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

const (
	DefaultQueueCapacity = 10000 // the default capacity of individual queues
	EmptyCapacity        = 0
)

// MemBackend is a memory-backed neoq backend
type MemBackend struct {
	cancelFuncs []context.CancelFunc // A collection of cancel functions to be called upon Shutdown()
	config      *memConfig
	cron        *cron.Cron
	logger      Logger
	mu          *sync.Mutex        // mutext to protect mutating state on a pgWorker
	handlers    map[string]Handler // a map of queue names to queue handlers
	jobCount    int64              // number of jobs that have been queued
	futureJobs  map[int64]Job
	queues      map[string](chan Job)
}

type memConfig struct{}

func NewMemBackend(opts ...ConfigOption) (n Neoq, err error) {
	mb := &MemBackend{
		config:      &memConfig{},
		cron:        cron.New(),
		mu:          &sync.Mutex{},
		handlers:    make(map[string]Handler),
		futureJobs:  make(map[int64]Job),
		queues:      make(map[string](chan Job)),
		logger:      slog.New(slog.NewTextHandler(os.Stdout)),
		jobCount:    0,
		cancelFuncs: []context.CancelFunc{},
	}
	mb.cron.Start()

	for _, opt := range opts {
		opt(mb)
	}

	n = mb

	return
}

// Enqueue queues jobs to be executed asynchronously
func (m *MemBackend) Enqueue(ctx context.Context, job Job) (jobID int64, err error) {
	if _, ok := m.queues[job.Queue]; !ok {
		return UnqueuedJobID, fmt.Errorf("queue has no listeners: %v", job.Queue)
	}

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

	m.mu.Lock()
	m.jobCount++
	m.mu.Unlock()

	job.ID = m.jobCount
	jobID = m.jobCount

	// notify listeners that a new job has arrived if it's not a future job
	if job.RunAfter == now {
		m.queues[job.Queue] <- job
	} else {
		m.mu.Lock()
		m.futureJobs[job.ID] = job
		m.mu.Unlock()
	}

	return
}

// Listen listens for jobs on a queue and processes them with the given handler
func (m *MemBackend) Listen(ctx context.Context, queue string, h Handler) (err error) {
	var queueCapacity = h.queueCapacity
	if queueCapacity == EmptyCapacity {
		queueCapacity = DefaultQueueCapacity
	}

	cctx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	m.cancelFuncs = append(m.cancelFuncs, cancel)
	m.handlers[queue] = h
	m.queues[queue] = make(chan Job, queueCapacity)
	m.mu.Unlock()

	err = m.start(cctx, queue)
	if err != nil {
		return
	}

	return
}

// ListenCron listens for jobs on a cron schedule and handles them with the provided handler
//
// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
func (m MemBackend) ListenCron(ctx context.Context, cronSpec string, h Handler) (err error) {
	cd, err := crondescriptor.NewCronDescriptor(cronSpec)
	if err != nil {
		return err
	}

	cdStr, err := cd.GetDescription(crondescriptor.Full)
	if err != nil {
		return err
	}

	queue := stripNonAlphanum(strcase.ToSnake(*cdStr))

	cctx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancelFuncs = append(m.cancelFuncs, cancel)
	m.mu.Unlock()

	err = m.Listen(cctx, queue, h)
	if err != nil {
		return
	}

	m.cron.AddFunc(cronSpec, func() {
		m.Enqueue(ctx, Job{Queue: queue})
	})

	return
}

// Shutdown halts the worker
func (m *MemBackend) Shutdown(ctx context.Context) (err error) {
	for _, f := range m.cancelFuncs {
		f()
	}

	m.cancelFuncs = nil

	return
}

// WithConfig configures neoq with with optional configuration
func (m MemBackend) WithConfig(opt ConfigOption) (n Neoq) {
	return
}

// start starts a queue listener, processes pending job, and fires up goroutines to process future jobs
func (m MemBackend) start(ctx context.Context, queue string) (err error) {
	var listenJobChan chan Job
	var handler Handler
	var ok bool
	if handler, ok = m.handlers[queue]; !ok {
		return fmt.Errorf("no handler for queue: %s", queue)
	}

	if listenJobChan, ok = m.queues[queue]; !ok {
		return fmt.Errorf("no listen channel configured for qeuue: %s", queue)
	}

	go func() { m.scheduleFutureJobs(ctx, queue) }()

	for i := 0; i < handler.concurrency; i++ {
		go func() {
			var err error
			var job Job

			for {
				select {
				case job = <-listenJobChan:
					err = m.handleJob(ctx, job, handler)
				case <-ctx.Done():
					return
				}

				if err != nil {
					m.logger.Error("error handling job", err, "job_id", job.ID)
				}
			}
		}()
	}
	return
}

func (m MemBackend) scheduleFutureJobs(ctx context.Context, queue string) {
	// check for new future jobs on an interval
	// TODO make this time configurable
	ticker := time.NewTicker(5 * time.Second)

	var job Job
	for {
		// loop over list of future jobs, scheduling goroutines to wait for jobs that are due within the next 30 seconds
		// TODO: Make 30 seconds configurable
		for _, job = range m.futureJobs {
			at := time.Until(job.RunAfter)
			if at <= time.Duration(30*time.Second) {
				m.removeFutureJob(job.ID)
				go func(j Job) {
					scheduleCh := time.After(at)
					<-scheduleCh
					if listenJobChan, ok := m.queues[queue]; ok {
						listenJobChan <- j
					} else {
						m.logger.Error(fmt.Sprintf("no listen channel configured for queue: %s", queue), errors.New("no listener configured"))
					}

				}(job)
			}
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (m MemBackend) handleJob(ctx context.Context, job Job, handler Handler) (err error) {
	ctxv := handlerCtxVars{job: &job}
	hctx := withHandlerContext(ctx, ctxv)

	// check if the job is being retried and increment retry count accordingly
	if job.Status != JobStatusNew {
		job.Retries = job.Retries + 1
	}

	// execute the queue handler of this job
	handlerErr := execHandler(hctx, handler)
	if handlerErr != nil {
		job.Error = null.StringFrom(handlerErr.Error())
	}

	return
}

// removeFutureJob removes a future job from the in-memory list of jobs that will execute in the future
func (m *MemBackend) removeFutureJob(jobID int64) {
	if _, ok := m.futureJobs[jobID]; ok {
		m.mu.Lock()
		delete(m.futureJobs, jobID)
		m.mu.Unlock()
	}
}
