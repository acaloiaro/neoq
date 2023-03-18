package handler

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/acaloiaro/neoq/jobs"
)

const (
	DefaultHandlerDeadline = 30 * time.Second
)

type contextKey struct{}

var (
	JobCtxVarKey           contextKey
	ErrContextHasNoJob     = errors.New("context has no Job")
	ErrNoHandlerForQueue   = errors.New("no handler for queue")
	ErrNoProcessorForQueue = errors.New("no processor configured for queue")
)

// Func is a function that Handlers execute for every Job on a queue
type Func func(ctx context.Context) error

// Handler handles jobs on a queue
type Handler struct {
	Handle        Func
	Concurrency   int
	Deadline      time.Duration
	QueueCapacity int64
	JobsChannel   chan *jobs.Job
}

// Option is function that sets optional configuration for Handlers
type Option func(w *Handler)

// WithOptions sets one or more options on handler
func (h *Handler) WithOptions(opts ...Option) {
	for _, opt := range opts {
		opt(h)
	}
}

// Deadline configures handlers with a time deadline for every executed job
// The deadline is the amount of time that can be spent executing the handler's Func
// when a deadline is exceeded, the job is failed and enters its retry phase
func Deadline(d time.Duration) Option {
	return func(h *Handler) {
		h.Deadline = d
	}
}

// Concurrency configures Neoq handlers to process jobs concurrently
// the default concurrency is the number of (v)CPUs on the machine running Neoq
func Concurrency(c int) Option {
	return func(h *Handler) {
		h.Concurrency = c
	}
}

// MaxQueueCapacity configures Handlers to enforce a maximum capacity on the queues that it handles
// queues that have reached capacity cause Enqueue() to block until the queue is below capacity
func MaxQueueCapacity(capacity int64) Option {
	return func(h *Handler) {
		h.QueueCapacity = capacity
	}
}

// New creates a new queue handler
func New(f Func, opts ...Option) (h Handler) {
	h = Handler{
		Handle: f,
	}

	h.WithOptions(opts...)

	// default to running one fewer threads than CPUs
	if h.Concurrency == 0 {
		Concurrency(runtime.NumCPU() - 1)(&h)
	}

	// always set a job deadline if none is set
	if h.Deadline == 0 {
		Deadline(DefaultHandlerDeadline)(&h)
	}

	return
}

// WithJobContext creates a new context with the Job set
func WithJobContext(ctx context.Context, j *jobs.Job) context.Context {
	return context.WithValue(ctx, JobCtxVarKey, j)
}

// Exec executes handler functions with a concrete time deadline
func Exec(ctx context.Context, handler Handler) (err error) {
	deadlineCtx, cancel := context.WithDeadline(ctx, time.Now().Add(handler.Deadline))
	defer cancel()

	var errCh = make(chan error, 1)
	var done = make(chan bool)
	go func(ctx context.Context) {
		errCh <- handler.Handle(ctx)
		done <- true
	}(ctx)

	select {
	case <-done:
		err = <-errCh
		if err != nil {
			err = fmt.Errorf("job failed to process: %w", err)
		}

	case <-deadlineCtx.Done():
		ctxErr := deadlineCtx.Err()
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			err = fmt.Errorf("job exceeded its %s deadline: %w", handler.Deadline, ctxErr)
		} else if errors.Is(ctxErr, context.Canceled) {
			err = ctxErr
		} else {
			err = fmt.Errorf("job failed to process: %w", ctxErr)
		}
	}

	return
}

// JobFromContext fetches the job from a context if the job context variable is already set
func JobFromContext(ctx context.Context) (j *jobs.Job, err error) {
	var ok bool
	if j, ok = ctx.Value(JobCtxVarKey).(*jobs.Job); ok {
		return
	}

	return nil, ErrContextHasNoJob
}
