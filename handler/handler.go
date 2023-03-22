package handler

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"
)

const (
	DefaultHandlerTimeout = 30 * time.Second
)

var (
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
	JobTimeout    time.Duration
	QueueCapacity int64
}

// Option is function that sets optional configuration for Handlers
type Option func(w *Handler)

// WithOptions sets one or more options on handler
func (h *Handler) WithOptions(opts ...Option) {
	for _, opt := range opts {
		opt(h)
	}
}

// JobTimeout configures handlers with a time deadline for every executed job
// The timeout is the amount of time that can be spent executing the handler's Func
// when a timeout is exceeded, the job fails and enters its retry phase
func JobTimeout(d time.Duration) Option {
	return func(h *Handler) {
		h.JobTimeout = d
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

	// default to running on as many goroutines as there are CPUs
	if h.Concurrency == 0 {
		Concurrency(runtime.NumCPU())(&h)
	}

	// always set a job timeout if none is set
	if h.JobTimeout == 0 {
		JobTimeout(DefaultHandlerTimeout)(&h)
	}

	return
}

// Exec executes handler functions with a concrete timeout
func Exec(ctx context.Context, handler Handler) (err error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, handler.JobTimeout)
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

	case <-timeoutCtx.Done():
		ctxErr := timeoutCtx.Err()
		if errors.Is(ctxErr, context.DeadlineExceeded) {
			err = fmt.Errorf("job exceeded its %s timeout: %w", handler.JobTimeout, ctxErr)
		} else if errors.Is(ctxErr, context.Canceled) {
			err = ctxErr
		} else {
			err = fmt.Errorf("job failed to process: %w", ctxErr)
		}
	}

	return
}
