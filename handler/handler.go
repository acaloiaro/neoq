package handler

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	nctx "github.com/acaloiaro/neoq/context"
)

const (
	DefaultHandlerDeadline = 30 * time.Second
)

// HandlerFunc is a function that Handlers execute for every Job on a queue
type HandlerFunc func(ctx context.Context) error

// Handler handles jobs on a queue
type Handler struct {
	Handle        HandlerFunc
	Concurrency   int
	Deadline      time.Duration
	QueueCapacity int64
}

// HandlerOption is function that sets optional configuration for Handlers
type HandlerOption func(w *Handler)

// WithOptions sets one or more options on handler
func (h *Handler) WithOptions(opts ...HandlerOption) {
	for _, opt := range opts {
		opt(h)
	}
}

// HandlerDeadline configures handlers with a time deadline for every executed job
// The deadline is the amount of time that can be spent executing the handler's HandlerFunc
// when a deadline is exceeded, the job is failed and enters its retry phase
func HandlerDeadline(d time.Duration) HandlerOption {
	return func(h *Handler) {
		h.Deadline = d
	}
}

// HandlerConcurrency configures Neoq handlers to process jobs concurrently
// the default concurrency is the number of (v)CPUs on the machine running Neoq
func HandlerConcurrency(c int) HandlerOption {
	return func(h *Handler) {
		h.Concurrency = c
	}
}

// MaxQueueCapacity configures Handlers to enforce a maximum capacity on the queues that it handles
// queues that have reached capacity cause Enqueue() to block until the queue is below capacity
func MaxQueueCapacity(capacity int64) HandlerOption {
	return func(h *Handler) {
		h.QueueCapacity = capacity
	}
}

// NewHandler creates a new queue handler
func NewHandler(f HandlerFunc, opts ...HandlerOption) (h Handler) {
	h = Handler{
		Handle: f,
	}

	h.WithOptions(opts...)

	// default to running one fewer threads than CPUs
	if h.Concurrency == 0 {
		HandlerConcurrency(runtime.NumCPU() - 1)(&h)
	}

	// always set a job deadline if none is set
	if h.Deadline == 0 {
		HandlerDeadline(DefaultHandlerDeadline)(&h)
	}

	return
}

// WithHandlerContext creates a new context with the job and transaction set
func WithHandlerContext(ctx context.Context, v nctx.HandlerCtxVars) context.Context {
	return context.WithValue(ctx, nctx.VarsKey, v)
}

// Exechandler executes handler functions with a concrete time deadline
func ExecHandler(ctx context.Context, handler Handler) (err error) {
	deadlineCtx, cancel := context.WithDeadline(ctx, time.Now().Add(handler.Deadline))
	defer cancel()

	var errCh = make(chan error, 1)
	var done = make(chan bool)
	go func(ctx context.Context) (e error) {
		errCh <- handler.Handle(ctx)
		done <- true
		return
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
			err = nil
		} else {
			err = fmt.Errorf("job failed to process: %w", ctxErr)
		}
	}

	return
}
