package handler

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"golang.org/x/exp/slog"
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

// RecoveryCallback is a function to be called when fatal errors/panics occur in Handlers
type RecoveryCallback func(ctx context.Context, err error) (erro error)

// DefaultRecoveryCallback is the function that gets called by default when handlers panic
func DefaultRecoveryCallback(_ context.Context, _ error) (err error) {
	slog.Error("recovering from a panic in the job handler", slog.Any("stack", string(debug.Stack())))
	return nil
}

// Handler handles jobs on a queue
type Handler struct {
	Handle          Func
	Concurrency     int
	JobTimeout      time.Duration
	QueueCapacity   int64
	Queue           string
	RecoverCallback RecoveryCallback // function called when fatal handler errors occur
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

// Queue configures the name of the queue that the handler runs on
func Queue(queue string) Option {
	return func(h *Handler) {
		h.Queue = queue
	}
}

// RecoverCallback configures the handler with a recovery function to be called when fatal errors occur in Handlers
func RecoverCallback(f RecoveryCallback) Option {
	return func(h *Handler) {
		h.RecoverCallback = f
	}
}

// New creates new queue handlers for specific queues. This function is to be usued to create new Handlers for
// non-periodic jobs (most jobs). Use [NewPeriodic] to initialize handlers for periodic jobs.
func New(queue string, f Func, opts ...Option) (h Handler) {
	h = Handler{
		Handle: f,
		Queue:  queue,
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

// NewPeriodic creates new queue handlers for periodic jobs.  Use [New] to initialize handlers for non-periodic jobs.
func NewPeriodic(f Func, opts ...Option) (h Handler) {
	h = New("", f, opts...)
	return
}

func errorFromPanic(x any) (err error) {
	_, file, line, ok := runtime.Caller(1) // skip the first frame (panic itself)
	if ok && strings.Contains(file, "runtime/") {
		// The panic came from the runtime, most likely due to incorrect
		// map/slice usage. The parent frame should have the real trigger.
		_, file, line, ok = runtime.Caller(2) //nolint: gomnd
	}

	// Include the file and line number info in the error, if runtime.Caller returned ok.
	if ok {
		err = fmt.Errorf("panic [%s:%d]: %v", file, line, x) // nolint: goerr113
	} else {
		err = fmt.Errorf("panic: %v", x) // nolint: goerr113
	}

	return
}

// Exec executes handler functions with a concrete timeout
func Exec(ctx context.Context, handler Handler) (err error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, handler.JobTimeout)
	defer cancel()

	errCh := make(chan error, 1)
	done := make(chan bool)

	go func(ctx context.Context) {
		log.Println("PS::handler.Exec called")
		defer func() {
			if x := recover(); x != nil {
				err = errorFromPanic(x)
				errCh <- err
				if handler.RecoverCallback != nil {
					err = handler.RecoverCallback(ctx, err)
					if err != nil {
						slog.Error("handler recovery callback also failed while recovering from panic", slog.Any("error", err))
					}
				}
			}
			done <- true
		}()

		errCh <- handler.Handle(ctx)
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

	return err
}
