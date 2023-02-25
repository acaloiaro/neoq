package neoq

// TODO factor out the following dependencies
// "github.com/iancoleman/strcase"
// "github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
import (
	"context"
	"errors"
	"time"

	"github.com/acaloiaro/neoq/backends/memory"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
)

// Neoq interface is Neoq's primary API
type Neoq interface {
	// Enqueue queues jobs to be executed asynchronously
	Enqueue(ctx context.Context, job jobs.Job) (jobID int64, err error)

	// Listen listens for jobs on a queue and processes them with the given handler
	Listen(ctx context.Context, queue string, h handler.Handler) (err error)

	// ListenCron listens for jobs on a cron schedule and processes them with the given handler
	//
	// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
	ListenCron(ctx context.Context, cron string, h handler.Handler) (err error)

	// SetConfigOption sets arbitrary backend config options
	SetConfigOption(option string, value any)

	// SetLogger sets the backend logger
	SetLogger(logger logging.Logger)

	// Shutdown halts job processing and releases resources
	Shutdown(ctx context.Context)
}

// ConfigOption is a function that sets optional Neoq configuration
type ConfigOption func(n Neoq)

// New creates a new Neoq instance for listening to queues and enqueing new jobs
func New(ctx context.Context, opts ...ConfigOption) (n Neoq, err error) {
	ic := internalConfig{}
	for _, opt := range opts {
		opt(&ic)
	}

	if ic.backend != nil {
		n = *ic.backend
		return
	}

	switch ic.backendName {
	case "postgres":
		if ic.connectionString == "" {
			err = errors.New("your must provide a postgres connection string to initialize the postgres backend: see neoq.ConnectionString(...)")
			return
		}
		// TODO implement pg backend initialization from New()
		//n, err = postgres.NewPgBackend(ctx, ic.connectionString, opts...)
	default:
		n, err = memory.NewMemBackend()
	}

	return
}

// WithBackendName configures neoq to create a new backend with the given name upon
// initialization
func WithBackendName(backendName string) ConfigOption {
	return func(n Neoq) {
		switch c := n.(type) {
		case *internalConfig:
			c.backendName = backendName
		default:
		}
	}
}

// WithJobCheckInterval configures the duration of time between checking for future jobs
func WithJobCheckInterval(interval time.Duration) ConfigOption {
	return func(n Neoq) {
		n.SetConfigOption("jobCheckInternal", interval)
	}
}

// WithBackend configures neoq to use the specified backend rather than initializing a new one
// during initialization
func WithBackend(backend Neoq) ConfigOption {
	return func(n Neoq) {
		n.SetConfigOption("backend", backend)
	}
}

// internalConfig models internal neoq configuratio not exposed to users
type internalConfig struct {
	backendName      string // the name of a known backend
	backend          *Neoq  // user-provided backend to use
	connectionString string // a connection string to use connecting to a backend
}

func (i internalConfig) Enqueue(ctx context.Context, job jobs.Job) (jobID int64, err error) {
	return
}

func (i internalConfig) Listen(ctx context.Context, queue string, h handler.Handler) (err error) {
	return
}

func (i internalConfig) ListenCron(ctx context.Context, cron string, h handler.Handler) (err error) {
	return
}

func (i internalConfig) SetConfigOption(option string, value any) {}

func (i internalConfig) SetLogger(logger logging.Logger) {}

func (i internalConfig) Shutdown(ctx context.Context) {}
