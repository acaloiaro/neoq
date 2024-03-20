package neoq

import (
	"context"
	"errors"
	"time"

	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/jobs"
	"github.com/pranavmodx/neoq-sqlite/logging"
)

const (
	DefaultIdleTxTimeout = 30000
	// the window of time between time.Now() and when a job's RunAfter comes due that neoq will schedule a goroutine to
	// schdule the job for execution.
	// E.g. right now is 16:00 and a job's RunAfter is 16:30 of the same date. This job will get a dedicated goroutine
	// to wait until the job's RunAfter, scheduling the job to be run exactly at RunAfter
	DefaultFutureJobWindow  = 30 * time.Second
	DefaultJobCheckInterval = 1 * time.Second
)

var ErrBackendNotSpecified = errors.New("a backend must be specified")

// Config configures neoq and its backends
//
// This configuration struct includes options for all backends. As such, some of its options are not applicable to all
// backends. [BackendConcurrency], for example, is only used by the redis backend. Other backends manage concurrency on a
// per-handler basis.
type Config struct {
	BackendInitializer     BackendInitializer
	BackendAuthPassword    string                   // password with which to authenticate to the backend
	BackendConcurrency     int                      // total number of backend processes available to process jobs
	ConnectionString       string                   // a string containing connection details for the backend
	JobCheckInterval       time.Duration            // the interval of time between checking for new future/retry jobs
	FutureJobWindow        time.Duration            // time duration between current time and job.RunAfter that future jobs get scheduled
	IdleTransactionTimeout int                      // number of milliseconds PgBackend transaction may idle before the connection is killed
	ShutdownTimeout        time.Duration            // duration to wait for jobs to finish during shutdown
	SynchronousCommit      bool                     // Postgres: Enable synchronous commits (increases durability, decreases performance)
	LogLevel               logging.LogLevel         // the log level of the default logger
	PGConnectionTimeout    time.Duration            // the amount of time to wait for a connection to become available before timing out
	RecoveryCallback       handler.RecoveryCallback // the recovery handler applied to all Handlers excuted by the associated Neoq instance
}

// ConfigOption is a function that sets optional backend configuration
type ConfigOption func(c *Config)

// NewConfig initiailizes a new Config with defaults
func NewConfig() *Config {
	return &Config{
		FutureJobWindow:  DefaultFutureJobWindow,
		JobCheckInterval: DefaultJobCheckInterval,
		RecoveryCallback: handler.DefaultRecoveryCallback,
	}
}

// BackendInitializer is a function that initializes a backend
type BackendInitializer func(ctx context.Context, opts ...ConfigOption) (backend Neoq, err error)

// Neoq interface is Neoq's primary API
//
// Neoq is implemented by:
//   - [pkg/github.com/acaloiaro/neoq/backends/memory.MemBackend]
//   - [pkg/github.com/acaloiaro/neoq/backends/postgres.PgBackend]
//   - [pkg/github.com/acaloiaro/neoq/backends/redis.RedisBackend]
type Neoq interface {
	// Enqueue queues jobs to be executed asynchronously
	Enqueue(ctx context.Context, job *jobs.Job) (jobID string, err error)

	// Start starts processing jobs on the queue specified in the Handler
	Start(ctx context.Context, h handler.Handler) (err error)

	// StartCron starts processing jobs with the specified cron schedule and handler
	//
	// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
	StartCron(ctx context.Context, cron string, h handler.Handler) (err error)

	// SetLogger sets the backend logger
	SetLogger(logger logging.Logger)

	// Shutdown halts job processing and releases resources
	Shutdown(ctx context.Context)
}

// New creates a new backend instance for job processing.
//
// By default, neoq initializes [memory.Backend] if New() is called without a backend configuration option.
//
// Use [neoq.WithBackend] to initialize different backends.
//
// For available configuration options see [neoq.ConfigOption].
func New(ctx context.Context, opts ...ConfigOption) (b Neoq, err error) {
	c := Config{}
	for _, opt := range opts {
		opt(&c)
	}

	if c.BackendInitializer == nil {
		err = ErrBackendNotSpecified
		return
	}

	b, err = c.BackendInitializer(ctx, opts...)
	if err != nil {
		return
	}

	return
}

// WithBackend configures neoq to initialize a specific backend for job processing.
//
// Neoq provides two [config.BackendInitializer] that may be used with WithBackend
//   - [pkg/github.com/acaloiaro/neoq/backends/memory.Backend]
//   - [pkg/github.com/acaloiaro/neoq/backends/postgres.Backend]
func WithBackend(initializer BackendInitializer) ConfigOption {
	return func(c *Config) {
		c.BackendInitializer = initializer
	}
}

// WithRecoveryCallback configures neoq with a function to be called when fatal errors occur in job Handlers.
//
// Recovery callbacks are useful for reporting errors to error loggers and collecting error metrics
func WithRecoveryCallback(cb handler.RecoveryCallback) ConfigOption {
	return func(c *Config) {
		c.RecoveryCallback = cb
	}
}

// WithJobCheckInterval configures the duration of time between checking for future jobs
func WithJobCheckInterval(interval time.Duration) ConfigOption {
	return func(c *Config) {
		c.JobCheckInterval = interval
	}
}

// WithLogLevel configures the log level for neoq's default logger. By default, log level is "INFO".
// if SetLogger is used, WithLogLevel has no effect on the set logger
func WithLogLevel(level logging.LogLevel) ConfigOption {
	return func(c *Config) {
		c.LogLevel = level
	}
}
