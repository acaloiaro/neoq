package config

import (
	"context"
	"time"

	"github.com/acaloiaro/neoq/types"
)

const (
	DefaultIdleTxTimeout = 30000
	// the window of time between time.Now() and when a job's RunAfter comes due that neoq will schedule a goroutine to
	// schdule the job for execution.
	// E.g. right now is 16:00 and a job's RunAfter is 16:30 of the same date. This job will get a dedicated goroutine to
	// wait until the job's RunAfter, scheduling the job to be run exactly at RunAfter
	DefaultFutureJobWindow    = 30 * time.Second
	DefaultJobCheckInterval   = 5 * time.Second
	DefaultTransactionTimeout = time.Minute
)

type Config struct {
	BackendInitializer     BackendInitializer
	ConnectionString       string        // a string containing connection details for the backend
	JobCheckInterval       time.Duration // the interval of time between checking for new future/retry jobs
	FutureJobWindow        time.Duration // time duration between current time and job.RunAfter that goroutines schedule for future jobs
	IdleTransactionTimeout int           // the number of milliseconds PgBackend transaction may idle before the connection is killed
}

// Option is a function that sets optional backend configuration
type Option func(c *Config)

// New initiailizes a new Config with defaults
func New() *Config {
	return &Config{
		FutureJobWindow:        DefaultFutureJobWindow,
		JobCheckInterval:       DefaultJobCheckInterval,
		IdleTransactionTimeout: DefaultIdleTxTimeout,
	}
}

// WithConnectionString configures neoq to use the specified connection string when connecting to a backend
func WithConnectionString(connectionString string) Option {
	return func(c *Config) {
		c.ConnectionString = connectionString
	}
}

// BackendInitializer is a function that initializes a backend
type BackendInitializer func(ctx context.Context, opts ...Option) (backend types.Backend, err error)
