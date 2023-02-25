package neoq

import (
	"context"
	"errors"
	"time"

	"github.com/acaloiaro/neoq/backends/memory"
	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/types"
)

var (
	ErrConnectionStringRequired = errors.New("a connection string is required for this backend. See [config.WithConnectionString]")
	ErrNoBackendSpecified       = errors.New("please specify a backend by using [config.WithBackend]")
)

// New creates a new backend instance for job processing.
//
// By default, neoq initializes [memory.Backend] if New() is called without a backend configuration option.
//
// Use [neoq.WithBackend] to initialize different backends.
//
// For available configuration options see [config.ConfigOption].
func New(ctx context.Context, opts ...config.Option) (b types.Backend, err error) {
	c := config.Config{}
	for _, opt := range opts {
		opt(&c)
	}

	if c.BackendInitializer == nil {
		c.BackendInitializer = memory.Backend
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
func WithBackend(initializer config.BackendInitializer) config.Option {
	return func(c *config.Config) {
		c.BackendInitializer = initializer
	}
}

// WithJobCheckInterval configures the duration of time between checking for future jobs
func WithJobCheckInterval(interval time.Duration) config.Option {
	return func(c *config.Config) {
		c.JobCheckInterval = interval
	}
}
