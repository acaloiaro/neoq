//go:build testing

package memory

import (
	"context"
	"sync"

	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/logging"
	"github.com/acaloiaro/neoq/types"
	"github.com/robfig/cron"
)

// TestingBackend initializes a backend for testing purposes
func TestingBackend(conf *config.Config,
	c *cron.Cron,
	queues, h, futureJobs, fingerprints *sync.Map,
	logger logging.Logger) config.BackendInitializer {
	return func(ctx context.Context, opts ...config.Option) (backend types.Backend, err error) {
		mb := &MemBackend{
			config:       conf,
			cron:         c,
			mu:           &sync.Mutex{},
			queues:       queues,
			handlers:     h,
			futureJobs:   futureJobs,
			fingerprints: fingerprints,
			logger:       logger,
			jobCount:     0,
			cancelFuncs:  []context.CancelFunc{},
		}
		mb.cron.Start()

		for _, opt := range opts {
			opt(mb.config)
		}

		backend = mb

		return
	}
}
