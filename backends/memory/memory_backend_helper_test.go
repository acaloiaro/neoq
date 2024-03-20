//go:build testing

package memory

import (
	"context"
	"sync"

	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/logging"
	"github.com/robfig/cron"
)

// TestingBackend initializes a backend for testing purposes
func TestingBackend(conf *neoq.Config,
	c *cron.Cron,
	queues, h, futureJobs, fingerprints *sync.Map,
	logger logging.Logger,
) neoq.BackendInitializer {
	return func(ctx context.Context, opts ...neoq.ConfigOption) (backend neoq.Neoq, err error) {
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
