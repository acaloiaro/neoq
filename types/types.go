package types

import (
	"context"

	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
)

// Backend interface is Neoq's primary API
//
// Backend is implemented by:
//   - [pkg/github.com/acaloiaro/neoq/backends/memory.PgBackend]
//   - [pkg/github.com/acaloiaro/neoq/backends/postgres.PgBackend]
type Backend interface {
	// Enqueue queues jobs to be executed asynchronously
	Enqueue(ctx context.Context, job *jobs.Job) (jobID string, err error)

	// Start starts processing jobs with the specified queue and handler
	Start(ctx context.Context, queue string, h handler.Handler) (err error)

	// StartCron starts processing jobs with the specified cron schedule and handler
	//
	// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
	StartCron(ctx context.Context, cron string, h handler.Handler) (err error)

	// SetLogger sets the backend logger
	SetLogger(logger logging.Logger)

	// Shutdown halts job processing and releases resources
	Shutdown(ctx context.Context)
}
