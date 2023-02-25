package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/pkg/errors"
	"golang.org/x/exp/slog"
)

// TestPgBackendBasicJobProcessing tests that the postgres backend is able to process the most basic jobs with the
// most basic configuration.
func TestPgBackendBasicJobProcessing(t *testing.T) {
	queue := "testing"
	numJobs := 10
	doneCnt := 0
	done := make(chan bool)
	var timeoutTimer = time.After(5 * time.Second)

	var connString = os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		t.Skip("Skipping: TEST_DATABASE_URL not set")
		return
	}

	ctx := context.TODO()
	nq, err := postgres.NewPgBackend(ctx, connString)
	if err != nil {
		t.Fatal(err)
	}

	h := handler.New(func(_ context.Context) (err error) {
		done <- true
		return
	})

	nq.Listen(ctx, queue, h)

	go func() {
		for i := 0; i < numJobs; i++ {
			jid, e := nq.Enqueue(ctx, &jobs.Job{
				Queue: queue,
				Payload: map[string]interface{}{
					"message": fmt.Sprintf("hello world: %d", i),
				},
			})
			if e != nil || jid == internal.DuplicateJobID {
				slog.Error("job was not enqueued. either it was duplicate or this error caused it:", e)
			}
		}
	}()

	for {
		select {
		case <-timeoutTimer:
			err = errors.New("timed out waiting for job(s)")
		case <-done:
			doneCnt++
		}

		if doneCnt >= numJobs {
			break
		}

		if err != nil {
			break
		}
	}

	if err != nil {
		t.Error(err)
	}

	nq.Shutdown(ctx)
}
