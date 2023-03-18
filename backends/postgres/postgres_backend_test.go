package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
)

var errPeriodicTimeout = errors.New("timed out waiting for periodic job")

// TestBasicJobProcessing tests that the postgres backend is able to process the most basic jobs with the
// most basic configuration.
func TestBasicJobProcessing(t *testing.T) {
	const queue = "testing"
	done := make(chan bool)
	defer close(done)

	var timeoutTimer = time.After(5 * time.Second)

	var connString = os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		t.Skip("Skipping: TEST_DATABASE_URL not set")
		return
	}

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), config.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = nq.Start(ctx, queue, h)
	if err != nil {
		t.Error(err)
	}

	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e)
	}

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}

	if err != nil {
		t.Error(err)
	}
}

// TestBasicJobMultipleQueue tests that the postgres backend is able to process jobs on multiple queues
func TestBasicJobMultipleQueue(t *testing.T) {
	const queue = "testing"
	const queue2 = "testing2"
	done := make(chan bool)
	doneCnt := 0

	var timeoutTimer = time.After(5 * time.Second)

	var connString = os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		t.Skip("Skipping: TEST_DATABASE_URL not set")
		return
	}

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), config.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(func(_ context.Context) (err error) {
		done <- true
		return
	})

	h2 := handler.New(func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = nq.Start(ctx, queue, h)
	if err != nil {
		t.Error(err)
	}

	err = nq.Start(ctx, queue2, h2)
	if err != nil {
		t.Error(err)
	}

	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": fmt.Sprintf("hello world: %d", internal.RandInt(10000000000)),
		},
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e)
	}

	jid2, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue2,
		Payload: map[string]interface{}{
			"message": fmt.Sprintf("hello world: %d", internal.RandInt(10000000000)),
		},
	})
	if e != nil || jid2 == jobs.DuplicateJobID {
		t.Error(e)
	}

results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case <-done:
			doneCnt++
			if doneCnt == 2 {
				break results_loop
			}
		}
	}

	if err != nil {
		t.Error(err)
	}
}

func TestCron(t *testing.T) {
	done := make(chan bool, 1)
	defer close(done)
	const cron = "* * * * * *"
	var connString = os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		t.Skip("Skipping: TEST_DATABASE_URL not set")
		return
	}

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), config.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(func(ctx context.Context) (err error) {
		done <- true
		return
	})

	h.WithOptions(
		handler.Deadline(500*time.Millisecond),
		handler.Concurrency(1),
	)

	err = nq.StartCron(ctx, cron, h)
	if err != nil {
		t.Error(err)
	}

	// allow time for listener to start
	time.Sleep(5 * time.Millisecond)

	select {
	case <-time.After(1 * time.Second):
		err = errPeriodicTimeout
	case <-done:
	}

	if err != nil {
		t.Error(err)
	}
}
