package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
	"github.com/acaloiaro/neoq/testutils"
	"github.com/jackc/pgx/v5"
	"golang.org/x/exp/slog"
)

var errPeriodicTimeout = errors.New("timed out waiting for periodic job")

func flushDB() {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		return
	}

	conn, err := pgx.Connect(context.Background(), dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	_, err = conn.Query(context.Background(), "DELETE FROM neoq_jobs") // nolint: gocritic
	if err != nil {
		fmt.Fprintf(os.Stderr, "'neoq_jobs' table flush failed: %v\n", err)
		os.Exit(1) // nolint: gocritic
	}
}

func TestMain(m *testing.M) {
	flushDB()
	code := m.Run()
	os.Exit(code)
}

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
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
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

	t.Cleanup(func() {
		flushDB()
	})
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
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
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

	t.Cleanup(func() {
		flushDB()
	})
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
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(func(ctx context.Context) (err error) {
		done <- true
		return
	})

	h.WithOptions(
		handler.JobTimeout(500*time.Millisecond),
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

	t.Cleanup(func() {
		flushDB()
	})
}

// TestBasicJobProcessingWithErrors tests that the postgres backend is able to update the status of jobs that fail
func TestBasicJobProcessingWithErrors(t *testing.T) {
	const queue = "testing"
	done := make(chan bool, 10)
	defer close(done)

	var timeoutTimer = time.After(5 * time.Second)

	var connString = os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		t.Skip("Skipping: TEST_DATABASE_URL not set")
		return
	}

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(postgres.Backend),
		postgres.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(func(_ context.Context) (err error) {
		err = errors.New("something bad happened") // nolint: goerr113
		return
	})

	buf := &strings.Builder{}
	nq.SetLogger(testutils.TestLogger{L: log.New(buf, "", 0), Done: done})

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
		// the error is returned after the done channel receives its message, so we need to give time for the logger to
		// have logged the error that was returned by the handler
		time.Sleep(100 * time.Millisecond)
		expectedLogMsg := "job failed to process: something bad happened"
		actualLogMsg := strings.Trim(buf.String(), "\n")
		if strings.Contains(actualLogMsg, expectedLogMsg) {
			t.Error(fmt.Errorf("'%s' NOT CONTAINS '%s'", actualLogMsg, expectedLogMsg)) //nolint:all
		}
	}

	if err != nil {
		t.Error(err)
	}

	nq.SetLogger(slog.New(slog.HandlerOptions{Level: slog.LevelDebug}.NewTextHandler(os.Stdout)))
	t.Cleanup(func() {
		flushDB()
	})
}
