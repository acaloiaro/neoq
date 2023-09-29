package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
)

var errPeriodicTimeout = errors.New("timed out waiting for periodic job")

func flushDB() {
	ctx := context.Background()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		return
	}

	conn, err := pgx.Connect(context.Background(), dbURL)
	if err != nil {
		// an error was encountered connecting to the db. this may simply mean that we're running tests against an
		// uninitialized database and the database needs to be created. By creating a new pg backend instance with
		// neoq.New, we run the db initialization process.
		// if no errors return from `New`, then we've succeeded
		var newErr error
		_, newErr = neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(dbURL))
		if newErr != nil {
			fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
			return
		}
	}
	defer conn.Close(context.Background())

	_, err = conn.Query(context.Background(), "DELETE FROM neoq_jobs") // nolint: gocritic
	if err != nil {
		fmt.Fprintf(os.Stderr, "'neoq_jobs' table flush failed: %v\n", err)
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

	timeoutTimer := time.After(5 * time.Second)

	connString := os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		t.Skip("Skipping: TEST_DATABASE_URL not set")
		return
	}

	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	deadline := time.Now().UTC().Add(5 * time.Second)
	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		Deadline: &deadline,
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

func TestMultipleProcessors(t *testing.T) {
	const queue = "testing"

	connString := os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		t.Skip("Skipping: TEST_DATABASE_URL not set")
		return
	}

	t.Cleanup(func() {
		flushDB()
	})

	var execCount uint32
	var wg sync.WaitGroup
	count := 8
	neos := make([]neoq.Neoq, 0, count)
	// Create several neoq processors such that we can enqueue several jobs and have them consumed by multiple different
	// workers. We want to make sure that a job is not processed twice in a pool of many different neoq workers.
	for i := 0; i < count; i++ {
		ctx := context.Background()
		nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString(connString))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			nq.Shutdown(ctx)
		})

		h := handler.New(queue, func(_ context.Context) (err error) {
			// Make sure that by wasting some time working on a thing we don't consume two jobs back to back.
			// This should give the other neoq clients enough time to grab a job as well.
			time.Sleep(500 * time.Millisecond)
			atomic.AddUint32(&execCount, 1)
			wg.Done()
			return
		})
		// Make sure that each neoq worker only works on one thing at a time.
		h.Concurrency = 1

		err = nq.Start(ctx, h)
		if err != nil {
			t.Error(err)
		}

		neos = append(neos, nq)
	}

	// From one of the neoq clients, enqueue several jobs. At least one per processor registered above.
	nq := neos[0]
	for i := 0; i < count; i++ {
		wg.Add(1)
		ctx := context.Background()
		deadline := time.Now().UTC().Add(10 * time.Second)
		jid, e := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("hello world: %d", i),
			},
			Deadline: &deadline,
		})
		if e != nil || jid == jobs.DuplicateJobID {
			t.Error(e)
		}
	}

	// Wait for all jobs to complete.
	wg.Wait()

	// Make sure that we executed the expected number of jobs.
	if execCount != uint32(count) {
		t.Fatalf("mismatch number of executions. Expected: %d Found: %d", count, execCount)
	}
}

// TestDuplicateJobRejection tests that the backend rejects jobs that are duplicates
func TestDuplicateJobRejection(t *testing.T) {
	const queue = "testing"

	connString := os.Getenv("TEST_DATABASE_URL")
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

	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})
	if e != nil || jid == jobs.DuplicateJobID {
		err = e
	}

	_, e2 := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})

	// we submitted two duplicate jobs; the error should be a duplicate job error
	if !errors.Is(e2, postgres.ErrDuplicateJob) {
		err = e2
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

	timeoutTimer := time.After(5 * time.Second)

	connString := os.Getenv("TEST_DATABASE_URL")
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

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	h2 := handler.New(queue2, func(_ context.Context) (err error) {
		done <- true
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	err = nq.Start(ctx, h2)
	if err != nil {
		t.Error(err)
	}

	go func() {
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
	}()

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
	connString := os.Getenv("TEST_DATABASE_URL")
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

	h := handler.NewPeriodic(func(ctx context.Context) (err error) {
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
	logsChan := make(chan string, 100)
	timeoutTimer := time.After(5 * time.Second)
	connString := os.Getenv("TEST_DATABASE_URL")
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

	h := handler.New(queue, func(_ context.Context) (err error) {
		err = errors.New("something bad happened") // nolint: goerr113
		return
	})

	nq.SetLogger(testutils.NewTestLogger(logsChan))

	err = nq.Start(ctx, h)
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

results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case actualLogMsg := <-logsChan:
			expectedLogMsg := "job failed to process: something bad happened"
			if strings.Contains(actualLogMsg, expectedLogMsg) {
				err = nil
				break results_loop
			}
			err = fmt.Errorf("'%s' NOT CONTAINS '%s'", actualLogMsg, expectedLogMsg) //nolint:all
		}
	}

	if err != nil {
		t.Error(err)
	}

	t.Cleanup(func() {
		flushDB()
	})
}
