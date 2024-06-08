package sqlite_test

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/sqlite"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed migrations/*.sql
var sqliteMigrationsFS embed.FS

const (
	ConcurrentWorkers = 8
)

var errPeriodicTimeout = errors.New("timed out waiting for periodic job")

func prepareAndCleanupDB(t *testing.T) (dbURL string, db *sql.DB) {
	t.Helper()

	migrations, err := iofs.New(sqliteMigrationsFS, "migrations")
	if err != nil {
		t.Fatalf("unable to run migrations, error during iofs new: %s", err.Error())
	}

	cwd, _ := os.Getwd()
	dbPath := cwd + "/test.db"
	dbURL = "sqlite3://" + dbPath

	m, err := migrate.NewWithSourceInstance("iofs", migrations, dbURL)
	if err != nil {
		t.Fatalf("unable to run migrations, could not create new source: %s", err.Error())
	}

	// We don't need the migration tooling to hold it's connections to the DB once it has been completed.
	defer m.Close()
	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("unable to run migrations, could not apply up migration: %s", err.Error())
	}

	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("unable to open database: %s", err.Error())
	}

	// Delete everything in the neoq_jobs table if it exists
	_, _ = db.Exec("DELETE FROM neoq_jobs")

	// Delete everything in the neoq_dead_jobs table if it exists
	_, _ = db.Exec("DELETE FROM neoq_dead_jobs")

	return dbURL, db
}

// TestBasicJobProcessing tests that the sqlite backend is able to process the most basic jobs with the
// most basic configuration.
func TestBasicJobProcessing(t *testing.T) {
	connString, db := prepareAndCleanupDB(t)
	const queue = "testing"
	maxRetries := 5
	done := make(chan bool)
	defer close(done)

	timeoutTimer := time.After(5 * time.Second)

	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelDebug),
	)
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
		Deadline:   &deadline,
		MaxRetries: &maxRetries,
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

	// ensure job has fields set correctly
	var jdl time.Time
	var jmxrt int
	var jqueue string

	err = db.
		QueryRow("SELECT queue, deadline,max_retries FROM neoq_jobs WHERE id = $1", jid).
		Scan(&jqueue, &jdl, &jmxrt)
	if err != nil {
		t.Error(err)
	}

	if jqueue != queue {
		t.Error(fmt.Errorf("job queue does not match its expected value: %v != %v", jqueue, queue))
	}

	jdl = jdl.In(time.UTC)
	// dates from postgres come out with only 6 decimal places of millisecond precision, naively format dates as
	// strings for comparison reasons. Ref https://www.postgresql.org/docs/current/datatype-datetime.html
	if jdl.Format(time.RFC3339) != deadline.Format(time.RFC3339) {
		t.Error(fmt.Errorf("job deadline does not match its expected value: %v != %v", jdl, deadline)) // nolint: goerr113
	}

	if jmxrt != maxRetries {
		t.Error(fmt.Errorf("job MaxRetries does not match its expected value: %v != %v", jmxrt, maxRetries)) // nolint: goerr113
	}
}

func TestMultipleProcessors(t *testing.T) {
	const queue = "testing"
	var execCount uint32
	var wg sync.WaitGroup

	connString, _ := prepareAndCleanupDB(t)

	h := handler.New(queue, func(_ context.Context) (err error) {
		atomic.AddUint32(&execCount, 1)
		wg.Done()
		return
	})
	// Make sure that each neoq worker only works on one thing at a time.
	h.Concurrency = 1

	neos := make([]neoq.Neoq, 0, ConcurrentWorkers)
	// Create several neoq processors such that we can enqueue several jobs and have them consumed by multiple different
	// workers. We want to make sure that a job is not processed twice in a pool of many different neoq workers.
	for i := 0; i < ConcurrentWorkers; i++ {
		ctx := context.Background()
		nq, err := neoq.New(ctx,
			neoq.WithBackend(sqlite.Backend),
			sqlite.WithConnectionString(connString),
			neoq.WithLogLevel(logging.LogLevelDebug),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			nq.Shutdown(ctx)
		})

		err = nq.Start(ctx, h)
		if err != nil {
			t.Error(err)
		}

		neos = append(neos, nq)
	}

	// From one of the neoq clients, enqueue several jobs. At least one per processor registered above.
	nq := neos[0]
	wg.Add(ConcurrentWorkers)
	for i := 0; i < ConcurrentWorkers; i++ {
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
	if atomic.LoadUint32(&execCount) != uint32(ConcurrentWorkers) {
		t.Fatalf("mismatch number of executions. Expected: %d Found: %d", ConcurrentWorkers, execCount)
	}
}

// TestDuplicateJobRejection tests that the backend rejects jobs that are duplicates
func TestDuplicateJobRejection(t *testing.T) {
	const queue = "testing"

	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelDebug),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		return
	})

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
		err = e
	}

	_, e2 := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})
	if e2 != nil || jid == jobs.DuplicateJobID {
		err = e2
	}

	// we submitted two duplicate jobs; the error should be a duplicate job error
	duplicateJobErr := "UNIQUE constraint failed: neoq_jobs.fingerprint"
	if err != nil && strings.Contains(err.Error(), duplicateJobErr) {
		return
	} else if err != nil {
		t.Error(err)
	}

	if err != nil {
		t.Error(err)
	}
}

// TestBasicJobMultipleQueue tests that the sqlite backend is able to process jobs on multiple queues
func TestBasicJobMultipleQueue(t *testing.T) {
	const queue = "testing"
	const queue2 = "testing2"
	done := make(chan bool)
	doneCnt := 0

	timeoutTimer := time.After(5 * time.Second)

	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelDebug),
	)
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
}

func TestCron(t *testing.T) {
	done := make(chan bool, 1)
	defer close(done)
	const cron = "* * * * * *"
	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelDebug),
	)
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
}

func TestMultipleCronNodes(t *testing.T) {
	jobsProcessed := sync.Map{}
	const cron = "* * * * * *"
	connString, _ := prepareAndCleanupDB(t)

	workers := make([]neoq.Neoq, ConcurrentWorkers)
	var jobsCompleted uint32
	var duplicateJobs uint32
	for i := 0; i < ConcurrentWorkers; i++ {
		ctx := context.TODO()
		nq, err := neoq.New(ctx,
			neoq.WithBackend(sqlite.Backend),
			sqlite.WithConnectionString(connString),
			neoq.WithLogLevel(logging.LogLevelDebug),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			nq.Shutdown(ctx)
		})
		h := handler.NewPeriodic(func(ctx context.Context) (err error) {
			job, err := jobs.FromContext(ctx)
			if err != nil {
				t.Fatalf("failed to extract job details from context: %+v", err)
				return nil
			}
			_, exists := jobsProcessed.LoadOrStore(job.ID, "foo")
			if exists {
				t.Fatalf("job (%d) has already been processed by another worker!", job.ID)
			}
			atomic.AddUint32(&jobsCompleted, 1)
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

		workers[i] = nq
	}

	const WaitForJobTime = 1100 * time.Millisecond

	// allow time for listener to start and for at least one job to process
	time.Sleep(WaitForJobTime)
	if atomic.LoadUint32(&jobsCompleted) == 0 {
		t.Fatalf("no jobs were completed after %v", WaitForJobTime)
	}

	if atomic.LoadUint32(&duplicateJobs) > 0 {
		t.Fatalf("some jobs were processed more than once")
	}
}

// TestBasicJobProcessingWithErrors tests that the sqlite backend is able to update the status of jobs that fail
func TestBasicJobProcessingWithErrors(t *testing.T) {
	const queue = "testing"
	logsChan := make(chan string, 100)
	timeoutTimer := time.After(5 * time.Second)
	connString, _ := prepareAndCleanupDB(t)

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		err = errors.New("something bad happened") // nolint: goerr113
		return
	})

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

	if err != nil && err != jobs.ErrJobTimeout {
		t.Error(err)
	}
}

// TestFutureJobProcessing tests that the sqlite backend is able to schedule and run future jobs
func TestFutureJobProcessing(t *testing.T) {
	connString, _ := prepareAndCleanupDB(t)
	const queue = "testing"
	done := make(chan bool)
	defer close(done)

	timeoutTimer := time.After(5 * time.Second)

	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelError))
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
	job := &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		RunAfter: time.Now().Add(time.Second * 1),
	}
	jid, e := nq.Enqueue(ctx, job)
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

	if time.Now().Before(job.RunAfter) {
		t.Error("job ran before RunAfter")
	}
}

// TestMoveJobsToDeadQueue tests that when a job's MaxRetries is reached, the job is moved to the dead queue successfully
func TestMoveJobsToDeadQueue(t *testing.T) {
	connString, db := prepareAndCleanupDB(t)
	const queue = "testing"
	maxRetries := 0
	done := make(chan bool)
	defer close(done)

	timeoutTimer := time.After(5 * time.Second)

	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		panic("no good")
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		MaxRetries: &maxRetries,
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

	// ensure job has fields set correctly
	maxWait := time.Now().Add(10 * time.Second)
	var status string
	for {
		if time.Now().After(maxWait) {
			break
		}

		err = db.
			QueryRowContext(context.Background(), "SELECT status FROM neoq_dead_jobs WHERE id = $1", jid).
			Scan(&status)

		if err == nil {
			break
		}

		noRowsErr := "no rows in result set"
		if err != nil && strings.Contains(err.Error(), noRowsErr) {
			time.Sleep(50 * time.Millisecond)
			continue
		} else if err != nil {
			t.Error(err)
		}
	}

	if status != internal.JobStatusFailed {
		t.Error("should be dead")
	}
}

// TestBasicJobMultipleQueueWithError tests that the sqlite backend is able to process jobs on multiple queues and retries occur
func TestBasicJobMultipleQueueWithError(t *testing.T) {
	connString, db := prepareAndCleanupDB(t)
	const queue = "testing"
	const queue2 = "testing2"

	ctx := context.TODO()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		return
	})

	h2 := handler.New(queue2, func(_ context.Context) (err error) {
		panic("no good")
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	err = nq.Start(ctx, h2)
	if err != nil {
		t.Error(err)
	}

	job2Chan := make(chan string, 100)
	go func() {
		jid, err := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("should not fail: %d", internal.RandInt(10000000000)),
			},
		})
		if err != nil || jid == jobs.DuplicateJobID {
			t.Error(err)
		}

		maxRetries := 1
		jid2, err := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue2,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("should fail: %d", internal.RandInt(10000000000)),
			},
			MaxRetries: &maxRetries,
		})
		if err != nil || jid2 == jobs.DuplicateJobID {
			t.Error(err)
		}

		job2Chan <- jid2
	}()

	// wait for the job to process before waiting for updates
	jid2 := <-job2Chan

	// ensure job has fields set correctly
	maxWait := time.Now().Add(30 * time.Second)
	var status string
	for {
		if time.Now().After(maxWait) {
			break
		}

		err = db.
			QueryRowContext(context.Background(), "SELECT status FROM neoq_dead_jobs WHERE id = $1", jid2).
			Scan(&status)

		if err == nil {
			break
		}

		noRowsErr := "no rows in result set"
		// jid2 is empty until the job gets queued
		if jid2 == "" || err != nil && strings.Contains(err.Error(), noRowsErr) {
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			t.Error(err)
		}
	}

	if status != internal.JobStatusFailed {
		t.Error("should be dead")
	}
}

// TestJobWithPastDeadline ensures that when a job is scheduled and its deadline is in the past, that the job is updated
// with an error indicating that its deadline was not met
func TestJobWithPastDeadline(t *testing.T) {
	connString, db := prepareAndCleanupDB(t)
	const queue = "testing"
	timeoutTimer := time.After(5 * time.Second)
	maxRetries := 5
	done := make(chan bool)
	defer close(done)

	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.WithBackend(sqlite.Backend),
		sqlite.WithConnectionString(connString),
		neoq.WithLogLevel(logging.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(_ context.Context) (err error) {
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	// deadline in the past
	deadline := time.Now().UTC().Add(time.Duration(-5) * time.Second)
	jid, e := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		Deadline:   &deadline,
		MaxRetries: &maxRetries,
	})
	if e != nil || jid == jobs.DuplicateJobID {
		t.Error(e) // nolint: goerr113
	}

	if e != nil && !errors.Is(e, jobs.ErrJobExceededDeadline) {
		t.Error(err) // nolint: goerr113
	}

	var status string
	go func() {
		// ensure job has failed/has the correct status
		for {
			err = db.
				QueryRowContext(context.Background(), "SELECT status FROM neoq_jobs WHERE id = $1", jid).
				Scan(&status)
			if err != nil {
				break
			}

			if status == internal.JobStatusFailed {
				done <- true
				break
			}

			time.Sleep(50 * time.Millisecond)
		}
	}()

	select {
	case <-timeoutTimer:
		err = jobs.ErrJobTimeout
	case <-done:
	}
	if err != nil {
		t.Errorf("job should have resulted in a status of 'failed', but its status is %s", status)
	}
}
