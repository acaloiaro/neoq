package sqlite_test

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/backends/sqlite"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/internal"
	"github.com/pranavmodx/neoq-sqlite/jobs"
	"github.com/pranavmodx/neoq-sqlite/logging"
)

//go:embed migrations/*.sql
var sqliteMigrationsFS embed.FS

var errPeriodicTimeout = errors.New("timed out waiting for periodic job")

func prepareAndCleanupDB(t *testing.T) (dbURL string, db *sql.DB) {
	t.Helper()

	migrations, err := iofs.New(sqliteMigrationsFS, "migrations")
	if err != nil {
		t.Fatalf("unable to run migrations, error during iofs new: %s", err.Error())
	}

	cwd, _ := os.Getwd()
	dbURL = "sqlite3://" + cwd + "/test.db"
	dbPath := cwd + "/test.db"

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
	nq, err := neoq.New(ctx, neoq.WithBackend(sqlite.Backend), sqlite.WithConnectionString(connString))
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
	// var jdl time.Time
	// var jmxrt int
	var jqueue string

	err = db.
		// QueryRow("SELECT deadline,max_retries FROM neoq_jobs WHERE id = $1", jid).
		// Scan(&jdl, &jmxrt)
		QueryRow("SELECT queue FROM neoq_jobs WHERE id = $1", jid).
		Scan(&jqueue)
	if err != nil {
		t.Error(err)
	}

	// jdl = jdl.In(time.UTC)
	// // dates from postgres come out with only 6 decimal places of millisecond precision, naively format dates as
	// // strings for comparison reasons. Ref https://www.postgresql.org/docs/current/datatype-datetime.html
	// if jdl.Format(time.RFC3339) != deadline.Format(time.RFC3339) {
	// 	t.Error(fmt.Errorf("job deadline does not match its expected value: %v != %v", jdl, deadline)) // nolint: goerr113
	// }

	// if jmxrt != maxRetries {
	// 	t.Error(fmt.Errorf("job MaxRetries does not match its expected value: %v != %v", jmxrt, maxRetries)) // nolint: goerr113
	// }

	if jqueue != queue {
		t.Error(fmt.Errorf("job queue does not match its expected value: %v != %v", jqueue, queue))
	}
}

// TestBasicJobMultipleQueue tests that the postgres backend is able to process jobs on multiple queues
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
	nq, err := neoq.New(ctx, neoq.WithBackend(sqlite.Backend), sqlite.WithConnectionString(connString))
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
