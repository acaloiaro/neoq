package neoq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/acaloiaro/neoq/backends/memory"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/testutils"
)

var (
	errTrigger         = errors.New("triggerering a log error")
	errPeriodicTimeout = errors.New("timed out waiting for periodic job")
)

func ExampleNew() {
	ctx := context.Background()
	nq, err := New(ctx, WithBackend(memory.Backend))
	if err != nil {
		fmt.Println("initializing a new Neoq with no params should not return an error:", err)
		return
	}
	defer nq.Shutdown(ctx)

	fmt.Println("neoq initialized with default memory backend")
	// Output: neoq initialized with default memory backend
}

func ExampleNew_postgres() {
	ctx := context.Background()
	var pgURL string
	var ok bool
	if pgURL, ok = os.LookupEnv("TEST_DATABASE_URL"); !ok {
		fmt.Println("Please set TEST_DATABASE_URL environment variable")
		return
	}

	nq, err := New(ctx, WithBackend(postgres.Backend), postgres.WithConnectionString(pgURL))
	if err != nil {
		fmt.Println("neoq's postgres backend failed to initialize:", err)
		return
	}
	defer nq.Shutdown(ctx)

	fmt.Println("neoq initialized with postgres backend")
	// Output: neoq initialized with postgres backend
}

func ExampleWithBackend() {
	ctx := context.Background()
	nq, err := New(ctx, WithBackend(memory.Backend))
	if err != nil {
		fmt.Println("initializing a new Neoq with no params should not return an error:", err)
		return
	}
	defer nq.Shutdown(ctx)

	fmt.Println("neoq initialized with memory backend")
	// Output: neoq initialized with memory backend
}

func ExampleWithBackend_postgres() {
	ctx := context.Background()
	var pgURL string
	var ok bool
	if pgURL, ok = os.LookupEnv("TEST_DATABASE_URL"); !ok {
		fmt.Println("Please set TEST_DATABASE_URL environment variable")
		return
	}

	nq, err := New(ctx, WithBackend(postgres.Backend), postgres.WithConnectionString(pgURL))
	if err != nil {
		fmt.Println("initializing a new Neoq with no params should not return an error:", err)
		return
	}
	defer nq.Shutdown(ctx)

	fmt.Println("neoq initialized with postgres backend")
	// Output: neoq initialized with postgres backend
}

func TestStart(t *testing.T) {
	const queue = "testing"
	timeout := false
	numJobs := 1
	doneCnt := 0
	done := make(chan bool, numJobs)

	ctx := context.TODO()
	nq, err := New(ctx, WithBackend(memory.Backend))
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

	// process jobs on the test queue
	err = nq.Start(ctx, queue, h)
	if err != nil {
		t.Error(err)
	}

	// allow time for processor to start
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < numJobs; i++ {
		jid, err := nq.Enqueue(ctx, &jobs.Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("hello world: %d", i),
			},
		})
		if err != nil || jid == jobs.DuplicateJobID {
			t.Fatal("job was not enqueued. either it was duplicate or this error caused it:", err)
		}
	}

	for {
		select {
		case <-time.After(5 * time.Second):
			timeout = true
			err = errors.New("timed out waiting for job") // nolint: goerr113
		case <-done:
			doneCnt++
		}

		if doneCnt >= numJobs {
			break
		}

		if timeout {
			break
		}
	}

	if timeout {
		t.Error(err)
	}
}

func TestStartCron(t *testing.T) {
	const cron = "* * * * * *"
	ctx := context.TODO()
	nq, err := New(ctx, WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	done := make(chan bool)
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
}

func TestSetLogger(t *testing.T) {
	timeoutTimer := time.After(5 * time.Second)
	const queue = "testing"
	logsChan := make(chan string, 10)
	ctx := context.Background()

	nq, err := New(ctx, WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	nq.SetLogger(testutils.NewTestLogger(logsChan))

	h := handler.New(func(ctx context.Context) (err error) {
		err = errTrigger
		return
	})
	if err != nil {
		t.Error(err)
	}
	err = nq.Start(ctx, queue, h)
	if err != nil {
		t.Error(err)
	}
	_, err = nq.Enqueue(ctx, &jobs.Job{Queue: queue})
	if err != nil {
		t.Error(err)
	}

	expectedLogMsg := "adding a new job [queue testing]"
results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case actualLogMsg := <-logsChan:
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
}
