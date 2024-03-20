package memory_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/backends/memory"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/jobs"
	"github.com/pranavmodx/neoq-sqlite/logging"
	"github.com/pranavmodx/neoq-sqlite/testutils"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

const (
	queue = "testing"
)

var (
	errPeriodicTimeout = errors.New("timed out waiting for periodic job")
	errTimeout         = errors.New("the test has timed out")
)

func ExampleNew() {
	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		fmt.Println("initializing a new Neoq with no params should not return an error:", err)
		return
	}
	defer nq.Shutdown(ctx)

	fmt.Println("neoq initialized with default memory backend")
	// Output: neoq initialized with default memory backend
}

// TestBasicJobProcessing tests that the memory backend is able to process the most basic jobs with the
// most basic configuration.
func TestBasicJobProcessing(t *testing.T) {
	numJobs := 1000
	doneCnt := 0
	done := make(chan bool)
	timeoutTimer := time.After(5 * time.Second)

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	if err := nq.Start(ctx, h); err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := 0; i < numJobs; i++ {
			jid, e := nq.Enqueue(ctx, &jobs.Job{
				Queue: queue,
				Payload: map[string]interface{}{
					"message": fmt.Sprintf("hello world: %d", i),
				},
			})
			if e != nil || jid == jobs.DuplicateJobID {
				slog.Error("job was not enqueued. either it was duplicate or this error caused it:", e)
			}
		}
	}()

	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
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

// TestBackendConfiguration tests that the memory backend receives and utilizes the MaxQueueCapacity handler
// configuration.
//
// This test works by enqueueing 3 jobs. Each job sleeps for longer than the test is willing to wait for the jobs to
// enqueue. The `done` channel is notified when all 3 jobs are enqueued.
//
// By serializing handler execution by initializing neoq with `handler.Concurrency(1)` and enqueueing jobs
// asynchronously, we can wait on `done` and a timeout channel to see which one completes first.
//
// Since the queue has a capacity of 1 and the handler is serialized, we know that `done` cannot be notified until job 1
// is complete, job 2 is processing, and job 3 can be added to the queue.
//
// If `done` is notified before the timeout channel, this test would fail, because that would mean Enqueue() is not
// blocking while the first Sleep()ing job is running. If the qeueue is blocking when it meets its capacity, we know
// that the max queue capacity configuration has taken effect.
func TestBackendConfiguration(t *testing.T) {
	numJobs := 3
	timeout := false

	done := make(chan bool)

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}

	h := handler.New(queue, func(_ context.Context) (err error) {
		time.Sleep(100 * time.Millisecond)
		return
	}, handler.Concurrency(1), handler.MaxQueueCapacity(1))

	if err := nq.Start(ctx, h); err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := 0; i < numJobs; i++ {
			jid, e := nq.Enqueue(ctx, &jobs.Job{
				Queue: queue,
				Payload: map[string]interface{}{
					"message": fmt.Sprintf("hello world: %d", i),
				},
			})
			if e != nil || jid == jobs.DuplicateJobID {
				slog.Error("job was not enqueued. either it was duplicate or this error caused it:", e)
			}
		}

		done <- true
	}()

	select {
	case <-time.After(time.Duration(50) * time.Millisecond):
		timeout = true
	case <-done:
		err = errors.New("this job should have timed out")
	}

	if !timeout {
		t.Error(err)
	}

	nq.Shutdown(ctx)
}

var testFutureJobs = &sync.Map{}

func TestFutureJobScheduling(t *testing.T) {
	ctx := context.Background()
	testBackend := memory.TestingBackend(
		neoq.NewConfig(),
		cron.New(),
		&sync.Map{},
		&sync.Map{},
		testFutureJobs,
		&sync.Map{},
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logging.LogLevelDebug})))

	nq, err := neoq.New(ctx, neoq.WithBackend(testBackend))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(ctx context.Context) (err error) {
		return
	})

	if err := nq.Start(ctx, h); err != nil {
		t.Fatal(err)
	}

	jid, err := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		RunAfter: time.Now().UTC().Add(5 * time.Second),
	})
	if err != nil || jid == jobs.DuplicateJobID {
		err = fmt.Errorf("job was not enqueued. either it was duplicate or this error caused it: %w", err)
		t.Error(err)
	}

	jobID, _ := strconv.ParseInt(jid, 0, 64)
	var ok bool
	if _, ok = testFutureJobs.Load(jobID); !ok {
		t.Error(err)
	}
}

// This test was added in response to the following issue: https://github.com/acaloiaro/neoq/issues/56
// nolint: gocognit, gocyclo
func TestFutureJobSchedulingMultipleQueues(t *testing.T) {
	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	jobsPerQueueCount := 100

	jobsProcessed1 := 0
	jobsProcessed2 := 0

	q1 := "queue1"
	q2 := "queue2"

	done1 := make(chan bool)
	done2 := make(chan bool)

	h1 := handler.New(q1, func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = jobs.FromContext(ctx)
		if err != nil {
			return
		}

		if j.Queue != q1 {
			err = errors.New("handling job from queu2 with queue1 handler. this is a bug")
		}

		done1 <- true
		return
	})

	h2 := handler.New(q2, func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = jobs.FromContext(ctx)
		if err != nil {
			return
		}

		if j.Queue != q2 {
			err = errors.New("handling job from queue1 with queue2 handler. this is a bug")
		}

		done2 <- true
		return
	})

	if err := nq.Start(ctx, h1); err != nil {
		t.Fatal(err)
	}

	if err := nq.Start(ctx, h2); err != nil {
		t.Fatal(err)
	}

	go func() {
		for i := 1; i <= jobsPerQueueCount; i++ {
			_, err = nq.Enqueue(ctx, &jobs.Job{
				ID:       int64(i),
				Queue:    q1,
				Payload:  map[string]interface{}{"ID": i},
				RunAfter: time.Now().Add(1 * time.Millisecond),
			})
			if err != nil {
				err = fmt.Errorf("job was not enqueued. either it was duplicate or this error caused it: %w", err)
				t.Error(err)
			}
		}

		for j := 1; j <= jobsPerQueueCount; j++ {
			_, err = nq.Enqueue(ctx, &jobs.Job{
				ID:       int64(j),
				Queue:    q2,
				Payload:  map[string]interface{}{"ID": j},
				RunAfter: time.Now().Add(1 * time.Millisecond),
			})
			if err != nil {
				err = fmt.Errorf("job was not enqueued. either it was duplicate or this error caused it: %w", err)
				t.Error(err)
			}
		}
	}()

	timeoutTimer := time.After(10 * time.Second)
results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case <-done1:
			jobsProcessed1++
		case <-done2:
			jobsProcessed2++
		default:
			if jobsProcessed1 == jobsPerQueueCount && jobsProcessed2 == jobsPerQueueCount {
				break results_loop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if err != nil {
		t.Error(err)
	}
	if jobsProcessed1 != jobsPerQueueCount {
		// nolint: goerr113
		t.Error(fmt.Errorf("handler1 should have handled %d jobs, but handled %d", jobsPerQueueCount, jobsProcessed1))
	}
	if jobsProcessed2 != jobsPerQueueCount {
		// nolint: goerr113
		t.Error(fmt.Errorf("handler2 should have handled %d jobs, but handled %d", jobsPerQueueCount, jobsProcessed2))
	}
}

func TestCron(t *testing.T) {
	const cronSpec = "* * * * * *"
	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	done := make(chan bool)
	h := handler.New("foobar", func(ctx context.Context) (err error) {
		done <- true
		return
	})

	h.WithOptions(
		handler.JobTimeout(500*time.Millisecond),
		handler.Concurrency(1),
	)

	err = nq.StartCron(ctx, cronSpec, h)
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

func TestMaxRetries(t *testing.T) {
	const queue = "foobar"
	maxRetries := 0

	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend), neoq.WithLogLevel(logging.LogLevelDebug))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	logChan := make(chan string, 1)
	nq.SetLogger(testutils.NewTestLogger(logChan))

	h := handler.New(queue, func(ctx context.Context) (err error) {
		panic("i refuse success!")
	})

	err = nq.Start(ctx, h)
	if err != nil {
		t.Error(err)
	}

	// allow time for listener to start
	time.Sleep(5 * time.Millisecond)

	_, err = nq.Enqueue(ctx, &jobs.Job{
		Queue:      queue,
		Payload:    map[string]any{},
		MaxRetries: &maxRetries,
	})
	if err != nil {
		t.Error(err)
	}

	timeoutTimer := time.After(5 * time.Second)

result_loop:
	for {
		select {
		case <-timeoutTimer:
			err = errTimeout
			break result_loop
		case newLog := <-logChan:
			if strings.Contains(newLog, "job exceeded max retries") {
				break result_loop
			}
		}
	}

	if err != nil {
		t.Error(err)
	}
}
