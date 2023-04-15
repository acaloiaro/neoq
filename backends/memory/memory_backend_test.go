package memory_test

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/memory"
	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/testutils"
	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

const (
	queue = "testing"
)

var errPeriodicTimeout = errors.New("timed out waiting for periodic job")

// TestBasicJobProcessing tests that the memory backend is able to process the most basic jobs with the
// most basic configuration.
func TestBasicJobProcessing(t *testing.T) {
	numJobs := 1000
	doneCnt := 0
	done := make(chan bool)
	var timeoutTimer = time.After(5 * time.Second)

	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}

	h := handler.New(func(_ context.Context) (err error) {
		done <- true
		return
	})

	if err := nq.Start(ctx, queue, h); err != nil {
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

	h := handler.New(func(_ context.Context) (err error) {
		time.Sleep(100 * time.Millisecond)
		return
	}, handler.Concurrency(1), handler.MaxQueueCapacity(1))

	if err := nq.Start(ctx, queue, h); err != nil {
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
	testLogger := testutils.TestLogger{L: log.New(&strings.Builder{}, "", 0), Done: make(chan bool)}
	testBackend := memory.TestingBackend(config.New(), cron.New(), &sync.Map{}, &sync.Map{}, testFutureJobs, &sync.Map{}, testLogger)

	nq, err := neoq.New(ctx, neoq.WithBackend(testBackend))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(func(ctx context.Context) (err error) {
		return
	})

	if err := nq.Start(ctx, queue, h); err != nil {
		t.Fatal(err)
	}

	jid, err := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello world",
		},
		RunAfter: time.Now().Add(5 * time.Second),
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

func TestCron(t *testing.T) {
	const cronSpec = "* * * * * *"
	ctx := context.TODO()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		t.Fatal(err)
	}
	defer nq.Shutdown(ctx)

	var done = make(chan bool)
	h := handler.New(func(ctx context.Context) (err error) {
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
