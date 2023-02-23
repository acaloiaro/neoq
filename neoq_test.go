package neoq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestWorkerListenConn(t *testing.T) {
	const queue = "testing"
	timeout := false
	complete := false
	numJobs := 1
	doneCnt := 0
	var done = make(chan bool, numJobs)

	ctx := context.TODO()
	backend, err := NewMemBackend()
	if err != nil {
		t.Fatal(err)
	}

	nq, err := New(ctx, Backend(backend))
	if err != nil {
		t.Fatal(err)
	}

	handler := NewHandler(func(ctx context.Context) (err error) {
		done <- true
		return
	})
	handler.WithOptions(
		HandlerDeadline(500*time.Millisecond),
		HandlerConcurrency(1),
	)

	if err != nil {
		t.Error(err)
	}

	// Listen for jobs on the queue
	nq.Listen(ctx, queue, handler)

	// allow time for listener to start
	time.Sleep(5 * time.Millisecond)

	for i := 0; i < numJobs; i++ {
		jid, err := nq.Enqueue(ctx, Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("hello world: %d", i),
			},
		})
		if err != nil || jid == -1 {
			t.Fatal("job was not enqueued. either it was duplicate or this error caused it:", err)
		}
	}

	for {
		select {
		case <-time.After(5 * time.Second):
			timeout = true
			err = errors.New("timed out waiting for job")
		case <-done:
			doneCnt++
		}

		if doneCnt >= numJobs {
			complete = true
			break
		}

		if timeout {
			break
		}
	}

	if !complete {
		t.Error(err)
	}

}

func TestWorkerListenCron(t *testing.T) {
	const cron = "* * * * * *"
	ctx := context.TODO()
	nq, err := New(ctx)
	if err != nil {
		t.Fatal(err)
	}

	complete := false
	var done = make(chan bool)
	handler := NewHandler(func(ctx context.Context) (err error) {
		done <- true
		return
	})

	handler.WithOptions(
		HandlerDeadline(500*time.Millisecond),
		HandlerConcurrency(1),
	)

	if err != nil {
		t.Error(err)
	}

	nq.ListenCron(ctx, cron, handler)

	// allow time for listener to start
	time.Sleep(5 * time.Millisecond)

	select {
	case <-time.After(5 * time.Second):
		err = errors.New("timed out waiting for periodic job")
	case <-done:
		complete = true
	}

	if !complete {
		t.Error(err)
	}
}
