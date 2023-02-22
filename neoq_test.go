package neoq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestWorkerListenConn(t *testing.T) {
	const queue = "foobar"
	timeout := false
	jobRan := false
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
		var j *Job
		j, err = JobFromContext(ctx)
		log.Println("queue:", j.Queue, "got job", "id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	})
	handler = handler.
		WithOption(HandlerDeadline(500 * time.Millisecond)).
		WithOption(HandlerConcurrency(1))

	if err != nil {
		t.Error(err)
	}

	// Listen for jobs on the queue
	nq.Listen(ctx, queue, handler)

	// allow time for listener to start
	time.Sleep(50 * time.Millisecond)

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
			jobRan = true
			break
		}

		if timeout {
			break
		}
	}

	// Allow time for job status to be updated in the database
	time.Sleep(50 * time.Millisecond)

	if !jobRan {
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

	jobRan := false
	var done = make(chan bool)
	handler := NewHandler(func(ctx context.Context) (err error) {
		log.Println("got periodic job")
		done <- true
		return
	})

	handler = handler.
		WithOption(HandlerDeadline(500 * time.Millisecond)).
		WithOption(HandlerConcurrency(1))

	if err != nil {
		t.Error(err)
	}

	nq.ListenCron(ctx, cron, handler)

	// allow time for listener to start
	time.Sleep(50 * time.Millisecond)

	select {
	case <-time.After(5 * time.Second):
		err = errors.New("timed out waiting for periodic job")
	case <-done:
		jobRan = true
	}

	// Allow time for job status to be updated in the database
	time.Sleep(50 * time.Millisecond)

	if !jobRan {
		t.Error(err)
	}
}
