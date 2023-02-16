package neoq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"golang.org/x/exp/slog"
)

func TestWorkerListenConn(t *testing.T) {
	const queue = "foobar"
	nq, err := New("postgres://postgres:postgres@127.0.0.1:5432/neoq?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	jobRan := false
	numJobs := 1
	var done = make(chan bool, numJobs)
	handler := NewHandler(func(ctx context.Context) (err error) {
		var j *Job
		j, err = JobFromContext(ctx)
		slog.Info("got job", "id", j.ID, "messsage", j.Payload["message"])
		done <- true
		return
	})
	handler = handler.
		WithOption(WithDeadline(time.Duration(200 * time.Millisecond))).
		WithOption(WithConcurrency(8))

	if err != nil {
		t.Error(err)
	}

	// Listen for jobs on the queue
	nq.Listen(queue, handler)

	time.Sleep(250 * time.Millisecond)

	for i := 0; i < numJobs; i++ {
		jid, err := nq.Enqueue(Job{
			Queue: queue,
			Payload: map[string]interface{}{
				"message": fmt.Sprintf("hello world: %d", i),
			},
		})
		if err != nil || jid == -1 {
			t.Fatal("job was not enqueued. either it was duplicate or this error caused it:", err)
		}
	}

	timeout := false
	doneCnt := 0
	for {
		select {
		case <-time.After(5 * time.Second):
			timeout = true
			err = errors.New("timed out waiting for job")
			break
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
