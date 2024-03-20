package main

import (
	"context"
	"log"

	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/backends/memory"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/jobs"
)

func main() {
	var err error
	const queue = "foobar"
	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// we use a done channel here to make sure that our test doesn't exit before the job finishes running
	// this is probably not a pattern you want to use in production jobs and you see it here only for testing reasons
	done := make(chan bool)

	// Concurrency and other options may be set on handlers both during creation (Option 1), or after the fact (Option 2)
	// Option 1: add options when creating the handler
	h := handler.New(queue, func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = jobs.FromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	}, handler.Concurrency(8))

	// Option 2: Set options after the handler is created
	h.WithOptions(handler.Concurrency(8))

	err = nq.Start(ctx, h)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	_, err = nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello, world",
		},
	})
	if err != nil {
		log.Println("error adding job", err)
	}

	<-done

	// job's status will be 'failed' and 'error' will be 'job exceeded its 10ms timeout'
	// until either the job's Sleep statement is decreased/removed or the handler's timeout is increased
	// this job will continue to fail and ultimately land on the dead jobs queue
}
