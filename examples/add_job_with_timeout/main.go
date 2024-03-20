package main

import (
	"context"
	"log"
	"time"

	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/backends/memory"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/jobs"
)

func main() {
	var err error
	const queue = "foobar"
	ctx := context.Background()

	// by default neoq connects to a local postgres server using: [neoq.DefaultPgConnectionString]
	// connection strings can be set explicitly as follows:
	// neoq.New(neoq.ConnectionString("postgres://username:passsword@hostname/database"))
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// we use a done channel here to make sure that our test doesn't exit before the job finishes running
	// this is probably not a pattern you want to use in production jobs and you see it here only for testing reasons
	done := make(chan bool)

	h := handler.New(queue, func(ctx context.Context) (err error) {
		var j *jobs.Job
		time.Sleep(1 * time.Second)
		j, err = jobs.FromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	})

	// this 10ms timeout will cause our job that sleeps for 1s to fail
	h.WithOptions(handler.JobTimeout(10 * time.Millisecond))

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
