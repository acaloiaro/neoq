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

	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// we use a done channel here to make sure that our test doesn't exit before the job finishes running
	// this is probably not a pattern you want to use in production jobs and you see it here only for testing reasons
	done := make(chan bool)

	h := handler.New(queue, func(_ context.Context) (err error) {
		<-done
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	// this job must complete before 5 seconds from now
	deadline := time.Now().Add(5 * time.Second)
	_, err = nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello, world",
		},
		Deadline: &deadline,
	})
	if err != nil {
		log.Println("error adding job", err)
	}

	<-done
}
