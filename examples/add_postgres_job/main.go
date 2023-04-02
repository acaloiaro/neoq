package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
)

func main() {
	var done = make(chan bool, 1)
	const queue = "foobar"
	ctx := context.Background()
	nq, err := neoq.New(ctx,
		config.WithConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq"),
		neoq.WithBackend(postgres.Backend),
	)
	if err != nil {
		log.Fatalf("error initializing postgres backend: %v", err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(func(ctx context.Context) (err error) {
		var j *jobs.Job
		time.Sleep(1 * time.Second)
		j, err = jobs.FromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	})

	err = nq.Start(ctx, queue, h)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	// Add a job that will execute 1 hour from now
	jobID, err := nq.Enqueue(ctx, &jobs.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello, world",
		},
	})
	if err != nil {
		log.Fatalf("error adding job: %v", err)
	}

	log.Println("added job:", jobID)
	<-done
}
