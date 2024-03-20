package main

import (
	"context"
	"log"

	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/backends/postgres"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/jobs"
)

func main() {
	done := make(chan bool, 1)
	const queue = "foobar"
	ctx := context.Background()
	nq, err := neoq.New(ctx,
		postgres.WithConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq?sslmode=disable"),
		neoq.WithBackend(postgres.Backend),
	)
	if err != nil {
		log.Fatalf("error initializing postgres backend: %v", err)
	}
	defer nq.Shutdown(ctx)

	h := handler.New(queue, func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = jobs.FromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		log.Println("error listening to queue", err)
	}

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
