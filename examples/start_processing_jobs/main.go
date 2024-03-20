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
	var err error
	const queue = "foobar"
	ctx := context.Background()

	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), postgres.WithConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq"))
	if err != nil {
		log.Fatalf("error initializing postgres backend: %v", err)
	}

	h := handler.New(queue, func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = jobs.FromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		return
	})

	err = nq.Start(ctx, h)
	if err != nil {
		log.Println("error processing queue", err)
	}

	// this code will exit quickly since Start() is asynchronous
	// real applications should call Start() on startup for every queue that needs to be handled
}
