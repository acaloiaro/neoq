package main

import (
	"context"
	"log"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	nctx "github.com/acaloiaro/neoq/context"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
)

func main() {
	var err error
	const queue = "foobar"
	ctx := context.Background()

	backend, err := postgres.NewPgBackend(ctx, "postgres://postgres:postgres@127.0.0.1:5432/neoq")
	if err != nil {
		log.Fatalf("error initializing postgres backend: %v", err)
	}

	nq, err := neoq.New(ctx, neoq.WithBackend(backend))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	h := handler.New(func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = nctx.JobFromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		return
	})

	err = nq.Listen(ctx, queue, h)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	// this code will exit quickly since Listen() is asynchronous
	// real applications should call Listen() on startup for every queue that needs to be handled
}
