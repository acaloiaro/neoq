package main

import (
	"context"
	"log"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
)

func main() {
	var err error
	const queue = "foobar"
	ctx := context.Background()

	nq, err := neoq.New(ctx, neoq.WithBackend(postgres.Backend), config.WithConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq"))
	if err != nil {
		log.Fatalf("error initializing postgres backend: %v", err)
	}

	h := handler.New(func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = jobs.FromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		return
	})

	err = nq.Start(ctx, queue, h)
	if err != nil {
		log.Println("error processing queue", err)
	}

	// this code will exit quickly since Start() is asynchronous
	// real applications should call Start() on startup for every queue that needs to be handled
}
