package main

import (
	"context"
	"log"

	"github.com/acaloiaro/neoq"
)

func main() {
	var err error
	const queue = "foobar"
	ctx := context.Background()
	nq, err := neoq.New(ctx,
		neoq.BackendName("postgres"),
		neoq.ConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq"))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	handler := neoq.NewHandler(func(ctx context.Context) (err error) {
		var j *neoq.Job
		j, err = neoq.JobFromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		return
	})

	err = nq.Listen(ctx, queue, handler)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	// this code will exit quickly since since Listen() is asynchronous
	// real applications should call Listen() on startup for every queue that needs to be handled
}
