package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
)

func main() {
	const queue = "foobar"
	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.PgTransactionTimeout(1000))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// Add a job that will execute 1 hour from now
	jobID, err := nq.Enqueue(ctx, neoq.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello, future world",
		},
		RunAfter: time.Now().Add(1 * time.Hour),
	})
	if err != nil {
		log.Fatalf("error adding job: %v", err)
	}

	log.Println("added job:", jobID)
}
