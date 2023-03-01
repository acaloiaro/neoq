package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/jobs"
)

func main() {
	const queue = "foobar"
	ctx := context.Background()
	nq, err := neoq.New(ctx,
		config.WithConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq"),
		neoq.WithBackend(postgres.Backend),
		postgres.WithTransactionTimeout(1000), // nolint: mnd, gomnd
	)
	if err != nil {
		log.Fatalf("error initializing postgres backend: %v", err)
	}

	// Add a job that will execute 1 hour from now
	jobID, err := nq.Enqueue(ctx, &jobs.Job{
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
