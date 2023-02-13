package main

import (
	"log"
	"time"

	"github.com/acaloiaro/neoq"
)

func main() {
	const queue = "foobar"
	nq, _ := neoq.New("postgres://postgres:postgres@127.0.0.1:5432/neoq?sslmode=disable", neoq.TransactionTimeoutOpt(1000))

	// Add a job that will execute 1 hour from now
	jobID, err := nq.Enqueue(neoq.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello, future world",
		},
		RunAfter: time.Now().Add(1 * time.Hour),
	})
	if err != nil {
		log.Println("error adding job", err)
	}

	log.Println("added job:", jobID)
}
