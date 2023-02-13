package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
)

func main() {
	var err error
	const queue = "foobar"
	//
	nq, _ := neoq.New(
		"postgres://postgres:postgres@127.0.0.1:5432/neoq?sslmode=disable",
		neoq.TransactionTimeoutOpt(1000), // transactions may be idle up to one second
	)

	// we use a done channel here to make sure that our test doesn't exit before the job finishes running
	// this is probably not a pattern you want to use in production jobs and you see it here only for testing reasons
	done := make(chan bool)

	handler := neoq.NewHandler(func(ctx context.Context) (err error) {
		var j *neoq.Job
		time.Sleep(1 * time.Second)
		j, err = neoq.JobFromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	})

	// this 10ms deadline will cause our job that sleeps for 1s to fail
	handler = handler.WithOption(neoq.WithDeadline(10 * time.Millisecond))

	err = nq.Listen(queue, handler)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	// Add a job that will execute 1 hour from now
	_, err = nq.Enqueue(neoq.Job{
		Queue: queue,
		Payload: map[string]interface{}{
			"message": "hello, world",
		},
	})
	if err != nil {
		log.Println("error adding job", err)
	}

	<-done

	// job's status will be 'failed' and 'error' will be 'job exceeded its 10ms deadline'
	// until either the job's Sleep statement is decreased/removed or the handler's deadline is increased
	// this job will continue to to fail and ultimately land on the dead jobs queue
}
