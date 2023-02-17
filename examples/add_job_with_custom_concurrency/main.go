package main

import (
	"context"
	"log"

	"github.com/acaloiaro/neoq"
)

func main() {
	var err error
	const queue = "foobar"
	//
	nq, _ := neoq.New("postgres://postgres:postgres@127.0.0.1:5432/neoq?sslmode=disable")

	// we use a done channel here to make sure that our test doesn't exit before the job finishes running
	// this is probably not a pattern you want to use in production jobs and you see it here only for testing reasons
	done := make(chan bool)

	// Concurrency and other options may be set on handlers both during creation (Option 1), or after the fact (Option 2)
	// Option 1: add options when creating the handler
	handler := neoq.NewHandler(func(ctx context.Context) (err error) {
		var j *neoq.Job
		j, err = neoq.JobFromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	}, neoq.WithConcurrency(8))

	// Option 2: Set options after the handler is created
	handler = handler.WithOption(neoq.WithConcurrency(8))

	err = nq.Listen(queue, handler)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	// enqueue a job
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
