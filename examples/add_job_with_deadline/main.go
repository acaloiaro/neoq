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
	ctx := context.Background()

	// by default neoq connects to a local postgres server using: [neoq.DefaultPgConnectionString]
	// connection strings can be set explicitly as follows:
	// neoq.New(neoq.ConnectionString("postgres://username:passsword@hostname/database"))
	nq, err := neoq.New(ctx)
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

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
	handler.WithOptions(neoq.HandlerDeadline(10 * time.Millisecond))

	err = nq.Listen(ctx, queue, handler)
	if err != nil {
		log.Println("error listening to queue", err)
	}

	_, err = nq.Enqueue(ctx, neoq.Job{
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
