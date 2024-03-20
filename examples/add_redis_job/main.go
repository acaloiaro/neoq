package main

import (
	"context"
	"log"

	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/backends/redis"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/jobs"
)

func main() {
	done := make(chan bool)
	ctx := context.Background()
	nq, _ := neoq.New(ctx,
		neoq.WithBackend(redis.Backend),
		redis.WithAddr("localhost:6379"),
		redis.WithPassword(""),
	)

	nq.Start(ctx, handler.New("hello_world", func(ctx context.Context) (err error) {
		j, _ := jobs.FromContext(ctx)
		log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
		done <- true
		return
	}))

	nq.Enqueue(ctx, &jobs.Job{
		Queue: "hello_world",
		Payload: map[string]interface{}{
			"message": "hello world",
		},
	})

	<-done
	nq.Shutdown(ctx)
}
