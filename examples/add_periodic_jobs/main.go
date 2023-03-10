package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/memory"
	"github.com/acaloiaro/neoq/handler"
)

func main() {
	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// run a job periodically
	h := handler.New(func(ctx context.Context) (err error) {
		log.Println("running periodic job")
		return
	})
	h.WithOptions(
		handler.Deadline(500*time.Millisecond),
		handler.Concurrency(1),
	)

	nq.StartCron(ctx, "* * * * * *", h)

	time.Sleep(5 * time.Second)
}
