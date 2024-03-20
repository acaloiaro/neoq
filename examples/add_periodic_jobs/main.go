package main

import (
	"context"
	"log"
	"time"

	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/backends/memory"
	"github.com/pranavmodx/neoq-sqlite/handler"
)

func main() {
	ctx := context.Background()
	nq, err := neoq.New(ctx, neoq.WithBackend(memory.Backend))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// run a job periodically
	h := handler.NewPeriodic(func(_ context.Context) (err error) {
		log.Println("running periodic job")
		return
	})
	h.WithOptions(
		handler.JobTimeout(500*time.Millisecond),
		handler.Concurrency(1),
	)

	nq.StartCron(ctx, "* * * * * *", h)

	time.Sleep(5 * time.Second)
}
