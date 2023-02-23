package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
)

func main() {
	ctx := context.Background()
	nq, err := neoq.New(ctx)
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// run a job periodically
	handler := neoq.NewHandler(func(ctx context.Context) (err error) {
		log.Println("running periodic job")
		return
	})
	handler.WithOptions(
		neoq.HandlerDeadline(500*time.Millisecond),
		neoq.HandlerConcurrency(1),
	)

	nq.ListenCron(ctx, "* * * * * *", handler)

	time.Sleep(5 * time.Second)
}
