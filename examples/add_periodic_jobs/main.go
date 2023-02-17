package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
)

func main() {
	nq, _ := neoq.New("postgres://postgres:postgres@127.0.0.1:5432/neoq?sslmode=disable", neoq.TransactionTimeoutOpt(1000))
	// run a job periodically
	handler := neoq.NewHandler(func(ctx context.Context) (err error) {
		log.Println("running periodic job")
		return
	})
	handler = handler.
		WithOption(neoq.HandlerDeadlineOpt(time.Duration(500 * time.Millisecond))).
		WithOption(neoq.HandlerConcurrencyOpt(1))

	nq.ListenCron("* * * * * *", handler)

	time.Sleep(5 * time.Second)
}
