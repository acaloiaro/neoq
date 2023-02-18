package main

import (
	"context"
	"log"
	"time"

	"github.com/acaloiaro/neoq"
)

func main() {
	ctx := context.Background()
	// by default neoq connects to a local postgres server using: [neoq.DefaultPgConnectionString]
	// connection strings can be set explicitly as follows:
	// neoq.New(neoq.ConnectionString("postgres://username:passsword@hostname/database"))
	nq, err := neoq.New(ctx, neoq.PgTransactionTimeoutOpt(1000))
	if err != nil {
		log.Fatalf("error initializing neoq: %v", err)
	}

	// run a job periodically
	handler := neoq.NewHandler(func(ctx context.Context) (err error) {
		log.Println("running periodic job")
		return
	})
	handler = handler.
		WithOption(neoq.HandlerDeadlineOpt(time.Duration(500 * time.Millisecond))).
		WithOption(neoq.HandlerConcurrencyOpt(1))

	nq.ListenCron(ctx, "* * * * * *", handler)

	time.Sleep(5 * time.Second)
}
