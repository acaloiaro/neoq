package tx

import (
	"context"

	"github.com/acaloiaro/neoq"
	"github.com/jackc/pgx/v5"
)

// TxWrapper wraps a standard library sql.Tx in the neoq Tx interface so that sql.Tx
// may be used as a transaction type for enqueueing jobs.
//
// sql.Tx is _nearly_ compatible with pgx's Tx type, after which neoq's Tx is modeled.
type TxWrapper struct {
	Tx pgx.Tx
}

func (t TxWrapper) Commit(ctx context.Context) (err error) {
	return t.Tx.Commit(ctx)
}

func (t TxWrapper) Rollback(ctx context.Context) (err error) {
	return t.Tx.Rollback(ctx)
}

func (t TxWrapper) QueryRow(ctx context.Context, query string, args ...any) neoq.Row {
	return t.Tx.QueryRow(ctx, query, args)
}

// FromStdlib creates a neoq DbTx from a standard library sql.Tx
func FromPgx(tx pgx.Tx) (t neoq.Tx) {
	return &TxWrapper{
		Tx: tx,
	}
}
