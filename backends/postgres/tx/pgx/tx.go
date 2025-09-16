package pgx

import (
	"github.com/acaloiaro/neoq"
	nqptx "github.com/acaloiaro/neoq/backends/postgres/tx"
	"github.com/jackc/pgx/v5"
)

// FromStdlib creates a neoq DbTx from a standard library sql.Tx
func FromPgx(tx pgx.Tx) neoq.Tx {
	return nqptx.TxWrapper{
		Tx: tx,
	}
}
