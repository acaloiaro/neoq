package context

import (
	"context"
	"errors"

	"github.com/acaloiaro/neoq/jobs"
	"github.com/jackc/pgx/v5"
)

type contextKey struct{}

var (
	VarsKey contextKey

	ErrNoJobForContext        = errors.New("context does not have a Job set")
	ErrNoTransactionInContext = errors.New("context does not have a Tx set")
)

// HandlerCtxVars are variables passed to every Handler context
type HandlerCtxVars struct {
	Job *jobs.Job
	Tx  pgx.Tx // TODO figure out how to move into postgres backend
}

// JobFromContext fetches the job from a context if the job context variable is already set
func JobFromContext(ctx context.Context) (*jobs.Job, error) {
	if v, ok := ctx.Value(VarsKey).(HandlerCtxVars); ok {
		return v.Job, nil
	}

	return nil, ErrNoJobForContext
}

// TxFromContext gets the transaction from a context, if the transaction is already set
func TxFromContext(ctx context.Context) (t pgx.Tx, err error) {
	if v, ok := ctx.Value(VarsKey).(HandlerCtxVars); ok {
		return v.Tx, nil
	}

	return nil, ErrNoTransactionInContext
}
