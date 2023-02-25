package context

import (
	"context"
	"errors"

	"github.com/acaloiaro/neoq/jobs"
	"github.com/jackc/pgx/v5"
)

type contextKey int

var VarsKey contextKey

// HandlerCtxVars are variables passed to every Handler context
type HandlerCtxVars struct {
	Job *jobs.Job
	Tx  pgx.Tx
}

// JobFromContext fetches the job from a context if the job context variable is already set
func JobFromContext(ctx context.Context) (j *jobs.Job, err error) {
	if v, ok := ctx.Value(VarsKey).(HandlerCtxVars); ok {
		j = v.Job
	} else {
		err = errors.New("context does not have a Job set")
	}

	return
}

// TxFromContext gets the transaction from a context, if the the transaction is already set
func TxFromContext(ctx context.Context) (t pgx.Tx, err error) {
	if v, ok := ctx.Value(VarsKey).(HandlerCtxVars); ok {
		t = v.Tx
	} else {
		err = errors.New("context does not have a Tx set")
	}

	return
}
