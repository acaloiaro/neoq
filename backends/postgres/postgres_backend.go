package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/acaloiaro/neoq/config"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/internal"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
	"github.com/acaloiaro/neoq/types"
	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

const (
	postgresBackendName       = "postgres"
	DefaultPgConnectionString = "postgres://postgres:postgres@127.0.0.1:5432/neoq"
	PendingJobIDQuery         = `SELECT id
					FROM neoq_jobs
					WHERE queue = $1
					AND status NOT IN ('processed')
					AND run_after <= NOW()
					FOR UPDATE SKIP LOCKED
					LIMIT 1`
	PendingJobQuery = `SELECT id,fingerprint,queue,status,payload,retries,max_retries,run_after,ran_at,created_at,error
					FROM neoq_jobs
					WHERE id = $1
					AND status NOT IN ('processed')
					AND run_after <= NOW()
					FOR UPDATE SKIP LOCKED
					LIMIT 1`
	FutureJobQuery = `SELECT id,run_after
					FROM neoq_jobs
					WHERE queue = $1
					AND status NOT IN ('processed')
					AND run_after > NOW()
					ORDER BY run_after ASC
					LIMIT 100
					FOR UPDATE SKIP LOCKED`
	setIdleInTxSessionTimeout = `SET idle_in_transaction_session_timeout = 0`
)

var (
	ErrCnxString              = errors.New("invalid connecton string: see documentation for valid connection strings")
	ErrDuplicateJobID         = errors.New("duplicate job id")
	ErrNoQueue                = errors.New("no queue specified")
	ErrNoTransactionInContext = errors.New("context does not have a Tx set")
)

// PgBackend is a Postgres-based Neoq backend
type PgBackend struct {
	types.Backend
	config      *config.Config
	logger      logging.Logger
	cron        *cron.Cron
	listenConn  *pgx.Conn
	mu          *sync.Mutex // mutex to protect mutating state on a pgWorker
	pool        *pgxpool.Pool
	futureJobs  map[int64]time.Time        // map of future job IDs to their due time
	handlers    map[string]handler.Handler // a map of queue names to queue handlers
	cancelFuncs []context.CancelFunc       // A collection of cancel functions to be called upon Shutdown()
}

// Backend initializes a new postgres-backed neoq backend
//
// If the database does not yet exist, Neoq will attempt to create the database and related tables by default.
//
// Backend requires that one of the [config.ConfigOption] is [config.WithConnectionString]
//
// Connection strings may be a URL or DSN-style connection strings. The connection string supports multiple
// options detailed below.
//
// options:
//   - pool_max_conns: integer greater than 0
//   - pool_min_conns: integer 0 or greater
//   - pool_max_conn_lifetime: duration string
//   - pool_max_conn_idle_time: duration string
//   - pool_health_check_period: duration string
//   - pool_max_conn_lifetime_jitter: duration string
//
// # Example DSN
//
// user=worker password=secret host=workerdb.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
// # Example URL
//
// postgres://worker:secret@workerdb.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
func Backend(ctx context.Context, opts ...config.Option) (pb types.Backend, err error) {
	p := &PgBackend{
		mu:          &sync.Mutex{},
		config:      config.New(),
		handlers:    make(map[string]handler.Handler),
		futureJobs:  make(map[int64]time.Time),
		logger:      slog.New(slog.NewTextHandler(os.Stdout)),
		cron:        cron.New(),
		cancelFuncs: []context.CancelFunc{},
	}

	// Set all options
	for _, opt := range opts {
		opt(p.config)
	}

	ctx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.cancelFuncs = append(p.cancelFuncs, cancel)
	p.mu.Unlock()

	err = p.initializeDB(ctx)
	if err != nil {
		return
	}

	if p.pool == nil {
		var poolConfig *pgxpool.Config
		poolConfig, err = pgxpool.ParseConfig(p.config.ConnectionString)
		if err != nil || p.config.ConnectionString == "" {
			return nil, ErrCnxString
		}

		// ensure that workers don't consume connections with idle transactions
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) (err error) {
			var query string
			if p.config.IdleTransactionTimeout > 0 {
				query = fmt.Sprintf("SET idle_in_transaction_session_timeout = '%dms'", p.config.IdleTransactionTimeout)
			} else {
				// there is no limit to the amount of time a worker's transactions may be idle
				query = setIdleInTxSessionTimeout
			}
			_, err = conn.Exec(ctx, query)
			return
		}

		p.pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return
		}
	}

	p.cron.Start()

	pb = p

	return
}

// WithTransactionTimeout sets the time that PgBackend's transactions may be idle before its underlying connection is
// closed
// The timeout is the number of milliseconds that a transaction may sit idle before postgres terminates the
// transaction's underlying connection. The timeout should be longer than your longest job takes to complete. If set
// too short, job state will become unpredictable, e.g. retry counts may become incorrect.
func WithTransactionTimeout(txTimeout int) config.Option {
	return func(c *config.Config) {
		c.IdleTransactionTimeout = txTimeout
	}
}

// txFromContext gets the transaction from a context, if the transaction is already set
func txFromContext(ctx context.Context) (t pgx.Tx, err error) {
	if v, ok := ctx.Value(handler.CtxVarsKey).(handler.CtxVars); ok {
		var tx pgx.Tx
		var ok bool
		if tx, ok = v.Tx.(pgx.Tx); !ok {
			return nil, ErrNoTransactionInContext
		}
		return tx, nil
	}

	return nil, ErrNoTransactionInContext
}

// initializeDB initializes the tables, types, and indices necessary to operate Neoq
//
//nolint:funlen,gocyclo,cyclop
func (p *PgBackend) initializeDB(ctx context.Context) (err error) {
	var pgxCfg *pgx.ConnConfig
	var tx pgx.Tx
	pgxCfg, err = pgx.ParseConfig(p.config.ConnectionString)
	if err != nil {
		return
	}

	connectStr := fmt.Sprintf("postgres://%s:%s@%s", pgxCfg.User, pgxCfg.Password, pgxCfg.Host)
	conn, err := pgx.Connect(ctx, connectStr)
	if err != nil {
		p.logger.Error("unableto connect to database", err)
		return
	}
	defer conn.Close(ctx)

	var dbExists bool
	dbExistsQ := fmt.Sprintf(`SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '%s');`, pgxCfg.Database)
	rows, err := conn.Query(ctx, dbExistsQ)
	if err != nil {
		return fmt.Errorf("unable to determne if jobs table exists: %w", err)
	}
	for rows.Next() {
		err = rows.Scan(&dbExists)
		if err != nil {
			return fmt.Errorf("unable to determine if jobs table exists: %w", err)
		}
	}
	defer rows.Close()

	conn.Close(ctx)
	conn, err = pgx.Connect(ctx, connectStr)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	if !dbExists {
		createDBQ := fmt.Sprintf("CREATE DATABASE %s", pgxCfg.Database)
		if _, err = conn.Exec(ctx, createDBQ); err != nil {
			return fmt.Errorf("unable to create neoq database: %w", err)
		}
	}

	conn, err = pgx.ConnectConfig(ctx, pgxCfg)
	if err != nil {
		return
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		return
	}
	defer func(ctx context.Context) { _ = tx.Rollback(ctx) }(ctx) // rollback has no effect if the transaction has been committed

	jobsTableExistsQ := `SELECT EXISTS (SELECT FROM
				pg_tables
		WHERE
				schemaname = 'public' AND
				tablename  = 'neoq_jobs'
    );`
	rows, err = tx.Query(ctx, jobsTableExistsQ)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to determne if jobs table exists: %v", err)
		return
	}

	var tablesInitialized bool
	for rows.Next() {
		err = rows.Scan(&tablesInitialized)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to determine if jobs table exists: %v", err)
			return
		}
	}
	defer rows.Close()

	if !tablesInitialized {
		createTablesQ := `
			CREATE TYPE job_status AS ENUM (
				'new',
				'processed',
				'failed'
			);

			CREATE TABLE neoq_dead_jobs (
					id SERIAL NOT NULL,
					fingerprint text NOT NULL,
					queue text NOT NULL,
					status job_status NOT NULL default 'failed',
					payload jsonb,
					retries integer,
					max_retries integer,
					created_at timestamp with time zone DEFAULT now(),
					error text
			);

			CREATE TABLE neoq_jobs (
					id SERIAL NOT NULL,
					fingerprint text NOT NULL,
					queue text NOT NULL,
					status job_status NOT NULL default 'new',
					payload jsonb,
					retries integer default 0,
					max_retries integer default 23,
					run_after timestamp with time zone DEFAULT now(),
					ran_at timestamp with time zone,
					created_at timestamp with time zone DEFAULT now(),
					error text
			);

			CREATE INDEX neoq_job_fetcher_idx ON neoq_jobs (id, status, run_after);
			CREATE INDEX neoq_jobs_fetcher_idx ON neoq_jobs (queue, status, run_after);
			CREATE INDEX neoq_jobs_fingerprint_idx ON neoq_jobs (fingerprint, status);
		`

		_, err = tx.Exec(ctx, createTablesQ)
		if err != nil {
			return fmt.Errorf("unable to create job status enum: %w", err)
		}

		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("error committing transaction: %w", err)
		}
	}

	return nil
}

// Enqueue adds jobs to the specified queue
func (p *PgBackend) Enqueue(ctx context.Context, job *jobs.Job) (jobID int64, err error) {
	ctx, cancel := context.WithCancel(ctx)
	p.mu.Lock()
	p.cancelFuncs = append(p.cancelFuncs, cancel)
	p.mu.Unlock()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		err = fmt.Errorf("error acquiring connection: %w", err)
		return
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		err = fmt.Errorf("error creating transaction: %w", err)
		return
	}

	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer func(ctx context.Context) { _ = tx.Rollback(ctx) }(ctx) // rollback has no effect if the transaction has been committed

	// Make sure RunAfter is set to a non-zero value if not provided by the caller
	// if already set, schedule the future job
	now := time.Now()
	if job.RunAfter.IsZero() {
		job.RunAfter = now
	}

	if job.Queue == "" {
		err = ErrNoQueue
		return
	}

	jobID, err = p.enqueueJob(ctx, tx, job)
	if err != nil {
		err = fmt.Errorf("error enqueuing job: %w", err)
	}
	if jobID == jobs.DuplicateJobID {
		err = ErrDuplicateJobID
		return
	}

	// notify listeners that a new job has arrived if it's not a future job
	if job.RunAfter.Equal(now) {
		_, err = tx.Exec(ctx, fmt.Sprintf("NOTIFY %s, '%d'", job.Queue, jobID))
		if err != nil {
			err = fmt.Errorf("error executing transaction: %w", err)
		}
	} else {
		p.mu.Lock()
		p.futureJobs[jobID] = job.RunAfter
		p.mu.Unlock()
	}

	err = tx.Commit(ctx)
	if err != nil {
		err = fmt.Errorf("error committing transaction: %w", err)
		return
	}

	return jobID, nil
}

// Start starts processing jobs with the specified queue and handler
func (p *PgBackend) Start(ctx context.Context, queue string, h handler.Handler) (err error) {
	ctx, cancel := context.WithCancel(ctx)

	p.mu.Lock()
	p.cancelFuncs = append(p.cancelFuncs, cancel)
	p.handlers[queue] = h
	p.mu.Unlock()

	err = p.start(ctx, queue)
	if err != nil {
		return
	}
	return
}

// StartCron starts processing jobs with the specified cron schedule and handler
//
// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
func (p *PgBackend) StartCron(ctx context.Context, cronSpec string, h handler.Handler) (err error) {
	cd, err := crondescriptor.NewCronDescriptor(cronSpec)
	if err != nil {
		return fmt.Errorf("error creating cron descriptor: %w", err)
	}

	cdStr, err := cd.GetDescription(crondescriptor.Full)
	if err != nil {
		return fmt.Errorf("error getting cron description: %w", err)
	}

	queue := internal.StripNonAlphanum(strcase.ToSnake(*cdStr))
	ctx, cancel := context.WithCancel(ctx)

	p.mu.Lock()
	p.cancelFuncs = append(p.cancelFuncs, cancel)
	p.mu.Unlock()

	if err = p.cron.AddFunc(cronSpec, func() {
		_, err := p.Enqueue(ctx, &jobs.Job{Queue: queue})
		if err != nil {
			p.logger.Error("error queueing cron job", err)
		}
	}); err != nil {
		return fmt.Errorf("error adding cron: %w", err)
	}

	err = p.Start(ctx, queue, h)

	return
}

// SetLogger sets this backend's logger
func (p *PgBackend) SetLogger(logger logging.Logger) {
	p.logger = logger
}

func (p *PgBackend) Shutdown(ctx context.Context) {
	p.pool.Close() // also closes the hijacked listenConn
	p.cron.Stop()

	for _, f := range p.cancelFuncs {
		f()
	}

	p.cancelFuncs = nil
}

// enqueueJob adds jobs to the queue, returning the job ID
//
// Jobs that are not already fingerprinted are fingerprinted before being added
// Duplicate jobs are not added to the queue. Any two unprocessed jobs with the same fingerprint are duplicates
func (p *PgBackend) enqueueJob(ctx context.Context, tx pgx.Tx, j *jobs.Job) (jobID int64, err error) {
	err = jobs.FingerprintJob(j)
	if err != nil {
		return
	}

	var rowCount int64
	countRow := tx.QueryRow(ctx, `SELECT COUNT(*) as row_count
		FROM neoq_jobs
		WHERE fingerprint = $1
		AND status NOT IN ('processed')`, j.Fingerprint)
	err = countRow.Scan(&rowCount)
	if err != nil {
		return
	}

	// this is a duplicate job; skip it
	if rowCount > 0 {
		return jobs.DuplicateJobID, nil
	}

	err = tx.QueryRow(ctx, `INSERT INTO neoq_jobs(queue, fingerprint, payload, run_after)
		VALUES ($1, $2, $3, $4) RETURNING id`,
		j.Queue, j.Fingerprint, j.Payload, j.RunAfter).Scan(&jobID)
	if err != nil {
		return
	}

	return
}

// moveToDeadQueue moves jobs from the pending queue to the dead queue
func (p *PgBackend) moveToDeadQueue(ctx context.Context, tx pgx.Tx, j *jobs.Job, jobErr error) (err error) {
	_, err = tx.Exec(ctx, "DELETE FROM neoq_jobs WHERE id = $1", j.ID)
	if err != nil {
		return
	}

	_, err = tx.Exec(ctx, `INSERT INTO neoq_dead_jobs(id, queue, fingerprint, payload, retries, max_retries, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		j.ID, j.Queue, j.Fingerprint, j.Payload, j.Retries, j.MaxRetries, jobErr.Error())

	return
}

// updateJob updates the status of jobs with: status, run time, error messages, and retries
// if the retry count exceeds the maximum number of retries for the job, move the job to the dead jobs queue
//
// if `tx`'s underlying connection dies, it results in this function throwing an error and job status inaccurately
// reflecting the status of the job and its number of retries.
// TODO: Handle dropped connections when updating job status in PgBackend
// e.g. acquiring a new connection in the event of connection failure
// nolint: cyclop
func (p *PgBackend) updateJob(ctx context.Context, jobErr error) (err error) {
	status := internal.JobStatusProcessed
	errMsg := ""

	if jobErr != nil {
		status = internal.JobStatusFailed
		errMsg = jobErr.Error()
	}

	var job *jobs.Job
	if job, err = handler.JobFromContext(ctx); err != nil {
		return fmt.Errorf("error getting job from context: %w", err)
	}

	var tx pgx.Tx
	if tx, err = txFromContext(ctx); err != nil {
		return fmt.Errorf("error getting tx from context: %w", err)
	}

	if job.Retries >= job.MaxRetries {
		err = p.moveToDeadQueue(ctx, tx, job, jobErr)
		return
	}

	var runAfter time.Time
	if job.Retries > 0 && status == internal.JobStatusFailed {
		runAfter = internal.CalculateBackoff(job.Retries)
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3, retries = $4, run_after = $5 WHERE id = $6"
		_, err = tx.Exec(ctx, qstr, time.Now(), errMsg, status, job.Retries, runAfter, job.ID)
	} else if job.Retries > 0 && status != internal.JobStatusFailed {
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3, retries = $4 WHERE id = $5"
		_, err = tx.Exec(ctx, qstr, time.Now(), errMsg, status, job.Retries, job.ID)
	} else {
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3 WHERE id = $4"
		_, err = tx.Exec(ctx, qstr, time.Now(), errMsg, status, job.ID)
	}

	if err == nil && time.Until(runAfter) > 0 {
		p.mu.Lock()
		p.futureJobs[job.ID] = runAfter
		p.mu.Unlock()
	}

	return nil
}

// start starts processing new, pending, and future jobs
// nolint: cyclop
func (p *PgBackend) start(ctx context.Context, queue string) (err error) {
	var h handler.Handler
	var ok bool
	if h, ok = p.handlers[queue]; !ok {
		return fmt.Errorf("%w: %s", handler.ErrNoHandlerForQueue, queue)
	}
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return
	}

	// use a single connection to listen for jobs on all queues
	// TODO: Give more thought to the implications of hijacking connections to LISTEN on in PgBackend
	// should this connecton not come from the pool, to avoid tainting it with connections that don't have an idle in
	// transaction time out set?
	p.mu.Lock()
	if p.listenConn == nil {
		p.listenConn = conn.Hijack()
	}
	p.mu.Unlock()

	listenJobChan := p.listen(ctx, queue)        // listen for 'new' jobs
	pendingJobsChan := p.pendingJobs(ctx, queue) // process overdue jobs *at startup*

	// process all future jobs and retries
	// TODO consider performance implications of `scheduleFutureJobs` in PgBackend
	go func() { p.scheduleFutureJobs(ctx, queue) }()

	for i := 0; i < h.Concurrency; i++ {
		go func() {
			var err error
			var jobID int64

			for {
				select {
				case jobID = <-listenJobChan:
					err = p.handleJob(ctx, jobID, h)
				case jobID = <-pendingJobsChan:
					err = p.handleJob(ctx, jobID, h)
				case <-ctx.Done():
					return
				}

				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						err = nil
						continue
					}

					p.logger.Error("job failed", err, "job_id", jobID)

					continue
				}
			}
		}()
	}

	return nil
}

// removeFutureJob removes a future job from the in-memory list of jobs that will execute in the future
func (p *PgBackend) removeFutureJob(jobID int64) {
	if _, ok := p.futureJobs[jobID]; ok {
		p.mu.Lock()
		delete(p.futureJobs, jobID)
		p.mu.Unlock()
	}
}

// initFutureJobs is intended to be run once to initialize the list of future jobs that must be monitored for
// execution. it should be run only during system startup.
func (p *PgBackend) initFutureJobs(ctx context.Context, queue string) {
	rows, err := p.pool.Query(ctx, FutureJobQuery, queue)
	if err != nil {
		p.logger.Error("error fetching future jobs list", err)
		return
	}

	var id int64
	var runAfter time.Time
	_, _ = pgx.ForEachRow(rows, []any{&id, &runAfter}, func() error {
		p.mu.Lock()
		p.futureJobs[id] = runAfter
		p.mu.Unlock()
		return nil
	})
}

// scheduleFutureJobs announces future jobs using NOTIFY on an interval
func (p *PgBackend) scheduleFutureJobs(ctx context.Context, queue string) {
	p.initFutureJobs(ctx, queue)

	// check for new future jobs on an interval
	ticker := time.NewTicker(p.config.JobCheckInterval)

	for {
		// loop over list of future jobs, scheduling goroutines to wait for jobs that are due within the next 30 seconds
		for jobID, runAfter := range p.futureJobs {
			timeUntillRunAfter := time.Until(runAfter)
			if timeUntillRunAfter <= p.config.FutureJobWindow {
				p.removeFutureJob(jobID)
				go func(jid int64) {
					scheduleCh := time.After(timeUntillRunAfter)
					<-scheduleCh
					p.announceJob(ctx, queue, jid)
				}(jobID)
			}
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return
		}
	}
}

// announceJob announces jobs to queue listeners.
//
// Announced jobs are executed by the first worker to respond to the announcement.
func (p *PgBackend) announceJob(ctx context.Context, queue string, jobID int64) {
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return
	}

	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer func(ctx context.Context) { _ = tx.Rollback(ctx) }(ctx)

	// notify listeners that a job is ready to run
	_, err = tx.Exec(ctx, fmt.Sprintf("NOTIFY %s, '%d'", queue, jobID))
	if err != nil {
		return
	}

	err = tx.Commit(ctx)
	if err != nil {
		return
	}
}

func (p *PgBackend) pendingJobs(ctx context.Context, queue string) (jobsCh chan int64) {
	jobsCh = make(chan int64)

	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		p.logger.Error("failed to acquire database connection to listen for pending queue items", err)
		return
	}

	go func(ctx context.Context) {
		defer conn.Release()

		for {
			jobID, err := p.getPendingJobID(ctx, conn, queue)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					p.logger.Error("failed to fetch pending job", err, "job_id", jobID)
				} else {
					// done fetching pending jobs
					break
				}
			} else {
				jobsCh <- jobID
			}
		}
	}(ctx)

	return
}

// handleJob is the workhorse of Neoq
// it receives pending, periodic, and retry job ids asynchronously
// 1. handleJob first creates a transactions inside of which a row lock is acquired for the job to be processed.
// 2. handleJob secondly calls the handler on the job, and finally updates the job's status
func (p *PgBackend) handleJob(ctx context.Context, jobID int64, h handler.Handler) (err error) {
	var tx pgx.Tx
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release()

	tx, err = conn.Begin(ctx)
	if err != nil {
		return
	}
	defer func(ctx context.Context) { _ = tx.Rollback(ctx) }(ctx) // rollback has no effect if the transaction has been committed

	ctxv := handler.CtxVars{Tx: tx}
	var job *jobs.Job
	job, err = p.getPendingJob(ctx, tx, jobID)
	if err != nil {
		return
	}

	ctxv.Job = job
	ctx = handler.WithContext(ctx, ctxv)

	// check if the job is being retried and increment retry count accordingly
	if job.Status != internal.JobStatusNew {
		job.Retries++
	}

	// execute the queue handler of this job
	jobErr := handler.Exec(ctx, h)
	if jobErr != nil {
		err = fmt.Errorf("error executing handler: %w", jobErr)
		return err
	}

	err = p.updateJob(ctx, jobErr)
	if err != nil {
		err = fmt.Errorf("error updating job status: %w", err)
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		p.logger.Error("unable to commit job transaction. retrying this job may dupliate work", err, "job_id", job.ID)
		return fmt.Errorf("unable to commit job transaction. retrying this job may dupliate work: %w", err)
	}

	return nil
}

// listen uses Postgres LISTEN to listen for jobs on a queue
// TODO: There is currently no handling of listener disconnects in PgBackend.
// This will lead to jobs not getting processed until the worker is restarted.
// Implement disconnect handling.
func (p *PgBackend) listen(ctx context.Context, queue string) (c chan int64) {
	var err error
	c = make(chan int64)

	// set this connection's idle in transaction timeout to infinite so it is not intermittently disconnected
	_, err = p.listenConn.Exec(ctx, fmt.Sprintf("SET idle_in_transaction_session_timeout = '0'; LISTEN %s", queue))
	if err != nil {
		err = fmt.Errorf("unable to create database connection for listener: %w", err)
		p.logger.Error("unablet o create database connection for listener", err)
		return
	}

	go func(ctx context.Context) {
		for {
			notification, waitErr := p.listenConn.WaitForNotification(ctx)
			if waitErr != nil {
				if errors.Is(waitErr, context.Canceled) {
					return
				}

				p.logger.Error("failed to wait for notification", waitErr)
				time.Sleep(1 * time.Second)
				continue
			}

			var jobID int64
			if jobID, err = strconv.ParseInt(notification.Payload, 0, 64); err != nil {
				p.logger.Error("unable to fetch job", err)
				continue
			}

			c <- jobID
		}
	}(ctx)

	return c
}

func (p *PgBackend) getPendingJob(ctx context.Context, tx pgx.Tx, jobID int64) (job *jobs.Job, err error) {
	row, err := tx.Query(ctx, PendingJobQuery, jobID)
	if err != nil {
		return
	}

	job, err = pgx.CollectOneRow(row, pgx.RowToAddrOfStructByName[jobs.Job])
	if err != nil {
		return
	}

	return
}

func (p *PgBackend) getPendingJobID(ctx context.Context, conn *pgxpool.Conn, queue string) (jobID int64, err error) {
	err = conn.QueryRow(ctx, PendingJobIDQuery, queue).Scan(&jobID)
	return
}
