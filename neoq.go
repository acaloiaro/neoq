// Package neoq provides background job processing for Go applications.
//
// Neoq's goal is to minimize the infrastructure necessary to add background job processing to Go applications. It does so by implementing queue durability with modular backends, rather than introducing a strict dependency on a particular backend such as Redis.
//
// A Postgres backend is provided out of a box. However, for Neoq to meet its goal of reducing the infrastructure
// necessary to run background jobs -- additional backends are necessary. E.g. Applications that use MySQL, MonogoDB, or
// Redis as their primary data stores will ideally use Neoq with corresponding backends.
package neoq

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"math/rand"

	"github.com/guregu/null"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/exp/slog"
)

type contextKey int

var varsKey contextKey

const (
	JobStatusNew                  = "new"
	JobStatusProcessed            = "processed"
	JobStatusFailed               = "failed"
	DefaultTransactionTimeout int = 60000 //ms
	DefaultHandlerDeadline        = 30000 //ms
	DuplicateJobID                = -1

	PendingJobIDQuery = `SELECT id
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
)

// Config configures Neoq's general behavior
type Config struct {
	// the time that a worker's transaction may be idle before its underlying connection is closed
	idleTxTimeout int
}

// ConfigOption is a function that sets optional Neoq configuration
type ConfigOption func(n Neoq)

// Job contains all the data pertaining to jobs
//
// Jobs are what are placed on queues for processing.
//
// The Fingerprint field can be supplied by the user to impact job deduplication.
type Job struct {
	ID          int64          `db:"id"`
	Fingerprint string         `db:"fingerprint"` // A md5 sum of the job's queue + payload, affects job deduplication
	Status      string         `db:"status"`      // The status of the job
	Queue       string         `db:"queue"`       // The queue the job is on
	Payload     map[string]any `db:"payload"`     // JSON job payload for more complex jobs
	RunAfter    time.Time      `db:"run_after"`   // The time after which the job is elligible to be picked up by a worker
	RanAt       null.Time      `db:"ran_at"`      // The last time the job ran
	Error       null.String    `db:"error"`       // The last error the job elicited
	Retries     int            `db:"retries"`     // The number of times the job has retried
	MaxRetries  int            `db:"max_retries"` // The maximum number of times the job can retry
	CreatedAt   time.Time      `db:"created_at"`  // The time the job was created
}

// handlerCtxVars are variables passed to every Handler context
type handlerCtxVars struct {
	job *Job
	tx  pgx.Tx
}

// TransactionTimeoutOpt sets the time that a worker's transaction may be idle before its underlying connection is
// closed
// The timeout is the number of milliseconds that a transaction may sit idle before postgres terminates the
// transaction's underlying connection. The timeout should be longer than your longest job takes to complete. If set
// too short, job state will become unpredictable, e.g. retry counts may become incorrect.
//
// TransactionTimeoutOpt is best set when calling neoq.New() rather than after creation using WithConfigOpt() because this
// setting results in the creation of a new database connection pool.
func TransactionTimeoutOpt(txTimeout int) ConfigOption {
	return func(n Neoq) {
		n.Config().idleTxTimeout = txTimeout

		// implementation is a pgWorker; sets the AfterConnect on its pool
		if pgw, ok := n.(*pgWorker); ok {
			// if the worker already has a pool configured, then setting the transaction timeout prints a warning and is a
			// no-op
			if pgw.pool != nil {
				log.Println("create a new Neoq instance to set transaction timeout")
				return
			}

			var err error
			var poolConfig *pgxpool.Config

			poolConfig, err = pgxpool.ParseConfig(pgw.dbConnectString)
			if err != nil || pgw.dbConnectString == "" {
				return
			}

			// ensure that workers don't consume connections with idle transactions
			poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) (err error) {
				var query string
				if n.Config().idleTxTimeout > 0 {
					query = fmt.Sprintf("SET idle_in_transaction_session_timeout = '%dms'", n.Config().idleTxTimeout)
				} else {
					// there is no limit to the amount of time a worker's transactions may be idle
					query = "SET idle_in_transaction_session_timeout = 0"
				}
				_, err = conn.Exec(ctx, query)
				return
			}

			pgw.pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
			if err != nil {
				return
			}
		}
	}
}

// JobFromContext fetches the job from a context if the job context variable is already set
func JobFromContext(ctx context.Context) (j *Job, err error) {
	if v, ok := ctx.Value(varsKey).(handlerCtxVars); ok {
		j = v.job
	} else {
		err = errors.New("context does not have a Job set")
	}

	return
}

// withHandlerContext creates a new context with the job and transaction set
func withHandlerContext(ctx context.Context, v handlerCtxVars) context.Context {
	return context.WithValue(ctx, varsKey, v)
}

// txFromContext gets the transaction from a context, if the the transaction is already set
func txFromContext(ctx context.Context) (t pgx.Tx, err error) {
	if v, ok := ctx.Value(varsKey).(handlerCtxVars); ok {
		t = v.tx
	} else {
		err = errors.New("context does not have a Tx set")
	}

	return
}

// Neoq interface is Neoq's primary API
type Neoq interface {
	// Enqueue queues jobs to be executed asynchronously
	Enqueue(job Job) (jobID int64, err error)

	// Listen listens for jobs on a queue and processes them with the given handler
	Listen(queue string, h Handler) (err error)

	// Shutdown halts the worker
	Shutdown() error

	// WithConfigOpt configures neoq with with optional configuration
	WithConfigOpt(opt ConfigOption) Neoq

	// Config retrieves neoq's configuration
	Config() *Config
}

// Logger interface is the interface that neoq's logger must implement
//
// This interface is a subset of [slog.Logger]. The slog interface was chosen under the assumption that its
// likely to be Golang's standard library logging interface.
//
// TODO: Add WithLogger() and WithLoggerOpt() for user-supplied logger configuration
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, err error, args ...any)
	Info(msg string, args ...any)
}

// HandlerFunc is a function that Handlers execute for every Job on a queue
type HandlerFunc func(ctx context.Context) error

// Handler handles jobs on a queue
type Handler struct {
	deadline    time.Duration
	handle      HandlerFunc
	concurrency int
}

// HandlerOption is function that sets optional configuration for Handlers
type HandlerOption func(w *Handler)

// WithOption returns the handler with the given options set
func (h Handler) WithOption(opt HandlerOption) (handler Handler) {
	opt(&h)
	return h
}

// HandlerDeadlineOpt configures handlers with a deadline for every job that it excutes
// The deadline is the amount of time (ms) that can be spent executing the handler's HandlerFunction
// when the deadline is exceeded, jobs are failed and begin the retry phase of their lifecycle
func HandlerDeadlineOpt(d time.Duration) HandlerOption {
	return func(h *Handler) {
		h.deadline = d
	}
}

// HandlerConcurrencyOpt configures Neoq handlers to process jobs concurrently
// the default concurrency is the number of (v)CPUs on the machine running Neoq
func HandlerConcurrencyOpt(c int) HandlerOption {
	return func(h *Handler) {
		h.concurrency = c
	}
}

// NewHandler creates a new queue handler
func NewHandler(f HandlerFunc, opts ...HandlerOption) (h Handler) {
	h = Handler{
		handle:      f,
		concurrency: runtime.NumCPU() - 1,
	}

	for _, opt := range opts {
		opt(&h)
	}

	// always set a job deadline if none is set
	if h.deadline == 0 {
		h.deadline = time.Duration(DefaultHandlerDeadline * time.Millisecond)
	}

	return
}

// pgWorker is a concrete Neoq implementation based on Postgres, using pgx/pgxpool
type pgWorker struct {
	config          *Config
	dbConnectString string
	idleTxTimeout   int
	listenConn      *pgx.Conn
	pool            *pgxpool.Pool
	handlers        map[string]Handler  // a map of queue names to queue handlers
	mu              *sync.Mutex         // mutext to protect mutating state on a pgWorker
	futureJobs      map[int64]time.Time // map of future job IDs to their due time
	logger          Logger
}

// New creates a new Neoq instance for listening to queues and enqueing new jobs
//
// If the database does not yet exist, Neoq will attempt to create the database and related tables by default.
//
// Connection strings may be a URL or DSN-style connection string to neoq's database. The connection string supports multiple
// options detailed below.
//
// options:
// pool_max_conns: integer greater than 0
// pool_min_conns: integer 0 or greater
// pool_max_conn_lifetime: duration string
// pool_max_conn_idle_time: duration string
// pool_health_check_period: duration string
// pool_max_conn_lifetime_jitter: duration string
//
// # Example DSN
// user=worker password=secret host=workerdb.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
// # Example URL
// postgres://worker:secret@workerdb.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
//
// Available ConfigOption
// - WithTransactionTimeout(timeout int): configure the idle_in_transaction_timeout for the worker's database
// connection(s)
func New(connection string, opts ...ConfigOption) (n Neoq, err error) {

	w := pgWorker{
		mu:              &sync.Mutex{},
		config:          &Config{},
		dbConnectString: connection,
		handlers:        make(map[string]Handler),
		futureJobs:      make(map[int64]time.Time),
		logger:          slog.New(slog.NewTextHandler(os.Stdout)),
	}

	// Set all options
	for _, opt := range opts {
		opt(w)
	}

	err = w.initializeDB()
	if err != nil {
		return
	}

	if w.pool == nil {
		var poolConfig *pgxpool.Config
		poolConfig, err = pgxpool.ParseConfig(w.dbConnectString)
		if err != nil || w.dbConnectString == "" {
			return nil, errors.New("invalid connecton string: see documentation for valid connection strings")
		}

		// ensure that workers don't consume connections with idle transactions
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) (err error) {
			var query string
			if w.idleTxTimeout > 0 {
				query = fmt.Sprintf("SET idle_in_transaction_session_timeout = '%dms'", w.idleTxTimeout)
			} else {
				// there is no limit to the amount of time a worker's transactions may be idle
				query = "SET idle_in_transaction_session_timeout = 0"
			}
			_, err = conn.Exec(ctx, query)
			return
		}

		w.pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err != nil {
			return
		}
	}

	n = w

	return
}

func (w pgWorker) Config() *Config {
	return w.config
}

// initializeDB initializes the tables, types, and indices necessary to operate Neoq
func (w pgWorker) initializeDB() (err error) {
	var pgxCfg *pgx.ConnConfig
	var tx pgx.Tx
	ctx := context.Background()
	pgxCfg, err = pgx.ParseConfig(w.dbConnectString)
	if err != nil {
		return
	}

	dbName := pgxCfg.Database
	pgxCfg.Database = ""
	conn, err := pgx.ConnectConfig(context.Background(), pgxCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	var dbExists bool
	dbExistsQ := fmt.Sprintf(`SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '%s');`, dbName)
	rows, err := conn.Query(context.Background(), dbExistsQ)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to determne if jobs table exists: %v", err)
		return
	}
	for rows.Next() {
		err = rows.Scan(&dbExists)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to determine if jobs table exists: %v", err)
			return
		}
	}
	defer rows.Close()

	conn.Close(ctx)
	pgxCfg.Database = dbName
	conn, err = pgx.ConnectConfig(context.Background(), pgxCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	if !dbExists {
		createDBQ := "CREATE DATABASE neoq"
		_, err := conn.Exec(ctx, createDBQ)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to create neoq database: %v", err)
			return err
		}
	}
	tx, err = conn.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx) // rollback has no effect if the transaction has been committed

	jobsTableExistsQ := `SELECT EXISTS (SELECT FROM
				pg_tables
		WHERE
				schemaname = 'public' AND
				tablename  = 'neoq_jobs'
    );`
	rows, err = tx.Query(context.Background(), jobsTableExistsQ)
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
			fmt.Fprintf(os.Stderr, "unable to create job status enum: %v", err)
			return err
		}

		err = tx.Commit(ctx)
	}

	return
}

// Enqueue adds jobs to the specified queue
func (w pgWorker) Enqueue(job Job) (jobID int64, err error) {
	ctx := context.Background()
	conn, err := w.pool.Acquire(ctx)
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
	defer tx.Rollback(ctx)

	// Make sure RunAfter is set to a non-zero value if not provided by the caller
	// if already set, schedule the future job
	now := time.Now()
	if job.RunAfter.IsZero() {
		job.RunAfter = now
	}

	if job.Queue == "" {
		err = errors.New("this job does not specify a Queue. Please specify a queue")

		return
	}

	jobID, err = w.enqueueJob(ctx, tx, job)

	if err != nil || jobID == DuplicateJobID {
		return
	}

	// notify listeners that a new job has arrived if it's not a future job
	if job.RunAfter == now {
		_, err = tx.Exec(ctx, fmt.Sprintf("NOTIFY %s, '%d'", job.Queue, jobID))
		if err != nil {
			return
		}
	} else {
		w.mu.Lock()
		w.futureJobs[jobID] = job.RunAfter
		w.mu.Unlock()
	}

	err = tx.Commit(ctx)
	if err != nil {
		return
	}

	return
}

// Listen sets the queue handler function for the specified queue.
//
// Neoq will not process any jobs until Listen() is called at least once.
func (w pgWorker) Listen(queue string, h Handler) (err error) {
	w.handlers[queue] = h

	err = w.start(queue)
	if err != nil {
		return
	}
	return
}

func (w pgWorker) Shutdown() (err error) {
	w.pool.Close()

	err = w.listenConn.Close(context.Background())

	return
}

func (w pgWorker) WithConfigOpt(opt ConfigOption) Neoq {
	opt(&w)
	return &w
}

// enqueueJob adds jobs to the queue, returning the job ID
//
// Jobs that are not already fingerprinted are fingerprinted before being added
// Duplicate jobs are not added to the queue. Any two unprocessed jobs with the same fingerprint are duplicates
func (w pgWorker) enqueueJob(ctx context.Context, tx pgx.Tx, j Job) (jobID int64, err error) {

	// fingerprint the job if it hasn't been fingerprinted already
	// a fingerprint is the md5 sum of: queue + payload
	if j.Fingerprint == "" {
		var js []byte
		js, err = json.Marshal(j.Payload)
		if err != nil {
			return
		}
		h := md5.New()
		io.WriteString(h, j.Queue)
		io.WriteString(h, string(js))
		j.Fingerprint = fmt.Sprintf("%x", h.Sum(nil))
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
		return DuplicateJobID, nil
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
func (w pgWorker) moveToDeadQueue(ctx context.Context, tx pgx.Tx, j *Job, jobErr error) (err error) {
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
// reflecting the status of the job and its number of retries. TODO: consider solutions to this problem, e.g. acquiring
// a new connection in the event of connection failure
func (w pgWorker) updateJob(ctx context.Context, jobErr error) (err error) {
	status := JobStatusProcessed
	errMsg := ""

	if jobErr != nil {
		status = JobStatusFailed
		errMsg = jobErr.Error()
	}

	var job *Job
	if job, err = JobFromContext(ctx); err != nil {
		return errors.New("unable to get job from context")
	}

	var tx pgx.Tx
	if tx, err = txFromContext(ctx); err != nil {
		return errors.New("unable to get transaction from context")
	}

	if job.Retries >= job.MaxRetries {
		err = w.moveToDeadQueue(ctx, tx, job, jobErr)
		return
	}

	var runAfter time.Time
	if job.Retries > 0 && status == JobStatusFailed {
		runAfter = calculateBackoff(job.Retries)
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3, retries = $4, run_after = $5 WHERE id = $6"
		_, err = tx.Exec(ctx, qstr, time.Now(), errMsg, status, job.Retries, runAfter, job.ID)
	} else if job.Retries > 0 && status != JobStatusFailed {
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3, retries = $4 WHERE id = $5"
		_, err = tx.Exec(ctx, qstr, time.Now(), errMsg, status, job.Retries, job.ID)
	} else {
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3 WHERE id = $4"
		_, err = tx.Exec(ctx, qstr, time.Now(), errMsg, status, job.ID)
	}

	if err == nil && time.Until(runAfter) > 0 {
		w.mu.Lock()
		w.futureJobs[job.ID] = runAfter
		w.mu.Unlock()
	}

	return
}

// start starts a queue listener, processes pending job, and fires up goroutines to process future jobs
func (w pgWorker) start(queue string) (err error) {

	var handler Handler
	var ok bool
	if handler, ok = w.handlers[queue]; !ok {
		return fmt.Errorf("no handler for queue: %s", queue)
	}
	ctx := context.Background()
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return
	}

	// TODO: Give more thought to the implications of hijacking a conn from the pool here
	// should this connect not come from the pool, to avoid tainting it with connections that don't have an idle in
	// transaction time out set?
	w.listenConn = conn.Hijack()

	listenJobChan := w.listen(ctx, queue)        // listen for 'new' jobs
	pendingJobsChan := w.pendingJobs(ctx, queue) // process overdue jobs *at startup*

	// process all future jobs and retries
	// TODO there are no illusions about the current future jobs system being robust
	// the current implementation is a brute force proof of concept that can certainly be improved upon
	go func() { w.scheduleFutureJobs(ctx, queue) }()

	for i := 0; i < handler.concurrency; i++ {
		go func() {
			var jobID int64

			for {
				select {
				case jobID = <-listenJobChan:
					err = w.handleJob(ctx, jobID, handler)
				case jobID = <-pendingJobsChan:
					err = w.handleJob(ctx, jobID, handler)
				case <-ctx.Done():
					return
				}

				if err != nil {
					if err.Error() == "no rows in result set" {
						err = nil
					} else {
						w.logger.Error("error handling job", err, "job_id", jobID)
					}

					continue
				}
			}
		}()
	}
	return
}

// removeFutureJob removes a future job from the in-memory list of jobs that will execute in the future
func (w pgWorker) removeFutureJob(jobID int64) {
	if _, ok := w.futureJobs[jobID]; ok {
		w.mu.Lock()
		delete(w.futureJobs, jobID)
		w.mu.Unlock()
	}
}

// initFutureJobs is intended to be run once to initialize the list of future jobs that must be monitored for
// execution. it should be run only during system startup.
func (w pgWorker) initFutureJobs(ctx context.Context, queue string) {
	rows, err := w.pool.Query(context.Background(), FutureJobQuery, queue)
	if err != nil {
		w.logger.Error("error fetching future jobs list", err)
		return
	}

	var id int64
	var runAfter time.Time
	pgx.ForEachRow(rows, []any{&id, &runAfter}, func() error {
		w.mu.Lock()
		w.futureJobs[id] = runAfter
		w.mu.Unlock()
		return nil
	})
}

// scheduleFutureJobs announces future jobs using NOTIFY on an interval
func (w pgWorker) scheduleFutureJobs(ctx context.Context, queue string) {
	w.initFutureJobs(ctx, queue)

	// check for new future jobs on an interval
	// TODO make this time configurable
	ticker := time.NewTicker(5 * time.Second)

	for {
		// loop over list of future jobs, scheduling goroutines to wait for jobs that are due within the next 30 seconds
		// TODO: Make 30 seconds configurable
		for jobID, runAfter := range w.futureJobs {
			at := time.Until(runAfter)
			if at <= time.Duration(30*time.Second) {
				w.removeFutureJob(jobID)
				go func(jid int64) {
					scheduleCh := time.After(at)
					<-scheduleCh
					w.announceJob(ctx, queue, jid)
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

// annoucneJob announces jobs to queue listeners.
//
// Announced jobs are executed by the first worker to respond to the announcement.
func (w pgWorker) announceJob(ctx context.Context, queue string, jobID int64) {
	conn, err := w.pool.Acquire(ctx)
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
	defer tx.Rollback(ctx)

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

func (w pgWorker) pendingJobs(ctx context.Context, queue string) (jobsCh chan int64) {
	jobsCh = make(chan int64)

	// TODO Consider refactoring to use pgxpool.AcquireFunc()
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		w.logger.Error("failed to acquire database connection to listen for pending queue items", err)
		return
	}

	go func(ctx context.Context) {
		defer conn.Release()

		for {
			jobID, err := w.getPendingJobID(ctx, conn, queue)
			if err != nil {
				if err.Error() != "no rows in result set" {
					w.logger.Error("failed to fetch pending job", err, "job_id", jobID)
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
func (w pgWorker) handleJob(ctx context.Context, jobID int64, handler Handler) (err error) {
	var tx pgx.Tx
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release()

	tx, err = conn.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx) // rollback has no effect if the transaction has been committed

	ctxv := handlerCtxVars{tx: tx}
	var job Job
	job, err = w.getPendingJob(ctx, tx, jobID)
	if err != nil {
		return
	}

	ctxv.job = &job
	ctx = withHandlerContext(ctx, ctxv)

	// check if the job is being retried and increment retry count accordingly
	if job.Status != JobStatusNew {
		job.Retries = job.Retries + 1
	}

	// execute the queue handler of this job
	handlerErr := w.execHandler(ctx, handler)
	err = w.updateJob(ctx, handlerErr)
	if err != nil {
		err = errors.Wrap(err, "error updating job status")
		return
	}

	err = tx.Commit(ctx)
	if err != nil {
		w.logger.Error("unable to commit job transaction. retrying this job may dupliate work", err, "job_id", job.ID)
		err = errors.Wrap(err, "unable to commit job transaction. retrying this job may dupliate work")
	}

	return
}

// exechandler executes handler functions with a concrete time deadline
func (w pgWorker) execHandler(ctx context.Context, handler Handler) (err error) {
	deadlineCtx, cancel := context.WithDeadline(ctx, time.Now().Add(handler.deadline))
	defer cancel()

	var done = make(chan bool)
	go func(ctx context.Context) {
		err = handler.handle(ctx)
		done <- true
	}(ctx)

	select {
	case <-done:
		return
	case <-deadlineCtx.Done():
		err = fmt.Errorf("job exceeded its %s deadline", handler.deadline)
	}

	return
}

// listen uses Postgres LISTEN to listen for jobs on a queue
// TODO: There is currently no handling for listener disconnects. This will lead to jobs not getting processed until the
// worker is restarted. Implement disconnect handling.
func (w pgWorker) listen(ctx context.Context, queue string) (c chan int64) {
	var err error
	c = make(chan int64)

	// set this connection's idle in transaction timeout to infinite so it is not intermittently disconnected
	_, err = w.listenConn.Exec(ctx, fmt.Sprintf("SET idle_in_transaction_session_timeout = '0'; LISTEN %s", queue))
	if err != nil {
		w.logger.Error("unable to create database connection for listener", err)
		return
	}

	go func(ctx context.Context) {
		for {
			notification, waitErr := w.listenConn.WaitForNotification(ctx)
			if waitErr != nil {
				w.logger.Error("failed to wait for notification", waitErr)
				time.Sleep(1 * time.Second)
				continue
			}

			var jobID int64
			if jobID, err = strconv.ParseInt(notification.Payload, 0, 64); err != nil {
				w.logger.Error("unable to fetch job", err)
				continue
			}

			c <- jobID
		}
	}(ctx)

	return
}

func (w pgWorker) getPendingJob(ctx context.Context, tx pgx.Tx, jobID int64) (job Job, err error) {
	row, err := tx.Query(ctx, PendingJobQuery, jobID)
	if err != nil {
		return
	}

	var j *Job
	j, err = pgx.CollectOneRow(row, pgx.RowToAddrOfStructByName[Job])
	if err != nil {
		return
	}

	return *j, err
}

// calculateBackoff calculates the number of seconds to back off before the next retry
// this formula is unabashedly taken from Sidekiq because it is good.
func calculateBackoff(retryCount int) time.Time {
	p := int(math.Round(math.Pow(float64(retryCount), 4)))
	return time.Now().Add(time.Duration(p+15+randInt(30)*retryCount+1) * time.Second)
}

func randInt(max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max)
}

func (w pgWorker) getPendingJobID(ctx context.Context, conn *pgxpool.Conn, queue string) (jobID int64, err error) {
	err = conn.QueryRow(ctx, PendingJobIDQuery, queue).Scan(&jobID)
	return
}
