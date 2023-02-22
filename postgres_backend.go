package neoq

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
	"github.com/pkg/errors"
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
)

// PgBackend is a Postgres-based Neoq backend
type PgBackend struct {
	config     *pgConfig
	cron       *cron.Cron
	listenConn *pgx.Conn
	pool       *pgxpool.Pool
	handlers   map[string]Handler  // a map of queue names to queue handlers
	mu         *sync.Mutex         // mutext to protect mutating state on a pgWorker
	futureJobs map[int64]time.Time // map of future job IDs to their due time
	logger     Logger
}

// NewPgBackend creates a new neoq backend backed by Postgres
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
func NewPgBackend(ctx context.Context, connectString string, opts ...ConfigOption) (n Neoq, err error) {
	w := PgBackend{
		mu:         &sync.Mutex{},
		config:     &pgConfig{connectString: connectString},
		handlers:   make(map[string]Handler),
		futureJobs: make(map[int64]time.Time),
		logger:     slog.New(slog.NewTextHandler(os.Stdout)),
		cron:       cron.New(),
	}
	w.cron.Start()

	// Set all options
	for _, opt := range opts {
		opt(&w)
	}

	err = w.initializeDB(ctx)
	if err != nil {
		return
	}

	if w.pool == nil {
		var poolConfig *pgxpool.Config
		poolConfig, err = pgxpool.ParseConfig(w.config.connectString)
		if err != nil || w.config.connectString == "" {
			return nil, errors.New("invalid connecton string: see documentation for valid connection strings")
		}

		// ensure that workers don't consume connections with idle transactions
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) (err error) {
			var query string
			if w.config.idleTxTimeout > 0 {
				query = fmt.Sprintf("SET idle_in_transaction_session_timeout = '%dms'", w.config.idleTxTimeout)
			} else {
				// there is no limit to the amount of time a worker's transactions may be idle
				query = "SET idle_in_transaction_session_timeout = 0"
			}
			_, err = conn.Exec(ctx, query)
			return
		}

		w.pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return
		}
	}

	n = w

	return
}

// pgConfig configures NeoqPg's general behavior
type pgConfig struct {
	idleTxTimeout int    // time a worker's transaction may be idle before its connection is closed
	connectString string // postgres connect string / DSN
}

// initializeDB initializes the tables, types, and indices necessary to operate Neoq
func (w PgBackend) initializeDB(ctx context.Context) (err error) {
	var pgxCfg *pgx.ConnConfig
	var tx pgx.Tx
	pgxCfg, err = pgx.ParseConfig(w.config.connectString)
	if err != nil {
		return
	}

	connectStr := fmt.Sprintf("postgres://%s:%s@%s", pgxCfg.User, pgxCfg.Password, pgxCfg.Host)
	conn, err := pgx.Connect(ctx, connectStr)
	if err != nil {
		w.logger.Error("unableto connect to database", err)
		return
	}
	defer conn.Close(ctx)

	var dbExists bool
	dbExistsQ := fmt.Sprintf(`SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = '%s');`, pgxCfg.Database)
	rows, err := conn.Query(ctx, dbExistsQ)
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
	conn, err = pgx.Connect(ctx, connectStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	if !dbExists {
		createDBQ := fmt.Sprintf("CREATE DATABASE %s", pgxCfg.Database)
		_, err := conn.Exec(ctx, createDBQ)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to create neoq database: %v", err)
			return err
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
	defer tx.Rollback(ctx) // rollback has no effect if the transaction has been committed

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
			fmt.Fprintf(os.Stderr, "unable to create job status enum: %v", err)
			return err
		}

		err = tx.Commit(ctx)
	}

	return
}

// Enqueue adds jobs to the specified queue
func (w PgBackend) Enqueue(ctx context.Context, job Job) (jobID int64, err error) {
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
// Neoq will not process any queues until Listen() is called at least once.
func (w PgBackend) Listen(ctx context.Context, queue string, h Handler) (err error) {
	w.mu.Lock()
	w.handlers[queue] = h
	w.mu.Unlock()

	err = w.start(ctx, queue)
	if err != nil {
		return
	}
	return
}

// ListenCron listens for jobs on a cron schedule and handles them with the provided handler
//
// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
func (w PgBackend) ListenCron(ctx context.Context, cronSpec string, h Handler) (err error) {
	cd, err := crondescriptor.NewCronDescriptor(cronSpec)
	if err != nil {
		return err
	}

	cdStr, err := cd.GetDescription(crondescriptor.Full)
	if err != nil {
		return err
	}

	queue := stripNonAlphanum(strcase.ToSnake(*cdStr))
	w.cron.AddFunc(cronSpec, func() {
		w.Enqueue(ctx, Job{Queue: queue})
	})

	err = w.Listen(ctx, queue, h)

	return
}

func (w PgBackend) Shutdown(ctx context.Context) (err error) {
	w.pool.Close()
	w.cron.Stop()

	err = w.listenConn.Close(ctx)

	return
}

func (w PgBackend) WithConfig(opt ConfigOption) Neoq {
	opt(&w)
	return &w
}

// enqueueJob adds jobs to the queue, returning the job ID
//
// Jobs that are not already fingerprinted are fingerprinted before being added
// Duplicate jobs are not added to the queue. Any two unprocessed jobs with the same fingerprint are duplicates
func (w PgBackend) enqueueJob(ctx context.Context, tx pgx.Tx, j Job) (jobID int64, err error) {

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
func (w PgBackend) moveToDeadQueue(ctx context.Context, tx pgx.Tx, j *Job, jobErr error) (err error) {
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
func (w PgBackend) updateJob(ctx context.Context, jobErr error) (err error) {
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
func (w PgBackend) start(ctx context.Context, queue string) (err error) {
	var handler Handler
	var ok bool
	if handler, ok = w.handlers[queue]; !ok {
		return fmt.Errorf("no handler for queue: %s", queue)
	}
	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return
	}

	// use a single connection to listen for jobs on all queues
	// TODO: Give more thought to the implications of hijacking a conn from the pool here
	// should this connect not come from the pool, to avoid tainting it with connections that don't have an idle in
	// transaction time out set?
	w.mu.Lock()
	if w.listenConn == nil {
		w.listenConn = conn.Hijack()
	}
	w.mu.Unlock()

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
					if errors.Is(err, pgx.ErrNoRows) {
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
func (w PgBackend) removeFutureJob(jobID int64) {
	if _, ok := w.futureJobs[jobID]; ok {
		w.mu.Lock()
		delete(w.futureJobs, jobID)
		w.mu.Unlock()
	}
}

// initFutureJobs is intended to be run once to initialize the list of future jobs that must be monitored for
// execution. it should be run only during system startup.
func (w PgBackend) initFutureJobs(ctx context.Context, queue string) {
	rows, err := w.pool.Query(ctx, FutureJobQuery, queue)
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
func (w PgBackend) scheduleFutureJobs(ctx context.Context, queue string) {
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

// announceJob announces jobs to queue listeners.
//
// Announced jobs are executed by the first worker to respond to the announcement.
func (w PgBackend) announceJob(ctx context.Context, queue string, jobID int64) {
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

func (w PgBackend) pendingJobs(ctx context.Context, queue string) (jobsCh chan int64) {
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
				if !errors.Is(err, pgx.ErrNoRows) {
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
func (w PgBackend) handleJob(ctx context.Context, jobID int64, handler Handler) (err error) {
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
	handlerErr := execHandler(ctx, handler)
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

// listen uses Postgres LISTEN to listen for jobs on a queue
// TODO: There is currently no handling for listener disconnects. This will lead to jobs not getting processed until the
// worker is restarted. Implement disconnect handling.
func (w PgBackend) listen(ctx context.Context, queue string) (c chan int64) {
	var err error
	c = make(chan int64)

	// set this connection's idle in transaction timeout to infinite so it is not intermittently disconnected
	_, err = w.listenConn.Exec(ctx, fmt.Sprintf("SET idle_in_transaction_session_timeout = '0'; LISTEN %s", queue))
	if err != nil {
		msg := "unable to create database connection for listener"
		err = errors.Wrap(err, msg)
		w.logger.Error(msg, err)
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

func (w PgBackend) getPendingJob(ctx context.Context, tx pgx.Tx, jobID int64) (job Job, err error) {
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

func (w PgBackend) getPendingJobID(ctx context.Context, conn *pgxpool.Conn, queue string) (jobID int64, err error) {
	err = conn.QueryRow(ctx, PendingJobIDQuery, queue).Scan(&jobID)
	return
}

// PgTransactionTimeout sets the time that NeoqPg's transactions may be idle before its underlying connection is
// closed
// The timeout is the number of milliseconds that a transaction may sit idle before postgres terminates the
// transaction's underlying connection. The timeout should be longer than your longest job takes to complete. If set
// too short, job state will become unpredictable, e.g. retry counts may become incorrect.
//
// PgTransactionTimeout is best set when calling neoq.New() rather than after creation using WithConfig() because this
// setting results in the creation of a new database connection pool.
func PgTransactionTimeout(txTimeout int) ConfigOption {
	return func(n Neoq) {
		var ok bool
		var npg PgBackend

		if npg, ok = n.(PgBackend); !ok {
			return
		}

		npg.config.idleTxTimeout = txTimeout

		// if the worker already has a pool configured, then setting the transaction timeout prints a warning and is a
		// no-op
		if npg.pool != nil {
			log.Println("create a new Neoq instance to set transaction timeout")
			return
		}

		var err error
		var poolConfig *pgxpool.Config

		poolConfig, err = pgxpool.ParseConfig(npg.config.connectString)
		if err != nil || npg.config.connectString == "" {
			return
		}

		// ensure that workers don't consume connections with idle transactions
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) (err error) {
			var query string
			if npg.config.idleTxTimeout > 0 {
				query = fmt.Sprintf("SET idle_in_transaction_session_timeout = '%dms'", npg.config.idleTxTimeout)
			} else {
				// there is no limit to the amount of time a worker's transactions may be idle
				query = "SET idle_in_transaction_session_timeout = 0"
			}
			_, err = conn.Exec(ctx, query)
			return
		}

		npg.pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err != nil {
			return
		}
	}
}

func stripNonAlphanum(s string) string {
	var result strings.Builder
	for i := 0; i < len(s); i++ {
		b := s[i]
		if (b == '_') ||
			('a' <= b && b <= 'z') ||
			('A' <= b && b <= 'Z') ||
			('0' <= b && b <= '9') ||
			b == ' ' {
			result.WriteByte(b)
		}
	}
	return result.String()
}
