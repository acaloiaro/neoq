package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/pranavmodx/neoq-sqlite"
	"github.com/pranavmodx/neoq-sqlite/handler"
	"github.com/pranavmodx/neoq-sqlite/internal"
	"github.com/pranavmodx/neoq-sqlite/jobs"
	"github.com/pranavmodx/neoq-sqlite/logging"
	"github.com/robfig/cron"
	"golang.org/x/exp/slog"
)

//go:embed migrations/*.sql
var sqliteMigrationsFS embed.FS

type contextKey struct{}

const (
	JobQuery = `SELECT id,fingerprint,queue,status,deadline,payload,retries,max_retries,run_after,ran_at,created_at,error
					FROM neoq_jobs
					WHERE id = $1
					AND status != "processed"`
	PendingJobIDQuery = `SELECT id
					FROM neoq_jobs
					WHERE queue = $1
					AND status != "processed"
					AND run_after <= datetime("now")
					LIMIT 1`
	AllPendingJobIDQuery = `SELECT id
					FROM neoq_jobs
					WHERE queue = $1
					AND status != "processed"
					AND run_after < datetime("now")`
	FutureJobQuery = `SELECT id,fingerprint,queue,status,deadline,payload,retries,max_retries,run_after,ran_at,created_at,error
					FROM neoq_jobs
					WHERE queue = $1
					AND status != "processed"
					AND run_after > datetime("now")
					ORDER BY run_after ASC
					LIMIT 100`
	shutdownJobID = "-1" // job ID announced when triggering a shutdown
)

var (
	txCtxVarKey               contextKey
	ErrNoTransactionInContext = errors.New("context does not have a Tx set")
)

type SqliteBackend struct {
	neoq.Neoq
	cancelFuncs       []context.CancelFunc       // cancel functions to be called upon Shutdown()
	config            *neoq.Config               // backend configuration
	cron              *cron.Cron                 // scheduler for periodic jobs
	futureJobs        map[string]*jobs.Job       // map of future job IDs to the corresponding job record
	handlers          map[string]handler.Handler // a map of queue names to queue handlers
	newQueues         chan string                // a channel that indicates that new queues are ready to be processed
	readyQueues       chan string                // a channel that indicates which queues are ready to have jobs processed.
	queueListenerChan map[string]chan string     // PS::each queue has a listener channel to process enqueued jobs
	logger            logging.Logger             // backend-wide logger
	mu                *sync.RWMutex              // protects concurrent access to fields on SqliteBackend
	mu2               *sync.RWMutex              // protects concurrent access to fields on SqliteBackend
	db                *sql.DB                    // connection to sqlite for backend, used to process and enqueue jobs
}

func Backend(ctx context.Context, opts ...neoq.ConfigOption) (sb neoq.Neoq, err error) {
	cfg := neoq.NewConfig()
	cfg.IdleTransactionTimeout = neoq.DefaultIdleTxTimeout

	s := &SqliteBackend{
		cancelFuncs:       []context.CancelFunc{},
		config:            cfg,
		cron:              cron.New(),
		futureJobs:        make(map[string]*jobs.Job),
		handlers:          make(map[string]handler.Handler),
		newQueues:         make(chan string),
		readyQueues:       make(chan string),
		queueListenerChan: make(map[string]chan string),
		mu:                &sync.RWMutex{},
		mu2:               &sync.RWMutex{},
	}

	// Set all options
	for _, opt := range opts {
		opt(s.config)
	}

	s.logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: s.config.LogLevel}))
	ctx, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.cancelFuncs = append(s.cancelFuncs, cancel)
	s.mu.Unlock()

	err = s.initializeDB()
	if err != nil {
		return nil, fmt.Errorf("unable to initialize jobs database: %w", err)
	}

	// monitor handlers for changes and LISTEN when new queues are added
	go s.newQueueMonitor(ctx)

	s.cron.Start()

	sb = s

	return sb, nil
}

func (s *SqliteBackend) initializeDB() (err error) {
	log.Println("db path", s.config.ConnectionString)
	log.Println("migration path", sqliteMigrationsFS)
	file, _ := sqliteMigrationsFS.ReadFile("33_create_db_tables.up.sql")
	log.Println(file)
	migrations, err := iofs.New(sqliteMigrationsFS, "migrations")
	if err != nil {
		err = fmt.Errorf("unable to run migrations, error during iofs new: %w", err)
		s.logger.Error("unable to run migrations", slog.Any("error", err))
		return
	}
	log.Println("got migration files", migrations)

	m, err := migrate.NewWithSourceInstance("iofs", migrations, s.config.ConnectionString)
	if err != nil {
		err = fmt.Errorf("unable to run migrations, could not create new source: %w", err)
		s.logger.Error("unable to run migrations", slog.Any("error", err))
		return
	}

	// We don't need the migration tooling to hold it's connections to the DB once it has been completed.
	defer m.Close()
	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		err = fmt.Errorf("unable to run migrations, could not apply up migration: %w", err)
		s.logger.Error("unable to run migrations", slog.Any("error", err))
		return
	}

	dbURI := strings.Split(s.config.ConnectionString, "/")
	dbPath := strings.Join(dbURI[1:], "/")

	workDir, _ := os.Getwd()
	s.db, err = sql.Open("sqlite3", workDir+dbPath)
	if err != nil {
		s.logger.Error("unable to set db connection")
		return
	}

	return nil
}

func (s *SqliteBackend) newQueueMonitor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case newQueue := <-s.newQueues:
			s.logger.Debug("configure new handler", "queue", newQueue)
			s.logger.Debug("listening on queue", "queue", newQueue)
			s.readyQueues <- newQueue
		}
	}
}

// Enqueue adds jobs to the specified queue
func (s *SqliteBackend) Enqueue(ctx context.Context, job *jobs.Job) (jobID string, err error) {
	if job.Queue == "" {
		err = jobs.ErrNoQueueSpecified
		return
	}

	s.logger.Debug("enqueueing job payload", slog.String("queue", job.Queue), slog.Any("job_payload", job.Payload2))

	// s.logger.Debug("acquiring new connection from connection pool", slog.String("queue", job.Queue))
	// conn, err := s.acquire(ctx)
	// if err != nil {
	// 	err = fmt.Errorf("error acquiring connection: %w", err)
	// 	return
	// }
	// defer conn.Release()

	// PS::lock here
	s.mu.Lock()

	log.Println("PS::Enqueue acquired lock for access to db")

	s.logger.Debug("beginning new transaction to enqueue job", slog.String("queue", job.Queue))
	tx, err := s.db.Begin()
	// s.logger.Debug("PS::tx", tx)
	if err != nil {
		err = fmt.Errorf("error creating transaction: %w", err)
		s.logger.Debug(err.Error())
		s.mu.Unlock()
		return
	}

	// // Rollback is safe to call even if the tx is already closed, so if
	// // the tx commits successfully, this is a no-op
	defer func(ctx context.Context) { _ = tx.Rollback() }(ctx) // rollback has no effect if the transaction has been committed
	jobID, err = s.enqueueJob(ctx, tx, job)
	if err != nil {
		s.logger.Error("error enqueueing job", slog.String("queue", job.Queue), slog.Any("error", err))
		err = fmt.Errorf("error enqueuing job: %w", err)
		s.mu.Unlock()
		return
	}

	err = tx.Commit()
	if err != nil {
		err = fmt.Errorf("error committing transaction: %w", err)
		s.mu.Unlock()
		return
	}
	s.logger.Debug("job added to queue:", slog.String("queue", job.Queue), slog.String("job_id", jobID))

	s.mu.Unlock()
	log.Println("PS::Enqueue released lock", jobID)
	log.Println("PS::before send to channel")

	// PS::send to channel
	// go func() {
	// log.Println("PS::assigning job id to queue listener")
	s.queueListenerChan[job.Queue] <- jobID
	// log.Println("PS::done assigning job id to queue listener")
	// }()

	// // add future jobs to the future job list
	// if job.RunAfter.After(time.Now().UTC()) {
	// 	s.mu.Lock()
	// 	s.futureJobs[jobID] = job
	// 	s.mu.Unlock()
	// 	s.logger.Debug(
	// 		"added job to future jobs list",
	// 		slog.String("queue", job.Queue),
	// 		slog.String("job_id", jobID),
	// 		slog.Time("run_after", job.RunAfter),
	// 	)
	// }

	fmt.Println("PS::after send to channel")

	return jobID, nil
}

// Start starts processing jobs with the specified queue and handler
func (s *SqliteBackend) Start(ctx context.Context, h handler.Handler) (err error) {
	ctx, cancel := context.WithCancel(ctx)

	s.logger.Debug("starting job processing", slog.String("queue", h.Queue))
	s.mu2.Lock()
	s.cancelFuncs = append(s.cancelFuncs, cancel)
	s.handlers[h.Queue] = h
	s.mu2.Unlock()

	s.newQueues <- h.Queue
	s.queueListenerChan[h.Queue] = make(chan string, 1000) // PS

	err = s.start(ctx, h)
	if err != nil {
		s.logger.Error("unable to start processing queue", slog.String("queue", h.Queue), slog.Any("error", err))
		return
	}
	return
}

// start starts processing new, pending, and future jobs
func (s *SqliteBackend) start(ctx context.Context, h handler.Handler) (err error) {
	// s.logger.Debug("PS::at start()")
	log.Println("PS::concurrency set to", h.Concurrency)
	var ok bool
	// var listenJobChan chan string
	var errCh chan error

	if h, ok = s.handlers[h.Queue]; !ok {
		return fmt.Errorf("%w: %s", handler.ErrNoHandlerForQueue, h.Queue)
	}

	// pendingJobsChan := s.pendingJobs(ctx, h.Queue) // process overdue jobs *at startup*
	pendingJobsChan := s.allPendingJobs(ctx, h.Queue) // process overdue jobs *at startup*

	// wait for the listener to be ready to listen
	for q := range s.readyQueues {
		if q == h.Queue {
			// listenJobChan, errCh = s.listen(ctx, q) // PS::instead of pg notify listener, we should probably poll for jobs

			break
		}

		s.logger.Debug("Picked up a queue that a different start() will be waiting for. Adding back to ready list",
			slog.String("queue", q))
		s.readyQueues <- q
	}

	// process all future jobs and retries
	go func() { s.scheduleFutureJobs(ctx, h.Queue) }()

	for i := 0; i < h.Concurrency; i++ {
		go func() {
			var err error
			var n string

			for {
				select {
				// case n = <-listenJobChan:
				case n = <-s.queueListenerChan[h.Queue]:
					s.logger.Debug("PS::received from listener channel")
					err = s.handleJob(ctx, n)
				case n = <-pendingJobsChan:
					s.logger.Debug("PS::received from pending channel")
					err = s.handleJob(ctx, n)
				case <-ctx.Done():
					return
				case <-errCh:
					s.logger.Error("error hanlding job", "error", err)
					continue
				}

				if err != nil {
					// if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, context.Canceled) {
					// 	err = nil
					// 	continue
					// }

					log.Println("PS::err here!!!")

					s.logger.Error(
						"job failed",
						slog.String("queue", h.Queue),
						slog.Any("error", err),
						slog.String("job_id", n),
					)

					continue
				}
			}
		}()
	}

	return nil
}

func (s *SqliteBackend) pendingJobs(ctx context.Context, queue string) (jobsCh chan string) {
	jobsCh = make(chan string)

	go func(ctx context.Context) {
		for {
			jobID, err := s.getPendingJobID(ctx, queue)
			log.Println("PS::pending job id", jobID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) || errors.Is(err, context.Canceled) {
					break
				}

				s.logger.Error(
					"failed to fetch pending job",
					slog.String("queue", queue),
					slog.Any("error", err),
					slog.String("job_id", jobID),
				)
			} else {
				jobsCh <- jobID
			}
		}
	}(ctx)

	return jobsCh
}

func (s *SqliteBackend) allPendingJobs(ctx context.Context, queue string) (jobsCh chan string) {
	jobsCh = make(chan string)

	go func(ctx context.Context) {
		jobIDs, err := s.getAllPendingJobIDs(ctx, queue)
		log.Println("PS::all pending job ids", jobIDs)
		if err != nil {
			log.Println("PS::err getting pending jobs", err)
		}
		for _, jobID := range jobIDs {
			// log.Println("PS::pending job id", jobID)
			jobsCh <- jobID
		}
	}(ctx)

	return jobsCh
}

func (s *SqliteBackend) getPendingJobID(ctx context.Context, queue string) (jobID string, err error) {
	// s.mu.Lock()
	// defer s.mu.Unlock()
	// conn, _ := s.db.Conn(ctx)
	err = s.db.QueryRowContext(ctx, PendingJobIDQuery, queue).Scan(&jobID)
	return
}

func (s *SqliteBackend) getAllPendingJobIDs(ctx context.Context, queue string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, AllPendingJobIDQuery, queue)
	if err != nil {
		return []string{}, err
	}
	jobIDs := []string{}
	for rows.Next() {
		var jobID string
		err := rows.Scan(&jobID)
		if err != nil {
			log.Println("PS::some err with getting pending job row", err)
			return jobIDs, nil
		}
		jobIDs = append(jobIDs, jobID)
	}

	return jobIDs, nil
}

func (s *SqliteBackend) listen(ctx context.Context, queue string) (c chan string, errCh chan error) {
	c = make(chan string)
	errCh = make(chan error)

	// go func(ctx context.Context) {
	// 	// var notification string
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			// our context has been canceled, the system is shutting down
	// 			return
	// 		default:
	// 			// notification = <-s.queueListenerChan[queue]
	// 		}

	// 		// s.logger.Debug(
	// 		// 	"job notification for queue",
	// 		// 	slog.Any("notification", notification),
	// 		// )

	// 		// // check if Shutdown() has been called
	// 		// if notification.Payload2 == shutdownJobID {
	// 		// 	return
	// 		// }

	// 		// c <- notification
	// 	}
	// }(ctx)

	return c, errCh
}

func (s *SqliteBackend) handleJob(ctx context.Context, jobID string) (err error) {
	log.Println("PS::handleJob start", jobID)
	// log.Println("PS::context-jobid", jobID, ctx.Value(jobID), ctx)

	job, err := s.getJobWithoutTx(ctx, jobID)
	if err != nil {
		log.Println("PS::could not retrieve row with jobid", jobID)
		return
	}

	// check if the job is being retried and increment retry count accordingly
	if job.Status != internal.JobStatusNew {
		job.Retries++
	}

	var jobErr error
	h, ok := s.handlers[job.Queue]
	if !ok {
		s.logger.Error("received a job for which no handler is configured",
			slog.String("queue", job.Queue),
			slog.Int64("job_id", job.ID))
		return handler.ErrNoHandlerForQueue
	}

	// PS::understand ctx in detail later
	ctx = withJobContext(ctx, job)

	// execute the queue handler of this job
	jobErr = handler.Exec(ctx, h)
	log.Println("PS::handlejob jobErr variable val", jobErr)

	var tx *sql.Tx

	// PS::lock here
	s.mu.Lock()
	defer func() {
		log.Println("PS::job id", jobID, "released lock")
		s.mu.Unlock()
	}()

	log.Println("PS::job id", jobID, "acquired lock to access db (in handleJob)")

	tx, err = s.db.Begin()
	if err != nil {
		log.Println("PS::acquiring tx in handleJob failed, returning")
		return
	}
	defer func() { _ = tx.Rollback() }() // rollback has no effect if the transaction has been committed

	ctx = context.WithValue(ctx, txCtxVarKey, tx)
	// log.Println("PS::set ctx with job", ctx, ctx.Value(internal.JobCtxVarKey))

	err = s.updateJob(ctx, jobErr)
	if err != nil {
		log.Println("PS::hitting here!")
		if errors.Is(err, context.Canceled) {
			return
		}

		err = fmt.Errorf("error updating job status: %w", err)
		return err
	}

	log.Println("PS::job id", jobID, "done updating also!!!")

	err = tx.Commit()
	if err != nil {
		errMsg := "unable to commit job transaction. retrying this job may dupliate work:"
		s.logger.Error(errMsg, slog.String("queue", h.Queue), slog.Any("error", err), slog.Int64("job_id", job.ID))
		return fmt.Errorf("%s %w", errMsg, err)
	}

	log.Println("PS::handleJob end - committed to db - released lock", jobID)
	// log.Println("PS::context-jobid", jobID, ctx.Value(jobID), ctx)

	return nil
}

func (s *SqliteBackend) getJob(ctx context.Context, tx *sql.Tx, jobID string) (job *jobs.Job, err error) {
	// s.mu.Lock()
	// defer s.mu.Unlock()
	job = &jobs.Job{}
	// log.Println("PS::query job id", jobID)
	row := tx.QueryRowContext(ctx, JobQuery, jobID)
	err = row.Scan(
		&job.ID, &job.Fingerprint, &job.Queue, &job.Status,
		&job.Deadline, &job.Payload2, &job.Retries, &job.MaxRetries,
		&job.RunAfter, &job.RanAt, &job.CreatedAt, &job.Error,
	)
	// id,fingerprint,queue,status,deadline,payload,retries,max_retries,run_after,ran_at,created_at,error
	if err != nil {
		return
	}
	return
}

func (s *SqliteBackend) getJobWithoutTx(ctx context.Context, jobID string) (job *jobs.Job, err error) {
	// s.mu.Lock()
	// defer s.mu.Unlock()
	job = &jobs.Job{}
	// log.Println("PS::query job id", jobID)
	row := s.db.QueryRowContext(ctx, JobQuery, jobID)
	err = row.Scan(
		&job.ID, &job.Fingerprint, &job.Queue, &job.Status,
		&job.Deadline, &job.Payload2, &job.Retries, &job.MaxRetries,
		&job.RunAfter, &job.RanAt, &job.CreatedAt, &job.Error,
	)
	// id,fingerprint,queue,status,deadline,payload,retries,max_retries,run_after,ran_at,created_at,error
	if err != nil {
		return
	}
	return
}

// withJobContext creates a new context with the Job set
func withJobContext(ctx context.Context, j *jobs.Job) context.Context {
	return context.WithValue(ctx, internal.JobCtxVarKey, j)
}

func (s *SqliteBackend) updateJob(ctx context.Context, jobErr error) (err error) {
	log.Println("PS::entered updateJob")
	status := internal.JobStatusProcessed
	errMsg := ""

	if jobErr != nil {
		s.logger.Error("job failed", slog.Any("job_error", jobErr))
		status = internal.JobStatusFailed
		errMsg = jobErr.Error()
	}

	var job *jobs.Job
	if job, err = jobs.FromContext(ctx); err != nil {
		return fmt.Errorf("error getting job from context: %w", err)
	}

	var tx *sql.Tx
	if tx, err = txFromContext(ctx); err != nil {
		return
	}

	if job.MaxRetries != nil && job.Retries >= *job.MaxRetries {
		err = s.moveToDeadQueue(ctx, job, errMsg)
		return
	}

	var runAfter time.Time
	if status == internal.JobStatusFailed {
		runAfter = internal.CalculateBackoff(job.Retries)
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3, retries = $4, run_after = $5 WHERE id = $6"
		_, err = tx.Exec(qstr, time.Now().UTC(), errMsg, status, job.Retries, runAfter, job.ID)
	} else {
		qstr := "UPDATE neoq_jobs SET ran_at = $1, error = $2, status = $3 WHERE id = $4"
		_, err = tx.Exec(qstr, time.Now().UTC(), errMsg, status, job.ID)
	}

	if err != nil {
		return
	}

	if time.Until(runAfter) > 0 {
		s.mu2.Lock()
		s.futureJobs[fmt.Sprint(job.ID)] = job
		s.mu2.Unlock()
	}

	return nil
}

// txFromContext gets the transaction from a context, if the transaction is already set
func txFromContext(ctx context.Context) (t *sql.Tx, err error) {
	var ok bool
	if t, ok = ctx.Value(txCtxVarKey).(*sql.Tx); ok {
		return
	}

	err = ErrNoTransactionInContext

	return
}

func (s *SqliteBackend) moveToDeadQueue(ctx context.Context, j *jobs.Job, jobErr string) (err error) {
	// _, err = tx.Exec(ctx, "DELETE FROM neoq_jobs WHERE id = $1", j.ID)
	// if err != nil {
	// 	return
	// }

	// _, err = tx.Exec(ctx, `INSERT INTO neoq_dead_jobs(id, queue, fingerprint, payload, retries, max_retries, error, deadline)
	// 	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
	// 	j.ID, j.Queue, j.Fingerprint, j.Payload2, j.Retries, j.MaxRetries, jobErr, j.Deadline)

	return
}

func (s *SqliteBackend) scheduleFutureJobs(ctx context.Context, queue string) {
	err := s.initFutureJobs(ctx, queue)
	if err != nil {
		return
	}

	// check for new future jobs on an interval
	ticker := time.NewTicker(s.config.JobCheckInterval)

	for {
		// loop over list of future jobs, scheduling goroutines to wait for jobs that are due within the next 30 seconds
		s.mu2.Lock()
		for jobID, job := range s.futureJobs {
			timeUntillRunAfter := time.Until(job.RunAfter)
			if timeUntillRunAfter <= s.config.FutureJobWindow {
				delete(s.futureJobs, jobID)
				go func(jid string, j *jobs.Job) {
					scheduleCh := time.After(timeUntillRunAfter)
					<-scheduleCh
					// s.announceJob(ctx, j.Queue, jid)
					s.queueListenerChan[j.Queue] <- jid
				}(jobID, job)
			}
		}
		s.mu2.Unlock()

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (s *SqliteBackend) initFutureJobs(ctx context.Context, queue string) (err error) {
	// rows, err := s.pool.Query(ctx, FutureJobQuery, queue)
	// if err != nil {
	// 	s.logger.Error("failed to fetch future jobs list", slog.String("queue", queue), slog.Any("error", err))
	// 	return
	// }

	// futureJobs, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[jobs.Job])
	// if err != nil {
	// 	return
	// }

	// for _, job := range futureJobs {
	// 	s.mu.Lock()
	// 	s.futureJobs[fmt.Sprintf("%d", job.ID)] = job
	// 	s.mu.Unlock()
	// }

	return
}

func WithConnectionString(connectionString string) neoq.ConfigOption {
	return func(c *neoq.Config) {
		c.ConnectionString = connectionString
	}
}

func (s *SqliteBackend) StartCron(ctx context.Context, cronSpec string, h handler.Handler) (err error) {
	return nil
}

// SetLogger sets this backend's logger
func (s *SqliteBackend) SetLogger(logger logging.Logger) {
	s.logger = logger
}

// Shutdown shuts this backend down
func (s *SqliteBackend) Shutdown(ctx context.Context) {
	s.logger.Debug("starting shutdown")
	// for queue := range s.handlers {

	// }

	for _, f := range s.cancelFuncs {
		f()
	}

	s.cron.Stop()

	s.cancelFuncs = nil
	s.logger.Debug("shutdown complete")
}

func (s *SqliteBackend) enqueueJob(ctx context.Context, tx *sql.Tx, j *jobs.Job) (jobID string, err error) {
	// s.logger.Debug("PS::entered enqueueJob")

	err = jobs.FingerprintJob(j)
	if err != nil {
		return
	}

	s.logger.Debug("adding job to the queue", slog.String("queue", j.Queue))
	err = tx.QueryRow(`INSERT INTO neoq_jobs(queue, fingerprint, payload, run_after, deadline, max_retries)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		j.Queue, j.Fingerprint, j.Payload2, j.RunAfter, j.Deadline, j.MaxRetries).Scan(&jobID)
	// j.Queue, j.Fingerprint, j.Payload2["message"].(string), j.RunAfter, j.Deadline, j.MaxRetries).Scan(&jobID)
	// err = tx.QueryRow(`INSERT INTO neoq_jobs(id, queue, fingerprint, payload, run_after, deadline, max_retries)
	// 	VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
	// 	1, j.Queue, j.Fingerprint, j.Payload2["message"].(string), j.RunAfter, j.Deadline, j.MaxRetries).Scan(&jobID)
	if err != nil {
		err = fmt.Errorf("unable add job to queue: %w", err)
		return
	}

	return jobID, err
}
