// Package neoq provides background job processing for Go applications.
//
// Neoq's goal is to minimize the infrastructure necessary to add background job processing to Go applications. It does so by implementing queue durability with modular backends, rather than introducing a strict dependency on a particular backend such as Redis.
//
// A Postgres backend is provided out of a box. However, for Neoq to meet its goal of reducing the infrastructure
// necessary to run background jobs -- additional backends are necessary. E.g. Applications that use MySQL, MonogoDB, or
// Redis as their primary data stores will ideally use Neoq with corresponding backends.
package neoq

// TODO dependencies to factor out
// "github.com/iancoleman/strcase"
// "github.com/jsuar/go-cron-descriptor/pkg/crondescriptor"
import (
	"context"
	"math"
	"runtime"
	"strings"
	"time"

	"math/rand"

	"github.com/guregu/null"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

type contextKey int

var varsKey contextKey

const (
	JobStatusNew       = "new"
	JobStatusProcessed = "processed"
	JobStatusFailed    = "failed"

	DefaultPgConnectionString = "postgres://postgres:postgres@127.0.0.1:5432/neoq"
	DefaultTransactionTimeout = time.Minute
	DefaultHandlerDeadline    = 30 * time.Second
	DuplicateJobID            = -1
	postgresBackendName       = "postgres"
)

// Neoq interface is Neoq's primary API
type Neoq interface {
	// Enqueue queues jobs to be executed asynchronously
	Enqueue(ctx context.Context, job Job) (jobID int64, err error)

	// Listen listens for jobs on a queue and processes them with the given handler
	Listen(ctx context.Context, queue string, h Handler) (err error)

	// ListenCron listens for jobs on a cron schedule and processes them with the given handler
	//
	// See: https://pkg.go.dev/github.com/robfig/cron?#hdr-CRON_Expression_Format for details on the cron spec format
	ListenCron(ctx context.Context, cron string, h Handler) (err error)

	// Shutdown halts the worker
	Shutdown(ctx context.Context) error

	// WithConfigOpt configures neoq with with optional configuration
	WithConfigOpt(opt ConfigOption) Neoq
}

// Logger interface is the interface that neoq's logger must implement
//
// This interface is a subset of [slog.Logger]. The slog interface was chosen under the assumption that its
// likely to be Golang's standard library logging interface.
//
// TODO: Add WithLoggerOpt() for user-supplied logger configuration
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, err error, args ...any)
	Info(msg string, args ...any)
}

// HandlerFunc is a function that Handlers execute for every Job on a queue
type HandlerFunc func(ctx context.Context) error

// Handler handles jobs on a queue
type Handler struct {
	handle      HandlerFunc
	concurrency int
	deadline    time.Duration
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
		h.deadline = DefaultHandlerDeadline
	}

	return
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

// New creates a new Neoq instance for listening to queues and enqueing new jobs
func New(ctx context.Context, opts ...ConfigOption) (n Neoq, err error) {
	ic := internalConfig{}
	for _, opt := range opts {
		opt(&ic)
	}

	if ic.backend != nil {
		n = *ic.backend
		return
	}

	switch ic.backendName {
	case postgresBackendName:
		if ic.connectionString == "" {
			err = errors.New("your must provide a postgres connection string to initialize the postgres backend: see neoq.ConnectionString(...)")
			return
		}
		n, err = NewPgBackend(ctx, ic.connectionString, opts...)
	// TODO make the default an in-memory backend
	default:
		if ic.connectionString == "" {
			ic.connectionString = DefaultPgConnectionString
		}
		n, err = NewPgBackend(ctx, ic.connectionString, opts...)
	}

	return
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

// BackendName is a configuration option that instructs neoq to create a new backend with the given name upon
// initialization
func BackendName(backendName string) ConfigOption {
	return func(n Neoq) {
		switch c := n.(type) {
		case *internalConfig:
			c.backendName = backendName
		default:
		}
	}
}

// Backend is a configuration option that instructs neoq to use the specified backend rather than initializing a new one
// during initialization
func Backend(backend Neoq) ConfigOption {
	return func(n Neoq) {
		switch c := n.(type) {
		case *internalConfig:
			c.backend = &backend
		default:
		}
	}
}

// ConnectionString is a configuration option that sets a connection string for backend initialization:w
func ConnectionString(connectionString string) ConfigOption {
	return func(n Neoq) {
		switch c := n.(type) {
		case *internalConfig:
			c.connectionString = connectionString
		case *PgBackend:
			c.config.connectString = connectionString
		default:
		}
	}
}

// handlerCtxVars are variables passed to every Handler context
type handlerCtxVars struct {
	job *Job
	tx  pgx.Tx
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

// internalConfig models internal neoq configuratio not exposed to users
type internalConfig struct {
	backendName      string // the name of a known backend
	backend          *Neoq  // user-provided backend to use
	connectionString string // a connection string to use connecting to a backend
}

func (i internalConfig) Enqueue(ctx context.Context, job Job) (jobID int64, err error) {
	return
}
func (i internalConfig) Listen(ctx context.Context, queue string, h Handler) (err error) {
	return
}
func (i internalConfig) ListenCron(ctx context.Context, cron string, h Handler) (err error) {
	return
}

func (i internalConfig) Shutdown(ctx context.Context) (err error) {
	return
}

func (i internalConfig) WithConfigOpt(opt ConfigOption) (n Neoq) {
	return
}
