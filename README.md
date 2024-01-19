# Neoq

Queue-agnostic background job library for Go, with a pleasant API and powerful features.

[![Go Reference](https://pkg.go.dev/badge/github.com/acaloiaro/neoq.svg)](https://pkg.go.dev/github.com/acaloiaro/neoq) [Matrix Chat <img src="https://img.shields.io/matrix/neoq%3Amatrix.org">](https://matrix.to/#/%23neoq:matrix.org)

# Getting Started 

See the [Getting Started](https://github.com/acaloiaro/neoq/wiki/Getting-Started) wiki to get started. 

# About

Neoq is a queue-agnostic background job library for Go, with a pleasant API and powerful features.

Queue-agnostic means that whether you're using an in-memory queue for developing and testing, or Postgres or Redis queue in production -- your job processing code doesn't change. Job handlers are agnostic to the queue providing jobs. It also means that you can mix queue types within a single application. If you have ephemeral or periodic tasks, you may want to process them in an in-memory queue, and use Postgres or Redis queues for jobs requiring queue durability.

Neoq aims to be _simple_, _reliable_, _easy to integrate_, and demand a _minimal infrastructure footprint_ by providing queue backends that match your existing tech stack.

# What it does

- **Multiple Backends**: In-memory, Postgres, Redis, or user-supplied custom backends.
- **Retries**: Jobs may be retried a configurable number of times with exponential backoff and jitter to prevent thundering herds
- **Job uniqueness**: jobs are fingerprinted based on their payload and status to prevent job duplication (multiple jobs with the same payload are not re-queued)
- **Job Timeouts**: Queue handlers can be configured with per-job timeouts with millisecond accuracy
- **Periodic Jobs**: Jobs can be scheduled periodically using standard cron syntax
- **Future Jobs**: Jobs can be scheduled in the future
- **Concurrency**: Concurrency is configurable for every queue
- **Job Deadlines**: If a job doesn't complete before a specific `time.Time`, the job expires 

# Getting Started

Getting started is as simple as declaring queue handlers and adding jobs. You can create multiple neoq instances with different backends to meet your application's needs. E.g. an in-memory backend instance for ephemeral jobs and a Postgres backend instance for queue durability between application restarts.

Additional documentation can be found in the wiki: https://github.com/acaloiaro/neoq/wiki

Error handling in this section is excluded for simplicity.

## Add queue handlers

Queue handlers listen for Jobs on queues. Jobs may consist of any payload that is JSON-serializable.

Queue Handlers are simple Go functions that accept a `Context` parameter.

**Example**: Add a listener on the `greetings` queue using the default in-memory backend

```go
ctx := context.Background()
nq, _ := neoq.New(ctx, neoq.WithBackend(memory.Backend))
nq.Start(ctx, handler.New("greetings", func(ctx context.Context) (err error) {
  j, _ := jobs.FromContext(ctx)
  log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
  return
}))
```

## Enqueue jobs

Enqueuing adds jobs to the specified queue to be processed asynchronously.

**Example**: Add a "Hello World" job to the `greetings` queue using the default in-memory backend.

```go
ctx := context.Background()
nq, _ := neoq.New(ctx, neoq.WithBackend(memory.Backend))
nq.Enqueue(ctx, &jobs.Job{
  Queue: "greetings",
  Payload: map[string]any{
    "message": "hello world",
  },
})
```

## Redis

**Example**: Process jobs on the "greetings" queue and add a job to it using the redis backend

```go
ctx := context.Background()
nq, _ := neoq.New(ctx,
  neoq.WithBackend(redis.Backend),
  redis.WithAddr("localhost:6379"),
  redis.WithPassword(""))

nq.Start(ctx, handler.New("greetings", func(ctx context.Context) (err error) {
  j, _ := jobs.FromContext(ctx)
  log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
  return
}))

nq.Enqueue(ctx, &jobs.Job{
  Queue: "greetings",
  Payload: map[string]interface{}{
    "message": "hello world",
  },
})
```

## Postgres

**Example**: Process jobs on the "greetings" queue and add a job to it using the postgres backend

```go
ctx := context.Background()
nq, _ := neoq.New(ctx,
  neoq.WithBackend(postgres.Backend),
  postgres.WithConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq"),
)

nq.Start(ctx, handler.New("greetings", func(ctx context.Context) (err error) {
  j, _ := jobs.FromContext(ctx)
  log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
  return
}))

nq.Enqueue(ctx, &jobs.Job{
  Queue: "greetings",
  Payload: map[string]interface{}{
    "message": "hello world",
  },
})
```
# Example Code

Additional example integration code can be found at https://github.com/acaloiaro/neoq/tree/main/examples

# Status

This project is currently in alpha. Future releases may change the API.
