# Neoq

Background job processing for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/acaloiaro/neoq.svg)](https://pkg.go.dev/github.com/acaloiaro/neoq) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://app.gitter.im/#/room/#neoq:gitter.im)

# Installation

`go get github.com/acaloiaro/neoq`

# About

Neoq is a queue-agnostic background job framework for Go.

Neoq job handlers are the same, whether queues are in-memory for development/testing, or Postgres, Redis, or a custom queue for production -- allowing queue infrastructure to change without code change.

Developing/testing or don't need a durable queue? Use the in-memory queue.

Running an application in production? Use Postgres.

Have higher throughput demands in production? Use Redis.

Neoq does not aim to be the _fastest_ background job processor. It aims to be _fast_, _reliable_, and demand a _minimal infrastructure footprint_.

# What it does

- **Multiple Backends**: In-memory, Postgres, Redis, or user-supplied custom backends.
- **Retries**: Jobs may be retried a configurable number of times with exponential backoff and jitter to prevent thundering herds
- **Job uniqueness**: jobs are fingerprinted based on their payload and status to prevent job duplication (multiple jobs with the same payload are not re-queued)
- **Job Timeouts**: Queue handlers can be configured with per-job timeouts with millisecond accuracy
- **Periodic Jobs**: Jobs can be scheduled periodically using standard cron syntax
- **Future Jobs**: Jobs can be scheduled either for the future or immediate execution
- **Concurrency**: Concurrency is configurable for every queue

# Getting Started

Getting started is as simple as declaring queue handlers and adding jobs.

Additional documentation can be found in the wiki: https://github.com/acaloiaro/neoq/wiki

Error handling in this section is excluded for simplicity.

## Add queue handlers

Queue handlers listen for Jobs on queues. Jobs may consist of any payload that is JSON-serializable.

Queue Handlers are simple Go functions that accept a `Context` parameter.

**Example**: Add a listener on the `hello_world` queue using the default in-memory backend

```go
ctx := context.Background()
nq, _ := neoq.New(ctx)
nq.Start(ctx, "hello_world", handler.New(func(ctx context.Context) (err error) {
  j, _ := jobs.FromContext(ctx)
  log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
  return
}))
```

## Enqueue jobs

Enqueuing jobs adds jobs to the specified queue to be processed asynchronously.

**Example**: Add a "Hello World" job to the `hello_world` queue using the default in-memory backend.

```go
ctx := context.Background()
nq, _ := neoq.New(ctx)
nq.Enqueue(ctx, &jobs.Job{
  Queue: "hello_world",
  Payload: map[string]interface{}{
    "message": "hello world",
  },
})
```

## Redis

**Example**: Process jobs on the "hello_world" queue and add a job to it using the redis backend

```go
ctx := context.Background()
nq, _ := neoq.New(ctx,
  neoq.WithBackend(redis.Backend),
  redis.WithAddr("localhost:6379"),
  redis.WithPassword(""),
)

nq.Start(ctx, "hello_world", handler.New(func(ctx context.Context) (err error) {
  j, _ := jobs.FromContext(ctx)
  log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
  return
}))

nq.Enqueue(ctx, &jobs.Job{
  Queue: "hello_world",
  Payload: map[string]interface{}{
    "message": "hello world",
  },
})
```

## Postgres

**Example**: Process jobs on the "hello_world" queue and add a job to it using the postgres backend

```go
ctx := context.Background()
nq, _ := neoq.New(ctx,
  neoq.WithBackend(postgres.Backend),
  postgres.WithConnectionString("postgres://postgres:postgres@127.0.0.1:5432/neoq"),
)

nq.Start(ctx, "hello_world", handler.New(func(ctx context.Context) (err error) {
  j, _ := jobs.FromContext(ctx)
  log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
  return
}))

nq.Enqueue(ctx, &jobs.Job{
  Queue: "hello_world",
  Payload: map[string]interface{}{
    "message": "hello world",
  },
})
```
# Example Code

Additional example integration code can be found at https://github.com/acaloiaro/neoq/tree/main/examples

# Status

This project is currently in alpha. Future releases may change the API. It currently leaks some resources. It can handle unimportant workloads.
