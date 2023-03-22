# Neoq

Background job processing for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/acaloiaro/neoq.svg)](https://pkg.go.dev/github.com/acaloiaro/neoq) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://app.gitter.im/#/room/#neoq:gitter.im)

# Installation

`go get github.com/acaloiaro/neoq`

# About

Neoq is a background job framework for Go applications. Its purpose is to minimize the infrastructure necessary to run production applications. It does so by implementing queue durability with modular backends.

This allows application to use the same type of data store for both application data and backround job processing. At the moment an in-memory and Postgres backends are provided. However, the goal is to have backends for every major datastore: Postgres, Redis, MySQL, etc.

Neoq does not aim to be the _fastest_ background job processor. It aims to be _fast_, _reliable_, and demand a minimal infrastructure footprint.

# What it does

- **Background job Processing**: Neoq has an in-memory and Postgres backend out of the box. Users may supply their own without changing neoq directly.
- **Retries**: Jobs may be retried a configurable number of times with exponential backoff and jitter to prevent thundering herds
- **Job uniqueness**: jobs are fingerprinted based on their payload and status to prevent job duplication (multiple unprocessed jobs with the same payload cannot be queued)
- **Deadlines**: Queue handlers can be configured with per-job time deadlines with millisecond accuracy
- **Configurable transaction idle time**: Don't let your background worker transactions run away with db resources. By default, worker transactions may idle for 60 seconds.
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
