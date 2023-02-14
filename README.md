# Neoq

Background job processing built on Postgres for Go

# Installation

`go get github.com/acaloiaro/neoq`

# About

Neoq is a background job framework for Go applications. Its purpose is to minimize the infrastructure necessary to run production applications. It does so by implementing queue durability with modular backends, rather than introducing a strict dependency on a particular backend such as Redis.

The initial backend is based on Postgres since many applications already require a ralational database, and Postgres is an excellent one.

Neoq does not aim to be the _fastest_ background job processor. It aims to be _fast_, _reliable_, and demand a minimal infrastructure footprint.

# Features

- **Postgres-backed** job processing
- **Job uniqueness**: jobs are fingerprinted based on their payload and status to prevent job duplication (multiple unprocessed jobs with the same payload cannot be queued)
- **Retries**: Jobs may be retried a configurable number of times with exponential backoff and jitter to prevent thundering herds
- **Deadlines**: Queue handlers can be configured with per-job time deadlines with millisecond accuracy
- **Configurable transaction idle time**: Don't let your background worker transactions run away with db resources. By default, worker transactions may idle for 60 seconds.
- **Future Jobs**: Jobs can be scheduled either for the future or immediate execution
- **Concurrency**: Concurrency is configurable for every queue

# Getting Started

Getting started is as simple as declaring queue handlers and adding jobs.

Additional documentation can be found in the wiki: https://github.com/acaloiaro/neoq/wiki

Error handling in this section is excluded for simplicity.

## Add queue handlers

Queue handlers listen for Jobs on queues. Jobs may consist of any payload that is JSON-serializable. Payloads are stored in Postgres as `jsonb` fields.

Queue Handlers are simple Go functions that accept a `Context` parameter.

**Example**: Add a listener on the `hello_world` queue

```go
nq, _ := neoq.New("postgres://username:password@127.0.0.1:5432/neoq?sslmode=disable")
nq.Listen("hello_world", neoq.NewHandler(func(ctx context.Context) (err error) {
  j, err := neoq.JobFromContext(ctx)
  log.Println("got job id:", j.ID, "messsage:", j.Payload["message"])
  return
}))
```

## Enqueue jobs

**Example**: Add a "Hello World" job to the `hello_world` queue

```go
nq, _ := neoq.New("postgres://username:password@127.0.0.1:5432/neoq?sslmode=disable")
jid, _ := nq.Enqueue(neoq.Job{
  Queue: "hello_world",
  Payload: map[string]interface{}{
    "message": "hello world",
  },
})

```



