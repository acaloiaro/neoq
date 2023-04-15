// Package neoq is a queue-agnostic background job framework for Go.
//
// Neoq job handlers are the same, whether queues are in-memory for development/testing,
// or Postgres, Redis, or a custom queue for production -- allowing queue infrastructure to
// change without code change.
//
// Developing/testing or don't need a durable queue? Use the in-memory queue.
// Running an application in production? Use Postgres.
// Have higher throughput demands in production? Use Redis.

// Neoq does not aim to be the _fastest_ background job processor. It aims to be _fast_, _reliable_, and demand a
// _minimal infrastructure footprint_.
package neoq
