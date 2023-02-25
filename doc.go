// Package neoq provides background job processing for Go applications.
//
// Neoq's goal is to minimize the infrastructure necessary to add background job processing to
// Go applications. It does so by implementing queue durability with modular backends, rather
// than introducing a strict dependency on a particular backend such as Redis.
//
// An in-memory and Postgres backend are provided out of the box.
package neoq
