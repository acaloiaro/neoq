//go:build testing

package testutils

import "log"

// TestLogger is a utility for logging in tests
type TestLogger struct {
	L    *log.Logger
	Done chan bool
}

// Info prints to stdout and signals its done channel
func (h TestLogger) Info(m string, args ...any) {
	h.L.Println(m)
	h.Done <- true
}

// Debug prints to stdout and signals its done channel
func (h TestLogger) Debug(m string, args ...any) {
	h.L.Println(m)
	h.Done <- true
}

// Error prints to stdout and signals its done channel
func (h TestLogger) Error(m string, err error, args ...any) {
	h.L.Println(m, err)
	h.Done <- true
}
