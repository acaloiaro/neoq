//go:build testing

package testutils

import (
	"log"
	"strings"
)

// NewTestLogger returns a new TestLogger that logs messages to the given channel
func NewTestLogger(ch chan string) *TestLogger {
	return &TestLogger{
		L: log.New(ChanWriter{ch: ch}, "", 0),
	}
}

// TestLogger is a utility for logging in tests
type TestLogger struct {
	L *log.Logger
}

// Info prints to stdout, with args separated by spaces
func (h TestLogger) Info(m string, args ...any) {
	h.L.Println(m, args)
}

// Debug prints to stdout, with args separates by spaces
func (h TestLogger) Debug(m string, args ...any) {
	h.L.Println(m, args)
}

// Error prints to stdout with args separated by spaces
func (h TestLogger) Error(m string, args ...any) {
	h.L.Println(m, args)
}

type ChanWriter struct {
	ch chan string
}

func (c ChanWriter) Write(p []byte) (n int, err error) {
	c.ch <- strings.Trim(string(p), "\n")
	return len(p), nil
}
