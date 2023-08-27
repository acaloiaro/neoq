//go:build testing

package testutils

import (
	"log"
	"strings"
)

// TestLogger is a utility for logging in tests
type TestLogger struct {
	L *log.Logger
}

// Info prints to stdout and signals its done channel
func (h TestLogger) Info(m string, args ...any) {
	h.L.Println(m, args)
}

// Debug prints to stdout and signals its done channel
func (h TestLogger) Debug(m string, args ...any) {
	h.L.Println(m, args)
}

// Error prints to stdout and signals its done channel
func (h TestLogger) Error(m string, args ...any) {
	h.L.Println(m, args)
}

type ChanWriter struct {
	Ch chan string
}

func (c ChanWriter) Write(p []byte) (n int, err error) {
	c.Ch <- strings.Trim(string(p), "\n")
	return len(p), nil
}
