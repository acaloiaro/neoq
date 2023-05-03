package logging

import (
	"golang.org/x/exp/slog"
)

// Logger interface is the interface that neoq's logger must implement
//
// This interface is a subset of [slog.Logger]. The slog interface was chosen under the assumption that its
// likely to be Golang's standard library logging interface.
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
	Info(msg string, args ...any)
}

// LogLevel represents levels at which to log
type LogLevel int

const (
	// LogLevelError logs only error messages
	LogLevelError LogLevel = 8
	// LogLevelInfo logs errors and info messages
	LogLevelInfo LogLevel = 0
	// LogLevelDebug logs errors, info, and debug messages
	LogLevelDebug LogLevel = -4
)

// Level returns the slog log level corresponding with the neoq log level
func (l LogLevel) Level() slog.Level {
	switch l {
	case LogLevelError:
		return slog.LevelError
	case LogLevelDebug:
		return slog.LevelDebug
	case LogLevelInfo:
		return slog.LevelInfo
	default:
		return slog.LevelError
	}
}
