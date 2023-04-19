package logging

// Logger interface is the interface that neoq's logger must implement
//
// This interface is a subset of [slog.Logger]. The slog interface was chosen under the assumption that its
// likely to be Golang's standard library logging interface.
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
	Info(msg string, args ...any)
}
