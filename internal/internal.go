package internal

import (
	"math"
	"math/rand"
	"strings"
	"time"
)

const (
	JobStatusNew              = "new"
	JobStatusProcessed        = "processed"
	JobStatusFailed           = "failed"
	DefaultTransactionTimeout = time.Minute
	DefaultHandlerDeadline    = 30 * time.Second
	DuplicateJobID            = -1
	UnqueuedJobID             = -2
	DefaultJobCheckInterval   = 5 * time.Second
	// the window of time between time.Now() and when a job's RunAfter comes due that neoq will schedule a goroutine to
	// schdule the job for execution.
	// E.g. right now is 16:00 and a job's RunAfter is 16:30 of the same date. This job will get a dedicated goroutine to
	// wait until the job's RunAfter, scheduling the job to be run exactly at RunAfter
	DefaultFutureJobWindow = 30 * time.Second
)

// CalculateBackoff calculates the number of seconds to back off before the next retry
// this formula is unabashedly taken from Sidekiq because it is good.
func CalculateBackoff(retryCount int) time.Time {
	p := int(math.Round(math.Pow(float64(retryCount), 4)))
	return time.Now().Add(time.Duration(p+15+RandInt(30)*retryCount+1) * time.Second)
}

// RandInt returns a random integer up to max
func RandInt(max int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(max)
}

// StripNonALphanum strips nonalphanumeric characters from a string and returns a new one
func StripNonAlphanum(s string) string {
	var result strings.Builder
	for i := 0; i < len(s); i++ {
		b := s[i]
		if (b == '_') ||
			('a' <= b && b <= 'z') ||
			('A' <= b && b <= 'Z') ||
			('0' <= b && b <= '9') ||
			b == ' ' {
			result.WriteByte(b)
		}
	}
	return result.String()
}
