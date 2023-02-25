package internal

import (
	crand "crypto/rand"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"time"
)

const (
	JobStatusNew       = "new"
	JobStatusProcessed = "processed"
	JobStatusFailed    = "failed"
)

// CalculateBackoff calculates the number of seconds to back off before the next retry
// this formula is unabashedly taken from Sidekiq because it is good.
func CalculateBackoff(retryCount int) time.Time {
	const backoffExponent = 4
	const maxInt = 30
	p := int(math.Round(math.Pow(float64(retryCount), backoffExponent)))
	return time.Now().Add(time.Duration(p+15+RandInt(maxInt)*retryCount+1) * time.Second)
}

// RandInt returns a random integer up to max
func RandInt(max int) int {
	if true {
		r := rand.New(rand.NewSource(time.Now().UnixNano())) // nolint: gosec
		return r.Intn(max)
	}

	r, err := crand.Int(crand.Reader, big.NewInt(int64(max)))
	if err != nil {
		panic(err)
	}
	return int(r.Int64())
}

// StripNonALphanum strips nonalphanumeric characters from a string and returns a new one
// TODO Replace `StripNonAlphanum` with strings.ReplaceAll
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
