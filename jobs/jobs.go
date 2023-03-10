package jobs

import (
	"crypto/md5" // nolint: gosec
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/guregu/null"
)

var (
	ErrJobTimeout = errors.New("timed out waiting for job(s)")
)

const (
	DuplicateJobID = -1
	UnqueuedJobID  = -2
)

// Job contains all the data pertaining to jobs
//
// Jobs are what are placed on queues for processing.
//
// The Fingerprint field can be supplied by the user to impact job deduplication.
// TODO Factor out usage of the null package: github.com/guregu/null
type Job struct {
	ID          int64          `db:"id"`
	Fingerprint string         `db:"fingerprint"` // A md5 sum of the job's queue + payload, affects job deduplication
	Status      string         `db:"status"`      // The status of the job
	Queue       string         `db:"queue"`       // The queue the job is on
	Payload     map[string]any `db:"payload"`     // JSON job payload for more complex jobs
	RunAfter    time.Time      `db:"run_after"`   // The time after which the job is elligible to be picked up by a worker
	RanAt       null.Time      `db:"ran_at"`      // The last time the job ran
	Error       null.String    `db:"error"`       // The last error the job elicited
	Retries     int            `db:"retries"`     // The number of times the job has retried
	MaxRetries  int            `db:"max_retries"` // The maximum number of times the job can retry
	CreatedAt   time.Time      `db:"created_at"`  // The time the job was created
}

// FingerprintJob fingerprints jobs as an md5 hash of its queue combined with its JSON-serialized payload
func FingerprintJob(j *Job) (err error) {
	// only generate a fingerprint if the job is not already fingerprinted
	if j.Fingerprint != "" {
		return
	}

	var js []byte
	js, err = json.Marshal(j.Payload)
	if err != nil {
		return
	}
	h := md5.New() // nolint: gosec
	_, err = io.WriteString(h, j.Queue)
	if err != nil {
		return
	}

	_, err = h.Write(js)
	if err != nil {
		return
	}

	j.Fingerprint = fmt.Sprintf("%x", h.Sum(nil))

	return
}
