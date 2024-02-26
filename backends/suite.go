package backends

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/acaloiaro/neoq"
	"github.com/acaloiaro/neoq/backends/postgres"
	"github.com/acaloiaro/neoq/handler"
	"github.com/acaloiaro/neoq/jobs"
	"github.com/acaloiaro/neoq/logging"
	"github.com/stretchr/testify/suite"
)

type NeoQTestSuite struct {
	suite.Suite
	NeoQ neoq.Neoq
}

// NewNeoQTestSuite constructs a new NeoQ test suite that can be used to test
// any impementation of the queue
func NewNeoQTestSuite(q neoq.Neoq) *NeoQTestSuite {
	n := new(NeoQTestSuite)
	n.NeoQ = q
	return n
}

func (s *NeoQTestSuite) TearDownSuite() {
	s.NeoQ.Shutdown(context.Background())
}

var testQueue = fmt.Sprintf("testq-%d", time.Now().Unix())

const (
	proceedKey = "proceed"
	// foundKey is populated in the payload if the test expects to find the job
	foundKey = "found"

	messageKey = "messageKey"

	doneKey      = "done"
	runAfterKey  = "runAfter"
	fingerprint1 = "fp1"
)

// TestSuite tests the backends independent of implementation
func TestSuite(t *testing.T) {
	ctx := context.Background()
	s := new(NeoQTestSuite)
	var err error
	s.NeoQ, err = neoq.New(ctx, neoq.WithBackend(postgres.Backend), neoq.WithLogLevel(logging.LogLevelDebug))
	if err != nil {
		t.Fatal(err)
	}
	suite.Run(t, s)
}

// TestOverrideFingerprint provides a test case that works with all the backends that
// verifies that overriding jobs with a new fingerprint works.
// The test case is queues several jobs, some are designed to be overridden by subsequent jobs, some are not.
func (s *NeoQTestSuite) TestOverrideFingerprint() {
	ctx := context.Background()
	fingerprint1 := fmt.Sprintf("fingerprint-1-%d", rand.Int63())
	fingerprint2 := fmt.Sprintf("fingerprint-2-%d", rand.Int63())

	var err error
	jobsProcessed, jobsToDo := 0, 3

	fingerPrints := make(map[string]int)

	done := make(chan bool)
	// proceed channel ensure that jobs that are supposed to be processed are processed
	proceed := make(chan bool)

	h1 := handler.New(testQueue, func(ctx context.Context) error {
		j, err := jobs.FromContext(ctx)
		s.NoError(err)
		s.NotEmpty(j)
		message := j.Payload[messageKey].(string)

		found := j.Payload[foundKey].(bool)
		s.Truef(found, "Found job that should not be found: %s", message)

		if _, ok := j.Payload[proceedKey]; ok {
			proceed <- true
		}

		fingerPrints[j.Fingerprint]++
		jobsProcessed++
		s.T().Logf("Handled job %d with fingerprint %s and ID %d Payload: %s", jobsProcessed, j.Fingerprint, j.ID, message)
		if jobsToDo == jobsProcessed {
			done <- true
		}
		return err
	})

	s.NoError(s.NeoQ.Start(ctx, h1))

	go func() {
		now := time.Now()
		_, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "first queued item.  we'll wait until this is processed",
				foundKey:   true,
				proceedKey: true,
			},
			RunAfter:    now,
			Fingerprint: fingerprint1,
		})
		s.NoError(err, "job was not enqueued.")
		<-proceed

		runAt := now.Add(120 * time.Second)
		_, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "should insert, since the prior key has been processed.",
				foundKey:   true,
				proceedKey: true,
			},
			RunAfter:    runAt,
			Fingerprint: fingerprint2,
		})
		s.NoError(err)
		_, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "should not be queued - conflicting key",
				foundKey:   false,
				proceedKey: true,
			},
			RunAfter:    runAt,
			Fingerprint: fingerprint2,
		})
		s.Error(err, "should not insert")
		s.True(errors.Is(err, jobs.ErrJobFingerprintConflict))
		_, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "(2) the item that overwrites may be found",
				proceedKey: true,
				foundKey:   true},
			RunAfter:    now,
			Fingerprint: fingerprint2,
		}, neoq.WithOverrideMatchingFingerprint())

		s.NoErrorf(err, "Should have returned nil but returned %v", err)
		<-proceed
		_, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]interface{}{
				messageKey: "(3) the new item",
				proceedKey: true,
				foundKey:   true},
			RunAfter:    now,
			Fingerprint: fingerprint2,
		})
		s.NoErrorf(err, "job was not enqueued.%w", err)
		<-proceed
	}()

	timeoutTimer := time.After(time.Minute)
results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case <-done:
			break results_loop
		}
	}
	s.NoError(err)
	s.Equalf(jobsToDo, jobsProcessed,
		"handler should have handled %d jobs, but handled %d. %v",
		jobsToDo, jobsProcessed, fingerPrints)
}

func makeHandler(s *NeoQTestSuite, done chan bool) handler.Handler {
	return handler.New(testQueue, func(ctx context.Context) (err error) {
		var j *jobs.Job
		j, err = jobs.FromContext(ctx)
		if !s.NoError(err) && !s.NotNil(j) {
			return fmt.Errorf("failed to get job from context %w", err)
		}
		message := j.Payload[messageKey].(string)

		found := j.Payload[foundKey].(bool)
		s.Truef(found, "Found job that should not be found: %s", message)

		isDone, ok := j.Payload[doneKey]
		if ok && isDone.(bool) {
			done <- true
		}
		return nil
	})

}

// TestConflictingFingerprints verifies that that conflicting fingerprints can't override one another
func (s *NeoQTestSuite) TestConflictingFingerprints() {
	ctx := context.Background()
	fingerPrint := fmt.Sprintf("ConflictingFingerprints-%d", time.Now().Nanosecond())

	done := make(chan bool)
	h := makeHandler(s, done)
	s.NoError(s.NeoQ.Start(ctx, h))
	runAt := time.Now().UTC().Add(time.Second * 2)
	go func() {
		_, err := s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "(1) first queued item we'll wait until this is processed",
				doneKey:    true,
				foundKey:   true},
			RunAfter:    runAt,
			Fingerprint: fingerPrint,
		})
		s.NoErrorf(err, "job was not enqueued.%w", err)

		_, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "conflicting item should be should not save",
				foundKey:   false},
			RunAfter:    runAt,
			Fingerprint: fingerPrint,
		})
		s.Errorf(err, "Job with fingerprint %s should not be queued", fingerPrint)
	}()

	var err error
	timeoutTimer := time.After(time.Minute)
results_loop:
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			break results_loop
		case <-done:
			break results_loop
		}
	}
	s.NoError(err)
}

// TestOverridingFingerprints verifies that when WithOverrideMatchingFingerprint is set, an overridding
// job can be written to the queue
func (s *NeoQTestSuite) TestOverridingFingerprints() {
	ctx := context.Background()
	conflictingFingerprint := fmt.Sprintf("OverridingFingerprints-%d", time.Now().UnixMicro())
	done := make(chan bool)

	s.NoError(s.NeoQ.Start(ctx, makeHandler(s, done)))

	now := time.Now().UTC()

	go func() {
		job, err := s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "TestOverridingFingerprints. No conflict.",
				doneKey:    false,
				foundKey:   true},
			RunAfter: now.Add(time.Hour),
		})
		s.NoErrorf(err, "job (%s) was not enqueued.%w", job, err)

		job, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: "TestOverridingFingerprints- job should not be found, it should be overridden",
				doneKey:    true,
				foundKey:   false},
			RunAfter:    now.Add(time.Hour),
			Fingerprint: conflictingFingerprint,
		})
		s.NoErrorf(err, "job was not enqueued.%w", err)

		job, err = s.NeoQ.Enqueue(ctx, &jobs.Job{
			Queue: testQueue,
			Payload: map[string]any{
				messageKey: fmt.Sprintf("job should be found - overwrites JOB: %s", job),
				foundKey:   true,
				doneKey:    true},
			RunAfter:    time.Now().UTC().Add(time.Second),
			Fingerprint: conflictingFingerprint,
		}, neoq.WithOverrideMatchingFingerprint())
		s.NoErrorf(err, "job was not enqueued job: %s. %w ", job, err)
	}()

	var err error
	timeoutTimer := time.After(2 * time.Minute)
	for finished := false; !finished; {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			finished = true
		case <-done:
			finished = true
		}
	}
	s.NoError(err)
}

// TestOverridingFingerprints verifies that when WithOverrideMatchingFingerprint is set, an overridding
// job can be written to the queue
func (s *NeoQTestSuite) TestMultipleOverridingFingerprints() {
	ctx := context.Background()
	fingerPrint := "fingerprint1" + time.Now().String()

	done := make(chan bool)

	s.NoError(s.NeoQ.Start(ctx, makeHandler(s, done)))

	go func() {
		last := 5
		for i := 1; i <= last; i++ {
			finalEntry := i == last
			runAfter := time.Now().UTC()
			if !finalEntry {
				runAfter = runAfter.Add(time.Hour)
			}
			_, err := s.NeoQ.Enqueue(ctx, &jobs.Job{
				Queue: testQueue,
				Payload: map[string]any{
					messageKey:  fmt.Sprintf("queue'd job %d. Is final? %v - run after %s", i, finalEntry, runAfter),
					foundKey:    finalEntry,
					doneKey:     finalEntry,
					runAfterKey: runAfter,
				},
				RunAfter:    runAfter,
				Fingerprint: fingerPrint,
			}, neoq.WithOverrideMatchingFingerprint())

			s.NoErrorf(err, "job was not enqueued.%w", err)
		}
	}()
	var err error
	timeoutTimer := time.After(time.Minute * 20)
	for finished := false; !finished; {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
			finished = true
		case <-done:
			finished = true
		}
	}
	s.NoError(err)
}

// TestBasicJobProcessing tests that the memory backend is able to process the most basic jobs with the
// most basic configuration.
func (s *NeoQTestSuite) TestBasicJobProcessing() {
	queue := fmt.Sprintf("basic-queue-%d", rand.Int63())
	numJobs := 1000
	doneCnt := 0
	done := make(chan bool)
	ctx := context.Background()
	timeoutTimer := time.After(5 * time.Second)

	h := handler.New(queue, func(_ context.Context) (err error) {
		done <- true
		return
	})

	s.NoError(s.NeoQ.Start(ctx, h))

	go func() {
		for i := 0; i < numJobs; i++ {
			jid, err := s.NeoQ.Enqueue(ctx, &jobs.Job{
				Queue: queue,
				Payload: map[string]interface{}{
					"message": fmt.Sprintf("hello world: %d", i),
				},
			})
			s.NoError(err, "job was not enqueued.")
			s.NotEqual(jobs.DuplicateJobID, jid, "duplicate or this error caused it: %v , %s", jid, err)
		}
	}()
	var err error
	for {
		select {
		case <-timeoutTimer:
			err = jobs.ErrJobTimeout
		case <-done:
			doneCnt++
		}

		if doneCnt >= numJobs {
			break
		}

		if err != nil {
			break
		}
	}
}
