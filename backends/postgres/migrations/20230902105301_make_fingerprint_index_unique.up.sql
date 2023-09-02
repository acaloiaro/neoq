--- This unique partial index prevents multiple unprocessed jobs with the same payload from being queued
CREATE UNIQUE INDEX IF NOT EXISTS neoq_jobs_fingerprint_unique_idx ON neoq_jobs (fingerprint, status) WHERE NOT (status = 'processed');
DROP INDEX IF EXISTS neoq_jobs_fingerprint_idx;