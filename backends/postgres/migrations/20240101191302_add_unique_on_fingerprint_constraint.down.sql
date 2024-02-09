DROP CONSTRAINT neoq_jobs_fingerprint_run_unique_idx;
DROP INDEX neoq_jobs_fingerprint_unique_idx;
CREATE UNIQUE INDEX IF NOT EXISTS neoq_jobs_fingerprint_unique_idx ON neoq_jobs (fingerprint, status) WHERE NOT (status = 'processed');
