CREATE UNIQUE INDEX IF NOT EXISTS neoq_jobs_fingerprint_run_unique_idx ON neoq_jobs (queue, fingerprint, status, ran_at);
DROP INDEX neoq_jobs_fingerprint_unique_idx;
CREATE UNIQUE INDEX IF NOT EXISTS neoq_jobs_fingerprint_unique_idx ON neoq_jobs (queue, fingerprint, status) WHERE NOT (status = 'processed');
