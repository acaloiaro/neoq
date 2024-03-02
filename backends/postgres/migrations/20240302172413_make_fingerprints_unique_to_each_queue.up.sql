DROP INDEX IF EXISTS neoq_jobs_fingerprint_unique_idx;
CREATE UNIQUE INDEX IF NOT EXISTS neoq_jobs_fingerprint_unique_idx ON neoq_jobs (queue, fingerprint, status) WHERE NOT (status = 'processed');
