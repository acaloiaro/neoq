CREATE INDEX IF NOT EXISTS neoq_jobs_fingerprint_idx ON neoq_jobs (fingerprint, status);
DROP INDEX IF EXISTS neoq_jobs_fingerprint_unique_idx;
