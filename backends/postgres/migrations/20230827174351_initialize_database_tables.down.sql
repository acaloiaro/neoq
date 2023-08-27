DROP TABLE IF EXISTS neoq_dead_jobs;
DROP TABLE IF EXISTS neoq_jobs;

DROP TYPE IF EXISTS job_status;

DROP INDEX IF EXISTS neoq_job_fetcher_idx;
DROP INDEX IF EXISTS neoq_jobs_fetcher_idx;
DROP INDEX IF EXISTS neoq_jobs_fingerprint_idx;

