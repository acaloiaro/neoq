DO $$ BEGIN
	CREATE TYPE job_status AS ENUM (
		'new',
		'processed',
		'failed'
	);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS neoq_dead_jobs (
		id SERIAL NOT NULL,
		fingerprint text NOT NULL,
		queue text NOT NULL,
		status job_status NOT NULL default 'failed',
		payload jsonb,
		retries integer,
		max_retries integer,
		created_at timestamp with time zone DEFAULT now(),
		error text
);

CREATE TABLE IF NOT EXISTS neoq_jobs (
		id SERIAL NOT NULL,
		fingerprint text NOT NULL,
		queue text NOT NULL,
		status job_status NOT NULL default 'new',
		payload jsonb,
		retries integer default 0,
		max_retries integer default 23,
		run_after timestamp with time zone DEFAULT now(),
		ran_at timestamp with time zone,
		created_at timestamp with time zone DEFAULT now(),
		error text
);

CREATE INDEX IF NOT EXISTS neoq_job_fetcher_idx ON neoq_jobs (id, status, run_after);
CREATE INDEX IF NOT EXISTS neoq_jobs_fetcher_idx ON neoq_jobs (queue, status, run_after);
CREATE INDEX IF NOT EXISTS neoq_jobs_fingerprint_idx ON neoq_jobs (fingerprint, status);

