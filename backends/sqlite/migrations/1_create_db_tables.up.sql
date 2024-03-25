CREATE TABLE neoq_jobs (
	id integer PRIMARY KEY NOT NULL,
    fingerprint text,
    queue text,
    job_status text,
    payload text,
    retries integer default 0,
    max_retries integer default 5,
    run_after datetime default CURRENT_TIMESTAMP,    
    deadline datetime,
    ran_at datetime,
    created_at datetime default CURRENT_TIMESTAMP,
    error text
);