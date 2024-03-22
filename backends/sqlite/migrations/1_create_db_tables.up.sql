CREATE TABLE neoq_jobs (
	id SERIAL NOT NULL,
    fingerprint text,
    retries integer default 0,
    max_retries integer default 5,
    run_after datetime default CURRENT_TIMESTAMP,    
    ran_at datetime,
    created_at datetime default CURRENT_TIMESTAMP,
    error text
);