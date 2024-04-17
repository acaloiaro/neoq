CREATE TABLE neoq_jobs (
	id integer primary key not null,
    fingerprint text,
    queue text,
    status text default "new",
    payload text,
    retries integer default 0,
    max_retries integer default 3,
    run_after datetime default CURRENT_TIMESTAMP,    
    deadline datetime,
    ran_at datetime,
    created_at datetime default CURRENT_TIMESTAMP,
    error text
);

CREATE TABLE neoq_dead_jobs (
	id integer primary key not null,
    fingerprint text,
    queue text,
    status text default "failed",
    payload text,
    retries integer,
    max_retries integer,
    run_after datetime default CURRENT_TIMESTAMP,    
    deadline datetime,
    ran_at datetime,
    created_at datetime default CURRENT_TIMESTAMP,
    error text
);