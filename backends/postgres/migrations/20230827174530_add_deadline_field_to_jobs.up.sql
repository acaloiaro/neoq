ALTER TABLE neoq_jobs ADD COLUMN IF NOT EXISTS deadline timestamp with time zone;
ALTER TABLE neoq_dead_jobs ADD COLUMN IF NOT EXISTS deadline timestamp with time zone;