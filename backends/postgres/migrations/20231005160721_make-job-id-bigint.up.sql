ALTER TABLE neoq_jobs ALTER COLUMN id SET DATA TYPE bigint;
-- Resolve the identity sequence from the column that owns it rather than
-- naming a schema: neoq's tables are created through search_path, so they do
-- not always live in `public` (e.g. a schema-per-tenant deployment).
DO $$
DECLARE
	seq regclass := pg_get_serial_sequence('neoq_jobs', 'id');
BEGIN
	IF seq IS NOT NULL THEN
		EXECUTE format('ALTER SEQUENCE %s AS bigint', seq);
	END IF;
END $$;
