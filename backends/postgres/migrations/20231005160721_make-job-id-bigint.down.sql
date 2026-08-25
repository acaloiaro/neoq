ALTER TABLE neoq_jobs ALTER COLUMN id SET DATA TYPE integer;
DO $$
DECLARE
	seq regclass := pg_get_serial_sequence('neoq_jobs', 'id');
BEGIN
	IF seq IS NOT NULL THEN
		EXECUTE format('ALTER SEQUENCE %s AS integer', seq);
	END IF;
END $$;
