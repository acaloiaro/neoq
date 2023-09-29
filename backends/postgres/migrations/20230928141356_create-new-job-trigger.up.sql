CREATE OR REPLACE FUNCTION announce_job() RETURNS trigger AS $$
DECLARE
BEGIN
  PERFORM pg_notify(CAST(NEW.queue AS text), CAST(NEW.id AS text));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER announce_job 
  AFTER INSERT ON neoq_jobs FOR EACH ROW 
  WHEN (NEW.run_after <= timezone('utc', NEW.created_at))
  EXECUTE PROCEDURE announce_job();