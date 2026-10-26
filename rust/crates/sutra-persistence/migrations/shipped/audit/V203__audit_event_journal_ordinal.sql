-- The journal ordinal: a per-instance position in an instance's audit journal, assigned in
-- COMMIT order, which is what a history reader pages by.
--
-- `seq` cannot be that cursor. The engine numbers its own events, and V202 lets a transport event
-- take the instance's current high-water seq, so a transport row written after a reader passed
-- seq N can land AT seq N: a reader paging `seq > N` never sees it. And no surrogate key a
-- sequence hands out can be the cursor either, because two transactions can take ids in one order
-- and commit in the other.
--
-- So every instance row takes `ordinal = MAX(ordinal) + 1` for its instance while holding a
-- transaction-scoped advisory lock on that instance: the next writer of the same instance waits
-- until this transaction commits (or aborts), so ordinals of one instance become visible in
-- increasing order and a reader paging `ordinal > N` misses nothing. The trigger does the
-- assignment, so every writer gets it, including one that predates this column. The lock is per
-- instance, so writers of different instances never wait on each other.
--
-- The engine's replay dedup is unchanged: the V202 partial unique index on `seq` still drops a
-- re-emitted engine event (its BEFORE trigger runs, the insert is then skipped; an ordinal that is
-- never stored leaves no gap a reader could wait on).
--
-- Deployment-level rows (no instance) carry no ordinal; they are not part of any instance journal.
--
-- PostgreSQL only, like V202.
ALTER TABLE audit_event ADD COLUMN ordinal BIGINT;

UPDATE audit_event AS e
   SET ordinal = numbered.ordinal
  FROM (SELECT id,
               ROW_NUMBER() OVER (PARTITION BY deployment_id, instance_id ORDER BY seq, id) AS ordinal
          FROM audit_event
         WHERE instance_id IS NOT NULL) AS numbered
 WHERE e.id = numbered.id;

CREATE UNIQUE INDEX audit_event_instance_ordinal
  ON audit_event (deployment_id, instance_id, ordinal)
  WHERE instance_id IS NOT NULL;

CREATE FUNCTION audit_event_assign_ordinal() RETURNS trigger AS $$
BEGIN
  IF NEW.instance_id IS NOT NULL THEN
    PERFORM pg_advisory_xact_lock(
      hashtextextended('audit_event:' || NEW.deployment_id || ':' || NEW.instance_id::text, 0));
    -- A new statement, so a new snapshot: it sees the rows of the writer this lock waited for.
    SELECT COALESCE(MAX(ordinal), 0) + 1 INTO NEW.ordinal
      FROM audit_event
     WHERE deployment_id = NEW.deployment_id AND instance_id = NEW.instance_id;
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_event_ordinal
  BEFORE INSERT ON audit_event
  FOR EACH ROW EXECUTE FUNCTION audit_event_assign_ordinal();
