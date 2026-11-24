-- sutra:no-transaction
-- The pass (attempt) that wrote an engine audit row. A step's events reach the audit spool before
-- the step commits, so a step whose commit failed, or whose process died before it, runs again
-- from its last snapshot and writes the same (deployment_id, instance_id, seq) numbers again,
-- possibly for a different path. Every pass carries a random attempt_id, the instance's seq when
-- it began (attempt_base) and when it began (attempt_at). At most one pass from a given base
-- commits, and it is the last one from that base, so for each (deployment_id, instance_id,
-- attempt_base) the store keeps the rows of the latest (attempt_at, attempt_id) and deletes the
-- rows of every earlier attempt: the journal is the committed trail.
--
-- Rows without an attempt (written before 0.9.0, transport and deployment-level rows) are NULL
-- here and never superseded.
--
-- Runs outside a transaction so the index is built without blocking writes. Every statement is
-- safe to repeat. PostgreSQL only, like V202 to V205.
ALTER TABLE audit_event ADD COLUMN IF NOT EXISTS attempt_id VARCHAR(40);
ALTER TABLE audit_event ADD COLUMN IF NOT EXISTS attempt_base INTEGER;
ALTER TABLE audit_event ADD COLUMN IF NOT EXISTS attempt_at TIMESTAMPTZ;
DROP INDEX CONCURRENTLY IF EXISTS audit_event_attempt;
CREATE INDEX CONCURRENTLY audit_event_attempt ON audit_event (deployment_id, instance_id, attempt_base) WHERE attempt_id IS NOT NULL;
