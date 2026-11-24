-- sutra:no-transaction
-- The spool reference of an audit row: `<spoolId>:<n>`, the engine's local audit spool (a UUID)
-- and the record's number in it. A record is shipped from the spool into this table at least
-- once, so the shipper can write it again after a crash; the unique index below makes that second
-- write a no-op. The (deployment_id, instance_id, seq) index already drops a re-shipped instance
-- row, but not a deployment-level row (no instance) or a transport row, which that index does not
-- cover: this one does.
--
-- Rows written before the spool existed carry no reference (NULL) and are outside the index.
--
-- Runs outside a transaction so the index is built without blocking writes. Every statement is
-- safe to repeat: a crash part-way runs the whole script again.
--
-- PostgreSQL only, like V202 and V203.
ALTER TABLE audit_event ADD COLUMN IF NOT EXISTS spool_ref VARCHAR(80);
DROP INDEX CONCURRENTLY IF EXISTS audit_event_spool_ref;
CREATE UNIQUE INDEX CONCURRENTLY audit_event_spool_ref ON audit_event (spool_ref) WHERE spool_ref IS NOT NULL;
