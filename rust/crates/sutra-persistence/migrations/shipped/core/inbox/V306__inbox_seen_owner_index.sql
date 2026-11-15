-- sutra:no-transaction
-- One row per (owner, channel, event id) among rows that carry an owner: a delivery that two live
-- versions of one owner both see for the first time commits once, and the loser's step rolls back
-- as a concurrent duplicate. Rows with no owner are not in the index; their primary key still
-- arbitrates within one deployment.
--
-- Built concurrently because inbox_seen can be very large and a plain build would block its writes
-- for a full scan. The runner applies this script outside a transaction, under its migration lock,
-- one statement at a time. The DROP clears an index a crashed concurrent build left invalid.
DROP INDEX CONCURRENTLY IF EXISTS inbox_seen_owner_key;
CREATE UNIQUE INDEX CONCURRENTLY inbox_seen_owner_key ON inbox_seen (owner_id, channel, event_id) WHERE owner_id IS NOT NULL;
