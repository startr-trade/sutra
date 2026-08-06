-- MySQL/MariaDB dialect of the outbox delivery-attempt ceiling (PostgreSQL reference:
-- V604__outbox_poisoned.sql). Marks an entry that exhausted `sutra.outbox.retry.max-attempts` as
-- terminal: no longer claimed, not deleted (at-least-once is never traded for silence), still
-- visible and redrivable by clearing the flag.
--
-- No partial index here (MySQL has no filtered indexes); the claim predicate leads with
-- `deployment_id`, so the existing due index still serves it.

ALTER TABLE outbox_entry ADD COLUMN poisoned BOOLEAN NOT NULL DEFAULT FALSE;
