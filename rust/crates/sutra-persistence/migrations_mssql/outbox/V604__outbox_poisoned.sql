-- SQL Server dialect of the outbox delivery-attempt ceiling (PostgreSQL reference:
-- V604__outbox_poisoned.sql). Marks an entry that exhausted `sutra.outbox.retry.max-attempts` as
-- terminal: no longer claimed, not deleted (at-least-once is never traded for silence), still
-- visible and redrivable by clearing the flag.
--
-- BIT NOT NULL with a named default constraint — SQL Server has no boolean type, and an unnamed
-- default would get a generated name that a later migration could not reference.

ALTER TABLE outbox_entry
    ADD poisoned BIT NOT NULL CONSTRAINT df_outbox_entry_poisoned DEFAULT 0;
