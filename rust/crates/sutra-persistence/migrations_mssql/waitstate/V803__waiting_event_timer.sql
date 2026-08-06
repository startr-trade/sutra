-- SQL Server dialect of the timer schema addendum
-- (PostgreSQL reference: V803__waiting_event_timer.sql in the Rust tree). Timer wait states
-- reuse waiting_event with a TIMER marker:
--
--   kind          'MESSAGE' (default — every pre-existing row) | 'TIMER'
--   timer_due_at  when a TIMER row becomes claimable (NULL on MESSAGE rows)
--
-- The timer poller claims due rows with SELECT TOP (n) ... WITH (UPDLOCK, ROWLOCK,
-- READPAST) — the SQL Server equivalent of FOR UPDATE SKIP LOCKED. The reference's
-- partial index maps 1:1 onto a FILTERED index. The GO separator splits the batches: the
-- index references columns added by the ALTER, which must be compiled first.

ALTER TABLE waiting_event ADD
  kind         NVARCHAR(16) COLLATE Latin1_General_100_BIN2 NOT NULL
                 CONSTRAINT df_waiting_event_kind DEFAULT 'MESSAGE',
  timer_due_at DATETIME2(6) NULL;
GO
CREATE INDEX waiting_event_timer_due
  ON waiting_event (deployment_id, status, timer_due_at)
  WHERE kind = 'TIMER';
