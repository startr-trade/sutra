-- MySQL/MariaDB dialect of the timer schema addendum
-- (PostgreSQL reference: V803__waiting_event_timer.sql in the Rust tree). Timer wait states
-- reuse waiting_event with a TIMER marker:
--
--   kind          'MESSAGE' (default — every pre-existing row) | 'TIMER'
--   timer_due_at  when a TIMER row becomes claimable (NULL on MESSAGE rows)
--
-- The timer poller claims due rows with FOR UPDATE SKIP LOCKED (the outbox
-- next_attempt_at pattern). The reference's partial index (WHERE kind = 'TIMER') has no
-- MySQL/MariaDB equivalent; `kind` joins the plain composite index instead.

ALTER TABLE waiting_event ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'MESSAGE';
ALTER TABLE waiting_event ADD COLUMN timer_due_at DATETIME(6) NULL;

CREATE INDEX waiting_event_timer_due
  ON waiting_event (deployment_id, kind, status, timer_due_at);
