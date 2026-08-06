-- Schema addendum, Rust engine only: timer wait states reuse
-- waiting_event with a TIMER marker. This migration lives in the Rust tree (never in the
-- reference baseline's migration resources) because timers do not exist there; the shared
-- sutra_schema_history ledger keeps the two runners interoperable, and the version number
-- continues the waitstate V8xx namespace.
--
--   kind          'MESSAGE' (default — every pre-existing row) | 'TIMER'
--   timer_due_at  when a TIMER row becomes claimable (NULL on MESSAGE rows)
--
-- The timer poller claims due rows with FOR UPDATE SKIP LOCKED (the outbox
-- next_attempt_at pattern): status='WAITING' AND kind='TIMER' AND timer_due_at <= now.
-- Firing is at-least-once; the resume step resolves the row, so a redundant claim finds
-- it RESOLVED (or the instance frontier moved on) and no-ops.

ALTER TABLE waiting_event ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'MESSAGE';
ALTER TABLE waiting_event ADD COLUMN timer_due_at TIMESTAMP WITH TIME ZONE;

-- The due-timer claim's access path (partial — TIMER rows only).
CREATE INDEX waiting_event_timer_due ON waiting_event (deployment_id, status, timer_due_at)
  WHERE kind = 'TIMER';
