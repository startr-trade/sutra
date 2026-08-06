-- Schema addendum, Rust engine only: the DEPLOYMENT-scoped timer-start schedule.
--
-- FLAG FOR THE INTEGRATOR: V804 is the next free number in the waitstate V8xx family as of this
-- branch. Sibling Wave-B branches may have claimed it too — renumber on merge if it collides.
--
-- Why a table of its own rather than widening `waiting_event` (V801/V803):
--   * `waiting_event.instance_id` is NOT NULL and part of its PRIMARY KEY. A start schedule has
--     NO instance — there is no instance until it fires — so the row could only exist by making
--     a primary-key column nullable, which is not portable across the three dialects this family
--     ships (NULLs in unique keys mean different things in each).
--   * The two rows have different OWNERS and lifecycles. A `waiting_event` row is written by an
--     instance's park step and dies with the instance; a `timer_schedule` row is written by
--     DEPLOYMENT ACTIVATION and dies when the deployment stops being ACTIVE.
--   * Half the columns would not overlap: a schedule carries the repeating-cycle budget and the
--     tenant/module identity the synthesized start dispatch needs, and carries none of
--     `waiting_event`'s correlation/instance columns.
-- What the two DO share is the claim protocol, so the poller reads both the same way:
-- `status='SCHEDULED' AND next_due_at <= now ... FOR UPDATE SKIP LOCKED`.
--
-- Lifecycle (owned by the activation flip — `activate_plans`, both deploy sources):
--   activation of an ACTIVE deployment -> UPSERT one row per timer start event (re-activating a
--       still-live deployment leaves the armed due-at alone; re-activating a previously RESOLVED
--       one re-arms it from scratch, which is what a rollback must do)
--   flip-away / retire / undeploy      -> UPDATE status='RESOLVED'  [schedules follow the ACTIVE
--       deployment and never the DRAINING tail: a drained deployment must stop MINTING work even
--       though its parked instances keep resuming]
--   poller fire                        -> single-shot kinds RESOLVE; a CYCLE advances next_due_at
--       and decrements remaining_fires, resolving when the R<n> budget is spent
--
-- `spec` stores the timer text exactly as authored (`PT1H`, `2026-03-01T09:00:00Z`, `R5/PT1H`)
-- and `kind` says how to read it, so the row round-trips through one parser and no derived,
-- lossy encoding of the schedule is ever persisted.
--
-- Deployment scoping mirrors waiting_event: an explicit deployment_id bind on every statement,
-- with PostgreSQL Row-Level Security layered on below (same policy shape as V802/V403/V602/V702).

CREATE TABLE timer_schedule (
  deployment_id   VARCHAR(64)  NOT NULL,
  process_id      VARCHAR(255) NOT NULL,
  node_id         VARCHAR(255) NOT NULL,
  -- The tenant + "<tenant>/<module>/<version>" namespace key the synthesized start dispatch
  -- binds, so a fired schedule is tenant-checked and quota-checked exactly like an inbound.
  tenant          VARCHAR(255) NOT NULL,
  module_key      VARCHAR(512) NOT NULL,
  kind            VARCHAR(16)  NOT NULL,   -- DURATION | DATE | CYCLE
  spec            VARCHAR(512) NOT NULL,   -- the authored timer text, verbatim
  next_due_at     TIMESTAMP WITH TIME ZONE NOT NULL,
  -- Fires left INCLUDING the next one; NULL = unbounded (`R/…`). Single-shot kinds start at 1.
  remaining_fires INTEGER,
  status          VARCHAR(16)  NOT NULL DEFAULT 'SCHEDULED',  -- SCHEDULED | RESOLVED
  created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  resolved_at     TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (deployment_id, process_id, node_id)
);

-- The due-schedule claim's access path.
CREATE INDEX timer_schedule_due ON timer_schedule (deployment_id, status, next_due_at);

ALTER TABLE timer_schedule ENABLE ROW LEVEL SECURITY;
CREATE POLICY timer_schedule_deployment_iso ON timer_schedule
  USING (deployment_id = current_setting('sutra.deployment_id', true));
