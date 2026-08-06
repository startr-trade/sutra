-- MySQL/MariaDB dialect of the deployment-scoped timer-start schedule
-- (PostgreSQL reference: V804__timer_schedule.sql in the Rust tree's `native` addendum, which
-- carries the full rationale for why this is a table of its own rather than a widened
-- waiting_event: a start schedule has no instance, and `waiting_event.instance_id` is a NOT NULL
-- primary-key column).
--
-- FLAG FOR THE INTEGRATOR: V804 is the next free waitstate V8xx number on this branch; sibling
-- Wave-B branches may collide — renumber on merge.
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE timer_schedule (
  deployment_id   VARCHAR(64)  NOT NULL,
  process_id      VARCHAR(255) NOT NULL,
  node_id         VARCHAR(255) NOT NULL,
  tenant          VARCHAR(255) NOT NULL,
  module_key      VARCHAR(512) NOT NULL,
  kind            VARCHAR(16)  NOT NULL,
  spec            VARCHAR(512) NOT NULL,
  next_due_at     DATETIME(6)  NOT NULL,
  remaining_fires INT NULL,
  status          VARCHAR(16)  NOT NULL DEFAULT 'SCHEDULED',
  created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  resolved_at     DATETIME(6)  NULL,
  PRIMARY KEY (deployment_id, process_id, node_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- The due-schedule claim's access path.
CREATE INDEX timer_schedule_due ON timer_schedule (deployment_id, status, next_due_at);
