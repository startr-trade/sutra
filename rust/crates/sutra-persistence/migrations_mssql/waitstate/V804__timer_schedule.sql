-- SQL Server dialect of the deployment-scoped timer-start schedule
-- (PostgreSQL reference: V804__timer_schedule.sql in the Rust tree's `native` addendum, which
-- carries the full rationale for why this is a table of its own rather than a widened
-- waiting_event: a start schedule has no instance, and `waiting_event.instance_id` is a NOT NULL
-- primary-key column).
--
-- FLAG FOR THE INTEGRATOR: V804 is the next free waitstate V8xx number on this branch; sibling
-- Wave-B branches may collide — renumber on merge.
--
-- No row-security on this dialect: enforced-bind posture, see V101 header. The GO separator
-- splits the batches: the index references columns created by the preceding CREATE TABLE.

CREATE TABLE timer_schedule (
  deployment_id   VARCHAR(64)  NOT NULL,
  process_id      NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  node_id         NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  tenant          NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  module_key      NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NOT NULL,
  kind            NVARCHAR(16)  COLLATE Latin1_General_100_BIN2 NOT NULL,
  spec            NVARCHAR(512) COLLATE Latin1_General_100_BIN2 NOT NULL,
  next_due_at     DATETIME2(6)  NOT NULL,
  remaining_fires INT NULL,
  status          NVARCHAR(16)  COLLATE Latin1_General_100_BIN2 NOT NULL
                    CONSTRAINT df_timer_schedule_status DEFAULT 'SCHEDULED',
  created_at      DATETIME2(6)  NOT NULL
                    CONSTRAINT df_timer_schedule_created_at DEFAULT SYSUTCDATETIME(),
  resolved_at     DATETIME2(6)  NULL,
  CONSTRAINT pk_timer_schedule PRIMARY KEY (deployment_id, process_id, node_id)
);
GO
-- The due-schedule claim's access path.
CREATE INDEX timer_schedule_due ON timer_schedule (deployment_id, status, next_due_at);
