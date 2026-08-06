-- SQL Server dialect of the external-task (pull) parking table (PostgreSQL reference:
-- V605__external_task.sql, which carries the rationale for the table existing at all).
-- deployment_id is the single isolation column, enforced-bind posture (see V101 header).
--
-- The GO separators split the batches: each index references columns created by the preceding
-- CREATE TABLE. SQL Server has no boolean type, so `failed` is a BIT with a NAMED default
-- constraint (an unnamed one gets a generated name a later migration could not reference).

CREATE TABLE external_task (
  task_id         UNIQUEIDENTIFIER NOT NULL CONSTRAINT pk_external_task PRIMARY KEY,
  deployment_id   NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id     UNIQUEIDENTIFIER NOT NULL,
  channel         NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  tenant          NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  module_key      NVARCHAR(450) COLLATE Latin1_General_100_BIN2 NOT NULL,

  body            VARBINARY(MAX) NOT NULL,
  content_type    NVARCHAR(256) NULL,
  headers_json    NVARCHAR(MAX) NOT NULL
                    CONSTRAINT df_external_task_headers_json DEFAULT '{}',
  outbox_key      NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  traceparent     NVARCHAR(64)  NULL,

  created_at      DATETIME2(6)  NOT NULL
                    CONSTRAINT df_external_task_created_at DEFAULT SYSUTCDATETIME(),
  fetchable_at    DATETIME2(6)  NOT NULL
                    CONSTRAINT df_external_task_fetchable_at DEFAULT SYSUTCDATETIME(),
  lock_owner      NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
  lock_expires_at DATETIME2(6)  NULL,

  attempt_count   INT NOT NULL CONSTRAINT df_external_task_attempt_count DEFAULT 0,
  retries_left    INT NOT NULL CONSTRAINT df_external_task_retries_left DEFAULT 3,
  failed          BIT NOT NULL CONSTRAINT df_external_task_failed DEFAULT 0,
  last_error      NVARCHAR(MAX) NULL
);
GO
-- Idempotent parking: a re-delivered outbox row must not create a second task.
CREATE UNIQUE INDEX external_task_key ON external_task (deployment_id, outbox_key);
GO
-- The fetch-and-lock claim's access path (live rows only — SQL Server does have filtered
-- indexes, so this mirrors the reference dialect 1:1).
CREATE INDEX external_task_fetchable ON external_task (deployment_id, channel, fetchable_at)
    WHERE failed = 0;
GO
CREATE INDEX external_task_instance ON external_task (deployment_id, instance_id);
