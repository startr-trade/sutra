-- SQL Server dialect of the outbox table (PostgreSQL reference:
-- V601__outbox_entry.sql). Row-exists = pending (no status column); the dispatcher claims
-- due rows with SELECT TOP (n) ... WITH (UPDLOCK, ROWLOCK, READPAST) — the SQL Server
-- equivalent of FOR UPDATE SKIP LOCKED: concurrent replicas lock disjoint rows and never
-- block on each other. deployment_id is the single isolation column, enforced-bind
-- posture (see V101 header).

CREATE TABLE outbox_entry (
  entry_id             UNIQUEIDENTIFIER NOT NULL CONSTRAINT pk_outbox_entry PRIMARY KEY,
  deployment_id        NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id          UNIQUEIDENTIFIER NOT NULL,

  body                 VARBINARY(MAX) NOT NULL,
  content_type         NVARCHAR(256) NULL,
  destination          NVARCHAR(2048) NOT NULL,
  headers_json         NVARCHAR(MAX) NOT NULL DEFAULT '{}',
  required             BIT NOT NULL DEFAULT 1,
  mode                 NVARCHAR(32) NOT NULL DEFAULT 'NATIVE',
  outbox_key           NVARCHAR(128) NOT NULL,
  cloud_event_json     NVARCHAR(MAX) NULL,
  auth_ref_json        NVARCHAR(MAX) NULL,
  labels_json          NVARCHAR(MAX) NOT NULL DEFAULT '{}',

  created_at           DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  next_attempt_at      DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  attempt_count        INT NOT NULL DEFAULT 0,
  last_diagnostic_json NVARCHAR(MAX) NULL
);

CREATE INDEX outbox_entry_due ON outbox_entry (deployment_id, next_attempt_at);
CREATE INDEX outbox_entry_instance ON outbox_entry (deployment_id, instance_id);
