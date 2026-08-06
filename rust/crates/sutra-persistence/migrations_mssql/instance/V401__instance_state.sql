-- SQL Server dialect of the instance recovery substrate (PostgreSQL reference:
-- V401__instance_state.sql). Holds the serialised snapshot v2 bytes keyed by
-- (deployment_id, instance_id); the store upserts via MERGE WITH (HOLDLOCK), the atomic
-- equivalent of the reference's INSERT ... ON CONFLICT DO UPDATE.
--
-- serialised is VARBINARY(MAX): snapshot v2 is a byte-deterministic blob and must
-- round-trip byte-identically. Timestamps are DATETIME2(6) written with SYSUTCDATETIME()
-- (UTC, microsecond precision — matching the reference's timestamptz resolution).
--
-- No security policy on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE instance_state (
  deployment_id NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id   UNIQUEIDENTIFIER NOT NULL,
  serialised    VARBINARY(MAX) NOT NULL,
  updated_at    DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_instance_state PRIMARY KEY (deployment_id, instance_id)
);

CREATE INDEX instance_state_updated_at ON instance_state (updated_at);
