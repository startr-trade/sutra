-- SQL Server dialect of the audit event table (PostgreSQL reference:
-- V201__audit_event.sql). The (deployment_id, instance_id, seq) uniqueness is the
-- cross-replica dedup guard. SQL Server unique CONSTRAINTs treat NULLs as equal (unlike
-- the reference dialect), so the guard is a FILTERED unique index over rows with an
-- instance id — identical semantics: per-instance seq uniqueness enforced, NULL-instance
-- system events unconstrained.
--
-- No security policy on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE audit_event (
  id              BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_audit_event PRIMARY KEY,
  deployment_id   NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id     UNIQUEIDENTIFIER NULL,
  seq             INT NOT NULL,
  at              DATETIME2(6) NOT NULL,
  event_type      NVARCHAR(128) NOT NULL,
  node_id         NVARCHAR(128) NULL,
  diagnostic_code NVARCHAR(128) NULL,
  diagnostic_json NVARCHAR(MAX) NULL,
  payload_json    NVARCHAR(MAX) NOT NULL DEFAULT '{}'
);

CREATE UNIQUE INDEX audit_event_seq_unique
  ON audit_event (deployment_id, instance_id, seq)
  WHERE instance_id IS NOT NULL;

CREATE INDEX audit_event_tenant_at ON audit_event (deployment_id, at DESC);
CREATE INDEX audit_event_event_type ON audit_event (event_type);
CREATE INDEX audit_event_instance ON audit_event (deployment_id, instance_id, seq);
