-- SQL Server dialect of the durable wait-point registry (PostgreSQL reference:
-- V801__waiting_event.sql). One row per token parked at a wait node, keyed by
-- (deployment_id, instance_id, node_id) — a queryable projection BESIDE instance_state,
-- never a resume authority. RESOLVED rows are retained with a resolved_at stamp.
--
-- The reference's partial correlation index maps 1:1 onto a FILTERED index.
--
-- No security policy on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE waiting_event (
  deployment_id   NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id     UNIQUEIDENTIFIER NOT NULL,
  node_id         NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  process_id      NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  correlation_key NVARCHAR(512) COLLATE Latin1_General_100_BIN2 NULL,
  status          NVARCHAR(16) COLLATE Latin1_General_100_BIN2 NOT NULL DEFAULT 'WAITING',
  created_at      DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  resolved_at     DATETIME2(6) NULL,
  CONSTRAINT pk_waiting_event PRIMARY KEY NONCLUSTERED
    (deployment_id, instance_id, node_id)
);

-- Access path for the admin listWaiting query.
CREATE INDEX waiting_event_list ON waiting_event (deployment_id, process_id, status, created_at);

-- Correlation-key lookups (filtered — the key is reserved-but-nullable).
CREATE INDEX waiting_event_correlation ON waiting_event (deployment_id, correlation_key)
  WHERE correlation_key IS NOT NULL;
