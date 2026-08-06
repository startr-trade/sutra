-- MySQL/MariaDB dialect of the durable wait-point registry (PostgreSQL
-- reference: V801__waiting_event.sql). One row per token parked at a wait node, keyed by
-- (deployment_id, instance_id, node_id) — a queryable projection BESIDE instance_state,
-- never a resume authority. RESOLVED rows are retained with a resolved_at stamp.
--
-- The reference's partial correlation index (WHERE correlation_key IS NOT NULL) has no
-- MySQL/MariaDB equivalent; a plain composite index stands in.
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE waiting_event (
  deployment_id   VARCHAR(64) NOT NULL,
  instance_id     BINARY(16) NOT NULL,
  node_id         VARCHAR(255) NOT NULL,
  process_id      VARCHAR(255) NOT NULL,
  correlation_key VARCHAR(512) NULL,
  status          VARCHAR(16) NOT NULL DEFAULT 'WAITING',
  created_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  resolved_at     DATETIME(6) NULL,
  PRIMARY KEY (deployment_id, instance_id, node_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- Access path for the admin listWaiting query.
CREATE INDEX waiting_event_list ON waiting_event (deployment_id, process_id, status, created_at);

-- Correlation-key lookups (display/filtering only; the key is reserved-but-nullable).
CREATE INDEX waiting_event_correlation ON waiting_event (deployment_id, correlation_key);
