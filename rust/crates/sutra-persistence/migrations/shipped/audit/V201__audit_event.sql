-- Audit event table (the audit store).
--
-- Audit rows are deployment-scoped: deployment_id is the single isolation column,
-- enforced at two layers — an explicit bind on every statement plus the PostgreSQL Row-Level
-- Security policy below, keyed off the sutra.deployment_id GUC set per transaction via
-- set_config (see DeploymentContext). The policy ships with the migrations rather than being
-- left to the deploying host to apply. The `true` flag on
-- current_setting returns NULL when the GUC isn't set so migration runs and bootstrap paths
-- that legitimately operate outside a deployment can still write.
--
-- The (deployment_id, instance_id, seq) unique constraint is the cross-replica dedup guard. The
-- engine guarantees a monotonic per-instance seq; concurrent replicas attempting to write the
-- same seq are rejected by the constraint rather than silently overwriting each other.
CREATE TABLE audit_event (
  id              BIGSERIAL PRIMARY KEY,
  deployment_id   VARCHAR(64) NOT NULL,
  instance_id     UUID,
  seq             INTEGER NOT NULL,
  at              TIMESTAMP WITH TIME ZONE NOT NULL,
  event_type      VARCHAR(128) NOT NULL,
  node_id         VARCHAR(128),
  diagnostic_code VARCHAR(128),
  diagnostic_json TEXT,
  payload_json    TEXT NOT NULL DEFAULT '{}',
  CONSTRAINT audit_event_seq_unique UNIQUE (deployment_id, instance_id, seq)
);

CREATE INDEX audit_event_tenant_at ON audit_event (deployment_id, at DESC);
CREATE INDEX audit_event_event_type ON audit_event (event_type);
CREATE INDEX audit_event_instance ON audit_event (deployment_id, instance_id, seq);

ALTER TABLE audit_event ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_event_deployment_iso ON audit_event
  USING (deployment_id = current_setting('sutra.deployment_id', true));
