-- Durable dead-letter / incident record. One row per non-idempotent
-- inbound execution failure the dispatcher consumed at-most-once (the IncidentSink seam —
-- sutra-channels/src/stores.rs InboundIncident/IncidentSink; emitted at dispatch.rs
-- gate_execution_failure). Mirrors InboundIncident's fields 1:1 — there is deliberately no
-- instance_id/payload/seq column: a dead-lettered arrival fails BEFORE any quiescent commit,
-- so no instance is ever persisted for it. This table is the durable floor beneath the
-- always-on tracing::error! log floor; writing it is gated by sutra.incident.sql (opt-in,
-- like sutra.audit.sql), best-effort — a failed insert never changes the ack decision.
--
-- Deployment-scoped: deployment_id is the single isolation column, enforced at two
-- layers — an explicit bind on every statement plus the PostgreSQL Row-Level Security policy
-- below, keyed off the sutra.deployment_id GUC set per transaction. The `true` flag on
-- current_setting returns NULL (not an error) when the GUC isn't set, so migration + bootstrap
-- paths that legitimately run outside a deployment still work.
CREATE TABLE dead_letter (
  id              BIGSERIAL PRIMARY KEY,
  deployment_id   VARCHAR(64) NOT NULL,
  channel         VARCHAR(256) NOT NULL,
  process_id      VARCHAR(256) NOT NULL,
  dedup_key       VARCHAR(512) NOT NULL DEFAULT '',
  failure_code    VARCHAR(128) NOT NULL,
  detail          TEXT NOT NULL,
  received_at     TIMESTAMP WITH TIME ZONE NOT NULL,
  recorded_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX dead_letter_deployment_recorded ON dead_letter (deployment_id, recorded_at DESC);
CREATE INDEX dead_letter_channel ON dead_letter (deployment_id, channel);

ALTER TABLE dead_letter ENABLE ROW LEVEL SECURITY;
CREATE POLICY dead_letter_deployment_iso ON dead_letter
  USING (deployment_id = current_setting('sutra.deployment_id', true));
