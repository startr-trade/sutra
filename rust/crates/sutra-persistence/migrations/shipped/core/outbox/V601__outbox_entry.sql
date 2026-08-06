-- Outbox table for outbound replies. One row per pending reply; the dispatcher claims rows via
-- SELECT ... FOR UPDATE SKIP LOCKED (PostgreSQL) so multiple replicas can run claim concurrently
-- without competing for the same rows.
--
-- deployment_id is the single isolation column; Row-Level Security is layered in
-- V602. labels_json carries the emitting deployment's authoring labels (tenant/module/version)
-- as PAYLOAD data — sinks rebuild the OutboundReply's display coordinates (CloudEvents ce-source,
-- per-tenant sink config keys) from it; it is never an isolation key.

CREATE TABLE outbox_entry (
  entry_id             UUID PRIMARY KEY,
  deployment_id        VARCHAR(64) NOT NULL,
  instance_id          UUID NOT NULL,

  body                 BYTEA NOT NULL,
  content_type         VARCHAR(256),
  destination          VARCHAR(2048) NOT NULL,
  headers_json         TEXT NOT NULL DEFAULT '{}',
  required             BOOLEAN NOT NULL DEFAULT TRUE,
  mode                 VARCHAR(32) NOT NULL DEFAULT 'NATIVE',
  outbox_key           VARCHAR(128) NOT NULL,
  cloud_event_json     TEXT,
  auth_ref_json        TEXT,
  labels_json          TEXT NOT NULL DEFAULT '{}',

  created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  next_attempt_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  attempt_count        INTEGER NOT NULL DEFAULT 0,
  last_diagnostic_json TEXT
);

CREATE INDEX outbox_entry_due ON outbox_entry (deployment_id, next_attempt_at);
CREATE INDEX outbox_entry_instance ON outbox_entry (deployment_id, instance_id);
