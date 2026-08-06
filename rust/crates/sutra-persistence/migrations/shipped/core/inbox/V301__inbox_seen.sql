-- sutra inbox dedup substrate. One row per (deployment, channel, event_id) tuple the engine has
-- already observed; the unique constraint plus INSERT ... ON CONFLICT DO NOTHING gives at-least-
-- once transports exactly-once semantics at the application boundary.
--
-- Deployment scoping is enforced at two layers: every read/write carries an explicit deployment_id bind
-- from the repository, and PostgreSQL Row-Level Security keys off the sutra.deployment_id GUC set per
-- transaction via SET LOCAL (see the book's "Multi-tenancy and isolation" chapter). The `true`
-- flag on current_setting returns NULL instead of erroring when the GUC isn't set — required
-- for migration runs and bootstrap paths that legitimately run outside any deployment.

CREATE TABLE inbox_seen (
  deployment_id  VARCHAR(64) NOT NULL,
  channel    VARCHAR(128) NOT NULL,
  event_id   VARCHAR(256) NOT NULL,
  seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, channel, event_id)
);

CREATE INDEX inbox_seen_seen_at ON inbox_seen (seen_at);

ALTER TABLE inbox_seen ENABLE ROW LEVEL SECURITY;
CREATE POLICY inbox_seen_deployment_iso ON inbox_seen
  USING (deployment_id = current_setting('sutra.deployment_id', true));
