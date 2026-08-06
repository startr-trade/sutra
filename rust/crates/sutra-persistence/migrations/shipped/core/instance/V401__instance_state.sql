-- sutra recovery substrate. Holds serialised process instance state keyed by
-- (deployment_id, instance_id). The composite primary key doubles as the conflict target for the
-- UPSERT issued by the instance store on persist.
--
-- deployment_id is the single isolation column: the instance's opaque DeploymentId
-- pin, mirrored inside the snapshot bytes as sutra.deploymentId. Identity does not decompose at
-- runtime — there are deliberately no module_version / target_namespace columns.
--
-- Deployment scoping: every read/write carries an explicit deployment_id bind from the repository,
-- and PostgreSQL Row-Level Security is layered in V403 (absent here so this runs cleanly on H2).
--
-- updated_at is auto-set on insert and refreshed on every UPSERT update via CURRENT_TIMESTAMP
-- in the application SQL. The trailing index supports recency scans (e.g. "find instances last
-- touched > 24h ago" for the orphan-recovery sweeper).

CREATE TABLE instance_state (
  deployment_id  VARCHAR(64) NOT NULL,
  instance_id    UUID NOT NULL,
  serialised     BYTEA NOT NULL,
  updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (deployment_id, instance_id)
);

CREATE INDEX instance_state_updated_at ON instance_state (updated_at);
