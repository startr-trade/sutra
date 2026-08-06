-- sutra alias correlation index. One row per (deployment, instance, alias_name, alias_value) tuple
-- that the engine has written from a <q:alias> binding at instance start. The unique
-- partial index on (deployment_id, alias_name, alias_value) WHERE unique_alias=TRUE AND live=TRUE
-- enforces the unique-alias guarantee atomically: the first inserter wins and every concurrent
-- caller observes the existing row via INSERT ... ON CONFLICT DO NOTHING, with a follow-up SELECT
-- resolving whether the conflict was the same instance (idempotent re-attempt) or a different
-- live instance (unique-conflict — onConflict=reject / correlate).
--
-- Non-unique aliases (unique_alias=FALSE) coexist freely on the same (deployment, name, value); the
-- unique partial index simply ignores them. The single composite index over (deployment, name, value,
-- live) supports follow-up signal correlation lookups in O(log N) without an extra table.
--
-- Deployment scoping is enforced at two layers: every read/write carries an explicit deployment_id bind
-- from the repository, and PostgreSQL Row-Level Security keys off the sutra.deployment_id GUC set per
-- transaction via SET LOCAL (see the book's "Multi-tenancy and isolation" chapter). The `true` flag
-- on current_setting returns NULL instead of erroring when the GUC isn't set — required for
-- migration runs and bootstrap paths that legitimately run outside any deployment.

CREATE TABLE alias_index (
  deployment_id     VARCHAR(64) NOT NULL,
  instance_id   UUID NOT NULL,
  alias_name    VARCHAR(256) NOT NULL,
  alias_value   VARCHAR(1024) NOT NULL,
  unique_alias  BOOLEAN NOT NULL DEFAULT FALSE,
  live          BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, instance_id, alias_name, alias_value)
);

-- Lookup path for follow-up signal correlation: given (deployment, name, value), find the live
-- instance. Filtered partial index keeps it small for long-lived workloads where most rows are
-- retired.
CREATE INDEX alias_index_live_lookup
  ON alias_index (deployment_id, alias_name, alias_value)
  WHERE live = TRUE;

-- Atomic unique-alias enforcement. A second insert attempt with the same (deployment, name, value)
-- against a DIFFERENT live instance fails the partial unique index; the engine resolves the
-- collision via onConflict=reject / correlate.
CREATE UNIQUE INDEX alias_index_unique_live
  ON alias_index (deployment_id, alias_name, alias_value)
  WHERE unique_alias = TRUE AND live = TRUE;

ALTER TABLE alias_index ENABLE ROW LEVEL SECURITY;
CREATE POLICY alias_index_deployment_iso ON alias_index
  USING (deployment_id = current_setting('sutra.deployment_id', true));
