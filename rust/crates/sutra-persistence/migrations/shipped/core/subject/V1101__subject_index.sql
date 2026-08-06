-- GDPR subject blind index. One row per (deployment, instance, subject_name, blind_value) tuple
-- that the engine has written from a <q:variable subjectKey="true"> binding at persist time.
-- `blind_value` is HMAC-SHA256(indexKey, normalize(value)) — a one-way, non-recoverable digest
-- keyed by a migration-stable indexKey (never the deployment id, so a version migration does
-- not invalidate existing blind-index rows). Neither the cleartext subject value nor a
-- recoverable ciphertext is stored here; the blind index exists solely so a GDPR
-- disclosure/erasure request can locate the instances that touched a given subject value
-- without decrypting anything.
--
-- Unlike alias_index's follow-up-correlation lookup (which only ever wants the LIVE owner),
-- the subject-index lookup backs the disclosure query and the erasure cascade:
-- "every instance that ever recorded this subject value, whether still live or already
-- retired/terminal". A retired instance is exactly the kind of row an erasure request needs
-- to find, so `subject_index_lookup` below carries **no `WHERE live` filter** — retired rows
-- must stay just as discoverable as live ones. `live` itself is retained as a descriptive flag
-- (mirrors alias_index's instance-lifecycle bookkeeping) rather than as a lookup-narrowing
-- predicate.
--
-- Deployment scoping follows the same two-layer pattern as alias_index: every read/write
-- carries an explicit deployment_id bind from the repository, and PostgreSQL Row-Level
-- Security keys off the sutra.deployment_id GUC set per transaction via SET LOCAL (see the
-- book's "Multi-tenancy and isolation" chapter). The `true` flag on current_setting returns
-- NULL instead of erroring when the GUC isn't set — required for migration runs and bootstrap
-- paths that legitimately run outside any deployment.

CREATE TABLE subject_index (
  deployment_id     VARCHAR(64) NOT NULL,
  instance_id       UUID NOT NULL,
  subject_name      VARCHAR(256) NOT NULL,
  blind_value       VARCHAR(128) NOT NULL,
  live              BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, instance_id, subject_name, blind_value)
);

-- Disclosure lookup path: given (deployment, subject_name, blind_value),
-- find EVERY instance that ever recorded it — live and retired alike. Deliberately not a
-- partial index (contrast alias_index_live_lookup's `WHERE live = TRUE`): retired rows are
-- first-class citizens of this lookup, not noise to be filtered out.
CREATE INDEX subject_index_lookup
  ON subject_index (deployment_id, subject_name, blind_value);

ALTER TABLE subject_index ENABLE ROW LEVEL SECURITY;
CREATE POLICY subject_index_deployment_iso ON subject_index
  USING (deployment_id = current_setting('sutra.deployment_id', true));
