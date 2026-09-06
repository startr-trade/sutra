-- Coverage metric-flag table — PostgreSQL dialect of the ENGINE-OWNED coverage schema.
--
-- SUPERSEDING RULING 2026-08-04: coverage marks persist in
-- the USER-DECLARED `coverage` data store — the author picks the database by pointing that
-- store's `sql.url` wherever they like — but the SCHEMA is the engine's. The author writes no
-- coverage SQL; this script is shipped INSIDE the engine binary (include_str! in
-- sutra-datastore's `coverage` module) and applied to the declared store's own connection on
-- first use, under the same advisory-lock-serialised, ledger-less first-use path a module's own
-- `migrations/<store>/` scripts take. Hence every statement is IDEMPOTENT: a re-run on the next
-- boot, on a replica, or after a redeploy must be a no-op.
--
-- Every declared coverage path — intra- and cross-process, by fully-qualified URN id — is seeded
-- covered=false at deploy / `coverage init` / reset (the "total to cover"), and flipped
-- covered=true when exercised. total / covered / coveragePercentage + the uncovered set derive
-- directly off these flags, as SQL aggregates.
--
-- Isolation is `deployment_id`: a column, and a predicate bound on EVERY statement. Row-level
-- security is deliberately NOT enabled here (§7). RLS is an engine-database convention — it
-- needs table ownership, a `sutra.deployment_id` GUC set per transaction, and a policy the
-- engine controls. None of that transfers to an arbitrary user-owned connection whose role may
-- not even own the table, and two of the three supported dialects have no equivalent at all. The
-- enforced bind is the isolation, exactly as it is for every non-PostgreSQL system store.
--
-- Idempotent seed: the (deployment_id, path_urn) PRIMARY KEY makes the seed an
-- `INSERT ... ON CONFLICT DO NOTHING`, so redeploy / reset never clobbers an already-covered flag.
CREATE TABLE IF NOT EXISTS coverage_metric (
  deployment_id VARCHAR(64)  NOT NULL,
  path_urn      VARCHAR(512) NOT NULL,
  covered       BOOLEAN      NOT NULL DEFAULT false,
  CONSTRAINT coverage_metric_pk PRIMARY KEY (deployment_id, path_urn)
);

-- Fast "uncovered set" / percentage scans within a deployment.
CREATE INDEX IF NOT EXISTS coverage_metric_deployment_covered
  ON coverage_metric (deployment_id, covered);
