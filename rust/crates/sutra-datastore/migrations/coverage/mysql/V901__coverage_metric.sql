-- MySQL/MariaDB dialect of the ENGINE-OWNED coverage metric-flag table (PostgreSQL reference:
-- ../postgres/V901__coverage_metric.sql — read it for the ownership + idempotence contract).
--
-- Shipped inside the engine and applied to the USER-DECLARED `coverage` store's own connection on
-- first use, so it must be re-runnable. `CREATE TABLE IF NOT
-- EXISTS` carries that; the secondary index is declared INLINE rather than as a separate
-- `CREATE INDEX` because MySQL 8 has no `CREATE INDEX IF NOT EXISTS` (MariaDB does) — inline, it
-- inherits the table statement's idempotence on both engines.
--
-- utf8mb4 + binary collation: path URNs are compared byte-wise like the reference dialect. The
-- (deployment_id, path_urn) PRIMARY KEY makes the seed idempotent (insert + duplicate-key
-- rejection). The composite key stays within InnoDB's 3072-byte index limit
-- (64 + 512 chars * 4 bytes = 2304).
--
-- No row-security on this dialect — none exists. The explicit `deployment_id` bind on every
-- statement is the isolation, which is also the posture the reference dialect takes on a
-- user-owned connection.
CREATE TABLE IF NOT EXISTS coverage_metric (
  deployment_id VARCHAR(64)  NOT NULL,
  path_urn      VARCHAR(512) NOT NULL,
  covered       BOOLEAN      NOT NULL DEFAULT FALSE,
  CONSTRAINT coverage_metric_pk PRIMARY KEY (deployment_id, path_urn),
  KEY coverage_metric_deployment_covered (deployment_id, covered)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
