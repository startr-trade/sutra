-- MySQL/MariaDB dialect of the ENGINE-OWNED compliance-cycle tables (PostgreSQL reference:
-- ../postgres/V904__coverage_cycle.sql, which states the contract). Re-runnable; indexes are inline
-- because MySQL 8 has no CREATE INDEX IF NOT EXISTS.
--
-- The widest key stays within InnoDB's 3072-byte index limit:
-- (64 + 128 + 512) chars * 4 bytes = 2816.
CREATE TABLE IF NOT EXISTS coverage_cycle (
  deployment_id VARCHAR(64)  NOT NULL,
  cycle_label   VARCHAR(128) NOT NULL,
  declared_at   DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (deployment_id, cycle_label)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS coverage_active_cycle (
  deployment_id VARCHAR(64)  NOT NULL,
  cycle_label   VARCHAR(128) NOT NULL,
  PRIMARY KEY (deployment_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS coverage_cycle_metric (
  deployment_id VARCHAR(64)  NOT NULL,
  cycle_label   VARCHAR(128) NOT NULL,
  path_urn      VARCHAR(512) NOT NULL,
  covered       BOOLEAN      NOT NULL DEFAULT FALSE,
  PRIMARY KEY (deployment_id, cycle_label, path_urn)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS coverage_cycle_fragment (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  deployment_id   VARCHAR(64)  NOT NULL,
  cycle_label     VARCHAR(128) NOT NULL,
  route_urn       VARCHAR(512) NOT NULL,
  segment_process VARCHAR(256) NOT NULL,
  instance_id     VARCHAR(128) NOT NULL,
  business_key    LONGTEXT     NULL,
  trace_id        LONGTEXT     NULL,
  at              DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY coverage_cycle_fragment_route (deployment_id, cycle_label, route_urn)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
