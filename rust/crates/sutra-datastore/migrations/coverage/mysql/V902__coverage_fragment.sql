-- MySQL/MariaDB dialect of the ENGINE-OWNED coverage reconstruction-fragment table (PostgreSQL
-- reference: ../postgres/V902__coverage_fragment.sql). Idempotent + inline index for the same
-- reasons as V901.
--
-- utf8mb4 + binary collation. business_key / trace_id are LONGTEXT (portable, arbitrary UTF-8);
-- `at` defaults to CURRENT_TIMESTAMP(6) mirroring the reference `DEFAULT now()`.
CREATE TABLE IF NOT EXISTS coverage_fragment (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  deployment_id   VARCHAR(64)  NOT NULL,
  route_urn       VARCHAR(512) NOT NULL,
  segment_process VARCHAR(256) NOT NULL,
  instance_id     VARCHAR(128) NOT NULL,
  business_key    LONGTEXT     NULL,
  trace_id        LONGTEXT     NULL,
  at              DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY coverage_fragment_route (deployment_id, route_urn)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
