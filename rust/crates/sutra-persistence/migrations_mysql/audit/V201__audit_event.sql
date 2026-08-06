-- MySQL/MariaDB dialect of the audit event table (PostgreSQL reference:
-- V201__audit_event.sql). The (deployment_id, instance_id, seq) unique constraint is the
-- cross-replica dedup guard; NULL instance_id rows never collide (MySQL unique indexes
-- treat NULLs as distinct — the same semantics the PostgreSQL constraint has).
--
-- utf8mb4 + binary collation: payload/diagnostic text is arbitrary UTF-8; comparisons stay
-- byte-wise like the reference dialect. No TEXT-column DEFAULT here (dialect limitation —
-- MySQL literal defaults on TEXT need expression syntax MariaDB parses differently);
-- writers bind payload_json explicitly on every INSERT.
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE audit_event (
  id              BIGINT AUTO_INCREMENT PRIMARY KEY,
  deployment_id   VARCHAR(64) NOT NULL,
  instance_id     BINARY(16) NULL,
  seq             INT NOT NULL,
  at              DATETIME(6) NOT NULL,
  event_type      VARCHAR(128) NOT NULL,
  node_id         VARCHAR(128) NULL,
  diagnostic_code VARCHAR(128) NULL,
  diagnostic_json LONGTEXT NULL,
  payload_json    LONGTEXT NOT NULL,
  CONSTRAINT audit_event_seq_unique UNIQUE (deployment_id, instance_id, seq)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE INDEX audit_event_tenant_at ON audit_event (deployment_id, at DESC);
CREATE INDEX audit_event_event_type ON audit_event (event_type);
CREATE INDEX audit_event_instance ON audit_event (deployment_id, instance_id, seq);
