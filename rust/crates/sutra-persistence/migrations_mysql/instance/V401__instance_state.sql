-- MySQL/MariaDB dialect of the instance recovery substrate (PostgreSQL
-- reference: V401__instance_state.sql). Holds the serialised snapshot v2 bytes keyed by
-- (deployment_id, instance_id); the composite primary key doubles as the conflict target
-- for the store's INSERT ... ON DUPLICATE KEY UPDATE upsert.
--
-- serialised is LONGBLOB: snapshot v2 is a byte-deterministic blob and must
-- round-trip byte-identically — no charset, no conversion. Timestamps are DATETIME(6)
-- (microsecond precision, matching the reference's timestamptz resolution); every session
-- runs at time_zone '+00:00' so CURRENT_TIMESTAMP(6) is UTC.
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE instance_state (
  deployment_id VARCHAR(64) NOT NULL,
  instance_id   BINARY(16) NOT NULL,
  serialised    LONGBLOB NOT NULL,
  updated_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (deployment_id, instance_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE INDEX instance_state_updated_at ON instance_state (updated_at);
