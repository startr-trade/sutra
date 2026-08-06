-- MySQL/MariaDB dialect of the per-channel concurrency substrate (PostgreSQL
-- reference: V701__channel_instance.sql). One row per non-terminal instance; the
-- (deployment_id, instance_id) primary key is the upsert conflict target (an instance
-- lives on exactly one channel); the (deployment_id, channel, status) index carries the
-- replica-coherent admission COUNT(*).
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE channel_instance (
  deployment_id VARCHAR(64) NOT NULL,
  instance_id   BINARY(16) NOT NULL,
  channel       VARCHAR(255) NOT NULL,
  status        VARCHAR(16) NOT NULL,
  updated_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (deployment_id, instance_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE INDEX channel_instance_count ON channel_instance (deployment_id, channel, status);
