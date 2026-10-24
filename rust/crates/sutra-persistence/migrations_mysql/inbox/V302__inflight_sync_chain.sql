-- MySQL/MariaDB dialect of the synchronous-call chain in-flight marker (PostgreSQL reference:
-- native/V302__inflight_sync_chain.sql, which carries the lifecycle). `dedup_keys` is a JSON
-- array of [channel, event_id] pairs.
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE inflight_sync_chain (
  deployment_id VARCHAR(64)  NOT NULL,
  instance_id   BINARY(16)   NOT NULL,
  dedup_keys    TEXT         NOT NULL,
  holder        VARCHAR(128) NOT NULL,
  started_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (deployment_id, instance_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE INDEX inflight_sync_chain_started_at ON inflight_sync_chain (started_at);
