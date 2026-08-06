-- MySQL/MariaDB dialect of the inbox dedup substrate (PostgreSQL reference:
-- V301__inbox_seen.sql). One row per (deployment, channel, event_id) tuple already
-- observed; the composite PRIMARY KEY is the first-observer-wins arbiter — the store layer
-- INSERTs and treats a duplicate-key rejection as "duplicate", so at-least-once transports
-- get exactly-once semantics at the application boundary with no read-then-insert race.
--
-- utf8mb4_bin: event ids compare byte-wise (case-sensitive), matching the reference
-- dialect. PK stays inside InnoDB's 3072-byte index limit (64 + (128+256)*4 = 1600).
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE inbox_seen (
  deployment_id VARCHAR(64) NOT NULL,
  channel       VARCHAR(128) NOT NULL,
  event_id      VARCHAR(256) NOT NULL,
  seen_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (deployment_id, channel, event_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE INDEX inbox_seen_seen_at ON inbox_seen (seen_at);
