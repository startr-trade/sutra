-- MySQL/MariaDB dialect of the outbox table (PostgreSQL reference:
-- V601__outbox_entry.sql). Row-exists = pending (no status column); the dispatcher claims
-- due rows with SELECT ... FOR UPDATE SKIP LOCKED (supported on MySQL 8 / MariaDB 10.6+),
-- so concurrent replicas never compete for the same rows. deployment_id is the single
-- isolation column, enforced-bind posture (see V101 header).
--
-- Dialect notes: BINARY(16) ids, LONGBLOB body, DATETIME(6) UTC timestamps, LONGTEXT for
-- the opaque JSON columns. The reference's TEXT DEFAULT '{}' is omitted (see V201 note);
-- the store binds headers_json/labels_json on every INSERT.

CREATE TABLE outbox_entry (
  entry_id             BINARY(16) PRIMARY KEY,
  deployment_id        VARCHAR(64) NOT NULL,
  instance_id          BINARY(16) NOT NULL,

  body                 LONGBLOB NOT NULL,
  content_type         VARCHAR(256) NULL,
  destination          VARCHAR(2048) NOT NULL,
  headers_json         LONGTEXT NOT NULL,
  required             BOOLEAN NOT NULL DEFAULT TRUE,
  mode                 VARCHAR(32) NOT NULL DEFAULT 'NATIVE',
  outbox_key           VARCHAR(128) NOT NULL,
  cloud_event_json     LONGTEXT NULL,
  auth_ref_json        LONGTEXT NULL,
  labels_json          LONGTEXT NOT NULL,

  created_at           DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  next_attempt_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  attempt_count        INT NOT NULL DEFAULT 0,
  last_diagnostic_json LONGTEXT NULL
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE INDEX outbox_entry_due ON outbox_entry (deployment_id, next_attempt_at);
CREATE INDEX outbox_entry_instance ON outbox_entry (deployment_id, instance_id);
