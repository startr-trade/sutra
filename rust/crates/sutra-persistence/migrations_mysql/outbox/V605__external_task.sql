-- MySQL/MariaDB dialect of the external-task (pull) parking table (PostgreSQL reference:
-- V605__external_task.sql, which carries the rationale for the table existing at all).
-- deployment_id is the single isolation column, enforced-bind posture (see V101 header).
--
-- Dialect notes: BINARY(16) ids, LONGBLOB body, DATETIME(6) UTC timestamps, LONGTEXT for the
-- opaque JSON/error columns. MySQL has no filtered indexes, so the fetch index is the plain
-- composite — the claim predicate leads with deployment_id + channel, which it still serves.

CREATE TABLE external_task (
  task_id         BINARY(16) PRIMARY KEY,
  deployment_id   VARCHAR(64)  NOT NULL,
  instance_id     BINARY(16)   NOT NULL,
  channel         VARCHAR(255) NOT NULL,
  tenant          VARCHAR(255) NOT NULL,
  module_key      VARCHAR(512) NOT NULL,

  body            LONGBLOB     NOT NULL,
  content_type    VARCHAR(256) NULL,
  headers_json    LONGTEXT     NOT NULL,
  outbox_key      VARCHAR(128) NOT NULL,
  traceparent     VARCHAR(64)  NULL,

  created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  fetchable_at    DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  lock_owner      VARCHAR(255) NULL,
  lock_expires_at DATETIME(6)  NULL,

  attempt_count   INT          NOT NULL DEFAULT 0,
  retries_left    INT          NOT NULL DEFAULT 3,
  failed          BOOLEAN      NOT NULL DEFAULT FALSE,
  last_error      LONGTEXT     NULL
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE UNIQUE INDEX external_task_key ON external_task (deployment_id, outbox_key);
CREATE INDEX external_task_fetchable ON external_task (deployment_id, channel, fetchable_at);
CREATE INDEX external_task_instance ON external_task (deployment_id, instance_id);
