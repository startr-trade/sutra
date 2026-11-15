-- MySQL/MariaDB dialect of V305 + V306 (PostgreSQL reference: V305__inbox_seen_owner.sql and
-- V306__inbox_seen_owner_index.sql). Inbox dedup per owner: `deployment_owner` maps a deployment to
-- its owner key, and `inbox_seen.owner_id` carries it. A unique index admits any number of NULLs,
-- so rows with no owner keep deduping through the primary key alone.
--
-- utf8mb4_bin as V301. Index key: 64*4 + (128+256)*4 = 1792 bytes, inside InnoDB's 3072.
--
-- No row-security on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE deployment_owner (
  deployment_id VARCHAR(64) NOT NULL PRIMARY KEY,
  owner_id      VARCHAR(64) NOT NULL
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

ALTER TABLE inbox_seen ADD COLUMN owner_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL;

CREATE UNIQUE INDEX inbox_seen_owner_key ON inbox_seen (owner_id, channel, event_id);
