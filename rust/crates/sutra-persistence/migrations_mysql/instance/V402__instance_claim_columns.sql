-- MySQL/MariaDB dialect of the claim/heartbeat columns (PostgreSQL reference:
-- V402__instance_claim_columns.sql). claim = CAS on claim_owner IS NULL; heartbeat
-- refreshes last_heartbeat_at; the sweeper clears claims whose heartbeat lapsed.
--
-- The reference dialect's partial index (WHERE last_heartbeat_at IS NOT NULL) has no
-- MySQL/MariaDB equivalent; a plain composite index carries the sweep scan instead.

ALTER TABLE instance_state
  ADD COLUMN claim_owner       VARCHAR(128) NULL,
  ADD COLUMN claimed_at        DATETIME(6) NULL,
  ADD COLUMN last_heartbeat_at DATETIME(6) NULL;

CREATE INDEX instance_state_heartbeat
  ON instance_state (deployment_id, last_heartbeat_at);
