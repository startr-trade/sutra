-- Stuck-instance scanner support. Adds claim ownership + heartbeat columns so a replica that
-- successfully claimed an instance can broadcast liveness; if its heartbeat lapses past the
-- configured claim timeout, the StuckInstanceScanner sweeps the row back to un-claimed.
--
-- The partial index keys (deployment_id, last_heartbeat_at) WHERE last_heartbeat_at IS NOT NULL
-- keeps the scan over claimed instances small even when the table is dominated by stable rows
-- that nothing is currently working on.

ALTER TABLE instance_state
  ADD COLUMN claim_owner       VARCHAR(128),
  ADD COLUMN claimed_at        TIMESTAMPTZ,
  ADD COLUMN last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX instance_state_heartbeat
  ON instance_state (deployment_id, last_heartbeat_at)
  WHERE last_heartbeat_at IS NOT NULL;
