-- SQL Server dialect of the claim/heartbeat columns (PostgreSQL reference:
-- V402__instance_claim_columns.sql). claim = CAS on claim_owner IS NULL; heartbeat
-- refreshes last_heartbeat_at; the sweeper clears claims whose heartbeat lapsed.
--
-- The reference's partial index maps 1:1 onto a SQL Server FILTERED index. The GO
-- separator splits the batches: the index references columns added by the ALTER, which
-- must be compiled first.

ALTER TABLE instance_state ADD
  claim_owner       NVARCHAR(128) NULL,
  claimed_at        DATETIME2(6) NULL,
  last_heartbeat_at DATETIME2(6) NULL;
GO
CREATE INDEX instance_state_heartbeat
  ON instance_state (deployment_id, last_heartbeat_at)
  WHERE last_heartbeat_at IS NOT NULL;
