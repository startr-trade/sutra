-- SQL Server dialect of the synchronous-call chain in-flight marker (PostgreSQL reference:
-- native/V302__inflight_sync_chain.sql, which carries the lifecycle). `dedup_keys` is a JSON
-- array of [channel, event_id] pairs.
--
-- No security policy on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE inflight_sync_chain (
  deployment_id NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id   UNIQUEIDENTIFIER NOT NULL,
  dedup_keys    NVARCHAR(MAX) NOT NULL,
  holder        NVARCHAR(128) NOT NULL,
  started_at    DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_inflight_sync_chain PRIMARY KEY NONCLUSTERED (deployment_id, instance_id)
);

CREATE INDEX inflight_sync_chain_started_at ON inflight_sync_chain (started_at);
