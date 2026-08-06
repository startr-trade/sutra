-- SQL Server dialect of the per-channel concurrency substrate (PostgreSQL
-- reference: V701__channel_instance.sql). One row per non-terminal instance; the
-- (deployment_id, instance_id) primary key is the MERGE conflict target (an instance
-- lives on exactly one channel); the (deployment_id, channel, status) index carries the
-- replica-coherent admission COUNT.
--
-- No security policy on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE channel_instance (
  deployment_id NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id   UNIQUEIDENTIFIER NOT NULL,
  channel       NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
  status        NVARCHAR(16) COLLATE Latin1_General_100_BIN2 NOT NULL,
  updated_at    DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_channel_instance PRIMARY KEY (deployment_id, instance_id)
);

CREATE INDEX channel_instance_count ON channel_instance (deployment_id, channel, status);
