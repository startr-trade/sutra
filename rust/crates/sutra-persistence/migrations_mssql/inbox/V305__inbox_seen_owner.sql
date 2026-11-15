-- SQL Server dialect of V305 + V306 (PostgreSQL reference: V305__inbox_seen_owner.sql and
-- V306__inbox_seen_owner_index.sql). Inbox dedup per owner: `deployment_owner` maps a deployment to
-- its owner key, and `inbox_seen.owner_id` carries it. The unique index is FILTERED to rows that
-- carry an owner, so rows with no owner keep deduping through the primary key alone.
--
-- BIN2 collation as V301. Index key: (64 + 128 + 256) * 2 = 896 bytes, inside the 1700 limit.
--
-- No security policy on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE deployment_owner (
  deployment_id NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  owner_id      NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  CONSTRAINT pk_deployment_owner PRIMARY KEY (deployment_id)
);

ALTER TABLE inbox_seen ADD owner_id NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NULL;
-- A batch is compiled before it runs, so the index that names the new column is a batch of its own.
GO

CREATE UNIQUE INDEX inbox_seen_owner_key ON inbox_seen (owner_id, channel, event_id)
  WHERE owner_id IS NOT NULL;
