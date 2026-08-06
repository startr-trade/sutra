-- SQL Server dialect of the inbox dedup substrate (PostgreSQL reference:
-- V301__inbox_seen.sql). The composite PRIMARY KEY is the first-observer-wins arbiter —
-- the store layer INSERTs and treats a duplicate-key rejection as "duplicate", giving
-- at-least-once transports exactly-once semantics with no read-then-insert race.
--
-- BIN2 collation: event ids compare byte-wise (case-sensitive) like the reference dialect.
-- NONCLUSTERED PK: the NVARCHAR key (128 + 256+512 bytes) clears the 1700-byte limit.
--
-- No security policy on this dialect: enforced-bind posture, see V101 header.

CREATE TABLE inbox_seen (
  deployment_id NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  channel       NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  event_id      NVARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
  seen_at       DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_inbox_seen PRIMARY KEY NONCLUSTERED (deployment_id, channel, event_id)
);

CREATE INDEX inbox_seen_seen_at ON inbox_seen (seen_at);
