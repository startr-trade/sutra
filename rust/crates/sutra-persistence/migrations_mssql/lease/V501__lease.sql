-- SQL Server dialect of the durable lease table (PostgreSQL reference:
-- V501__lease.sql). Holder + ttl semantics, one row per lease name; NOT deployment-scoped
-- (leases are a process-level primitive). The store resolves acquire-or-renew contention
-- with a single MERGE WITH (HOLDLOCK) ... OUTPUT inserted.* — the atomic equivalent of the
-- reference's conditional-upsert-RETURNING: exactly one winner, losers get no row back.

CREATE TABLE lease (
  name        NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL
                CONSTRAINT pk_lease PRIMARY KEY,
  holder      NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  acquired_at DATETIME2(6) NOT NULL,
  expires_at  DATETIME2(6) NOT NULL
);

CREATE INDEX lease_expires_at ON lease (expires_at);
