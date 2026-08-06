-- MySQL/MariaDB dialect of the durable lease table (PostgreSQL reference:
-- V501__lease.sql). Holder + ttl semantics, one row per lease name; NOT deployment-scoped
-- (leases are a process-level primitive). The store resolves acquire-or-renew contention
-- with a row-locking INSERT ... ON DUPLICATE KEY UPDATE inside a transaction — exactly one
-- winner, same guarantee the reference dialect gets from its conditional-upsert-RETURNING.

CREATE TABLE lease (
  name        VARCHAR(128) PRIMARY KEY,
  holder      VARCHAR(128) NOT NULL,
  acquired_at DATETIME(6) NOT NULL,
  expires_at  DATETIME(6) NOT NULL
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE INDEX lease_expires_at ON lease (expires_at);
