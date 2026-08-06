-- sutra durable lease table — leader-election fallback for environments without a native
-- Kubernetes Lease. Holder + ttl semantics: a single row per lease name; acquire/renew/release
-- mutate the same row. NOT tenant-scoped — leases are a process-level primitive shared across
-- tenants (e.g. timer-leader, outbox-leader).
--
-- The index on expires_at supports the operator-surfacing "which leases are expiring soon"
-- query without scanning the whole table; primary key already covers the by-name lookup.

CREATE TABLE lease (
  name        VARCHAR(128) PRIMARY KEY,
  holder      VARCHAR(128) NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX lease_expires_at ON lease (expires_at);
