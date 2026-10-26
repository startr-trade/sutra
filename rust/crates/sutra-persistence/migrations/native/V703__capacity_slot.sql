-- Schema addendum, Rust engine only: capacity pools — a named, cluster-wide count of slots that
-- channels of any deployment draw from (`pool: <name>` in channels.yaml; the pools themselves are
-- declared in the engine configuration, `sutra.capacity-pools.<name>.*`).
--
-- Two tables, because a pool spans deployments while everything else here is deployment-scoped:
--
--   capacity_pool  ONE row per pool, holding the live per-direction counts. It is the pool's lock:
--                  every acquisition and release takes this row FOR UPDATE, so two replicas can
--                  never both take the last slot. It carries no tenant data (a pool name and two
--                  counters), so it has no row-level security — a deployment-scoped policy would
--                  hide the other deployments' share of the count and let them oversubscribe.
--   capacity_slot  ONE row per held slot — one per active call: (instance, call_key, pool) —
--                  deployment-scoped like every other instance table: an explicit deployment_id
--                  bind on every statement, with PostgreSQL Row-Level Security below. A deployment sees only its
--                  own holders; the shared count lives on the pool row.
--
-- The counters and the slot rows move together, in one transaction, always through the store:
--   acquire  -> lock pool row; check slots and the other direction's reserve; INSERT slot;
--               increment the direction's counter
--   rebind   -> re-key a slot from an admission key to the instance id (counts unchanged)
--   release  -> DELETE the instance's slots — all of them, or one call's, or one direction's;
--               decrement each pool's counter (pool rows locked in name order, so concurrent
--               releases cannot deadlock)
--   sweep    -> release a slot whose instance has finished (terminal_at stamped), or whose
--               instance was never stored and has no in-flight chain marker once its holder is a
--               previous process on this host or the slot is older than the sweep's bound
--
-- `holder` is the engine replica id (`<host>-<pid>-<rand>`) that took the slot; with acquired_at
-- it is the lease the sweep judges an unstored holder by.

CREATE TABLE capacity_pool (
  pool           VARCHAR(255) PRIMARY KEY,
  inbound_used   INTEGER      NOT NULL DEFAULT 0,
  outbound_used  INTEGER      NOT NULL DEFAULT 0,
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE capacity_slot (
  deployment_id  VARCHAR(64)  NOT NULL,
  instance_id    UUID         NOT NULL,
  pool           VARCHAR(255) NOT NULL,
  -- The call the slot is held for: an inbound delivery's identity or an outbound entry's id,
  -- or the value of the channel's declared `pool-call-key` header.
  call_key       VARCHAR(512) NOT NULL,
  tenant         VARCHAR(255) NOT NULL,
  direction      VARCHAR(16)  NOT NULL,
  holder         VARCHAR(128) NOT NULL,
  acquired_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, instance_id, pool, call_key)
);

CREATE INDEX capacity_slot_acquired_at ON capacity_slot (deployment_id, acquired_at);

ALTER TABLE capacity_slot ENABLE ROW LEVEL SECURITY;
CREATE POLICY capacity_slot_deployment_iso ON capacity_slot
  USING (deployment_id = current_setting('sutra.deployment_id', true));
