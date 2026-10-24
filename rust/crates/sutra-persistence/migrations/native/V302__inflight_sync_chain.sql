-- Schema addendum, Rust engine only: the IN-FLIGHT marker of a synchronous-call chain.
--
-- A chain (an instance waiting on calls to `ack-mode: on-complete` outbound channels) lives in
-- memory until it parks, completes or fails, so a crash mid-chain persists none of it — except
-- what was written before it began: the delivery's inbox dedup rows (V301) and, on a capped
-- channel, its RUNNING admission row (V701). This row records that a chain is in flight and
-- which dedup keys it holds, so a recovery sweep can release them when the holder is gone: the
-- client's retry then runs again instead of being swallowed as a duplicate.
--
-- Lifecycle (owned by the dispatcher and the instance step commits):
--   chain begins            -> UPSERT (holder = the engine replica id, started_at = now)
--   any final step commits  -> DELETE in the same transaction (park, re-park, complete, failed,
--                              sync-path emissions); a chain that commits nothing DELETEs it
--                              directly
--   sweep                   -> a marker whose holder is a previous process on this host, or
--                              older than the configured bound: DELETE its dedup rows, its
--                              RUNNING admission row when the instance has no stored row, and
--                              the marker
--
-- `dedup_keys` is a JSON array of [channel, event_id] pairs.
-- Deployment scoping mirrors inbox_seen: an explicit deployment_id bind on every statement, with
-- PostgreSQL Row-Level Security below; the sweep is cross-deployment maintenance, like the
-- inbox prune, and runs under the same RLS-bypassing posture.

CREATE TABLE inflight_sync_chain (
  deployment_id  VARCHAR(64)  NOT NULL,
  instance_id    UUID         NOT NULL,
  dedup_keys     TEXT         NOT NULL DEFAULT '[]',
  holder         VARCHAR(128) NOT NULL,
  started_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, instance_id)
);

CREATE INDEX inflight_sync_chain_started_at ON inflight_sync_chain (started_at);

ALTER TABLE inflight_sync_chain ENABLE ROW LEVEL SECURITY;
CREATE POLICY inflight_sync_chain_deployment_iso ON inflight_sync_chain
  USING (deployment_id = current_setting('sutra.deployment_id', true));
