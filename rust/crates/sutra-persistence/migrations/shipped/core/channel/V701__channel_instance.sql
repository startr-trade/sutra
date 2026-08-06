-- sutra per-channel concurrency substrate. One row per non-terminal instance, recording which
-- channel admitted it and whether it is in-flight (RUNNING) or parked at a wait node (WAITING).
-- This is the REPLICA-COHERENT source of truth for the per-channel concurrency cap
-- (max-concurrent-instances): every replica reads the same COUNT(*), so a channel at capacity on
-- one replica is at capacity on all of them. See the ChannelConcurrencyStore SPI.
--
-- Lifecycle (the channel-concurrency store, driven by the dispatcher + its concurrency tracker):
--   recordStarted   -> INSERT (status RUNNING)   [dispatcher, at instance start — it knows the channel]
--   recordSuspended -> UPDATE status = WAITING    [tracker, onInstanceSuspended]
--   recordResumed   -> UPDATE status = RUNNING     [tracker, onInstanceResumed]
--   recordTerminal  -> DELETE                       [tracker, onInstanceCompleted / onInstanceFailed]
--
-- (deployment_id, instance_id) is the primary key (an instance lives on exactly one channel) and the
-- UPSERT conflict target. The (deployment_id, channel, status) index is the access path for the
-- admission COUNT(*) — useOnlyInFlightForConcurrencyCap=true counts status='RUNNING' only; false
-- (VoIP, where a held call still holds its line) counts RUNNING + WAITING.
--
-- Deployment scoping mirrors instance_state: an explicit deployment_id bind on every statement, with
-- PostgreSQL Row-Level Security layered in V702 (absent here so this runs cleanly on H2).

CREATE TABLE channel_instance (
  deployment_id    VARCHAR(64) NOT NULL,
  instance_id  UUID NOT NULL,
  channel      VARCHAR(255) NOT NULL,
  status       VARCHAR(16) NOT NULL,
  updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (deployment_id, instance_id)
);

CREATE INDEX channel_instance_count ON channel_instance (deployment_id, channel, status);
