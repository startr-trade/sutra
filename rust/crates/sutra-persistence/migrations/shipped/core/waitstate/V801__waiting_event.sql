-- Durable registry of wait points. One row per token parked at a wait node (an intermediate
-- message catch event or a userTask), so the admin API can LIST what is awaiting a relay decision
-- without deserializing instance snapshots, and a relay can be reconciled against a known wait point.
--
-- This is a queryable projection BESIDE instance_state: instance_state holds the resume seed (the full
-- token snapshot), while waiting_event is the index of WHAT is waiting WHERE. See the WaitStateStore SPI
-- and the book's "Wait states and human tasks" chapter.
--
-- Lifecycle (the wait-state store, driven by the engine where the wait frontier is known):
--   recordWaiting -> UPSERT (status WAITING)   [dispatcher on start->suspend; resume core on re-suspend]
--   resolve       -> UPDATE status=RESOLVED     [resume core, when the relay satisfies the node]
--   resolveAll    -> UPDATE status=RESOLVED      [on terminal — a completed/failed instance leaves no live wait]
--
-- (deployment_id, instance_id, node_id) is the primary key: a token parks at a given node at most once per
-- slice-1 process (an IMCE reachable more than once — standard-loop / MI — is out of slice 1). RESOLVED
-- rows are retained with a resolved_at stamp for audit; the default listing filters to WAITING.
--
-- Deployment scoping mirrors instance_state: an explicit deployment_id bind on every statement, with PostgreSQL
-- Row-Level Security layered in V802 (absent here so this DDL applies before the policy attaches).

CREATE TABLE waiting_event (
  deployment_id       VARCHAR(64) NOT NULL,
  instance_id     UUID NOT NULL,
  node_id         VARCHAR(255) NOT NULL,
  process_id      VARCHAR(255) NOT NULL,
  correlation_key VARCHAR(512),
  status          VARCHAR(16) NOT NULL DEFAULT 'WAITING',
  created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  resolved_at     TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (deployment_id, instance_id, node_id)
);

-- Access path for the admin listWaiting query: waits for a tenant, optionally narrowed by process, by status.
CREATE INDEX waiting_event_list ON waiting_event (deployment_id, process_id, status, created_at);

-- Correlation-key lookups (display/filtering) — partial, since the key is nullable in slice 1.
CREATE INDEX waiting_event_correlation ON waiting_event (deployment_id, correlation_key)
  WHERE correlation_key IS NOT NULL;
