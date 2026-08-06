-- Coverage reconstruction-fragment table — PostgreSQL dialect of the ENGINE-OWNED coverage
-- schema (see V901 for the ownership + idempotence contract).
--
-- One row per injected coverage-segment completion: the completing segment's route + process, the
-- instance that completed it, and the correlation dimensions the union-find pass unions the
-- cascade on — per-hop businessKey, W3C traceId, instanceId. The correlation-aware
-- `coverage check` reconstructs a cross-process route from coverage's OWN records here, never
-- from audit — this store is deliberately decoupled from the audit stream.
--
-- Deployment-scoped like coverage_metric: the explicit `deployment_id` bind on every statement IS
-- the isolation (no RLS on a user-owned connection — see V901). business_key / trace_id are
-- portable TEXT (author-declared correlation value / traceparent — domain-neutral); instance_id
-- is TEXT (not necessarily a UUID) to avoid coupling to the engine's instance-id shape.
CREATE TABLE IF NOT EXISTS coverage_fragment (
  id              BIGSERIAL PRIMARY KEY,
  deployment_id   VARCHAR(64)  NOT NULL,
  route_urn       VARCHAR(512) NOT NULL,
  segment_process VARCHAR(256) NOT NULL,
  instance_id     VARCHAR(128) NOT NULL,
  business_key    TEXT,
  trace_id        TEXT,
  at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS coverage_fragment_route
  ON coverage_fragment (deployment_id, route_urn);
