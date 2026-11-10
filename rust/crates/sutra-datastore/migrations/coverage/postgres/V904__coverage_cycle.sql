-- Compliance cycles, PostgreSQL dialect of the ENGINE-OWNED coverage schema.
--
-- A cycle is an operator-declared free-form label under which coverage is recorded separately, so
-- one release of a system under test can be measured without an out-of-band reset. These tables
-- sit BESIDE coverage_metric and coverage_fragment rather than re-keying them: that keeps every
-- existing row readable, unchanged, as "no cycle", it keeps the statements a deployment with no
-- active cycle runs exactly as they were, and an engine rolled back to a build that predates
-- cycles still finds the table shape it expects.
--
-- Applied on first use on every boot, like the rest of this schema, so every statement must stay
-- re-runnable. That is also the whole upgrade path: a database that predates cycles gains these
-- tables the first time the store is used, and nothing else in it is touched.
CREATE TABLE IF NOT EXISTS coverage_cycle (
  deployment_id VARCHAR(64)  NOT NULL,
  cycle_label   VARCHAR(128) NOT NULL,
  declared_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  CONSTRAINT coverage_cycle_pk PRIMARY KEY (deployment_id, cycle_label)
);

-- At most one active cycle per deployment: the primary key IS the single-active rule, enforced by
-- the database on all three dialects, so two concurrent activations cannot both win.
CREATE TABLE IF NOT EXISTS coverage_active_cycle (
  deployment_id VARCHAR(64)  NOT NULL,
  cycle_label   VARCHAR(128) NOT NULL,
  CONSTRAINT coverage_active_cycle_pk PRIMARY KEY (deployment_id)
);

CREATE TABLE IF NOT EXISTS coverage_cycle_metric (
  deployment_id VARCHAR(64)  NOT NULL,
  cycle_label   VARCHAR(128) NOT NULL,
  path_urn      VARCHAR(512) NOT NULL,
  covered       BOOLEAN      NOT NULL DEFAULT false,
  CONSTRAINT coverage_cycle_metric_pk PRIMARY KEY (deployment_id, cycle_label, path_urn)
);

CREATE TABLE IF NOT EXISTS coverage_cycle_fragment (
  id              BIGSERIAL    PRIMARY KEY,
  deployment_id   VARCHAR(64)  NOT NULL,
  cycle_label     VARCHAR(128) NOT NULL,
  route_urn       VARCHAR(512) NOT NULL,
  segment_process VARCHAR(256) NOT NULL,
  instance_id     VARCHAR(128) NOT NULL,
  business_key    TEXT,
  trace_id        TEXT,
  at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS coverage_cycle_fragment_route
  ON coverage_cycle_fragment (deployment_id, cycle_label, route_urn);
