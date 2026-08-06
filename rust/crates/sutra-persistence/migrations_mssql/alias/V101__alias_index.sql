-- SQL Server dialect of the alias correlation index (PostgreSQL reference:
-- V101__alias_index.sql). Semantics are normative, syntax is not — this file must reproduce:
--
--   * the unique-LIVE alias guarantee: PostgreSQL's partial unique index maps 1:1 onto a
--     SQL Server FILTERED unique index (WHERE unique_alias = 1 AND live = 1) — first
--     inserter wins, retire (live = 0) frees the slot for a successor.
--   * byte-wise comparison semantics: the Latin1_General_100_BIN2 collation (the default
--     server collation is case-insensitive, which would merge 'ABC'/'abc' — a semantics
--     change the reference dialect does not have).
--
-- Column widths: alias_name/alias_value are VARCHAR (1 byte/char) so the composite keys
-- stay inside SQL Server's index byte limits (900 clustered / 1700 nonclustered — hence
-- PRIMARY KEY NONCLUSTERED, the same trade the per-dialect data_store scaffolding makes).
-- Consequence, flagged: non-ASCII alias values degrade on this dialect (code-page
-- conversion); ASCII values — the engine's correlation ids — are exact.
--
-- Deployment isolation on this dialect is single-layer: the explicit deployment_id bind on
-- every statement (the documented enforced-bind-only posture — no security policy is
-- shipped because none exists in the shipped migration trees for any non-PG dialect; the
-- V403/V602/V702/V802 policy scripts are intentionally absent from this tree).

CREATE TABLE alias_index (
  deployment_id NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
  instance_id   UNIQUEIDENTIFIER NOT NULL,
  alias_name    VARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
  alias_value   VARCHAR(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,
  unique_alias  BIT NOT NULL DEFAULT 0,
  live          BIT NOT NULL DEFAULT 1,
  created_at    DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_alias_index PRIMARY KEY NONCLUSTERED
    (deployment_id, instance_id, alias_name, alias_value)
);

-- Lookup path for follow-up signal correlation (filtered — live rows only).
CREATE INDEX alias_index_live_lookup
  ON alias_index (deployment_id, alias_name, alias_value)
  WHERE live = 1;

-- Atomic unique-alias enforcement: the filtered unique index (see header).
CREATE UNIQUE INDEX alias_index_unique_live
  ON alias_index (deployment_id, alias_name, alias_value)
  WHERE unique_alias = 1 AND live = 1;
