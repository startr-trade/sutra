-- SQL Server dialect of the deployment-archive table (see the book's "Deploy, hot-deploy,
-- and rollback" chapter; PostgreSQL reference: V1001__deployment_archive.sql).
-- The DB-backed deployment source: sealed .sutra archives live here as the durable,
-- cluster-shared source of truth. The engine boots its ACTIVE set from this table, and the
-- sync deploy API (POST /admin/deployments) validates -> stores active -> activates.
--
-- Identity: `slot` is the stable archive key (the ConfigMap-key / file-name analogue) — a
-- hot-deploy REPLACES a slot in place. `deployment_id` is the content-hash id (a new revision
-- per new content), so a slot accumulates one active row + prior draining/retired revisions.
-- `revision` is monotonic per slot (audit + the convergence stamp clients watch). NOT
-- deployment-scoped: this is the registry itself (like `lease`, a process-level primitive),
-- so the single deployment_id isolation column the other tables carry is absent by design;
-- tenant isolation is enforced at the deploy-API layer, not by this table.
--
-- Dialect notes (semantics normative, syntax not): BYTEA -> VARBINARY(MAX), TIMESTAMPTZ ->
-- DATETIME2(6) UTC, and the text columns compare byte-wise via Latin1_General_100_BIN2 (the
-- default server collation is case-insensitive, which would merge status/slot casings).
--
--   * one-ACTIVE-per-slot: PostgreSQL's partial unique index maps 1:1 onto a SQL Server
--     FILTERED unique index (WHERE status = 'active') — a replace-in-place demotes the prior
--     active row then inserts the new one, and the index admits exactly one active row per
--     slot throughout; draining/retired revisions are exempt.

CREATE TABLE deployment_archive (
  deployment_id  NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL
                   CONSTRAINT pk_deployment_archive PRIMARY KEY,
  slot           NVARCHAR(512) COLLATE Latin1_General_100_BIN2 NOT NULL,
  tenant         NVARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
  module         NVARCHAR(256) COLLATE Latin1_General_100_BIN2 NOT NULL,
  version        NVARCHAR(64)  COLLATE Latin1_General_100_BIN2 NOT NULL,
  status         NVARCHAR(32)  COLLATE Latin1_General_100_BIN2 NOT NULL,  -- validated|active|draining|retired
  revision       BIGINT        NOT NULL,
  bytes          VARBINARY(MAX) NOT NULL,
  checksum       NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  created_at     DATETIME2(6)  NOT NULL DEFAULT SYSUTCDATETIME(),
  activated_at   DATETIME2(6)  NULL
);

-- Exactly one ACTIVE row per slot — the invariant a hot-deploy's replace-in-place maintains
-- (the filtered unique index; see header).
CREATE UNIQUE INDEX deployment_archive_active_slot
  ON deployment_archive (slot) WHERE status = 'active';
-- Boot-load + the status projection scan the active set; per-slot history lookups key on slot.
CREATE INDEX deployment_archive_status ON deployment_archive (status);
CREATE INDEX deployment_archive_slot ON deployment_archive (slot);
