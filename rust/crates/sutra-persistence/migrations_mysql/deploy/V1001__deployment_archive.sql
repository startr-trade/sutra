-- MySQL/MariaDB dialect of the deployment-archive table (see the book's "Deploy, hot-deploy,
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
-- Dialect notes (semantics normative, syntax not): BYTEA -> LONGBLOB, TIMESTAMPTZ ->
-- DATETIME(6) UTC, and the VARCHAR text columns compare byte-wise via utf8mb4_bin.
--
--   * one-ACTIVE-per-slot. PostgreSQL uses a partial unique index (WHERE status='active').
--     MySQL/MariaDB have no partial indexes, so the ruled functional workaround applies (see
--     V101__alias_index): a STORED generated column `active_slot` that carries the slot only
--     while the row is active and NULL otherwise, plus a unique index over it. NULLs never
--     collide in a MySQL unique index, so draining/retired revisions are exempt — exactly the
--     partial-index behaviour: a replace-in-place demotes the prior active row then inserts
--     the new one, and the index admits exactly one active row per slot throughout.

CREATE TABLE deployment_archive (
  deployment_id  VARCHAR(64)  NOT NULL,
  slot           VARCHAR(512) NOT NULL,
  tenant         VARCHAR(256) NOT NULL,
  module         VARCHAR(256) NOT NULL,
  version        VARCHAR(64)  NOT NULL,
  status         VARCHAR(32)  NOT NULL,            -- validated | active | draining | retired
  revision       BIGINT       NOT NULL,
  bytes          LONGBLOB     NOT NULL,
  checksum       VARCHAR(128) NOT NULL,
  created_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  activated_at   DATETIME(6)  NULL,
  active_slot    VARCHAR(512) GENERATED ALWAYS AS
                   (CASE WHEN status = 'active' THEN slot ELSE NULL END) STORED,

  PRIMARY KEY (deployment_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- Exactly one ACTIVE row per slot — the invariant a hot-deploy's replace-in-place maintains
-- (the generated-column unique index; see header).
CREATE UNIQUE INDEX deployment_archive_active_slot ON deployment_archive (active_slot);
-- Boot-load + the status projection scan the active set; per-slot history lookups key on slot.
CREATE INDEX deployment_archive_status ON deployment_archive (status);
CREATE INDEX deployment_archive_slot ON deployment_archive (slot);
