-- The DB-backed deployment source (see the book's "Deploy, hot-deploy, and rollback" chapter). This table
-- replaces the dir/ConfigMap deployment source: sealed .sutra archives live here as the durable,
-- cluster-shared source of truth. The engine boots its ACTIVE set from this table, and the sync
-- deploy API (POST /admin/deployments) validates -> stores active -> activates.
--
-- Identity: `slot` is the stable archive key (the ConfigMap-key / file-name analogue) — a hot-deploy
-- REPLACES a slot in place. `deployment_id` is the content-hash id (a new revision per new content),
-- so a slot accumulates one active row + prior draining/retired revisions. `revision` is monotonic
-- per slot (audit + the convergence stamp clients watch).
--
-- Tenant isolation is enforced at the deploy-API layer (the caller's identity is validated against
-- the archive's tenant) rather than by RLS here: the registry is engine infra and the boot-load
-- reads the whole active set across tenants. (Tenant-scoped RLS on this table is a tracked
-- follow-up.)
CREATE TABLE deployment_archive (
  deployment_id  VARCHAR(64)  NOT NULL,
  slot           VARCHAR(512) NOT NULL,
  tenant         VARCHAR(256) NOT NULL,
  module         VARCHAR(256) NOT NULL,
  version        VARCHAR(64)  NOT NULL,
  status         VARCHAR(32)  NOT NULL,            -- validated | active | draining | retired
  revision       BIGINT       NOT NULL,
  bytes          BYTEA        NOT NULL,
  checksum       VARCHAR(128) NOT NULL,
  created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  activated_at   TIMESTAMP WITH TIME ZONE,

  PRIMARY KEY (deployment_id)
);

-- Exactly one ACTIVE row per slot — the invariant a hot-deploy's replace-in-place maintains.
CREATE UNIQUE INDEX deployment_archive_active_slot ON deployment_archive (slot) WHERE status = 'active';
-- Boot-load + the status projection scan the active set; per-slot history lookups key on slot.
CREATE INDEX deployment_archive_status ON deployment_archive (status);
CREATE INDEX deployment_archive_slot ON deployment_archive (slot);
