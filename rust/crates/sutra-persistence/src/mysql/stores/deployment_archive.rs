//! Deployment-archive store, MySQL/MariaDB dialect (`deployment_archive`, V1001) — the
//! DB-backed deployment source (see the book's *Deploy, hot-deploy, and rollback* chapter).
//! NOT deployment-scoped: this is the registry itself (like `lease`, a process-level
//! primitive), so there is no `deployment_id` bind and no scoping — tenant isolation is a
//! deploy-API-layer concern.
//!
//! Same surface + semantics as the reference [`crate::stores::PgDeploymentArchiveStore`]:
//! `slot` is the stable archive key (a hot-deploy REPLACES a slot in place),
//! `deployment_id` is the content-hash id (a new revision per new content). Exactly one
//! `active` row per slot — the reference's partial unique index becomes the generated-column
//! unique index of the V1001 dialect DDL here. `upsert_active` demotes the prior active row
//! to `draining` then inserts (or re-activates an identical content-hash re-deploy) the new
//! one with a bumped per-slot revision, in one row-serialised transaction.

use sqlx::{MySqlPool, Row};
use time::OffsetDateTime;

use crate::mysql::scope::begin_tx;
use crate::mysql::{str_col, to_db};
use crate::stores::{ActiveArchive, ArchiveStatus, ArchiveStatusRow, NewArchive, ServedArchiveRow};
use crate::{PersistenceError, Result};

/// MySQL/MariaDB implementation of the deployment-archive store.
#[derive(Debug, Clone)]
pub struct MySqlDeploymentArchiveStore {
    pool: MySqlPool,
}

/// Next monotonic revision for a slot. `CAST(... AS SIGNED)` pins the aggregate to `BIGINT`
/// (i64) rather than the widened decimal MySQL can return for `MAX(...) + 1`.
const SQL_NEXT_REVISION: &str =
    "SELECT CAST(COALESCE(MAX(revision), 0) + 1 AS SIGNED) FROM deployment_archive WHERE slot = ?";

const SQL_DEMOTE: &str =
    "UPDATE deployment_archive SET status = 'draining' WHERE slot = ? AND status = 'active'";

/// Keyed on the `deployment_id` PRIMARY KEY: an identical content-hash re-deploy (a
/// rollback) re-activates the existing row — flipped to `draining` by the demote above —
/// instead of inserting a duplicate. Mirrors the reference `ON CONFLICT (deployment_id)`
/// update set (status/slot/revision/bytes/checksum/activated_at; identity columns unchanged).
const SQL_INSERT: &str = "INSERT INTO deployment_archive \
       (deployment_id, slot, tenant, module, version, status, revision, bytes, checksum, activated_at) \
     VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?) \
     ON DUPLICATE KEY UPDATE \
       status = 'active', slot = VALUES(slot), revision = VALUES(revision), \
       bytes = VALUES(bytes), checksum = VALUES(checksum), activated_at = VALUES(activated_at)";

const SQL_LIST_ACTIVE: &str =
    "SELECT deployment_id, slot, tenant, module, version, revision, bytes \
     FROM deployment_archive WHERE status = 'active' ORDER BY slot";

/// The SERVED set (active + the draining tail), newest revision first within a slot — the
/// reference's `list_active_and_draining` select, verbatim apart from the placeholder dialect.
const SQL_LIST_SERVED: &str =
    "SELECT deployment_id, slot, tenant, module, version, revision, bytes, status \
     FROM deployment_archive WHERE status IN ('active', 'draining') \
     ORDER BY slot, revision DESC";

const SQL_LIST_STATUS: &str =
    "SELECT deployment_id, slot, tenant, module, version, status, revision \
     FROM deployment_archive ORDER BY slot, revision";

/// Terminal flip of one drained archive (the quiescence sweep), guarded on `draining`.
const SQL_RETIRE_DEPLOYMENT: &str = "UPDATE deployment_archive SET status = 'retired' \
     WHERE deployment_id = ? AND status = 'draining'";

const SQL_GET_BYTES: &str = "SELECT bytes FROM deployment_archive WHERE deployment_id = ?";

const SQL_RETIRE: &str =
    "UPDATE deployment_archive SET status = 'draining' WHERE slot = ? AND status = 'active'";

impl MySqlDeploymentArchiveStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Store `new` as the ACTIVE archive for its slot, replacing in place: the prior active
    /// row for the slot is demoted to `draining` and the new row is inserted (or
    /// re-activated, if the same content-hash id is re-deployed) with a bumped per-slot
    /// `revision`, all in one transaction. Returns the new revision.
    pub async fn upsert_active(&self, new: &NewArchive) -> Result<i64> {
        let now = OffsetDateTime::now_utc();
        let mut tx = begin_tx(&self.pool).await?;

        let revision: i64 = sqlx::query_scalar(SQL_NEXT_REVISION)
            .bind(&new.slot)
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("deployment_archive next-revision"))?;

        // Demote the prior active row (if any) so the one-active-per-slot index stays satisfied.
        sqlx::query(SQL_DEMOTE)
            .bind(&new.slot)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("deployment_archive demote"))?;

        sqlx::query(SQL_INSERT)
            .bind(&new.deployment_id)
            .bind(&new.slot)
            .bind(&new.tenant)
            .bind(&new.module)
            .bind(&new.version)
            .bind(revision)
            .bind(&new.bytes)
            .bind(&new.checksum)
            .bind(to_db(now))
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("deployment_archive insert"))?;

        tx.commit()
            .await
            .map_err(PersistenceError::db("deployment_archive upsert commit"))?;
        Ok(revision)
    }

    /// The full ACTIVE set with bytes — the engine's boot-load and re-hydration source.
    pub async fn list_active(&self) -> Result<Vec<ActiveArchive>> {
        let rows = sqlx::query(SQL_LIST_ACTIVE)
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::db("deployment_archive list_active"))?;
        rows.iter()
            .map(|r| {
                Ok(ActiveArchive {
                    deployment_id: str_col(r, "deployment_id")?,
                    slot: str_col(r, "slot")?,
                    tenant: str_col(r, "tenant")?,
                    module: str_col(r, "module")?,
                    version: str_col(r, "version")?,
                    revision: r.try_get("revision").map_err(map_row)?,
                    bytes: r.try_get("bytes").map_err(map_row)?,
                })
            })
            .collect()
    }

    /// The SERVED set with bytes — ACTIVE plus the DRAINING tail, so instances pinned to a
    /// hot-deployed-away revision keep resuming (see the reference
    /// [`crate::stores::PgDeploymentArchiveStore::list_active_and_draining`]).
    pub async fn list_active_and_draining(&self) -> Result<Vec<ServedArchiveRow>> {
        let rows = sqlx::query(SQL_LIST_SERVED)
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::db(
                "deployment_archive list_active_and_draining",
            ))?;
        rows.iter()
            .map(|r| {
                Ok(ServedArchiveRow {
                    archive: ActiveArchive {
                        deployment_id: str_col(r, "deployment_id")?,
                        slot: str_col(r, "slot")?,
                        tenant: str_col(r, "tenant")?,
                        module: str_col(r, "module")?,
                        version: str_col(r, "version")?,
                        revision: r.try_get("revision").map_err(map_row)?,
                        bytes: r.try_get("bytes").map_err(map_row)?,
                    },
                    status: ArchiveStatus::parse(&str_col(r, "status")?)?,
                })
            })
            .collect()
    }

    /// The status projection over ALL rows (no bytes) — the source the CRD mirror + the
    /// `/sutra/deployments` status endpoint read.
    pub async fn list_status(&self) -> Result<Vec<ArchiveStatusRow>> {
        let rows = sqlx::query(SQL_LIST_STATUS)
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::db("deployment_archive list_status"))?;
        rows.iter()
            .map(|r| {
                Ok(ArchiveStatusRow {
                    deployment_id: str_col(r, "deployment_id")?,
                    slot: str_col(r, "slot")?,
                    tenant: str_col(r, "tenant")?,
                    module: str_col(r, "module")?,
                    version: str_col(r, "version")?,
                    status: ArchiveStatus::parse(&str_col(r, "status")?)?,
                    revision: r.try_get("revision").map_err(map_row)?,
                })
            })
            .collect()
    }

    /// The sealed bytes of one archive by id (for re-verification / re-hydration).
    pub async fn get_bytes(&self, deployment_id: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query(SQL_GET_BYTES)
            .bind(deployment_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::db("deployment_archive get_bytes"))?;
        match row {
            Some(r) => Ok(Some(r.try_get("bytes").map_err(map_row)?)),
            None => Ok(None),
        }
    }

    /// Retire the active row for a slot (undeploy): active → draining. Returns `true` when a
    /// row was flipped, `false` when the slot had no active row.
    pub async fn retire_slot(&self, slot: &str) -> Result<bool> {
        let affected = sqlx::query(SQL_RETIRE)
            .bind(slot)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("deployment_archive retire_slot"))?
            .rows_affected();
        Ok(affected >= 1)
    }

    /// Terminal flip of ONE drained archive: `draining` → `retired` (the retire-when-quiescent
    /// sweep's durable half). Idempotent — a retired id flips nothing and returns `false`.
    pub async fn retire_deployment(&self, deployment_id: &str) -> Result<bool> {
        let affected = sqlx::query(SQL_RETIRE_DEPLOYMENT)
            .bind(deployment_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("deployment_archive retire_deployment"))?
            .rows_affected();
        Ok(affected >= 1)
    }
}

/// Row-decode error → persistence error.
fn map_row(e: sqlx::Error) -> PersistenceError {
    PersistenceError::db("deployment_archive row decode")(e)
}
