//! Deployment-archive store, SQL Server dialect (`deployment_archive`, V1001) — the
//! DB-backed deployment source (see the book's *Deploy, hot-deploy, and rollback* chapter).
//! NOT deployment-scoped: this is the registry itself (like `lease`, a process-level
//! primitive), so there is no `deployment_id` bind and no scoping — tenant isolation is a
//! deploy-API-layer concern.
//!
//! Same surface + semantics as the reference [`crate::stores::PgDeploymentArchiveStore`]:
//! `slot` is the stable archive key (a hot-deploy REPLACES a slot in place),
//! `deployment_id` is the content-hash id. Exactly one `active` row per slot — the
//! reference's partial unique index maps onto the V1001 filtered unique index here.
//! `upsert_active` demotes the prior active row to `draining` then `MERGE`s the new one (an
//! identical content-hash re-deploy re-activates the same PK row) with a bumped per-slot
//! revision, all inside one transaction — the `MERGE ... WITH (HOLDLOCK)` is the atomic
//! equivalent of the reference's conflict-target upsert.

use time::OffsetDateTime;

use crate::mssql::{req, to_db, MssqlPool, MssqlTx};
use crate::stores::{ActiveArchive, ArchiveStatus, ArchiveStatusRow, NewArchive, ServedArchiveRow};
use crate::{PersistenceError, Result};

/// SQL Server implementation of the deployment-archive store.
#[derive(Clone)]
pub struct MssqlDeploymentArchiveStore {
    pool: MssqlPool,
}

/// Next monotonic revision for a slot; `CAST(... AS BIGINT)` pins the aggregate to i64.
const SQL_NEXT_REVISION: &str =
    "SELECT CAST(COALESCE(MAX(revision), 0) + 1 AS BIGINT) AS next_rev \
     FROM deployment_archive WHERE slot = @P1";

const SQL_DEMOTE: &str =
    "UPDATE deployment_archive SET status = 'draining' WHERE slot = @P1 AND status = 'active'";

/// Keyed on the `deployment_id` PRIMARY KEY: an identical content-hash re-deploy (a
/// rollback) re-activates the existing row — flipped to `draining` by the demote above —
/// instead of inserting a duplicate. The MATCHED update mirrors the reference
/// `ON CONFLICT (deployment_id)` set (status/slot/revision/bytes/checksum/activated_at;
/// identity columns unchanged). MERGE statements must terminate with `;`.
const SQL_MERGE: &str = "MERGE deployment_archive WITH (HOLDLOCK) AS t \
     USING (SELECT @P1 AS deployment_id) AS s ON t.deployment_id = s.deployment_id \
     WHEN MATCHED THEN UPDATE SET \
       status = 'active', slot = @P2, revision = @P6, bytes = @P7, checksum = @P8, \
       activated_at = @P9 \
     WHEN NOT MATCHED THEN INSERT \
       (deployment_id, slot, tenant, module, version, status, revision, bytes, checksum, activated_at) \
       VALUES (@P1, @P2, @P3, @P4, @P5, 'active', @P6, @P7, @P8, @P9);";

const SQL_LIST_ACTIVE: &str =
    "SELECT deployment_id, slot, tenant, module, version, revision, bytes \
     FROM deployment_archive WHERE status = 'active' ORDER BY slot";

const SQL_LIST_STATUS: &str =
    "SELECT deployment_id, slot, tenant, module, version, status, revision \
     FROM deployment_archive ORDER BY slot, revision";

/// The SERVED set (active + the draining tail), newest revision first within a slot — the
/// reference's `list_active_and_draining` select, verbatim apart from the placeholder dialect.
const SQL_LIST_SERVED: &str =
    "SELECT deployment_id, slot, tenant, module, version, revision, bytes, status \
     FROM deployment_archive WHERE status IN ('active', 'draining') \
     ORDER BY slot, revision DESC";

const SQL_GET_BYTES: &str = "SELECT bytes FROM deployment_archive WHERE deployment_id = @P1";

const SQL_RETIRE: &str =
    "UPDATE deployment_archive SET status = 'draining' WHERE slot = @P1 AND status = 'active'";

/// Terminal flip of one drained archive (the quiescence sweep), guarded on `draining`.
const SQL_RETIRE_DEPLOYMENT: &str = "UPDATE deployment_archive SET status = 'retired' \
     WHERE deployment_id = @P1 AND status = 'draining'";

impl MssqlDeploymentArchiveStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }

    /// Store `new` as the ACTIVE archive for its slot, replacing in place: the prior active
    /// row for the slot is demoted to `draining` and the new row is inserted (or
    /// re-activated, if the same content-hash id is re-deployed) with a bumped per-slot
    /// `revision`, all in one transaction. Returns the new revision.
    pub async fn upsert_active(&self, new: &NewArchive) -> Result<i64> {
        let now = to_db(OffsetDateTime::now_utc());
        let mut tx = MssqlTx::begin(&self.pool).await?;

        let rev_row = tx
            .client()
            .query(SQL_NEXT_REVISION, &[&new.slot.as_str()])
            .await
            .map_err(PersistenceError::mssql("deployment_archive next-revision"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql(
                "deployment_archive next-revision row",
            ))?;
        // An aggregate with no GROUP BY always returns one row; default defensively to 1.
        let revision: i64 = rev_row
            .map(|r| req::<i64>(&r, "next_rev"))
            .transpose()?
            .unwrap_or(1);

        // Demote the prior active row (if any) so the one-active-per-slot index stays satisfied.
        tx.client()
            .execute(SQL_DEMOTE, &[&new.slot.as_str()])
            .await
            .map_err(PersistenceError::mssql("deployment_archive demote"))?;

        let bytes = new.bytes.as_slice();
        tx.client()
            .execute(
                SQL_MERGE,
                &[
                    &new.deployment_id.as_str(),
                    &new.slot.as_str(),
                    &new.tenant.as_str(),
                    &new.module.as_str(),
                    &new.version.as_str(),
                    &revision,
                    &bytes,
                    &new.checksum.as_str(),
                    &now,
                ],
            )
            .await
            .map_err(PersistenceError::mssql("deployment_archive merge"))?;

        tx.commit().await?;
        Ok(revision)
    }

    /// The full ACTIVE set with bytes — the engine's boot-load and re-hydration source.
    pub async fn list_active(&self) -> Result<Vec<ActiveArchive>> {
        let mut conn = self.pool.acquire().await?;
        let rows = conn
            .client()
            .query(SQL_LIST_ACTIVE, &[])
            .await
            .map_err(PersistenceError::mssql("deployment_archive list_active"))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql(
                "deployment_archive list_active rows",
            ))?;
        rows.iter().map(active_of).collect()
    }

    /// The SERVED set with bytes — ACTIVE plus the DRAINING tail, so instances pinned to a
    /// hot-deployed-away revision keep resuming (see the reference
    /// [`crate::stores::PgDeploymentArchiveStore::list_active_and_draining`]).
    pub async fn list_active_and_draining(&self) -> Result<Vec<ServedArchiveRow>> {
        let mut conn = self.pool.acquire().await?;
        let rows = conn
            .client()
            .query(SQL_LIST_SERVED, &[])
            .await
            .map_err(PersistenceError::mssql(
                "deployment_archive list_active_and_draining",
            ))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql(
                "deployment_archive list_active_and_draining rows",
            ))?;
        rows.iter().map(served_of).collect()
    }

    /// The status projection over ALL rows (no bytes) — the source the CRD mirror + the
    /// `/sutra/deployments` status endpoint read.
    pub async fn list_status(&self) -> Result<Vec<ArchiveStatusRow>> {
        let mut conn = self.pool.acquire().await?;
        let rows = conn
            .client()
            .query(SQL_LIST_STATUS, &[])
            .await
            .map_err(PersistenceError::mssql("deployment_archive list_status"))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql(
                "deployment_archive list_status rows",
            ))?;
        rows.iter().map(status_of).collect()
    }

    /// The sealed bytes of one archive by id (for re-verification / re-hydration).
    pub async fn get_bytes(&self, deployment_id: &str) -> Result<Option<Vec<u8>>> {
        let mut conn = self.pool.acquire().await?;
        let row = conn
            .client()
            .query(SQL_GET_BYTES, &[&deployment_id])
            .await
            .map_err(PersistenceError::mssql("deployment_archive get_bytes"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("deployment_archive get_bytes row"))?;
        match row {
            Some(r) => Ok(Some(req::<&[u8]>(&r, "bytes")?.to_vec())),
            None => Ok(None),
        }
    }

    /// Retire the active row for a slot (undeploy): active → draining. Returns `true` when a
    /// row was flipped, `false` when the slot had no active row.
    pub async fn retire_slot(&self, slot: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        let affected = conn
            .client()
            .execute(SQL_RETIRE, &[&slot])
            .await
            .map_err(PersistenceError::mssql("deployment_archive retire_slot"))?
            .total();
        Ok(affected >= 1)
    }

    /// Terminal flip of ONE drained archive: `draining` → `retired` (the retire-when-quiescent
    /// sweep's durable half). Idempotent — a retired id flips nothing and returns `false`.
    pub async fn retire_deployment(&self, deployment_id: &str) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        let affected = conn
            .client()
            .execute(SQL_RETIRE_DEPLOYMENT, &[&deployment_id])
            .await
            .map_err(PersistenceError::mssql(
                "deployment_archive retire_deployment",
            ))?
            .total();
        Ok(affected >= 1)
    }
}

fn served_of(row: &tiberius::Row) -> Result<ServedArchiveRow> {
    let status: &str = req(row, "status")?;
    Ok(ServedArchiveRow {
        archive: active_of(row)?,
        status: ArchiveStatus::parse(status)?,
    })
}

fn active_of(row: &tiberius::Row) -> Result<ActiveArchive> {
    Ok(ActiveArchive {
        deployment_id: req::<&str>(row, "deployment_id")?.to_owned(),
        slot: req::<&str>(row, "slot")?.to_owned(),
        tenant: req::<&str>(row, "tenant")?.to_owned(),
        module: req::<&str>(row, "module")?.to_owned(),
        version: req::<&str>(row, "version")?.to_owned(),
        revision: req(row, "revision")?,
        bytes: req::<&[u8]>(row, "bytes")?.to_vec(),
    })
}

fn status_of(row: &tiberius::Row) -> Result<ArchiveStatusRow> {
    let status: &str = req(row, "status")?;
    Ok(ArchiveStatusRow {
        deployment_id: req::<&str>(row, "deployment_id")?.to_owned(),
        slot: req::<&str>(row, "slot")?.to_owned(),
        tenant: req::<&str>(row, "tenant")?.to_owned(),
        module: req::<&str>(row, "module")?.to_owned(),
        version: req::<&str>(row, "version")?.to_owned(),
        status: ArchiveStatus::parse(status)?,
        revision: req(row, "revision")?,
    })
}
