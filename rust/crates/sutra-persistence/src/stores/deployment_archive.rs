//! Deployment-archive store (`deployment_archive`, V1001) — the DB-backed deployment source
//! (see the book's *Deploy, hot-deploy, and rollback* chapter). Replaces the dir/ConfigMap source:
//! sealed `.sutra` archives live here as the durable, cluster-shared source of truth. The engine
//! boots its ACTIVE set from this table; the sync deploy API validates → stores active → activates.
//!
//! Semantics: `slot` is the stable archive key (a hot-deploy REPLACES a slot in place);
//! `deployment_id` is the content-hash id (a new revision per new content). Exactly one `active`
//! row per slot (a partial unique index enforces it); a replace demotes the prior active row to
//! `draining` and inserts/activates the new one in one transaction, bumping the per-slot `revision`.
//! NOT deployment-scoped — this is the registry itself (like `lease`, a process-level primitive):
//! no GUC, no RLS; tenant isolation is a deploy-API-layer concern.

use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::{PersistenceError, Result};

/// Lifecycle status of a stored archive row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveStatus {
    /// Verified + stored, not yet activated.
    Validated,
    /// The live row for its slot (exactly one per slot).
    Active,
    /// Flipped away by a replace; finishing in-flight work.
    Draining,
    /// Terminal — no longer served.
    Retired,
}

impl ArchiveStatus {
    /// Column string form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Validated => "validated",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Retired => "retired",
        }
    }

    /// Parses the column string form.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "validated" => Ok(Self::Validated),
            "active" => Ok(Self::Active),
            "draining" => Ok(Self::Draining),
            "retired" => Ok(Self::Retired),
            other => Err(PersistenceError::InvalidArgument(format!(
                "unknown deployment-archive status '{other}'"
            ))),
        }
    }
}

/// The identity + bytes of an archive to store (the deploy API's validated input).
#[derive(Debug, Clone)]
pub struct NewArchive {
    /// Content-hash deployment id (`dep-<hex>`).
    pub deployment_id: String,
    /// Stable archive key (the replace target).
    pub slot: String,
    /// Path-derived identity.
    pub tenant: String,
    pub module: String,
    pub version: String,
    /// The sealed `.sutra` bytes.
    pub bytes: Vec<u8>,
    /// Integrity checksum re-verified on load.
    pub checksum: String,
}

/// One active archive row (boot-load projection — carries the bytes).
#[derive(Debug, Clone)]
pub struct ActiveArchive {
    pub deployment_id: String,
    pub slot: String,
    pub tenant: String,
    pub module: String,
    pub version: String,
    pub revision: i64,
    pub bytes: Vec<u8>,
}

/// One row of the SERVED set — an archive the engine must keep registered — carrying the
/// bytes plus the lifecycle status that says HOW it is served.
///
/// `Active` rows serve intake (the live set). `Draining` rows serve NOTHING new: they stay
/// registered under their own deployment ids purely so instances PINNED to them keep resuming
/// via relay and timer until the quiescence gate retires them. Because the bytes travel with
/// the row, a replica that never saw the hot-deploy — a fresh pod after a restart, or a peer
/// converging on `LISTEN/NOTIFY` — can re-plan the drained definition from durable truth
/// instead of depending on a plan that only ever existed in one process's memory. `Validated`
/// and `Retired` rows are deliberately absent: nothing is pinned to a row that never activated,
/// and retirement is the terminal state of the zero-instances/zero-outbox gate.
#[derive(Debug, Clone)]
pub struct ServedArchiveRow {
    /// Identity, revision, and the sealed `.sutra` bytes — the same projection
    /// [`PgDeploymentArchiveStore::list_active`] returns.
    pub archive: ActiveArchive,
    /// `Active` (serving intake) or `Draining` (registered for pinned resume only).
    pub status: ArchiveStatus,
}

/// One status row (the CRD-mirror / status-endpoint projection — no bytes).
#[derive(Debug, Clone)]
pub struct ArchiveStatusRow {
    pub deployment_id: String,
    pub slot: String,
    pub tenant: String,
    pub module: String,
    pub version: String,
    pub status: ArchiveStatus,
    pub revision: i64,
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgDeploymentArchiveStore {
    pool: PgPool,
}

impl PgDeploymentArchiveStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Store `new` as the ACTIVE archive for its slot, replacing in place: the prior active row for
    /// the slot is demoted to `draining` and the new row is inserted (or re-activated, if the same
    /// content-hash id is re-deployed) with a bumped per-slot `revision`, all in one transaction.
    /// Returns the new revision. This is the durable half of both a first deploy and a hot-deploy.
    pub async fn upsert_active(&self, new: &NewArchive) -> Result<i64> {
        let now = OffsetDateTime::now_utc();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PersistenceError::db("deployment_archive upsert begin"))?;

        // Next monotonic revision for this slot.
        let revision: i64 = sqlx::query(
            "SELECT COALESCE(MAX(revision), 0) + 1 FROM deployment_archive WHERE slot = $1",
        )
        .bind(&new.slot)
        .fetch_one(&mut *tx)
        .await
        .map_err(PersistenceError::db("deployment_archive next-revision"))?
        .try_get::<i64, _>(0)
        .map_err(PersistenceError::db(
            "deployment_archive next-revision decode",
        ))?;

        // Demote the prior active row (if any) so the one-active-per-slot index stays satisfied.
        sqlx::query(
            "UPDATE deployment_archive SET status = 'draining' WHERE slot = $1 AND status = 'active'",
        )
        .bind(&new.slot)
        .execute(&mut *tx)
        .await
        .map_err(PersistenceError::db("deployment_archive demote"))?;

        // Insert the new active row — or re-activate an identical content-hash re-deploy.
        sqlx::query(
            "INSERT INTO deployment_archive \
               (deployment_id, slot, tenant, module, version, status, revision, bytes, checksum, activated_at) \
             VALUES ($1, $2, $3, $4, $5, 'active', $6, $7, $8, $9) \
             ON CONFLICT (deployment_id) DO UPDATE SET \
               status = 'active', slot = EXCLUDED.slot, revision = EXCLUDED.revision, \
               bytes = EXCLUDED.bytes, checksum = EXCLUDED.checksum, activated_at = EXCLUDED.activated_at",
        )
        .bind(&new.deployment_id)
        .bind(&new.slot)
        .bind(&new.tenant)
        .bind(&new.module)
        .bind(&new.version)
        .bind(revision)
        .bind(&new.bytes)
        .bind(&new.checksum)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(PersistenceError::db("deployment_archive insert"))?;

        // Multi-replica convergence (pg): notify other replicas' listeners to reload this slot on
        // commit. Best-effort — a non-listening single-replica engine simply ignores it.
        sqlx::query("SELECT pg_notify('sutra_deployments', $1)")
            .bind(&new.slot)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("deployment_archive notify"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("deployment_archive upsert commit"))?;
        Ok(revision)
    }

    /// The full ACTIVE set with bytes — the engine's boot-load and re-hydration source.
    pub async fn list_active(&self) -> Result<Vec<ActiveArchive>> {
        let rows = sqlx::query(
            "SELECT deployment_id, slot, tenant, module, version, revision, bytes \
             FROM deployment_archive WHERE status = 'active' ORDER BY slot",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::db("deployment_archive list_active"))?;

        rows.into_iter()
            .map(|r| {
                Ok(ActiveArchive {
                    deployment_id: r.try_get("deployment_id").map_err(map_row)?,
                    slot: r.try_get("slot").map_err(map_row)?,
                    tenant: r.try_get("tenant").map_err(map_row)?,
                    module: r.try_get("module").map_err(map_row)?,
                    version: r.try_get("version").map_err(map_row)?,
                    revision: r.try_get("revision").map_err(map_row)?,
                    bytes: r.try_get("bytes").map_err(map_row)?,
                })
            })
            .collect()
    }

    /// The SERVED set with bytes: the ACTIVE rows **plus the DRAINING tail** — what a replica
    /// must register to keep both new intake and PINNED resumes working.
    ///
    /// [`Self::list_active`] keeps its narrower contract (the live intake set, unchanged for its
    /// callers); this is the widened listing the deployment source reads, because an in-flight
    /// instance pinned to a hot-deployed-away revision can only resume while that revision's
    /// graph is still registered. Ordered `slot, revision DESC` so each slot's newest revision
    /// comes first — the order the relay's DRAINING scope walk (most-recently-drained first)
    /// wants, with no re-sorting in the engine.
    pub async fn list_active_and_draining(&self) -> Result<Vec<ServedArchiveRow>> {
        let rows = sqlx::query(
            "SELECT deployment_id, slot, tenant, module, version, revision, bytes, status \
             FROM deployment_archive WHERE status IN ('active', 'draining') \
             ORDER BY slot, revision DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::db(
            "deployment_archive list_active_and_draining",
        ))?;

        rows.into_iter()
            .map(|r| {
                let status: String = r.try_get("status").map_err(map_row)?;
                Ok(ServedArchiveRow {
                    archive: ActiveArchive {
                        deployment_id: r.try_get("deployment_id").map_err(map_row)?,
                        slot: r.try_get("slot").map_err(map_row)?,
                        tenant: r.try_get("tenant").map_err(map_row)?,
                        module: r.try_get("module").map_err(map_row)?,
                        version: r.try_get("version").map_err(map_row)?,
                        revision: r.try_get("revision").map_err(map_row)?,
                        bytes: r.try_get("bytes").map_err(map_row)?,
                    },
                    status: ArchiveStatus::parse(&status)?,
                })
            })
            .collect()
    }

    /// The status projection over ALL rows (no bytes) — the source the CRD mirror + the
    /// `/sutra/deployments` status endpoint read.
    pub async fn list_status(&self) -> Result<Vec<ArchiveStatusRow>> {
        let rows = sqlx::query(
            "SELECT deployment_id, slot, tenant, module, version, status, revision \
             FROM deployment_archive ORDER BY slot, revision",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::db("deployment_archive list_status"))?;

        rows.into_iter()
            .map(|r| {
                let status: String = r.try_get("status").map_err(map_row)?;
                Ok(ArchiveStatusRow {
                    deployment_id: r.try_get("deployment_id").map_err(map_row)?,
                    slot: r.try_get("slot").map_err(map_row)?,
                    tenant: r.try_get("tenant").map_err(map_row)?,
                    module: r.try_get("module").map_err(map_row)?,
                    version: r.try_get("version").map_err(map_row)?,
                    status: ArchiveStatus::parse(&status)?,
                    revision: r.try_get("revision").map_err(map_row)?,
                })
            })
            .collect()
    }

    /// The sealed bytes of one archive by id (for re-verification / re-hydration).
    pub async fn get_bytes(&self, deployment_id: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query("SELECT bytes FROM deployment_archive WHERE deployment_id = $1")
            .bind(deployment_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::db("deployment_archive get_bytes"))?;
        match row {
            Some(r) => Ok(Some(r.try_get("bytes").map_err(map_row)?)),
            None => Ok(None),
        }
    }

    /// Retire the active row for a slot (undeploy): active → draining. Returns `true` when a row
    /// was flipped, `false` when the slot had no active row.
    pub async fn retire_slot(&self, slot: &str) -> Result<bool> {
        let affected =
            sqlx::query("UPDATE deployment_archive SET status = 'draining' WHERE slot = $1 AND status = 'active'")
                .bind(slot)
                .execute(&self.pool)
                .await
                .map_err(PersistenceError::db("deployment_archive retire_slot"))?
                .rows_affected();
        if affected >= 1 {
            // Convergence notify (pg) — best-effort.
            let _ = sqlx::query("SELECT pg_notify('sutra_deployments', $1)")
                .bind(slot)
                .execute(&self.pool)
                .await;
        }
        Ok(affected >= 1)
    }

    /// Terminal flip of ONE drained archive: `draining` → `retired`, the durable half of the
    /// retire-when-quiescent sweep (zero active instances AND zero pending outbox rows). Retired
    /// rows drop out of [`Self::list_active_and_draining`], so the next activation deregisters
    /// the definition fleet-wide. Returns `true` when a row flipped.
    ///
    /// Guarded on `status = 'draining'`: an ACTIVE row is never retired out from under intake
    /// (undeploy goes through [`Self::retire_slot`] first), and a re-run of the sweep on an
    /// already-retired id is a no-op — the sweep is idempotent.
    pub async fn retire_deployment(&self, deployment_id: &str) -> Result<bool> {
        let row = sqlx::query(
            "UPDATE deployment_archive SET status = 'retired' \
             WHERE deployment_id = $1 AND status = 'draining' RETURNING slot",
        )
        .bind(deployment_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(PersistenceError::db("deployment_archive retire_deployment"))?;
        let Some(row) = row else {
            return Ok(false);
        };
        // Convergence notify (pg) — best-effort, so peers deregister the retired definition too.
        let slot: String = row.try_get("slot").map_err(map_row)?;
        let _ = sqlx::query("SELECT pg_notify('sutra_deployments', $1)")
            .bind(&slot)
            .execute(&self.pool)
            .await;
        Ok(true)
    }
}

/// Row-decode error → persistence error.
fn map_row(e: sqlx::Error) -> PersistenceError {
    PersistenceError::db("deployment_archive row decode")(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_roundtrips_through_its_column_form() {
        for s in [
            ArchiveStatus::Validated,
            ArchiveStatus::Active,
            ArchiveStatus::Draining,
            ArchiveStatus::Retired,
        ] {
            assert_eq!(ArchiveStatus::parse(s.as_str()).unwrap(), s);
        }
        assert!(ArchiveStatus::parse("bogus").is_err());
    }
}
