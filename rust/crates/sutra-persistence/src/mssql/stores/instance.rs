//! Instance store, SQL Server dialect (`instance_state`).
//!
//! Same surface as the reference implementation: upsert persist keyed by
//! `(deployment_id, instance_id)` (MERGE WITH (HOLDLOCK) — atomic under concurrency),
//! row-locking `load_for_update` (UPDLOCK), and the claim/heartbeat/sweep trio
//! (claim = CAS on `claim_owner IS NULL`; heartbeat returning 0 rows means "swept —
//! abandon"; sweep clears lapsed heartbeats). SQL Server reports matched rows for
//! UPDATE, matching the reference's `rows_affected` semantics.

use uuid::Uuid;

use crate::mssql::{req, MssqlClient, MssqlPool};
use crate::stores::{
    summarise_instances, InstanceFilter, InstanceState, InstanceStore, InstanceSummary,
};
use crate::{DeploymentId, PersistenceError, Result};

/// SQL Server implementation of [`InstanceStore`].
#[derive(Clone)]
pub struct MssqlInstanceStore {
    pool: MssqlPool,
}

/// Upsert = UPDATE, INSERT on zero rows, retry-as-UPDATE on a duplicate-key race. The
/// serializable-MERGE alternative range-locks and deadlocks under concurrent inserts;
/// this shape only ever takes the single-key locks the reference dialect takes.
const SQL_UPDATE: &str = "UPDATE instance_state \
     SET serialised = @P3, updated_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P1 AND instance_id = @P2";

const SQL_UPSERT: &str = "UPDATE instance_state \
     SET serialised = @P3, updated_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P1 AND instance_id = @P2; \
     IF @@ROWCOUNT = 0 \
       INSERT INTO instance_state (deployment_id, instance_id, serialised, updated_at) \
       VALUES (@P1, @P2, @P3, SYSUTCDATETIME());";

const SQL_SELECT: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = @P1 AND instance_id = @P2";

const SQL_SELECT_FOR_UPDATE: &str = "SELECT instance_id, serialised \
     FROM instance_state WITH (UPDLOCK, ROWLOCK) \
     WHERE deployment_id = @P1 AND instance_id = @P2";

const SQL_DELETE: &str =
    "DELETE FROM instance_state WHERE deployment_id = @P1 AND instance_id = @P2";

const SQL_COUNT_ACTIVE: &str =
    "SELECT COUNT_BIG(*) AS n FROM instance_state WHERE deployment_id = @P1";

const SQL_LIST: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = @P1 ORDER BY updated_at DESC";

const SQL_HEARTBEAT: &str = "UPDATE instance_state SET last_heartbeat_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND claim_owner = @P3";

/// The claim CAS: unowned OR already ours (re-entrant refresh). `@P1` is referenced twice —
/// T-SQL parameters are named, so the owner binds once.
const SQL_CLAIM: &str = "UPDATE instance_state SET \
       claim_owner = @P1, claimed_at = SYSUTCDATETIME(), last_heartbeat_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P2 AND instance_id = @P3 \
       AND (claim_owner IS NULL OR claim_owner = @P1)";

/// The owner-scoped hand-back — the `claim_owner = @P3` predicate makes a duplicate
/// release harmless.
const SQL_RELEASE: &str = "UPDATE instance_state SET \
       claim_owner = NULL, claimed_at = NULL, last_heartbeat_at = NULL \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND claim_owner = @P3";

/// Millisecond cutoff computed on the database clock (DATEADD's count argument is a
/// 32-bit int, so milliseconds bound the practical claim-timeout range at ~24 days —
/// far beyond any sweep configuration).
const SQL_SWEEP: &str = "UPDATE instance_state SET \
       claim_owner = NULL, claimed_at = NULL, last_heartbeat_at = NULL \
     WHERE deployment_id = @P1 \
       AND last_heartbeat_at IS NOT NULL \
       AND last_heartbeat_at < DATEADD(MILLISECOND, -@P2, SYSUTCDATETIME())";

fn state_of(row: &tiberius::Row) -> Result<InstanceState> {
    Ok(InstanceState {
        instance_id: req::<Uuid>(row, "instance_id")?,
        serialised: req::<&[u8]>(row, "serialised")?.to_vec(),
    })
}

impl MssqlInstanceStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }

    /// Upsert on a caller-supplied connection (the transactional-step building block).
    pub async fn persist_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        state: &InstanceState,
    ) -> Result<()> {
        let dep = deployment.as_str();
        let bytes = state.serialised.as_slice();
        let outcome = client
            .execute(SQL_UPSERT, &[&dep, &state.instance_id, &bytes])
            .await;
        match outcome {
            Ok(_) => Ok(()),
            Err(e) if crate::mssql::is_duplicate_key(&e) => {
                // A concurrent inserter won the race between our UPDATE and INSERT; the
                // row exists now, so the UPDATE half applies cleanly.
                client
                    .execute(SQL_UPDATE, &[&dep, &state.instance_id, &bytes])
                    .await
                    .map_err(PersistenceError::mssql("instance persist retry"))?;
                Ok(())
            }
            Err(e) => Err(PersistenceError::mssql("instance persist")(e)),
        }
    }

    /// Row-locking load on a caller-supplied transaction connection (UPDLOCK — held until
    /// the transaction ends, the concurrent-replica serialisation point).
    pub async fn load_for_update(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<InstanceState>> {
        let row = client
            .query(SQL_SELECT_FOR_UPDATE, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("instance loadForUpdate"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("instance loadForUpdate row"))?;
        row.as_ref().map(state_of).transpose()
    }
}

impl InstanceStore for MssqlInstanceStore {
    async fn persist(&self, deployment: &DeploymentId, state: &InstanceState) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        Self::persist_in(conn.client(), deployment, state).await
    }

    async fn load(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<InstanceState>> {
        let mut conn = self.pool.acquire().await?;
        let row = conn
            .client()
            .query(SQL_SELECT, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("instance load"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("instance load row"))?;
        row.as_ref().map(state_of).transpose()
    }

    async fn count_active(&self, deployment: &DeploymentId) -> Result<i64> {
        let mut conn = self.pool.acquire().await?;
        let row = conn
            .client()
            .query(SQL_COUNT_ACTIVE, &[&deployment.as_str()])
            .await
            .map_err(PersistenceError::mssql("instance countActive"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("instance countActive row"))?
            .ok_or_else(|| {
                PersistenceError::InvalidArgument("count query returned no row".to_owned())
            })?;
        req::<i64>(&row, "n")
    }

    async fn list(
        &self,
        deployment: &DeploymentId,
        filter: &InstanceFilter,
    ) -> Result<Vec<InstanceSummary>> {
        let mut conn = self.pool.acquire().await?;
        let rows = conn
            .client()
            .query(SQL_LIST, &[&deployment.as_str()])
            .await
            .map_err(PersistenceError::mssql("instance list"))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql("instance list rows"))?;
        let decoded: Vec<(Uuid, Vec<u8>)> = rows
            .iter()
            .map(|row| {
                Ok((
                    req::<Uuid>(row, "instance_id")?,
                    req::<&[u8]>(row, "serialised")?.to_vec(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        summarise_instances(deployment, decoded, filter)
    }

    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(SQL_DELETE, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("instance delete"))?;
        Ok(())
    }

    async fn claim(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        let updated = conn
            .client()
            .execute(
                SQL_CLAIM,
                &[&claim_owner, &deployment.as_str(), &instance_id],
            )
            .await
            .map_err(PersistenceError::mssql("instance claim"))?
            .total();
        Ok(updated == 1)
    }

    async fn release(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64> {
        let mut conn = self.pool.acquire().await?;
        let released = conn
            .client()
            .execute(
                SQL_RELEASE,
                &[&deployment.as_str(), &instance_id, &claim_owner],
            )
            .await
            .map_err(PersistenceError::mssql("instance release"))?
            .total();
        Ok(released)
    }

    async fn heartbeat(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64> {
        let mut conn = self.pool.acquire().await?;
        let updated = conn
            .client()
            .execute(
                SQL_HEARTBEAT,
                &[&deployment.as_str(), &instance_id, &claim_owner],
            )
            .await
            .map_err(PersistenceError::mssql("instance heartbeat"))?
            .total();
        Ok(updated)
    }

    async fn sweep_stuck(
        &self,
        deployment: &DeploymentId,
        claim_timeout: std::time::Duration,
    ) -> Result<u64> {
        let millis = i32::try_from(claim_timeout.as_millis()).map_err(|_| {
            PersistenceError::InvalidArgument("claim timeout out of range".to_owned())
        })?;
        let mut conn = self.pool.acquire().await?;
        let swept = conn
            .client()
            .execute(SQL_SWEEP, &[&deployment.as_str(), &millis])
            .await
            .map_err(PersistenceError::mssql("instance sweep"))?
            .total();
        Ok(swept)
    }
}
