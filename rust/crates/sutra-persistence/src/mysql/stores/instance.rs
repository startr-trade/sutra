//! Instance store, MySQL/MariaDB dialect (`instance_state`).
//!
//! Same surface as the reference implementation: upsert persist keyed by
//! `(deployment_id, instance_id)`, row-locking `load_for_update`, and the
//! claim/heartbeat/release/sweep quartet (claim = CAS on "unowned OR already mine";
//! heartbeat returning 0 rows means "swept — abandon"; release is the owner-scoped
//! hand-back at the quiescent point; sweep clears lapsed heartbeats). `rows_affected` is
//! matched-rows on these connections (`CLIENT_FOUND_ROWS`), matching the reference.

use sqlx::{MySqlConnection, MySqlPool};
use uuid::Uuid;

use crate::mysql::scope::begin_tx;
use crate::stores::{
    summarise_instances, InstanceFilter, InstanceState, InstanceStore, InstanceSummary,
};
use crate::{DeploymentId, PersistenceError, Result};

/// MySQL/MariaDB implementation of [`InstanceStore`].
#[derive(Debug, Clone)]
pub struct MySqlInstanceStore {
    pool: MySqlPool,
}

const SQL_UPSERT: &str = "INSERT INTO instance_state \
     (deployment_id, instance_id, serialised, updated_at) \
     VALUES (?, ?, ?, CURRENT_TIMESTAMP(6)) \
     ON DUPLICATE KEY UPDATE \
       serialised = VALUES(serialised), updated_at = CURRENT_TIMESTAMP(6)";

const SQL_SELECT: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = ? AND instance_id = ?";

const SQL_SELECT_FOR_UPDATE: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = ? AND instance_id = ? FOR UPDATE";

const SQL_DELETE: &str = "DELETE FROM instance_state WHERE deployment_id = ? AND instance_id = ?";

const SQL_COUNT_ACTIVE: &str = "SELECT COUNT(*) FROM instance_state WHERE deployment_id = ?";

const SQL_LIST: &str = "SELECT instance_id, serialised FROM instance_state \
     WHERE deployment_id = ? ORDER BY updated_at DESC";

const SQL_HEARTBEAT: &str = "UPDATE instance_state SET last_heartbeat_at = NOW(6) \
     WHERE deployment_id = ? AND instance_id = ? AND claim_owner = ?";

/// The claim CAS: unowned OR already ours (re-entrant refresh). The owner is bound TWICE
/// — MySQL placeholders are positional, so the WHERE arm needs its own bind.
const SQL_CLAIM: &str = "UPDATE instance_state SET \
       claim_owner = ?, claimed_at = NOW(6), last_heartbeat_at = NOW(6) \
     WHERE deployment_id = ? AND instance_id = ? \
       AND (claim_owner IS NULL OR claim_owner = ?)";

/// The owner-scoped hand-back — the `claim_owner = ?` predicate makes a duplicate release
/// harmless.
const SQL_RELEASE: &str = "UPDATE instance_state SET \
       claim_owner = NULL, claimed_at = NULL, last_heartbeat_at = NULL \
     WHERE deployment_id = ? AND instance_id = ? AND claim_owner = ?";

/// Microsecond-precision cutoff computed on the database clock (the dialect's stand-in
/// for the reference's fractional-seconds interval arithmetic).
const SQL_SWEEP: &str = "UPDATE instance_state SET \
       claim_owner = NULL, claimed_at = NULL, last_heartbeat_at = NULL \
     WHERE deployment_id = ? \
       AND last_heartbeat_at IS NOT NULL \
       AND last_heartbeat_at < TIMESTAMPADD(MICROSECOND, -?, NOW(6))";

impl MySqlInstanceStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Upsert on a caller-supplied connection (the transactional-step building block).
    pub async fn persist_in(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        state: &InstanceState,
    ) -> Result<()> {
        sqlx::query(SQL_UPSERT)
            .bind(deployment.as_str())
            .bind(state.instance_id)
            .bind(&state.serialised)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("instance persist"))?;
        Ok(())
    }

    /// Row-locking load on a caller-supplied transaction connection (`SELECT ... FOR
    /// UPDATE`). The lock is held until the caller's transaction ends — the
    /// concurrent-replica serialisation point.
    pub async fn load_for_update(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<InstanceState>> {
        let row: Option<(Uuid, Vec<u8>)> = sqlx::query_as(SQL_SELECT_FOR_UPDATE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_optional(conn)
            .await
            .map_err(PersistenceError::db("instance loadForUpdate"))?;
        Ok(row.map(|(instance_id, serialised)| InstanceState {
            instance_id,
            serialised,
        }))
    }
}

impl InstanceStore for MySqlInstanceStore {
    async fn persist(&self, deployment: &DeploymentId, state: &InstanceState) -> Result<()> {
        let mut tx = begin_tx(&self.pool).await?;
        Self::persist_in(&mut tx, deployment, state).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("instance persist commit"))
    }

    async fn load(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Option<InstanceState>> {
        let row: Option<(Uuid, Vec<u8>)> = sqlx::query_as(SQL_SELECT)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::db("instance load"))?;
        Ok(row.map(|(instance_id, serialised)| InstanceState {
            instance_id,
            serialised,
        }))
    }

    async fn count_active(&self, deployment: &DeploymentId) -> Result<i64> {
        sqlx::query_scalar(SQL_COUNT_ACTIVE)
            .bind(deployment.as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(PersistenceError::db("instance countActive"))
    }

    async fn list(
        &self,
        deployment: &DeploymentId,
        filter: &InstanceFilter,
    ) -> Result<Vec<InstanceSummary>> {
        let rows: Vec<(Uuid, Vec<u8>)> = sqlx::query_as(SQL_LIST)
            .bind(deployment.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::db("instance list"))?;
        summarise_instances(deployment, rows, filter)
    }

    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("instance delete"))?;
        Ok(())
    }

    async fn claim(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<bool> {
        let updated = sqlx::query(SQL_CLAIM)
            .bind(claim_owner)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(claim_owner)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("instance claim"))?
            .rows_affected();
        Ok(updated == 1)
    }

    async fn release(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64> {
        let released = sqlx::query(SQL_RELEASE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(claim_owner)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("instance release"))?
            .rows_affected();
        Ok(released)
    }

    async fn heartbeat(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        claim_owner: &str,
    ) -> Result<u64> {
        let updated = sqlx::query(SQL_HEARTBEAT)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(claim_owner)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("instance heartbeat"))?
            .rows_affected();
        Ok(updated)
    }

    async fn sweep_stuck(
        &self,
        deployment: &DeploymentId,
        claim_timeout: std::time::Duration,
    ) -> Result<u64> {
        // Whole-microseconds bind keeps sub-second timeouts precise (parity with
        // the reference's fractional-seconds interval).
        let micros = i64::try_from(claim_timeout.as_micros()).map_err(|_| {
            PersistenceError::InvalidArgument("claim timeout out of range".to_owned())
        })?;
        let swept = sqlx::query(SQL_SWEEP)
            .bind(deployment.as_str())
            .bind(micros)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("instance sweep"))?
            .rows_affected();
        Ok(swept)
    }
}
