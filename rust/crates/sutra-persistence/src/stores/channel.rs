//! Channel-concurrency store (`channel_instance`, V701–V702) — the replica-coherent source of
//! truth for the per-channel concurrency cap.
//!
//! Semantics: one row per non-terminal instance,
//! `(deployment_id, instance_id)` PK (an instance lives on exactly one channel), status
//! `RUNNING` (in-flight) or `WAITING` (parked). Admission reads `COUNT(*)` — the same count
//! on every replica.

use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// Store trait for the per-channel concurrency substrate.
pub trait ChannelConcurrencyStore {
    /// UPSERT a RUNNING row at instance start (redelivery re-dispatching the same id resets
    /// it rather than duplicating).
    async fn record_started(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        channel: &str,
    ) -> Result<()>;
    /// Instance parked at a wait node → WAITING. Unknown instance is a silent no-op.
    async fn record_suspended(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
    /// Instance resumed → RUNNING. Unknown instance is a silent no-op.
    async fn record_resumed(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
    /// Instance completed/failed → row deleted. Unknown instance is a silent no-op.
    async fn record_terminal(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
    /// Admission count: RUNNING only (`include_waiting == false`) or RUNNING + WAITING
    /// (`true`, e.g. VoIP where a held call still holds its line).
    async fn count_active_by_channel(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        include_waiting: bool,
    ) -> Result<i64>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgChannelConcurrencyStore {
    pool: PgPool,
}

const SQL_RECORD_STARTED: &str = "INSERT INTO channel_instance \
     (deployment_id, instance_id, channel, status, updated_at) \
     VALUES ($1, $2, $3, 'RUNNING', CURRENT_TIMESTAMP) \
     ON CONFLICT (deployment_id, instance_id) DO UPDATE \
     SET channel = EXCLUDED.channel, status = 'RUNNING', updated_at = CURRENT_TIMESTAMP";

const SQL_SET_STATUS: &str = "UPDATE channel_instance \
     SET status = $1, updated_at = CURRENT_TIMESTAMP \
     WHERE deployment_id = $2 AND instance_id = $3";

const SQL_DELETE: &str =
    "DELETE FROM channel_instance WHERE deployment_id = $1 AND instance_id = $2";

const SQL_COUNT_RUNNING: &str = "SELECT COUNT(*) FROM channel_instance \
     WHERE deployment_id = $1 AND channel = $2 AND status = 'RUNNING'";

const SQL_COUNT_ALL: &str =
    "SELECT COUNT(*) FROM channel_instance WHERE deployment_id = $1 AND channel = $2";

impl PgChannelConcurrencyStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn set_status(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        status: &str,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_SET_STATUS)
            .bind(status)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("channel setStatus"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("channel setStatus commit"))
    }

    /// UPSERT on a caller-supplied connection (step building block for channel-started rows).
    pub async fn record_started_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        channel: &str,
    ) -> Result<()> {
        sqlx::query(SQL_RECORD_STARTED)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(channel)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("channel recordStarted"))?;
        Ok(())
    }
}

impl ChannelConcurrencyStore for PgChannelConcurrencyStore {
    async fn record_started(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        channel: &str,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        Self::record_started_in(&mut tx, deployment, instance_id, channel).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("channel recordStarted commit"))
    }

    async fn record_suspended(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        self.set_status(deployment, instance_id, "WAITING").await
    }

    async fn record_resumed(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        self.set_status(deployment, instance_id, "RUNNING").await
    }

    async fn record_terminal(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("channel recordTerminal"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("channel recordTerminal commit"))
    }

    async fn count_active_by_channel(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        include_waiting: bool,
    ) -> Result<i64> {
        let sql = if include_waiting {
            SQL_COUNT_ALL
        } else {
            SQL_COUNT_RUNNING
        };
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let count: i64 = sqlx::query_scalar(sql)
            .bind(deployment.as_str())
            .bind(channel)
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("channel countActiveByChannel"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("channel countActiveByChannel commit"))?;
        Ok(count)
    }
}
