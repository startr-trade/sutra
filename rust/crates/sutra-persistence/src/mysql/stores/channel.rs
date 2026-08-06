//! Channel-concurrency store, MySQL/MariaDB dialect (`channel_instance`).
//!
//! Same surface as the reference implementation: one row per non-terminal instance,
//! `(deployment_id, instance_id)` PK, status `RUNNING`/`WAITING`, admission `COUNT(*)`
//! coherent across replicas.

use sqlx::{MySqlConnection, MySqlPool};
use uuid::Uuid;

use crate::stores::ChannelConcurrencyStore;
use crate::{DeploymentId, PersistenceError, Result};

/// MySQL/MariaDB implementation of [`ChannelConcurrencyStore`].
#[derive(Debug, Clone)]
pub struct MySqlChannelConcurrencyStore {
    pool: MySqlPool,
}

const SQL_RECORD_STARTED: &str = "INSERT INTO channel_instance \
     (deployment_id, instance_id, channel, status, updated_at) \
     VALUES (?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP(6)) \
     ON DUPLICATE KEY UPDATE \
       channel = VALUES(channel), status = 'RUNNING', updated_at = CURRENT_TIMESTAMP(6)";

const SQL_SET_STATUS: &str = "UPDATE channel_instance \
     SET status = ?, updated_at = CURRENT_TIMESTAMP(6) \
     WHERE deployment_id = ? AND instance_id = ?";

const SQL_DELETE: &str = "DELETE FROM channel_instance WHERE deployment_id = ? AND instance_id = ?";

const SQL_COUNT_RUNNING: &str = "SELECT COUNT(*) FROM channel_instance \
     WHERE deployment_id = ? AND channel = ? AND status = 'RUNNING'";

const SQL_COUNT_ALL: &str =
    "SELECT COUNT(*) FROM channel_instance WHERE deployment_id = ? AND channel = ?";

impl MySqlChannelConcurrencyStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    async fn set_status(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        status: &str,
    ) -> Result<()> {
        sqlx::query(SQL_SET_STATUS)
            .bind(status)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("channel setStatus"))?;
        Ok(())
    }

    /// Upsert on a caller-supplied connection (step building block for channel-started
    /// rows).
    pub async fn record_started_in(
        conn: &mut MySqlConnection,
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

impl ChannelConcurrencyStore for MySqlChannelConcurrencyStore {
    async fn record_started(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        channel: &str,
    ) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(PersistenceError::db("channel recordStarted acquire"))?;
        Self::record_started_in(&mut conn, deployment, instance_id, channel).await
    }

    async fn record_suspended(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        self.set_status(deployment, instance_id, "WAITING").await
    }

    async fn record_resumed(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        self.set_status(deployment, instance_id, "RUNNING").await
    }

    async fn record_terminal(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("channel recordTerminal"))?;
        Ok(())
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
        sqlx::query_scalar(sql)
            .bind(deployment.as_str())
            .bind(channel)
            .fetch_one(&self.pool)
            .await
            .map_err(PersistenceError::db("channel countActiveByChannel"))
    }
}
