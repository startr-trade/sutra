//! Channel-concurrency store, SQL Server dialect (`channel_instance`).
//!
//! Same surface as the reference implementation: one row per non-terminal instance,
//! `(deployment_id, instance_id)` PK (MERGE conflict target), status `RUNNING`/`WAITING`,
//! admission count coherent across replicas.

use uuid::Uuid;

use crate::mssql::{req, MssqlClient, MssqlPool};
use crate::stores::ChannelConcurrencyStore;
use crate::{DeploymentId, PersistenceError, Result};

/// SQL Server implementation of [`ChannelConcurrencyStore`].
#[derive(Clone)]
pub struct MssqlChannelConcurrencyStore {
    pool: MssqlPool,
}

/// Upsert = UPDATE, INSERT on zero rows, retry-as-UPDATE on a duplicate-key race (the
/// same non-deadlocking shape the instance store uses).
const SQL_RECORD_STARTED_UPDATE: &str = "UPDATE channel_instance SET \
       channel = @P3, status = 'RUNNING', updated_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P1 AND instance_id = @P2";

const SQL_RECORD_STARTED: &str = "UPDATE channel_instance SET \
       channel = @P3, status = 'RUNNING', updated_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P1 AND instance_id = @P2; \
     IF @@ROWCOUNT = 0 \
       INSERT INTO channel_instance (deployment_id, instance_id, channel, status, updated_at) \
       VALUES (@P1, @P2, @P3, 'RUNNING', SYSUTCDATETIME());";

const SQL_SET_STATUS: &str = "UPDATE channel_instance \
     SET status = @P1, updated_at = SYSUTCDATETIME() \
     WHERE deployment_id = @P2 AND instance_id = @P3";

const SQL_DELETE: &str =
    "DELETE FROM channel_instance WHERE deployment_id = @P1 AND instance_id = @P2";

const SQL_COUNT_RUNNING: &str = "SELECT COUNT_BIG(*) AS n FROM channel_instance \
     WHERE deployment_id = @P1 AND channel = @P2 AND status = 'RUNNING'";

const SQL_COUNT_ALL: &str = "SELECT COUNT_BIG(*) AS n FROM channel_instance \
     WHERE deployment_id = @P1 AND channel = @P2";

impl MssqlChannelConcurrencyStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }

    async fn set_status(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        status: &str,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(
                SQL_SET_STATUS,
                &[&status, &deployment.as_str(), &instance_id],
            )
            .await
            .map_err(PersistenceError::mssql("channel setStatus"))?;
        Ok(())
    }

    /// Upsert on a caller-supplied connection (step building block for channel-started
    /// rows).
    pub async fn record_started_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        instance_id: Uuid,
        channel: &str,
    ) -> Result<()> {
        let dep = deployment.as_str();
        let outcome = client
            .execute(SQL_RECORD_STARTED, &[&dep, &instance_id, &channel])
            .await;
        match outcome {
            Ok(_) => Ok(()),
            Err(e) if crate::mssql::is_duplicate_key(&e) => {
                client
                    .execute(SQL_RECORD_STARTED_UPDATE, &[&dep, &instance_id, &channel])
                    .await
                    .map_err(PersistenceError::mssql("channel recordStarted retry"))?;
                Ok(())
            }
            Err(e) => Err(PersistenceError::mssql("channel recordStarted")(e)),
        }
    }
}

impl ChannelConcurrencyStore for MssqlChannelConcurrencyStore {
    async fn record_started(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        channel: &str,
    ) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        Self::record_started_in(conn.client(), deployment, instance_id, channel).await
    }

    async fn record_suspended(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        self.set_status(deployment, instance_id, "WAITING").await
    }

    async fn record_resumed(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        self.set_status(deployment, instance_id, "RUNNING").await
    }

    async fn record_terminal(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(SQL_DELETE, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("channel recordTerminal"))?;
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
        let mut conn = self.pool.acquire().await?;
        let row = conn
            .client()
            .query(sql, &[&deployment.as_str(), &channel])
            .await
            .map_err(PersistenceError::mssql("channel countActiveByChannel"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("channel countActiveByChannel row"))?
            .ok_or_else(|| {
                PersistenceError::InvalidArgument("count query returned no row".to_owned())
            })?;
        req::<i64>(&row, "n")
    }
}
