//! Alias store, SQL Server dialect (`alias_index`).
//!
//! Same surface and conflict-resolution flow as the reference implementation. The
//! unique-LIVE guarantee rides the FILTERED unique index installed by the dialect's V101
//! (the 1:1 mapping of the reference's partial unique index): a plain INSERT whose
//! duplicate-key rejection (either the row PK — idempotent re-attempt — or the filtered
//! unique index — collision) is disambiguated by re-reading the live owner. Transactions
//! run XACT_ABORT OFF, so the rejection terminates only the INSERT statement.

use uuid::Uuid;

use crate::mssql::{is_duplicate_key, req, MssqlClient, MssqlPool};
use crate::stores::{AliasRow, AliasStore};
use crate::{DeploymentId, PersistenceError, Result};

/// SQL Server implementation of [`AliasStore`].
#[derive(Clone)]
pub struct MssqlAliasStore {
    pool: MssqlPool,
}

const SQL_INSERT: &str = "INSERT INTO alias_index \
     (deployment_id, instance_id, alias_name, alias_value, unique_alias, live) \
     VALUES (@P1, @P2, @P3, @P4, @P5, 1)";

const SQL_FIND_LIVE: &str = "SELECT TOP (1) instance_id FROM alias_index \
     WHERE deployment_id = @P1 AND alias_name = @P2 AND alias_value = @P3 AND live = 1";

const SQL_RETIRE: &str = "UPDATE alias_index SET live = 0 \
     WHERE deployment_id = @P1 AND instance_id = @P2 AND live = 1";

const SQL_LIST_FOR: &str = "SELECT alias_name, alias_value, unique_alias, live \
     FROM alias_index WHERE deployment_id = @P1 AND instance_id = @P2";

const SQL_DELETE: &str = "DELETE FROM alias_index WHERE deployment_id = @P1 AND instance_id = @P2";

async fn find_live_on(
    client: &mut MssqlClient,
    deployment: &DeploymentId,
    alias_name: &str,
    alias_value: &str,
) -> Result<Option<Uuid>> {
    let row = client
        .query(
            SQL_FIND_LIVE,
            &[&deployment.as_str(), &alias_name, &alias_value],
        )
        .await
        .map_err(PersistenceError::mssql("alias findLive"))?
        .into_row()
        .await
        .map_err(PersistenceError::mssql("alias findLive row"))?;
    row.as_ref()
        .map(|r| req::<Uuid>(r, "instance_id"))
        .transpose()
}

impl MssqlAliasStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }

    /// Record on a caller-supplied connection (the transactional-step building block).
    /// Same return semantics as [`AliasStore::record`].
    pub async fn record_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        instance_id: Uuid,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> Result<bool> {
        let inserted = client
            .execute(
                SQL_INSERT,
                &[
                    &deployment.as_str(),
                    &instance_id,
                    &alias_name,
                    &alias_value,
                    &unique,
                ],
            )
            .await;
        match inserted {
            Ok(_) => return Ok(true),
            Err(e) if is_duplicate_key(&e) => {
                // Conflict path — fall through to disambiguation below.
            }
            Err(e) => return Err(PersistenceError::mssql("alias record")(e)),
        }
        // Non-unique aliases can only have hit the row PK (same instance re-attempt) —
        // idempotent success. Unique aliases must disambiguate idempotent retry vs a
        // DIFFERENT live instance owning this (deployment, name, value).
        if !unique {
            return Ok(true);
        }
        let owner = find_live_on(client, deployment, alias_name, alias_value).await?;
        Ok(owner == Some(instance_id))
    }
}

impl AliasStore for MssqlAliasStore {
    async fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        Self::record_in(
            conn.client(),
            deployment,
            instance_id,
            alias_name,
            alias_value,
            unique,
        )
        .await
    }

    async fn find_live(
        &self,
        deployment: &DeploymentId,
        alias_name: &str,
        alias_value: &str,
    ) -> Result<Option<Uuid>> {
        let mut conn = self.pool.acquire().await?;
        find_live_on(conn.client(), deployment, alias_name, alias_value).await
    }

    async fn retire(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(SQL_RETIRE, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("alias retire"))?;
        Ok(())
    }

    async fn list_for(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Vec<AliasRow>> {
        let mut conn = self.pool.acquire().await?;
        let rows = conn
            .client()
            .query(SQL_LIST_FOR, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("alias listFor"))?
            .into_first_result()
            .await
            .map_err(PersistenceError::mssql("alias listFor rows"))?;
        rows.iter()
            .map(|row| {
                Ok(AliasRow {
                    deployment: deployment.clone(),
                    instance_id,
                    alias_name: req::<&str>(row, "alias_name")?.to_owned(),
                    alias_value: req::<&str>(row, "alias_value")?.to_owned(),
                    unique_alias: req(row, "unique_alias")?,
                    live: req(row, "live")?,
                })
            })
            .collect()
    }

    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(SQL_DELETE, &[&deployment.as_str(), &instance_id])
            .await
            .map_err(PersistenceError::mssql("alias delete"))?;
        Ok(())
    }
}
