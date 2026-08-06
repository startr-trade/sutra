//! Alias store (`alias_index`, V101) — THE relay-correlation mechanism.
//!
//! Semantics: aliases written at instance start with
//! `INSERT ... ON CONFLICT DO NOTHING`; the partial unique index
//! `alias_index_unique_live (deployment_id, alias_name, alias_value) WHERE unique_alias AND
//! live` enforces the unique-alias guarantee atomically — the first inserter wins, and a
//! conflicting caller disambiguates idempotent retry (same instance) from genuine collision
//! (different live instance) by re-reading the live owner. `find_live` resolves the instance
//! to resume; `retire` on terminal.

use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// One alias row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasRow {
    /// Owning deployment.
    pub deployment: DeploymentId,
    /// Owning instance.
    pub instance_id: Uuid,
    /// Alias name (from the `<q:alias>` binding).
    pub alias_name: String,
    /// Alias value.
    pub alias_value: String,
    /// Whether the unique-live guarantee applies.
    pub unique_alias: bool,
    /// Live (correlatable) vs retired.
    pub live: bool,
}

/// Store trait for relay correlation.
pub trait AliasStore {
    /// Records an alias. Returns `true` on success (including idempotent re-attempts by the
    /// same instance); `false` when a unique alias is already owned by a DIFFERENT live
    /// instance (the collision the engine resolves via `onConflict=reject`/`correlate`).
    async fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> Result<bool>;
    /// Resolves the live instance for `(deployment, name, value)` — the follow-up-signal
    /// correlation path.
    async fn find_live(
        &self,
        deployment: &DeploymentId,
        alias_name: &str,
        alias_value: &str,
    ) -> Result<Option<Uuid>>;
    /// Retires every live alias of the instance (terminal transition).
    async fn retire(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
    /// Lists all alias rows of one instance (admin/diagnostics).
    async fn list_for(&self, deployment: &DeploymentId, instance_id: Uuid)
        -> Result<Vec<AliasRow>>;
    /// Hard-deletes every alias row of the instance (GDPR erasure) — in contrast
    /// to [`AliasStore::retire`]'s soft `live = FALSE` flip, the rows are removed entirely.
    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgAliasStore {
    pool: PgPool,
}

const SQL_INSERT: &str = "INSERT INTO alias_index \
     (deployment_id, instance_id, alias_name, alias_value, unique_alias, live) \
     VALUES ($1, $2, $3, $4, $5, TRUE) \
     ON CONFLICT DO NOTHING";

const SQL_FIND_LIVE: &str = "SELECT instance_id FROM alias_index \
     WHERE deployment_id = $1 AND alias_name = $2 AND alias_value = $3 AND live = TRUE \
     LIMIT 1";

const SQL_RETIRE: &str = "UPDATE alias_index SET live = FALSE \
     WHERE deployment_id = $1 AND instance_id = $2 AND live = TRUE";

const SQL_LIST_FOR: &str = "SELECT alias_name, alias_value, unique_alias, live FROM alias_index \
     WHERE deployment_id = $1 AND instance_id = $2";

const SQL_DELETE: &str = "DELETE FROM alias_index WHERE deployment_id = $1 AND instance_id = $2";

impl PgAliasStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record on a caller-supplied connection (the transactional-step building block).
    /// Same return semantics as [`AliasStore::record`].
    pub async fn record_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> Result<bool> {
        let inserted = sqlx::query(SQL_INSERT)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(alias_name)
            .bind(alias_value)
            .bind(unique)
            .execute(&mut *conn)
            .await
            .map_err(PersistenceError::db("alias record"))?
            .rows_affected();
        if inserted == 1 {
            return Ok(true);
        }
        // Conflict path. Non-unique aliases can only have hit the row PK (same instance
        // re-attempt) — idempotent success. Unique aliases must disambiguate idempotent
        // retry vs a DIFFERENT live instance owning this (deployment, name, value).
        if !unique {
            return Ok(true);
        }
        let owner: Option<Uuid> = sqlx::query_scalar(SQL_FIND_LIVE)
            .bind(deployment.as_str())
            .bind(alias_name)
            .bind(alias_value)
            .fetch_optional(conn)
            .await
            .map_err(PersistenceError::db("alias record owner check"))?;
        Ok(owner == Some(instance_id))
    }
}

impl AliasStore for PgAliasStore {
    async fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let recorded = Self::record_in(
            &mut tx,
            deployment,
            instance_id,
            alias_name,
            alias_value,
            unique,
        )
        .await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("alias record commit"))?;
        Ok(recorded)
    }

    async fn find_live(
        &self,
        deployment: &DeploymentId,
        alias_name: &str,
        alias_value: &str,
    ) -> Result<Option<Uuid>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let owner: Option<Uuid> = sqlx::query_scalar(SQL_FIND_LIVE)
            .bind(deployment.as_str())
            .bind(alias_name)
            .bind(alias_value)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("alias findLive"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("alias findLive commit"))?;
        Ok(owner)
    }

    async fn retire(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_RETIRE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("alias retire"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("alias retire commit"))
    }

    async fn list_for(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Vec<AliasRow>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows = sqlx::query(SQL_LIST_FOR)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("alias listFor"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("alias listFor commit"))?;

        fn e(source: sqlx::Error) -> PersistenceError {
            PersistenceError::Database {
                operation: "alias read row",
                source,
            }
        }
        rows.iter()
            .map(|row| {
                Ok(AliasRow {
                    deployment: deployment.clone(),
                    instance_id,
                    alias_name: row.try_get("alias_name").map_err(e)?,
                    alias_value: row.try_get("alias_value").map_err(e)?,
                    unique_alias: row.try_get("unique_alias").map_err(e)?,
                    live: row.try_get("live").map_err(e)?,
                })
            })
            .collect()
    }

    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("alias delete"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("alias delete commit"))
    }
}
