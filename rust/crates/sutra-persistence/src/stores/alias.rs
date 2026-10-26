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

/// Retire ONE alias row of a still-live instance — the key of a wait it has left.
const SQL_RETIRE_ONE: &str = "UPDATE alias_index SET live = FALSE \
     WHERE deployment_id = $1 AND instance_id = $2 AND alias_name = $3 AND alias_value = $4 \
       AND live = TRUE";

/// Bring a retired row of the SAME instance back to life — a loop whose new iteration parks on
/// a key an earlier iteration retired.
const SQL_REVIVE: &str = "UPDATE alias_index SET live = TRUE, unique_alias = $5 \
     WHERE deployment_id = $1 AND instance_id = $2 AND alias_name = $3 AND alias_value = $4 \
       AND live = FALSE";

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
        // re-attempt, or this instance's own row RETIRED by an earlier step) — revive it and
        // succeed. Unique aliases must disambiguate idempotent retry vs a DIFFERENT live
        // instance owning this (deployment, name, value) first: only an unowned key may be
        // revived, or the revival itself would break the unique-live guarantee.
        if unique {
            let owner: Option<Uuid> = sqlx::query_scalar(SQL_FIND_LIVE)
                .bind(deployment.as_str())
                .bind(alias_name)
                .bind(alias_value)
                .fetch_optional(&mut *conn)
                .await
                .map_err(PersistenceError::db("alias record owner check"))?;
            if let Some(owner) = owner {
                return Ok(owner == instance_id);
            }
        }
        let revived = sqlx::query(SQL_REVIVE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(alias_name)
            .bind(alias_value)
            .bind(unique)
            .execute(&mut *conn)
            .await
            .map_err(PersistenceError::db("alias revive"))?
            .rows_affected();
        // A non-unique conflict with nothing to revive is the same instance's live row — an
        // idempotent re-attempt. A unique key with no live owner and no own row to revive
        // cannot happen (the insert conflicted with something); report it as not recorded.
        Ok(revived == 1 || !unique)
    }

    /// Retire ONE alias row of an instance that stays live, on a caller-supplied connection
    /// (the transactional-step building block): the correlation key of a wait the instance
    /// has left. A missing or already-retired row is a no-op.
    pub async fn retire_one_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        alias_name: &str,
        alias_value: &str,
    ) -> Result<()> {
        sqlx::query(SQL_RETIRE_ONE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(alias_name)
            .bind(alias_value)
            .execute(&mut *conn)
            .await
            .map_err(PersistenceError::db("alias retire one"))?;
        Ok(())
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
