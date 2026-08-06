//! Alias store, MySQL/MariaDB dialect (`alias_index`).
//!
//! Same surface and conflict-resolution flow as the reference implementation. The
//! unique-LIVE guarantee rides the generated-column unique key installed by the dialect's
//! V101 (the ruled workaround for the missing partial-index feature): a plain INSERT
//! whose duplicate-key rejection (either the row PK — idempotent re-attempt — or the
//! unique-live key — collision) is disambiguated by re-reading the live owner.

use sqlx::{MySqlConnection, MySqlPool, Row};
use uuid::Uuid;

use crate::mysql::is_duplicate_key;
use crate::stores::{AliasRow, AliasStore};
use crate::{DeploymentId, PersistenceError, Result};

/// MySQL/MariaDB implementation of [`AliasStore`].
#[derive(Debug, Clone)]
pub struct MySqlAliasStore {
    pool: MySqlPool,
}

const SQL_INSERT: &str = "INSERT INTO alias_index \
     (deployment_id, instance_id, alias_name, alias_value, unique_alias, live) \
     VALUES (?, ?, ?, ?, ?, TRUE)";

const SQL_FIND_LIVE: &str = "SELECT instance_id FROM alias_index \
     WHERE deployment_id = ? AND alias_name = ? AND alias_value = ? AND live = TRUE \
     LIMIT 1";

const SQL_RETIRE: &str = "UPDATE alias_index SET live = FALSE \
     WHERE deployment_id = ? AND instance_id = ? AND live = TRUE";

const SQL_LIST_FOR: &str = "SELECT alias_name, alias_value, unique_alias, live FROM alias_index \
     WHERE deployment_id = ? AND instance_id = ?";

const SQL_DELETE: &str = "DELETE FROM alias_index WHERE deployment_id = ? AND instance_id = ?";

impl MySqlAliasStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Record on a caller-supplied connection (the transactional-step building block).
    /// Same return semantics as [`AliasStore::record`].
    ///
    /// A duplicate-key rejection terminates only the INSERT statement, never the caller's
    /// transaction — the conflict path continues inside the same transaction, exactly like
    /// the reference dialect's conflict-ignoring insert.
    pub async fn record_in(
        conn: &mut MySqlConnection,
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
            .await;
        match inserted {
            Ok(_) => return Ok(true),
            Err(e) if is_duplicate_key(&e) => {
                // Conflict path — fall through to disambiguation below.
            }
            Err(e) => return Err(PersistenceError::db("alias record")(e)),
        }
        // Non-unique aliases can only have hit the row PK (same instance re-attempt) —
        // idempotent success. Unique aliases must disambiguate idempotent retry vs a
        // DIFFERENT live instance owning this (deployment, name, value).
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

impl AliasStore for MySqlAliasStore {
    async fn record(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        alias_name: &str,
        alias_value: &str,
        unique: bool,
    ) -> Result<bool> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(PersistenceError::db("alias record acquire"))?;
        Self::record_in(
            &mut conn,
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
        sqlx::query_scalar(SQL_FIND_LIVE)
            .bind(deployment.as_str())
            .bind(alias_name)
            .bind(alias_value)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::db("alias findLive"))
    }

    async fn retire(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        sqlx::query(SQL_RETIRE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("alias retire"))?;
        Ok(())
    }

    async fn list_for(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Vec<AliasRow>> {
        let rows = sqlx::query(SQL_LIST_FOR)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::db("alias listFor"))?;

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
                    alias_name: crate::mysql::str_col(row, "alias_name")?,
                    alias_value: crate::mysql::str_col(row, "alias_value")?,
                    unique_alias: row.try_get("unique_alias").map_err(e)?,
                    live: row.try_get("live").map_err(e)?,
                })
            })
            .collect()
    }

    async fn delete(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("alias delete"))?;
        Ok(())
    }
}
