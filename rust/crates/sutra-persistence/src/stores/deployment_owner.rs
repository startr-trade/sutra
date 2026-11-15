//! The deployment → owner map (`deployment_owner`, V305) the inbox reads to dedupe per owner. Engine
//! infrastructure like `deployment_archive`: no deployment scope, no RLS. A deployment's owner never
//! changes (both are derived from its identity), so a row, once written, is never updated.

use sqlx::{PgConnection, PgPool};

use crate::{PersistenceError, Result};

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgDeploymentOwnerStore {
    pool: PgPool,
}

const SQL_UPSERT_MANY: &str = "INSERT INTO deployment_owner (deployment_id, owner_id) \
     SELECT * FROM UNNEST($1::text[], $2::text[]) \
     ON CONFLICT (deployment_id) DO NOTHING";

impl PgDeploymentOwnerStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record each `(deployment id, owner id)` that is not recorded yet, in one statement.
    pub async fn upsert_many(&self, rows: &[(String, String)]) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(PersistenceError::db("deployment_owner acquire"))?;
        Self::upsert_many_in(&mut conn, rows).await
    }

    /// [`Self::upsert_many`] on a caller's connection or transaction.
    pub async fn upsert_many_in(conn: &mut PgConnection, rows: &[(String, String)]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let (deployments, owners): (Vec<&str>, Vec<&str>) = rows
            .iter()
            .map(|(deployment, owner)| (deployment.as_str(), owner.as_str()))
            .unzip();
        sqlx::query(SQL_UPSERT_MANY)
            .bind(deployments)
            .bind(owners)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("deployment_owner upsert"))?;
        Ok(())
    }

    /// The owner recorded for `deployment_id`, if any.
    pub async fn owner_of(&self, deployment_id: &str) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT owner_id FROM deployment_owner WHERE deployment_id = $1")
            .bind(deployment_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::db("deployment_owner read"))
    }
}
