//! The durable record of who holds each exclusive inbound endpoint (`deployment_endpoint_holder`,
//! V1002), for the folder deployment source. A folder lists archives but cannot say which of two on
//! one endpoint got there first; this does, for every replica and across restarts. Engine
//! infrastructure like `deployment_archive`: no deployment scope, no RLS.

use sqlx::{PgPool, Row};

use crate::{PersistenceError, Result};

/// The longest values the record's columns hold, in characters.
pub const ENDPOINT_HOLDER_TRANSPORT_MAX: usize = 64;
pub const ENDPOINT_HOLDER_IDENTITY_MAX: usize = 2048;
pub const ENDPOINT_HOLDER_OWNER_MAX: usize = 1024;
pub const ENDPOINT_HOLDER_LABEL_MAX: usize = 1024;

/// One endpoint and who holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointHolderRow {
    pub transport: String,
    pub identity: String,
    pub owner: String,
    pub holder_deployment_id: String,
    pub holder_label: String,
    /// The other deployments on an endpoint that can have several, by id.
    pub also_held_by: Vec<String>,
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgEndpointHolderStore {
    pool: PgPool,
}

impl PgEndpointHolderStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Does this database have the table? An engine pointed at a migration root without the deploy
    /// family has no record to read.
    pub async fn available(&self) -> Result<bool> {
        sqlx::query_scalar("SELECT to_regclass('deployment_endpoint_holder') IS NOT NULL")
            .fetch_one(&self.pool)
            .await
            .map_err(PersistenceError::db("endpoint_holder available"))
    }

    /// Every recorded holder.
    pub async fn load(&self) -> Result<Vec<EndpointHolderRow>> {
        let rows = sqlx::query(
            "SELECT transport, identity, owner, holder_deployment_id, holder_label, also_held_by \
             FROM deployment_endpoint_holder",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(PersistenceError::db("endpoint_holder load"))?;
        rows.into_iter()
            .map(|r| {
                let text = |column: &str| -> Result<String> {
                    r.try_get(column)
                        .map_err(PersistenceError::db("endpoint_holder decode"))
                };
                Ok(EndpointHolderRow {
                    transport: text("transport")?,
                    identity: text("identity")?,
                    owner: text("owner")?,
                    holder_deployment_id: text("holder_deployment_id")?,
                    holder_label: text("holder_label")?,
                    also_held_by: r
                        .try_get("also_held_by")
                        .map_err(PersistenceError::db("endpoint_holder decode"))?,
                })
            })
            .collect()
    }

    /// Claim each endpoint for its row, all or none. Each claim comes with the owner the caller saw
    /// holding the endpoint and judged gone, if any. A claim succeeds when the endpoint is free, is
    /// already this owner's, or is still held by exactly that stale owner. Any other holder means a
    /// peer got there first: nothing is written and this returns `false`, and the caller judges again
    /// from what the record now says.
    pub async fn claim(&self, claims: &[(EndpointHolderRow, Option<String>)]) -> Result<bool> {
        if claims.is_empty() {
            return Ok(true);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PersistenceError::db("endpoint_holder claim begin"))?;
        for (row, stale_owner) in claims {
            let won = sqlx::query(
                "INSERT INTO deployment_endpoint_holder \
                   (transport, identity, owner, holder_deployment_id, holder_label, also_held_by) \
                 VALUES ($1, $2, $3, $4, $5, $7) \
                 ON CONFLICT (transport, identity) DO UPDATE SET \
                   owner = EXCLUDED.owner, \
                   holder_deployment_id = EXCLUDED.holder_deployment_id, \
                   holder_label = EXCLUDED.holder_label, \
                   also_held_by = EXCLUDED.also_held_by \
                 WHERE deployment_endpoint_holder.owner = EXCLUDED.owner \
                    OR deployment_endpoint_holder.owner = $6 \
                 RETURNING owner",
            )
            .bind(&row.transport)
            .bind(&row.identity)
            .bind(&row.owner)
            .bind(&row.holder_deployment_id)
            .bind(&row.holder_label)
            .bind(stale_owner)
            .bind(&row.also_held_by)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("endpoint_holder claim"))?;
            if won.is_none() {
                return Ok(false);
            }
        }
        tx.commit()
            .await
            .map_err(PersistenceError::db("endpoint_holder claim commit"))?;
        Ok(true)
    }

    /// Let an owner's hold on an endpoint go. A row another owner took since changes nothing.
    pub async fn release(&self, transport: &str, identity: &str, owner: &str) -> Result<()> {
        sqlx::query(
            "DELETE FROM deployment_endpoint_holder \
             WHERE transport = $1 AND identity = $2 AND owner = $3",
        )
        .bind(transport)
        .bind(identity)
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(PersistenceError::db("endpoint_holder release"))?;
        Ok(())
    }
}
