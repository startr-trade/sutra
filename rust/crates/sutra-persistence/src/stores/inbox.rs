//! Inbox dedup store (`inbox_seen`, V301) — exactly-once at the application boundary over
//! at-least-once transports.
//!
//! Semantics: `(deployment_id, channel, event_id)` PK +
//! `INSERT ... ON CONFLICT DO NOTHING`; the first observer sees one affected row, every
//! concurrent duplicate sees zero — first-observer-wins with no read-then-insert race.
//! `prune_older_than` is the sanctioned cross-deployment maintenance op: it runs WITHOUT the
//! deployment GUC and relies on an RLS-bypassing role (table owner in dev/test, a documented
//! BYPASSRLS maintenance role in production).

use sqlx::{PgConnection, PgPool};
use time::OffsetDateTime;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// Store trait for inbound-event dedup.
pub trait InboxStore {
    /// Records `(deployment, channel, event_id)` as seen. `true` for the first observer,
    /// `false` for a duplicate.
    async fn record_seen(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool>;
    /// Deletes rows older than `age` across ALL deployments (maintenance bypass posture).
    /// Returns rows pruned.
    async fn prune_older_than(&self, age: std::time::Duration) -> Result<u64>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgInboxStore {
    pool: PgPool,
}

const SQL_INSERT: &str = "INSERT INTO inbox_seen (deployment_id, channel, event_id, seen_at) \
     VALUES ($1, $2, $3, $4) \
     ON CONFLICT (deployment_id, channel, event_id) DO NOTHING";

const SQL_PRUNE: &str = "DELETE FROM inbox_seen WHERE seen_at < $1";

impl PgInboxStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Dedup INSERT on a caller-supplied connection — for callers whose dedup must be atomic
    /// with downstream work.
    pub async fn record_seen_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let inserted = sqlx::query(SQL_INSERT)
            .bind(deployment.as_str())
            .bind(channel)
            .bind(event_id)
            .bind(OffsetDateTime::now_utc())
            .execute(conn)
            .await
            .map_err(PersistenceError::db("inbox recordSeen"))?
            .rows_affected();
        Ok(inserted == 1)
    }
}

impl InboxStore for PgInboxStore {
    async fn record_seen(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let first = Self::record_seen_in(&mut tx, deployment, channel, event_id).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("inbox recordSeen commit"))?;
        Ok(first)
    }

    async fn prune_older_than(&self, age: std::time::Duration) -> Result<u64> {
        // Cross-deployment maintenance: deliberately NO deployment GUC — the delete must hit
        // every deployment's rows. Requires an RLS-bypassing role (owner / BYPASSRLS).
        let cutoff = OffsetDateTime::now_utc() - age;
        let pruned = sqlx::query(SQL_PRUNE)
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("inbox pruneOlderThan"))?
            .rows_affected();
        Ok(pruned)
    }
}
