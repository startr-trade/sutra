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
use uuid::Uuid;

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
    /// Deletes the `(deployment, channel, event_id)` row, so a redelivery of that event is a
    /// first observation again. Returns whether a row was removed. Used when the work the row
    /// guarded failed without persisting anything.
    async fn forget(
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

const SQL_INSERT: &str = "INSERT INTO inbox_seen \
     (deployment_id, channel, event_id, seen_at, instance_id) \
     VALUES ($1, $2, $3, $4, $5) \
     ON CONFLICT (deployment_id, channel, event_id) DO NOTHING";

const SQL_PRUNE: &str = "DELETE FROM inbox_seen WHERE seen_at < $1";

const SQL_FORGET: &str =
    "DELETE FROM inbox_seen WHERE deployment_id = $1 AND channel = $2 AND event_id = $3";

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
        Self::record_seen_for_in(conn, deployment, channel, event_id, None).await
    }

    /// [`Self::record_seen_in`], keeping the instance the delivery started or resumed so a later
    /// duplicate can name it.
    pub async fn record_seen_for_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
        instance_id: Option<Uuid>,
    ) -> Result<bool> {
        let inserted = sqlx::query(SQL_INSERT)
            .bind(deployment.as_str())
            .bind(channel)
            .bind(event_id)
            .bind(OffsetDateTime::now_utc())
            .bind(instance_id)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("inbox recordSeen"))?
            .rows_affected();
        Ok(inserted == 1)
    }
}

/// What the store holds for one `(deployment, channel, event_id)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxSeenState {
    /// No record.
    Absent,
    /// A record, and the delivery that wrote it finished.
    Recorded,
    /// A record that an in-flight chain's marker still names: that chain is running, or it failed
    /// and could not release the record. The sweep resolves a marker nobody ends.
    InDoubt,
}

const SQL_HELD_BY_CHAIN: &str = "SELECT 1 FROM inflight_sync_chain \
     WHERE deployment_id = $1 AND dedup_keys::jsonb @> $2::jsonb LIMIT 1";

impl PgInboxStore {
    /// [`Self::seen`], and when a record exists, whether a chain's marker still holds it. The marker
    /// is only read for a key that has a record, so a first delivery pays for one query.
    pub async fn check(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<InboxSeenState> {
        if !self.seen(deployment, channel, event_id).await? {
            return Ok(InboxSeenState::Absent);
        }
        let key = serde_json::to_string(&[[channel, event_id]])
            .map_err(|e| PersistenceError::InvalidArgument(format!("inbox check key: {e}")))?;
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let held: Option<i32> = sqlx::query_scalar(SQL_HELD_BY_CHAIN)
            .bind(deployment.as_str())
            .bind(key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("inbox check marker"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("inbox check marker commit"))?;
        Ok(if held.is_some() {
            InboxSeenState::InDoubt
        } else {
            InboxSeenState::Recorded
        })
    }

    /// Whether `(deployment, channel, event_id)` is already recorded — a read, no write. The
    /// engine checks before it processes a delivery and writes the record in the step that
    /// processes it (`crate::step::record_inbox_seen_in`), so record and effects commit together.
    pub async fn seen(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let found: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM inbox_seen WHERE deployment_id = $1 AND channel = $2 AND event_id = $3",
        )
        .bind(deployment.as_str())
        .bind(channel)
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(PersistenceError::db("inbox seen"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("inbox seen commit"))?;
        Ok(found.is_some())
    }

    /// The instance kept with a recorded delivery. `None` for no record, and for a record that
    /// names none: its delivery ended without an instance, or it predates the column.
    pub async fn original_instance(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<Option<Uuid>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let found: Option<Option<Uuid>> = sqlx::query_scalar(
            "SELECT instance_id FROM inbox_seen \
             WHERE deployment_id = $1 AND channel = $2 AND event_id = $3",
        )
        .bind(deployment.as_str())
        .bind(channel)
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(PersistenceError::db("inbox original instance"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("inbox original instance commit"))?;
        Ok(found.flatten())
    }

    /// Record `(deployment, channel, event_id)` with its instance, in a transaction of its own.
    pub async fn record_seen_for(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
        instance_id: Option<Uuid>,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let first =
            Self::record_seen_for_in(&mut tx, deployment, channel, event_id, instance_id).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("inbox recordSeen commit"))?;
        Ok(first)
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

    async fn forget(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let removed = sqlx::query(SQL_FORGET)
            .bind(deployment.as_str())
            .bind(channel)
            .bind(event_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("inbox forget"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("inbox forget commit"))?;
        Ok(removed == 1)
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
