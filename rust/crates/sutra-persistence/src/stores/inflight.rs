//! The in-flight marker of a synchronous-call chain (`inflight_sync_chain`, V302) and its
//! recovery sweep.
//!
//! A chain keeps its instance in memory until it parks, completes or fails, so the only durable
//! trace a crash mid-chain leaves is what was written before it began: the delivery's inbox
//! dedup rows and, on a capped channel, a RUNNING `channel_instance` row. The marker names them;
//! every final step deletes it in its own transaction ([`PgInflightChainStore::clear_in`]), and
//! the sweep releases what a vanished holder left behind.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use sqlx::{PgConnection, PgPool, Row};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

const SQL_BEGIN: &str = "INSERT INTO inflight_sync_chain \
     (deployment_id, instance_id, dedup_keys, holder, started_at) VALUES ($1, $2, $3, $4, $5) \
     ON CONFLICT (deployment_id, instance_id) DO UPDATE SET \
     dedup_keys = EXCLUDED.dedup_keys, holder = EXCLUDED.holder, started_at = EXCLUDED.started_at";

const SQL_CLEAR: &str =
    "DELETE FROM inflight_sync_chain WHERE deployment_id = $1 AND instance_id = $2";

/// Stranded markers of one deployment: held by a previous process on this host
/// (`holder LIKE '<host>-%'` but not this process), or older than the bound.
const SQL_STRANDED: &str = "SELECT instance_id, dedup_keys FROM inflight_sync_chain \
     WHERE deployment_id = $1 AND ((holder LIKE $2 AND holder <> $3) OR started_at < $4)";

const SQL_FORGET_DEDUP: &str =
    "DELETE FROM inbox_seen WHERE deployment_id = $1 AND channel = $2 AND event_id = $3";

/// The admission row of an instance that never reached a stored row (a chain that died before
/// its first park); a stored instance keeps its row — it is genuinely waiting or running.
const SQL_FORGET_ADMISSION: &str = "DELETE FROM channel_instance \
     WHERE deployment_id = $1 AND instance_id = $2 AND status = 'RUNNING' \
     AND NOT EXISTS (SELECT 1 FROM instance_state s \
                     WHERE s.deployment_id = $1 AND s.instance_id = $2)";

/// The markers THIS process wrote and has not yet cleared. Every final step of every instance
/// calls [`PgInflightChainStore::clear_in`]; only an instance in this set has a marker to clear,
/// so every other step skips the statement entirely. A marker left by a crashed process is the
/// sweep's to release, never a step's.
fn live_markers() -> &'static Mutex<HashSet<(String, Uuid)>> {
    static LIVE: OnceLock<Mutex<HashSet<(String, Uuid)>>> = OnceLock::new();
    LIVE.get_or_init(|| Mutex::new(HashSet::new()))
}

fn marker_key(deployment: &DeploymentId, instance: Uuid) -> (String, Uuid) {
    (deployment.as_str().to_string(), instance)
}

/// PostgreSQL store of the in-flight markers.
#[derive(Debug, Clone)]
pub struct PgInflightChainStore {
    pool: PgPool,
}

impl PgInflightChainStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record that a chain is in flight for `instance`, holding `dedup_keys` (`(channel,
    /// event_id)`), on behalf of `holder` (the engine replica id). Re-recording replaces.
    pub async fn begin(
        &self,
        deployment: &DeploymentId,
        instance: Uuid,
        dedup_keys: &[(String, String)],
        holder: &str,
    ) -> Result<()> {
        let keys = serde_json::to_string(dedup_keys).map_err(|e| {
            PersistenceError::InvalidArgument(format!("in-flight marker keys: {e}"))
        })?;
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_BEGIN)
            .bind(deployment.as_str())
            .bind(instance)
            .bind(keys)
            .bind(holder)
            .bind(OffsetDateTime::now_utc())
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("inflight begin"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("inflight begin commit"))?;
        live_markers()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(marker_key(deployment, instance));
        Ok(())
    }

    /// Delete the marker on a caller's (deployment-scoped) transaction — what every final step
    /// of an instance runs, so the step and the marker's end commit together. A no-op when
    /// there is no marker — and then no statement is issued at all (see [`live_markers`]).
    pub async fn clear_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance: Uuid,
    ) -> Result<()> {
        let key = marker_key(deployment, instance);
        if !live_markers()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&key)
        {
            return Ok(());
        }
        sqlx::query(SQL_CLEAR)
            .bind(deployment.as_str())
            .bind(instance)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("inflight clear"))?;
        // Forgotten before the caller commits: if that commit fails the row survives, and the
        // sweep — not a later step — releases it.
        live_markers()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&key);
        Ok(())
    }

    /// [`Self::clear_in`] in a transaction of its own — for a chain that ends committing
    /// nothing.
    pub async fn clear(&self, deployment: &DeploymentId, instance: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        Self::clear_in(&mut tx, deployment, instance).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("inflight clear commit"))
    }

    /// Release `deployment`'s stranded markers: one held by a previous process on `host` (any
    /// holder starting `<host>-` other than `current_holder`), or older than `max_age`. For
    /// each, in one deployment-scoped transaction: delete its dedup rows, its RUNNING admission
    /// row when the instance has no stored row, and the marker. Returns how many were released.
    pub async fn sweep(
        &self,
        deployment: &DeploymentId,
        host: &str,
        current_holder: &str,
        max_age: std::time::Duration,
    ) -> Result<u64> {
        let cutoff = OffsetDateTime::now_utc() - max_age;
        let host_pattern = format!("{}-%", escape_like(host));
        let rows = {
            let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
            let rows = sqlx::query(SQL_STRANDED)
                .bind(deployment.as_str())
                .bind(host_pattern)
                .bind(current_holder)
                .bind(cutoff)
                .fetch_all(&mut *tx)
                .await
                .map_err(PersistenceError::db("inflight sweep scan"))?;
            tx.commit()
                .await
                .map_err(PersistenceError::db("inflight sweep scan commit"))?;
            rows
        };
        let deployment_id = deployment.as_str();
        let mut released = 0;
        for row in rows {
            let instance: Uuid = row.get("instance_id");
            let keys: String = row.get("dedup_keys");
            // An unreadable key list still releases the marker and the admission row.
            let keys: Vec<(String, String)> = serde_json::from_str(&keys).unwrap_or_default();
            let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
            for (channel, event_id) in &keys {
                sqlx::query(SQL_FORGET_DEDUP)
                    .bind(deployment_id)
                    .bind(channel)
                    .bind(event_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(PersistenceError::db("inflight sweep dedup"))?;
            }
            sqlx::query(SQL_FORGET_ADMISSION)
                .bind(deployment_id)
                .bind(instance)
                .execute(&mut *tx)
                .await
                .map_err(PersistenceError::db("inflight sweep admission"))?;
            sqlx::query(SQL_CLEAR)
                .bind(deployment_id)
                .bind(instance)
                .execute(&mut *tx)
                .await
                .map_err(PersistenceError::db("inflight sweep marker"))?;
            tx.commit()
                .await
                .map_err(PersistenceError::db("inflight sweep commit"))?;
            released += 1;
        }
        Ok(released)
    }
}

/// Escape `LIKE` metacharacters in a host name (`\` is PostgreSQL's default escape).
fn escape_like(raw: &str) -> String {
    raw.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
