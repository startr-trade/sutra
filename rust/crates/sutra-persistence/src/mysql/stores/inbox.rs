//! Inbox dedup store, MySQL/MariaDB dialect (`inbox_seen`) — exactly-once at the
//! application boundary over at-least-once transports.
//!
//! First-observer-wins rides the composite PRIMARY KEY: a plain INSERT whose
//! duplicate-key rejection means "already seen". No read-then-insert race; no
//! error-swallowing insert variant (see the dialect module docs).

use sqlx::{MySqlConnection, MySqlPool};
use time::OffsetDateTime;

use crate::mysql::{is_duplicate_key, to_db};
use crate::stores::InboxStore;
use crate::{DeploymentId, PersistenceError, Result};

/// MySQL/MariaDB implementation of [`InboxStore`].
#[derive(Debug, Clone)]
pub struct MySqlInboxStore {
    pool: MySqlPool,
}

const SQL_INSERT: &str = "INSERT INTO inbox_seen (deployment_id, channel, event_id, seen_at) \
     VALUES (?, ?, ?, ?)";

const SQL_PRUNE: &str = "DELETE FROM inbox_seen WHERE seen_at < ?";

impl MySqlInboxStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Dedup INSERT on a caller-supplied connection — for callers whose dedup must be
    /// atomic with downstream work. A duplicate-key rejection terminates only the INSERT
    /// statement; the caller's transaction stays usable.
    pub async fn record_seen_in(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let inserted = sqlx::query(SQL_INSERT)
            .bind(deployment.as_str())
            .bind(channel)
            .bind(event_id)
            .bind(to_db(OffsetDateTime::now_utc()))
            .execute(conn)
            .await;
        match inserted {
            Ok(_) => Ok(true),
            Err(e) if is_duplicate_key(&e) => Ok(false),
            Err(e) => Err(PersistenceError::db("inbox recordSeen")(e)),
        }
    }
}

impl InboxStore for MySqlInboxStore {
    async fn record_seen(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(PersistenceError::db("inbox recordSeen acquire"))?;
        Self::record_seen_in(&mut conn, deployment, channel, event_id).await
    }

    async fn prune_older_than(&self, age: std::time::Duration) -> Result<u64> {
        // Cross-deployment maintenance: deliberately NO deployment bind — the
        // delete must hit every deployment's rows. On this dialect there is no database
        // isolation layer to bypass; the posture is the documented enforced-bind-only one.
        let cutoff = OffsetDateTime::now_utc() - age;
        let pruned = sqlx::query(SQL_PRUNE)
            .bind(to_db(cutoff))
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("inbox pruneOlderThan"))?
            .rows_affected();
        Ok(pruned)
    }
}
