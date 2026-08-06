//! Inbox dedup store, SQL Server dialect (`inbox_seen`) — exactly-once at the
//! application boundary over at-least-once transports.
//!
//! First-observer-wins rides the composite PRIMARY KEY: a plain INSERT whose
//! duplicate-key rejection means "already seen". No read-then-insert race; transactions
//! run XACT_ABORT OFF so the rejection terminates only the INSERT statement.

use time::OffsetDateTime;

use crate::mssql::{is_duplicate_key, to_db, MssqlClient, MssqlPool};
use crate::stores::InboxStore;
use crate::{DeploymentId, PersistenceError, Result};

/// SQL Server implementation of [`InboxStore`].
#[derive(Clone)]
pub struct MssqlInboxStore {
    pool: MssqlPool,
}

const SQL_INSERT: &str = "INSERT INTO inbox_seen (deployment_id, channel, event_id, seen_at) \
     VALUES (@P1, @P2, @P3, @P4)";

const SQL_PRUNE: &str = "DELETE FROM inbox_seen WHERE seen_at < @P1";

impl MssqlInboxStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }

    /// Dedup INSERT on a caller-supplied connection — for callers whose dedup must be
    /// atomic with downstream work. A duplicate-key rejection terminates only the INSERT
    /// statement; the caller's transaction stays usable.
    pub async fn record_seen_in(
        client: &mut MssqlClient,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let seen_at = to_db(OffsetDateTime::now_utc());
        let inserted = client
            .execute(
                SQL_INSERT,
                &[&deployment.as_str(), &channel, &event_id, &seen_at],
            )
            .await;
        match inserted {
            Ok(_) => Ok(true),
            Err(e) if is_duplicate_key(&e) => Ok(false),
            Err(e) => Err(PersistenceError::mssql("inbox recordSeen")(e)),
        }
    }
}

impl InboxStore for MssqlInboxStore {
    async fn record_seen(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        event_id: &str,
    ) -> Result<bool> {
        let mut conn = self.pool.acquire().await?;
        Self::record_seen_in(conn.client(), deployment, channel, event_id).await
    }

    async fn prune_older_than(&self, age: std::time::Duration) -> Result<u64> {
        // Cross-deployment maintenance: deliberately NO deployment bind — the
        // delete must hit every deployment's rows. On this dialect there is no database
        // isolation layer to bypass; the posture is the documented enforced-bind-only one.
        let cutoff = to_db(OffsetDateTime::now_utc() - age);
        let mut conn = self.pool.acquire().await?;
        let pruned = conn
            .client()
            .execute(SQL_PRUNE, &[&cutoff])
            .await
            .map_err(PersistenceError::mssql("inbox pruneOlderThan"))?
            .total();
        Ok(pruned)
    }
}
