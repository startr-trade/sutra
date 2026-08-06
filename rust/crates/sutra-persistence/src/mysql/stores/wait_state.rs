//! Wait-state store, MySQL/MariaDB dialect (`waiting_event`, plus the timer addendum).
//!
//! Same surface as the reference implementation: a queryable projection BESIDE
//! `instance_state`, upsert-on-record with reset-to-WAITING semantics, RESOLVED rows
//! retained for audit, and the TIMER rows claimed via `FOR UPDATE SKIP LOCKED`.

use sqlx::{MySqlConnection, MySqlPool, QueryBuilder, Row};
use time::{OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::mysql::scope::begin_tx;
use crate::mysql::{from_db, to_db};
use crate::stores::{DueTimer, WaitStateStore, WaitingEvent, WaitingFilter, STATUS_WAITING};
use crate::{DeploymentId, PersistenceError, Result};

/// MySQL/MariaDB implementation of [`WaitStateStore`].
#[derive(Debug, Clone)]
pub struct MySqlWaitStateStore {
    pool: MySqlPool,
}

const SQL_RECORD: &str = "INSERT INTO waiting_event \
     (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at) \
     VALUES (?, ?, ?, ?, ?, 'WAITING', CURRENT_TIMESTAMP(6)) \
     ON DUPLICATE KEY UPDATE \
       process_id = VALUES(process_id), correlation_key = VALUES(correlation_key), \
       status = 'WAITING', resolved_at = NULL";

/// Fresh-incarnation upsert (PostgreSQL reference: `SQL_RECORD_FRESH`) — additionally resets
/// the V803 timer columns, for a node the same step both resolved and re-parks (a channel-call
/// `<q:retry>` re-drive must not carry the dead backoff's TIMER kind/due-at onto the fresh
/// MESSAGE wait).
const SQL_RECORD_FRESH: &str = "INSERT INTO waiting_event \
     (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at) \
     VALUES (?, ?, ?, ?, ?, 'WAITING', CURRENT_TIMESTAMP(6)) \
     ON DUPLICATE KEY UPDATE \
       process_id = VALUES(process_id), correlation_key = VALUES(correlation_key), \
       status = 'WAITING', resolved_at = NULL, kind = 'MESSAGE', timer_due_at = NULL";

const SQL_RESOLVE_NODE: &str = "UPDATE waiting_event \
     SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP(6) \
     WHERE deployment_id = ? AND instance_id = ? AND node_id = ? AND status = 'WAITING'";

const SQL_RESOLVE_ALL: &str = "UPDATE waiting_event \
     SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP(6) \
     WHERE deployment_id = ? AND instance_id = ? AND status = 'WAITING'";

const SQL_RECORD_TIMER: &str = "INSERT INTO waiting_event \
     (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at, \
      kind, timer_due_at) \
     VALUES (?, ?, ?, ?, NULL, 'WAITING', CURRENT_TIMESTAMP(6), 'TIMER', ?) \
     ON DUPLICATE KEY UPDATE \
       process_id = VALUES(process_id), status = 'WAITING', resolved_at = NULL, \
       kind = 'TIMER', timer_due_at = VALUES(timer_due_at)";

const SQL_CLAIM_DUE_TIMERS: &str = "SELECT instance_id, process_id, node_id, timer_due_at \
     FROM waiting_event \
     WHERE deployment_id = ? AND kind = 'TIMER' AND status = 'WAITING' \
       AND timer_due_at <= ? \
     ORDER BY timer_due_at \
     LIMIT ? \
     FOR UPDATE SKIP LOCKED";

const SQL_DEFER_TIMER: &str = "UPDATE waiting_event SET timer_due_at = ? \
     WHERE deployment_id = ? AND instance_id = ? AND node_id = ? \
       AND kind = 'TIMER' AND status = 'WAITING'";

impl MySqlWaitStateStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    /// Upsert on a caller-supplied connection (the transactional-step building block).
    pub async fn record_waiting_in(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()> {
        sqlx::query(SQL_RECORD)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .bind(process_id)
            .bind(correlation_key)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("waiting_event recordWaiting"))?;
        Ok(())
    }

    /// [`Self::record_waiting_in`] for a node's FRESH incarnation ([`SQL_RECORD_FRESH`]).
    pub async fn record_waiting_fresh_in(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()> {
        sqlx::query(SQL_RECORD_FRESH)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .bind(process_id)
            .bind(correlation_key)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("waiting_event recordWaitingFresh"))?;
        Ok(())
    }

    // ---- timer wait rows -------------------------------------------------------------------

    /// Upsert a TIMER wait row due at `due_at` on a caller-supplied connection (the
    /// park-step building block). Re-recording resets it to WAITING with the new due-at.
    pub async fn record_timer_waiting_in(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        due_at: OffsetDateTime,
    ) -> Result<()> {
        sqlx::query(SQL_RECORD_TIMER)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .bind(process_id)
            .bind(to_db(due_at))
            .execute(conn)
            .await
            .map_err(PersistenceError::db("waiting_event recordTimerWaiting"))?;
        Ok(())
    }

    /// Claim up to `max_entries` DUE timer rows via `FOR UPDATE SKIP LOCKED` and commit
    /// (the outbox `next_attempt_at` pattern — concurrent pollers never compete for the
    /// same rows). Firing is at-least-once: the resume step resolves the row.
    pub async fn claim_due_timers(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<DueTimer>> {
        let mut tx = begin_tx(&self.pool).await?;
        let claimed = Self::claim_due_timers_in(&mut tx, deployment, now, max_entries).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event claimDueTimers commit"))?;
        Ok(claimed)
    }

    /// SKIP LOCKED claim on a caller-supplied transaction connection (rows stay locked
    /// until the caller's transaction ends).
    pub async fn claim_due_timers_in(
        conn: &mut MySqlConnection,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<DueTimer>> {
        if max_entries <= 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_CLAIM_DUE_TIMERS)
            .bind(deployment.as_str())
            .bind(to_db(now))
            .bind(max_entries)
            .fetch_all(conn)
            .await
            .map_err(PersistenceError::db("waiting_event claimDueTimers"))?;
        fn e(source: sqlx::Error) -> PersistenceError {
            PersistenceError::Database {
                operation: "waiting_event read timer row",
                source,
            }
        }
        rows.iter()
            .map(|row| {
                let due_at: PrimitiveDateTime = row.try_get("timer_due_at").map_err(e)?;
                Ok(DueTimer {
                    instance_id: row.try_get("instance_id").map_err(e)?,
                    process_id: crate::mysql::str_col(row, "process_id")?,
                    node_id: crate::mysql::str_col(row, "node_id")?,
                    due_at: from_db(due_at),
                })
            })
            .collect()
    }

    /// Push a TIMER row's due-at forward (the poller's failure-path backoff). A
    /// resolved/missing row is a silent no-op.
    pub async fn defer_timer(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
        new_due_at: OffsetDateTime,
    ) -> Result<()> {
        sqlx::query(SQL_DEFER_TIMER)
            .bind(to_db(new_due_at))
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("waiting_event deferTimer"))?;
        Ok(())
    }
}

impl WaitStateStore for MySqlWaitStateStore {
    async fn record_waiting(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(PersistenceError::db("waiting_event recordWaiting acquire"))?;
        Self::record_waiting_in(
            &mut conn,
            deployment,
            instance_id,
            process_id,
            node_id,
            correlation_key,
        )
        .await
    }

    async fn resolve(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
    ) -> Result<()> {
        sqlx::query(SQL_RESOLVE_NODE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("waiting_event resolve"))?;
        Ok(())
    }

    async fn resolve_all(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        sqlx::query(SQL_RESOLVE_ALL)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("waiting_event resolveAll"))?;
        Ok(())
    }

    async fn list_waiting(
        &self,
        deployment: &DeploymentId,
        filter: &WaitingFilter,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WaitingEvent>> {
        let status = filter.status.as_deref().unwrap_or(STATUS_WAITING);
        let mut qb: QueryBuilder<sqlx::MySql> = QueryBuilder::new(
            "SELECT instance_id, process_id, node_id, correlation_key, status, created_at, \
             resolved_at FROM waiting_event WHERE deployment_id = ",
        );
        qb.push_bind(deployment.as_str());
        qb.push(" AND status = ");
        qb.push_bind(status);
        if let Some(process_id) = &filter.process_id {
            qb.push(" AND process_id = ");
            qb.push_bind(process_id);
        }
        if let Some(correlation_key) = &filter.correlation_key {
            qb.push(" AND correlation_key = ");
            qb.push_bind(correlation_key);
        }
        qb.push(" ORDER BY created_at DESC, node_id ASC LIMIT ");
        qb.push_bind(limit.max(0));
        qb.push(" OFFSET ");
        qb.push_bind(offset.max(0));

        let rows = qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::db("waiting_event listWaiting"))?;

        fn e(source: sqlx::Error) -> PersistenceError {
            PersistenceError::Database {
                operation: "waiting_event read row",
                source,
            }
        }
        rows.iter()
            .map(|row| {
                let created_at: PrimitiveDateTime = row.try_get("created_at").map_err(e)?;
                let resolved_at: Option<PrimitiveDateTime> =
                    row.try_get("resolved_at").map_err(e)?;
                Ok(WaitingEvent {
                    instance_id: row.try_get("instance_id").map_err(e)?,
                    process_id: crate::mysql::str_col(row, "process_id")?,
                    node_id: crate::mysql::str_col(row, "node_id")?,
                    correlation_key: crate::mysql::opt_str_col(row, "correlation_key")?,
                    status: crate::mysql::str_col(row, "status")?,
                    created_at: from_db(created_at),
                    resolved_at: resolved_at.map(from_db),
                })
            })
            .collect()
    }
}
