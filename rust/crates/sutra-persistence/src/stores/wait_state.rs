//! Wait-state store (`waiting_event`, V801–V802) — the queryable projection of wait points.
//!
//! Semantics: one row per token parked at a wait node, keyed by
//! `(deployment_id, instance_id, node_id)`. A projection BESIDE `instance_state` (the resume
//! authority is `alias_index.find_live` + the snapshot) — the admin `listWaiting`
//! API reads it, a relay reconciles against it. `correlation_key` stays reserved-but-nullable.

use sqlx::{PgConnection, PgPool, QueryBuilder, Row};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// Row status: parked and awaiting a relay decision.
pub const STATUS_WAITING: &str = "WAITING";
/// Row status: satisfied (kept with `resolved_at` for audit).
pub const STATUS_RESOLVED: &str = "RESOLVED";

/// Row kind: a message/relay wait (the default — every row predating the timer addendum).
pub const KIND_MESSAGE: &str = "MESSAGE";
/// Row kind: a timer wait (the TIMER marker; `timer_due_at` set).
pub const KIND_TIMER: &str = "TIMER";

/// One waiting-event row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingEvent {
    /// Parked instance.
    pub instance_id: Uuid,
    /// Archive-local process id.
    pub process_id: String,
    /// The wait node the token is parked at.
    pub node_id: String,
    /// Reserved (written NULL today).
    pub correlation_key: Option<String>,
    /// `WAITING` or `RESOLVED`.
    pub status: String,
    /// When the wait was recorded.
    pub created_at: OffsetDateTime,
    /// When the wait was resolved, if it was.
    pub resolved_at: Option<OffsetDateTime>,
}

/// Listing filter; `None` fields don't constrain.
#[derive(Debug, Clone, Default)]
pub struct WaitingFilter {
    /// Row status; defaults to `WAITING` when `None`.
    pub status: Option<String>,
    /// Narrow to one process id.
    pub process_id: Option<String>,
    /// Narrow to one correlation key (display/filtering only).
    pub correlation_key: Option<String>,
}

/// Store trait for the durable wait-point registry.
pub trait WaitStateStore {
    /// UPSERT a WAITING row; re-recording the same wait point resets it to WAITING and
    /// clears any prior resolved stamp rather than duplicating.
    async fn record_waiting(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()>;
    /// Marks one wait point RESOLVED; already-resolved / never-recorded is a silent no-op.
    async fn resolve(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
    ) -> Result<()>;
    /// Marks every live wait point of the instance RESOLVED (terminal transition).
    async fn resolve_all(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()>;
    /// Lists wait points for the deployment, newest first, paged.
    async fn list_waiting(
        &self,
        deployment: &DeploymentId,
        filter: &WaitingFilter,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WaitingEvent>>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgWaitStateStore {
    pool: PgPool,
}

const SQL_RECORD: &str = "INSERT INTO waiting_event \
     (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at) \
     VALUES ($1, $2, $3, $4, $5, 'WAITING', CURRENT_TIMESTAMP) \
     ON CONFLICT (deployment_id, instance_id, node_id) DO UPDATE \
     SET process_id = EXCLUDED.process_id, correlation_key = EXCLUDED.correlation_key, \
         status = 'WAITING', resolved_at = NULL";

/// [`SQL_RECORD`] that additionally RESETS the V803 timer columns to the MESSAGE default.
/// The plain upsert deliberately leaves `kind`/`timer_due_at` alone — an unrelated re-park's
/// MESSAGE write must not clobber a still-pending timer catch node's due-at. But a node this
/// same step both RESOLVED and re-parks is a NEW INCARNATION of the wait (a channel-call
/// `<q:retry>` re-drive: the resolved row was the backoff TIMER, the fresh park is the
/// response MESSAGE wait), and carrying the dead incarnation's TIMER kind forward would leave
/// an already-elapsed due-at on a message wait — claimed forever by the poller as a phantom
/// fire.
const SQL_RECORD_FRESH: &str = "INSERT INTO waiting_event \
     (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at) \
     VALUES ($1, $2, $3, $4, $5, 'WAITING', CURRENT_TIMESTAMP) \
     ON CONFLICT (deployment_id, instance_id, node_id) DO UPDATE \
     SET process_id = EXCLUDED.process_id, correlation_key = EXCLUDED.correlation_key, \
         status = 'WAITING', resolved_at = NULL, kind = 'MESSAGE', timer_due_at = NULL";

const SQL_RESOLVE_NODE: &str = "UPDATE waiting_event \
     SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP \
     WHERE deployment_id = $1 AND instance_id = $2 AND node_id = $3 AND status = 'WAITING'";

const SQL_RESOLVE_ALL: &str = "UPDATE waiting_event \
     SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP \
     WHERE deployment_id = $1 AND instance_id = $2 AND status = 'WAITING'";

impl PgWaitStateStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// UPSERT on a caller-supplied connection (the transactional-step building block).
    pub async fn record_waiting_in(
        conn: &mut PgConnection,
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

    /// [`Self::record_waiting_in`] for a node's FRESH incarnation ([`SQL_RECORD_FRESH`]):
    /// used by the step primitive for a node it both resolved and re-parks in one step, so
    /// the dead incarnation's TIMER kind/due-at cannot leak onto the new MESSAGE wait.
    pub async fn record_waiting_fresh_in(
        conn: &mut PgConnection,
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

    // ---- timer wait rows (the V803 addendum) ------------------------

    /// UPSERT a TIMER wait row due at `due_at` on a caller-supplied connection (the
    /// park-step building block). Re-recording resets it to WAITING with the new due-at.
    pub async fn record_timer_waiting_in(
        conn: &mut PgConnection,
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
            .bind(due_at)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("waiting_event recordTimerWaiting"))?;
        Ok(())
    }

    /// Claim up to `max_entries` DUE timer rows via `FOR UPDATE SKIP LOCKED` (the outbox
    /// `next_attempt_at` pattern — concurrent pollers never compete for the same rows) and
    /// commit. Firing is at-least-once: the resume step resolves the row; a claim that
    /// loses the race finds the row RESOLVED next tick and no-ops.
    pub async fn claim_due_timers(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<DueTimer>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let claimed = Self::claim_due_timers_in(&mut tx, deployment, now, max_entries).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event claimDueTimers commit"))?;
        Ok(claimed)
    }

    /// SKIP LOCKED claim on a caller-supplied transaction connection (rows stay locked
    /// until the caller's transaction ends).
    pub async fn claim_due_timers_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<DueTimer>> {
        if max_entries <= 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_CLAIM_DUE_TIMERS)
            .bind(deployment.as_str())
            .bind(now)
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
                Ok(DueTimer {
                    instance_id: row.try_get("instance_id").map_err(e)?,
                    process_id: row.try_get("process_id").map_err(e)?,
                    node_id: row.try_get("node_id").map_err(e)?,
                    due_at: row.try_get("timer_due_at").map_err(e)?,
                })
            })
            .collect()
    }

    /// Every wait row of ONE instance, WAITING and RESOLVED alike, with the row's `kind` and
    /// `timer_due_at` — the admin instance-migration read.
    ///
    /// Distinct from [`WaitStateStore::list_waiting`], which is the deployment-wide operator
    /// listing and deliberately projects neither column: migration has to tell a MESSAGE park from
    /// a TIMER park (they resume through different entry points and therefore validate against
    /// different target constructs), and it has to carry a timer's due-at across the move
    /// unchanged so a park that had another hour left still has another hour left.
    pub async fn list_for_instance(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Vec<InstanceWait>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows = sqlx::query(SQL_LIST_FOR_INSTANCE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("waiting_event listForInstance"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event listForInstance commit"))?;
        fn e(source: sqlx::Error) -> PersistenceError {
            PersistenceError::Database {
                operation: "waiting_event read instance row",
                source,
            }
        }
        rows.iter()
            .map(|row| {
                Ok(InstanceWait {
                    node_id: row.try_get("node_id").map_err(e)?,
                    process_id: row.try_get("process_id").map_err(e)?,
                    kind: row.try_get("kind").map_err(e)?,
                    status: row.try_get("status").map_err(e)?,
                    timer_due_at: row.try_get("timer_due_at").map_err(e)?,
                    resolved_at: row.try_get("resolved_at").map_err(e)?,
                })
            })
            .collect()
    }

    /// Push a TIMER row's due-at forward (the poller's failure-path backoff — the outbox
    /// `defer` analog). A resolved/missing row is a silent no-op.
    pub async fn defer_timer(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
        new_due_at: OffsetDateTime,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DEFER_TIMER)
            .bind(new_due_at)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("waiting_event deferTimer"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event deferTimer commit"))
    }
}

/// One claimed due TIMER row — what the poller maps to a `TimerFire`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueTimer {
    pub instance_id: Uuid,
    pub process_id: String,
    /// The waiting timer node (timer catch event / timer boundary / `#timeout` synthetic).
    pub node_id: String,
    pub due_at: OffsetDateTime,
}

/// One instance's wait row as the migration validator reads it — the projection
/// [`WaitStateStore::list_waiting`] does not carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceWait {
    /// The node the token is (or was) parked at.
    pub node_id: String,
    /// Archive-local process id.
    pub process_id: String,
    /// [`KIND_MESSAGE`] or [`KIND_TIMER`] — which resume path owns this park.
    pub kind: String,
    /// [`STATUS_WAITING`] or [`STATUS_RESOLVED`].
    pub status: String,
    /// When a TIMER park becomes claimable; `None` for a message park.
    pub timer_due_at: Option<OffsetDateTime>,
    /// When the park was satisfied (or torn down by the failure commit); `None` while WAITING.
    ///
    /// Carried for the migrate-then-resume convenience: the failure commit resolves an instance's
    /// live parks in ONE statement, so those rows share a single `resolved_at` — the instance's
    /// latest — which is what tells a resume which parks to re-arm and which are genuinely spent
    /// history. See [`crate::step::commit_instance_migration`]'s `rearm_parks`.
    pub resolved_at: Option<OffsetDateTime>,
}

const SQL_LIST_FOR_INSTANCE: &str =
    "SELECT node_id, process_id, kind, status, timer_due_at, resolved_at FROM waiting_event \
     WHERE deployment_id = $1 AND instance_id = $2 ORDER BY node_id";

const SQL_RECORD_TIMER: &str = "INSERT INTO waiting_event \
     (deployment_id, instance_id, node_id, process_id, correlation_key, status, created_at, \
      kind, timer_due_at) \
     VALUES ($1, $2, $3, $4, NULL, 'WAITING', CURRENT_TIMESTAMP, 'TIMER', $5) \
     ON CONFLICT (deployment_id, instance_id, node_id) DO UPDATE \
     SET process_id = EXCLUDED.process_id, status = 'WAITING', resolved_at = NULL, \
         kind = 'TIMER', timer_due_at = EXCLUDED.timer_due_at";

const SQL_CLAIM_DUE_TIMERS: &str = "SELECT instance_id, process_id, node_id, timer_due_at \
     FROM waiting_event \
     WHERE deployment_id = $1 AND kind = 'TIMER' AND status = 'WAITING' \
       AND timer_due_at <= $2 \
     ORDER BY timer_due_at \
     LIMIT $3 \
     FOR UPDATE SKIP LOCKED";

const SQL_DEFER_TIMER: &str = "UPDATE waiting_event SET timer_due_at = $1 \
     WHERE deployment_id = $2 AND instance_id = $3 AND node_id = $4 \
       AND kind = 'TIMER' AND status = 'WAITING'";

impl WaitStateStore for PgWaitStateStore {
    async fn record_waiting(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        process_id: &str,
        node_id: &str,
        correlation_key: Option<&str>,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        Self::record_waiting_in(
            &mut tx,
            deployment,
            instance_id,
            process_id,
            node_id,
            correlation_key,
        )
        .await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event recordWaiting commit"))
    }

    async fn resolve(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_RESOLVE_NODE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("waiting_event resolve"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event resolve commit"))
    }

    async fn resolve_all(&self, deployment: &DeploymentId, instance_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_RESOLVE_ALL)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("waiting_event resolveAll"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event resolveAll commit"))
    }

    async fn list_waiting(
        &self,
        deployment: &DeploymentId,
        filter: &WaitingFilter,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WaitingEvent>> {
        let status = filter.status.as_deref().unwrap_or(STATUS_WAITING);
        let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
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

        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows = qb
            .build()
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("waiting_event listWaiting"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("waiting_event listWaiting commit"))?;

        fn e(source: sqlx::Error) -> PersistenceError {
            PersistenceError::Database {
                operation: "waiting_event read row",
                source,
            }
        }
        rows.iter()
            .map(|row| {
                Ok(WaitingEvent {
                    instance_id: row.try_get("instance_id").map_err(e)?,
                    process_id: row.try_get("process_id").map_err(e)?,
                    node_id: row.try_get("node_id").map_err(e)?,
                    correlation_key: row.try_get("correlation_key").map_err(e)?,
                    status: row.try_get("status").map_err(e)?,
                    created_at: row.try_get("created_at").map_err(e)?,
                    resolved_at: row.try_get("resolved_at").map_err(e)?,
                })
            })
            .collect()
    }
}
