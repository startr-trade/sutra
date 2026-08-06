//! Timer-start schedule store (`timer_schedule`, V804) — the durable, deployment-scoped
//! registry of "this deployment mints an instance of process P at start node N, on this
//! schedule".
//!
//! Sibling of [`crate::stores::wait_state`], deliberately NOT the same table: a `waiting_event`
//! row belongs to an INSTANCE (its `instance_id` is a NOT NULL primary-key column), and a start
//! schedule has no instance — there is nothing to start until it fires. See the V804 header for
//! the full argument.
//!
//! Ownership: the DEPLOYMENT-ACTIVATION flip writes these rows, not the executor.
//! [`PgTimerScheduleStore::arm`] upserts one row per timer start of every ACTIVE deployment, and
//! [`PgTimerScheduleStore::resolve_deployment`] retires every row of a deployment that stopped
//! being active (flipped away, drained, undeployed, or replaced in its slot by a hot-deploy).
//! Schedules follow the ACTIVE deployment and never the DRAINING tail — a drained deployment
//! must stop MINTING work even while its already-parked instances keep resuming.
//!
//! Claiming mirrors the timer poller's `waiting_event` protocol exactly (`FOR UPDATE SKIP
//! LOCKED`, the outbox `next_attempt_at` pattern), so one leader-gated loop drives both and two
//! replicas never fire the same occurrence.

use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// Row status: armed and awaiting its next occurrence.
pub const SCHEDULE_STATUS_SCHEDULED: &str = "SCHEDULED";
/// Row status: retired — the deployment stopped being ACTIVE, or the repeat budget is spent.
/// Kept with a `resolved_at` stamp (audit), never deleted.
pub const SCHEDULE_STATUS_RESOLVED: &str = "RESOLVED";

/// One timer start to arm — what deployment activation knows about a schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerScheduleArming {
    /// Archive-local process id whose start event this is.
    pub process_id: String,
    /// The `<startEvent>` id the schedule fires.
    pub node_id: String,
    /// The tenant the synthesized start dispatch binds (tenancy + quota).
    pub tenant: String,
    /// `"<tenant>/<module>/<version>"` — the namespace key of the deployment.
    pub module_key: String,
    /// `DURATION` / `DATE` / `CYCLE` — how to read [`Self::spec`].
    pub kind: String,
    /// The authored timer text, verbatim.
    pub spec: String,
    /// The first occurrence, already computed from the arming instant.
    pub next_due_at: OffsetDateTime,
    /// Fires left INCLUDING the next one; `None` = unbounded.
    pub remaining_fires: Option<i32>,
}

/// One claimed due schedule row — what the poller turns into a synthesized start dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueTimerSchedule {
    pub process_id: String,
    pub node_id: String,
    pub tenant: String,
    pub module_key: String,
    pub kind: String,
    pub spec: String,
    pub next_due_at: OffsetDateTime,
    pub remaining_fires: Option<i32>,
}

/// A schedule row as stored — the read projection (admin listing, tests).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerScheduleRow {
    pub process_id: String,
    pub node_id: String,
    pub tenant: String,
    pub module_key: String,
    pub kind: String,
    pub spec: String,
    pub next_due_at: OffsetDateTime,
    pub remaining_fires: Option<i32>,
    pub status: String,
    pub resolved_at: Option<OffsetDateTime>,
}

/// PostgreSQL implementation of the timer-start schedule registry.
#[derive(Debug, Clone)]
pub struct PgTimerScheduleStore {
    pool: PgPool,
}

// Re-arming semantics, encoded in the conflict clause and worth reading carefully:
//
//   * A deployment id is a CONTENT HASH, so for a given (deployment, process, node) the kind and
//     spec can never change. Re-activating a deployment that is still armed must therefore NOT
//     disturb `next_due_at` / `remaining_fires` — an unrelated deployment's flip would otherwise
//     silently restart every other deployment's schedule on every tick.
//   * Re-activating a deployment whose rows were RESOLVED (it drained, then a rollback brought it
//     back, or its budget ran out and it is being redeployed) DOES re-arm from scratch: the row
//     goes back to SCHEDULED with the freshly-computed first occurrence.
const SQL_ARM: &str = "INSERT INTO timer_schedule \
     (deployment_id, process_id, node_id, tenant, module_key, kind, spec, next_due_at, \
      remaining_fires, status, created_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'SCHEDULED', CURRENT_TIMESTAMP) \
     ON CONFLICT (deployment_id, process_id, node_id) DO UPDATE \
     SET tenant = EXCLUDED.tenant, module_key = EXCLUDED.module_key, \
         kind = EXCLUDED.kind, spec = EXCLUDED.spec, \
         next_due_at = CASE WHEN timer_schedule.status = 'RESOLVED' \
                            THEN EXCLUDED.next_due_at ELSE timer_schedule.next_due_at END, \
         remaining_fires = CASE WHEN timer_schedule.status = 'RESOLVED' \
                                THEN EXCLUDED.remaining_fires \
                                ELSE timer_schedule.remaining_fires END, \
         status = 'SCHEDULED', resolved_at = NULL";

const SQL_CLAIM_DUE: &str = "SELECT process_id, node_id, tenant, module_key, kind, spec, \
     next_due_at, remaining_fires \
     FROM timer_schedule \
     WHERE deployment_id = $1 AND status = 'SCHEDULED' AND next_due_at <= $2 \
     ORDER BY next_due_at \
     LIMIT $3 \
     FOR UPDATE SKIP LOCKED";

const SQL_ADVANCE: &str = "UPDATE timer_schedule \
     SET next_due_at = $4, remaining_fires = $5 \
     WHERE deployment_id = $1 AND process_id = $2 AND node_id = $3 AND status = 'SCHEDULED'";

const SQL_RESOLVE_NODE: &str = "UPDATE timer_schedule \
     SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP \
     WHERE deployment_id = $1 AND process_id = $2 AND node_id = $3 AND status = 'SCHEDULED'";

const SQL_RESOLVE_DEPLOYMENT: &str = "UPDATE timer_schedule \
     SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP \
     WHERE deployment_id = $1 AND status = 'SCHEDULED'";

const SQL_LIST: &str = "SELECT process_id, node_id, tenant, module_key, kind, spec, next_due_at, \
     remaining_fires, status, resolved_at \
     FROM timer_schedule WHERE deployment_id = $1 \
     ORDER BY process_id, node_id";

impl PgTimerScheduleStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Arm (or re-arm) every schedule of one ACTIVE deployment in a single transaction, and
    /// RESOLVE any row of that deployment the arming set no longer names — a deployment id is a
    /// content hash so this last case is vestigial in practice, but it keeps the table honest if
    /// an id is ever reused.
    ///
    /// Idempotent by construction: an activation flip runs on every deployment change, so this
    /// is called far more often than schedules actually change.
    pub async fn arm(
        &self,
        deployment: &DeploymentId,
        schedules: &[TimerScheduleArming],
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        for s in schedules {
            sqlx::query(SQL_ARM)
                .bind(deployment.as_str())
                .bind(&s.process_id)
                .bind(&s.node_id)
                .bind(&s.tenant)
                .bind(&s.module_key)
                .bind(&s.kind)
                .bind(&s.spec)
                .bind(s.next_due_at)
                .bind(s.remaining_fires)
                .execute(&mut *tx)
                .await
                .map_err(PersistenceError::db("timer_schedule arm"))?;
        }
        // Anything armed for this deployment that the plan no longer declares is retired in the
        // same transaction, so "armed set" always equals "what the ACTIVE plan says".
        let mut keep = String::from(
            "UPDATE timer_schedule SET status = 'RESOLVED', resolved_at = CURRENT_TIMESTAMP \
             WHERE deployment_id = $1 AND status = 'SCHEDULED'",
        );
        for i in 0..schedules.len() {
            keep.push_str(&format!(
                " AND NOT (process_id = ${} AND node_id = ${})",
                2 + i * 2,
                3 + i * 2
            ));
        }
        let mut prune = sqlx::query(&keep).bind(deployment.as_str());
        for s in schedules {
            prune = prune.bind(&s.process_id).bind(&s.node_id);
        }
        prune
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("timer_schedule prune"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("timer_schedule arm commit"))
    }

    /// RESOLVE every armed schedule of a deployment — the flip-away / retire / undeploy /
    /// hot-deploy-handoff step. Returns how many rows stopped being armed.
    pub async fn resolve_deployment(&self, deployment: &DeploymentId) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let done = sqlx::query(SQL_RESOLVE_DEPLOYMENT)
            .bind(deployment.as_str())
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("timer_schedule resolveDeployment"))?
            .rows_affected();
        tx.commit().await.map_err(PersistenceError::db(
            "timer_schedule resolveDeployment commit",
        ))?;
        Ok(done)
    }

    /// Claim up to `max_entries` DUE schedule rows via `FOR UPDATE SKIP LOCKED` and commit —
    /// the same protocol as the due-timer claim, so concurrent pollers never compete for the
    /// same occurrence.
    ///
    /// Firing is at-least-once. The caller advances or resolves the row after the fire; a claim
    /// that crashes before advancing re-claims the SAME occurrence next tick and starts the
    /// process again — a schedule mints work, so there is nothing to deduplicate against, and
    /// "at least once" is the honest contract to document.
    pub async fn claim_due(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<DueTimerSchedule>> {
        if max_entries <= 0 {
            return Ok(Vec::new());
        }
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows = sqlx::query(SQL_CLAIM_DUE)
            .bind(deployment.as_str())
            .bind(now)
            .bind(max_entries)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("timer_schedule claimDue"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("timer_schedule claimDue commit"))?;
        fn e(source: sqlx::Error) -> PersistenceError {
            PersistenceError::Database {
                operation: "timer_schedule read due row",
                source,
            }
        }
        rows.iter()
            .map(|row| {
                Ok(DueTimerSchedule {
                    process_id: row.try_get("process_id").map_err(e)?,
                    node_id: row.try_get("node_id").map_err(e)?,
                    tenant: row.try_get("tenant").map_err(e)?,
                    module_key: row.try_get("module_key").map_err(e)?,
                    kind: row.try_get("kind").map_err(e)?,
                    spec: row.try_get("spec").map_err(e)?,
                    next_due_at: row.try_get("next_due_at").map_err(e)?,
                    remaining_fires: row.try_get("remaining_fires").map_err(e)?,
                })
            })
            .collect()
    }

    /// Move an armed schedule to its next occurrence (a repeating cycle that still has budget).
    pub async fn advance(
        &self,
        deployment: &DeploymentId,
        process_id: &str,
        node_id: &str,
        next_due_at: OffsetDateTime,
        remaining_fires: Option<i32>,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_ADVANCE)
            .bind(deployment.as_str())
            .bind(process_id)
            .bind(node_id)
            .bind(next_due_at)
            .bind(remaining_fires)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("timer_schedule advance"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("timer_schedule advance commit"))
    }

    /// RESOLVE one schedule — a single-shot timer that has fired, an exhausted `R<n>` budget, or
    /// a row whose process/start node is no longer in the deployment. Idempotent.
    pub async fn resolve(
        &self,
        deployment: &DeploymentId,
        process_id: &str,
        node_id: &str,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_RESOLVE_NODE)
            .bind(deployment.as_str())
            .bind(process_id)
            .bind(node_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("timer_schedule resolve"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("timer_schedule resolve commit"))
    }

    /// Every schedule row of a deployment (both statuses), ordered by `(process, node)`.
    pub async fn list(&self, deployment: &DeploymentId) -> Result<Vec<TimerScheduleRow>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows = sqlx::query(SQL_LIST)
            .bind(deployment.as_str())
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("timer_schedule list"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("timer_schedule list commit"))?;
        fn e(source: sqlx::Error) -> PersistenceError {
            PersistenceError::Database {
                operation: "timer_schedule read row",
                source,
            }
        }
        rows.iter()
            .map(|row| {
                Ok(TimerScheduleRow {
                    process_id: row.try_get("process_id").map_err(e)?,
                    node_id: row.try_get("node_id").map_err(e)?,
                    tenant: row.try_get("tenant").map_err(e)?,
                    module_key: row.try_get("module_key").map_err(e)?,
                    kind: row.try_get("kind").map_err(e)?,
                    spec: row.try_get("spec").map_err(e)?,
                    next_due_at: row.try_get("next_due_at").map_err(e)?,
                    remaining_fires: row.try_get("remaining_fires").map_err(e)?,
                    status: row.try_get("status").map_err(e)?,
                    resolved_at: row.try_get("resolved_at").map_err(e)?,
                })
            })
            .collect()
    }
}
