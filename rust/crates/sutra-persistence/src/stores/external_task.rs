//! External-task store (`external_task`, V605) — the durable half of the pull delivery surface.
//!
//! Semantics: a `pull://` outbound delivery PARKS here instead of being pushed; a worker
//! fetch-and-locks rows with a single claim `UPDATE` over a `FOR UPDATE SKIP LOCKED` selection
//! (the outbox/timer-poller claim shape), so concurrent replicas and concurrent workers never
//! hand out the same task twice. Lock expiry is part of the claim PREDICATE, not a sweeper's
//! job: an abandoned lock simply becomes fetchable again on the next fetch.
//!
//! Completion and failure are both OWNERSHIP-GUARDED single statements — the worker's id and an
//! unexpired lock are in the `WHERE` clause, so a stale worker cannot win the race with the one
//! that legitimately holds the lock. Zero rows affected is not "already done": the caller
//! distinguishes "no such task" from "lock lost" with [`PgExternalTaskStore::peek`] and fails
//! closed with the appropriate structured code.
//!
//! Retry/terminal posture mirrors the outbox (`poisoned`, V604): a task that exhausts
//! `retries_left` is marked `failed` — never claimed again, never deleted, keeping its last
//! error — so "we gave up" cannot degrade into "it silently vanished".

use std::collections::BTreeMap;

use sqlx::{PgConnection, PgPool, Row};
use sutra_crypto::Sensitive;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// One parked (or locked) external task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTaskRow {
    pub deployment: DeploymentId,
    /// UUID of the task row itself — the worker-facing `{id}` path segment.
    pub task_id: Uuid,
    /// The instance whose `<q:send>` produced this delivery.
    pub instance_id: Uuid,
    /// The fetch TOPIC and the inbound channel the completion is delivered to.
    pub channel: String,
    pub tenant: String,
    /// The version-bearing `"<tenant>/<module>/<version>"` triple.
    pub module_key: String,
    /// The request payload handed to the worker — [`Sensitive`]-wrapped so a stray `{:?}` on
    /// the persisted row masks it.
    pub body: Sensitive<Vec<u8>>,
    pub content_type: Option<String>,
    pub headers: BTreeMap<String, String>,
    /// The originating outbox row's key — the worker-visible correlation key and the inbox
    /// dedup key the completion re-enters the engine under.
    pub outbox_key: String,
    pub traceparent: Option<String>,
    pub created_at: OffsetDateTime,
    pub fetchable_at: OffsetDateTime,
    pub lock_owner: Option<String>,
    pub lock_expires_at: Option<OffsetDateTime>,
    /// Fetches handed out so far (the honest record — never reset by a retry).
    pub attempt_count: i32,
    /// Remaining failure budget; zero means the next failure is terminal.
    pub retries_left: i32,
    /// TERMINAL — exhausted its retries, never fetched again, retained for inspection.
    pub failed: bool,
    pub last_error: Option<String>,
}

/// Store trait for the external-task (pull) surface.
pub trait ExternalTaskStore {
    /// Parks a new task. A row already carrying this `(deployment, outbox_key)` is left
    /// untouched and reported as `false` — the outbox delivers at-least-once, so a re-delivered
    /// row must not produce a second task.
    async fn park(&self, task: &ExternalTaskRow) -> Result<bool>;

    /// Fetch-and-lock: claims up to `max_tasks` fetchable rows on any of `channels`, stamping
    /// `worker` as the lock owner until `lock_expires_at`. Fetchable = not `failed`, due
    /// (`fetchable_at <= now`) and either unlocked or holding an EXPIRED lock.
    async fn fetch_and_lock(
        &self,
        deployment: &DeploymentId,
        channels: &[String],
        worker: &str,
        now: OffsetDateTime,
        lock_expires_at: OffsetDateTime,
        max_tasks: i64,
    ) -> Result<Vec<ExternalTaskRow>>;

    /// Re-reads one task regardless of lock state — how a caller tells "no such task" from
    /// "someone else holds the lock" after a guarded statement affected zero rows.
    async fn peek(
        &self,
        deployment: &DeploymentId,
        task_id: Uuid,
    ) -> Result<Option<ExternalTaskRow>>;

    /// Ownership-guarded lock extension, held across the completion's engine dispatch so the
    /// lock cannot expire mid-flight. Returns the row when `worker` still holds an unexpired
    /// lock, `None` otherwise (missing row OR lost lock — [`Self::peek`] separates them).
    async fn hold(
        &self,
        deployment: &DeploymentId,
        task_id: Uuid,
        worker: &str,
        now: OffsetDateTime,
        new_expiry: OffsetDateTime,
    ) -> Result<Option<ExternalTaskRow>>;

    /// Deletes a completed task. Called only after the completion has been accepted by the
    /// engine's inbound path, so a crash between the two re-offers the task and the inbox dedup
    /// on `outbox_key` absorbs the duplicate (at-least-once, never at-most-once).
    async fn delete(&self, deployment: &DeploymentId, task_id: Uuid) -> Result<()>;

    /// Ownership-guarded failure. `retries_left` is set from `retries_left_after` and the lock is
    /// released; the row becomes fetchable again at `fetchable_at`. When `retries_left_after` is
    /// zero the row is marked TERMINAL (`failed`) instead. Returns `false` when the guard did not
    /// match (missing row or lost lock).
    #[allow(clippy::too_many_arguments)]
    async fn fail(
        &self,
        deployment: &DeploymentId,
        task_id: Uuid,
        worker: &str,
        now: OffsetDateTime,
        retries_left_after: i32,
        fetchable_at: OffsetDateTime,
        error: &str,
    ) -> Result<bool>;

    /// Deployment-scoped count of tasks still in play — live (non-`failed`) rows regardless of
    /// lock state. The pull-side twin of [`super::OutboxStore::count_pending_for_deployment`]:
    /// parked work that has not been completed is work still moving, so the DRAINING-deployment
    /// retirement gate must see it. Runs through the deployment-scoped path for the same reason
    /// its outbox sibling does — a raw-pool count reads with the GUC unset and returns 0 under
    /// an enforcing RLS posture.
    async fn count_pending_for_deployment(&self, deployment: &DeploymentId) -> Result<i64>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgExternalTaskStore {
    pool: PgPool,
}

const COLUMNS: &str = "task_id, deployment_id, instance_id, channel, tenant, module_key, body, \
     content_type, headers_json, outbox_key, traceparent, created_at, fetchable_at, lock_owner, \
     lock_expires_at, attempt_count, retries_left, failed, last_error";

const SQL_PARK: &str = "INSERT INTO external_task \
     (task_id, deployment_id, instance_id, channel, tenant, module_key, body, content_type, \
      headers_json, outbox_key, traceparent, created_at, fetchable_at, attempt_count, \
      retries_left) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 0, $14) \
     ON CONFLICT (deployment_id, outbox_key) DO NOTHING";

/// The claim: an inner `FOR UPDATE SKIP LOCKED` selection feeding one `UPDATE ... RETURNING`, so
/// the lock stamp and the hand-out are the same statement. Lock EXPIRY is in the predicate — an
/// abandoned task needs no sweeper to come back.
const SQL_FETCH_AND_LOCK: &str = "UPDATE external_task t \
     SET lock_owner = $1, lock_expires_at = $2, attempt_count = t.attempt_count + 1 \
     FROM ( \
       SELECT task_id FROM external_task \
        WHERE deployment_id = $3 AND NOT failed AND channel = ANY($4) AND fetchable_at <= $5 \
          AND (lock_expires_at IS NULL OR lock_expires_at <= $5) \
        ORDER BY fetchable_at, created_at \
        LIMIT $6 \
        FOR UPDATE SKIP LOCKED \
     ) due \
     WHERE t.task_id = due.task_id \
     RETURNING t.task_id, t.deployment_id, t.instance_id, t.channel, t.tenant, t.module_key, \
      t.body, t.content_type, t.headers_json, t.outbox_key, t.traceparent, t.created_at, \
      t.fetchable_at, t.lock_owner, t.lock_expires_at, t.attempt_count, t.retries_left, \
      t.failed, t.last_error";

const SQL_HOLD: &str = "UPDATE external_task t SET lock_expires_at = $1 \
     WHERE t.deployment_id = $2 AND t.task_id = $3 AND t.lock_owner = $4 \
       AND t.lock_expires_at > $5 AND NOT t.failed \
     RETURNING t.task_id, t.deployment_id, t.instance_id, t.channel, t.tenant, t.module_key, \
      t.body, t.content_type, t.headers_json, t.outbox_key, t.traceparent, t.created_at, \
      t.fetchable_at, t.lock_owner, t.lock_expires_at, t.attempt_count, t.retries_left, \
      t.failed, t.last_error";

const SQL_FAIL: &str = "UPDATE external_task \
     SET lock_owner = NULL, lock_expires_at = NULL, retries_left = $1, fetchable_at = $2, \
         failed = ($1 <= 0), last_error = $3 \
     WHERE deployment_id = $4 AND task_id = $5 AND lock_owner = $6 AND lock_expires_at > $7 \
       AND NOT failed";

const SQL_DELETE: &str = "DELETE FROM external_task WHERE deployment_id = $1 AND task_id = $2";

const SQL_COUNT_PENDING: &str =
    "SELECT COUNT(*) FROM external_task WHERE deployment_id = $1 AND NOT failed";

fn headers_json(map: &BTreeMap<String, String>) -> String {
    serde_json::to_string(map).unwrap_or_else(|_| "{}".to_owned())
}

fn parse_headers(json: &str) -> BTreeMap<String, String> {
    serde_json::from_str(json).unwrap_or_default()
}

impl PgExternalTaskStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// INSERT on a caller-supplied connection.
    pub async fn park_in(conn: &mut PgConnection, task: &ExternalTaskRow) -> Result<bool> {
        let affected = sqlx::query(SQL_PARK)
            .bind(task.task_id)
            .bind(task.deployment.as_str())
            .bind(task.instance_id)
            .bind(&task.channel)
            .bind(&task.tenant)
            .bind(&task.module_key)
            .bind(task.body.get())
            .bind(task.content_type.as_deref())
            .bind(headers_json(&task.headers))
            .bind(&task.outbox_key)
            .bind(task.traceparent.as_deref())
            .bind(task.created_at)
            .bind(task.fetchable_at)
            .bind(task.retries_left)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("external task park"))?
            .rows_affected();
        Ok(affected == 1)
    }

    /// The claim on a caller-supplied transaction connection — rows stay locked until the
    /// caller's transaction ends, which is what the concurrent-claim tests hold open.
    pub async fn fetch_and_lock_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        channels: &[String],
        worker: &str,
        now: OffsetDateTime,
        lock_expires_at: OffsetDateTime,
        max_tasks: i64,
    ) -> Result<Vec<ExternalTaskRow>> {
        if max_tasks <= 0 || channels.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_FETCH_AND_LOCK)
            .bind(worker)
            .bind(lock_expires_at)
            .bind(deployment.as_str())
            .bind(channels)
            .bind(now)
            .bind(max_tasks)
            .fetch_all(conn)
            .await
            .map_err(PersistenceError::db("external task fetchAndLock"))?;
        rows.iter().map(read_row).collect()
    }
}

fn read_row(row: &sqlx::postgres::PgRow) -> Result<ExternalTaskRow> {
    fn e(source: sqlx::Error) -> PersistenceError {
        PersistenceError::Database {
            operation: "external task read row",
            source,
        }
    }
    let deployment_raw: String = row.try_get("deployment_id").map_err(e)?;
    let headers_raw: String = row.try_get("headers_json").map_err(e)?;
    Ok(ExternalTaskRow {
        deployment: DeploymentId::new(deployment_raw)?,
        task_id: row.try_get("task_id").map_err(e)?,
        instance_id: row.try_get("instance_id").map_err(e)?,
        channel: row.try_get("channel").map_err(e)?,
        tenant: row.try_get("tenant").map_err(e)?,
        module_key: row.try_get("module_key").map_err(e)?,
        body: row.try_get::<Vec<u8>, _>("body").map_err(e)?.into(),
        content_type: row.try_get("content_type").map_err(e)?,
        headers: parse_headers(&headers_raw),
        outbox_key: row.try_get("outbox_key").map_err(e)?,
        traceparent: row.try_get("traceparent").map_err(e)?,
        created_at: row.try_get("created_at").map_err(e)?,
        fetchable_at: row.try_get("fetchable_at").map_err(e)?,
        lock_owner: row.try_get("lock_owner").map_err(e)?,
        lock_expires_at: row.try_get("lock_expires_at").map_err(e)?,
        attempt_count: row.try_get("attempt_count").map_err(e)?,
        retries_left: row.try_get("retries_left").map_err(e)?,
        failed: row.try_get("failed").map_err(e)?,
        last_error: row.try_get("last_error").map_err(e)?,
    })
}

impl ExternalTaskStore for PgExternalTaskStore {
    async fn park(&self, task: &ExternalTaskRow) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, &task.deployment).await?;
        let inserted = Self::park_in(&mut tx, task).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("external task park commit"))?;
        Ok(inserted)
    }

    async fn fetch_and_lock(
        &self,
        deployment: &DeploymentId,
        channels: &[String],
        worker: &str,
        now: OffsetDateTime,
        lock_expires_at: OffsetDateTime,
        max_tasks: i64,
    ) -> Result<Vec<ExternalTaskRow>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let locked = Self::fetch_and_lock_in(
            &mut tx,
            deployment,
            channels,
            worker,
            now,
            lock_expires_at,
            max_tasks,
        )
        .await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("external task fetchAndLock commit"))?;
        Ok(locked)
    }

    async fn peek(
        &self,
        deployment: &DeploymentId,
        task_id: Uuid,
    ) -> Result<Option<ExternalTaskRow>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let sql = format!(
            "SELECT {COLUMNS} FROM external_task WHERE deployment_id = $1 AND task_id = $2"
        );
        let row = sqlx::query(&sql)
            .bind(deployment.as_str())
            .bind(task_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("external task peek"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("external task peek commit"))?;
        row.as_ref().map(read_row).transpose()
    }

    async fn hold(
        &self,
        deployment: &DeploymentId,
        task_id: Uuid,
        worker: &str,
        now: OffsetDateTime,
        new_expiry: OffsetDateTime,
    ) -> Result<Option<ExternalTaskRow>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let row = sqlx::query(SQL_HOLD)
            .bind(new_expiry)
            .bind(deployment.as_str())
            .bind(task_id)
            .bind(worker)
            .bind(now)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("external task hold"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("external task hold commit"))?;
        row.as_ref().map(read_row).transpose()
    }

    async fn delete(&self, deployment: &DeploymentId, task_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(task_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("external task delete"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("external task delete commit"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn fail(
        &self,
        deployment: &DeploymentId,
        task_id: Uuid,
        worker: &str,
        now: OffsetDateTime,
        retries_left_after: i32,
        fetchable_at: OffsetDateTime,
        error: &str,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let affected = sqlx::query(SQL_FAIL)
            .bind(retries_left_after)
            .bind(fetchable_at)
            .bind(error)
            .bind(deployment.as_str())
            .bind(task_id)
            .bind(worker)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("external task fail"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("external task fail commit"))?;
        Ok(affected == 1)
    }

    async fn count_pending_for_deployment(&self, deployment: &DeploymentId) -> Result<i64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let count: i64 = sqlx::query_scalar(SQL_COUNT_PENDING)
            .bind(deployment.as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("external task countPending"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("external task countPending commit"))?;
        Ok(count)
    }
}
