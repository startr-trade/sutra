//! Outbox store (`outbox_entry`, V601–V606) — the durable half of the transactional outbox.
//!
//! Semantics: row-exists-and-not-`poisoned` = pending; the dispatcher claims
//! due rows with `SELECT ... FOR UPDATE SKIP LOCKED` so concurrent replicas never compete for
//! the same rows; `delete` on success, `defer` (backoff + diagnostic) on failure, and — only
//! when the operator configured `sutra.outbox.retry.max-attempts` — [`OutboxStore::mark_poisoned`]
//! when an entry exhausts that ceiling (V604). A poisoned row is TERMINAL, not gone: it is never
//! claimed again, but it is never deleted either (at-least-once is not traded for silence), it
//! keeps its last diagnostic, and clearing the flag re-arms delivery. The
//! `traceparent` column carries the enqueuing request's W3C trace context; `labels_json`
//! carries the emitting deployment's authoring labels as PAYLOAD data (never an isolation
//! key).
//!
//! The ancillary JSON columns (`cloud_event_json`, `auth_ref_json`, `last_diagnostic_json`)
//! are opaque JSON strings at this layer — their shapes belong to the channels/codecs layer;
//! persistence stores and returns them verbatim.

use std::collections::BTreeMap;

use sqlx::{PgConnection, PgPool, Row};
use sutra_crypto::Sensitive;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// Delivery mode of one outbound reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyMode {
    /// Plain body + headers on the destination.
    Native,
    /// CloudEvents binary content mode.
    CloudEventBinary,
    /// CloudEvents structured content mode.
    CloudEventStructured,
    /// Mirror whatever mode the inbound message used.
    MatchInbound,
}

impl ReplyMode {
    /// Column string form (the enum constant names).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Native => "NATIVE",
            Self::CloudEventBinary => "CLOUDEVENT_BINARY",
            Self::CloudEventStructured => "CLOUDEVENT_STRUCTURED",
            Self::MatchInbound => "MATCH_INBOUND",
        }
    }

    /// Parses the column string form.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "NATIVE" => Ok(Self::Native),
            "CLOUDEVENT_BINARY" => Ok(Self::CloudEventBinary),
            "CLOUDEVENT_STRUCTURED" => Ok(Self::CloudEventStructured),
            "MATCH_INBOUND" => Ok(Self::MatchInbound),
            other => Err(PersistenceError::InvalidArgument(format!(
                "unknown reply mode '{other}'"
            ))),
        }
    }
}

/// One outbox row (entry columns + embedded outbound-reply columns, flattened to the table
/// shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    /// Deployment binding (matches the originating instance's pin).
    pub deployment: DeploymentId,
    /// UUID of the outbox row itself (distinct from `outbox_key`).
    pub entry_id: Uuid,
    /// Originating process instance.
    pub instance_id: Uuid,
    /// The BPMN node that emitted this entry (V606). `None` on rows enqueued before the
    /// column existed. Load-bearing for channel-call `<q:retry>`: the backoff park WITHDRAWS
    /// the dead attempt's rows by (instance, node), and the poison wake is verified against a
    /// poisoned row for that exact pair.
    pub node_id: Option<String>,
    /// Codec-encoded reply body — [`Sensitive`]-wrapped so a stray `{:?}` on an `OutboxEntry`
    /// (the persisted at-rest row) masks the payload; `into_inner()`/`get()` at the SQL bind +
    /// the engine hand-off are the explicit, greppable unwrap boundaries.
    pub body: Sensitive<Vec<u8>>,
    /// MIME type of `body`, when known.
    pub content_type: Option<String>,
    /// Destination URI string.
    pub destination: String,
    /// Reply headers (stored as `headers_json`).
    pub headers: BTreeMap<String, String>,
    /// Whether delivery failure must surface as an incident (vs best-effort).
    pub required: bool,
    /// Delivery mode.
    pub mode: ReplyMode,
    /// Consumer idempotency key (`Idempotency-Key` on delivery).
    pub outbox_key: String,
    /// Opaque CloudEvents JSON, when the reply is a CloudEvent.
    pub cloud_event_json: Option<String>,
    /// Opaque auth-reference JSON for the delivering sink.
    pub auth_ref_json: Option<String>,
    /// The emitting deployment's authoring labels — payload data, never isolation.
    pub labels: BTreeMap<String, String>,
    /// Enqueue time.
    pub created_at: OffsetDateTime,
    /// Not returned by `claim_due` before this instant.
    pub next_attempt_at: OffsetDateTime,
    /// Failed delivery attempts so far.
    pub attempt_count: i32,
    /// Opaque diagnostic JSON from the last failed attempt.
    pub last_diagnostic_json: Option<String>,
    /// W3C `traceparent` of the enqueuing request (the async trace-context bridge); `None` when
    /// untraced.
    pub traceparent: Option<String>,
}

/// Store trait for the outbound-reply outbox.
pub trait OutboxStore {
    /// Persists a new entry. For commit atomicity with the instance snapshot use
    /// [`crate::step::commit_step`] — this method manages its own transaction.
    async fn enqueue(&self, entry: &OutboxEntry) -> Result<()>;
    /// Claims up to `max_entries` due rows via `FOR UPDATE SKIP LOCKED` and commits: the
    /// claim transaction closes before dispatch; delivery idempotency rides on `outbox_key`.
    async fn claim_due(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<OutboxEntry>>;
    /// Deletes an entry after successful delivery; missing row is a no-op.
    async fn delete(&self, deployment: &DeploymentId, entry_id: Uuid) -> Result<()>;
    /// Schedules a retry: sets `next_attempt_at`, increments `attempt_count`, records the
    /// diagnostic JSON.
    async fn defer(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_due_at: OffsetDateTime,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()>;
    /// Marks an entry TERMINAL (V604 `poisoned = TRUE`) and records the diagnostic that ended it.
    /// Called only when the operator configured `sutra.outbox.retry.max-attempts` and this entry
    /// exhausted it. Unlike [`Self::defer`] it does NOT move `next_attempt_at` and does NOT
    /// increment `attempt_count`: the row stops being claimable because of the flag, and its final
    /// attempt count stays the honest record of how many deliveries were tried. A missing row is a
    /// no-op (the delivery raced a redrive/delete).
    async fn mark_poisoned(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()>;
    /// Deployment-scoped pending count — every non-`poisoned` row bound to `deployment`
    /// regardless of `next_attempt_at` (a not-yet-due row is still undelivered work). Terminal
    /// rows are excluded deliberately: they are undelivered but they will never progress, so
    /// counting them would block the DRAINING deployment they belong to from EVER retiring — the
    /// gate asks "is there work still moving here", not "is the table empty". The quiescence half
    /// of the DRAINING-deployment retirement gate, mirroring
    /// [`crate::stores::InstanceStore::count_active`]: both sides of that gate MUST run
    /// through the deployment-scoped path, never the raw pool. A raw-pool count would read
    /// with the `sutra.deployment_id` GUC unset, so under an enforcing RLS posture the policy
    /// evaluates `deployment_id = NULL` and returns 0 — retiring a deployment whose outbox
    /// still holds undelivered replies.
    async fn count_pending_for_deployment(&self, deployment: &DeploymentId) -> Result<i64>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgOutboxStore {
    pool: PgPool,
}

const SQL_INSERT: &str = "INSERT INTO outbox_entry \
     (entry_id, deployment_id, instance_id, body, content_type, destination, \
      headers_json, required, mode, outbox_key, cloud_event_json, auth_ref_json, \
      created_at, next_attempt_at, attempt_count, last_diagnostic_json, traceparent, \
      labels_json, node_id) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, \
      $19)";

const SQL_CLAIM_DUE: &str = "SELECT entry_id, deployment_id, instance_id, body, content_type, \
      destination, headers_json, required, mode, outbox_key, cloud_event_json, auth_ref_json, \
      created_at, next_attempt_at, attempt_count, last_diagnostic_json, traceparent, labels_json, \
      node_id \
     FROM outbox_entry \
     WHERE deployment_id = $1 AND NOT poisoned AND next_attempt_at <= $2 \
     ORDER BY next_attempt_at \
     LIMIT $3 \
     FOR UPDATE SKIP LOCKED";

/// The channel-call `<q:retry>` WITHDRAWAL: delete every row the named node emitted for this
/// instance — pending AND poisoned alike. Runs inside the backoff-park step transaction. The
/// deliberate exception to "the outbox never deletes undelivered rows": a superseded request
/// delivered late would race the re-drive's fresh emission into a double-submit, and one
/// poisoned later would mis-fire a failure against the live attempt. The durable record of the
/// failure lives in the snapshot marker and the poison incident, not in this row.
const SQL_WITHDRAW_FOR_NODE: &str = "DELETE FROM outbox_entry \
     WHERE deployment_id = $1 AND instance_id = $2 AND node_id = $3";

/// The poison-wake evidence read: does a TERMINALLY POISONED row exist for this exact
/// (instance, node)? The engine refuses to fail a parked channel-call on the in-process
/// notification alone — the durable row is the fact.
const SQL_POISONED_EXISTS: &str = "SELECT COUNT(*) FROM outbox_entry \
     WHERE deployment_id = $1 AND instance_id = $2 AND node_id = $3 AND poisoned";

const SQL_DELETE: &str = "DELETE FROM outbox_entry WHERE deployment_id = $1 AND entry_id = $2";

const SQL_DEFER: &str = "UPDATE outbox_entry SET next_attempt_at = $1, \
      attempt_count = attempt_count + 1, last_diagnostic_json = $2 \
     WHERE deployment_id = $3 AND entry_id = $4";

const SQL_POISON: &str = "UPDATE outbox_entry SET poisoned = TRUE, last_diagnostic_json = $1 \
     WHERE deployment_id = $2 AND entry_id = $3";

const SQL_COUNT_PENDING: &str =
    "SELECT COUNT(*) FROM outbox_entry WHERE deployment_id = $1 AND NOT poisoned";

/// [`SQL_COUNT_PENDING`] narrowed to one instance — the admin instance-migration read. Migration
/// does NOT move outbox rows (they were minted by the source deployment's channel bindings and are
/// dispatched against them), so this count exists to REPORT that fact rather than to act on it.
const SQL_COUNT_PENDING_FOR_INSTANCE: &str = "SELECT COUNT(*) FROM outbox_entry \
     WHERE deployment_id = $1 AND instance_id = $2 AND NOT poisoned";

fn headers_json(map: &BTreeMap<String, String>) -> String {
    serde_json::to_string(map).unwrap_or_else(|_| "{}".to_owned())
}

fn parse_headers(json: &str) -> BTreeMap<String, String> {
    serde_json::from_str(json).unwrap_or_default()
}

impl PgOutboxStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// INSERT on a caller-supplied connection (the transactional-step building block).
    pub async fn enqueue_in(conn: &mut PgConnection, entry: &OutboxEntry) -> Result<()> {
        sqlx::query(SQL_INSERT)
            .bind(entry.entry_id)
            .bind(entry.deployment.as_str())
            .bind(entry.instance_id)
            .bind(entry.body.get())
            .bind(entry.content_type.as_deref())
            .bind(&entry.destination)
            .bind(headers_json(&entry.headers))
            .bind(entry.required)
            .bind(entry.mode.as_str())
            .bind(&entry.outbox_key)
            .bind(entry.cloud_event_json.as_deref())
            .bind(entry.auth_ref_json.as_deref())
            .bind(entry.created_at)
            .bind(entry.next_attempt_at)
            .bind(entry.attempt_count)
            .bind(entry.last_diagnostic_json.as_deref())
            .bind(entry.traceparent.as_deref())
            .bind(headers_json(&entry.labels))
            .bind(entry.node_id.as_deref())
            .execute(conn)
            .await
            .map_err(PersistenceError::db("outbox enqueue"))?;
        Ok(())
    }

    /// [`SQL_WITHDRAW_FOR_NODE`] on a caller-supplied connection (the backoff-park step
    /// building block). Returns the number of rows withdrawn.
    pub async fn withdraw_for_node_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
    ) -> Result<u64> {
        let result = sqlx::query(SQL_WITHDRAW_FOR_NODE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("outbox withdrawForNode"))?;
        Ok(result.rows_affected())
    }

    /// [`SQL_POISONED_EXISTS`] through the deployment-scoped path (RLS-safe like every read
    /// here).
    pub async fn poisoned_exists_for_node(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        node_id: &str,
    ) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let count: i64 = sqlx::query_scalar(SQL_POISONED_EXISTS)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(node_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("outbox poisonedExistsForNode"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox poisonedExistsForNode commit"))?;
        Ok(count > 0)
    }

    /// How many non-terminal outbox rows one INSTANCE still has pending under `deployment` —
    /// read through the deployment-scoped path like every other count here, so an enforcing RLS
    /// posture cannot turn it into a silent zero.
    pub async fn count_pending_for_instance(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<i64> {
        let mut tx = crate::scope::begin_deployment_tx(&self.pool, deployment).await?;
        let count: i64 = sqlx::query_scalar(SQL_COUNT_PENDING_FOR_INSTANCE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("outbox countPendingForInstance"))?;
        tx.commit().await.map_err(PersistenceError::db(
            "outbox countPendingForInstance commit",
        ))?;
        Ok(count)
    }

    /// SKIP LOCKED claim on a caller-supplied transaction connection. Rows stay locked until
    /// the caller's transaction ends — this is what the concurrent-claim tests hold open.
    pub async fn claim_due_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<OutboxEntry>> {
        if max_entries <= 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(SQL_CLAIM_DUE)
            .bind(deployment.as_str())
            .bind(now)
            .bind(max_entries)
            .fetch_all(conn)
            .await
            .map_err(PersistenceError::db("outbox claimDue"))?;
        rows.iter().map(read_row).collect()
    }
}

fn read_row(row: &sqlx::postgres::PgRow) -> Result<OutboxEntry> {
    fn e(source: sqlx::Error) -> PersistenceError {
        PersistenceError::Database {
            operation: "outbox read row",
            source,
        }
    }
    let deployment_raw: String = row.try_get("deployment_id").map_err(e)?;
    let mode_raw: String = row.try_get("mode").map_err(e)?;
    let headers_raw: String = row.try_get("headers_json").map_err(e)?;
    let labels_raw: String = row.try_get("labels_json").map_err(e)?;
    Ok(OutboxEntry {
        deployment: DeploymentId::new(deployment_raw)?,
        entry_id: row.try_get("entry_id").map_err(e)?,
        instance_id: row.try_get("instance_id").map_err(e)?,
        node_id: row.try_get("node_id").map_err(e)?,
        body: row.try_get::<Vec<u8>, _>("body").map_err(e)?.into(),
        content_type: row.try_get("content_type").map_err(e)?,
        destination: row.try_get("destination").map_err(e)?,
        headers: parse_headers(&headers_raw),
        required: row.try_get("required").map_err(e)?,
        mode: ReplyMode::parse(&mode_raw)?,
        outbox_key: row.try_get("outbox_key").map_err(e)?,
        cloud_event_json: row.try_get("cloud_event_json").map_err(e)?,
        auth_ref_json: row.try_get("auth_ref_json").map_err(e)?,
        labels: parse_headers(&labels_raw),
        created_at: row.try_get("created_at").map_err(e)?,
        next_attempt_at: row.try_get("next_attempt_at").map_err(e)?,
        attempt_count: row.try_get("attempt_count").map_err(e)?,
        last_diagnostic_json: row.try_get("last_diagnostic_json").map_err(e)?,
        traceparent: row.try_get("traceparent").map_err(e)?,
    })
}

impl OutboxStore for PgOutboxStore {
    async fn enqueue(&self, entry: &OutboxEntry) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, &entry.deployment).await?;
        Self::enqueue_in(&mut tx, entry).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox enqueue commit"))
    }

    async fn claim_due(
        &self,
        deployment: &DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> Result<Vec<OutboxEntry>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let claimed = Self::claim_due_in(&mut tx, deployment, now, max_entries).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox claimDue commit"))?;
        Ok(claimed)
    }

    async fn delete(&self, deployment: &DeploymentId, entry_id: Uuid) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DELETE)
            .bind(deployment.as_str())
            .bind(entry_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("outbox delete"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox delete commit"))
    }

    async fn defer(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_due_at: OffsetDateTime,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_DEFER)
            .bind(new_due_at)
            .bind(new_diagnostic_json)
            .bind(deployment.as_str())
            .bind(entry_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("outbox defer"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox defer commit"))
    }

    async fn mark_poisoned(
        &self,
        deployment: &DeploymentId,
        entry_id: Uuid,
        new_diagnostic_json: Option<&str>,
    ) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        sqlx::query(SQL_POISON)
            .bind(new_diagnostic_json)
            .bind(deployment.as_str())
            .bind(entry_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("outbox markPoisoned"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox markPoisoned commit"))
    }

    async fn count_pending_for_deployment(&self, deployment: &DeploymentId) -> Result<i64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let count: i64 = sqlx::query_scalar(SQL_COUNT_PENDING)
            .bind(deployment.as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(PersistenceError::db("outbox countPending"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("outbox countPending commit"))?;
        Ok(count)
    }
}
