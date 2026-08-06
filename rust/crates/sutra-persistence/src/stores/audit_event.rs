//! Audit-event store (`audit_event`, V201) — the durable per-instance audit trail.
//!
//! Semantics: `(deployment_id, instance_id, seq)` UNIQUE + `INSERT ... ON CONFLICT DO NOTHING`.
//! The engine guarantees a monotonic per-instance seq (persisted in the snapshot's `audit_seq`,
//! seeded back on resume), so a mid-flight restart or a concurrent replica re-emitting an
//! already-persisted `(instance, seq)` is a NO-OP rather than a constraint error. Best-effort:
//! the engine-side sink logs any `Err` and never fails execution.
//!
//! pg-only, like the engine's other system stores (instance/inbox/alias) — the runtime
//! persistence bridge is `PgPool`. The `audit_event` mysql/mssql migrations exist for
//! dialect-completeness but there is no engine writer for them.

use sqlx::{PgConnection, PgPool};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// One row to persist into `audit_event`. The engine's `AuditEvent` maps onto this: the
/// `instance_id` string is parsed to a UUID (`None` when it is not a UUID — the column is
/// nullable and such a row is still recorded best-effort), and `at` is parsed from RFC 3339.
#[derive(Debug, Clone)]
pub struct AuditEventRow {
    pub deployment: DeploymentId,
    pub instance_id: Option<Uuid>,
    /// Monotonic per-instance seq. `INTEGER` in the table (i32) — far beyond any realistic
    /// per-instance event count; the engine sink saturates a `u32` into it.
    pub seq: i32,
    pub at: OffsetDateTime,
    pub event_type: String,
    pub node_id: Option<String>,
    pub diagnostic_code: Option<String>,
    pub diagnostic_json: Option<String>,
    pub payload_json: String,
}

/// Default page size of an instance-history read when the caller supplies no `?limit=`.
pub const AUDIT_HISTORY_PAGE_DEFAULT: i64 = 100;
/// Hard ceiling on an instance-history page. A long-lived instance can accumulate thousands of
/// per-token-move events, and each row can carry a captured payload — the ceiling is what keeps one
/// request from materialising the whole journal.
pub const AUDIT_HISTORY_PAGE_MAX: i64 = 1000;

/// One persisted audit row as READ BACK — the journal projection behind
/// `GET /admin/instances/{id}/history`.
///
/// Distinct from [`AuditEventRow`] (the write shape) on purpose: this carries the surrogate `id`
/// the table assigns and the `instance_id` is already known by the caller, so the read shape is
/// what an operator sees rather than what a sink submits. Every field is rendered as-is —
/// including `payload_json`, which is why the endpoint that serves this is ADMIN-ONLY: a captured
/// payload is business data, held to the same posture as a dead letter's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEventRecord {
    /// The table's surrogate key (`BIGSERIAL`) — stable ordering tiebreaker, not a cursor.
    pub id: i64,
    /// Monotonic per-instance seq — the ORDER and the paging cursor (`?afterSeq=`).
    pub seq: i32,
    /// When the engine emitted the event.
    pub at: OffsetDateTime,
    /// `NODE_ENTERED` / `INSTANCE_COMPLETED` / … (the engine's audit event type).
    pub event_type: String,
    /// The node the event happened at, when it is a node-scoped event.
    pub node_id: Option<String>,
    /// The stable `SUTRA.*` code, for the events that carry a diagnostic.
    pub diagnostic_code: Option<String>,
    /// The diagnostic's JSON body, when one was captured.
    pub diagnostic_json: Option<String>,
    /// The captured payload JSON (`{}` when the process captured nothing, or after a GDPR
    /// erasure redacted it).
    pub payload_json: String,
}

/// PostgreSQL audit-event store.
#[derive(Debug, Clone)]
pub struct PgAuditEventStore {
    pool: PgPool,
}

const SQL_INSERT: &str = "INSERT INTO audit_event \
     (deployment_id, instance_id, seq, at, event_type, node_id, diagnostic_code, \
      diagnostic_json, payload_json) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
     ON CONFLICT (deployment_id, instance_id, seq) DO NOTHING";

const SQL_REDACT_INSTANCE_PAYLOADS: &str = "UPDATE audit_event SET payload_json = '{}' \
     WHERE deployment_id = $1 AND instance_id = $2 AND payload_json <> '{}'";

/// The instance-history page: seq-ordered, cursor-paged on `seq > $3`. Rides the
/// `audit_event_instance (deployment_id, instance_id, seq)` index end to end — no sort, no scan.
const SQL_LIST_FOR_INSTANCE: &str = "SELECT id, seq, at, event_type, node_id, diagnostic_code, \
      diagnostic_json, payload_json \
     FROM audit_event \
     WHERE deployment_id = $1 AND instance_id = $2 AND seq > $3 \
     ORDER BY seq ASC, id ASC LIMIT $4";

/// The column tuple [`SQL_LIST_FOR_INSTANCE`] selects, IN SELECT ORDER. Named rather than inlined
/// so the query and the [`AuditEventRecord`] projection below sit next to a single declaration of
/// the row shape — a column added to one without the other stops compiling instead of silently
/// transposing. (sqlx's `FromRow` derive would be the other way to say this; the workspace does not
/// enable its `derive` feature, and one read query is not a reason to.)
type AuditEventColumns = (
    i64,
    i32,
    OffsetDateTime,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
);

impl PgAuditEventStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Idempotent INSERT on a caller-supplied connection. `true` for a first-write, `false`
    /// when the `(deployment_id, instance_id, seq)` row already exists (idempotent replay).
    pub async fn insert_in(conn: &mut PgConnection, row: &AuditEventRow) -> Result<bool> {
        let inserted = sqlx::query(SQL_INSERT)
            .bind(row.deployment.as_str())
            .bind(row.instance_id)
            .bind(row.seq)
            .bind(row.at)
            .bind(&row.event_type)
            .bind(&row.node_id)
            .bind(&row.diagnostic_code)
            .bind(&row.diagnostic_json)
            .bind(&row.payload_json)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("audit_event insert"))?
            .rows_affected();
        Ok(inserted == 1)
    }

    /// Persist one audit row in its own deployment-scoped transaction (RLS GUC set). `true`
    /// for a first-write, `false` for an already-present `(deployment, instance, seq)`.
    pub async fn insert(&self, row: &AuditEventRow) -> Result<bool> {
        let mut tx = begin_deployment_tx(&self.pool, &row.deployment).await?;
        let first = Self::insert_in(&mut tx, row).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event insert commit"))?;
        Ok(first)
    }

    /// One seq-ordered PAGE of an instance's audit journal — the read side of the trail this store
    /// has been writing since V201, and the backing of `GET /admin/instances/{id}/history`.
    ///
    /// Cursor paging on the per-instance seq (`seq > after_seq`, ascending) rather than an offset:
    /// the seq is the engine's own monotonic per-instance counter, so a cursor page is stable even
    /// while the instance is still running and appending events — an OFFSET page is not. Pass
    /// `after_seq = 0` for the first page (seqs start at 1) and the last returned `seq` for the
    /// next; a short page means the end of the journal.
    ///
    /// An empty result is NOT an error and NOT proof the instance never ran: the journal is
    /// OPT-IN twice over — engine-side (`sutra.audit.sql`) and per-process (`<q:audit>`) — so an
    /// instance with auditing off has no rows at all. The endpoint says so in its response rather
    /// than implying the history was lost.
    pub async fn list_for_instance(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        after_seq: i32,
        limit: i64,
    ) -> Result<Vec<AuditEventRecord>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows: Vec<AuditEventColumns> = sqlx::query_as(SQL_LIST_FOR_INSTANCE)
            .bind(deployment.as_str())
            .bind(instance_id)
            .bind(after_seq)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("audit_event list for instance"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event list for instance commit"))?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    seq,
                    at,
                    event_type,
                    node_id,
                    diagnostic_code,
                    diagnostic_json,
                    payload_json,
                )| AuditEventRecord {
                    id,
                    seq,
                    at,
                    event_type,
                    node_id,
                    diagnostic_code,
                    diagnostic_json,
                    payload_json,
                },
            )
            .collect())
    }

    /// GDPR erasure: nulls out the captured `payload_json` of every audit row
    /// belonging to `instance_id`, RETAINING the row and its audit metadata (`event_type`,
    /// `node_id`, `at`, diagnostics, …) so the erasure itself stays auditable — only the
    /// captured PII payload is redacted, not the trail that it happened. Idempotent: rows
    /// already redacted (`payload_json = '{}'`) are excluded, so a repeat call redacts 0.
    /// Returns the number of rows redacted.
    pub async fn redact_instance_payloads(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let redacted = sqlx::query(SQL_REDACT_INSTANCE_PAYLOADS)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("audit_event redact instance payloads"))?
            .rows_affected();
        tx.commit().await.map_err(PersistenceError::db(
            "audit_event redact instance payloads commit",
        ))?;
        Ok(redacted)
    }
}
