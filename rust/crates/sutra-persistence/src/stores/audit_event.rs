//! Audit-event store (`audit_event`, V201) — the durable per-instance audit trail.
//!
//! Semantics: `(deployment_id, instance_id, seq)` UNIQUE + `INSERT ... ON CONFLICT DO NOTHING`.
//! The engine guarantees a monotonic per-instance seq (persisted in the snapshot's `audit_seq`,
//! seeded back on resume), so a mid-flight restart or a concurrent replica re-emitting an
//! already-persisted `(instance, seq)` is a NO-OP rather than a constraint error. Best-effort:
//! the engine-side sink logs any `Err` and never fails execution.
//!
//! A step re-run after a failed or lost commit re-emits the same seqs under a new attempt
//! (V206): [`PgAuditEventStore::insert_shipped`] keeps, per `(deployment, instance,
//! attempt_base)`, the rows of the latest attempt and deletes every earlier attempt's rows, so
//! the journal holds the committed pass, never the one that did not commit.
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
    /// The audit spool record this row was shipped from, `<spoolId>:<n>` (V205). The shipper
    /// can write a record more than once (after a crash between its write and its checkpoint);
    /// a second row with the same reference is dropped, which the `(deployment, instance, seq)`
    /// index alone cannot do for a deployment-level or transport row. `None` for a row not
    /// shipped from a spool (no dedup beyond that index).
    pub spool_ref: Option<String>,
    /// The pass that wrote the row (V206); `None` for a row outside a pass (transport,
    /// deployment-level, engine-written events, rows from before 0.9.0).
    pub attempt: Option<AuditRowAttempt>,
}

/// The pass (attempt) an engine audit row came from (V206). For each `(deployment, instance,
/// base)` the store keeps the rows of the latest `(at, id)` attempt: an earlier attempt from the
/// same base is one whose step never committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRowAttempt {
    pub id: String,
    pub base: i32,
    pub at: OffsetDateTime,
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
    /// The engine's per-instance seq (the replay-dedup number). Kept as the legacy `?afterSeq=`
    /// cursor; a transport event shares the seq of the engine event it followed.
    pub seq: i32,
    /// The journal ordinal (V203): the row's position in its instance's journal, assigned in
    /// commit order — the lossless paging cursor (`?afterOrdinal=`). `None` only on a
    /// deployment-level row, which belongs to no instance journal.
    pub ordinal: Option<i64>,
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

/// An engine event. `ON CONFLICT DO NOTHING` with no target: any unique violation drops the row,
/// so both the V202 `(deployment, instance, seq)` index and the V205 `spool_ref` index dedup it
/// (first writer wins).
const SQL_INSERT: &str = "INSERT INTO audit_event \
     (deployment_id, instance_id, seq, at, event_type, node_id, diagnostic_code, \
      diagnostic_json, payload_json, spool_ref, attempt_id, attempt_base, attempt_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
     ON CONFLICT DO NOTHING";

/// A run of engine events in one statement (the batch form of [`SQL_INSERT`]): `$1` is the
/// deployment, `$2..$10` are parallel arrays, one element per row. `WITH ORDINALITY` + `ORDER BY`
/// insert the rows in array order, and the V203 ordinal trigger (a row-level BEFORE trigger whose
/// query sees the rows the same statement already inserted) numbers them in that order. A replayed
/// `(deployment, instance, seq)` is skipped exactly as in the single-row form; `DO NOTHING` also
/// tolerates two rows with one key in the same statement.
const SQL_INSERT_BATCH: &str = "INSERT INTO audit_event \
     (deployment_id, instance_id, seq, at, event_type, node_id, diagnostic_code, \
      diagnostic_json, payload_json, spool_ref, attempt_id, attempt_base, attempt_at) \
     SELECT $1, u.instance_id, u.seq, u.at, u.event_type, u.node_id, u.diagnostic_code, \
            u.diagnostic_json, u.payload_json, u.spool_ref, u.attempt_id, u.attempt_base, \
            u.attempt_at \
     FROM UNNEST($2::uuid[], $3::int4[], $4::timestamptz[], $5::text[], $6::text[], \
                 $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::int4[], \
                 $13::timestamptz[]) \
          WITH ORDINALITY AS u(instance_id, seq, at, event_type, node_id, diagnostic_code, \
                               diagnostic_json, payload_json, spool_ref, attempt_id, \
                               attempt_base, attempt_at, ord) \
     ORDER BY u.ord \
     ON CONFLICT DO NOTHING";

/// The attempts of a shipped batch (`$2..$5` parallel arrays: instance, base, at, id)
/// that a LATER attempt from the same base already superseded. `COLLATE "C"` keeps the id
/// tie-break a byte order.
const SQL_SUPERSEDED_ATTEMPTS: &str = "SELECT g.id FROM UNNEST($2::uuid[], $3::int4[], \
     $4::timestamptz[], $5::text[]) AS g(instance_id, base, at, id) \
     WHERE EXISTS (SELECT 1 FROM audit_event a WHERE a.deployment_id = $1 \
       AND a.instance_id = g.instance_id AND a.attempt_base = g.base \
       AND a.attempt_id IS NOT NULL \
       AND (a.attempt_at, a.attempt_id COLLATE \"C\") > (g.at, g.id COLLATE \"C\"))";

/// Delete the rows of every EARLIER attempt from the same base as one of the batch's
/// (surviving) attempts: those passes never committed.
const SQL_DELETE_EARLIER_ATTEMPTS: &str = "DELETE FROM audit_event a USING UNNEST($2::uuid[], \
     $3::int4[], $4::timestamptz[], $5::text[]) AS g(instance_id, base, at, id) \
     WHERE a.deployment_id = $1 AND a.instance_id = g.instance_id AND a.attempt_base = g.base \
       AND a.attempt_id IS NOT NULL \
       AND (a.attempt_at, a.attempt_id COLLATE \"C\") < (g.at, g.id COLLATE \"C\")";

/// A transport event in an instance's journal: numbered with the instance's current high-water
/// seq over the ENGINE's events (1 before the first, so a read from the start of the journal —
/// `seq > 0` — sees it), so it sorts right after the event it followed and never takes a number
/// the engine will write. Outside the V202 unique index; a re-shipped one is dropped by its V205
/// `spool_ref`.
const SQL_INSERT_TRANSPORT_INSTANCE: &str = "INSERT INTO audit_event \
     (deployment_id, instance_id, seq, at, event_type, node_id, diagnostic_code, \
      diagnostic_json, payload_json, spool_ref) \
     SELECT $1, $2, COALESCE((SELECT MAX(seq) FROM audit_event \
                              WHERE deployment_id = $1 AND instance_id = $2 \
                                AND event_type NOT LIKE '%.%'), 1), \
            $3, $4, $5, $6, $7, $8, $9 \
     ON CONFLICT DO NOTHING";

/// A transport event in the deployment-level journal (no instance), seq 0. A re-shipped one is
/// dropped by its V205 `spool_ref`.
const SQL_INSERT_TRANSPORT_DEPLOYMENT: &str = "INSERT INTO audit_event \
     (deployment_id, instance_id, seq, at, event_type, node_id, diagnostic_code, \
      diagnostic_json, payload_json, spool_ref) \
     VALUES ($1, NULL, 0, $2, $3, $4, $5, $6, $7, $8) \
     ON CONFLICT DO NOTHING";

/// Whether `event_type` is one a transport wrote (`<transport>.<EVENT>`; the engine's own event
/// types never carry a `.`) — the rule the V202 index predicate encodes.
fn is_transport_audit_event_type(event_type: &str) -> bool {
    event_type.contains('.')
}

const SQL_REDACT_INSTANCE_PAYLOADS: &str = "UPDATE audit_event SET payload_json = '{}' \
     WHERE deployment_id = $1 AND instance_id = $2 AND payload_json <> '{}'";

/// A media-capture payload (`{"media": [...]}`, what `<q:audit capture="media">` writes) whose
/// media has not been erased yet. The executor serialises the payload with sorted keys and no
/// whitespace, so the prefix test is exact; the erasure marker is the `mediaErasure` key.
///
/// The V204 partial index `audit_event_media_pending` carries this predicate word for word, which
/// is what lets the planner answer the retention sweep's lookup from it instead of reading the
/// whole journal every tick. Change one and the other must change with it.
const MEDIA_PAYLOAD_PREDICATE: &str = "payload_json LIKE '{\"media\":%' \
     AND payload_json NOT LIKE '%\"mediaErasure\":%'";

/// The media rows of one instance, still to be erased.
fn sql_list_media_for_instance() -> String {
    format!(
        "SELECT id, payload_json FROM audit_event \
         WHERE deployment_id = $1 AND instance_id = $2 AND {MEDIA_PAYLOAD_PREDICATE} \
         ORDER BY seq ASC, id ASC"
    )
}

/// A page of media rows still to be erased, after the id cursor `$2`, each with whether its
/// instance has an `INSTANCE_COMPLETED` / `INSTANCE_FAILED` event older than the window `$3`. Only
/// the audit half of "retention is done with this instance": the instance's own row is in the
/// engine database ([`crate::stores::PgInstanceStore::terminal_retention_states`]), and the
/// sweeper combines the two.
fn sql_list_media_page() -> String {
    format!(
        "SELECT a.id, a.instance_id, a.payload_json, \
                EXISTS (SELECT 1 FROM audit_event t \
                        WHERE t.deployment_id = $1 AND t.instance_id = a.instance_id \
                          AND t.event_type IN ('INSTANCE_COMPLETED', 'INSTANCE_FAILED') \
                          AND t.at <= now() - make_interval(secs => $3)) \
         FROM audit_event a \
         WHERE a.deployment_id = $1 AND a.instance_id IS NOT NULL AND a.id > $2 \
           AND {MEDIA_PAYLOAD_PREDICATE} \
         ORDER BY a.id ASC LIMIT $4"
    )
}

/// One media row still to be erased, as the retention sweeper reads it from the audit database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMediaRow {
    /// The row's id (the next page's cursor, and the row [`PgAuditEventStore::amend_payload`]
    /// amends).
    pub id: i64,
    /// The instance the media belongs to.
    pub instance_id: Uuid,
    /// The media payload.
    pub payload_json: String,
    /// Whether the instance has a terminal event older than the window asked for.
    pub terminal_event_expired: bool,
}

/// One batch of audit retention: up to `$3` rows of deployment `$1` older than `$2` seconds, oldest
/// id first. A media event whose object is not erased yet is never deleted: it is the only record
/// of an object still to be deleted.
fn sql_delete_expired_batch() -> String {
    format!(
        "DELETE FROM audit_event WHERE id IN ( \
           SELECT id FROM audit_event \
           WHERE deployment_id = $1 AND at < now() - make_interval(secs => $2) \
             AND NOT ({MEDIA_PAYLOAD_PREDICATE}) \
           ORDER BY id LIMIT $3)"
    )
}

const SQL_AMEND_PAYLOAD: &str = "UPDATE audit_event SET payload_json = $3 \
     WHERE deployment_id = $1 AND id = $2";

/// The instance-history page: seq-ordered, cursor-paged on `seq > $3`. Rides the
/// `audit_event_instance (deployment_id, instance_id, seq)` index end to end — no sort, no scan.
const SQL_LIST_FOR_INSTANCE: &str = "SELECT id, seq, ordinal, at, event_type, node_id, \
      diagnostic_code, diagnostic_json, payload_json \
     FROM audit_event \
     WHERE deployment_id = $1 AND instance_id = $2 AND seq > $3 \
     ORDER BY seq ASC, ordinal ASC, id ASC LIMIT $4";

/// The instance-history page by journal ordinal: cursor-paged on `ordinal > $3`, riding the V203
/// `audit_event_instance_ordinal (deployment_id, instance_id, ordinal)` index.
const SQL_LIST_FOR_INSTANCE_BY_ORDINAL: &str = "SELECT id, seq, ordinal, at, event_type, \
      node_id, diagnostic_code, diagnostic_json, payload_json \
     FROM audit_event \
     WHERE deployment_id = $1 AND instance_id = $2 AND ordinal > $3 \
     ORDER BY ordinal ASC LIMIT $4";

/// The column tuple [`SQL_LIST_FOR_INSTANCE`] selects, IN SELECT ORDER. Named rather than inlined
/// so the query and the [`AuditEventRecord`] projection below sit next to a single declaration of
/// the row shape — a column added to one without the other stops compiling instead of silently
/// transposing. (sqlx's `FromRow` derive would be the other way to say this; the workspace does not
/// enable its `derive` feature, and one read query is not a reason to.)
type AuditEventColumns = (
    i64,
    i32,
    Option<i64>,
    OffsetDateTime,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
);

/// The statement [`PgAuditEventStore::list_media_page`] runs (`$1` deployment, `$2` the id
/// cursor, `$3` retention in seconds, `$4` limit), for tests that check its plan.
#[doc(hidden)]
pub fn list_media_page_sql() -> String {
    sql_list_media_page()
}

impl PgAuditEventStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Idempotent INSERT on a caller-supplied connection. `true` for a first-write, `false`
    /// when the `(deployment_id, instance_id, seq)` row already exists (idempotent replay).
    ///
    /// A transport event (`<transport>.<EVENT>`) is not numbered by the caller: in an instance's
    /// journal it takes the instance's current high-water seq over the engine's events (its
    /// `row.seq` is ignored), in the deployment-level journal (no instance) seq 0. It is always a
    /// first write, unless its `spool_ref` was written before (then `false`).
    pub async fn insert_in(conn: &mut PgConnection, row: &AuditEventRow) -> Result<bool> {
        if is_transport_audit_event_type(&row.event_type) {
            let query = match row.instance_id {
                Some(instance) => sqlx::query(SQL_INSERT_TRANSPORT_INSTANCE)
                    .bind(row.deployment.as_str())
                    .bind(instance),
                None => sqlx::query(SQL_INSERT_TRANSPORT_DEPLOYMENT).bind(row.deployment.as_str()),
            };
            let inserted = query
                .bind(row.at)
                .bind(&row.event_type)
                .bind(&row.node_id)
                .bind(&row.diagnostic_code)
                .bind(&row.diagnostic_json)
                .bind(&row.payload_json)
                .bind(&row.spool_ref)
                .execute(conn)
                .await
                .map_err(PersistenceError::db("audit_event transport insert"))?
                .rows_affected();
            return Ok(inserted == 1);
        }
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
            .bind(&row.spool_ref)
            .bind(row.attempt.as_ref().map(|a| a.id.as_str()))
            .bind(row.attempt.as_ref().map(|a| a.base))
            .bind(row.attempt.as_ref().map(|a| a.at))
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

    /// Persist a run of rows of ONE deployment in a single deployment-scoped transaction — one
    /// commit for the lot, which is what lets the audit writer keep up with an engine emitting
    /// thousands of events a second on a durable database. Returns the number of first-writes
    /// (replayed `(deployment, instance, seq)` rows are skipped, as in [`Self::insert`]).
    ///
    /// Each instance's rows are written in the order given, so their journal ordinals follow it.
    /// Rows of different instances are grouped by instance first (a stable sort, so per-instance
    /// order is untouched): the ordinal trigger takes a per-instance advisory lock that is held
    /// to commit, and taking those locks in one global order means two batches touching the same
    /// instances wait for each other instead of deadlocking. A transport event keeps its place
    /// among its instance's engine events (it is numbered from the engine events before it).
    ///
    /// All or nothing: on `Err` nothing of the batch was committed, so the caller may write the
    /// rows again (one by one, to isolate a bad row) without duplicating any. A row of another
    /// deployment is a caller error, refused before anything is written.
    pub async fn insert_batch(&self, rows: &[AuditEventRow]) -> Result<u64> {
        let Some(first) = rows.first() else {
            return Ok(0);
        };
        let deployment = &first.deployment;
        if let Some(other) = rows.iter().find(|r| &r.deployment != deployment) {
            return Err(PersistenceError::InvalidArgument(format!(
                "audit_event batch mixes deployments '{}' and '{}'",
                deployment.as_str(),
                other.deployment.as_str()
            )));
        }
        let mut ordered: Vec<&AuditEventRow> = rows.iter().collect();
        ordered.sort_by_key(|r| r.instance_id);

        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let inserted = Self::insert_ordered_in(&mut tx, deployment, &ordered).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event batch commit"))?;
        Ok(inserted)
    }

    /// Persist rows of ANY deployments in ONE transaction — the SQL audit store's shipped batch.
    /// The transaction re-scopes itself per deployment (`sutra.deployment_id` is transaction-local,
    /// so each run of one deployment's rows is written under its own scope), and commits once.
    /// Returns the number of first-writes; a row whose `spool_ref` or `(deployment, instance,
    /// seq)` was written before is skipped.
    ///
    /// Rows are written grouped by `(deployment, instance)` in one global order (a stable sort,
    /// so each instance's rows keep the order given): the ordinal trigger's per-instance locks are
    /// then always taken in the same order, and two batches never deadlock. All or nothing: on
    /// `Err` nothing was committed and the caller ships the same batch again.
    pub async fn insert_shipped(&self, rows: &[AuditEventRow]) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut ordered: Vec<&AuditEventRow> = rows.iter().collect();
        ordered.sort_by(|a, b| {
            (a.deployment.as_str(), a.instance_id).cmp(&(b.deployment.as_str(), b.instance_id))
        });
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PersistenceError::db("audit_event shipped batch begin"))?;
        let mut inserted = 0u64;
        let mut start = 0;
        while start < ordered.len() {
            let deployment = &ordered[start].deployment;
            let mut end = start + 1;
            while end < ordered.len() && &ordered[end].deployment == deployment {
                end += 1;
            }
            crate::scope::set_deployment_guc(&mut tx, deployment).await?;
            let rows = Self::supersede_in(&mut tx, deployment, &ordered[start..end]).await?;
            inserted += Self::insert_ordered_in(&mut tx, deployment, &rows).await?;
            start = end;
        }
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event shipped batch commit"))?;
        Ok(inserted)
    }

    /// For one deployment's rows of a shipped batch: per `(instance, base)` only the latest
    /// attempt's rows go on (an earlier attempt in the same batch is dropped), an attempt a later
    /// one already stored supersedes is dropped, and the stored rows of every earlier attempt
    /// from a surviving attempt's base are deleted. Rows without an attempt pass through.
    /// Returns the rows to insert, in their order.
    async fn supersede_in<'a>(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        rows: &[&'a AuditEventRow],
    ) -> Result<Vec<&'a AuditEventRow>> {
        // The latest attempt per (instance, base) within the batch.
        let mut latest: std::collections::HashMap<(Option<Uuid>, i32), &AuditRowAttempt> =
            std::collections::HashMap::new();
        for row in rows {
            if let Some(a) = &row.attempt {
                let slot = latest.entry((row.instance_id, a.base)).or_insert(a);
                if (a.at, a.id.as_bytes()) > (slot.at, slot.id.as_bytes()) {
                    *slot = a;
                }
            }
        }
        if latest.is_empty() {
            return Ok(rows.to_vec());
        }
        let groups: Vec<(Uuid, &AuditRowAttempt)> = latest
            .iter()
            .filter_map(|((instance, _), a)| instance.map(|i| (i, *a)))
            .collect();
        let instances: Vec<Uuid> = groups.iter().map(|(i, _)| *i).collect();
        let bases: Vec<i32> = groups.iter().map(|(_, a)| a.base).collect();
        let ats: Vec<OffsetDateTime> = groups.iter().map(|(_, a)| a.at).collect();
        let ids: Vec<&str> = groups.iter().map(|(_, a)| a.id.as_str()).collect();
        let superseded: Vec<String> = sqlx::query_scalar(SQL_SUPERSEDED_ATTEMPTS)
            .bind(deployment.as_str())
            .bind(&instances)
            .bind(&bases)
            .bind(&ats)
            .bind(&ids)
            .fetch_all(&mut *conn)
            .await
            .map_err(PersistenceError::db("audit_event superseded attempts"))?;
        sqlx::query(SQL_DELETE_EARLIER_ATTEMPTS)
            .bind(deployment.as_str())
            .bind(&instances)
            .bind(&bases)
            .bind(&ats)
            .bind(&ids)
            .execute(&mut *conn)
            .await
            .map_err(PersistenceError::db(
                "audit_event delete superseded attempts",
            ))?;
        Ok(rows
            .iter()
            .copied()
            .filter(|row| match &row.attempt {
                None => true,
                Some(a) => {
                    latest
                        .get(&(row.instance_id, a.base))
                        .is_some_and(|l| l.id == a.id)
                        && !superseded.contains(&a.id)
                }
            })
            .collect())
    }

    /// Write rows of one deployment, already in write order, on `conn` (in its scope): runs of
    /// engine events as one multi-row insert each, transport events one by one.
    async fn insert_ordered_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        ordered: &[&AuditEventRow],
    ) -> Result<u64> {
        let mut inserted = 0u64;
        let mut start = 0;
        while start < ordered.len() {
            if is_transport_audit_event_type(&ordered[start].event_type) {
                if Self::insert_in(conn, ordered[start]).await? {
                    inserted += 1;
                }
                start += 1;
                continue;
            }
            let mut end = start + 1;
            while end < ordered.len() && !is_transport_audit_event_type(&ordered[end].event_type) {
                end += 1;
            }
            inserted += Self::insert_engine_run_in(conn, deployment, &ordered[start..end]).await?;
            start = end;
        }
        Ok(inserted)
    }

    /// One multi-row INSERT of engine events ([`SQL_INSERT_BATCH`]); returns the rows written.
    async fn insert_engine_run_in(
        conn: &mut PgConnection,
        deployment: &DeploymentId,
        rows: &[&AuditEventRow],
    ) -> Result<u64> {
        let instance_ids: Vec<Option<Uuid>> = rows.iter().map(|r| r.instance_id).collect();
        let seqs: Vec<i32> = rows.iter().map(|r| r.seq).collect();
        let ats: Vec<OffsetDateTime> = rows.iter().map(|r| r.at).collect();
        let event_types: Vec<&str> = rows.iter().map(|r| r.event_type.as_str()).collect();
        let node_ids: Vec<Option<&str>> = rows.iter().map(|r| r.node_id.as_deref()).collect();
        let codes: Vec<Option<&str>> = rows.iter().map(|r| r.diagnostic_code.as_deref()).collect();
        let diagnostics: Vec<Option<&str>> =
            rows.iter().map(|r| r.diagnostic_json.as_deref()).collect();
        let payloads: Vec<&str> = rows.iter().map(|r| r.payload_json.as_str()).collect();
        let spool_refs: Vec<Option<&str>> = rows.iter().map(|r| r.spool_ref.as_deref()).collect();
        let attempt_ids: Vec<Option<&str>> = rows
            .iter()
            .map(|r| r.attempt.as_ref().map(|a| a.id.as_str()))
            .collect();
        let attempt_bases: Vec<Option<i32>> = rows
            .iter()
            .map(|r| r.attempt.as_ref().map(|a| a.base))
            .collect();
        let attempt_ats: Vec<Option<OffsetDateTime>> = rows
            .iter()
            .map(|r| r.attempt.as_ref().map(|a| a.at))
            .collect();
        let done = sqlx::query(SQL_INSERT_BATCH)
            .bind(deployment.as_str())
            .bind(instance_ids)
            .bind(seqs)
            .bind(ats)
            .bind(event_types)
            .bind(node_ids)
            .bind(codes)
            .bind(diagnostics)
            .bind(payloads)
            .bind(spool_refs)
            .bind(attempt_ids)
            .bind(attempt_bases)
            .bind(attempt_ats)
            .execute(conn)
            .await
            .map_err(PersistenceError::db("audit_event batch insert"))?;
        Ok(done.rows_affected())
    }

    /// One seq-ordered PAGE of an instance's audit journal — the read side of the trail this store
    /// has been writing since V201, and the backing of `GET /admin/instances/{id}/history`.
    ///
    /// Cursor paging on the per-instance seq (`seq > after_seq`, ascending) — the LEGACY cursor,
    /// kept for existing clients. It can miss a row: a transport event takes the seq of the engine
    /// event it followed, so one written after a reader passed that seq is behind the cursor.
    /// [`Self::list_for_instance_after_ordinal`] is the lossless form. Pass `after_seq = 0` for
    /// the first page (seqs start at 1) and the last returned `seq` for the next; a short page
    /// means the end of the journal.
    ///
    /// An empty result is NOT an error and NOT proof the instance never ran: the journal is
    /// written only for a process that declares `<q:audit>` and routes it to the SQL audit store,
    /// so an instance without that has no rows at all. The endpoint says so in its response rather
    /// than implying the history was lost.
    pub async fn list_for_instance(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        after_seq: i32,
        limit: i64,
    ) -> Result<Vec<AuditEventRecord>> {
        self.list_page(
            deployment,
            sqlx::query_as(SQL_LIST_FOR_INSTANCE)
                .bind(deployment.as_str())
                .bind(instance_id)
                .bind(after_seq)
                .bind(limit),
        )
        .await
    }

    /// One ordinal-ordered PAGE of an instance's audit journal: the rows with `ordinal >
    /// after_ordinal`, ascending. LOSSLESS while the instance is still writing: an instance's
    /// ordinals become visible in increasing order (V203 assigns them under a per-instance lock
    /// held to commit), so a row a later page could return is never numbered below a cursor
    /// already handed out — which a `seq` cursor cannot promise, because a transport event takes
    /// the seq of the engine event it followed. Pass `after_ordinal = 0` for the first page and
    /// the last returned `ordinal` for the next.
    pub async fn list_for_instance_after_ordinal(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        after_ordinal: i64,
        limit: i64,
    ) -> Result<Vec<AuditEventRecord>> {
        self.list_page(
            deployment,
            sqlx::query_as(SQL_LIST_FOR_INSTANCE_BY_ORDINAL)
                .bind(deployment.as_str())
                .bind(instance_id)
                .bind(after_ordinal)
                .bind(limit),
        )
        .await
    }

    async fn list_page(
        &self,
        deployment: &DeploymentId,
        query: sqlx::query::QueryAs<
            '_,
            sqlx::Postgres,
            AuditEventColumns,
            sqlx::postgres::PgArguments,
        >,
    ) -> Result<Vec<AuditEventRecord>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows: Vec<AuditEventColumns> = query
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
                    ordinal,
                    at,
                    event_type,
                    node_id,
                    diagnostic_code,
                    diagnostic_json,
                    payload_json,
                )| AuditEventRecord {
                    id,
                    seq,
                    ordinal,
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

    /// The not-yet-erased media payloads of one instance, as `(row id, payload)`.
    pub async fn list_media_for_instance(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
    ) -> Result<Vec<(i64, String)>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows: Vec<(i64, String)> = sqlx::query_as(&sql_list_media_for_instance())
            .bind(deployment.as_str())
            .bind(instance_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("audit_event list media for instance"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event list media commit"))?;
        Ok(rows)
    }

    /// Delete one batch of `deployment`'s audit rows older than `retention`, at most `batch` rows,
    /// keeping media events whose object is not erased yet. Returns the rows deleted; the caller
    /// loops while a batch comes back full.
    pub async fn delete_expired_batch(
        &self,
        deployment: &DeploymentId,
        retention: std::time::Duration,
        batch: i64,
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let deleted = sqlx::query(&sql_delete_expired_batch())
            .bind(deployment.as_str())
            .bind(retention.as_secs_f64())
            .bind(batch)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("audit_event retention delete"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event retention commit"))?;
        Ok(deleted)
    }

    /// Up to `limit` not-yet-erased media rows of `deployment` with an id above `after_id`, in id
    /// order, each saying whether its instance has a terminal event older than `retention` (see
    /// [`PendingMediaRow`]).
    pub async fn list_media_page(
        &self,
        deployment: &DeploymentId,
        after_id: i64,
        retention: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<PendingMediaRow>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows: Vec<(i64, Uuid, String, bool)> = sqlx::query_as(&sql_list_media_page())
            .bind(deployment.as_str())
            .bind(after_id)
            .bind(retention.as_secs_f64())
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("audit_event list media page"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event list media page commit"))?;
        Ok(rows
            .into_iter()
            .map(
                |(id, instance_id, payload_json, terminal_event_expired)| PendingMediaRow {
                    id,
                    instance_id,
                    payload_json,
                    terminal_event_expired,
                },
            )
            .collect())
    }

    /// Replace one row's payload — the media-erasure amendment, which says the media the row
    /// referenced was deleted.
    pub async fn amend_payload(
        &self,
        deployment: &DeploymentId,
        row_id: i64,
        payload_json: &str,
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let amended = sqlx::query(SQL_AMEND_PAYLOAD)
            .bind(deployment.as_str())
            .bind(row_id)
            .bind(payload_json)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("audit_event amend payload"))?
            .rows_affected();
        tx.commit()
            .await
            .map_err(PersistenceError::db("audit_event amend payload commit"))?;
        Ok(amended)
    }

    /// [`Self::redact_instance_payloads`], then — in the same transaction — `amended`'s rows set
    /// to their media-erasure payloads, so an erased subject's media events still say what was
    /// deleted. Returns the number of rows redacted.
    pub async fn redact_instance_payloads_amending(
        &self,
        deployment: &DeploymentId,
        instance_id: Uuid,
        amended: &[(i64, String)],
    ) -> Result<u64> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let redacted = sqlx::query(SQL_REDACT_INSTANCE_PAYLOADS)
            .bind(deployment.as_str())
            .bind(instance_id)
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("audit_event redact instance payloads"))?
            .rows_affected();
        for (row_id, payload) in amended {
            sqlx::query(SQL_AMEND_PAYLOAD)
                .bind(deployment.as_str())
                .bind(row_id)
                .bind(payload)
                .execute(&mut *tx)
                .await
                .map_err(PersistenceError::db("audit_event amend payload"))?;
        }
        tx.commit().await.map_err(PersistenceError::db(
            "audit_event redact instance payloads commit",
        ))?;
        Ok(redacted)
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
