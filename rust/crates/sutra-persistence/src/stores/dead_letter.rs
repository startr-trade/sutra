//! Dead-letter / incident store (`dead_letter`, V1201 + V1202) — the durable floor beneath the
//! `IncidentSink` seam's always-on `tracing::error!` log floor, and the redrive substrate behind
//! the admin dead-letter surface.
//!
//! One row per consumed failure the engine refused to blind-retry:
//! - an inbound arrival on a NON-idempotent process that failed during execution (the dispatcher
//!   acks it at-most-once — `sutra_channels::stores::InboundIncident`), and
//! - an outbound `required` delivery the outbox dispatcher poisoned (recorded ONCE for the entry,
//!   not once per retry tick).
//!
//! There is no instance_id/seq column: a dead-lettered arrival fails BEFORE any quiescent commit,
//! so no instance is ever persisted for it; the row records the metadata of the failure itself
//! (channel, process, dedup key, failure code + detail, timestamps).
//!
//! **V1202 added the replay capture** — `payload`, `headers_json`, `content_type`, `tenant`,
//! `module_key`. Together they are exactly what the NORMAL intake path needs to re-dispatch the
//! consumed message as a FRESH delivery; all five are nullable, because rows written before V1202
//! (and outbound incidents, which have no inbound message) carry none and must degrade to a
//! structured "no payload captured" answer rather than a fabricated one.
//!
//! **Sensitive-data posture (normative).** `payload`/`headers_json` hold RAW, UNREDACTED business
//! data. Two protections carry it:
//! 1. **Deployment-RLS scoped.** Every statement here binds `deployment_id` explicitly AND runs
//!    inside a deployment-scoped transaction ([`begin_deployment_tx`]) so V1201's
//!    `dead_letter_deployment_iso` policy applies — one deployment can never read another's dead
//!    letters, whatever id a caller forges.
//! 2. **Admin-only, and the bytes never render.** The read surface is the OIDC/key-gated
//!    `/admin/dead-letters…` router, never the unauthenticated `/sutra/*` operate routes. The
//!    listing/get projection ([`DeadLetterRecord`]) carries the payload's LENGTH, never its bytes
//!    — `octet_length(payload)` means they are not even read off disk; the bytes leave this store
//!    on exactly one call, [`PgDeadLetterStore::replay_payload`], whose result feeds intake and is
//!    never serialised into a response.
//!
//! Operators who cannot accept business data at rest leave `sutra.incident.sql` off (the default):
//! no row is written at all and the `tracing::error!` floor remains the record.
//!
//! pg-only, like the engine's other system stores (audit/instance/inbox) — the runtime
//! persistence bridge is `PgPool`. Writing is best-effort: the engine-side sink logs any `Err`
//! and never fails execution (the `record` decision is already committed by then).

use std::collections::BTreeMap;

use sqlx::{PgConnection, PgPool, Row};
use time::OffsetDateTime;

use crate::scope::begin_deployment_tx;
use crate::{DeploymentId, PersistenceError, Result};

/// The hard ceiling on how many rows one dead-letter listing page returns, whatever the caller
/// asks for. A DLQ listing is an operator console read against a table with no retention policy
/// yet — an unbounded `SELECT` is how one becomes an outage.
pub const DEAD_LETTER_PAGE_MAX: i64 = 200;

/// The listing page size used when a caller supplies none.
pub const DEAD_LETTER_PAGE_DEFAULT: i64 = 50;

/// One row to persist into `dead_letter`. Maps `sutra_channels::stores::InboundIncident` 1:1
/// (the `deployment` string is parsed to a [`DeploymentId`] at the engine boundary, and
/// `received_at` from RFC 3339).
#[derive(Clone)]
pub struct DeadLetterRow {
    pub deployment: DeploymentId,
    pub channel: String,
    pub process_id: String,
    /// The inbound dedup key, or `""` when none was supplied.
    pub dedup_key: String,
    /// The causing diagnostic code (e.g. `SUTRA.INBOUND.NON_IDEMPOTENT_FAILURE`).
    pub failure_code: String,
    /// The causing diagnostic message.
    pub detail: String,
    /// When the message was received (RFC 3339 on the wire; the engine parses it).
    pub received_at: OffsetDateTime,
    /// The consumed body, already truncated to the channel's effective payload cap by the
    /// capturing dispatcher. `None` when nothing was captured (a pre-V1202 writer, or an outbound
    /// incident with no inbound message) — the replay path then fails closed.
    pub payload: Option<Vec<u8>>,
    /// The inbound transport headers, replayed verbatim. Empty when nothing was captured.
    pub headers: BTreeMap<String, String>,
    /// The declared inbound media type, when the delivery carried one.
    pub content_type: Option<String>,
    /// The delivering tenant (replay re-stamps it — a client never supplies it).
    pub tenant: String,
    /// The `"<tenant>/<module>/<version>"` namespace key of the serving channel — with `channel`,
    /// the pair the channel registry resolves a binding by.
    pub module_key: String,
}

/// Masks the raw payload: a `{:?}` on a dead-letter row must never spill the business bytes the
/// row exists to hold (the `Sensitive` posture, applied by hand because this is a SQL bind shape).
impl std::fmt::Debug for DeadLetterRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadLetterRow")
            .field("deployment", &self.deployment)
            .field("channel", &self.channel)
            .field("process_id", &self.process_id)
            .field("dedup_key", &self.dedup_key)
            .field("failure_code", &self.failure_code)
            .field("detail", &self.detail)
            .field("received_at", &self.received_at)
            .field("payload_bytes", &self.payload.as_ref().map(Vec::len))
            .field("header_count", &self.headers.len())
            .field("content_type", &self.content_type)
            .field("tenant", &self.tenant)
            .field("module_key", &self.module_key)
            .finish()
    }
}

/// One dead-letter row as the ADMIN READ surface projects it: the full failure metadata plus a
/// payload *indicator* — `payload_bytes` — and never the payload itself. Shared by the listing and
/// the by-id read so both agree on the projection, and deliberately not carrying bytes at all: the
/// only way to obtain them is [`PgDeadLetterStore::replay_payload`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterRecord {
    /// Row identity (`BIGSERIAL`) — the admin path segment + replay key.
    pub id: i64,
    pub deployment: DeploymentId,
    pub channel: String,
    pub process_id: String,
    pub dedup_key: String,
    pub failure_code: String,
    pub detail: String,
    pub received_at: OffsetDateTime,
    /// When the engine wrote the row (DB clock).
    pub recorded_at: OffsetDateTime,
    /// Size of the captured payload in bytes; `None` when none was captured — the signal an
    /// operator reads as "this one is not replayable".
    pub payload_bytes: Option<i32>,
    pub content_type: Option<String>,
    /// Empty when the row predates V1202's capture columns.
    pub tenant: String,
    /// Empty when the row predates V1202's capture columns.
    pub module_key: String,
}

/// Everything needed to re-dispatch one dead letter through the NORMAL intake path — the ONLY
/// projection that carries raw bytes out of this store. Never serialised into an HTTP response;
/// its single consumer is the replay handler, which hands it to the engine actor with a freshly
/// minted event id (so inbox dedup treats the redrive as a new delivery rather than swallowing it).
#[derive(Clone)]
pub struct DeadLetterReplayPayload {
    pub deployment: DeploymentId,
    pub tenant: String,
    pub module_key: String,
    pub channel: String,
    pub content_type: Option<String>,
    pub headers: BTreeMap<String, String>,
    /// The captured body. `None` ⇒ the row carries no payload and the replay must fail closed
    /// rather than deliver an empty message.
    pub payload: Option<Vec<u8>>,
}

/// Same masking rationale as [`DeadLetterRow`]: this type exists to carry business bytes.
impl std::fmt::Debug for DeadLetterReplayPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadLetterReplayPayload")
            .field("deployment", &self.deployment)
            .field("tenant", &self.tenant)
            .field("module_key", &self.module_key)
            .field("channel", &self.channel)
            .field("content_type", &self.content_type)
            .field("header_count", &self.headers.len())
            .field("payload_bytes", &self.payload.as_ref().map(Vec::len))
            .finish()
    }
}

/// PostgreSQL dead-letter store.
#[derive(Debug, Clone)]
pub struct PgDeadLetterStore {
    pool: PgPool,
}

const SQL_INSERT: &str = "INSERT INTO dead_letter \
     (deployment_id, channel, process_id, dedup_key, failure_code, detail, received_at, \
      payload, headers_json, content_type, tenant, module_key) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";

/// The metadata projection — note `octet_length(payload)` rather than `payload`: the bytes are
/// never read off disk on a listing/get path (the posture the module header states).
const SQL_LIST: &str = "SELECT id, deployment_id, channel, process_id, dedup_key, failure_code, \
      detail, received_at, recorded_at, octet_length(payload) AS payload_bytes, content_type, \
      tenant, module_key \
     FROM dead_letter WHERE deployment_id = $1 \
     ORDER BY recorded_at DESC, id DESC LIMIT $2 OFFSET $3";

const SQL_GET: &str = "SELECT id, deployment_id, channel, process_id, dedup_key, failure_code, \
      detail, received_at, recorded_at, octet_length(payload) AS payload_bytes, content_type, \
      tenant, module_key \
     FROM dead_letter WHERE deployment_id = $1 AND id = $2";

const SQL_REPLAY: &str = "SELECT deployment_id, channel, payload, headers_json, content_type, \
      tenant, module_key \
     FROM dead_letter WHERE deployment_id = $1 AND id = $2";

fn headers_json(map: &BTreeMap<String, String>) -> Option<String> {
    if map.is_empty() {
        return None;
    }
    serde_json::to_string(map).ok()
}

fn parse_headers(json: Option<String>) -> BTreeMap<String, String> {
    json.and_then(|j| serde_json::from_str(&j).ok())
        .unwrap_or_default()
}

/// `""` → `None` so an uncaptured tenant/module_key stores as SQL NULL (the "not captured" form
/// the replay path checks) instead of an empty string that reads like a real value.
fn opt_str(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

/// A stored `deployment_id` back into its validated form. The column can only hold what this store
/// wrote (an already-validated id), so a parse failure means a corrupt row, not a caller error.
fn row_deployment(raw: String) -> Result<DeploymentId> {
    DeploymentId::new(raw)
}

fn to_record(row: &sqlx::postgres::PgRow) -> Result<DeadLetterRecord> {
    Ok(DeadLetterRecord {
        id: row
            .try_get("id")
            .map_err(PersistenceError::db("dead_letter id"))?,
        deployment: row_deployment(
            row.try_get("deployment_id")
                .map_err(PersistenceError::db("dead_letter deployment_id"))?,
        )?,
        channel: row
            .try_get("channel")
            .map_err(PersistenceError::db("dead_letter channel"))?,
        process_id: row
            .try_get("process_id")
            .map_err(PersistenceError::db("dead_letter process_id"))?,
        dedup_key: row
            .try_get("dedup_key")
            .map_err(PersistenceError::db("dead_letter dedup_key"))?,
        failure_code: row
            .try_get("failure_code")
            .map_err(PersistenceError::db("dead_letter failure_code"))?,
        detail: row
            .try_get("detail")
            .map_err(PersistenceError::db("dead_letter detail"))?,
        received_at: row
            .try_get("received_at")
            .map_err(PersistenceError::db("dead_letter received_at"))?,
        recorded_at: row
            .try_get("recorded_at")
            .map_err(PersistenceError::db("dead_letter recorded_at"))?,
        payload_bytes: row
            .try_get("payload_bytes")
            .map_err(PersistenceError::db("dead_letter payload_bytes"))?,
        content_type: row
            .try_get("content_type")
            .map_err(PersistenceError::db("dead_letter content_type"))?,
        tenant: row
            .try_get::<Option<String>, _>("tenant")
            .map_err(PersistenceError::db("dead_letter tenant"))?
            .unwrap_or_default(),
        module_key: row
            .try_get::<Option<String>, _>("module_key")
            .map_err(PersistenceError::db("dead_letter module_key"))?
            .unwrap_or_default(),
    })
}

impl PgDeadLetterStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// INSERT on a caller-supplied connection (the RLS GUC must already be set by the caller).
    pub async fn insert_in(conn: &mut PgConnection, row: &DeadLetterRow) -> Result<()> {
        sqlx::query(SQL_INSERT)
            .bind(row.deployment.as_str())
            .bind(&row.channel)
            .bind(&row.process_id)
            .bind(&row.dedup_key)
            .bind(&row.failure_code)
            .bind(&row.detail)
            .bind(row.received_at)
            .bind(row.payload.as_deref())
            .bind(headers_json(&row.headers))
            .bind(row.content_type.as_deref())
            .bind(opt_str(&row.tenant))
            .bind(opt_str(&row.module_key))
            .execute(conn)
            .await
            .map_err(PersistenceError::db("dead_letter insert"))?;
        Ok(())
    }

    /// Persist one dead-letter row in its own deployment-scoped transaction (RLS GUC set).
    pub async fn insert(&self, row: &DeadLetterRow) -> Result<()> {
        let mut tx = begin_deployment_tx(&self.pool, &row.deployment).await?;
        Self::insert_in(&mut tx, row).await?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("dead_letter insert commit"))?;
        Ok(())
    }

    /// One page of a deployment's dead letters, NEWEST FIRST (`recorded_at DESC`, ties broken by
    /// the monotonic id so paging stays stable under concurrent inserts). `limit` is clamped to
    /// `1..=`[`DEAD_LETTER_PAGE_MAX`] and a negative `offset` reads as 0 — the surface cannot be
    /// talked into an unbounded scan. Payload bytes are not read, only their length.
    pub async fn list(
        &self,
        deployment: &DeploymentId,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DeadLetterRecord>> {
        let limit = limit.clamp(1, DEAD_LETTER_PAGE_MAX);
        let offset = offset.max(0);
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let rows = sqlx::query(SQL_LIST)
            .bind(deployment.as_str())
            .bind(limit)
            .bind(offset)
            .fetch_all(&mut *tx)
            .await
            .map_err(PersistenceError::db("dead_letter list"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("dead_letter list commit"))?;
        rows.iter().map(to_record).collect()
    }

    /// One dead letter by `(deployment, id)`; `None` when absent or invisible to this deployment.
    /// Metadata only — see [`Self::replay_payload`] for the bytes.
    pub async fn get(
        &self,
        deployment: &DeploymentId,
        id: i64,
    ) -> Result<Option<DeadLetterRecord>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let row = sqlx::query(SQL_GET)
            .bind(deployment.as_str())
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("dead_letter get"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("dead_letter get commit"))?;
        row.as_ref().map(to_record).transpose()
    }

    /// The REPLAY read: the captured payload + headers + routing keys of one dead letter. The one
    /// call that lifts raw business bytes out of this store — its result goes to intake and
    /// nowhere else. `None` = no such row; a row whose `payload` is NULL comes back with
    /// `payload: None`, so the caller answers "nothing was captured" precisely instead of
    /// replaying an empty body.
    pub async fn replay_payload(
        &self,
        deployment: &DeploymentId,
        id: i64,
    ) -> Result<Option<DeadLetterReplayPayload>> {
        let mut tx = begin_deployment_tx(&self.pool, deployment).await?;
        let row = sqlx::query(SQL_REPLAY)
            .bind(deployment.as_str())
            .bind(id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("dead_letter replay fetch"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("dead_letter replay commit"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(DeadLetterReplayPayload {
            deployment: row_deployment(
                row.try_get("deployment_id")
                    .map_err(PersistenceError::db("dead_letter deployment_id"))?,
            )?,
            tenant: row
                .try_get::<Option<String>, _>("tenant")
                .map_err(PersistenceError::db("dead_letter tenant"))?
                .unwrap_or_default(),
            module_key: row
                .try_get::<Option<String>, _>("module_key")
                .map_err(PersistenceError::db("dead_letter module_key"))?
                .unwrap_or_default(),
            channel: row
                .try_get("channel")
                .map_err(PersistenceError::db("dead_letter channel"))?,
            content_type: row
                .try_get("content_type")
                .map_err(PersistenceError::db("dead_letter content_type"))?,
            headers: parse_headers(
                row.try_get("headers_json")
                    .map_err(PersistenceError::db("dead_letter headers_json"))?,
            ),
            payload: row
                .try_get("payload")
                .map_err(PersistenceError::db("dead_letter payload"))?,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_headers_store_as_null_and_round_trip_empty() {
        assert!(headers_json(&BTreeMap::new()).is_none());
        assert!(parse_headers(None).is_empty());
    }

    #[test]
    fn headers_round_trip_through_the_json_column() {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_owned(), "application/json".to_owned());
        headers.insert("x-corr".to_owned(), "abc".to_owned());
        let json = headers_json(&headers).expect("non-empty headers serialise");
        assert_eq!(parse_headers(Some(json)), headers);
    }

    #[test]
    fn malformed_header_json_degrades_to_empty_rather_than_failing_a_replay() {
        assert!(parse_headers(Some("{not json".to_owned())).is_empty());
    }

    #[test]
    fn uncaptured_routing_keys_store_as_null() {
        assert_eq!(opt_str(""), None);
        assert_eq!(opt_str("acme"), Some("acme"));
    }
}
