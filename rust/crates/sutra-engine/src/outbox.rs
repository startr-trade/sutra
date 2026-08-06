//! The outbox row-access bridge — `sutra_channels::OutboxRowStore` implemented
//! over `sutra-persistence`'s `PgOutboxStore` (SKIP-LOCKED claim, delete-on-delivery,
//! defer-with-diagnostic), keeping `sutra-channels` persistence-free (the
//! [`crate::bridge`] pattern applied to delivery).
//!
//! Plus [`PgOutboxIncidentSink`], the delivery-side twin of the [`crate::bridge`]
//! `IncidentSink`: where a poisoned `<q:send required>` entry's durable incident lands.

use std::str::FromStr;

use sqlx::PgPool;
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;

use sutra_channels::codes as channel_codes;
use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::BoxFuture;
use sutra_channels::stores::{InboundIncident, IncidentSink};
use sutra_channels::{ClaimedOutboxRow, OutboxRowStore};
use sutra_persistence::stores::{
    DeadLetterRow, OutboxEntry, OutboxStore, PgDeadLetterStore, PgOutboxStore, ReplyMode,
};
use sutra_persistence::DeploymentId as PersistDeploymentId;

/// PG-backed [`OutboxRowStore`].
pub struct PgOutboxRows {
    store: PgOutboxStore,
}

impl PgOutboxRows {
    pub fn new(pool: PgPool) -> PgOutboxRows {
        PgOutboxRows {
            store: PgOutboxStore::new(pool),
        }
    }
}

fn persist_dep(
    deployment: &sutra_executor::DeploymentId,
) -> Result<PersistDeploymentId, Diagnostic> {
    PersistDeploymentId::new(deployment.value()).map_err(|e| {
        Diagnostic::error(
            channel_codes::RUNTIME_UNEXPECTED,
            format!("deployment id failed persistence-form validation: {e}"),
        )
    })
}

fn parse_entry_id(entry_id: &str) -> Result<Uuid, Diagnostic> {
    Uuid::from_str(entry_id).map_err(|e| {
        Diagnostic::error(
            channel_codes::RUNTIME_UNEXPECTED,
            format!("outbox entry id '{entry_id}' is not a UUID: {e}"),
        )
    })
}

fn store_diag(context: &str, e: sutra_persistence::PersistenceError) -> Diagnostic {
    Diagnostic::error(channel_codes::RUNTIME_UNEXPECTED, format!("{context}: {e}"))
}

/// Persistence row → the dispatcher's transport-neutral claimed shape.
fn to_claimed(entry: OutboxEntry) -> ClaimedOutboxRow {
    ClaimedOutboxRow {
        entry_id: entry.entry_id.to_string(),
        attempt_count: entry.attempt_count,
        destination: entry.destination,
        headers: entry.headers,
        // Boundary exit: the claimed row carries raw bytes onto the delivery leg. `into_inner()`
        // marks the deliberate unwrap of the persisted `Sensitive` body at the persistence→engine
        // hand-off (the downstream ClaimedOutboxRow/OutboundMessage sweep is a separate follow-up).
        body: entry.body.into_inner(),
        content_type: entry.content_type,
        outbox_key: entry.outbox_key,
        mode: match entry.mode {
            ReplyMode::Native => sutra_bpmn::qbindings::ReplyMode::Native,
            ReplyMode::CloudEventBinary => sutra_bpmn::qbindings::ReplyMode::CloudeventBinary,
            ReplyMode::CloudEventStructured => {
                sutra_bpmn::qbindings::ReplyMode::CloudeventStructured
            }
            ReplyMode::MatchInbound => sutra_bpmn::qbindings::ReplyMode::MatchInbound,
        },
        cloud_event_json: entry.cloud_event_json,
        auth_ref_json: entry.auth_ref_json,
        labels: entry.labels,
        traceparent: entry.traceparent,
        instance_id: entry.instance_id.to_string(),
        // The emitting node (V606) — what routes a terminal poison back to a parked
        // channel-call task's <q:retry> policy.
        node_id: entry.node_id,
        // `required` (persisted since V601) and the last diagnostic now reach the dispatcher: the
        // first says a poisoned delivery must surface as an incident, the second carries the
        // once-only marker that keeps it to ONE incident per entry.
        required: entry.required,
        last_diagnostic_json: entry.last_diagnostic_json,
    }
}

impl OutboxRowStore for PgOutboxRows {
    fn claim_due<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let claimed = self
                .store
                .claim_due(&dep, now, max_entries)
                .await
                .map_err(|e| store_diag("outbox claimDue failed", e))?;
            Ok(claimed.into_iter().map(to_claimed).collect())
        })
    }

    fn delete<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        entry_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let id = parse_entry_id(entry_id)?;
            self.store
                .delete(&dep, id)
                .await
                .map_err(|e| store_diag("outbox delete failed", e))
        })
    }

    fn defer<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        entry_id: &'a str,
        new_due_at: OffsetDateTime,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let id = parse_entry_id(entry_id)?;
            self.store
                .defer(&dep, id, new_due_at, Some(diagnostic_json))
                .await
                .map_err(|e| store_diag("outbox defer failed", e))
        })
    }

    fn mark_poisoned<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        entry_id: &'a str,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let id = parse_entry_id(entry_id)?;
            self.store
                .mark_poisoned(&dep, id, Some(diagnostic_json))
                .await
                .map_err(|e| store_diag("outbox markPoisoned failed", e))
        })
    }
}

/// The durable [`IncidentSink`] for the OUTBOX dispatcher — where a poisoned `<q:send required>`
/// entry's incident lands in `dead_letter`.
///
/// Historically this existed because the [`crate::bridge::PersistenceBridge`]'s sink drove
/// its writes with `Handle::block_on` — a panic if entered from a runtime task like the
/// outbox dispatcher. That constraint is gone (the seam is async end to end since the
/// Phase 3 conversion; the insert is simply awaited on the dispatcher's own task), and the
/// sink is kept as the dispatcher's dedicated, dependency-light sink: it needs a pool and
/// nothing else of the engine bridge. Best-effort exactly as before — the dispatcher's
/// `tracing::error!` floor has already fired unconditionally, so a failed write costs
/// visibility, never correctness.
pub struct PgOutboxIncidentSink {
    pool: PgPool,
}

impl PgOutboxIncidentSink {
    pub fn new(pool: PgPool) -> PgOutboxIncidentSink {
        PgOutboxIncidentSink { pool }
    }
}

#[async_trait::async_trait]
impl IncidentSink for PgOutboxIncidentSink {
    async fn record(&self, incident: InboundIncident) {
        let Ok(dep) = PersistDeploymentId::new(&incident.deployment) else {
            warn!(
                deployment = %incident.deployment,
                "required-delivery incident carries an invalid deployment id — not durably \
                 recorded (the tracing::error! floor already fired)"
            );
            return;
        };
        let received_at = OffsetDateTime::parse(
            &incident.received_at,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap_or_else(|_| OffsetDateTime::now_utc());
        let row = DeadLetterRow {
            deployment: dep,
            channel: incident.channel,
            process_id: incident.process_id,
            dedup_key: incident.dedup_key,
            failure_code: incident.failure_code,
            detail: incident.detail,
            received_at,
            // An outbound incident captures no payload: there is no inbound message, and the entry
            // itself is still in `outbox_entry`, parked at the poison horizon and redrivable there.
            payload: None,
            headers: std::collections::BTreeMap::new(),
            content_type: None,
            tenant: String::new(),
            module_key: String::new(),
        };
        let store = PgDeadLetterStore::new(self.pool.clone());
        if let Err(e) = store.insert(&row).await {
            warn!(error = %e, "durable required-delivery incident write failed (best-effort)");
        }
    }
}
