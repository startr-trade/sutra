//! The outbox delivery spine — the outbox dispatcher, its retry policy,
//! and the dispatch scheduler on the transport seams.
//!
//! Loop shape: claim one due batch per deployment → resolve the
//! [`MessageSink`](crate::sink::MessageSink) by the destination's URI scheme → send →
//! `Delivered` ⇒ delete the row / `RetryableFailure` ⇒ defer with exponential backoff /
//! `PermanentFailure` (and an unresolvable scheme) ⇒ **poison isolation**: the row is
//! deferred at the retry policy's MAX delay so it neither hot-loops nor blocks the batch,
//! stays visible (diagnostic attached) and stays redrivable — at-least-once is never
//! traded for silence. Every entry is isolated: one poison entry never stalls the rest.
//!
//! This crate stays persistence-free ([`crate::bridge`] pattern): the dispatcher owns
//! rows only through the small [`OutboxRowStore`] seam (`sutra-engine` implements it over
//! `sutra-persistence`'s `PgOutboxStore`); sinks see only the transport-neutral
//! [`OutboundMessage`].
//!
//! Tick loop: a tokio interval task ([`spawn_dispatch_loop`]) walking the known
//! deployments — config `sutra.outbox.tick-interval`, default PT5S, missed ticks skipped
//! (the `@Scheduled(concurrentExecution = SKIP)` posture) — with a drain-aware hook
//! ([`OutboxDispatcherHandle::drain`]) that refuses subsequent ticks and lets the
//! in-flight batch finish (the `Drainable` posture of `OutboxDispatchScheduler`).

use std::collections::BTreeMap;
use std::sync::Arc;

use time::OffsetDateTime;
use tracing::{info, warn};

use sutra_bpmn::qbindings::ReplyMode;
use sutra_executor::DeploymentId;

use crate::capacity::{
    CapacityPoolStore, SlotAcquisition, SlotDirection, SlotRequest, CAPACITY_CALL_HEADER,
    CAPACITY_CALL_KEY_HEADER, CAPACITY_POOL_HEADER,
};
use crate::codes;
use crate::diag::Diagnostic;
use crate::sink::{BoxFuture, OutboundMessage, SinkRegistry};
use crate::stores::{InboundIncident, IncidentSink};
use crate::SendOutcome;

// ---- retry policy (exact backoff arithmetic) -----------------------------------------------

/// A [`Diagnostic`] attribute a sink sets on a `RetryableFailure` to bound how many delivery
/// attempts that kind of failure is retried for (the value is the attempt count, as a decimal).
/// Once a row's attempt number reaches it, the failure is treated as permanent: the row takes the
/// poison posture (deferred to the policy's max delay, kept, visible, redriven), exactly as a
/// `PermanentFailure` would. Absent, a retryable failure follows the ordinary backoff.
pub const BOUNDED_RETRY_ATTR: &str = "sutra.boundedRetryAttempts";

/// Whether the attempt just made (number `attempt_count + 1`) used up the bound a sink set
/// through [`BOUNDED_RETRY_ATTR`]. An unparsable or non-positive bound is ignored.
fn bounded_retry_spent(diagnostic: &Diagnostic, attempt_count: i32) -> bool {
    diagnostic
        .attributes
        .get(BOUNDED_RETRY_ATTR)
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|bound| *bound >= 1)
        .is_some_and(|bound| attempt_count.saturating_add(1) >= bound)
}

/// Exponential backoff with optional full jitter — the canonical replica-friendly retry
/// curve (`now + min(base * 2^attempt, max)`, uniform `[0, computed)` jitter when
/// enabled) so concurrent dispatchers don't synchronise their retry waves. The default
/// outbox retry config: base `PT1S`, max `PT5M`, jitter on.
#[derive(Clone)]
pub struct RetryPolicy {
    base_delay_ms: i64,
    max_delay_ms: i64,
    jitter: bool,
    /// `sutra.outbox.retry.max-attempts` — total delivery attempts before an entry is marked
    /// TERMINAL. `None` (the DEFAULT) is the historical retry-forever posture, kept because
    /// at-least-once is never silently traded away: absent this key, nothing about the dispatcher
    /// changes. `Some(n)` bounds it — see [`OutboxDispatcher::attempts_exhausted`].
    max_attempts: Option<i32>,
    /// `(bound_exclusive) -> next long in [0, bound_exclusive)` — production samples OS
    /// entropy; tests inject a deterministic sampler for exact backoff assertions.
    sampler: Arc<dyn Fn(i64) -> i64 + Send + Sync>,
}

impl RetryPolicy {
    /// Panics on non-positive `base_delay` or `max_delay < base_delay` (wiring-time
    /// programming errors, per the constructor contract).
    pub fn new(
        base_delay: std::time::Duration,
        max_delay: std::time::Duration,
        jitter: bool,
    ) -> RetryPolicy {
        let base_delay_ms = base_delay.as_millis() as i64;
        let max_delay_ms = max_delay.as_millis() as i64;
        assert!(base_delay_ms > 0, "baseDelay must be positive");
        assert!(
            max_delay_ms >= base_delay_ms,
            "maxDelay must be >= baseDelay"
        );
        RetryPolicy {
            base_delay_ms,
            max_delay_ms,
            jitter,
            max_attempts: None,
            sampler: Arc::new(entropy_sample),
        }
    }

    /// Bound the total delivery attempts (`sutra.outbox.retry.max-attempts`). Values below 1 are
    /// treated as "no ceiling" rather than "give up before trying": a misconfigured 0 must not
    /// silently stop the outbox delivering anything at all.
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: Option<i32>) -> RetryPolicy {
        self.max_attempts = max_attempts.filter(|n| *n >= 1);
        self
    }

    /// The configured attempt ceiling, if any.
    pub fn max_attempts(&self) -> Option<i32> {
        self.max_attempts
    }

    /// Jittered exponential backoff — the production configuration.
    pub fn exponential(
        base_delay: std::time::Duration,
        max_delay: std::time::Duration,
    ) -> RetryPolicy {
        RetryPolicy::new(base_delay, max_delay, true)
    }

    /// Test seam: inject a deterministic jitter sampler (e.g. `|bound| bound / 2`) so
    /// backoff assertions are reproducible.
    pub fn with_sampler(
        mut self,
        sampler: impl Fn(i64) -> i64 + Send + Sync + 'static,
    ) -> RetryPolicy {
        self.sampler = Arc::new(sampler);
        self
    }

    /// The instant the entry becomes claimable again after its `attempt_count`-th failed
    /// attempt — the exact next-attempt arithmetic (shift clamp at 30,
    /// overflow → max, jitter over the ceiling, non-positive → base).
    pub fn next_attempt(&self, now: OffsetDateTime, attempt_count: i32) -> OffsetDateTime {
        let base_ms = self.base_delay_ms;
        let max_ms = self.max_delay_ms;
        let shift = attempt_count.clamp(0, 30) as u32;
        let unbounded = base_ms.checked_shl(shift).unwrap_or(-1);
        let ceiling = if shift >= 30 || unbounded < 0 {
            max_ms
        } else {
            unbounded.min(max_ms)
        };
        let mut delay = if self.jitter {
            self.next_long(ceiling)
        } else {
            ceiling
        };
        if delay <= 0 {
            delay = base_ms;
        }
        now + time::Duration::milliseconds(delay)
    }

    /// The poison-isolation deferral: the fixed MAX delay (no growth, no jitter). Used
    /// for `PermanentFailure` / unresolvable-scheme rows so they park at the policy's
    /// horizon instead of hot-looping — visible, redrivable, never blocking the batch.
    pub fn poison_delay(&self) -> time::Duration {
        time::Duration::milliseconds(self.max_delay_ms)
    }

    fn next_long(&self, bound_exclusive: i64) -> i64 {
        if bound_exclusive <= 1 {
            return bound_exclusive;
        }
        (self.sampler)(bound_exclusive)
    }
}

impl Default for RetryPolicy {
    /// The outbox retry defaults: base PT1S, max PT5M, jitter on.
    fn default() -> RetryPolicy {
        RetryPolicy::exponential(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(300),
        )
    }
}

/// Uniform `[0, bound)` from OS entropy — anti-synchronisation across replicas, NOT a
/// security primitive (never crosses the wire), so modulo bias is acceptable.
fn entropy_sample(bound_exclusive: i64) -> i64 {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes).expect("OS entropy source");
    (u64::from_le_bytes(bytes) % bound_exclusive as u64) as i64
}

// ---- the row-access seam (sutra-engine implements over sutra-persistence) ------------------

/// One claimed outbox row, as the dispatcher consumes it — the persistence adapter maps
/// its row type onto this transport-neutral shape (the outbox-entry
/// analog, flattened; entry identity is an opaque string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedOutboxRow {
    /// Opaque row id (UUID string on the durable store) — distinct from `outbox_key`.
    pub entry_id: String,
    /// Failed delivery attempts so far (pre-claim).
    pub attempt_count: i32,
    /// Scheme-bearing destination URI.
    pub destination: String,
    /// Reply transport headers persisted at enqueue.
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    /// Consumer idempotency key — MUST reach the wire (`Idempotency-Key` on HTTP).
    pub outbox_key: String,
    /// Wire-rendering mode (`MatchInbound` renders native at delivery).
    pub mode: ReplyMode,
    /// Opaque CloudEvents JSON persisted at enqueue (see [`crate::bridge`] encode side).
    pub cloud_event_json: Option<String>,
    /// Opaque auth-reference JSON (`{"scheme","secretRef","header"?}`).
    pub auth_ref_json: Option<String>,
    /// The emitting deployment's authoring labels (payload data — `ce-source` fallback).
    pub labels: BTreeMap<String, String>,
    /// W3C traceparent persisted at enqueue (trace-context bridge).
    pub traceparent: Option<String>,
    /// The originating process instance (UUID string) — carried so a poisoned `required`
    /// delivery's incident can name the flow it belongs to.
    pub instance_id: String,
    /// The BPMN node that emitted this row (V606); `None` on rows enqueued before the column
    /// existed. What lets terminal poison WAKE a parked channel-call task's `<q:retry>`
    /// policy (the poison notifier below fires only for node-bearing rows).
    pub node_id: Option<String>,
    /// `<q:send required>` / `<q:reply required>`: the author declared that a delivery failure
    /// must SURFACE as an incident rather than sit silently at the poison horizon. Persisted since
    /// V601 and, until the incident path below, never read by the dispatcher.
    pub required: bool,
    /// The diagnostic JSON of this row's last failed attempt (`last_diagnostic_json`), as claimed.
    /// The dispatcher reads it for one thing only: the [`INCIDENT_RECORDED_ATTR`] marker that makes
    /// the `required` incident once-only.
    pub last_diagnostic_json: Option<String>,
}

/// Row access as the dispatcher needs it — the persistence-free seam (`sutra-engine`
/// bridges it onto `sutra-persistence`'s `PgOutboxStore`, whose claim uses
/// `FOR UPDATE SKIP LOCKED` so concurrent replicas never compete for the same rows).
pub trait OutboxRowStore: Send + Sync {
    /// Claims up to `max_entries` rows due at `now` for `deployment`.
    fn claim_due<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
    ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>>;

    /// Deletes an entry after successful delivery; a missing row is a no-op.
    fn delete<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>>;

    /// Schedules a retry: sets the due time, increments the attempt count, records the
    /// failure diagnostic (JSON) for ops visibility.
    fn defer<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_id: &'a str,
        new_due_at: OffsetDateTime,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>>;

    /// Marks an entry TERMINAL — it exhausted `sutra.outbox.retry.max-attempts` and must never be
    /// claimed again — recording the diagnostic that ended it. Deliberately NOT a delete: the
    /// payload stays inspectable and redrivable, which is what keeps "we gave up" from becoming
    /// "it silently vanished". Unlike [`Self::defer`] it neither moves the due time nor increments
    /// the attempt count; the final count is the honest record of how many deliveries were tried.
    fn mark_poisoned<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_id: &'a str,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>>;

    /// Puts an entry back WITHOUT counting a delivery attempt: sets the due time and records the
    /// diagnostic, leaving the attempt count alone. Used while a pooled request waits for a slot
    /// — nothing was sent, so nothing was attempted.
    fn requeue<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_id: &'a str,
        new_due_at: OffsetDateTime,
        diagnostic_json: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>>;

    /// [`Self::claim_due`] that skips the entries named in `exclude` — the rows this process
    /// already has in flight, which the concurrent dispatcher must not hand out twice. The
    /// default claims enough extra rows to cover the exclusions and filters them out; a store
    /// that can exclude in its query should.
    fn claim_due_excluding<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        now: OffsetDateTime,
        max_entries: i64,
        exclude: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
        Box::pin(async move {
            let claimed = self
                .claim_due(deployment, now, max_entries + exclude.len() as i64)
                .await?;
            Ok(claimed
                .into_iter()
                .filter(|row| !exclude.contains(&row.entry_id))
                .take(max_entries.max(0) as usize)
                .collect())
        })
    }

    /// Deletes several delivered entries at once — one transaction, so a burst of deliveries
    /// pays one durable commit instead of one each. The default deletes them one by one.
    fn delete_many<'a>(
        &'a self,
        deployment: &'a DeploymentId,
        entry_ids: &'a [String],
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            for entry_id in entry_ids {
                self.delete(deployment, entry_id).await?;
            }
            Ok(())
        })
    }
}

// ---- wire-shape rendering (HTTP sink mode branches — frozen strings) -----------------------

/// CloudEvents structured content type (HTTP binding spec).
pub const CE_CONTENT_TYPE_STRUCTURED: &str = "application/cloudevents+json";
/// Default CE `type` when the emission carried no CloudEvent view. Engine-emitted
/// CloudEvents use the structured `sutra.<area>.<event>` namespace, leaving room for
/// future sibling types (e.g. other `sutra.channel.*`).
const CE_DEFAULT_TYPE: &str = "sutra.channel.reply";
/// The pre-rekey unresolved-tenant literal (`OutboundReply.tenantLabel()` fallback).
const UNRESOLVED_TENANT: &str = "__unresolved__";
const HEADER_CONTENT_TYPE: &str = "Content-Type";

/// Render a claimed row to the transport-neutral wire shape the sinks consume: the
/// reply-mode branches of the HTTP sink's request build (NATIVE /
/// CE-binary `ce-*` headers / CE-structured JSON envelope — wire strings frozen), plus
/// dispatcher-side auth-header resolution (the sink never
/// sees the secret reference, only the resolved material riding `headers`).
pub fn encode_wire_message(
    row: &ClaimedOutboxRow,
    now_rfc3339: &str,
    secret_env: &dyn Fn(&str) -> Option<String>,
) -> OutboundMessage {
    let ce = row.cloud_event_json.as_deref().and_then(parse_cloud_event);
    let mut headers = row.headers.clone();

    let (body, content_type) = match row.mode {
        ReplyMode::CloudeventStructured => (
            render_structured_envelope(row, ce.as_ref(), now_rfc3339),
            Some(CE_CONTENT_TYPE_STRUCTURED.to_string()),
        ),
        ReplyMode::CloudeventBinary => {
            let binding = ce_binding_for(&row.destination);
            for (name, value) in ce_binary_headers(row, ce.as_ref(), now_rfc3339, binding) {
                headers.insert(name, value);
            }
            (row.body.clone(), resolve_content_type(row))
        }
        // NATIVE — and MATCH_INBOUND, which renders native at delivery (the sink's
        // `else` branch covers both).
        ReplyMode::Native | ReplyMode::MatchInbound => {
            (row.body.clone(), resolve_content_type(row))
        }
    };

    for (name, value) in resolve_auth_headers(row, secret_env) {
        headers.insert(name, value);
    }

    OutboundMessage {
        destination: row.destination.clone(),
        headers,
        body,
        content_type,
        outbox_key: row.outbox_key.clone(),
        traceparent: row.traceparent.clone(),
    }
}

/// Declared content type → a persisted `Content-Type` header (case-insensitive) → none
/// (the HTTP sink defaults to `application/octet-stream` on the wire).
fn resolve_content_type(row: &ClaimedOutboxRow) -> Option<String> {
    if let Some(declared) = &row.content_type {
        if !declared.trim().is_empty() {
            return Some(declared.clone());
        }
    }
    row.headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(HEADER_CONTENT_TYPE))
        .map(|(_, value)| value.clone())
}

/// The parsed opaque CloudEvents JSON (the enqueue side writes it via
/// [`crate::bridge::cloud_event_to_json`] — structured-mode attribute names).
struct CloudEventView {
    id: Option<String>,
    source: Option<String>,
    spec_version: Option<String>,
    ce_type: Option<String>,
    subject: Option<String>,
    time: Option<String>,
    data_content_type: Option<String>,
}

fn parse_cloud_event(json: &str) -> Option<CloudEventView> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let get = |key: &str| {
        value
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };
    Some(CloudEventView {
        id: get("id"),
        source: get("source"),
        spec_version: get("specversion"),
        ce_type: get("type"),
        subject: get("subject"),
        time: get("time"),
        data_content_type: get("datacontenttype"),
    })
}

fn tenant_label(row: &ClaimedOutboxRow) -> String {
    row.labels
        .get("tenant")
        .cloned()
        .unwrap_or_else(|| UNRESOLVED_TENANT.to_string())
}

/// The per-binding CloudEvents binary attribute projection — the frozen m9 matrix, one
/// documented and distinct prefix per broker family (the contract is the requirement,
/// R12): the AMQP 0.9.1 binding (rabbitmq/amqp
/// destinations) uses `cloudEvents:<attr>` message headers (and, per the AMQP binding,
/// also carries `datacontenttype`); the Kafka protocol binding (`kafka://` destinations)
/// uses the `ce_<attr>` record-header prefix (UNDERSCORE — Kafka header keys forbid the
/// HTTP dash form, and Kafka does NOT lift `datacontenttype`); every other destination
/// here is the HTTP binary binding's `ce-<attr>` headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CeBinding {
    /// HTTP binary binding — `ce-<attr>` headers.
    Http,
    /// AMQP 0.9.1 binding — `cloudEvents:<attr>` message headers.
    Amqp,
    /// Kafka protocol binding — `ce_<attr>` record headers (underscore form).
    Kafka,
    /// AWS SQS binding — `ce-<attr>` message attributes (DASH; SQS attribute names allow
    /// the HTTP dash form). Same prefix as HTTP, a distinct variant for the per-broker
    /// matrix (G2a).
    Sqs,
    /// GCP Pub/Sub binding — `ce-<attr>` message attributes (DASH form; Pub/Sub attribute
    /// keys allow the HTTP dash, and Pub/Sub does NOT lift `datacontenttype`).
    GcpPubsub,
    /// AMQP 1.0 binding (`amqp10://` — Artemis / Azure SB / Solace / Qpid) — `ce-<attr>`
    /// application properties (DASH form; native AMQP 1.0 application-property keys are
    /// unrestricted, so the HTTP dash spelling carries verbatim). Unlike the AMQP 0.9.1
    /// binding it does NOT lift `datacontenttype`.
    Amqp10,
}

impl CeBinding {
    fn prefix(self) -> &'static str {
        match self {
            CeBinding::Http => "ce-",
            CeBinding::Amqp => "cloudEvents:",
            CeBinding::Kafka => "ce_",
            CeBinding::Sqs => "ce-",
            CeBinding::GcpPubsub => "ce-",
            CeBinding::Amqp10 => "ce-",
        }
    }
}

/// The CE binding for a destination, keyed on its URI scheme (the projection happens
/// dispatcher-side in the Rust shape; the broker sink carries headers verbatim).
fn ce_binding_for(destination: &str) -> CeBinding {
    match crate::sink::scheme_of(destination) {
        Some(scheme)
            if scheme.eq_ignore_ascii_case("rabbitmq") || scheme.eq_ignore_ascii_case("amqp") =>
        {
            CeBinding::Amqp
        }
        Some(scheme) if scheme.eq_ignore_ascii_case("kafka") => CeBinding::Kafka,
        Some(scheme) if scheme.eq_ignore_ascii_case("aws-sqs") => CeBinding::Sqs,
        Some(scheme) if scheme.eq_ignore_ascii_case("gcp-pubsub") => CeBinding::GcpPubsub,
        Some(scheme)
            if scheme.eq_ignore_ascii_case("amqp10") || scheme.eq_ignore_ascii_case("amqp10s") =>
        {
            CeBinding::Amqp10
        }
        _ => CeBinding::Http,
    }
}

/// The CE-binary headers (the HTTP and RabbitMQ sinks' CE lift — exact wire
/// names/values, prefixed per binding).
fn ce_binary_headers(
    row: &ClaimedOutboxRow,
    ce: Option<&CloudEventView>,
    now_rfc3339: &str,
    binding: CeBinding,
) -> Vec<(String, String)> {
    let p = binding.prefix();
    let mut out = Vec::new();
    match ce {
        None => {
            out.push((format!("{p}id"), row.outbox_key.clone()));
            out.push((format!("{p}source"), format!("/{}", tenant_label(row))));
            out.push((format!("{p}specversion"), "1.0".to_string()));
            out.push((format!("{p}type"), CE_DEFAULT_TYPE.to_string()));
            out.push((format!("{p}time"), now_rfc3339.to_string()));
        }
        Some(ce) => {
            out.push((
                format!("{p}id"),
                ce.id.clone().unwrap_or_else(|| row.outbox_key.clone()),
            ));
            out.push((
                format!("{p}source"),
                ce.source
                    .clone()
                    .unwrap_or_else(|| format!("/{}", tenant_label(row))),
            ));
            out.push((
                format!("{p}specversion"),
                ce.spec_version.clone().unwrap_or_else(|| "1.0".to_string()),
            ));
            out.push((
                format!("{p}type"),
                ce.ce_type
                    .clone()
                    .unwrap_or_else(|| CE_DEFAULT_TYPE.to_string()),
            ));
            if let Some(subject) = &ce.subject {
                out.push((format!("{p}subject"), subject.clone()));
            }
            if let Some(time) = &ce.time {
                out.push((format!("{p}time"), time.clone()));
            }
            if binding == CeBinding::Amqp {
                // The AMQP binding also lifts datacontenttype (the RabbitMQ sink lift).
                if let Some(ct) = &ce.data_content_type {
                    out.push((format!("{p}datacontenttype"), ct.clone()));
                }
            }
        }
    }
    out
}

/// The CE-structured JSON envelope (exact key names;
/// binary payloads land base64-encoded under `data_base64` per the CE JSON format spec,
/// textual ones inline under `data`).
fn render_structured_envelope(
    row: &ClaimedOutboxRow,
    ce: Option<&CloudEventView>,
    now_rfc3339: &str,
) -> Vec<u8> {
    let mut envelope = serde_json::Map::new();
    let id = ce
        .and_then(|c| c.id.clone())
        .unwrap_or_else(|| row.outbox_key.clone());
    let source = ce
        .and_then(|c| c.source.clone())
        .unwrap_or_else(|| format!("/{}", tenant_label(row)));
    let spec_version = ce
        .and_then(|c| c.spec_version.clone())
        .unwrap_or_else(|| "1.0".to_string());
    let ce_type = ce
        .and_then(|c| c.ce_type.clone())
        .unwrap_or_else(|| CE_DEFAULT_TYPE.to_string());
    envelope.insert("id".to_string(), serde_json::Value::String(id));
    envelope.insert("source".to_string(), serde_json::Value::String(source));
    envelope.insert(
        "specversion".to_string(),
        serde_json::Value::String(spec_version),
    );
    envelope.insert("type".to_string(), serde_json::Value::String(ce_type));
    match ce {
        Some(ce) => {
            if let Some(subject) = &ce.subject {
                envelope.insert(
                    "subject".to_string(),
                    serde_json::Value::String(subject.clone()),
                );
            }
            if let Some(time) = &ce.time {
                envelope.insert("time".to_string(), serde_json::Value::String(time.clone()));
            }
            if let Some(ct) = &ce.data_content_type {
                envelope.insert(
                    "datacontenttype".to_string(),
                    serde_json::Value::String(ct.clone()),
                );
            }
        }
        None => {
            envelope.insert(
                "time".to_string(),
                serde_json::Value::String(now_rfc3339.to_string()),
            );
            if let Some(ct) = &row.content_type {
                if !ct.trim().is_empty() {
                    envelope.insert(
                        "datacontenttype".to_string(),
                        serde_json::Value::String(ct.clone()),
                    );
                }
            }
        }
    }
    if !row.body.is_empty() {
        let data_ct = ce
            .and_then(|c| c.data_content_type.clone())
            .or_else(|| row.content_type.clone());
        if looks_textual(data_ct.as_deref()) {
            envelope.insert(
                "data".to_string(),
                serde_json::Value::String(String::from_utf8_lossy(&row.body).into_owned()),
            );
        } else {
            envelope.insert(
                "data_base64".to_string(),
                serde_json::Value::String(base64_encode(&row.body)),
            );
        }
    }
    serde_json::to_vec(&serde_json::Value::Object(envelope)).unwrap_or_default()
}

/// The textual-payload heuristic.
fn looks_textual(content_type: Option<&str>) -> bool {
    let Some(ct) = content_type else {
        return false;
    };
    if ct.trim().is_empty() {
        return false;
    }
    let lower = ct.to_ascii_lowercase();
    lower.starts_with("text/")
        || lower.contains("json")
        || lower.contains("xml")
        || lower.contains("yaml")
        || lower.contains("yml")
}

/// Standard-alphabet base64 with padding (encode-only — the CE `data_base64` field).
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize & 0x3f] as char);
        out.push(ALPHABET[(triple >> 12) as usize & 0x3f] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(triple >> 6) as usize & 0x3f] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[triple as usize & 0x3f] as char
        } else {
            '='
        });
    }
    out
}

/// Dispatcher-side auth-header resolution (the auth-header
/// contract, moved dispatcher-side so sinks never see secret refs).
/// `bearer` → `Authorization: Bearer <material>`; `apikey` → the declared header (default
/// `X-API-Key`); `mtls` and unresolvable references warn and skip (the upstream answers
/// 401/403 rather than receiving a malformed header) — never a hard failure.
fn resolve_auth_headers(
    row: &ClaimedOutboxRow,
    secret_env: &dyn Fn(&str) -> Option<String>,
) -> Vec<(String, String)> {
    let Some(json) = row.auth_ref_json.as_deref() else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        warn!(
            entry_id = %row.entry_id,
            "outbox entry carries unparsable auth-ref JSON — skipping auth header injection"
        );
        return Vec::new();
    };
    let scheme = value.get("scheme").and_then(|v| v.as_str()).unwrap_or("");
    let secret_ref = value
        .get("secretRef")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if scheme == "mtls" {
        warn!(
            code = codes::OUTBOUND_HTTP_MTLS_UNSUPPORTED,
            destination = %row.destination,
            secret_ref,
            "mTLS auth-ref present but the outbox dispatcher does not yet wire a custom \
             TLS identity — skipping auth"
        );
        return Vec::new();
    }
    // Pass the FULL secret-ref (scheme and all) to the injected resolver so any scheme
    // (`env:`/`secret:`/`vault:`/`aws-secrets:`) resolves through the engine's shared envref
    // registry. The DEFAULT (uninjected) resolver stays `env:`-only for back-compat — it strips
    // its own `env:` prefix (see `OutboxDispatcher::new`).
    let material = secret_env(secret_ref);
    let Some(material) = material else {
        warn!(
            code = codes::OUTBOUND_HTTP_AUTH_REF_UNRESOLVED,
            destination = %row.destination,
            secret_ref,
            "auth-ref could not be resolved — skipping auth header injection"
        );
        return Vec::new();
    };
    // The shared auth helper owns the scheme→header mapping (bearer → `authorization:
    // Bearer <material>`, apikey → the configured header) so every transport agrees.
    let header = value.get("header").and_then(|v| v.as_str()).unwrap_or("");
    crate::auth::outbound_auth_headers(scheme, &material, header)
}

// ---- the dispatcher (per-entry isolation) --------------------------------------------------

/// Secret resolution for an outbound auth reference: given the FULL secret-ref (`env:NAME`,
/// `secret:KEY`, `vault:…`, …) return the resolved material, or `None` when the ref does not
/// resolve. The default resolves `env:NAME` only (`std::env::var`); the engine injects an
/// envref-backed resolver ([`OutboxDispatcher::with_secret_env`]) that resolves every scheme.
pub type SecretEnv = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// One drain batch's summary (the dispatch result).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DispatchStats {
    pub attempted: u32,
    pub succeeded: u32,
    pub failed: u32,
    /// Entries for a pooled outbound channel put back to wait for a slot (no attempt counted).
    pub waiting: u32,
}

/// What one claimed entry came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowOutcome {
    Delivered,
    Failed,
    /// A pooled request had no slot: put back, no attempt counted.
    Waiting,
}

/// How many extra claims one tick may make past entries that went back to wait for a pool
/// slot — waiting entries are claimed first (they are the oldest), so without these a full
/// batch of them would hold every other entry of the deployment back until a slot frees.
const MAX_CLAIMS_PAST_WAITING: u32 = 8;

/// Drains [`OutboxRowStore`] rows through the scheme-resolved sinks. Stateless across
/// calls (multi-replica safe — the store's SKIP-LOCKED claim is the coordination);
/// per-entry isolation: no sink outcome or row-action failure ever propagates out of
/// [`OutboxDispatcher::dispatch_deployment`].
pub struct OutboxDispatcher {
    store: Arc<dyn OutboxRowStore>,
    sinks: SinkRegistry,
    retry: RetryPolicy,
    batch_size: i64,
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    secret_env: SecretEnv,
    /// Where a poisoned `required` delivery's incident lands (the SAME durable seam the inbound
    /// dead-letter path uses). `None` ⇒ no durable sink configured; the `tracing::error!` floor in
    /// [`OutboxDispatcher::poison`] still fires, as everywhere else in this engine.
    incidents: Option<Arc<dyn IncidentSink + Send + Sync>>,
    /// Fired after a NODE-BEARING row goes TERMINAL (`mark_poisoned` committed) — the
    /// channel-call `<q:retry>` wake prompt. The assembly wires it to enqueue an
    /// `EngineRequest::FailChannelCall` on the engine actor (a fresh serialized turn, exactly
    /// like the timer poller — never a nested call). Best-effort by contract: the engine
    /// validates against durable facts and a LOST prompt is recovered by the call's
    /// `<q:timeout>` boundary, so this fires-and-forgets and never blocks the drain loop.
    poison_notify: Option<Arc<dyn Fn(PoisonedDelivery) + Send + Sync>>,
    /// The capacity pools a pooled outbound request takes its instance's slot from — the SAME
    /// store the dispatcher lanes admit through. `None` = no pool declared: a request carrying
    /// a pool marker is sent without one (and the marker is still stripped).
    capacity: Option<Arc<dyn CapacityPoolStore>>,
}

/// The call a pooled request's slot is held for: its declared call key, else the entry's id.
fn call_key(row: &ClaimedOutboxRow) -> String {
    row.headers
        .get(CAPACITY_CALL_KEY_HEADER)
        .cloned()
        .unwrap_or_else(|| row.entry_id.clone())
}

/// One terminal poison of a node-bearing outbox row, as handed to the poison notifier.
#[derive(Debug, Clone)]
pub struct PoisonedDelivery {
    pub deployment: DeploymentId,
    /// The originating instance (UUID string).
    pub instance_id: String,
    /// The emitting BPMN node (rows without one never notify).
    pub node_id: String,
}

impl OutboxDispatcher {
    /// Panics on `batch_size <= 0` (wiring-time programming error).
    pub fn new(
        store: Arc<dyn OutboxRowStore>,
        sinks: SinkRegistry,
        retry: RetryPolicy,
        batch_size: i64,
    ) -> OutboxDispatcher {
        assert!(batch_size > 0, "batchSize must be > 0");
        OutboxDispatcher {
            store,
            sinks,
            retry,
            batch_size,
            now: Arc::new(OffsetDateTime::now_utc),
            // Default (uninjected) resolver is `env:`-only for back-compat: it receives the FULL
            // secret-ref and resolves ONLY `env:NAME` through the process environment; every other
            // scheme is unresolved (→ auth header skipped, upstream 401). The engine injects an
            // envref-backed resolver via `with_secret_env` to resolve all schemes.
            secret_env: Arc::new(|full_ref| {
                full_ref
                    .strip_prefix("env:")
                    .and_then(|name| std::env::var(name).ok())
            }),
            incidents: None,
            poison_notify: None,
            capacity: None,
        }
    }

    /// Wire the capacity-pool store: an entry marked with a pool waits until its instance
    /// holds a slot of that pool.
    #[must_use]
    pub fn with_capacity_pools(mut self, store: Arc<dyn CapacityPoolStore>) -> OutboxDispatcher {
        self.capacity = Some(store);
        self
    }

    /// Wire the terminal-poison notifier — the channel-call `<q:retry>` wake prompt (see the
    /// field doc). Optional: without it a poisoned channel-call request simply waits for its
    /// `<q:timeout>` boundary to deliver the failure.
    #[must_use]
    pub fn with_poison_notifier(
        mut self,
        notify: impl Fn(PoisonedDelivery) + Send + Sync + 'static,
    ) -> OutboxDispatcher {
        self.poison_notify = Some(Arc::new(notify));
        self
    }

    /// Wire the durable incident sink a poisoned `required` delivery records into. Optional
    /// exactly like the inbound dead-letter sink (`sutra.incident.sql`): without it the
    /// `required` failure is still logged at error level, it just isn't durable.
    #[must_use]
    pub fn with_incident_sink(
        mut self,
        incidents: Arc<dyn IncidentSink + Send + Sync>,
    ) -> OutboxDispatcher {
        self.incidents = Some(incidents);
        self
    }

    /// Test seam: a fixed clock for deterministic due/backoff assertions.
    pub fn with_clock(
        mut self,
        now: impl Fn() -> OffsetDateTime + Send + Sync + 'static,
    ) -> OutboxDispatcher {
        self.now = Arc::new(now);
        self
    }

    /// Inject the secret resolver for outbound auth references. The engine wires this to its
    /// shared envref registry so the FULL secret-ref — any scheme (`env:`/`secret:`/`vault:`/
    /// `aws-secrets:`) — resolves at delivery; tests use it as a deterministic seam. Receives the
    /// whole ref (scheme included), NOT a bare `env:` NAME.
    pub fn with_secret_env(
        mut self,
        secret_env: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) -> OutboxDispatcher {
        self.secret_env = Arc::new(secret_env);
        self
    }

    /// Drain one batch of due entries for `deployment`. Safe to call repeatedly from the
    /// tick loop; never panics a delivery — failures defer/poison their own row only.
    ///
    /// Pooled entries that find no slot go back due one millisecond after this tick's claim
    /// instant, so they keep their place ahead of every entry enqueued later (the oldest waiter
    /// is offered the next free slot first), are not re-claimed within this tick, and are
    /// re-checked on the next one. When a whole batch went back to wait, the tick claims again
    /// at the same instant (bounded by [`MAX_CLAIMS_PAST_WAITING`]) so the rest of the
    /// deployment's entries still flow.
    pub async fn dispatch_deployment(&self, deployment: &DeploymentId) -> DispatchStats {
        let now = (self.now)();
        let mut stats = DispatchStats::default();
        // Pools found full this tick: their later entries wait without asking again, so a slot
        // freed mid-tick cannot go to a newer entry ahead of an older one.
        let mut exhausted: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for round in 0..=MAX_CLAIMS_PAST_WAITING {
            let claimed = match self.store.claim_due(deployment, now, self.batch_size).await {
                Ok(claimed) => claimed,
                Err(diagnostic) => {
                    warn!(
                        deployment = %deployment.value(),
                        code = %diagnostic.code,
                        error = %diagnostic.message,
                        "outbox claim failed — nothing more drained this tick"
                    );
                    return stats;
                }
            };
            let full_batch = claimed.len() as i64 >= self.batch_size;
            let mut waited = 0;
            stats.attempted += claimed.len() as u32;
            for row in &claimed {
                match self
                    .dispatch_one(deployment, row, now, &mut exhausted)
                    .await
                {
                    RowOutcome::Delivered => stats.succeeded += 1,
                    RowOutcome::Failed => stats.failed += 1,
                    RowOutcome::Waiting => {
                        stats.waiting += 1;
                        waited += 1;
                    }
                }
            }
            // Claim again only when this batch was full AND some of it went back to wait —
            // otherwise the next claim would return nothing new this tick.
            if !full_batch || waited == 0 || round == MAX_CLAIMS_PAST_WAITING {
                break;
            }
        }
        stats
    }

    /// One entry: pool slot → resolve → encode → send → row action.
    async fn dispatch_one(
        &self,
        deployment: &DeploymentId,
        row: &ClaimedOutboxRow,
        claimed_at: OffsetDateTime,
        exhausted: &mut std::collections::BTreeSet<String>,
    ) -> RowOutcome {
        if let Some(pool) = row.headers.get(CAPACITY_POOL_HEADER) {
            match self.admit_pooled(deployment, row, pool, exhausted).await {
                PoolAdmit::Send => {}
                PoolAdmit::Wait(diagnostic) => {
                    let json = diagnostic_json_marked(&diagnostic, incident_recorded(row));
                    let due = claimed_at + time::Duration::milliseconds(1);
                    if let Err(e) = self
                        .store
                        .requeue(deployment, &row.entry_id, due, &json)
                        .await
                    {
                        warn!(
                            entry_id = %row.entry_id,
                            code = %e.code,
                            error = %e.message,
                            "outbox requeue of a pool-waiting entry failed — it stays due"
                        );
                    }
                    return RowOutcome::Waiting;
                }
                PoolAdmit::Abandon(diagnostic) => {
                    warn!(
                        entry_id = %row.entry_id,
                        instance = %row.instance_id,
                        pool = %pool,
                        "pooled channel-call request abandoned: its instance finished while it \
                         waited for a slot (the entry is kept, terminal and redrivable)"
                    );
                    let json = diagnostic_json_marked(&diagnostic, incident_recorded(row));
                    if let Err(e) = self
                        .store
                        .mark_poisoned(deployment, &row.entry_id, &json)
                        .await
                    {
                        warn!(
                            entry_id = %row.entry_id,
                            code = %e.code,
                            error = %e.message,
                            "outbox terminal mark of an abandoned entry failed"
                        );
                    }
                    return RowOutcome::Failed;
                }
            }
        }
        if self.send_row(deployment, row).await {
            RowOutcome::Delivered
        } else {
            RowOutcome::Failed
        }
    }

    /// Take a slot of `pool` for the request's call before it is sent. One slot per active
    /// call: the call key is the request's declared `pool-call-key` header value, else the
    /// outbox entry's own id, so a request on a call its instance already holds a slot for sends
    /// at once. A store failure waits rather than risk oversubscribing the pool.
    async fn admit_pooled(
        &self,
        deployment: &DeploymentId,
        row: &ClaimedOutboxRow,
        pool: &str,
        exhausted: &mut std::collections::BTreeSet<String>,
    ) -> PoolAdmit {
        let Some(store) = &self.capacity else {
            return PoolAdmit::Send;
        };
        let waiting = |detail: String| {
            PoolAdmit::Wait(
                Diagnostic::error(codes::OUTBOUND_POOL_WAITING, detail)
                    .with_attribute("pool", pool),
            )
        };
        if exhausted.contains(pool) {
            // An ended session takes no slot, so a full pool is no reason for it to wait.
            if store
                .session_ended(deployment, &row.instance_id, &call_key(row))
                .await
            {
                return PoolAdmit::Send;
            }
            return waiting(format!(
                "waiting for a free slot of capacity pool '{pool}' (no attempt counted)"
            ));
        }
        let tenant = row
            .labels
            .get("tenant")
            .cloned()
            .unwrap_or_else(|| UNRESOLVED_TENANT.to_string());
        let request = SlotRequest {
            pool,
            deployment,
            tenant: &tenant,
            holder: &row.instance_id,
            call: &call_key(row),
            direction: SlotDirection::Outbound,
            require_live: row.headers.contains_key(CAPACITY_CALL_HEADER),
        };
        match store.acquire(&request).await {
            Ok(SlotAcquisition::Acquired | SlotAcquisition::AlreadyHeld) => PoolAdmit::Send,
            // The instance's session ended while the request waited (or before it was sent): it
            // is still sent, but an ended session holds no slot, so it takes none.
            Ok(SlotAcquisition::SessionEnded) => PoolAdmit::Send,
            Ok(SlotAcquisition::Exhausted {
                inbound_used,
                outbound_used,
            }) => {
                exhausted.insert(pool.to_string());
                waiting(format!(
                    "waiting for a free slot of capacity pool '{pool}' ({inbound_used} inbound + \
                     {outbound_used} outbound held; no attempt counted)"
                ))
            }
            Ok(SlotAcquisition::InstanceEnded) => PoolAdmit::Abandon(
                Diagnostic::error(
                    codes::OUTBOUND_POOL_ABANDONED,
                    format!(
                        "the request's instance {} finished while the request waited for a slot \
                         of capacity pool '{pool}'; it was not sent",
                        row.instance_id
                    ),
                )
                .with_attribute("pool", pool),
            ),
            Ok(SlotAcquisition::UnknownPool) => {
                warn!(
                    entry_id = %row.entry_id,
                    pool,
                    "outbox entry names a capacity pool the engine no longer declares — sent \
                     without a slot"
                );
                PoolAdmit::Send
            }
            Err(d) => waiting(format!(
                "capacity pool '{pool}' could not be read ({}: {}); waiting (no attempt counted)",
                d.code, d.message
            )),
        }
    }

    /// A pooled request was delivered, or given up on (poisoned): when its instance has already
    /// finished, nothing is left to hold the request's slot for, so it is returned now rather
    /// than at the next sweep. A request of a live instance keeps its slot — the call it opened
    /// lasts until it ends (a `session-end:` naming it) or the instance does.
    async fn release_finished_call(&self, deployment: &DeploymentId, row: &ClaimedOutboxRow) {
        let (Some(store), true) = (
            &self.capacity,
            row.headers.contains_key(CAPACITY_POOL_HEADER),
        ) else {
            return;
        };
        store
            .release_call_if_ended(deployment, &row.instance_id, &call_key(row))
            .await;
    }

    /// Resolve → encode → send → row action. `true` = delivered + deleted.
    async fn send_row(&self, deployment: &DeploymentId, row: &ClaimedOutboxRow) -> bool {
        if !self.deliver(deployment, row).await {
            return false;
        }
        self.finish_delivered(deployment, row).await
    }

    /// The delete half of a delivery: the row went out, so it is removed. A failed delete keeps
    /// the row, which redelivers — at-least-once, the consumer's idempotency key (`outbox_key`)
    /// absorbs the duplicate — and is deferred with backoff exactly like the catch-all.
    async fn finish_delivered(&self, deployment: &DeploymentId, row: &ClaimedOutboxRow) -> bool {
        match self.store.delete(deployment, &row.entry_id).await {
            Ok(()) => {
                self.release_finished_call(deployment, row).await;
                true
            }
            Err(diagnostic) => {
                warn!(
                    entry_id = %row.entry_id,
                    code = %diagnostic.code,
                    error = %diagnostic.message,
                    "outbox delete after delivery failed — deferring for redelivery"
                );
                self.defer_backoff(deployment, row, &diagnostic).await;
                false
            }
        }
    }

    /// Resolve → encode → send, and on failure the row action (defer or poison). `true` = the
    /// sink took the message; the row is NOT yet deleted — the caller deletes it, now
    /// ([`Self::send_row`]) or in a batch (the concurrent loop).
    async fn deliver(&self, deployment: &DeploymentId, row: &ClaimedOutboxRow) -> bool {
        let Some(sink) = self.sinks.resolve(&row.destination) else {
            // A retry can never grow a sink — the sink set is fixed at assembly, so an
            // unregistered scheme takes the poison posture (max-delay deferral) under the
            // OUTBOUND_SEND_FAILED diagnostic.
            let diagnostic = Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                format!(
                    "no MessageSink registered for the scheme of destination '{}' \
                     (registered schemes: {:?})",
                    row.destination,
                    self.sinks.schemes()
                ),
            );
            self.poison(deployment, row, &diagnostic).await;
            return false;
        };
        let now_rfc3339 = rfc3339((self.now)());
        let mut message = encode_wire_message(row, &now_rfc3339, &*self.secret_env);
        // The pool markers are engine bookkeeping, never wire headers.
        message.headers.remove(CAPACITY_POOL_HEADER);
        message.headers.remove(CAPACITY_CALL_HEADER);
        message.headers.remove(CAPACITY_CALL_KEY_HEADER);
        // The media-capture marker is for a sink that records the session it opens; any other
        // destination never sees it.
        if !sink.records_media() {
            message.headers.remove(crate::audit::AUDIT_CAPTURE_HEADER);
        }
        // The instance's audit routing is for a sink that writes audit events; it gets the
        // owning deployment and instance with it, every other sink none of the three.
        crate::transport_audit::stamp_journal_identity(
            &mut message.headers,
            sink.writes_audit_events(),
            deployment.value(),
            &row.instance_id,
        );
        // The PULL sink parks the delivery in a deployment-scoped table instead of dialing
        // anything, so it needs the owning deployment + instance — the two identities the
        // transport-neutral `OutboundMessage` deliberately does not carry onto a wire. Stamping
        // them as reserved headers is gated on the `pull` scheme, so no network transport can
        // ever observe them; the pull sink strips them before the row is parked.
        if crate::sink::scheme_of(&message.destination) == Some(crate::external_task::PULL_SCHEME) {
            message.headers.insert(
                crate::external_task::PARK_DEPLOYMENT_HEADER.to_string(),
                deployment.value().to_string(),
            );
            message.headers.insert(
                crate::external_task::PARK_INSTANCE_HEADER.to_string(),
                row.instance_id.clone(),
            );
        }
        match sink.send(&message).await {
            SendOutcome::Delivered => true,
            // A sink that bounds its own retries: past the bound, the row takes the poison
            // posture instead of growing the backoff further.
            SendOutcome::RetryableFailure(diagnostic)
                if bounded_retry_spent(&diagnostic, row.attempt_count) =>
            {
                self.poison(deployment, row, &diagnostic).await;
                false
            }
            SendOutcome::RetryableFailure(diagnostic) => {
                self.defer_backoff(deployment, row, &diagnostic).await;
                false
            }
            SendOutcome::PermanentFailure(diagnostic) => {
                self.poison(deployment, row, &diagnostic).await;
                false
            }
        }
    }

    /// Transient failure: defer to `now + backoff(attempt_count + 1)` with the diagnostic
    /// attached (the defer-on-failure step). A defer failure is logged and swallowed —
    /// the row stays due and the next claim cycle retries it.
    async fn defer_backoff(
        &self,
        deployment: &DeploymentId,
        row: &ClaimedOutboxRow,
        diagnostic: &Diagnostic,
    ) {
        if self.attempts_exhausted(row) {
            self.give_up(deployment, row, diagnostic).await;
            return;
        }
        let next_at = self.retry.next_attempt((self.now)(), row.attempt_count + 1);
        // Carry any existing incident marker forward: a transient failure AFTER a poison must not
        // erase the record that this entry's incident was already raised.
        self.defer_row(deployment, row, next_at, diagnostic, incident_recorded(row))
            .await;
    }

    /// True when this delivery attempt is the last the configured ceiling allows.
    /// `attempt_count` is the number of attempts that had already FAILED when the row was
    /// claimed, so the delivery just attempted is number `attempt_count + 1`.
    ///
    /// `None` (no ceiling configured) is always `false` — the retry-forever default is untouched.
    fn attempts_exhausted(&self, row: &ClaimedOutboxRow) -> bool {
        self.retry
            .max_attempts()
            .is_some_and(|max| row.attempt_count + 1 >= max)
    }

    /// The attempt ceiling was reached: stop scheduling this entry for good.
    ///
    /// Three things happen, in the order that keeps them honest under partial failure:
    /// 1. an unconditional `tracing::error!` — the observability floor, so exhaustion is never
    ///    silent even with no incident sink and no reachable database;
    /// 2. ONE durable incident, gated by the SAME `incidentRecorded` latch the `required`-delivery
    ///    path sets. That composition is the point: a `required` entry that already alerted when
    ///    it first poisoned does not alert again on the way to terminal, while a NON-required
    ///    entry — which never alerts on poison, by design — finally raises the single incident
    ///    that says a message will never be delivered;
    /// 3. the terminal mark itself, carrying the latch forward in `last_diagnostic_json`.
    ///
    /// A failing mark is logged and swallowed, exactly like a failing defer: the row simply stays
    /// claimable and the next tick re-reaches this decision with the same inputs.
    async fn give_up(
        &self,
        deployment: &DeploymentId,
        row: &ClaimedOutboxRow,
        diagnostic: &Diagnostic,
    ) {
        self.release_finished_call(deployment, row).await;
        let attempts = row.attempt_count + 1;
        tracing::error!(
            code = codes::OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED,
            entry_id = %row.entry_id,
            instance = %row.instance_id,
            destination = %row.destination,
            outbox_key = %row.outbox_key,
            attempts,
            required = row.required,
            cause_code = %diagnostic.code,
            "outbox entry EXHAUSTED sutra.outbox.retry.max-attempts — marked terminal (never \
             claimed again; the row is kept, visible and redrivable, not deleted)"
        );
        let already_recorded = incident_recorded(row);
        if !already_recorded {
            if let Some(sink) = &self.incidents {
                sink.record(exhausted_delivery_incident(deployment, row, diagnostic))
                    .await;
            }
        }
        let terminal = Diagnostic::error(
            codes::OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED,
            format!(
                "delivery to '{}' abandoned after {attempts} attempt(s) \
                 (sutra.outbox.retry.max-attempts): {} — {}",
                row.destination, diagnostic.code, diagnostic.message
            ),
        );
        let json = diagnostic_json_marked(&terminal, true);
        if let Err(mark_error) = self
            .store
            .mark_poisoned(deployment, &row.entry_id, &json)
            .await
        {
            warn!(
                entry_id = %row.entry_id,
                code = %mark_error.code,
                error = %mark_error.message,
                "outbox terminal mark failed — entry stays claimable until the next tick"
            );
            // The mark did not commit — no durable poison fact exists, so no wake fires
            // (the engine would refuse it against the missing evidence anyway).
            return;
        }
        // The wake prompt: a NODE-BEARING row went terminal, so the emitting node may be a
        // parked channel-call whose <q:retry> policy should rule on the failure NOW rather
        // than at its timeout. Fired only after the durable mark committed.
        if let (Some(notify), Some(node_id)) = (&self.poison_notify, &row.node_id) {
            notify(PoisonedDelivery {
                deployment: deployment.clone(),
                instance_id: row.instance_id.clone(),
                node_id: node_id.clone(),
            });
        }
    }

    /// Poison isolation: park the row at the retry policy's MAX horizon — one poison
    /// entry never hot-loops and never blocks its batch, yet stays visible + redrivable.
    ///
    /// **`required` incident, recorded ONCE.** A poisoned row is not deleted: it is deferred at the
    /// max-delay horizon and re-claimed there forever, so the naive "record on poison" would emit
    /// one incident per horizon tick until a human intervened — an alert storm, not a signal. The
    /// once-only key is a marker the dispatcher writes into the row's own
    /// `last_diagnostic_json` ([`INCIDENT_RECORDED_ATTR`]) in the SAME defer that parks it: the
    /// next claim of that row sees the marker and skips recording. Two honest consequences,
    /// stated rather than hidden:
    /// * if the marking defer itself fails (the store is down), the marker never lands and a later
    ///   poison records a second incident — a duplicate alert on an already-alerting failure, which
    ///   is the safe side of the trade;
    /// * a NON-required delivery never records at all, whatever it does — that is the entire
    ///   meaning of the flag the author set.
    async fn poison(
        &self,
        deployment: &DeploymentId,
        row: &ClaimedOutboxRow,
        diagnostic: &Diagnostic,
    ) {
        // A permanent failure still CONSUMES an attempt: when that spends the configured ceiling,
        // the entry goes terminal here rather than parking at the horizon to be re-claimed
        // forever. With no ceiling configured this is never taken and poison behaves exactly as
        // it always has.
        if self.attempts_exhausted(row) {
            self.give_up(deployment, row, diagnostic).await;
            return;
        }
        self.release_finished_call(deployment, row).await;
        warn!(
            entry_id = %row.entry_id,
            destination = %row.destination,
            code = %diagnostic.code,
            error = %diagnostic.message,
            required = row.required,
            "outbox entry poisoned — deferred at the max-delay horizon"
        );
        let already_recorded = incident_recorded(row);
        let record_now = row.required && !already_recorded;
        if record_now {
            // Always observable, sink or no sink — the record floor the durable write sits under.
            tracing::error!(
                code = codes::OUTBOUND_REQUIRED_DELIVERY_FAILED,
                entry_id = %row.entry_id,
                instance = %row.instance_id,
                destination = %row.destination,
                outbox_key = %row.outbox_key,
                cause_code = %diagnostic.code,
                "REQUIRED outbound delivery poisoned — recording a durable incident (once for \
                 this entry; the row stays at the poison horizon, visible and redrivable)"
            );
            if let Some(sink) = &self.incidents {
                sink.record(required_delivery_incident(deployment, row, diagnostic))
                    .await;
            }
        }
        let next_at = (self.now)() + self.retry.poison_delay();
        self.defer_row(
            deployment,
            row,
            next_at,
            diagnostic,
            already_recorded || record_now,
        )
        .await;
    }

    async fn defer_row(
        &self,
        deployment: &DeploymentId,
        row: &ClaimedOutboxRow,
        next_at: OffsetDateTime,
        diagnostic: &Diagnostic,
        incident_recorded: bool,
    ) {
        let json = diagnostic_json_marked(diagnostic, incident_recorded);
        if let Err(defer_error) = self
            .store
            .defer(deployment, &row.entry_id, next_at, &json)
            .await
        {
            warn!(
                entry_id = %row.entry_id,
                code = %defer_error.code,
                error = %defer_error.message,
                "outbox defer failed — entry remains due until the next claim cycle"
            );
        }
    }
}

/// What a pooled entry may do this tick.
enum PoolAdmit {
    Send,
    Wait(Diagnostic),
    Abandon(Diagnostic),
}

/// The `last_diagnostic_json` column payload — `{code, message, attributes}` — with the once-only
/// incident marker folded in when set. The
/// marker rides the diagnostic rather than a new column deliberately: the column already exists,
/// already round-trips through the claim, and a poisoned row's diagnostic is exactly the place an
/// operator looks to ask "was this alerted?".
fn diagnostic_json_marked(diagnostic: &Diagnostic, incident_recorded: bool) -> String {
    let mut value = serde_json::json!({
        "code": diagnostic.code,
        "message": diagnostic.message,
        "attributes": diagnostic.attributes,
    });
    if incident_recorded {
        if let Some(map) = value.as_object_mut() {
            map.insert(
                INCIDENT_RECORDED_ATTR.to_string(),
                serde_json::Value::Bool(true),
            );
        }
    }
    value.to_string()
}

/// The `last_diagnostic_json` key marking "this entry's `required`-delivery incident has already
/// been recorded" — the once-only latch (see [`OutboxDispatcher::poison`]).
pub const INCIDENT_RECORDED_ATTR: &str = "incidentRecorded";

/// True when the claimed row's last diagnostic already carries the [`INCIDENT_RECORDED_ATTR`]
/// latch. Malformed/absent JSON reads as "not recorded" — fail towards alerting.
fn incident_recorded(row: &ClaimedOutboxRow) -> bool {
    row.last_diagnostic_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|value| {
            value
                .get(INCIDENT_RECORDED_ATTR)
                .and_then(serde_json::Value::as_bool)
        })
        .unwrap_or(false)
}

/// The incident one poisoned `required` delivery records. The outbound shape of the same
/// [`InboundIncident`] the dead-letter table stores: `channel` = the destination URI (that IS the
/// outbound channel), `process_id` = the originating instance, `dedup_key` = the frozen
/// `outbox_key` (so an operator can join the incident to the wire attempt), and
/// `failure_code` = [`codes::OUTBOUND_REQUIRED_DELIVERY_FAILED`] — the classification — with the
/// underlying sink diagnostic quoted in the detail. No payload capture: an outbound incident has no
/// inbound message, and the entry itself is still in the outbox, redrivable.
fn required_delivery_incident(
    deployment: &DeploymentId,
    row: &ClaimedOutboxRow,
    cause: &Diagnostic,
) -> InboundIncident {
    InboundIncident::of_failure(
        deployment.value(),
        &row.destination,
        &row.instance_id,
        &row.outbox_key,
        codes::OUTBOUND_REQUIRED_DELIVERY_FAILED,
        format!(
            "required delivery to '{}' poisoned after {} attempt(s): {} — {}",
            row.destination,
            row.attempt_count + 1,
            cause.code,
            cause.message
        ),
        rfc3339(OffsetDateTime::now_utc()),
    )
}

/// The single incident an EXHAUSTED entry records. Same [`InboundIncident`] shape as the
/// `required`-delivery incident above (channel = the destination URI, `dedup_key` = the frozen
/// `outbox_key` so it joins to the wire attempts), classified under
/// [`codes::OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED`] so an operator can tell "still retrying, and
/// loudly" from "stopped retrying". No payload capture: the entry itself survives in
/// `outbox_entry`, flagged terminal and redrivable.
fn exhausted_delivery_incident(
    deployment: &DeploymentId,
    row: &ClaimedOutboxRow,
    cause: &Diagnostic,
) -> InboundIncident {
    InboundIncident::of_failure(
        deployment.value(),
        &row.destination,
        &row.instance_id,
        &row.outbox_key,
        codes::OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED,
        format!(
            "delivery to '{}' abandoned after {} attempt(s) at the configured \
             sutra.outbox.retry.max-attempts ceiling: {} — {}",
            row.destination,
            row.attempt_count + 1,
            cause.code,
            cause.message
        ),
        rfc3339(OffsetDateTime::now_utc()),
    )
}

fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

// ---- the tick loop (the dispatch scheduler — PT5S default, drain-aware) --------------------

/// Handle to the spawned tick loop: [`drain`](OutboxDispatcherHandle::drain) refuses
/// subsequent ticks (the in-flight batch always completes — the loop is a single task,
/// so ticks never overlap); [`shutdown`](OutboxDispatcherHandle::shutdown) drains and
/// awaits the task.
pub struct OutboxDispatcherHandle {
    drain: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl OutboxDispatcherHandle {
    /// Drain-aware hook: refuse every subsequent tick. Idempotent.
    pub fn drain(&self) {
        let _ = self.drain.send(true);
    }

    /// Drain and await loop exit (the in-flight tick finishes first).
    pub async fn shutdown(self) {
        self.drain();
        let _ = self.task.await;
    }
}

/// The live deployment-id set the background loops (outbox dispatcher, timer poller)
/// walk each tick — swapped whole by an activation flip (active + DRAINING ids;
/// RETIRED ids drop out). Cheap to clone; readers take a snapshot per tick.
#[derive(Clone, Default)]
pub struct LiveDeploymentSet {
    ids: Arc<std::sync::RwLock<Vec<DeploymentId>>>,
}

impl LiveDeploymentSet {
    pub fn new(ids: Vec<DeploymentId>) -> LiveDeploymentSet {
        LiveDeploymentSet {
            ids: Arc::new(std::sync::RwLock::new(ids)),
        }
    }

    /// Atomically replace the set (the activation flip / retire step).
    pub fn replace(&self, ids: Vec<DeploymentId>) {
        *self.ids.write().expect("deployment set lock") = ids;
    }

    /// The current ids (per-tick snapshot).
    pub fn snapshot(&self) -> Vec<DeploymentId> {
        self.ids.read().expect("deployment set lock").clone()
    }
}

/// Spawn the dispatcher tick loop on `runtime`: every `tick_interval` (config
/// `sutra.outbox.tick-interval`, default PT5S) drain one batch per known deployment.
/// Missed ticks are skipped, mirroring `@Scheduled(concurrentExecution = SKIP)`.
/// `deployments` is read per tick, so an activation flip is picked up on the next tick.
pub fn spawn_dispatch_loop(
    runtime: &tokio::runtime::Handle,
    dispatcher: Arc<OutboxDispatcher>,
    deployments: LiveDeploymentSet,
    tick_interval: std::time::Duration,
) -> OutboxDispatcherHandle {
    let (drain, mut drained) = tokio::sync::watch::channel(false);
    let interval_duration = tick_interval.max(std::time::Duration::from_millis(1));
    let task = runtime.spawn(async move {
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if *drained.borrow() {
                        break;
                    }
                    let deployments = deployments.snapshot();
                    if deployments.is_empty() {
                        continue;
                    }
                    let mut totals = DispatchStats::default();
                    for deployment in &deployments {
                        let stats = dispatcher.dispatch_deployment(deployment).await;
                        totals.attempted += stats.attempted;
                        totals.succeeded += stats.succeeded;
                        totals.failed += stats.failed;
                    }
                    if totals.attempted > 0 {
                        info!(
                            deployments = deployments.len(),
                            attempted = totals.attempted,
                            succeeded = totals.succeeded,
                            failed = totals.failed,
                            "outbox tick"
                        );
                    }
                }
                _ = drained.changed() => break,
            }
        }
        info!("outbox dispatch loop drained — refusing further ticks");
    });
    OutboxDispatcherHandle { drain, task }
}

// ---- the concurrent dispatcher (0.6.5) ---------------------------------------------------------

/// The in-process link between the code that ENQUEUES outbox rows (the engine's step commits) and
/// the concurrent dispatcher that delivers them.
///
/// * [`OutboxSignal::wake`] — a step committed rows: claim now rather than at the next tick. The
///   tick stays as the fallback that picks up retries falling due and rows another replica
///   enqueued.
/// * [`OutboxSignal::reserve`] / [`OutboxSignal::hand_off`] — the local fast path: a row the step
///   is about to commit is reserved (marked in flight) BEFORE the commit, and once the commit is
///   durable the very same row is handed straight to a worker, so it is never claimed back out of
///   the table. It stays a durable row the whole time: if this process dies before the delivery
///   is deleted, any dispatcher claims it after restart, and the receiver's inbox dedup absorbs a
///   second delivery.
///
/// The in-flight set is what keeps two workers of this process from delivering one row twice:
/// the claim query excludes every entry in it, and an entry leaves it only once its delivery has
/// been deleted (or deferred). Cross-replica, nothing changes: rows are claimed with
/// `FOR UPDATE SKIP LOCKED` exactly as before, and delivery stays at-least-once.
#[derive(Clone, Default)]
pub struct OutboxSignal {
    inner: Arc<SignalInner>,
}

#[derive(Default)]
struct SignalInner {
    notify: tokio::sync::Notify,
    /// Every entry in flight in this process: reserved for a hand-off, or claimed.
    inflight: std::sync::Mutex<std::collections::HashSet<String>>,
    /// The subset that was CLAIMED (due rows). Only these need excluding from the next claim:
    /// a handed-off row is not due until its hand-off grace has passed.
    claimed: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Entries this process delivered and deleted recently. A claim whose query read a row
    /// just before its delete committed must not hand it out again; remembering the id for a
    /// while closes that window.
    finished: std::sync::Mutex<FinishedMemory>,
    handoff: std::sync::Mutex<
        Option<tokio::sync::mpsc::UnboundedSender<(DeploymentId, ClaimedOutboxRow)>>,
    >,
}

impl OutboxSignal {
    pub fn new() -> OutboxSignal {
        OutboxSignal::default()
    }

    /// Rows were enqueued: wake the dispatcher. A wake with no dispatcher waiting is kept
    /// (one permit), so a commit that lands mid-claim is never missed.
    pub fn wake(&self) {
        self.inner.notify.notify_one();
    }

    /// Whether a running dispatcher accepts hand-offs (the fast path is live).
    pub fn accepts_hand_off(&self) -> bool {
        self.inner
            .handoff
            .lock()
            .expect("outbox signal lock")
            .as_ref()
            .is_some_and(|tx| !tx.is_closed())
    }

    /// Mark entries in flight before the transaction that inserts them commits, so no claim
    /// can hand them to a second worker between the commit and the hand-off.
    pub fn reserve(&self, entry_ids: &[String]) {
        let mut inflight = self.inner.inflight.lock().expect("outbox signal lock");
        for id in entry_ids {
            inflight.insert(id.clone());
        }
    }

    /// Undo [`Self::reserve`] (the commit failed, or the entries were delivered and deleted).
    pub fn release(&self, entry_ids: &[String]) {
        {
            let mut inflight = self.inner.inflight.lock().expect("outbox signal lock");
            for id in entry_ids {
                inflight.remove(id);
            }
        }
        let mut claimed = self.inner.claimed.lock().expect("outbox signal lock");
        for id in entry_ids {
            claimed.remove(id);
        }
    }

    /// Hand committed, reserved rows straight to the dispatcher. With no dispatcher running the
    /// rows are released and the dispatcher woken instead, so they go out through the ordinary
    /// claim — nothing is ever dropped.
    pub fn hand_off(&self, deployment: &DeploymentId, rows: Vec<ClaimedOutboxRow>) {
        let sender = self
            .inner
            .handoff
            .lock()
            .expect("outbox signal lock")
            .clone();
        let mut unsent = Vec::new();
        match sender {
            Some(tx) => {
                for row in rows {
                    if let Err(returned) = tx.send((deployment.clone(), row)) {
                        unsent.push(returned.0 .1.entry_id);
                    }
                }
            }
            None => unsent.extend(rows.into_iter().map(|row| row.entry_id)),
        }
        if !unsent.is_empty() {
            self.release(&unsent);
            self.wake();
        }
    }

    /// The rows were delivered and their delete committed: release them, and remember them for
    /// [`FINISHED_MEMORY`] so a claim that raced the delete cannot deliver them again.
    fn finished(&self, entry_ids: &[String]) {
        self.inner
            .finished
            .lock()
            .expect("outbox signal lock")
            .remember(entry_ids, std::time::Instant::now());
        self.release(entry_ids);
    }

    fn claimed_snapshot(&self) -> Vec<String> {
        self.inner
            .claimed
            .lock()
            .expect("outbox signal lock")
            .iter()
            .cloned()
            .collect()
    }

    /// Mark a claimed row in flight; `false` if it already was (a hand-off owns it).
    fn claim_inflight(&self, entry_id: &str) -> bool {
        if self
            .inner
            .finished
            .lock()
            .expect("outbox signal lock")
            .recently_finished(entry_id, std::time::Instant::now())
        {
            return false;
        }
        let fresh = self
            .inner
            .inflight
            .lock()
            .expect("outbox signal lock")
            .insert(entry_id.to_string());
        if fresh {
            self.inner
                .claimed
                .lock()
                .expect("outbox signal lock")
                .insert(entry_id.to_string());
        }
        fresh
    }

    /// How many rows this process has in flight (tests and diagnostics).
    pub fn inflight_count(&self) -> usize {
        self.inner
            .inflight
            .lock()
            .expect("outbox signal lock")
            .len()
    }

    fn open_hand_off(
        &self,
    ) -> tokio::sync::mpsc::UnboundedReceiver<(DeploymentId, ClaimedOutboxRow)> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        *self.inner.handoff.lock().expect("outbox signal lock") = Some(tx);
        rx
    }

    fn close_hand_off(&self) {
        *self.inner.handoff.lock().expect("outbox signal lock") = None;
    }
}

/// Sizing of the concurrent dispatcher (`sutra.outbox.workers`, `sutra.outbox.batch-size`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxConcurrency {
    /// Delivery workers. Rows of one instance always go to the same worker, in claim order.
    pub workers: usize,
}

/// How far in the future a row handed off in-process is made due in the table. The hand-off
/// delivers it at once; the due time only matters if this process dies first, when any
/// dispatcher claims the row after this grace — and while it runs, no claim competes with the
/// hand-off for it.
pub const HANDOFF_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a delivered-and-deleted entry id is remembered (see `OutboxSignal::finished`).
const FINISHED_MEMORY: std::time::Duration = std::time::Duration::from_secs(60);

/// The ids of entries delivered and deleted within the last [`FINISHED_MEMORY`], with the
/// instant each was remembered. Entries are remembered in time order, so expiry pops from the
/// front of `order`: each call does work proportional to what it adds and what expired, never to
/// the size of the memory (which holds a minute of deliveries — tens of thousands of ids at a
/// few hundred settles a second).
#[derive(Default)]
struct FinishedMemory {
    at: std::collections::HashMap<String, std::time::Instant>,
    order: std::collections::VecDeque<(std::time::Instant, String)>,
}

impl FinishedMemory {
    fn remember(&mut self, entry_ids: &[String], now: std::time::Instant) {
        self.expire(now);
        for id in entry_ids {
            self.at.insert(id.clone(), now);
            self.order.push_back((now, id.clone()));
        }
    }

    fn recently_finished(&self, entry_id: &str, now: std::time::Instant) -> bool {
        self.at
            .get(entry_id)
            .is_some_and(|at| now.saturating_duration_since(*at) < FINISHED_MEMORY)
    }

    /// Forget what is older than [`FINISHED_MEMORY`]. An id remembered again later keeps its
    /// newer instant: the older queue entry only removes it when the instants match.
    fn expire(&mut self, now: std::time::Instant) {
        while let Some((at, _)) = self.order.front() {
            if now.saturating_duration_since(*at) < FINISHED_MEMORY {
                break;
            }
            let (at, id) = self.order.pop_front().expect("front exists");
            if self.at.get(&id) == Some(&at) {
                self.at.remove(&id);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.at.len()
    }
}

/// The most claims one deployment gets per claimer round (a round ends early when a claim comes
/// back short of a full batch).
const MAX_CLAIM_ROUNDS: usize = 64;

/// The most delivered rows one delete transaction removes.
const MAX_DELETE_BATCH: usize = 256;

impl OutboxDispatcher {
    /// The batch size this dispatcher claims.
    pub fn batch_size(&self) -> i64 {
        self.batch_size
    }

    /// The deletes of a batch of delivered rows, in one transaction per deployment. When the
    /// batch delete fails each row is deleted on its own, and a row whose delete still fails is
    /// deferred with backoff — the same outcome a failed single delete has always had.
    /// Returns the ids whose delete committed.
    async fn finish_delivered_batch(
        &self,
        rows: Vec<(DeploymentId, ClaimedOutboxRow)>,
    ) -> Vec<String> {
        let mut deleted = Vec::new();
        let mut by_deployment: BTreeMap<String, (DeploymentId, Vec<ClaimedOutboxRow>)> =
            BTreeMap::new();
        for (deployment, row) in rows {
            by_deployment
                .entry(deployment.value().to_string())
                .or_insert_with(|| (deployment, Vec::new()))
                .1
                .push(row);
        }
        for (_, (deployment, rows)) in by_deployment {
            let ids: Vec<String> = rows.iter().map(|r| r.entry_id.clone()).collect();
            match self.store.delete_many(&deployment, &ids).await {
                Ok(()) => {
                    for row in &rows {
                        self.release_finished_call(&deployment, row).await;
                    }
                    deleted.extend(ids);
                }
                Err(diagnostic) => {
                    warn!(
                        deployment = %deployment.value(),
                        rows = ids.len(),
                        code = %diagnostic.code,
                        error = %diagnostic.message,
                        "outbox batch delete failed — deleting the delivered rows one by one"
                    );
                    for row in &rows {
                        if self.finish_delivered(&deployment, row).await {
                            deleted.push(row.entry_id.clone());
                        }
                    }
                }
            }
        }
        deleted
    }
}

/// Spawn the CONCURRENT dispatcher (0.6.5): one claimer, `workers` delivery workers and one
/// delete batcher.
///
/// * The claimer runs when an enqueue wakes it ([`OutboxSignal::wake`]), on every
///   `tick_interval` (the fallback for retries and other replicas' rows), and for every row
///   handed off by a step commit. It claims due rows past the ones already in flight and routes
///   each to the worker its instance hashes to, so one instance's rows are delivered one after
///   another in claim order (`next_attempt_at`, then `created_at`) — the order the serial loop
///   delivered them in. Rows of different instances are delivered in parallel.
/// * A row on a pooled outbound channel (capacity pools) is delivered by the claimer itself, in
///   claim order, exactly as the serial loop does: a free slot must go to the oldest waiter.
/// * A worker sends; on success the row goes to the delete batcher, which deletes whatever has
///   accumulated in one transaction. A row stays in flight until its delete commits, so it is
///   never claimed again in between.
///
/// Retry, backoff, poison and incident semantics are the serial loop's, by construction: the
/// workers call the same per-row code.
pub fn spawn_concurrent_dispatch_loop(
    runtime: &tokio::runtime::Handle,
    dispatcher: Arc<OutboxDispatcher>,
    deployments: LiveDeploymentSet,
    tick_interval: std::time::Duration,
    concurrency: OutboxConcurrency,
    signal: OutboxSignal,
) -> OutboxDispatcherHandle {
    let (drain, mut drained) = tokio::sync::watch::channel(false);
    let interval_duration = tick_interval.max(std::time::Duration::from_millis(1));
    let workers = concurrency.workers.max(1);
    let batch_size = dispatcher.batch_size();
    let rt = runtime.clone();
    let task = runtime.spawn(async move {
        let (delete_tx, mut delete_rx) =
            tokio::sync::mpsc::unbounded_channel::<(DeploymentId, ClaimedOutboxRow)>();
        // ---- the delete batcher
        let batcher = {
            let dispatcher = Arc::clone(&dispatcher);
            let signal = signal.clone();
            rt.spawn(async move {
                while let Some(first) = delete_rx.recv().await {
                    let mut batch = vec![first];
                    while batch.len() < MAX_DELETE_BATCH {
                        match delete_rx.try_recv() {
                            Ok(next) => batch.push(next),
                            Err(_) => break,
                        }
                    }
                    let ids: Vec<String> = batch.iter().map(|(_, r)| r.entry_id.clone()).collect();
                    let deleted = dispatcher.finish_delivered_batch(batch).await;
                    // Tombstone the deleted rows BEFORE anything leaves the in-flight set, so a
                    // claim that read a row just before its delete can never find it free. A row
                    // whose delete failed was deferred: it is released and stays claimable.
                    signal.finished(&deleted);
                    signal.release(&ids);
                }
            })
        };
        // ---- the workers
        let mut worker_tx = Vec::with_capacity(workers);
        let mut worker_tasks = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<(DeploymentId, ClaimedOutboxRow)>(
                (batch_size.max(1) as usize).max(16),
            );
            worker_tx.push(tx);
            let dispatcher = Arc::clone(&dispatcher);
            let signal = signal.clone();
            let delete_tx = delete_tx.clone();
            worker_tasks.push(rt.spawn(async move {
                while let Some((deployment, row)) = rx.recv().await {
                    if dispatcher.deliver(&deployment, &row).await {
                        if let Err(returned) = delete_tx.send((deployment, row)) {
                            // The batcher is gone (shutdown): delete in place.
                            let (deployment, row) = returned.0;
                            dispatcher.finish_delivered(&deployment, &row).await;
                            signal.release(&[row.entry_id]);
                        }
                    } else {
                        signal.release(&[row.entry_id]);
                    }
                }
            }));
        }
        drop(delete_tx);
        let route = move |row: &ClaimedOutboxRow| -> usize {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            row.instance_id.hash(&mut hasher);
            (hasher.finish() % workers as u64) as usize
        };
        // ---- the hand-off router: rows straight from step commits. Its own task, so a slow
        // claim never delays a hand-off.
        let mut handoff_rx = signal.open_hand_off();
        let router = {
            let dispatcher = Arc::clone(&dispatcher);
            let signal = signal.clone();
            let worker_tx = worker_tx.clone();
            let mut drained = drained.clone();
            rt.spawn(async move {
                loop {
                    let handed = tokio::select! {
                        biased;
                        _ = drained.changed() => break,
                        handed = handoff_rx.recv() => handed,
                    };
                    let Some((deployment, row)) = handed else {
                        break;
                    };
                    if row.headers.contains_key(CAPACITY_POOL_HEADER) {
                        let mut exhausted = std::collections::BTreeSet::new();
                        let now = (dispatcher.now)();
                        dispatcher
                            .dispatch_one(&deployment, &row, now, &mut exhausted)
                            .await;
                        signal.release(&[row.entry_id]);
                    } else if worker_tx[route(&row)]
                        .send((deployment, row))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                signal.close_hand_off();
                // Rows still queued stay committed: release them so a later claim (here or on
                // another replica) delivers them once their hand-off grace has passed.
                handoff_rx.close();
                while let Ok((_, row)) = handoff_rx.try_recv() {
                    signal.release(&[row.entry_id]);
                }
            })
        };
        // ---- the claimer (this task)
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = drained.changed() => break,
                _ = signal.inner.notify.notified() => {}
                _ = interval.tick() => {}
            }
            if *drained.borrow() {
                break;
            }
            for deployment in deployments.snapshot() {
                let mut exhausted = std::collections::BTreeSet::new();
                // Rows a claim returned that a hand-off still owns (it outlived its grace in a
                // backlog): excluded for the rest of this round, so they cannot fill every batch
                // and hold the other due rows back.
                let mut owned_elsewhere: Vec<String> = Vec::new();
                for _round in 0..MAX_CLAIM_ROUNDS {
                    let now = (dispatcher.now)();
                    let mut exclude = signal.claimed_snapshot();
                    exclude.extend(owned_elsewhere.iter().cloned());
                    let claimed = match dispatcher
                        .store
                        .claim_due_excluding(&deployment, now, batch_size, &exclude)
                        .await
                    {
                        Ok(claimed) => claimed,
                        Err(diagnostic) => {
                            warn!(
                                deployment = %deployment.value(),
                                code = %diagnostic.code,
                                error = %diagnostic.message,
                                "outbox claim failed — nothing more drained this round"
                            );
                            break;
                        }
                    };
                    let full = claimed.len() as i64 >= batch_size;
                    for row in claimed {
                        if !signal.claim_inflight(&row.entry_id) {
                            // A hand-off (or an earlier claim) owns it.
                            owned_elsewhere.push(row.entry_id);
                            continue;
                        }
                        if row.headers.contains_key(CAPACITY_POOL_HEADER) {
                            dispatcher
                                .dispatch_one(&deployment, &row, now, &mut exhausted)
                                .await;
                            signal.release(&[row.entry_id]);
                        } else {
                            let _ = worker_tx[route(&row)].send((deployment.clone(), row)).await;
                        }
                    }
                    if !full {
                        break;
                    }
                }
            }
        }
        drop(worker_tx);
        let _ = router.await;
        for task in worker_tasks {
            let _ = task.await;
        }
        let _ = batcher.await;
        info!("concurrent outbox dispatcher drained — refusing further claims");
    });
    OutboxDispatcherHandle { drain, task }
}

// ---- tests ----------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::sink::MessageSink;

    fn t0() -> OffsetDateTime {
        time::macros::datetime!(2026-05-20 10:00:00 UTC)
    }

    fn dep() -> DeploymentId {
        DeploymentId::of("dep-0000000000000000000000a1").expect("valid deployment id")
    }

    fn row(entry_id: &str, destination: &str, attempt_count: i32) -> ClaimedOutboxRow {
        ClaimedOutboxRow {
            entry_id: entry_id.to_string(),
            attempt_count,
            destination: destination.to_string(),
            headers: BTreeMap::new(),
            body: b"payload".to_vec(),
            content_type: Some("text/plain".to_string()),
            outbox_key: format!("key-{entry_id}"),
            mode: ReplyMode::Native,
            cloud_event_json: None,
            auth_ref_json: None,
            labels: BTreeMap::from([("tenant".to_string(), "acme".to_string())]),
            traceparent: None,
            node_id: None,
            instance_id: "11111111-1111-4111-8111-111111111111".to_string(),
            required: false,
            last_diagnostic_json: None,
        }
    }

    /// The same row with `<q:send required>` set — delivery failure must surface as an incident.
    fn required_row(entry_id: &str, destination: &str, attempt_count: i32) -> ClaimedOutboxRow {
        ClaimedOutboxRow {
            required: true,
            ..row(entry_id, destination, attempt_count)
        }
    }

    /// In-memory [`OutboxRowStore`] backing the dispatcher tests below.
    #[derive(Default)]
    struct InMemoryRows {
        rows: Mutex<Vec<(ClaimedOutboxRow, OffsetDateTime, Option<String>)>>,
        fail_delete: bool,
        /// Entry ids marked TERMINAL, with the diagnostic JSON that ended them — the durable
        /// `poisoned` flag's in-memory stand-in. A row here is never claimed again, exactly as
        /// `WHERE NOT poisoned` behaves against PostgreSQL.
        terminal: Mutex<Vec<(String, String)>>,
    }

    impl InMemoryRows {
        fn enqueue(&self, row: ClaimedOutboxRow, due: OffsetDateTime) {
            self.rows.lock().unwrap().push((row, due, None));
        }

        fn snapshot(&self) -> Vec<(ClaimedOutboxRow, OffsetDateTime, Option<String>)> {
            self.rows.lock().unwrap().clone()
        }

        fn terminal_marks(&self) -> Vec<(String, String)> {
            self.terminal.lock().unwrap().clone()
        }

        fn is_terminal(&self, entry_id: &str) -> bool {
            self.terminal
                .lock()
                .unwrap()
                .iter()
                .any(|(id, _)| id == entry_id)
        }

        /// Re-arm every row as due at `now` — the frozen-clock stand-in for wall-clock time
        /// passing between two dispatcher ticks.
        fn make_all_due(&self, now: OffsetDateTime) {
            for (_, due, _) in self.rows.lock().unwrap().iter_mut() {
                *due = now;
            }
        }
    }

    impl OutboxRowStore for InMemoryRows {
        fn claim_due<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            now: OffsetDateTime,
            max_entries: i64,
        ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
            Box::pin(async move {
                Ok(self
                    .rows
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(row, due, _)| *due <= now && !self.is_terminal(&row.entry_id))
                    .take(max_entries as usize)
                    .map(|(row, _, _)| row.clone())
                    .collect())
            })
        }

        fn delete<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            entry_id: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            Box::pin(async move {
                if self.fail_delete {
                    return Err(Diagnostic::error(codes::RUNTIME_UNEXPECTED, "delete down"));
                }
                self.rows
                    .lock()
                    .unwrap()
                    .retain(|(row, _, _)| row.entry_id != entry_id);
                Ok(())
            })
        }

        fn defer<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            entry_id: &'a str,
            new_due_at: OffsetDateTime,
            diagnostic_json: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            Box::pin(async move {
                for (row, due, diag) in self.rows.lock().unwrap().iter_mut() {
                    if row.entry_id == entry_id {
                        row.attempt_count += 1;
                        *due = new_due_at;
                        *diag = Some(diagnostic_json.to_string());
                        // The durable store returns `last_diagnostic_json` on the NEXT claim —
                        // mirrored here so the once-only incident latch is actually exercised.
                        row.last_diagnostic_json = Some(diagnostic_json.to_string());
                    }
                }
                Ok(())
            })
        }

        fn requeue<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            entry_id: &'a str,
            new_due_at: OffsetDateTime,
            diagnostic_json: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            Box::pin(async move {
                for (row, due, diag) in self.rows.lock().unwrap().iter_mut() {
                    if row.entry_id == entry_id {
                        *due = new_due_at;
                        *diag = Some(diagnostic_json.to_string());
                        row.last_diagnostic_json = Some(diagnostic_json.to_string());
                    }
                }
                Ok(())
            })
        }

        fn mark_poisoned<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            entry_id: &'a str,
            diagnostic_json: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            Box::pin(async move {
                self.terminal
                    .lock()
                    .unwrap()
                    .push((entry_id.to_string(), diagnostic_json.to_string()));
                // The durable store neither moves the due time nor bumps the attempt count; it
                // only records the final diagnostic on the row that stays behind.
                for (row, _, diag) in self.rows.lock().unwrap().iter_mut() {
                    if row.entry_id == entry_id {
                        *diag = Some(diagnostic_json.to_string());
                        row.last_diagnostic_json = Some(diagnostic_json.to_string());
                    }
                }
                Ok(())
            })
        }
    }

    struct FixedSink {
        schemes: Vec<String>,
        outcomes: Mutex<Vec<SendOutcome>>,
        last: SendOutcome,
        sent: Mutex<Vec<OutboundMessage>>,
    }

    impl FixedSink {
        fn always(scheme: &str, outcome: SendOutcome) -> FixedSink {
            FixedSink {
                schemes: vec![scheme.to_string()],
                outcomes: Mutex::new(Vec::new()),
                last: outcome,
                sent: Mutex::new(Vec::new()),
            }
        }

        fn sequence(scheme: &str, outcomes: Vec<SendOutcome>, then: SendOutcome) -> FixedSink {
            FixedSink {
                schemes: vec![scheme.to_string()],
                outcomes: Mutex::new(outcomes),
                last: then,
                sent: Mutex::new(Vec::new()),
            }
        }
    }

    impl MessageSink for FixedSink {
        fn schemes(&self) -> Vec<String> {
            self.schemes.clone()
        }

        fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
            Box::pin(async move {
                self.sent.lock().unwrap().push(message.clone());
                let mut outcomes = self.outcomes.lock().unwrap();
                if outcomes.is_empty() {
                    self.last.clone()
                } else {
                    outcomes.remove(0)
                }
            })
        }
    }

    fn dispatcher(store: Arc<InMemoryRows>, sink: Arc<FixedSink>, batch: i64) -> OutboxDispatcher {
        let mut sinks = SinkRegistry::new();
        sinks.register(sink);
        OutboxDispatcher::new(
            store,
            sinks,
            RetryPolicy::new(
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(300),
                false,
            ),
            batch,
        )
        .with_clock(t0)
    }

    // ---- RetryPolicy ----------------------------------------------------------------------

    #[test]
    fn retry_policy_backs_off_exponentially_and_caps_at_max() {
        let policy = RetryPolicy::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
            false,
        );
        assert_eq!(
            policy.next_attempt(t0(), 1),
            t0() + time::Duration::seconds(2)
        );
        assert_eq!(
            policy.next_attempt(t0(), 2),
            t0() + time::Duration::seconds(4)
        );
        assert_eq!(
            policy.next_attempt(t0(), 6),
            t0() + time::Duration::seconds(60)
        );
        assert_eq!(
            policy.next_attempt(t0(), 50),
            t0() + time::Duration::seconds(60)
        );
    }

    #[test]
    fn retry_policy_jitter_samples_below_the_ceiling() {
        let policy = RetryPolicy::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
            true,
        )
        .with_sampler(|bound| bound / 2);
        // attempt 2 → ceiling 4000ms → sampled 2000ms.
        assert_eq!(
            policy.next_attempt(t0(), 2),
            t0() + time::Duration::seconds(2)
        );
        // A zero sample falls back to the base delay (the `delay <= 0` guard).
        let zero = RetryPolicy::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
            true,
        )
        .with_sampler(|_| 0);
        assert_eq!(
            zero.next_attempt(t0(), 3),
            t0() + time::Duration::seconds(1)
        );
    }

    // ---- dispatcher outcomes ---------------------------------------------------------------

    #[tokio::test]
    async fn success_path_sends_then_deletes() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/cb", 0), t0());
        let sink = Arc::new(FixedSink::always("http", SendOutcome::Delivered));
        let d = dispatcher(Arc::clone(&store), Arc::clone(&sink), 10);

        let stats = d.dispatch_deployment(&dep()).await;

        assert_eq!(
            stats,
            DispatchStats {
                attempted: 1,
                succeeded: 1,
                failed: 0,
                waiting: 0
            }
        );
        assert_eq!(sink.sent.lock().unwrap().len(), 1);
        assert!(store.snapshot().is_empty(), "delivered row deleted");
    }

    #[tokio::test]
    async fn retryable_failure_defers_with_diagnostic_and_backoff() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/cb", 0), t0());
        let sink = Arc::new(FixedSink::always(
            "http",
            SendOutcome::RetryableFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                "downstream 503",
            )),
        ));
        let d = dispatcher(Arc::clone(&store), sink, 10);

        let stats = d.dispatch_deployment(&dep()).await;

        assert_eq!(
            stats,
            DispatchStats {
                attempted: 1,
                succeeded: 0,
                failed: 1,
                waiting: 0
            }
        );
        let rows = store.snapshot();
        assert_eq!(rows.len(), 1);
        let (deferred, due, diag) = &rows[0];
        assert_eq!(deferred.attempt_count, 1);
        // attempt 1, no jitter → base << 1 = 2s.
        assert_eq!(*due, t0() + time::Duration::seconds(2));
        assert!(diag
            .as_deref()
            .unwrap()
            .contains(codes::OUTBOUND_SEND_FAILED));
    }

    #[test]
    fn finished_memory_expires_in_time_order_and_keeps_a_re_remembered_id() {
        let t = std::time::Instant::now();
        let ids = |r: std::ops::Range<u32>| r.map(|i| format!("e{i}")).collect::<Vec<_>>();
        let mut memory = FinishedMemory::default();
        memory.remember(&ids(0..3), t);
        memory.remember(&ids(3..5), t + std::time::Duration::from_secs(30));
        assert!(memory.recently_finished("e0", t + std::time::Duration::from_secs(59)));
        assert!(!memory.recently_finished("e9", t));
        // e1 is delivered again later: its newer instant must survive e1's first expiry.
        memory.remember(&ids(1..2), t + std::time::Duration::from_secs(45));
        memory.remember(&[], t + std::time::Duration::from_secs(61));
        assert!(!memory.recently_finished("e0", t + std::time::Duration::from_secs(61)));
        assert!(memory.recently_finished("e1", t + std::time::Duration::from_secs(61)));
        assert!(memory.recently_finished("e3", t + std::time::Duration::from_secs(61)));
        assert_eq!(memory.len(), 3, "e0 and e2 forgotten; e1, e3, e4 kept");
        memory.remember(&[], t + std::time::Duration::from_secs(200));
        assert_eq!(memory.len(), 0);
        assert!(memory.order.is_empty());
    }

    /// A sink-bounded retryable failure backs off like any other until its bound, then takes the
    /// poison posture (kept at the max-delay horizon, diagnostic attached) instead of retrying on.
    #[tokio::test]
    async fn bounded_retryable_failure_backs_off_then_poisons_at_its_bound() {
        let bounded = || {
            SendOutcome::RetryableFailure(
                Diagnostic::error(codes::OUTBOUND_SEND_FAILED, "not parked yet")
                    .with_attribute(BOUNDED_RETRY_ATTR, "3"),
            )
        };
        // Attempt 2 of 3 (attempt_count 1 failed before): ordinary backoff.
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/cb", 1), t0());
        let d = dispatcher(
            Arc::clone(&store),
            Arc::new(FixedSink::always("http", bounded())),
            10,
        );
        d.dispatch_deployment(&dep()).await;
        let (deferred, due, _) = &store.snapshot()[0];
        assert_eq!(deferred.attempt_count, 2);
        assert_eq!(
            *due,
            t0() + time::Duration::seconds(4),
            "attempt 2 → base << 2"
        );

        // Attempt 3 of 3: the bound is spent → poison horizon, never dropped.
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/cb", 2), t0());
        let d = dispatcher(
            Arc::clone(&store),
            Arc::new(FixedSink::always("http", bounded())),
            10,
        );
        d.dispatch_deployment(&dep()).await;
        let rows = store.snapshot();
        assert_eq!(rows.len(), 1, "a spent bound poisons; the row is kept");
        let (_, due, diag) = &rows[0];
        assert_eq!(
            *due,
            t0() + time::Duration::seconds(300),
            "max-delay horizon"
        );
        assert!(diag.as_deref().unwrap().contains("not parked yet"));
    }

    #[test]
    fn bounded_retry_is_spent_only_at_the_bound_and_ignores_bad_values() {
        let with = |v: &str| {
            Diagnostic::error(codes::OUTBOUND_SEND_FAILED, "x")
                .with_attribute(BOUNDED_RETRY_ATTR, v)
        };
        assert!(!bounded_retry_spent(&with("8"), 0));
        assert!(!bounded_retry_spent(&with("8"), 6));
        assert!(bounded_retry_spent(&with("8"), 7));
        assert!(bounded_retry_spent(&with("1"), 0));
        assert!(
            !bounded_retry_spent(&with("0"), 50),
            "non-positive bound ignored"
        );
        assert!(
            !bounded_retry_spent(&with("many"), 50),
            "unparsable bound ignored"
        );
        assert!(!bounded_retry_spent(
            &Diagnostic::error(codes::OUTBOUND_SEND_FAILED, "x"),
            50
        ));
    }

    #[tokio::test]
    async fn unknown_scheme_poisons_without_propagating() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "amqp://broker/queue", 0), t0());
        let d = dispatcher(
            Arc::clone(&store),
            Arc::new(FixedSink::always("http", SendOutcome::Delivered)),
            10,
        );

        let stats = d.dispatch_deployment(&dep()).await;

        assert_eq!(stats.failed, 1);
        let rows = store.snapshot();
        assert_eq!(
            rows.len(),
            1,
            "row survives (poison isolation, never dropped)"
        );
        let (_, due, diag) = &rows[0];
        assert_eq!(
            *due,
            t0() + time::Duration::seconds(300),
            "max-delay horizon"
        );
        assert!(diag
            .as_deref()
            .unwrap()
            .contains(codes::OUTBOUND_SEND_FAILED));
        assert!(diag
            .as_deref()
            .unwrap()
            .contains("no MessageSink registered"));
    }

    #[tokio::test]
    async fn permanent_failure_poisons_the_entry_only() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("poison", "http://reply.example/bad", 4), t0());
        store.enqueue(row("ok", "http://reply.example/ok", 0), t0());
        let sink = Arc::new(FixedSink::sequence(
            "http",
            vec![SendOutcome::PermanentFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                "400 contract reject",
            ))],
            SendOutcome::Delivered,
        ));
        let d = dispatcher(Arc::clone(&store), sink, 10);

        let stats = d.dispatch_deployment(&dep()).await;

        assert_eq!(
            stats,
            DispatchStats {
                attempted: 2,
                succeeded: 1,
                failed: 1,
                waiting: 0
            }
        );
        let rows = store.snapshot();
        assert_eq!(rows.len(), 1, "one poison entry never blocks the rest");
        assert_eq!(rows[0].0.entry_id, "poison");
        assert_eq!(rows[0].1, t0() + time::Duration::seconds(300));
    }

    #[tokio::test]
    async fn mixed_batch_succeeds_and_fails_entries_independently() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("ok", "http://reply.example/ok", 0), t0());
        store.enqueue(row("fail", "http://reply.example/fail", 2), t0());
        let sink = Arc::new(FixedSink::sequence(
            "http",
            vec![SendOutcome::Delivered],
            SendOutcome::RetryableFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                "second call boom",
            )),
        ));
        let d = dispatcher(Arc::clone(&store), sink, 10);

        let stats = d.dispatch_deployment(&dep()).await;

        assert_eq!(stats.succeeded, 1);
        assert_eq!(stats.failed, 1);
        let rows = store.snapshot();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0.entry_id, "fail");
        assert_eq!(rows[0].0.attempt_count, 3);
    }

    #[tokio::test]
    async fn no_entries_returns_empty_stats() {
        let store = Arc::new(InMemoryRows::default());
        let d = dispatcher(
            store,
            Arc::new(FixedSink::always("http", SendOutcome::Delivered)),
            10,
        );
        assert_eq!(
            d.dispatch_deployment(&dep()).await,
            DispatchStats::default()
        );
    }

    #[tokio::test]
    async fn batch_size_bounds_the_claim() {
        let store = Arc::new(InMemoryRows::default());
        for i in 0..10 {
            store.enqueue(row(&format!("e{i}"), "http://r/", 0), t0());
        }
        let d = dispatcher(
            Arc::clone(&store),
            Arc::new(FixedSink::always("http", SendOutcome::Delivered)),
            3,
        );

        let stats = d.dispatch_deployment(&dep()).await;

        assert_eq!(stats.attempted, 3);
        assert_eq!(store.snapshot().len(), 7);
    }

    #[tokio::test]
    async fn delete_failure_defers_for_redelivery() {
        let store = Arc::new(InMemoryRows {
            fail_delete: true,
            ..InMemoryRows::default()
        });
        store.enqueue(row("e1", "http://reply.example/cb", 0), t0());
        let d = dispatcher(
            Arc::clone(&store),
            Arc::new(FixedSink::always("http", SendOutcome::Delivered)),
            10,
        );

        let stats = d.dispatch_deployment(&dep()).await;

        assert_eq!(
            stats.failed, 1,
            "delivered-but-not-deleted counts as failed"
        );
        let rows = store.snapshot();
        assert_eq!(
            rows[0].0.attempt_count, 1,
            "deferred for at-least-once redelivery"
        );
    }

    // ---- wire encode (HTTP sink mode branches + m9 assertions) ------------------------------

    #[test]
    fn native_mode_keeps_body_and_resolves_content_type() {
        let mut r = row("e1", "http://cb/", 0);
        r.content_type = None;
        r.headers
            .insert("content-type".to_string(), "application/xml".to_string());
        r.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.body, b"payload");
        assert_eq!(m.content_type.as_deref(), Some("application/xml"));
        assert_eq!(m.outbox_key, "key-e1");
        assert_eq!(
            m.traceparent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn match_inbound_renders_native_at_delivery() {
        let mut r = row("e1", "http://cb/", 0);
        r.mode = ReplyMode::MatchInbound;
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.body, b"payload");
        assert_eq!(m.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn ce_binary_adds_headers_without_rewriting_body() {
        let mut r = row("e1", "http://cb/", 0);
        r.mode = ReplyMode::CloudeventBinary;
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.body, b"payload", "CE-binary leaves body bytes untouched");
        assert_eq!(m.headers.get("ce-id").unwrap(), "key-e1");
        assert_eq!(m.headers.get("ce-source").unwrap(), "/acme");
        assert_eq!(m.headers.get("ce-specversion").unwrap(), "1.0");
        assert_eq!(m.headers.get("ce-type").unwrap(), "sutra.channel.reply");
        assert_eq!(m.headers.get("ce-time").unwrap(), "2026-05-20T10:00:00Z");
    }

    #[test]
    fn ce_binary_prefers_the_persisted_cloud_event_view() {
        let mut r = row("e1", "http://cb/", 0);
        r.mode = ReplyMode::CloudeventBinary;
        r.cloud_event_json = Some(
            r#"{"id":"ce-7","source":"/sutra/instance/i1","specversion":"1.0",
                "type":"io.sutra.reply.v1","subject":"s","time":"2026-01-01T00:00:00Z"}"#
                .to_string(),
        );
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.headers.get("ce-id").unwrap(), "ce-7");
        assert_eq!(m.headers.get("ce-source").unwrap(), "/sutra/instance/i1");
        assert_eq!(m.headers.get("ce-type").unwrap(), "io.sutra.reply.v1");
        assert_eq!(m.headers.get("ce-subject").unwrap(), "s");
        assert_eq!(m.headers.get("ce-time").unwrap(), "2026-01-01T00:00:00Z");
    }

    #[test]
    fn ce_binary_uses_the_amqp_binding_prefix_for_broker_destinations() {
        // The m9 per-broker CE-binding matrix: RabbitMQ (AMQP 0.9.1) carries CE
        // attributes as `cloudEvents:<attr>` message headers, never `ce-<attr>`.
        for destination in [
            "rabbitmq://broker:5672/replies.q",
            "amqp://broker/replies.q",
        ] {
            let mut r = row("e1", destination, 0);
            r.mode = ReplyMode::CloudeventBinary;
            r.cloud_event_json = Some(
                r#"{"id":"ce-7","source":"/sutra/instance/i1","specversion":"1.0",
                    "type":"io.sutra.reply.v1","datacontenttype":"application/xml"}"#
                    .to_string(),
            );
            let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
            assert_eq!(m.body, b"payload", "CE-binary leaves body bytes untouched");
            assert_eq!(m.headers.get("cloudEvents:id").unwrap(), "ce-7");
            assert_eq!(
                m.headers.get("cloudEvents:source").unwrap(),
                "/sutra/instance/i1"
            );
            assert_eq!(m.headers.get("cloudEvents:specversion").unwrap(), "1.0");
            assert_eq!(
                m.headers.get("cloudEvents:type").unwrap(),
                "io.sutra.reply.v1"
            );
            assert_eq!(
                m.headers.get("cloudEvents:datacontenttype").unwrap(),
                "application/xml"
            );
            assert!(
                !m.headers.keys().any(|k| k.starts_with("ce-")),
                "no HTTP-binding ce-* header may leak onto an AMQP destination"
            );
        }
    }

    #[test]
    fn ce_binary_amqp_binding_synthesizes_required_attributes_without_a_view() {
        let mut r = row("e1", "rabbitmq://broker:5672/replies.q", 0);
        r.mode = ReplyMode::CloudeventBinary;
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.headers.get("cloudEvents:id").unwrap(), "key-e1");
        assert_eq!(m.headers.get("cloudEvents:source").unwrap(), "/acme");
        assert_eq!(m.headers.get("cloudEvents:specversion").unwrap(), "1.0");
        assert_eq!(
            m.headers.get("cloudEvents:type").unwrap(),
            "sutra.channel.reply"
        );
        assert_eq!(
            m.headers.get("cloudEvents:time").unwrap(),
            "2026-05-20T10:00:00Z"
        );
    }

    #[test]
    fn ce_binary_uses_the_kafka_binding_prefix_for_kafka_destinations() {
        // The m9 per-broker CE-binding matrix: the Kafka protocol binding carries CE
        // attributes as `ce_<attr>` record headers (UNDERSCORE), never `ce-<attr>` nor
        // `cloudEvents:<attr>`. Unlike AMQP, Kafka does NOT lift `datacontenttype`.
        let mut r = row("e1", "kafka://payment-replies/customer-7", 0);
        r.mode = ReplyMode::CloudeventBinary;
        r.cloud_event_json = Some(
            r#"{"id":"ce-7","source":"/sutra/instance/i1","specversion":"1.0",
                "type":"io.sutra.reply.v1","subject":"s","datacontenttype":"application/xml"}"#
                .to_string(),
        );
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.body, b"payload", "CE-binary leaves body bytes untouched");
        assert_eq!(m.headers.get("ce_id").unwrap(), "ce-7");
        assert_eq!(m.headers.get("ce_source").unwrap(), "/sutra/instance/i1");
        assert_eq!(m.headers.get("ce_specversion").unwrap(), "1.0");
        assert_eq!(m.headers.get("ce_type").unwrap(), "io.sutra.reply.v1");
        assert_eq!(m.headers.get("ce_subject").unwrap(), "s");
        assert!(
            !m.headers.contains_key("ce_datacontenttype"),
            "the Kafka binding must not lift datacontenttype (AMQP-only lift)"
        );
        assert!(
            !m.headers.keys().any(|k| k.starts_with("ce-")),
            "no HTTP-binding ce-* header may leak onto a kafka destination"
        );
        assert!(
            !m.headers.keys().any(|k| k.starts_with("cloudEvents:")),
            "no AMQP-binding cloudEvents: header may leak onto a kafka destination"
        );
    }

    #[test]
    fn ce_binary_kafka_binding_synthesizes_required_attributes_without_a_view() {
        let mut r = row("e1", "kafka://payment-replies", 0);
        r.mode = ReplyMode::CloudeventBinary;
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.headers.get("ce_id").unwrap(), "key-e1");
        assert_eq!(m.headers.get("ce_source").unwrap(), "/acme");
        assert_eq!(m.headers.get("ce_specversion").unwrap(), "1.0");
        assert_eq!(m.headers.get("ce_type").unwrap(), "sutra.channel.reply");
        assert_eq!(m.headers.get("ce_time").unwrap(), "2026-05-20T10:00:00Z");
    }

    #[test]
    fn ce_binary_uses_the_sqs_binding_prefix_for_aws_sqs_destinations() {
        // The m9 per-broker CE-binding matrix: the AWS SQS binding carries CE attributes
        // as `ce-<attr>` message attributes (DASH, like HTTP — SQS attribute names allow
        // it), never `ce_<attr>` nor `cloudEvents:<attr>`, and (like Kafka/HTTP binary) does
        // NOT lift `datacontenttype`.
        let mut r = row("e1", "aws-sqs://payment-replies", 0);
        r.mode = ReplyMode::CloudeventBinary;
        r.cloud_event_json = Some(
            r#"{"id":"ce-7","source":"/sutra/instance/i1","specversion":"1.0",
                "type":"io.sutra.reply.v1","subject":"s","datacontenttype":"application/xml"}"#
                .to_string(),
        );
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.body, b"payload", "CE-binary leaves body bytes untouched");
        assert_eq!(m.headers.get("ce-id").unwrap(), "ce-7");
        assert_eq!(m.headers.get("ce-source").unwrap(), "/sutra/instance/i1");
        assert_eq!(m.headers.get("ce-type").unwrap(), "io.sutra.reply.v1");
        assert_eq!(m.headers.get("ce-subject").unwrap(), "s");
        assert!(
            !m.headers.contains_key("ce-datacontenttype"),
            "the SQS binary binding must not lift datacontenttype"
        );
        assert!(
            !m.headers.keys().any(|k| k.starts_with("ce_")),
            "no Kafka-binding ce_* attribute may leak onto an aws-sqs destination"
        );
        assert!(
            !m.headers.keys().any(|k| k.starts_with("cloudEvents:")),
            "no AMQP-binding cloudEvents: attribute may leak onto an aws-sqs destination"
        );
    }

    #[test]
    fn ce_binary_uses_the_ce_dash_binding_prefix_for_gcp_pubsub_destinations() {
        // The m9 per-broker CE-binding matrix: the GCP Pub/Sub binding carries CE
        // attributes as `ce-<attr>` message attributes (DASH), never `ce_<attr>` nor
        // `cloudEvents:<attr>`. Like Kafka (and unlike AMQP 0.9.1), it does NOT lift
        // `datacontenttype`.
        let mut r = row("e1", "gcp-pubsub://payment-replies", 0);
        r.mode = ReplyMode::CloudeventBinary;
        r.cloud_event_json = Some(
            r#"{"id":"ce-7","source":"/sutra/instance/i1","specversion":"1.0",
                "type":"io.sutra.reply.v1","subject":"s","datacontenttype":"application/xml"}"#
                .to_string(),
        );
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.body, b"payload", "CE-binary leaves body bytes untouched");
        assert_eq!(m.headers.get("ce-id").unwrap(), "ce-7");
        assert_eq!(m.headers.get("ce-source").unwrap(), "/sutra/instance/i1");
        assert_eq!(m.headers.get("ce-specversion").unwrap(), "1.0");
        assert_eq!(m.headers.get("ce-type").unwrap(), "io.sutra.reply.v1");
        assert_eq!(m.headers.get("ce-subject").unwrap(), "s");
        assert!(
            !m.headers.contains_key("ce-datacontenttype"),
            "the GCP Pub/Sub binding must not lift datacontenttype (AMQP-0.9.1-only lift)"
        );
        assert!(
            !m.headers.keys().any(|k| k.starts_with("ce_")),
            "no Kafka-binding ce_* attribute may leak onto a gcp-pubsub destination"
        );
        assert!(
            !m.headers.keys().any(|k| k.starts_with("cloudEvents:")),
            "no AMQP-binding cloudEvents: attribute may leak onto a gcp-pubsub destination"
        );
    }

    // NOTE: the cross-broker "all non-RabbitMQ brokers share the `sutra-outbox-key` carrier
    // string" invariant (m9) moved to the engine's `transport_bundle` test during the
    // domain-neutrality refactor — the vendor constants now live in the
    // `sutra-transport-<vendor>` crates, visible only at the bundling layer, not here in the
    // neutral outbox dispatcher.

    #[test]
    fn ce_binary_uses_the_amqp10_binding_prefix_for_amqp10_destinations() {
        // The m9 per-broker CE-binding matrix: the AMQP 1.0 protocol binding
        // (`amqp10://` — distinct from AMQP-0.9.1 `amqp://`/`rabbitmq://`) carries CE
        // attributes as `ce-<attr>` application properties (DASH), never `cloudEvents:<attr>`
        // (the 0.9.1 form) nor `ce_<attr>` (the Kafka form). Unlike AMQP 0.9.1 it does NOT
        // lift `datacontenttype`.
        for destination in ["amqp10://broker:5672/replies", "amqp10s://broker/replies"] {
            let mut r = row("e1", destination, 0);
            r.mode = ReplyMode::CloudeventBinary;
            r.cloud_event_json = Some(
                r#"{"id":"ce-7","source":"/sutra/instance/i1","specversion":"1.0",
                    "type":"io.sutra.reply.v1","subject":"s","datacontenttype":"application/xml"}"#
                    .to_string(),
            );
            let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
            assert_eq!(m.body, b"payload", "CE-binary leaves body bytes untouched");
            assert_eq!(m.headers.get("ce-id").unwrap(), "ce-7");
            assert_eq!(m.headers.get("ce-source").unwrap(), "/sutra/instance/i1");
            assert_eq!(m.headers.get("ce-specversion").unwrap(), "1.0");
            assert_eq!(m.headers.get("ce-type").unwrap(), "io.sutra.reply.v1");
            assert_eq!(m.headers.get("ce-subject").unwrap(), "s");
            assert!(
                !m.headers.contains_key("ce-datacontenttype"),
                "the AMQP 1.0 binding must not lift datacontenttype (AMQP-0.9.1-only lift)"
            );
            assert!(
                !m.headers.keys().any(|k| k.starts_with("cloudEvents:")),
                "no AMQP-0.9.1 cloudEvents: header may leak onto an amqp10 destination"
            );
            assert!(
                !m.headers.keys().any(|k| k.starts_with("ce_")),
                "no Kafka ce_ header may leak onto an amqp10 destination"
            );
        }
    }

    #[test]
    fn ce_binary_amqp10_binding_synthesizes_required_attributes_without_a_view() {
        let mut r = row("e1", "amqp10://broker:5672/replies", 0);
        r.mode = ReplyMode::CloudeventBinary;
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(m.headers.get("ce-id").unwrap(), "key-e1");
        assert_eq!(m.headers.get("ce-source").unwrap(), "/acme");
        assert_eq!(m.headers.get("ce-specversion").unwrap(), "1.0");
        assert_eq!(m.headers.get("ce-type").unwrap(), "sutra.channel.reply");
        assert_eq!(m.headers.get("ce-time").unwrap(), "2026-05-20T10:00:00Z");
    }

    #[test]
    fn ce_structured_wraps_textual_data_inline() {
        let mut r = row("e1", "http://cb/", 0);
        r.mode = ReplyMode::CloudeventStructured;
        r.body = b"{\"answer\":42}".to_vec();
        r.content_type = Some("application/json".to_string());
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert_eq!(
            m.content_type.as_deref(),
            Some("application/cloudevents+json")
        );
        let envelope: serde_json::Value = serde_json::from_slice(&m.body).unwrap();
        assert_eq!(envelope["id"], "key-e1");
        assert_eq!(envelope["source"], "/acme");
        assert_eq!(envelope["specversion"], "1.0");
        assert_eq!(envelope["type"], "sutra.channel.reply");
        assert_eq!(envelope["datacontenttype"], "application/json");
        assert_eq!(envelope["data"], "{\"answer\":42}");
    }

    #[test]
    fn ce_structured_base64_encodes_binary_data() {
        let mut r = row("e1", "http://cb/", 0);
        r.mode = ReplyMode::CloudeventStructured;
        r.body = vec![0x00, 0x01, 0xff];
        r.content_type = Some("application/octet-stream".to_string());
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        let envelope: serde_json::Value = serde_json::from_slice(&m.body).unwrap();
        assert!(envelope.get("data").is_none());
        assert_eq!(envelope["data_base64"], "AAH/");
    }

    #[test]
    fn base64_encoding_is_standard_padded() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn auth_bearer_and_apikey_resolve_dispatcher_side() {
        let mut r = row("e1", "https://cb/", 0);
        r.auth_ref_json =
            Some(r#"{"scheme":"bearer","secretRef":"env:CALLBACK_TOKEN"}"#.to_string());
        // The resolver now receives the FULL secret-ref (scheme included), not a bare env: NAME.
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|full_ref| {
            (full_ref == "env:CALLBACK_TOKEN").then(|| "s3cret".to_string())
        });
        // The shared helper writes the lowercase `authorization` header (the broker
        // binding form; HTTP treats it case-insensitively).
        assert_eq!(m.headers.get("authorization").unwrap(), "Bearer s3cret");

        r.auth_ref_json = Some(
            r#"{"scheme":"apikey","secretRef":"env:CALLBACK_TOKEN","header":"X-Callback-Key"}"#
                .to_string(),
        );
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| Some("k3y".to_string()));
        assert_eq!(m.headers.get("X-Callback-Key").unwrap(), "k3y");
    }

    #[test]
    fn unresolvable_and_mtls_auth_refs_skip_headers() {
        let mut r = row("e1", "https://cb/", 0);
        r.auth_ref_json = Some(r#"{"scheme":"bearer","secretRef":"env:MISSING"}"#.to_string());
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| None);
        assert!(!m.headers.contains_key("authorization"));

        r.auth_ref_json = Some(r#"{"scheme":"mtls","secretRef":"env:BUNDLE"}"#.to_string());
        let m = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|_| {
            Some("material".to_string())
        });
        assert!(!m.headers.contains_key("authorization"));
    }

    #[test]
    fn a_non_env_scheme_resolves_through_the_injected_resolver() {
        // The generalization: an injected resolver receives the FULL ref, so `secret:`/`vault:`
        // refs resolve at dispatch (the engine wires this to envref). The default env-only
        // resolver, by contrast, drops a `secret:` ref (no header) — back-compat preserved.
        let mut r = row("e1", "https://cb/", 0);
        r.auth_ref_json = Some(
            r#"{"scheme":"apikey","secretRef":"secret:LOOPBACK_KEY","header":"X-Api-Key"}"#
                .to_string(),
        );
        let injected = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|full_ref| {
            (full_ref == "secret:LOOPBACK_KEY").then(|| "k3y".to_string())
        });
        assert_eq!(injected.headers.get("X-Api-Key").unwrap(), "k3y");

        // Default env-only shape (strip `env:`): a `secret:` ref does not resolve → no header.
        let env_only = encode_wire_message(&r, "2026-05-20T10:00:00Z", &|full_ref| {
            full_ref
                .strip_prefix("env:")
                .and_then(|name| (name == "LOOPBACK_KEY").then(|| "k3y".to_string()))
        });
        assert!(!env_only.headers.contains_key("X-Api-Key"));
    }

    // ---- tick loop -------------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn tick_loop_drains_and_refuses_after_drain() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/cb", 0), t0());
        let sink = Arc::new(FixedSink::always("http", SendOutcome::Delivered));
        let d = Arc::new(dispatcher(Arc::clone(&store), Arc::clone(&sink), 10));

        let handle = spawn_dispatch_loop(
            &tokio::runtime::Handle::current(),
            d,
            LiveDeploymentSet::new(vec![dep()]),
            std::time::Duration::from_millis(20),
        );
        // The first tick fires immediately; poll until the row is delivered.
        for _ in 0..100 {
            if store.snapshot().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(store.snapshot().is_empty(), "tick loop drained the row");

        handle.shutdown().await;
        // Post-drain: a new due row is never picked up.
        store.enqueue(row("e2", "http://reply.example/cb", 0), t0());
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert_eq!(store.snapshot().len(), 1, "drained loop refuses new ticks");
    }

    // ---- `required` delivery incidents (once per entry, never per retry tick) ---------------

    /// A `Send + Sync` [`IncidentSink`] double — the dispatcher's sink must cross threads
    /// (the in-crate `InMemoryIncidentSink` is `RefCell`-based and actor-thread-only).
    #[derive(Default)]
    struct CollectingIncidentSink {
        incidents: Mutex<Vec<InboundIncident>>,
    }

    impl CollectingIncidentSink {
        fn recorded(&self) -> Vec<InboundIncident> {
            self.incidents.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl IncidentSink for CollectingIncidentSink {
        async fn record(&self, incident: InboundIncident) {
            self.incidents.lock().unwrap().push(incident);
        }
    }

    fn dispatcher_with_incidents(
        store: Arc<InMemoryRows>,
        sink: Arc<FixedSink>,
        incidents: Arc<CollectingIncidentSink>,
    ) -> OutboxDispatcher {
        dispatcher(store, sink, 10).with_incident_sink(incidents)
    }

    #[tokio::test]
    async fn required_delivery_poisoned_records_exactly_one_incident_across_ticks() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(required_row("e1", "http://reply.example/cb", 0), t0());
        let sink = Arc::new(FixedSink::always(
            "http",
            SendOutcome::PermanentFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                "404 from the destination",
            )),
        ));
        let incidents = Arc::new(CollectingIncidentSink::default());
        let d = dispatcher_with_incidents(
            Arc::clone(&store),
            Arc::clone(&sink),
            Arc::clone(&incidents),
        );

        // Three drains: the row is poisoned to the max horizon each time and re-claimed by the
        // fixed clock, exactly as the real dispatcher re-claims it every poison_delay forever.
        for _ in 0..3 {
            d.dispatch_deployment(&dep()).await;
        }

        let recorded = incidents.recorded();
        assert_eq!(
            recorded.len(),
            1,
            "a poisoned required entry alerts ONCE, not once per retry tick: {recorded:?}"
        );
        let incident = &recorded[0];
        assert_eq!(incident.deployment, dep().value());
        assert_eq!(incident.channel, "http://reply.example/cb");
        assert_eq!(
            incident.failure_code,
            codes::OUTBOUND_REQUIRED_DELIVERY_FAILED
        );
        assert_eq!(
            incident.dedup_key, "key-e1",
            "the frozen outbox key joins the incident to the wire attempt"
        );
        assert_eq!(incident.process_id, "11111111-1111-4111-8111-111111111111");
        assert!(
            incident.detail.contains(codes::OUTBOUND_SEND_FAILED)
                && incident.detail.contains("404 from the destination"),
            "the cause is quoted in the detail: {}",
            incident.detail
        );
        assert!(
            incident.payload.is_none() && incident.headers.is_empty(),
            "an outbound incident captures no inbound payload"
        );
        // The row survives at the poison horizon with the latch persisted (still redrivable).
        let rows = store.snapshot();
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0]
                .2
                .as_deref()
                .expect("diagnostic persisted")
                .contains(INCIDENT_RECORDED_ATTR),
            "the once-only latch rides last_diagnostic_json"
        );
    }

    #[tokio::test]
    async fn a_non_required_delivery_never_records_an_incident_however_often_it_is_poisoned() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/cb", 0), t0());
        let sink = Arc::new(FixedSink::always(
            "http",
            SendOutcome::PermanentFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                "410 gone",
            )),
        ));
        let incidents = Arc::new(CollectingIncidentSink::default());
        let d = dispatcher_with_incidents(
            Arc::clone(&store),
            Arc::clone(&sink),
            Arc::clone(&incidents),
        );

        for _ in 0..2 {
            d.dispatch_deployment(&dep()).await;
        }

        assert!(
            incidents.recorded().is_empty(),
            "best-effort delivery is exactly that — the author did not ask to be alerted"
        );
    }

    #[tokio::test]
    async fn a_transient_failure_after_the_incident_does_not_re_arm_it() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(required_row("e1", "http://reply.example/cb", 0), t0());
        // Poison → transient → poison again: the latch must survive the middle defer.
        let sink = Arc::new(FixedSink::sequence(
            "http",
            vec![
                SendOutcome::PermanentFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    "permanent",
                )),
                SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    "connect reset",
                )),
            ],
            SendOutcome::PermanentFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                "permanent again",
            )),
        ));
        let incidents = Arc::new(CollectingIncidentSink::default());
        let d = dispatcher_with_incidents(
            Arc::clone(&store),
            Arc::clone(&sink),
            Arc::clone(&incidents),
        );

        for _ in 0..3 {
            d.dispatch_deployment(&dep()).await;
        }

        assert_eq!(
            incidents.recorded().len(),
            1,
            "the latch is carried forward through a transient defer"
        );
    }

    #[tokio::test]
    async fn a_required_delivery_that_succeeds_records_nothing() {
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(required_row("e1", "http://reply.example/cb", 0), t0());
        let sink = Arc::new(FixedSink::always("http", SendOutcome::Delivered));
        let incidents = Arc::new(CollectingIncidentSink::default());
        let d = dispatcher_with_incidents(
            Arc::clone(&store),
            Arc::clone(&sink),
            Arc::clone(&incidents),
        );

        d.dispatch_deployment(&dep()).await;

        assert!(store.snapshot().is_empty(), "delivered ⇒ row deleted");
        assert!(incidents.recorded().is_empty());
    }

    #[test]
    fn the_incident_latch_reads_false_on_absent_or_malformed_diagnostics() {
        let mut r = required_row("e1", "http://x/y", 0);
        assert!(!incident_recorded(&r), "no diagnostic ⇒ not yet recorded");
        r.last_diagnostic_json = Some("{not json".to_string());
        assert!(!incident_recorded(&r), "malformed ⇒ fail towards alerting");
        r.last_diagnostic_json = Some(diagnostic_json_marked(
            &Diagnostic::error(codes::OUTBOUND_SEND_FAILED, "x"),
            true,
        ));
        assert!(incident_recorded(&r));
        r.last_diagnostic_json = Some(diagnostic_json_marked(
            &Diagnostic::error(codes::OUTBOUND_SEND_FAILED, "x"),
            false,
        ));
        assert!(!incident_recorded(&r), "an unmarked defer never latches");
    }

    // ---- `sutra.outbox.retry.max-attempts` (P1-1) -------------------------------------------
    //
    // The default is RETRY FOREVER and must stay bit-for-bit unchanged, so the first test here
    // is the one that proves the ceiling is opt-in. Everything after it configures a ceiling.

    fn dispatcher_capped(
        store: Arc<InMemoryRows>,
        sink: Arc<FixedSink>,
        max_attempts: Option<i32>,
    ) -> OutboxDispatcher {
        let mut sinks = SinkRegistry::new();
        sinks.register(sink);
        OutboxDispatcher::new(
            store,
            sinks,
            RetryPolicy::new(
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(300),
                false,
            )
            .with_max_attempts(max_attempts),
            10,
        )
        .with_clock(t0)
    }

    fn failing_sink() -> Arc<FixedSink> {
        Arc::new(FixedSink::always(
            "http",
            SendOutcome::RetryableFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                "downstream 503",
            )),
        ))
    }

    #[test]
    fn a_ceiling_below_one_is_read_as_no_ceiling() {
        // "Give up before attempting anything" is never what was meant; a misconfigured 0 must
        // not silently stop the outbox delivering.
        assert_eq!(
            RetryPolicy::default()
                .with_max_attempts(None)
                .max_attempts(),
            None
        );
        assert_eq!(
            RetryPolicy::default()
                .with_max_attempts(Some(0))
                .max_attempts(),
            None
        );
        assert_eq!(
            RetryPolicy::default()
                .with_max_attempts(Some(-3))
                .max_attempts(),
            None
        );
        assert_eq!(
            RetryPolicy::default()
                .with_max_attempts(Some(1))
                .max_attempts(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn without_a_ceiling_a_failing_entry_defers_forever_exactly_as_before() {
        let store = Arc::new(InMemoryRows::default());
        // Already 999 failed attempts — with no ceiling this is still just another deferral.
        store.enqueue(row("e1", "http://reply.example/cb", 999), t0());
        let d = dispatcher_capped(Arc::clone(&store), failing_sink(), None);

        d.dispatch_deployment(&dep()).await;

        assert!(
            !store.is_terminal("e1"),
            "the retry-forever default must never mark an entry terminal"
        );
        let (deferred, due, _) = store.snapshot()[0].clone();
        assert_eq!(deferred.attempt_count, 1000, "it deferred and counted up");
        assert!(due > t0(), "and was rescheduled");
    }

    #[tokio::test]
    async fn an_entry_that_exhausts_the_ceiling_is_marked_terminal_and_never_claimed_again() {
        let store = Arc::new(InMemoryRows::default());
        // attempt_count = 2 ⇒ this delivery is attempt 3, which spends a ceiling of 3.
        store.enqueue(row("e1", "http://reply.example/cb", 2), t0());
        let d = dispatcher_capped(Arc::clone(&store), failing_sink(), Some(3));

        let stats = d.dispatch_deployment(&dep()).await;
        assert_eq!(stats.attempted, 1);
        assert_eq!(stats.failed, 1);

        assert!(store.is_terminal("e1"), "the ceiling was spent");
        // NOT deleted: at-least-once is never traded for silence — the payload stays inspectable
        // and redrivable.
        assert_eq!(store.snapshot().len(), 1, "the row survives, flagged");
        let (row_after, due, diag) = store.snapshot()[0].clone();
        assert_eq!(
            row_after.attempt_count, 2,
            "the terminal mark neither defers nor bumps the count"
        );
        assert_eq!(due, t0(), "and never moves the due time");
        let diag = diag.expect("the terminal diagnostic is recorded");
        assert!(
            diag.contains(codes::OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED),
            "{diag}"
        );
        assert!(
            diag.contains("downstream 503"),
            "the cause is quoted: {diag}"
        );

        // A second tick cannot see it at all.
        let stats = d.dispatch_deployment(&dep()).await;
        assert_eq!(stats.attempted, 0, "a terminal row is never claimed again");
    }

    #[tokio::test]
    async fn an_entry_below_the_ceiling_keeps_deferring_normally() {
        let store = Arc::new(InMemoryRows::default());
        // attempt 2 of 5 — plenty left.
        store.enqueue(row("e1", "http://reply.example/cb", 1), t0());
        let d = dispatcher_capped(Arc::clone(&store), failing_sink(), Some(5));

        d.dispatch_deployment(&dep()).await;

        assert!(!store.is_terminal("e1"));
        let (deferred, due, _) = store.snapshot()[0].clone();
        assert_eq!(deferred.attempt_count, 2);
        // attempt 2, no jitter → base << 2 = 4s.
        assert_eq!(due, t0() + time::Duration::seconds(4));
    }

    #[tokio::test]
    async fn a_permanent_failure_that_spends_the_ceiling_goes_terminal_not_to_the_horizon() {
        // A permanent failure CONSUMES an attempt like any other; when that spends the ceiling
        // the entry stops instead of parking at the poison horizon to be re-claimed forever.
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/bad", 1), t0());
        let sink = Arc::new(FixedSink::always(
            "http",
            SendOutcome::PermanentFailure(Diagnostic::error(codes::OUTBOUND_SEND_FAILED, "400")),
        ));
        let d = dispatcher_capped(Arc::clone(&store), sink, Some(2));

        d.dispatch_deployment(&dep()).await;

        assert!(store.is_terminal("e1"));
        assert_eq!(store.terminal_marks().len(), 1);
    }

    #[tokio::test]
    async fn a_non_required_entry_finally_raises_its_one_incident_when_it_is_abandoned() {
        // The capability that did not exist before: a non-required delivery never alerts on
        // poison (by design), so without a ceiling it could fail silently forever. Exhaustion is
        // the one event that must always surface.
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(row("e1", "http://reply.example/cb", 0), t0());
        let incidents = Arc::new(CollectingIncidentSink::default());
        let d = dispatcher_capped(Arc::clone(&store), failing_sink(), Some(1))
            .with_incident_sink(Arc::clone(&incidents) as Arc<dyn IncidentSink + Send + Sync>);

        d.dispatch_deployment(&dep()).await;

        let recorded = incidents.recorded();
        assert_eq!(recorded.len(), 1, "exactly one incident");
        assert_eq!(
            recorded[0].failure_code,
            codes::OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED
        );
        assert_eq!(
            recorded[0].dedup_key, "key-e1",
            "keyed by the frozen outbox_key so it joins to the wire attempts"
        );
        assert!(store.is_terminal("e1"));
    }

    #[tokio::test]
    async fn a_required_entry_that_already_alerted_does_not_alert_twice_on_exhaustion() {
        // Composition with Wave A's `required` latch: ONE incident per entry, ever. The first
        // poison alerts; reaching the ceiling later marks terminal without a duplicate alert.
        let store = Arc::new(InMemoryRows::default());
        store.enqueue(required_row("e1", "http://reply.example/cb", 0), t0());
        let incidents = Arc::new(CollectingIncidentSink::default());
        let sink = Arc::new(FixedSink::always(
            "http",
            SendOutcome::PermanentFailure(Diagnostic::error(codes::OUTBOUND_SEND_FAILED, "400")),
        ));
        // Ceiling of 3: tick 1 poisons (alerting), tick 2 poisons again, tick 3 exhausts.
        let d = dispatcher_capped(Arc::clone(&store), sink, Some(3))
            .with_incident_sink(Arc::clone(&incidents) as Arc<dyn IncidentSink + Send + Sync>);

        // Three ticks. The test clock is frozen, so each round re-arms the deferred due time —
        // standing in for wall-clock time passing between real ticks.
        for _ in 0..3 {
            store.make_all_due(t0());
            d.dispatch_deployment(&dep()).await;
        }

        assert!(store.is_terminal("e1"), "the ceiling was reached");
        assert_eq!(
            incidents.recorded().len(),
            1,
            "the required-delivery incident already fired; exhaustion must not duplicate it"
        );
        assert_eq!(
            incidents.recorded()[0].failure_code,
            codes::OUTBOUND_REQUIRED_DELIVERY_FAILED
        );
    }
}

/// The concurrent dispatcher (0.6.5): exactly-once hand-out within a process, per-instance
/// order, the local hand-off, batched deletes and unchanged failure semantics.
#[cfg(test)]
mod concurrent_tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;
    use crate::sink::MessageSink;

    fn dep() -> DeploymentId {
        DeploymentId::of("dep-0000000000000000000000c1").expect("valid deployment id")
    }

    fn row(entry_id: &str, instance: &str, destination: &str) -> ClaimedOutboxRow {
        ClaimedOutboxRow {
            entry_id: entry_id.to_string(),
            attempt_count: 0,
            destination: destination.to_string(),
            headers: BTreeMap::new(),
            body: entry_id.as_bytes().to_vec(),
            content_type: Some("text/plain".to_string()),
            outbox_key: format!("key-{entry_id}"),
            mode: ReplyMode::Native,
            cloud_event_json: None,
            auth_ref_json: None,
            labels: BTreeMap::new(),
            traceparent: None,
            node_id: None,
            instance_id: instance.to_string(),
            required: false,
            last_diagnostic_json: None,
        }
    }

    /// A table of rows with a claim that honours `exclude`, counting delete transactions.
    #[derive(Default)]
    struct Table {
        rows: Mutex<Vec<(ClaimedOutboxRow, bool)>>, // (row, deferred)
        delete_calls: Mutex<Vec<usize>>,
        deferred: Mutex<Vec<String>>,
    }

    impl Table {
        fn insert(&self, row: ClaimedOutboxRow) {
            self.rows.lock().unwrap().push((row, false));
        }
        fn remaining(&self) -> usize {
            self.rows.lock().unwrap().len()
        }
    }

    impl OutboxRowStore for Table {
        fn claim_due<'a>(
            &'a self,
            deployment: &'a DeploymentId,
            now: OffsetDateTime,
            max_entries: i64,
        ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
            self.claim_due_excluding(deployment, now, max_entries, &[])
        }

        fn claim_due_excluding<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            _now: OffsetDateTime,
            max_entries: i64,
            exclude: &'a [String],
        ) -> BoxFuture<'a, Result<Vec<ClaimedOutboxRow>, Diagnostic>> {
            Box::pin(async move {
                Ok(self
                    .rows
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(r, deferred)| !deferred && !exclude.contains(&r.entry_id))
                    .take(max_entries as usize)
                    .map(|(r, _)| r.clone())
                    .collect())
            })
        }

        fn delete<'a>(
            &'a self,
            deployment: &'a DeploymentId,
            entry_id: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            let ids = [entry_id.to_string()];
            Box::pin(async move {
                let ids = ids;
                self.delete_many(deployment, &ids).await
            })
        }

        fn delete_many<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            entry_ids: &'a [String],
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            Box::pin(async move {
                self.delete_calls.lock().unwrap().push(entry_ids.len());
                self.rows
                    .lock()
                    .unwrap()
                    .retain(|(r, _)| !entry_ids.contains(&r.entry_id));
                Ok(())
            })
        }

        fn defer<'a>(
            &'a self,
            _deployment: &'a DeploymentId,
            entry_id: &'a str,
            _new_due_at: OffsetDateTime,
            _diagnostic_json: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            Box::pin(async move {
                self.deferred.lock().unwrap().push(entry_id.to_string());
                for (r, deferred) in self.rows.lock().unwrap().iter_mut() {
                    if r.entry_id == entry_id {
                        *deferred = true;
                    }
                }
                Ok(())
            })
        }

        fn mark_poisoned<'a>(
            &'a self,
            deployment: &'a DeploymentId,
            entry_id: &'a str,
            diagnostic_json: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            self.defer(
                deployment,
                entry_id,
                OffsetDateTime::now_utc(),
                diagnostic_json,
            )
        }

        fn requeue<'a>(
            &'a self,
            deployment: &'a DeploymentId,
            entry_id: &'a str,
            new_due_at: OffsetDateTime,
            diagnostic_json: &'a str,
        ) -> BoxFuture<'a, Result<(), Diagnostic>> {
            self.defer(deployment, entry_id, new_due_at, diagnostic_json)
        }
    }

    /// Records every delivery (body = entry id) and the peak number of sends in flight.
    struct SlowSink {
        delay: std::time::Duration,
        delivered: Mutex<Vec<(String, String)>>, // (instance header, entry id)
        in_flight: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        fail: Option<String>,
    }

    impl SlowSink {
        fn new(delay_ms: u64) -> SlowSink {
            SlowSink {
                delay: std::time::Duration::from_millis(delay_ms),
                delivered: Mutex::new(Vec::new()),
                in_flight: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                fail: None,
            }
        }
    }

    impl MessageSink for SlowSink {
        fn schemes(&self) -> Vec<String> {
            vec!["test".to_string()]
        }

        fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
            Box::pin(async move {
                use std::sync::atomic::Ordering;
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(self.delay).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                let id = String::from_utf8(message.body.clone()).unwrap_or_default();
                if self.fail.as_deref() == Some(id.as_str()) {
                    return SendOutcome::RetryableFailure(Diagnostic::error(
                        codes::OUTBOUND_SEND_FAILED,
                        "down",
                    ));
                }
                let instance = message.destination.clone();
                self.delivered.lock().unwrap().push((instance, id));
                SendOutcome::Delivered
            })
        }
    }

    fn dispatcher(table: Arc<Table>, sink: Arc<SlowSink>, batch: i64) -> Arc<OutboxDispatcher> {
        let mut sinks = SinkRegistry::new();
        sinks.register(sink);
        Arc::new(OutboxDispatcher::new(
            table,
            sinks,
            RetryPolicy::new(
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(300),
                false,
            ),
            batch,
        ))
    }

    async fn wait_until(mut done: impl FnMut() -> bool) {
        for _ in 0..500 {
            if done() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("condition not reached in 5 s");
    }

    fn spawn(
        dispatcher: Arc<OutboxDispatcher>,
        workers: usize,
        signal: &OutboxSignal,
        tick_ms: u64,
    ) -> OutboxDispatcherHandle {
        spawn_concurrent_dispatch_loop(
            &tokio::runtime::Handle::current(),
            dispatcher,
            LiveDeploymentSet::new(vec![dep()]),
            std::time::Duration::from_millis(tick_ms),
            OutboxConcurrency { workers },
            signal.clone(),
        )
    }

    /// N workers, many instances: every row is delivered exactly once, deliveries really run in
    /// parallel, the deletes are batched, and nothing stays in flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_workers_deliver_every_row_exactly_once_in_parallel() {
        let table = Arc::new(Table::default());
        for i in 0..200 {
            // The destination carries the instance, so the sink can check per-instance order.
            let instance = format!("inst-{}", i % 40);
            table.insert(row(
                &format!("e{i:03}"),
                &instance,
                &format!("test://{instance}"),
            ));
        }
        let sink = Arc::new(SlowSink::new(5));
        let signal = OutboxSignal::new();
        let handle = spawn(
            dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
            8,
            &signal,
            10,
        );
        signal.wake();
        wait_until(|| table.remaining() == 0).await;
        handle.shutdown().await;
        let delivered = sink.delivered.lock().unwrap().clone();
        let mut ids: Vec<&str> = delivered.iter().map(|(_, id)| id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(delivered.len(), 200, "no row delivered twice");
        assert_eq!(ids.len(), 200, "every row delivered");
        assert!(
            sink.peak.load(std::sync::atomic::Ordering::SeqCst) > 1,
            "parallel delivery"
        );
        let calls = table.delete_calls.lock().unwrap().clone();
        assert!(calls.len() < 200, "deletes were batched: {calls:?}");
        assert_eq!(signal.inflight_count(), 0);
    }

    /// One instance's rows are delivered one after another, in claim order, whatever the
    /// worker count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_instances_rows_are_delivered_in_claim_order() {
        let table = Arc::new(Table::default());
        for i in 0..60 {
            let instance = format!("inst-{}", i % 3);
            table.insert(row(
                &format!("e{i:03}"),
                &instance,
                &format!("test://{instance}"),
            ));
        }
        let sink = Arc::new(SlowSink::new(2));
        let signal = OutboxSignal::new();
        let handle = spawn(
            dispatcher(Arc::clone(&table), Arc::clone(&sink), 7),
            8,
            &signal,
            10,
        );
        signal.wake();
        wait_until(|| table.remaining() == 0).await;
        handle.shutdown().await;
        let delivered = sink.delivered.lock().unwrap().clone();
        let mut per_instance: HashMap<String, Vec<String>> = HashMap::new();
        for (instance, id) in delivered {
            per_instance.entry(instance).or_default().push(id);
        }
        for (instance, ids) in per_instance {
            let mut sorted = ids.clone();
            sorted.sort();
            assert_eq!(ids, sorted, "{instance} delivered out of order");
        }
    }

    /// A row reserved and handed off by a step commit is delivered once — never also through
    /// the claim, even though it is due in the table the whole time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_handed_off_row_is_never_also_claimed() {
        let table = Arc::new(Table::default());
        let sink = Arc::new(SlowSink::new(20));
        let signal = OutboxSignal::new();
        // A 1 ms tick: the claim runs constantly while the hand-off is in flight.
        let handle = spawn(
            dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
            4,
            &signal,
            1,
        );
        wait_until(|| signal.accepts_hand_off()).await;
        for i in 0..30 {
            let r = row(&format!("h{i:02}"), &format!("inst-{i}"), "test://local");
            signal.reserve(std::slice::from_ref(&r.entry_id));
            table.insert(r.clone()); // the "commit"
            signal.hand_off(&dep(), vec![r]);
        }
        wait_until(|| table.remaining() == 0).await;
        handle.shutdown().await;
        assert_eq!(sink.delivered.lock().unwrap().len(), 30);
        assert_eq!(signal.inflight_count(), 0);
    }

    /// With no dispatcher running a hand-off releases the rows and wakes, so the next claim
    /// delivers them — a hand-off can never drop a row.
    #[tokio::test]
    async fn a_hand_off_without_a_dispatcher_falls_back_to_the_claim() {
        let signal = OutboxSignal::new();
        let r = row("x1", "inst", "test://local");
        signal.reserve(std::slice::from_ref(&r.entry_id));
        assert!(!signal.accepts_hand_off());
        signal.hand_off(&dep(), vec![r]);
        assert_eq!(signal.inflight_count(), 0);
    }

    /// A failed delivery is deferred exactly as by the serial loop, and released from flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_delivery_is_deferred_and_released() {
        let table = Arc::new(Table::default());
        table.insert(row("ok1", "i1", "test://i1"));
        table.insert(row("bad", "i2", "test://i2"));
        let mut slow = SlowSink::new(1);
        slow.fail = Some("bad".to_string());
        let sink = Arc::new(slow);
        let signal = OutboxSignal::new();
        let handle = spawn(
            dispatcher(Arc::clone(&table), Arc::clone(&sink), 50),
            4,
            &signal,
            10,
        );
        signal.wake();
        wait_until(|| table.deferred.lock().unwrap().contains(&"bad".to_string())).await;
        wait_until(|| table.remaining() == 1).await;
        handle.shutdown().await;
        assert_eq!(signal.inflight_count(), 0);
        assert_eq!(sink.delivered.lock().unwrap().len(), 1);
    }
}
