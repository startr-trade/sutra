//! Audit events and the audit-store SPI.
//!
//! # Shape
//!
//! The [`AuditListener`] turns the executor's lifecycle callbacks into [`AuditEvent`]s on the
//! actor thread and appends each to the durable audit spool ([`crate::audit_spool`]) without
//! blocking. Before a step commits, the engine's persistence bridge waits for the spool's group
//! sync, so every event of a committed step is on disk. One shipper per configured store
//! ([`crate::audit_shipper`]) then loads the spool into that store through the [`AuditSink`]
//! seam: the JSONL file store ([`crate::audit_files::JsonlFileStore`]) lives here, the SQL and
//! OTel stores in the engine, which owns the pool and the telemetry facade.

use std::collections::HashMap;

pub use crate::audit_spool::SpoolRecord;
use crate::diag::Diagnostic;
use crate::sink::BoxFuture;

/// A sink write could not be persisted (best-effort — never fails execution).
pub const AUDIT_SINK_WRITE_FAILED: &str = "SUTRA.AUDIT.SINK.WRITE_FAILED";

/// Reserved header on every request a `capture="media"` process sends: value
/// [`AUDIT_CAPTURE_MEDIA`]. A transport that carries a media session (a call) records it when it
/// sees this mark, and reports the recording as a media reference (`kind: "media"`, `uri`,
/// `sha256`, …) that the process's audit events then carry. Other transports ignore it.
pub const AUDIT_CAPTURE_HEADER: &str = "sutra-audit-capture";
/// The [`AUDIT_CAPTURE_HEADER`] value asking for the session's media to be recorded.
pub const AUDIT_CAPTURE_MEDIA: &str = "media";

/// One audit record — the deployment-scoped, per-instance-sequenced event the sinks persist.
/// Maps 1:1 onto the shipped `audit_event` table (`deployment_id, instance_id, seq, at,
/// event_type, node_id, diagnostic_code, diagnostic_json, payload_json`) and, via
/// [`Self::to_jsonl`], onto the `sutra audit-replay --from-jsonl` line contract
/// (`instanceId, tenant, eventType, at, nodeId`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    /// The SINGLE sink this event routes to (`"sql"` | `"jsonl"` | `"otel"`), resolved from the
    /// process's audit policy. The dispatcher delivers the event to exactly this sink (one source of
    /// truth), never fanned. Empty when the deploy resolved no destination: the spool counts the
    /// event as undeliverable and never re-routes it.
    pub sink: String,
    pub deployment_id: String,
    /// The authoring tenant (from the instance labels) — the JSONL `tenant` field.
    pub tenant: String,
    pub instance_id: String,
    /// Monotonic per-instance sequence (continues across resume; the cross-replica dedup key).
    pub seq: u32,
    /// RFC 3339 event timestamp.
    pub at: String,
    /// `INSTANCE_STARTED` | `INSTANCE_COMPLETED` | `INSTANCE_SUSPENDED` | `INSTANCE_RESUMED` |
    /// `INSTANCE_FAILED` | `NODE_ENTERED` | `NODE_LEFT` | … .
    pub event_type: String,
    pub node_id: Option<String>,
    pub diagnostic_code: Option<String>,
    pub diagnostic_json: Option<String>,
    /// Capture payload — `"{}"` at metadata level, or the node's variables (with every
    /// `@sensitive` value already redacted by the listener) at payload level. Stored verbatim.
    pub payload_json: String,
    /// The instance's correlation keys at this event: each `<q:alias>` name the process declares
    /// and its current value, already redacted by the executor (a `sensitive` or
    /// redactor-controlled input shows masked). Empty for an instance with no aliases and for
    /// transport events. The JSONL line carries it as `correlation`, the OTLP record as one
    /// `correlation.<name>` attribute per key; the SQL row does not store it.
    pub correlation: std::collections::BTreeMap<String, String>,
    /// The pass that emitted the event (`None` for an event outside a pass: transport events,
    /// deployment-level and engine-written events). For each `(deployment, instance,
    /// attempt.base)` the stores keep only the latest attempt's events: an earlier attempt from
    /// the same base is one that never committed.
    pub attempt: Option<AuditAttempt>,
}

/// One pass of an instance on a lane, from its seed (a start, or a resume from a snapshot or
/// chain) to the hand-off of its seq. At most one pass from a given base commits, and it is the
/// last one from that base, so a later attempt supersedes an earlier one from the same base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditAttempt {
    /// A random id, one per pass.
    pub id: String,
    /// The instance's seq when the pass began: its events are `base + 1 …`.
    pub base: u32,
    /// When the pass began (RFC 3339): the order between two attempts from one base, with `id`
    /// breaking a tie.
    pub at: String,
}

impl AuditAttempt {
    /// Whether this attempt is later than `other` (both from the same base).
    pub fn is_later_than(&self, other: &AuditAttempt) -> bool {
        let parse = |at: &str| {
            time::OffsetDateTime::parse(at, &time::format_description::well_known::Rfc3339).ok()
        };
        match (parse(&self.at), parse(&other.at)) {
            (Some(a), Some(b)) if a != b => a > b,
            _ => (self.at.as_str(), self.id.as_str()) > (other.at.as_str(), other.id.as_str()),
        }
    }
}

impl AuditEvent {
    /// The JSON object the `sutra audit-replay --from-jsonl` reader consumes. It keys on
    /// `instanceId` / `tenant` / `eventType` / `at` / `nodeId`; `seq` + `payload` ride along for
    /// richer readers. `payload_json` is embedded as a nested object when it parses, else as a
    /// string (so a malformed payload never breaks the line).
    pub fn to_jsonl(&self) -> String {
        let payload: serde_json::Value = serde_json::from_str(&self.payload_json)
            .unwrap_or_else(|_| serde_json::Value::String(self.payload_json.clone()));
        let mut obj = serde_json::Map::new();
        obj.insert("instanceId".into(), serde_json::json!(self.instance_id));
        obj.insert("deploymentId".into(), serde_json::json!(self.deployment_id));
        obj.insert("tenant".into(), serde_json::json!(self.tenant));
        obj.insert("seq".into(), serde_json::json!(self.seq));
        obj.insert("at".into(), serde_json::json!(self.at));
        obj.insert("eventType".into(), serde_json::json!(self.event_type));
        if let Some(n) = &self.node_id {
            obj.insert("nodeId".into(), serde_json::json!(n));
        }
        if let Some(c) = &self.diagnostic_code {
            obj.insert("diagnosticCode".into(), serde_json::json!(c));
        }
        obj.insert("payload".into(), payload);
        if !self.correlation.is_empty() {
            obj.insert("correlation".into(), serde_json::json!(self.correlation));
        }
        if let Some(a) = &self.attempt {
            obj.insert("attemptId".into(), serde_json::json!(a.id));
            obj.insert("attemptBase".into(), serde_json::json!(a.base));
            obj.insert("attemptAt".into(), serde_json::json!(a.at));
        }
        serde_json::Value::Object(obj).to_string()
    }
}

/// The audit-store SPI: one configured audit store (`"sql"`, `"jsonl"`, `"otel"`). A store never
/// sees an event straight from execution: every event is first made durable in the local audit
/// spool, and the store's shipper hands it the spool's records in order, in batches.
/// Implementations run on the tokio side.
pub trait AuditSink: Send + Sync {
    /// The store name an audit destination resolves to: `"sql"` | `"jsonl"` | `"otel"`.
    fn name(&self) -> &str;
    /// Make `records` durable in the store, in order. Must be idempotent per record: the shipper
    /// hands the same records again after a crash between this call and its checkpoint, and after
    /// a failed call. `Err` means nothing may be assumed written; the shipper retries the same
    /// batch.
    fn ship<'a>(
        &'a self,
        spool_id: &'a str,
        records: &'a [SpoolRecord],
    ) -> BoxFuture<'a, Result<(), Diagnostic>>;
    /// The highest `n` of `spool_id` already durable in the store, if the store can tell. The
    /// shipper starts after the larger of this and its own checkpoint.
    fn recover<'a>(&'a self, spool_id: &'a str) -> BoxFuture<'a, Result<Option<u64>, Diagnostic>> {
        let _ = spool_id;
        Box::pin(async { Ok(None) })
    }
}

// ---------------------------------------------------------------------------------------------
// Listener + dispatcher — bridge the executor's `ExecutionListener` seam (actor thread, sync) to
// the async sinks (tokio side) without blocking execution.
// ---------------------------------------------------------------------------------------------

use std::cell::RefCell;

use sutra_bpmn::SutraError;
use sutra_executor::listener::{ExecutionListener, InstanceEvent, TokenEvent};

use crate::audit_spool::AuditSpoolSender;

/// A random attempt id (UUID v4 text, from the OS random source).
fn new_attempt_id() -> String {
    let mut bytes = [0u8; 16];
    if getrandom::getrandom(&mut bytes).is_err() {
        // No OS randomness: fall back to the clock and a process counter, still unique here.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        bytes[..8].copy_from_slice(&(nanos as u64).to_be_bytes());
        bytes[8..].copy_from_slice(&n.to_be_bytes());
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// The default `sutra.audit.batch-max`: the most spool records a shipper hands its store in one
/// [`AuditSink::ship`]. 256 events is about 25 process instances' worth of metadata-level audit;
/// past that the commit is no longer the cost that dominates a write.
pub const AUDIT_BATCH_MAX: usize = 256;

/// The [`ExecutionListener`] that turns per-instance / per-node execution lifecycle callbacks into
/// [`AuditEvent`]s and appends them (non-blocking) to the audit spool. Registered on the executor
/// via `TokenExecutor::builder(...).with_listener(Rc::new(listener))`, exactly like the OTel
/// metrics listener. Runs on the `Rc`/single-threaded actor thread, so per-instance seq lives in a
/// `RefCell`.
///
/// This landing emits at METADATA level (`payload_json = "{}"`) — enough for the `sutra
/// audit-replay` trail. Per-node `<q:audit capture="payload">` variable capture (with `@sensitive`
/// redaction) is the follow-on enrichment.
pub struct AuditListener {
    spool: AuditSpoolSender,
    /// RFC 3339 `at` supplier (injected — the engine passes a real clock, tests a fixed one).
    clock: Box<dyn Fn() -> String>,
    /// Per-instance monotonic seq (the `(deployment_id, instance_id, seq)` dedup key), for the
    /// instances RUNNING on this lane only. An entry is dropped when its instance completes and
    /// when its high-water is handed to a snapshot or a chain ([`Self::take_seq`]): every resume
    /// re-seeds it from what was persisted ([`Self::seed`]), and resumes land on whichever lane
    /// the instance id hashes to, so an entry kept past the hand-off would never be read again
    /// and the map would grow by one entry per instance for the life of the process.
    seqs: RefCell<HashMap<String, InstanceSeq>>,
    /// The lane's attempt tracker. With one, every event carries its pass's attempt and the
    /// shippers hold it until its commit marker; without one (no persistence: nothing commits),
    /// events carry no attempt and ship at once.
    tracker: Option<crate::audit_attempts::AuditAttemptTracker>,
}

/// An instance's seq on this lane, and the pass (attempt) its next events belong to.
#[derive(Default)]
struct InstanceSeq {
    seq: u32,
    /// Opened by the pass's first event, closed by a seed (a new pass begins).
    attempt: Option<AuditAttempt>,
}

impl AuditListener {
    pub fn new(spool: AuditSpoolSender, clock: impl Fn() -> String + 'static) -> AuditListener {
        AuditListener {
            spool,
            clock: Box::new(clock),
            seqs: RefCell::new(HashMap::new()),
            tracker: None,
        }
    }

    /// Tag events with their pass's attempt, registered in `tracker` (the lane's, shared with
    /// its persistence bridge, which settles the attempts at each commit).
    #[must_use]
    pub fn with_attempt_tracker(
        mut self,
        tracker: crate::audit_attempts::AuditAttemptTracker,
    ) -> AuditListener {
        self.tracker = Some(tracker);
        self
    }

    /// The lane finished a request: an attempt still open reached no commit (the pass wrote
    /// nothing to the database). It is committed when the request succeeded and aborted when it
    /// failed.
    pub fn settle_open_attempts(&self, succeeded: bool) {
        if let Some(tracker) = &self.tracker {
            tracker.settle(&self.spool, succeeded);
        }
    }

    /// The next seq of `instance_id`, and the attempt it belongs to (with a tracker): the pass's
    /// open attempt, or a new one when there is none or it was settled (a commit closes it), with
    /// the seq before this event as its base.
    fn next_seq(&self, deployment: &str, instance_id: &str) -> (u32, Option<AuditAttempt>) {
        let mut seqs = self.seqs.borrow_mut();
        let slot = seqs.entry(instance_id.to_string()).or_default();
        let attempt = self.tracker.as_ref().map(|tracker| {
            let open = slot
                .attempt
                .as_ref()
                .is_some_and(|a| tracker.is_open(&a.id));
            if !open {
                let attempt = AuditAttempt {
                    id: new_attempt_id(),
                    base: slot.seq,
                    at: (self.clock)(),
                };
                tracker.register(deployment, instance_id, &attempt);
                slot.attempt = Some(attempt);
            }
            slot.attempt.clone().expect("set above")
        });
        slot.seq += 1;
        (slot.seq, attempt)
    }

    /// Seed the per-instance seq high-water from a resumed snapshot's persisted `audit_seq`.
    /// The engine calls this on the actor thread right before `resume`, so the first
    /// post-resume event continues at `seq + 1` instead of restarting at 1 and colliding with
    /// the rows a prior pass already persisted (the DB sink's `(deployment_id, instance_id,
    /// seq)` uniqueness). A seed begins a pass: the counter is SET to the persisted value, even
    /// below what this lane last emitted. Everything above it belongs to a pass that never
    /// committed (its high-water would have been persisted), so the re-run numbers its events
    /// from there again under a new attempt, which supersedes the lost one.
    pub fn seed(&self, instance_id: &str, seq: u32) {
        let mut seqs = self.seqs.borrow_mut();
        let slot = seqs.entry(instance_id.to_string()).or_default();
        slot.seq = seq;
        // A seed begins a pass: its events belong to a new attempt.
        slot.attempt = None;
    }

    /// The current per-instance seq high-water (0 when the instance is unseen). The engine
    /// reads it synchronously at suspend — after `on_instance_suspended` has fired on this same
    /// actor thread — to persist into the snapshot's `audit_seq`, so a later resume can seed
    /// continuity.
    pub fn seq_for(&self, instance_id: &str) -> u32 {
        self.seqs.borrow().get(instance_id).map_or(0, |s| s.seq)
    }

    /// [`Self::seq_for`], and forget the instance: its high-water now travels with the snapshot
    /// (or chain) the caller is building, and the next pass of the instance, on this lane or
    /// another, seeds from that. The engine calls it where it persists or hands off the seq.
    pub fn take_seq(&self, instance_id: &str) -> u32 {
        self.seqs
            .borrow_mut()
            .remove(instance_id)
            .map_or(0, |s| s.seq)
    }

    /// How many instances this listener holds a seq for (tests and diagnostics): the instances
    /// mid-pass on this lane, not every instance it has seen.
    pub fn tracked_instances(&self) -> usize {
        self.seqs.borrow().len()
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(
        &self,
        sink: String,
        deployment: &str,
        tenant: &str,
        instance_id: &str,
        event_type: &str,
        node_id: Option<String>,
        diagnostic_json: Option<String>,
        payload_json: Option<String>,
        correlation: &std::collections::BTreeMap<String, String>,
    ) {
        let (seq, attempt) = self.next_seq(deployment, instance_id);
        if let (Some(tracker), Some(a)) = (&self.tracker, &attempt) {
            tracker.observe(&a.id, seq, event_type);
        }
        let event = AuditEvent {
            sink,
            deployment_id: deployment.to_string(),
            tenant: tenant.to_string(),
            instance_id: instance_id.to_string(),
            seq,
            at: (self.clock)(),
            event_type: event_type.to_string(),
            node_id,
            diagnostic_code: None,
            diagnostic_json,
            // `<q:audit capture="payload">` supplies the redacted variable snapshot; a metadata
            // node (the default) carries `"{}"`.
            payload_json: payload_json.unwrap_or_else(|| "{}".to_string()),
            correlation: correlation.clone(),
            attempt,
        };
        // Never blocks; the step's commit waits for the spool's sync (the bridge's barrier).
        self.spool.append(event);
    }

    fn tenant_of(event: &InstanceEvent) -> &str {
        event.labels.get("tenant").map(String::as_str).unwrap_or("")
    }
    fn tenant_of_token(event: &TokenEvent) -> &str {
        event.labels.get("tenant").map(String::as_str).unwrap_or("")
    }
}

impl ExecutionListener for AuditListener {
    fn on_instance_started(&self, e: &InstanceEvent) {
        let Some(sink) = e.audit_sink.clone() else {
            return;
        };
        self.emit(
            sink,
            e.deployment.value(),
            Self::tenant_of(e),
            &e.instance_id,
            "INSTANCE_STARTED",
            None,
            None,
            None,
            &e.correlation,
        );
    }
    fn on_instance_completed(&self, e: &InstanceEvent) {
        let Some(sink) = e.audit_sink.clone() else {
            return;
        };
        self.emit(
            sink,
            e.deployment.value(),
            Self::tenant_of(e),
            &e.instance_id,
            "INSTANCE_COMPLETED",
            None,
            None,
            None,
            &e.correlation,
        );
        // The instance is finished: nothing emits for it again.
        self.seqs.borrow_mut().remove(&e.instance_id);
    }
    fn on_instance_suspended(&self, e: &InstanceEvent) {
        let Some(sink) = e.audit_sink.clone() else {
            return;
        };
        self.emit(
            sink,
            e.deployment.value(),
            Self::tenant_of(e),
            &e.instance_id,
            "INSTANCE_SUSPENDED",
            None,
            None,
            None,
            &e.correlation,
        );
    }
    fn on_instance_resumed(&self, e: &InstanceEvent) {
        let Some(sink) = e.audit_sink.clone() else {
            return;
        };
        self.emit(
            sink,
            e.deployment.value(),
            Self::tenant_of(e),
            &e.instance_id,
            "INSTANCE_RESUMED",
            None,
            None,
            None,
            &e.correlation,
        );
    }
    fn on_instance_failed(&self, e: &InstanceEvent, diagnostic: &SutraError) {
        let Some(sink) = e.audit_sink.clone() else {
            return;
        };
        self.emit(
            sink,
            e.deployment.value(),
            Self::tenant_of(e),
            &e.instance_id,
            "INSTANCE_FAILED",
            None,
            Some(serde_json::json!({ "error": diagnostic.to_string() }).to_string()),
            None,
            &e.correlation,
        );
    }
    fn on_token_entered(&self, e: &TokenEvent) {
        // `audit_sink == None` = suppressed (node `capture="none"`) or process not audited.
        let Some(sink) = e.audit_sink.clone() else {
            return;
        };
        // A payload-level process rides the redacted variable snapshot on NODE_ENTERED.
        self.emit(
            sink,
            e.deployment.value(),
            Self::tenant_of_token(e),
            &e.instance_id,
            "NODE_ENTERED",
            Some(e.node_id.clone()),
            None,
            e.payload_json.clone(),
            &e.correlation,
        );
    }
    fn on_token_left(&self, e: &TokenEvent) {
        let Some(sink) = e.audit_sink.clone() else {
            return;
        };
        self.emit(
            sink,
            e.deployment.value(),
            Self::tenant_of_token(e),
            &e.instance_id,
            "NODE_LEFT",
            Some(e.node_id.clone()),
            None,
            None,
            &e.correlation,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::audit_spool::{AuditSpool, AuditSpoolConfig};

    /// A spool in a fresh temporary directory that takes every store's events.
    fn test_spool() -> (AuditSpool, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("sutra-audit-listener-{}", uuid::Uuid::new_v4()));
        let spool = AuditSpool::open(AuditSpoolConfig {
            dir: dir.clone(),
            sync_linger: std::time::Duration::ZERO,
            sinks: ["jsonl", "sql", "otel"]
                .map(String::from)
                .into_iter()
                .collect(),
            ..AuditSpoolConfig::default()
        })
        .unwrap();
        (spool, dir)
    }

    /// Everything appended to `spool` so far, once it is synced.
    fn spooled(spool: &AuditSpool) -> Vec<AuditEvent> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(spool.sender().barrier()).unwrap();
        spool
            .reader(0)
            .next_batch(10_000, spool.durable_through())
            .unwrap()
            .into_iter()
            .map(|r| r.event)
            .collect()
    }

    fn sample() -> AuditEvent {
        AuditEvent {
            sink: "sql".into(),
            deployment_id: "dep-000000000000000000000001".into(),
            tenant: "acme".into(),
            instance_id: "11111111-1111-1111-1111-111111111111".into(),
            seq: 3,
            at: "2026-07-19T12:00:00Z".into(),
            event_type: "NODE_ENTERED".into(),
            node_id: Some("Start".into()),
            diagnostic_code: None,
            diagnostic_json: None,
            payload_json: "{\"amount\":10}".into(),
            correlation: Default::default(),
            attempt: None,
        }
    }

    #[test]
    fn jsonl_line_carries_the_replay_contract_keys() {
        let v: serde_json::Value = serde_json::from_str(&sample().to_jsonl()).unwrap();
        // The keys `sutra audit-replay --from-jsonl` reads.
        assert_eq!(v["instanceId"], "11111111-1111-1111-1111-111111111111");
        assert_eq!(v["tenant"], "acme");
        assert_eq!(v["eventType"], "NODE_ENTERED");
        assert_eq!(v["at"], "2026-07-19T12:00:00Z");
        assert_eq!(v["nodeId"], "Start");
        // Richer fields for DB/OTel-parity readers.
        assert_eq!(v["seq"], 3);
        assert_eq!(v["payload"]["amount"], 10);
    }

    #[test]
    fn malformed_payload_embeds_as_a_string_not_a_parse_error() {
        let mut e = sample();
        e.payload_json = "not json".into();
        let v: serde_json::Value = serde_json::from_str(&e.to_jsonl()).unwrap();
        assert_eq!(v["payload"], "not json");
    }

    #[test]
    fn jsonl_line_carries_correlation_keys_only_when_present() {
        let v: serde_json::Value = serde_json::from_str(&sample().to_jsonl()).unwrap();
        assert!(v.get("correlation").is_none(), "no aliases, no key: {v}");
        let mut e = sample();
        e.correlation.insert("orderRef".into(), "R-1".into());
        e.correlation
            .insert("holder".into(), sutra_bpmn::REDACTED_PLACEHOLDER.into());
        let v: serde_json::Value = serde_json::from_str(&e.to_jsonl()).unwrap();
        assert_eq!(v["correlation"]["orderRef"], "R-1");
        assert_eq!(v["correlation"]["holder"], sutra_bpmn::REDACTED_PLACEHOLDER);
    }

    #[test]
    fn listener_events_carry_the_executor_correlation_keys() {
        use std::collections::BTreeMap;
        use sutra_executor::listener::{ExecutionListener, InstanceEvent, TokenEvent};
        use sutra_executor::DeploymentId;

        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let keys: BTreeMap<String, String> = [("orderRef".to_string(), "R-7".to_string())]
            .into_iter()
            .collect();
        let inst = InstanceEvent {
            deployment: dep.clone(),
            labels: BTreeMap::new(),
            instance_id: "i-9".into(),
            process_id: "p".into(),
            module_version: "1.0".into(),
            audit_sink: Some("jsonl".to_string()),
            correlation: keys.clone(),
        };
        let token = TokenEvent {
            deployment: dep,
            labels: BTreeMap::new(),
            instance_id: "i-9".into(),
            node_id: "Start".into(),
            node_type: "startEvent".into(),
            audit_sink: Some("jsonl".to_string()),
            payload_json: None,
            correlation: keys.clone(),
        };
        let (spool, dir) = test_spool();
        let listener = AuditListener::new(spool.sender(), || "t".to_string());
        listener.on_instance_started(&inst);
        listener.on_token_entered(&token);
        listener.on_instance_completed(&inst);
        let events = spooled(&spool);
        assert_eq!(events.len(), 3);
        for event in events {
            assert_eq!(event.correlation, keys);
        }
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn seed_makes_seq_continue_across_a_simulated_restart() {
        use std::collections::BTreeMap;
        use sutra_executor::listener::{ExecutionListener, InstanceEvent, TokenEvent};
        use sutra_executor::DeploymentId;

        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let labels: BTreeMap<String, String> = [("tenant".to_string(), "acme".to_string())]
            .into_iter()
            .collect();
        let inst = InstanceEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: "i-1".into(),
            process_id: "p".into(),
            module_version: "1.0".into(),
            audit_sink: Some("jsonl".to_string()),
            correlation: Default::default(),
        };
        let token = TokenEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: "i-1".into(),
            node_id: "Start".into(),
            node_type: "startEvent".into(),
            audit_sink: Some("jsonl".to_string()),
            payload_json: None,
            correlation: Default::default(),
        };

        // First pass: STARTED=1, NODE_ENTERED=2, SUSPENDED=3 — the persisted high-water is 3.
        let (spool, dir) = test_spool();
        let first = AuditListener::new(spool.sender(), || "t".to_string());
        first.on_instance_started(&inst);
        first.on_token_entered(&token);
        first.on_instance_suspended(&inst);
        assert_eq!(
            first.seq_for("i-1"),
            3,
            "high-water persisted into the snapshot"
        );

        // Restart / deploy-flip: a FRESH listener has no memory — unseeded it would restart at 1
        // and collide with the rows seq 1..3 already persisted.
        let resumed = AuditListener::new(spool.sender(), || "t".to_string());
        assert_eq!(
            resumed.seq_for("i-1"),
            0,
            "fresh listener — no in-memory seq"
        );

        // Seed from the snapshot's audit_seq, then resume: the next event continues at 4, not 1.
        resumed.seed("i-1", 3);
        assert_eq!(resumed.seq_for("i-1"), 3);
        resumed.on_instance_resumed(&inst);
        assert_eq!(
            resumed.seq_for("i-1"),
            4,
            "RESUMED continues the seq, never collides"
        );
        resumed.on_instance_completed(&inst);
        let seqs: Vec<u32> = spooled(&spool).iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5], "COMPLETED takes the next seq");
        assert_eq!(
            resumed.tracked_instances(),
            0,
            "a completed instance is forgotten"
        );
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A pass's events share one attempt, opened by its first event with the seq before
    /// it as the base; a seed begins a new attempt (a re-run from the same snapshot gets a new
    /// id and the same base), and a hand-off ends the pass.
    #[test]
    fn attempt_opens_at_first_event_and_closes_at_handoff() {
        use sutra_executor::listener::{ExecutionListener, InstanceEvent};
        use sutra_executor::DeploymentId;

        let inst = InstanceEvent {
            deployment: DeploymentId::of("dep-000000000000000000000001").unwrap(),
            labels: Default::default(),
            instance_id: "i-1".into(),
            process_id: "p".into(),
            module_version: "1.0".into(),
            audit_sink: Some("jsonl".to_string()),
            correlation: Default::default(),
        };
        let (spool, dir) = test_spool();
        let lane = AuditListener::new(spool.sender(), || "2026-11-24T10:00:00Z".to_string())
            .with_attempt_tracker(crate::audit_attempts::AuditAttemptTracker::new());
        lane.on_instance_started(&inst);
        lane.on_instance_suspended(&inst);
        assert_eq!(lane.take_seq("i-1"), 2);
        // First resume from the snapshot (seq 2): its commit is lost.
        lane.seed("i-1", 2);
        lane.on_instance_resumed(&inst);
        // The re-run from the same snapshot.
        lane.seed("i-1", 2);
        lane.on_instance_resumed(&inst);
        lane.on_instance_completed(&inst);
        let events = spooled(&spool);
        let attempts: Vec<(u32, u32, String)> = events
            .iter()
            .map(|e| {
                let a = e.attempt.clone().expect("every pass event has an attempt");
                (e.seq, a.base, a.id)
            })
            .collect();
        assert_eq!(attempts.len(), 5);
        assert_eq!((attempts[0].0, attempts[0].1), (1, 0));
        assert_eq!(attempts[0].2, attempts[1].2, "one attempt per pass");
        assert_eq!((attempts[2].0, attempts[2].1), (3, 2), "the lost pass");
        assert_eq!(
            (attempts[3].0, attempts[3].1),
            (3, 2),
            "the re-run: same seq, same base"
        );
        assert_ne!(attempts[2].2, attempts[3].2, "a new attempt id");
        assert_eq!(attempts[3].2, attempts[4].2);
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The per-instance seq map holds only the instances mid-pass: an instance that parks (its
    /// high-water taken into the snapshot) and later resumes and completes on ANOTHER lane leaves
    /// nothing behind on either, however many instances run. Unbounded before 0.8.6: one entry per
    /// instance ever started, for the life of the process.
    #[test]
    fn the_seq_map_is_empty_after_many_instances_park_resume_elsewhere_and_complete() {
        use sutra_executor::listener::{ExecutionListener, InstanceEvent};
        use sutra_executor::DeploymentId;

        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let (spool, dir) = test_spool();
        let start_lane = AuditListener::new(spool.sender(), || "t".to_string());
        let owner_lane = AuditListener::new(spool.sender(), || "t".to_string());
        for n in 0..10_000 {
            let inst = InstanceEvent {
                deployment: dep.clone(),
                labels: Default::default(),
                instance_id: format!("i-{n}"),
                process_id: "p".into(),
                module_version: "1.0".into(),
                audit_sink: Some("jsonl".to_string()),
                correlation: Default::default(),
            };
            start_lane.on_instance_started(&inst);
            start_lane.on_instance_suspended(&inst);
            // The suspend snapshot takes the high-water, as the dispatcher does.
            let persisted = start_lane.take_seq(&inst.instance_id);
            assert_eq!(persisted, 2);
            owner_lane.seed(&inst.instance_id, persisted);
            owner_lane.on_instance_resumed(&inst);
            owner_lane.on_instance_completed(&inst);
        }
        assert_eq!(start_lane.tracked_instances(), 0);
        assert_eq!(owner_lane.tracked_instances(), 0);
        // And the seqs stayed continuous: STARTED 1, SUSPENDED 2, RESUMED 3, COMPLETED 4.
        let seqs: Vec<u32> = spooled(&spool).iter().take(4).map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn token_entered_payload_rides_into_the_audit_event_and_metadata_defaults_to_empty() {
        use std::collections::BTreeMap;
        use sutra_executor::listener::{ExecutionListener, TokenEvent};
        use sutra_executor::DeploymentId;

        let (spool, dir) = test_spool();
        let listener = AuditListener::new(spool.sender(), || "t".to_string());
        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let labels: BTreeMap<String, String> = [("tenant".to_string(), "acme".to_string())]
            .into_iter()
            .collect();
        let captured = r#"{"amount":"100","ssn":"***REDACTED***"}"#;
        let token = TokenEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: "i-1".into(),
            node_id: "T".into(),
            node_type: "serviceTask".into(),
            audit_sink: Some("jsonl".to_string()),
            payload_json: Some(captured.to_string()),
            correlation: Default::default(),
        };
        // NODE_ENTERED carries the captured (already-redacted) payload verbatim.
        listener.on_token_entered(&token);
        // NODE_LEFT is metadata-level regardless — it records "{}".
        listener.on_token_left(&token);

        let mut events = spooled(&spool).into_iter();
        let entered = events.next().unwrap();
        assert_eq!(entered.event_type, "NODE_ENTERED");
        assert_eq!(entered.payload_json, captured);
        let left = events.next().unwrap();
        assert_eq!(left.event_type, "NODE_LEFT");
        assert_eq!(left.payload_json, "{}");
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }
}
