//! Audit-event SPI — the `AuditSink` seam + the built-in JSONL sink.
//!
//! # Why this exists
//!
//! `<q:audit>` ([`sutra_bpmn::qbindings::AuditBinding`]) parses and the per-instance
//! `audit_seq` persists across park/resume, and `sutra audit-replay` reads a JSONL stream —
//! but nothing EMITS audit events, so `<q:audit>` is inert in production. This module supplies
//! the emit side: an [`AuditSink`] trait + a name-keyed [`AuditSinkRegistry`] the engine
//! fans a per-instance/per-node [`AuditEvent`] out to, plus the built-in [`JsonlAuditSink`]
//! (the DB + OTel sinks live in `sutra-engine`, which owns the pool + telemetry facade —
//! exactly like the outbox `MessageSink` trait here vs. its engine-side persistence impl).
//!
//! # Shape (mirrors [`crate::sink`])
//!
//! Sinks run on the tokio side (a spawned audit-dispatcher task drains a channel the
//! actor-thread listener feeds), so everything here is `Send + Sync` and the write is async via
//! the dependency-free boxed-future seam ([`crate::sink::BoxFuture`]). Audit is best-effort: a
//! sink failure is logged, never propagated into execution.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::diag::Diagnostic;
use crate::sink::BoxFuture;

/// A sink write could not be persisted (best-effort — never fails execution).
pub const AUDIT_SINK_WRITE_FAILED: &str = "SUTRA.AUDIT.SINK.WRITE_FAILED";

/// One audit record — the deployment-scoped, per-instance-sequenced event the sinks persist.
/// Maps 1:1 onto the shipped `audit_event` table (`deployment_id, instance_id, seq, at,
/// event_type, node_id, diagnostic_code, diagnostic_json, payload_json`) and, via
/// [`Self::to_jsonl`], onto the `sutra audit-replay --from-jsonl` line contract
/// (`instanceId, tenant, eventType, at, nodeId`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    /// The SINGLE sink this event routes to (`"sql"` | `"jsonl"` | `"otel"`), resolved from the
    /// process's audit policy. The dispatcher delivers the event to exactly this sink (one source of
    /// truth), never fanned. Falls back to a registered sink only if this one is absent.
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
        serde_json::Value::Object(obj).to_string()
    }
}

/// The audit-sink SPI — one configured audit target (`<q:audit sink="…">`; the default is
/// `"sql"`). Implementations run on the tokio side and answer best-effort: a failure is a
/// [`Diagnostic`] the dispatcher logs, never an execution error.
pub trait AuditSink: Send + Sync {
    /// The sink name matched against `<q:audit sink="…">` — `"jsonl"` / `"sql"` / `"otel"`.
    fn name(&self) -> &str;
    /// Persist one event. Best-effort; a returned `Err` is logged by the dispatcher.
    fn emit<'a>(&'a self, event: &'a AuditEvent) -> BoxFuture<'a, Result<(), Diagnostic>>;
}

/// Name-keyed audit sinks — the `<q:audit sink="…">` selector resolves against this. Empty
/// when a deployment configures no audit target (then audit emission is a no-op).
#[derive(Default, Clone)]
pub struct AuditSinkRegistry {
    by_name: HashMap<String, Arc<dyn AuditSink>>,
}

impl AuditSinkRegistry {
    pub fn new() -> AuditSinkRegistry {
        AuditSinkRegistry {
            by_name: HashMap::new(),
        }
    }

    /// Register a sink under its [`AuditSink::name`] (last registration wins).
    pub fn register(&mut self, sink: Arc<dyn AuditSink>) {
        self.by_name.insert(sink.name().to_string(), sink);
    }

    /// The sink for a `<q:audit sink="…">` name, if configured.
    pub fn get(&self, name: &str) -> Option<Arc<dyn AuditSink>> {
        self.by_name.get(name).cloned()
    }

    /// Every configured sink (for the "emit to all default sinks" instance-lifecycle path).
    pub fn all(&self) -> Vec<Arc<dyn AuditSink>> {
        let mut names: Vec<&String> = self.by_name.keys().collect();
        names.sort();
        names.into_iter().map(|n| self.by_name[n].clone()).collect()
    }

    /// Sorted sink names (deterministic).
    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.by_name.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

/// The built-in JSONL audit sink — appends one [`AuditEvent::to_jsonl`] line per event to a
/// target file, exactly the shape `sutra audit-replay --from-jsonl` reads back. Append is
/// guarded by a `Mutex` so concurrent emits interleave cleanly (whole lines, never torn).
pub struct JsonlAuditSink {
    path: PathBuf,
    file: Mutex<std::fs::File>,
}

impl JsonlAuditSink {
    /// Open (creating + appending to) the JSONL target file.
    pub fn open(path: impl Into<PathBuf>) -> std::io::Result<JsonlAuditSink> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(JsonlAuditSink {
            path,
            file: Mutex::new(file),
        })
    }

    /// The target file path.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl AuditSink for JsonlAuditSink {
    fn name(&self) -> &str {
        "jsonl"
    }

    fn emit<'a>(&'a self, event: &'a AuditEvent) -> BoxFuture<'a, Result<(), Diagnostic>> {
        let line = event.to_jsonl();
        Box::pin(async move {
            // A dedicated audit-dispatcher task drives this; the append is small + best-effort.
            let mut file = self.file.lock().map_err(|_| {
                Diagnostic::error(AUDIT_SINK_WRITE_FAILED, "audit file lock poisoned")
            })?;
            writeln!(file, "{line}").map_err(|e| {
                Diagnostic::error(AUDIT_SINK_WRITE_FAILED, format!("jsonl append failed: {e}"))
            })
        })
    }
}

// ---------------------------------------------------------------------------------------------
// Listener + dispatcher — bridge the executor's `ExecutionListener` seam (actor thread, sync) to
// the async sinks (tokio side) without blocking execution.
// ---------------------------------------------------------------------------------------------

use std::cell::RefCell;

use sutra_bpmn::SutraError;
use sutra_executor::listener::{ExecutionListener, InstanceEvent, TokenEvent};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// A fresh audit channel: the listener holds the [`UnboundedSender`], the dispatcher drains the
/// [`UnboundedReceiver`]. Unbounded so a listener callback NEVER blocks the actor thread; audit is
/// best-effort, and a send only fails once the dispatcher (and thus the whole engine) is gone.
pub fn audit_channel() -> (UnboundedSender<AuditEvent>, UnboundedReceiver<AuditEvent>) {
    unbounded_channel()
}

/// Spawn the audit dispatcher on `handle`: drain the channel and fan each event out to every
/// configured sink. A sink failure is logged, never propagated (audit is best-effort). The task
/// ends when every [`AuditListener`] sender has dropped (engine shutdown / flip). Takes a
/// [`tokio::runtime::Handle`] so it can be spawned from the engine's non-async actor thread.
pub fn spawn_audit_dispatcher(
    handle: &tokio::runtime::Handle,
    mut rx: UnboundedReceiver<AuditEvent>,
    registry: AuditSinkRegistry,
) -> tokio::task::JoinHandle<()> {
    handle.spawn(async move {
        while let Some(event) = rx.recv().await {
            // Single source of truth: route to the ONE sink the event names. If that sink is not
            // registered, fall back to the first registered sink (deterministic — sorted names) so
            // the trail is never silently dropped; audit is best-effort but never lost on a typo.
            let sink = match registry.get(&event.sink) {
                Some(s) => Some(s),
                None => {
                    let fallback = registry.all().into_iter().next();
                    if let Some(fb) = &fallback {
                        tracing::warn!(
                            requested = %event.sink,
                            using = %fb.name(),
                            "audit sink '{}' is not registered — routing to fallback '{}'",
                            event.sink,
                            fb.name()
                        );
                    }
                    fallback
                }
            };
            if let Some(sink) = sink {
                if let Err(d) = sink.emit(&event).await {
                    tracing::warn!(
                        code = %d.code,
                        sink = sink.name(),
                        instance = %event.instance_id,
                        event_type = %event.event_type,
                        "audit sink write failed (best-effort): {}",
                        d.message
                    );
                }
            }
        }
    })
}

/// The [`ExecutionListener`] that turns per-instance / per-node execution lifecycle callbacks into
/// [`AuditEvent`]s and enqueues them (non-blocking) for the dispatcher. Registered on the executor
/// via `TokenExecutor::builder(...).with_listener(Rc::new(listener))`, exactly like the OTel
/// metrics listener. Runs on the `Rc`/single-threaded actor thread, so per-instance seq lives in a
/// `RefCell`.
///
/// This landing emits at METADATA level (`payload_json = "{}"`) — enough for the `sutra
/// audit-replay` trail. Per-node `<q:audit capture="payload">` variable capture (with `@sensitive`
/// redaction) is the follow-on enrichment.
pub struct AuditListener {
    tx: UnboundedSender<AuditEvent>,
    /// RFC 3339 `at` supplier (injected — the engine passes a real clock, tests a fixed one).
    clock: Box<dyn Fn() -> String>,
    /// Per-instance monotonic seq (the `(deployment_id, instance_id, seq)` dedup key).
    seqs: RefCell<HashMap<String, u32>>,
}

impl AuditListener {
    pub fn new(
        tx: UnboundedSender<AuditEvent>,
        clock: impl Fn() -> String + 'static,
    ) -> AuditListener {
        AuditListener {
            tx,
            clock: Box::new(clock),
            seqs: RefCell::new(HashMap::new()),
        }
    }

    fn next_seq(&self, instance_id: &str) -> u32 {
        let mut seqs = self.seqs.borrow_mut();
        let slot = seqs.entry(instance_id.to_string()).or_insert(0);
        *slot += 1;
        *slot
    }

    /// Seed the per-instance seq high-water from a resumed snapshot's persisted `audit_seq`.
    /// The engine calls this on the actor thread right before `resume`, so the first
    /// post-resume event continues at `seq + 1` instead of restarting at 1 and colliding with
    /// the rows a prior pass already persisted (the DB sink's `(deployment_id, instance_id,
    /// seq)` uniqueness). Never regresses the counter — takes the max of the in-memory and
    /// persisted values (idempotent if the same instance is seeded twice in one engine life).
    pub fn seed(&self, instance_id: &str, seq: u32) {
        let mut seqs = self.seqs.borrow_mut();
        let slot = seqs.entry(instance_id.to_string()).or_insert(0);
        *slot = (*slot).max(seq);
    }

    /// The current per-instance seq high-water (0 when the instance is unseen). The engine
    /// reads it synchronously at suspend — after `on_instance_suspended` has fired on this same
    /// actor thread — to persist into the snapshot's `audit_seq`, so a later resume can seed
    /// continuity.
    pub fn seq_for(&self, instance_id: &str) -> u32 {
        self.seqs.borrow().get(instance_id).copied().unwrap_or(0)
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
    ) {
        let event = AuditEvent {
            sink,
            deployment_id: deployment.to_string(),
            tenant: tenant.to_string(),
            instance_id: instance_id.to_string(),
            seq: self.next_seq(instance_id),
            at: (self.clock)(),
            event_type: event_type.to_string(),
            node_id,
            diagnostic_code: None,
            diagnostic_json,
            // `<q:audit capture="payload">` supplies the redacted variable snapshot; a metadata
            // node (the default) carries `"{}"`.
            payload_json: payload_json.unwrap_or_else(|| "{}".to_string()),
        };
        // Best-effort: a full/closed channel means the dispatcher is gone — drop silently.
        let _ = self.tx.send(event);
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
        );
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
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn registry_resolves_by_name_and_lists_sorted() {
        let dir = std::env::temp_dir().join(format!("audit-reg-{}", std::process::id()));
        let sink = Arc::new(JsonlAuditSink::open(dir.join("a.jsonl")).unwrap());
        let mut reg = AuditSinkRegistry::new();
        assert!(reg.is_empty());
        reg.register(sink);
        assert_eq!(reg.names(), vec!["jsonl".to_string()]);
        assert!(reg.get("jsonl").is_some());
        assert!(reg.get("sql").is_none());
        assert_eq!(reg.all().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn jsonl_sink_appends_a_line_per_event() {
        let dir = std::env::temp_dir().join(format!("audit-jsonl-{}", std::process::id()));
        let path = dir.join("audit.jsonl");
        let sink = JsonlAuditSink::open(&path).unwrap();
        sink.emit(&sample()).await.unwrap();
        let mut second = sample();
        second.seq = 4;
        second.event_type = "NODE_LEFT".into();
        sink.emit(&second).await.unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let last: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(first["seq"], 3);
        assert_eq!(last["eventType"], "NODE_LEFT");
        let _ = std::fs::remove_dir_all(&dir);
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
        };
        let token = TokenEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: "i-1".into(),
            node_id: "Start".into(),
            node_type: "startEvent".into(),
            audit_sink: Some("jsonl".to_string()),
            payload_json: None,
        };

        // First pass: STARTED=1, NODE_ENTERED=2, SUSPENDED=3 — the persisted high-water is 3.
        let (tx, _rx) = audit_channel();
        let first = AuditListener::new(tx, || "t".to_string());
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
        let (tx2, _rx2) = audit_channel();
        let resumed = AuditListener::new(tx2, || "t".to_string());
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
        assert_eq!(resumed.seq_for("i-1"), 5);

        // Seeding never regresses the counter (idempotent re-seed within one engine life).
        resumed.seed("i-1", 2);
        assert_eq!(resumed.seq_for("i-1"), 5);
    }

    #[test]
    fn token_entered_payload_rides_into_the_audit_event_and_metadata_defaults_to_empty() {
        use std::collections::BTreeMap;
        use sutra_executor::listener::{ExecutionListener, TokenEvent};
        use sutra_executor::DeploymentId;

        let (tx, mut rx) = audit_channel();
        let listener = AuditListener::new(tx, || "t".to_string());
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
        };
        // NODE_ENTERED carries the captured (already-redacted) payload verbatim.
        listener.on_token_entered(&token);
        // NODE_LEFT is metadata-level regardless — it records "{}".
        listener.on_token_left(&token);

        let entered = rx.try_recv().unwrap();
        assert_eq!(entered.event_type, "NODE_ENTERED");
        assert_eq!(entered.payload_json, captured);
        let left = rx.try_recv().unwrap();
        assert_eq!(left.event_type, "NODE_LEFT");
        assert_eq!(left.payload_json, "{}");
    }

    #[tokio::test]
    async fn listener_emits_lifecycle_events_through_the_dispatcher_to_jsonl() {
        use std::collections::BTreeMap;
        use sutra_executor::listener::{InstanceEvent, TokenEvent};
        use sutra_executor::DeploymentId;

        let dir = std::env::temp_dir().join(format!("audit-listener-{}", std::process::id()));
        let path = dir.join("audit.jsonl");
        let mut registry = AuditSinkRegistry::new();
        registry.register(Arc::new(JsonlAuditSink::open(&path).unwrap()));
        let (tx, rx) = audit_channel();
        let handle = spawn_audit_dispatcher(&tokio::runtime::Handle::current(), rx, registry);

        let dep = DeploymentId::of("dep-000000000000000000000001").unwrap();
        let labels: BTreeMap<String, String> = [("tenant".to_string(), "acme".to_string())]
            .into_iter()
            .collect();
        let listener = AuditListener::new(tx, || "2026-07-19T00:00:00Z".to_string());

        let inst = InstanceEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: "i-1".into(),
            process_id: "p".into(),
            module_version: "1.0".into(),
            audit_sink: Some("jsonl".to_string()),
        };
        let token = TokenEvent {
            deployment: dep.clone(),
            labels: labels.clone(),
            instance_id: "i-1".into(),
            node_id: "Start".into(),
            node_type: "startEvent".into(),
            audit_sink: Some("jsonl".to_string()),
            payload_json: None,
        };
        listener.on_instance_started(&inst);
        listener.on_token_entered(&token);
        listener.on_instance_completed(&inst);
        drop(listener); // drops the only sender → the dispatcher drains + exits.
        handle.await.unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        let events: Vec<serde_json::Value> = body
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(events.len(), 3);
        // Per-instance monotonic seq, in lifecycle order.
        assert_eq!(events[0]["eventType"], "INSTANCE_STARTED");
        assert_eq!(events[0]["seq"], 1);
        assert_eq!(events[0]["tenant"], "acme");
        assert_eq!(events[1]["eventType"], "NODE_ENTERED");
        assert_eq!(events[1]["seq"], 2);
        assert_eq!(events[1]["nodeId"], "Start");
        assert_eq!(events[2]["eventType"], "INSTANCE_COMPLETED");
        assert_eq!(events[2]["seq"], 3);
        assert_eq!(events[2]["deploymentId"], "dep-000000000000000000000001");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A sink that records the `(routed-sink, event_type)` of every event it receives.
    struct RecordingSink {
        name: String,
        seen: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl AuditSink for RecordingSink {
        fn name(&self) -> &str {
            &self.name
        }
        fn emit<'a>(&'a self, e: &'a AuditEvent) -> BoxFuture<'a, Result<(), Diagnostic>> {
            self.seen
                .lock()
                .unwrap()
                .push((e.sink.clone(), e.event_type.clone()));
            Box::pin(async { Ok(()) })
        }
    }

    /// Single source of truth: the dispatcher delivers each event to the ONE sink it names, never
    /// fanned; an event naming an unregistered sink falls back to a registered one (never dropped).
    #[tokio::test]
    async fn dispatcher_routes_to_the_named_sink_only_with_fallback() {
        let sql_seen = Arc::new(Mutex::new(Vec::new()));
        let jsonl_seen = Arc::new(Mutex::new(Vec::new()));
        let mut registry = AuditSinkRegistry::new();
        registry.register(Arc::new(RecordingSink {
            name: "sql".into(),
            seen: Arc::clone(&sql_seen),
        }));
        registry.register(Arc::new(RecordingSink {
            name: "jsonl".into(),
            seen: Arc::clone(&jsonl_seen),
        }));
        let (tx, rx) = audit_channel();
        let handle = spawn_audit_dispatcher(&tokio::runtime::Handle::current(), rx, registry);

        let event = |sink: &str, event_type: &str| AuditEvent {
            sink: sink.into(),
            deployment_id: "dep-000000000000000000000001".into(),
            tenant: "acme".into(),
            instance_id: "i-1".into(),
            seq: 1,
            at: "t".into(),
            event_type: event_type.into(),
            node_id: None,
            diagnostic_code: None,
            diagnostic_json: None,
            payload_json: "{}".into(),
        };
        tx.send(event("sql", "A")).unwrap();
        tx.send(event("jsonl", "B")).unwrap();
        // "otel" is not registered → falls back to the first registered sink (sorted: "jsonl").
        tx.send(event("otel", "C")).unwrap();
        drop(tx);
        handle.await.unwrap();

        // Each named sink saw ONLY its own event — never fanned.
        assert_eq!(
            *sql_seen.lock().unwrap(),
            vec![("sql".to_string(), "A".to_string())]
        );
        // jsonl saw its own "B" plus the fallback "C" (routed by name "otel", absent).
        assert_eq!(
            *jsonl_seen.lock().unwrap(),
            vec![
                ("jsonl".to_string(), "B".to_string()),
                ("otel".to_string(), "C".to_string())
            ]
        );
    }
}
