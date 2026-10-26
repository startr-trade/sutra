//! Audit events a transport writes — the neutral seam between a transport and the audit journal.
//!
//! The engine writes an instance's audit journal (`<q:audit>`) from execution lifecycle
//! callbacks. Some facts only a transport sees belong in that journal too: a request the
//! transport refused on a policy of its own, a session it recovered after a restart. Logging
//! them is not enough — a log line is not the journal an operator reads back for an instance.
//!
//! A transport appends such an event through the process-wide [`transport_audit`] registry:
//!
//! - to an INSTANCE's journal, when it holds the [`TransportAuditContext`] the engine stamped on
//!   the outbound message it is serving (a transport asks for that context by answering `true`
//!   from `MessageSink::writes_audit_events` / `InlineCaller::writes_audit_events`);
//! - otherwise to the DEPLOYMENT's journal, named by the module key of the channel it serves.
//!
//! The engine installs the journal at boot (and again on every activation, with the new
//! deployment set); until it does — a transport's own tests, a process with no audit sink — an
//! append is a no-op answering [`TransportAuditOutcome::NotInstalled`]. The transport depends on
//! this crate only, never on the engine.
//!
//! What the journal guarantees, whatever implements it:
//!
//! - the audit level of the instance's process applies (`<q:audit capture="…">`): no event when
//!   the process is not audited or captures `none`; the event's fields are recorded only at
//!   `payload` level, and every field the transport marked sensitive is replaced by the same
//!   placeholder an `@sensitive` variable gets ([`transport_audit_payload`]);
//! - an event type a transport writes is `<transport>.<EVENT>` ([`transport_event_type`]): the
//!   engine's own event types never carry a `.`, so a transport can never write one of them;
//! - an append never blocks: it queues the event (bounded) and answers at once; a full queue
//!   drops the event and counts the drop ([`TransportAuditRegistry::dropped_total`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};

use sutra_bpmn::qbindings::AuditCapture;

/// Reserved header the engine stamps on every request of an audited instance: the instance's
/// audit routing (`capture=<level>[;sink=<name>]`). Kept on the message only for a transport
/// that writes audit events; stripped for every other one.
pub const AUDIT_JOURNAL_HEADER: &str = "sutra-audit-journal";
/// Reserved header carrying the deployment id of the instance a request belongs to (only on a
/// message bound for a transport that writes audit events).
pub const AUDIT_JOURNAL_DEPLOYMENT_HEADER: &str = "sutra-audit-deployment";
/// Reserved header carrying the instance id a request belongs to (only on a message bound for a
/// transport that writes audit events).
pub const AUDIT_JOURNAL_INSTANCE_HEADER: &str = "sutra-audit-instance";

/// The three reserved journal headers, for stripping.
pub const AUDIT_JOURNAL_HEADERS: [&str; 3] = [
    AUDIT_JOURNAL_HEADER,
    AUDIT_JOURNAL_DEPLOYMENT_HEADER,
    AUDIT_JOURNAL_INSTANCE_HEADER,
];

/// Longest transport namespace (the part before the `.`).
const MAX_TRANSPORT_NAMESPACE: usize = 32;
/// Longest event name (the part after the `.`). With the namespace and the dot, a type stays
/// inside the journal's 128-character `event_type` column.
const MAX_EVENT_NAME: usize = 64;

/// The audit routing of the instance a request belongs to, as the engine stamped it on the
/// outbound message. A transport keeps it with the session the request opened and hands it back
/// with every event for that instance. It is opaque to the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportAuditContext {
    pub deployment_id: String,
    pub instance_id: String,
    /// The instance's process audit level.
    pub capture: AuditCapture,
    /// The audit sink the process names (`<q:audit sink>`); `None` routes to the engine's
    /// default sink.
    pub sink: Option<String>,
}

impl TransportAuditContext {
    /// The context carried by a message's headers (names matched case-insensitively), when the
    /// engine stamped one: `None` for a request of a process that is not audited, or for a
    /// transport that did not ask for it.
    pub fn from_headers(headers: &BTreeMap<String, String>) -> Option<TransportAuditContext> {
        let find = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let (capture, sink) = parse_journal_policy(&find(AUDIT_JOURNAL_HEADER)?)?;
        Some(TransportAuditContext {
            deployment_id: find(AUDIT_JOURNAL_DEPLOYMENT_HEADER)?,
            instance_id: find(AUDIT_JOURNAL_INSTANCE_HEADER)?,
            capture,
            sink,
        })
    }
}

/// The [`AUDIT_JOURNAL_HEADER`] value for a process audited at `capture` into `sink` (`None`:
/// the engine default).
pub fn journal_policy_value(capture: AuditCapture, sink: Option<&str>) -> String {
    match sink {
        Some(sink) => format!("capture={};sink={sink}", capture.as_str()),
        None => format!("capture={}", capture.as_str()),
    }
}

/// Finish the reserved journal headers of a message about to reach a transport: for a
/// transport that writes audit events, add the owning deployment and instance next to the
/// routing the engine stamped at emission (nothing when the request carries no routing — its
/// process is not audited); for any other transport, remove all three.
pub fn stamp_journal_identity(
    headers: &mut BTreeMap<String, String>,
    writes_audit_events: bool,
    deployment_id: &str,
    instance_id: &str,
) {
    if writes_audit_events && headers.contains_key(AUDIT_JOURNAL_HEADER) && !instance_id.is_empty()
    {
        headers.insert(
            AUDIT_JOURNAL_DEPLOYMENT_HEADER.to_string(),
            deployment_id.to_string(),
        );
        headers.insert(
            AUDIT_JOURNAL_INSTANCE_HEADER.to_string(),
            instance_id.to_string(),
        );
    } else {
        for name in AUDIT_JOURNAL_HEADERS {
            headers.remove(name);
        }
    }
}

/// Parse an [`AUDIT_JOURNAL_HEADER`] value.
pub fn parse_journal_policy(value: &str) -> Option<(AuditCapture, Option<String>)> {
    let mut capture = None;
    let mut sink = None;
    for part in value.split(';') {
        match part.trim().split_once('=') {
            Some(("capture", level)) => {
                capture = Some(match level.trim() {
                    "none" => AuditCapture::None,
                    "metadata" => AuditCapture::Metadata,
                    "payload" => AuditCapture::Payload,
                    "media" => AuditCapture::Media,
                    _ => return None,
                })
            }
            Some(("sink", name)) if !name.trim().is_empty() => sink = Some(name.trim().to_string()),
            _ => {}
        }
    }
    Some((capture?, sink))
}

/// Which journal an event goes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportAuditScope {
    /// The journal of the instance the context names.
    Instance(TransportAuditContext),
    /// The deployment-level journal of the deployment that runs `module_key`
    /// (`<tenant>/<module>/<version>`, the module key of the channel the transport serves):
    /// for an event no instance owns.
    Deployment { module_key: String },
}

/// One event a transport writes. Built with [`TransportAuditEvent::new`] and the `with_*`
/// methods.
#[derive(Debug, Clone, PartialEq)]
pub struct TransportAuditEvent {
    /// The transport's namespace (`[a-z][a-z0-9-]*`, at most 32 characters).
    pub transport: String,
    /// The event's name (`[A-Z][A-Z0-9_]*`, at most 64 characters).
    pub name: String,
    /// The process node the event concerns, when the transport knows it.
    pub node_id: Option<String>,
    /// A stable diagnostic code the event carries.
    pub diagnostic_code: Option<String>,
    /// The event's fields. Recorded only when the process captures at `payload` level.
    pub fields: serde_json::Map<String, serde_json::Value>,
    /// The names of the fields whose values are sensitive: recorded as the redaction
    /// placeholder, never as their value.
    pub sensitive: BTreeSet<String>,
}

impl TransportAuditEvent {
    pub fn new(transport: &str, name: &str) -> TransportAuditEvent {
        TransportAuditEvent {
            transport: transport.to_string(),
            name: name.to_string(),
            node_id: None,
            diagnostic_code: None,
            fields: serde_json::Map::new(),
            sensitive: BTreeSet::new(),
        }
    }

    pub fn with_field(mut self, name: &str, value: impl Into<serde_json::Value>) -> Self {
        self.fields.insert(name.to_string(), value.into());
        self
    }

    /// A field whose value is never recorded: the journal holds the redaction placeholder.
    pub fn with_sensitive_field(mut self, name: &str, value: impl Into<serde_json::Value>) -> Self {
        self.sensitive.insert(name.to_string());
        self.with_field(name, value)
    }

    pub fn with_node(mut self, node_id: &str) -> Self {
        self.node_id = Some(node_id.to_string());
        self
    }

    pub fn with_diagnostic_code(mut self, code: &str) -> Self {
        self.diagnostic_code = Some(code.to_string());
        self
    }

    /// The event type the journal records: `<transport>.<NAME>`.
    pub fn event_type(&self) -> Result<String, String> {
        transport_event_type(&self.transport, &self.name)
    }
}

/// `<transport>.<event>`, checked: the namespace `[a-z][a-z0-9-]*` (at most 32 characters), the
/// name `[A-Z][A-Z0-9_]*` (at most 64). The engine's own event types carry no `.`, so a type
/// built here can never be mistaken for one.
pub fn transport_event_type(transport: &str, event: &str) -> Result<String, String> {
    let namespace_ok = !transport.is_empty()
        && transport.len() <= MAX_TRANSPORT_NAMESPACE
        && transport.starts_with(|c: char| c.is_ascii_lowercase())
        && transport
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !namespace_ok {
        return Err(format!(
            "'{transport}' is not a transport namespace ([a-z][a-z0-9-]*, at most \
             {MAX_TRANSPORT_NAMESPACE} characters)"
        ));
    }
    let name_ok = !event.is_empty()
        && event.len() <= MAX_EVENT_NAME
        && event.starts_with(|c: char| c.is_ascii_uppercase())
        && event
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
    if !name_ok {
        return Err(format!(
            "'{event}' is not an audit event name ([A-Z][A-Z0-9_]*, at most {MAX_EVENT_NAME} \
             characters)"
        ));
    }
    Ok(format!("{transport}.{event}"))
}

/// Whether `event_type` is one a transport wrote (it carries the namespace `.`).
pub fn is_transport_event_type(event_type: &str) -> bool {
    event_type.contains('.')
}

/// The payload the journal records for `event` at audit level `capture`: the fields, with every
/// sensitive one replaced by the redaction placeholder, at `payload` level; `{}` at any other
/// level — exactly what the engine records for a node at those levels.
pub fn transport_audit_payload(capture: AuditCapture, event: &TransportAuditEvent) -> String {
    if capture != AuditCapture::Payload {
        return "{}".to_string();
    }
    let mut fields = event.fields.clone();
    for name in &event.sensitive {
        if let Some(value) = fields.get_mut(name) {
            *value = serde_json::Value::String(sutra_bpmn::REDACTED_PLACEHOLDER.to_string());
        }
    }
    serde_json::Value::Object(fields).to_string()
}

/// What became of an append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportAuditOutcome {
    /// Queued for the journal; written in the background.
    Queued,
    /// Not written: the audit level says so (the process is not audited, or captures `none`),
    /// or the deployment is not one the journal knows.
    Suppressed,
    /// Not written: the journal's queue was full. Counted in
    /// [`TransportAuditRegistry::dropped_total`].
    Dropped,
    /// Not written: the event is malformed (its type is not `<transport>.<EVENT>`).
    Refused(String),
    /// Not written: no journal is installed (no audit sink, or not inside an engine).
    NotInstalled,
}

/// A journal transport events are appended to. The engine implements it over its audit sinks.
/// `append` must never block: queue and answer.
pub trait TransportAuditJournal: Send + Sync {
    fn append(
        &self,
        scope: TransportAuditScope,
        event: TransportAuditEvent,
    ) -> TransportAuditOutcome;
}

/// The installed journal (at most one), swapped whole on every activation.
#[derive(Default)]
pub struct TransportAuditRegistry {
    journal: RwLock<Option<Arc<dyn TransportAuditJournal>>>,
    dropped: AtomicU64,
}

impl TransportAuditRegistry {
    pub fn new() -> TransportAuditRegistry {
        TransportAuditRegistry::default()
    }

    /// Install `journal`, replacing the one installed before.
    pub fn install(&self, journal: Arc<dyn TransportAuditJournal>) {
        *self.journal.write().unwrap_or_else(|e| e.into_inner()) = Some(journal);
    }

    /// Remove the installed journal (appends answer [`TransportAuditOutcome::NotInstalled`]).
    pub fn clear(&self) {
        *self.journal.write().unwrap_or_else(|e| e.into_inner()) = None;
    }

    pub fn is_installed(&self) -> bool {
        self.journal
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    /// Append `event` to the journal `scope` names. Never blocks.
    pub fn append(
        &self,
        scope: TransportAuditScope,
        event: TransportAuditEvent,
    ) -> TransportAuditOutcome {
        let journal = self
            .journal
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some(journal) = journal else {
            return TransportAuditOutcome::NotInstalled;
        };
        let outcome = journal.append(scope, event);
        if outcome == TransportAuditOutcome::Dropped {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        outcome
    }

    /// How many events were dropped on a full queue since the process started.
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

static REGISTRY: LazyLock<TransportAuditRegistry> = LazyLock::new(TransportAuditRegistry::new);

/// The process-wide registry the engine installs its journal into and transports append to.
pub fn transport_audit() -> &'static TransportAuditRegistry {
    &REGISTRY
}

/// A journal that writes nothing (answers [`TransportAuditOutcome::Suppressed`]).
pub struct NoopTransportAuditJournal;

impl TransportAuditJournal for NoopTransportAuditJournal {
    fn append(
        &self,
        _scope: TransportAuditScope,
        _event: TransportAuditEvent,
    ) -> TransportAuditOutcome {
        TransportAuditOutcome::Suppressed
    }
}

/// A journal that keeps every event it is given, in order, for a transport's own tests to
/// assert on. It applies no policy.
#[derive(Default)]
pub struct CapturingTransportAuditJournal {
    events: Mutex<Vec<(TransportAuditScope, TransportAuditEvent)>>,
}

impl CapturingTransportAuditJournal {
    pub fn new() -> CapturingTransportAuditJournal {
        CapturingTransportAuditJournal::default()
    }

    /// Every event appended so far, with its scope.
    pub fn events(&self) -> Vec<(TransportAuditScope, TransportAuditEvent)> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl TransportAuditJournal for CapturingTransportAuditJournal {
    fn append(
        &self,
        scope: TransportAuditScope,
        event: TransportAuditEvent,
    ) -> TransportAuditOutcome {
        if let Err(reason) = event.event_type() {
            return TransportAuditOutcome::Refused(reason);
        }
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((scope, event));
        TransportAuditOutcome::Queued
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context_headers() -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "Sutra-Audit-Journal".to_string(),
                "capture=payload;sink=jsonl".to_string(),
            ),
            (
                AUDIT_JOURNAL_DEPLOYMENT_HEADER.to_string(),
                "dep-000000000000000000000001".to_string(),
            ),
            (
                AUDIT_JOURNAL_INSTANCE_HEADER.to_string(),
                "11111111-1111-1111-1111-111111111111".to_string(),
            ),
        ])
    }

    #[test]
    fn the_context_round_trips_through_the_reserved_headers() {
        let context = TransportAuditContext::from_headers(&context_headers()).unwrap();
        assert_eq!(context.capture, AuditCapture::Payload);
        assert_eq!(context.sink.as_deref(), Some("jsonl"));
        assert_eq!(context.deployment_id, "dep-000000000000000000000001");
        assert_eq!(
            journal_policy_value(AuditCapture::Metadata, None),
            "capture=metadata"
        );
        assert_eq!(
            parse_journal_policy("capture=media"),
            Some((AuditCapture::Media, None))
        );
        assert_eq!(parse_journal_policy("capture=loud"), None);
        assert_eq!(parse_journal_policy("sink=sql"), None);
        // Without the instance identity there is no instance context.
        let mut partial = context_headers();
        partial.remove(AUDIT_JOURNAL_INSTANCE_HEADER);
        assert_eq!(TransportAuditContext::from_headers(&partial), None);
    }

    #[test]
    fn journal_identity_reaches_only_a_transport_that_writes_audit_events() {
        let routed = BTreeMap::from([(
            AUDIT_JOURNAL_HEADER.to_string(),
            "capture=metadata".to_string(),
        )]);
        let mut kept = routed.clone();
        stamp_journal_identity(&mut kept, true, "dep-1", "i-1");
        let context = TransportAuditContext::from_headers(&kept).unwrap();
        assert_eq!(
            (context.deployment_id.as_str(), context.instance_id.as_str()),
            ("dep-1", "i-1")
        );
        let mut stripped = kept.clone();
        stamp_journal_identity(&mut stripped, false, "dep-1", "i-1");
        assert!(stripped.is_empty());
        // A request of an unaudited process carries no routing, so gains no identity.
        let mut unaudited = BTreeMap::new();
        stamp_journal_identity(&mut unaudited, true, "dep-1", "i-1");
        assert!(unaudited.is_empty());
    }

    #[test]
    fn transport_event_types_are_namespaced_and_checked() {
        assert_eq!(
            transport_event_type("media", "SESSION_RECOVERED").unwrap(),
            "media.SESSION_RECOVERED"
        );
        assert!(is_transport_event_type("media.SESSION_RECOVERED"));
        assert!(!is_transport_event_type("INSTANCE_STARTED"));
        for (transport, event) in [
            ("", "X"),
            ("Media", "X"),
            ("me.dia", "X"),
            ("media", ""),
            ("media", "lower"),
            ("media", "A.B"),
            ("media", &"X".repeat(65)),
            (&"m".repeat(33), "X"),
        ] {
            assert!(
                transport_event_type(transport, event).is_err(),
                "{transport} {event}"
            );
        }
    }

    #[test]
    fn the_payload_follows_the_capture_level_and_redacts_sensitive_fields() {
        let event = TransportAuditEvent::new("media", "REQUEST_REFUSED")
            .with_field("rule", "rate-limit")
            .with_sensitive_field("destination", "+15550100");
        let payload: serde_json::Value =
            serde_json::from_str(&transport_audit_payload(AuditCapture::Payload, &event)).unwrap();
        assert_eq!(payload["rule"], "rate-limit");
        assert_eq!(payload["destination"], sutra_bpmn::REDACTED_PLACEHOLDER);
        for level in [
            AuditCapture::Metadata,
            AuditCapture::Media,
            AuditCapture::None,
        ] {
            assert_eq!(transport_audit_payload(level, &event), "{}");
        }
    }

    #[test]
    fn the_registry_is_a_no_op_until_a_journal_is_installed_and_counts_drops() {
        struct Full;
        impl TransportAuditJournal for Full {
            fn append(
                &self,
                _scope: TransportAuditScope,
                _event: TransportAuditEvent,
            ) -> TransportAuditOutcome {
                TransportAuditOutcome::Dropped
            }
        }
        let registry = TransportAuditRegistry::new();
        let scope = TransportAuditScope::Deployment {
            module_key: "t/m/1.0.0".into(),
        };
        let event = TransportAuditEvent::new("media", "X");
        assert_eq!(
            registry.append(scope.clone(), event.clone()),
            TransportAuditOutcome::NotInstalled
        );
        registry.install(Arc::new(NoopTransportAuditJournal));
        assert_eq!(
            registry.append(scope.clone(), event.clone()),
            TransportAuditOutcome::Suppressed
        );
        let capturing = Arc::new(CapturingTransportAuditJournal::new());
        registry.install(capturing.clone());
        assert_eq!(
            registry.append(scope.clone(), event.clone()),
            TransportAuditOutcome::Queued
        );
        assert!(matches!(
            registry.append(scope.clone(), TransportAuditEvent::new("media", "bad")),
            TransportAuditOutcome::Refused(_)
        ));
        assert_eq!(capturing.events().len(), 1);
        registry.install(Arc::new(Full));
        assert_eq!(
            registry.append(scope, event),
            TransportAuditOutcome::Dropped
        );
        assert_eq!(registry.dropped_total(), 1);
        registry.clear();
        assert!(!registry.is_installed());
    }
}
