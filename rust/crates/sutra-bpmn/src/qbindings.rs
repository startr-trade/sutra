//! Parsed `q:` extension bindings attached to a BPMN node — the per-element `q:` shapes
//! declared in `xsd/q.xsd`.
//!
//! Each binding carries the parsed attribute values verbatim — no normalization beyond the
//! schema-declared defaults, applied at construction so downstream executor wiring can rely
//! on a populated value.

/// Inbound-channel ack semantics per `xsd/q.xsd#AckMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    OnPersist,
    OnComplete,
}

/// GDPR data-class tag per `xsd/q.xsd#DataClass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataClass {
    None,
    Pii,
    Pci,
    Phi,
    Financial,
}

/// Dispatch fallback per `xsd/q.xsd#OnNoMatch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnNoMatch {
    Error,
    Skip,
}

/// Alias conflict policy per `xsd/q.xsd#AliasConflict`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasConflict {
    Reject,
    Correlate,
}

/// Audit capture per `xsd/q.xsd#AuditCapture`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditCapture {
    None,
    Metadata,
    Payload,
}

/// Validation policy on payload structural failure per `xsd/q.xsd#OnValidationMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnValidationMode {
    Route,
    Reject,
    Error,
}

/// Outbound auth scheme per `xsd/q.xsd#OutboundAuthScheme`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundAuthScheme {
    Mtls,
    Bearer,
    Apikey,
}

/// How an outbound reply/send renders on the wire — the reply-mode selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyMode {
    Native,
    CloudeventBinary,
    CloudeventStructured,
    MatchInbound,
}

/// Parsed `<q:simpleValidator ref="…" path="…"/>` — a field content validator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleValidator {
    pub reference: String,
    pub path: String,
}

/// Parsed `<q:header name="…" value="<FEEL>"/>` — an author-declared header attribute set on an
/// outbound `<q:send>` / `<q:reply>` message. [`Self::value`] is a FEEL
/// expression evaluated against the sending process context at dispatch; the resolved string lands
/// as a transport header / broker application-property (the traceparent / `sutra-outbox-key`
/// carriage). Domain-neutral: [`Self::name`] is an author-declared string — no domain semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderAttr {
    pub name: String,
    /// FEEL expression over the sending process context.
    pub value: String,
}

/// Parsed `<q:source>` — the consolidated inbound trigger on a Start Event.
///
/// Declares where the start event listens ([`Self::channel`]), what message type it handles
/// ([`Self::message_type_value`] / [`Self::message_type_pattern`]), the payload variable
/// [`Self::name`], the validator chain, and per-channel inbound semantics. The codec is NOT
/// declared here — it is bound on the channel YAML (YAML-authoritative).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceBinding {
    pub channel: String,
    /// Payload variable name; defaults to `"payload"`.
    pub name: String,
    pub ack: AckMode,
    /// `<q:source dedupKey>`: an expression that extracts a **duplicate-detection** value
    /// (`header.<field>`, `ce.id`, or `body.<path>`) for inbox dedup. Renamed from the misnamed
    /// `idempotencyKey` — a dedup key detects redelivery; it does NOT assert idempotency (that is
    /// the process-level `<q:process idempotent>` boolean). A `body.<path>` form drives inbox dedup
    /// post-decode; `header.*` / `ce.id` forms are resolved transport-side.
    pub dedup_key: Option<String>,
    pub message_type: Option<String>,
    pub data_class: DataClass,
    pub complex_validators: Vec<String>,
    pub simple_validators: Vec<SimpleValidator>,
    /// `<q:redactors><q:redactor ref="…"/></q:redactors>` — names of registered `ContentRedactor`s
    /// run over the decoded payload to locate sensitive spans (masked in observability, marked for
    /// encryption at rest). Process-level `<q:redactors>` inherit down like validators.
    pub redactors: Vec<String>,
    pub message_type_value: Option<String>,
    pub message_type_pattern: Option<String>,
}

/// Parsed `<q:case>` — one row of a [`DispatchTable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseEntry {
    pub when: String,
    pub called_element: String,
}

/// Parsed `<q:dispatch>` — drives a call activity's `calledElement` choice at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchTable {
    pub default_called_element: Option<String>,
    pub on_no_match: OnNoMatch,
    pub cases: Vec<CaseEntry>,
}

/// Parsed `<q:alias>` — an alias key derived from a FEEL expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasBinding {
    pub name: String,
    pub expression: String,
    pub unique: bool,
    pub on_conflict: Option<AliasConflict>,
    pub multi: bool,
}

/// Parsed `<q:reply>` — an outbound reply on a Service Task or End Event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyBinding {
    pub mode: ReplyMode,
    pub destination: Option<String>,
    pub content_type: Option<String>,
    pub required: bool,
    pub ce_type: Option<String>,
    pub ce_source: Option<String>,
    pub ce_subject: Option<String>,
    pub ce_data_content_type: Option<String>,
    pub auth: Option<OutboundAuthScheme>,
    pub auth_secret_ref: Option<String>,
    pub auth_header: Option<String>,
    pub message_type: Option<String>,
    /// Respond-and-continue (`@continue="true"`): flush this reply when the task completes, then
    /// park + self-resume the remaining nodes asynchronously. Meaningful on a non-terminal
    /// `serviceTask`; `false` is the synchronous reply (caller waits for completion).
    pub continue_after: bool,
    /// Author-declared `<q:header>` attributes; each `value` is FEEL over the
    /// sending process context, resolved at dispatch and carried as a transport header. A reply leg
    /// is itself a coverage hop, so header carriage lands here symmetrically with `<q:send>`.
    pub headers: Vec<HeaderAttr>,
}

/// Parsed `<q:send>` — an unsolicited outbound message (emit-and-continue). Exactly one of
/// [`Self::destination`] / [`Self::channel`] is present (the parser fails closed otherwise).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendBinding {
    pub mode: ReplyMode,
    pub destination: Option<String>,
    pub channel: Option<String>,
    pub content_type: Option<String>,
    pub ce_type: Option<String>,
    pub ce_source: Option<String>,
    pub ce_subject: Option<String>,
    pub ce_data_content_type: Option<String>,
    pub auth: Option<OutboundAuthScheme>,
    pub auth_secret_ref: Option<String>,
    pub auth_header: Option<String>,
    pub message_type: Option<String>,
    /// Author-declared `<q:header>` attributes; each `value` is FEEL over the
    /// sending process context, resolved at dispatch and carried as a transport header so a hop
    /// `key` can name a header the sender sets and the receiver correlates on.
    pub headers: Vec<HeaderAttr>,
}

/// Parsed per-element `<q:audit>` — audit-sink targeting + capture level for one flow node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditBinding {
    /// Defaults to `"sql"` per `xsd/q.xsd#AuditType`.
    pub sink: String,
    pub target: Option<String>,
    pub capture: AuditCapture,
}

/// Parsed `<q:onValidation>` — payload-failure policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnValidationBinding {
    pub mode: OnValidationMode,
    pub error_code: Option<String>,
}

/// Parsed `<q:timeout duration="PT30S"/>` — the attribute form of a
/// timer boundary on a channel-call task. The loader synthesizes an interrupting timer
/// boundary node (`<taskId>#timeout`) from it, so the executor sees ONE timer shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutBinding {
    /// ISO-8601 duration (validated parseable at load time).
    pub duration: String,
}

/// Parsed `<q:retry>` — the per-task retry policy on a `serviceTask`: a REGISTERED-TASK
/// service task, or a CHANNEL-CALL service task (F1 — retry reachability).
///
/// On a registered task it is the declared curve for re-attempting a task function that failed
/// with an uncaught error (`TaskError::Failed`). On a channel-call task it governs the
/// task-level failure set — the route-less `<q:timeout>` boundary firing, and a request
/// delivery the outbox terminally poisoned — with the re-drive RE-EMITTING the request as a
/// fresh outbox emission. It is still NOT a delivery policy: each individual delivery attempt
/// is the outbox dispatcher's retry curve (`sutra.outbox.retry.*`); this policy sits one level
/// above, deciding whether the TASK gets another request at all. A BPMN error
/// (`TaskError::BpmnError`) is a MODELLED outcome that routes to its boundary event and is
/// never retried, and for the same reason a channel-call timer boundary WITH outgoing flows (a
/// modelled timeout route) refuses to coexist with a retry policy. The loader fails closed on
/// a `<q:retry>` anywhere else ([`crate::codes::CONFIG_BPMN_RETRY_NOT_APPLICABLE`]).
///
/// Execution shape (why the fields are durations rather than a sleep budget): a failed
/// attempt with retries remaining parks the instance as a durable TIMER wait at
/// `now + delay` and the ordinary timer poller re-drives it. The engine's actor is
/// single-threaded and `block_on`-ed, so a sleep would freeze every other instance on the
/// replica; a timer park costs one row and survives a crash. Attempt state is durable with
/// the park (the snapshot's `sutra.retry.<nodeId>` counter), so the count is not lost to a
/// restart or a hand-off between replicas.
///
/// The nth attempt's delay is `min(initial_delay × backoff_coefficient^(n-1), max_delay)`
/// — attempt 1 waits `initial_delay`. No jitter: unlike the outbox curve (where every replica
/// re-attempts the SAME rows and would synchronise), each instance owns its own timer row, so
/// there is no wave to spread.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryBinding {
    /// `@maxAttempts` (REQUIRED, ≥ 1) — total attempts INCLUDING the first. `1` is an
    /// explicit "never retry" and is legal (it documents the intent in the model).
    pub max_attempts: u32,
    /// `@initialDelay` — ISO-8601 duration before attempt 2; default `PT1S`. Validated
    /// parseable at load.
    pub initial_delay: String,
    /// `@backoffCoefficient` — the geometric growth factor; default `2.0`. Must be ≥ 1.0
    /// (a shrinking curve would re-hammer a failing dependency; `1.0` is a fixed delay).
    pub backoff_coefficient: f64,
    /// `@maxDelay` — the ISO-8601 ceiling the growing delay clamps at; default `PT5M`.
    /// Must be ≥ [`Self::initial_delay`].
    pub max_delay: String,
    /// `@nonRetryableCodes` — comma-separated classification codes that SKIP the remaining
    /// attempts and fail the instance immediately (Temporal's `nonRetryableErrorTypes`).
    /// A failure's classification code is the leading `CODE:` token of the task's failure
    /// message when it has one (`TaskError::Failed("ACCOUNT_CLOSED: …")` classifies as
    /// `ACCOUNT_CLOSED`), else the stable diagnostic code the engine wraps it in
    /// (`SUTRA.RUNTIME.TASK.UNCAUGHT`) — so an author can both name their own permanent
    /// failures and opt out of retrying unclassified ones. Empty = every uncaught failure
    /// retries.
    pub non_retryable_codes: Vec<String>,
}

/// Parsed `<q:output variable="…"/>` — the render-capture binding: a template task's
/// render is ADDITIONALLY bound to the named process variable as a string, independent of
/// any `<q:reply>`/`<q:send>` emission of the bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBinding {
    pub variable: String,
}

/// Aggregate of every parsed `q:` binding hung off a single BPMN node. All fields are
/// optional/empty when the corresponding `<q:*>` element was absent.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeBindings {
    pub sources: Vec<SourceBinding>,
    pub on_validation: Option<OnValidationBinding>,
    pub dispatch: Option<DispatchTable>,
    pub reply: Option<ReplyBinding>,
    pub send: Option<SendBinding>,
    pub aliases: Vec<AliasBinding>,
    pub audit: Option<AuditBinding>,
    /// `<q:timeout>` on a channel-call task.
    pub timeout: Option<TimeoutBinding>,
    /// `<q:output variable>` render capture.
    pub output: Option<OutputBinding>,
    /// `<q:retry>` on a registered-task or channel-call service task — the per-task retry
    /// policy.
    pub retry: Option<RetryBinding>,
}

impl NodeBindings {
    /// The single `<q:source>` on this node, if any (the parser enforces at most one).
    pub fn source(&self) -> Option<&SourceBinding> {
        self.sources.first()
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
            && self.on_validation.is_none()
            && self.dispatch.is_none()
            && self.reply.is_none()
            && self.send.is_none()
            && self.aliases.is_empty()
            && self.audit.is_none()
            && self.timeout.is_none()
            && self.output.is_none()
            && self.retry.is_none()
    }
}
