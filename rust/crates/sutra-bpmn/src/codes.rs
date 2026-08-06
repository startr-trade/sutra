//! Stable `SUTRA.*` diagnostic code strings the model/loader raise; `diagnostics.yaml` is the
//! catalog of record.

pub const PARSE_BPMN_MISSING_PROCESS: &str = "SUTRA.PARSE.BPMN.MISSING_PROCESS";
pub const PARSE_BPMN_UNSUPPORTED_CATCH_EVENT: &str = "SUTRA.PARSE.BPMN.UNSUPPORTED_CATCH_EVENT";
pub const PARSE_QXSD_INVALID_SOURCE: &str = "SUTRA.PARSE.QXSD.INVALID_SOURCE";
pub const PARSE_Q_SOURCE_CODEC_NOT_ALLOWED: &str = "SUTRA.PARSE.Q_SOURCE.CODEC_NOT_ALLOWED";
/// `<q:source>` declares the retired `idempotencyKey` attribute (renamed to `dedupKey`).
/// A hard deploy-time error: the attribute was a misnomer (a dedup key ≠ an idempotency assertion).
pub const PARSE_Q_SOURCE_IDEMPOTENCY_KEY_RENAMED: &str =
    "SUTRA.PARSE.Q_SOURCE.IDEMPOTENCY_KEY_RENAMED";
pub const PARSE_Q_SOURCE_MESSAGE_TYPE_CONFLICT: &str = "SUTRA.PARSE.Q_SOURCE.MESSAGE_TYPE_CONFLICT";
pub const PARSE_Q_SOURCE_MULTIPLE: &str = "SUTRA.PARSE.Q_SOURCE.MULTIPLE";
pub const PARSE_Q_SIMPLE_VALIDATOR_INCOMPLETE: &str = "SUTRA.PARSE.Q_SIMPLE_VALIDATOR.INCOMPLETE";
pub const PARSE_Q_CASE_MISSING_WHEN: &str = "SUTRA.PARSE.Q_CASE_MISSING_WHEN";
pub const PARSE_Q_CASE_MISSING_CALLED_ELEMENT: &str = "SUTRA.PARSE.Q_CASE_MISSING_CALLED_ELEMENT";
pub const PARSE_Q_ALIAS_MISSING_NAME: &str = "SUTRA.PARSE.Q_ALIAS_MISSING_NAME";
pub const PARSE_Q_ALIAS_MISSING_EXPRESSION: &str = "SUTRA.PARSE.Q_ALIAS_MISSING_EXPRESSION";
pub const PARSE_Q_REPLY_INVALID_MODE: &str = "SUTRA.PARSE.Q_REPLY_INVALID_MODE";
pub const PARSE_Q_SEND_CHANNEL_OR_DESTINATION: &str = "SUTRA.PARSE.Q_SEND.CHANNEL_OR_DESTINATION";
pub const PARSE_Q_HEADER_INCOMPLETE: &str = "SUTRA.PARSE.Q_HEADER.INCOMPLETE";
pub const PARSE_THROW_SEND_REQUIRED: &str = "SUTRA.PARSE.THROW.SEND_REQUIRED";
pub const PARSE_LINK_EVENT_NO_NAME: &str = "SUTRA.PARSE.LINK_EVENT.NO_NAME";
pub const PARSE_LINK_CATCH_NOT_FOUND: &str = "SUTRA.PARSE.LINK.CATCH_NOT_FOUND";
pub const PARSE_LINK_CATCH_DUPLICATE: &str = "SUTRA.PARSE.LINK.CATCH_DUPLICATE";
pub const PARSE_SUBPROCESS_UNSUPPORTED: &str = "SUTRA.PARSE.SUBPROCESS.UNSUPPORTED";
pub const PARSE_DATA_ASSOCIATION_UNSUPPORTED: &str = "SUTRA.PARSE.DATA_ASSOCIATION.UNSUPPORTED";
pub const PARSE_STORE_KEY_REQUIRED: &str = "SUTRA.PARSE.STORE.KEY_REQUIRED";
pub const PARSE_Q_ON_VALIDATION_INVALID_MODE: &str = "SUTRA.PARSE.Q_ON_VALIDATION_INVALID_MODE";
pub const PARSE_BOUNDARY_EVENT_INVALID_REF: &str = "SUTRA.PARSE.BOUNDARY_EVENT.INVALID_REF";

pub const CONFIG_BPMN_UNSUPPORTED_ELEMENT: &str = "SUTRA.CONFIG.BPMN.UNSUPPORTED_ELEMENT";
/// A `<startEvent>` whose `<timerEventDefinition>` names a scheduling form this engine
/// deliberately does not execute.
///
/// NARROWED (P1-5b): timer start events now RUN — `<timeDuration>` (once, that long after the
/// deployment activates), `<timeDate>` (once, at the instant) and `<timeCycle>` as an ISO-8601
/// repeating interval (`R/PT1H`, `R5/PT1H`, `R/<start>/PT1H`) are all scheduled. What is left
/// under this code is exactly the set that stays out of contract: a **cron-syntax** `timeCycle`
/// (a vendor extension, not BPMN — deliberately deferred) and a **calendar-length** duration
/// (`P1Y` / `P1M` before the `T`, which has no exact length). A start timer declaring one of
/// those still fails CLOSED rather than being silently accepted and never firing.
pub const CONFIG_BPMN_TIMER_START_UNSUPPORTED: &str = "SUTRA.CONFIG.BPMN.TIMER_START_UNSUPPORTED";
/// A `<startEvent>` declares BOTH a `<q:source>` (channel-triggered intake) and a
/// `<timerEventDefinition>` (schedule-triggered). The two are different trigger contracts —
/// a channel start carries an inbound payload, a timer start carries none — and one event
/// cannot honour both. Pick one.
pub const CONFIG_BPMN_TIMER_START_SOURCE_CONFLICT: &str =
    "SUTRA.CONFIG.BPMN.TIMER_START_SOURCE_CONFLICT";
pub const CONFIG_BPMN_VARIABLE_SOURCE_UNKNOWN: &str = "SUTRA.CONFIG.BPMN.VARIABLE_SOURCE_UNKNOWN";

// ---- <q:retry> (per-task retry policy) -------------------------------------------------------
// The three fail-closed load errors of the retry policy. They are CONFIG.BPMN.* rather than
// PARSE.* because each one is a well-formed document making an unexecutable declaration — the
// same class as `CONFIG_BPMN_TIMER_START_UNSUPPORTED`.

/// `<q:retry>` declares no `@maxAttempts`, or one that is not a positive integer. Required
/// with no default on purpose: an unbounded retry policy is exactly what the outbox used to do
/// and what P1-1 exists to stop, so the author must state the ceiling.
pub const CONFIG_BPMN_RETRY_MAX_ATTEMPTS_INVALID: &str =
    "SUTRA.CONFIG.BPMN.RETRY_MAX_ATTEMPTS_INVALID";
/// A `<q:retry>` attribute other than `@maxAttempts` is malformed: an unparseable
/// `@initialDelay`/`@maxDelay`, a `@backoffCoefficient` that is not a number ≥ 1.0, a
/// `@maxDelay` below the `@initialDelay`, or a `@nonRetryableCodes` list that names nothing.
pub const CONFIG_BPMN_RETRY_POLICY_INVALID: &str = "SUTRA.CONFIG.BPMN.RETRY_POLICY_INVALID";
/// `<q:retry>` sits on a node that can never honour it: anything but a `<serviceTask>`, a
/// CHANNEL-CALL service task (its delivery retry is the outbox's `sutra.outbox.retry.*` curve,
/// not a task-level re-invocation), or a service task wrapped in loop characteristics / nested in
/// a sub-process (neither can park the durable timer the retry wait needs).
pub const CONFIG_BPMN_RETRY_NOT_APPLICABLE: &str = "SUTRA.CONFIG.BPMN.RETRY_NOT_APPLICABLE";
pub const CONFIG_COVERAGE_UNKNOWN_FLOW: &str = "SUTRA.CONFIG.COVERAGE.UNKNOWN_FLOW";
pub const CONFIG_COVERAGE_INVALID_ROUTE: &str = "SUTRA.CONFIG.COVERAGE.INVALID_ROUTE";
pub const CONFIG_COVERAGE_DUPLICATE_PATH: &str = "SUTRA.CONFIG.COVERAGE.DUPLICATE_PATH";

pub const RESOLVE_TASK_NAME_COLLISION: &str = "SUTRA.RESOLVE.TASK.NAME_COLLISION";
pub const RESOLVE_TASK_UNKNOWN: &str = "SUTRA.RESOLVE.TASK.UNKNOWN";
pub const RESOLVE_MODULE_NOT_FOUND: &str = "SUTRA.RESOLVE.MODULE.NOT_FOUND";

// ---- timer / channel-call codes ------------------------------------------------------------
// Raised by the BPMN loader's timer / channel-call validation and pinned by the timer +
// channel-call conformance suites.

/// A channel-call task without a timer boundary or `<q:timeout>` is a
/// package/load-time error.
pub const DISPATCH_CHANNEL_CALL_TIMEOUT_REQUIRED: &str =
    "SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT_REQUIRED";
/// The park is keyed by a DECLARED `<q:alias>`; a channel-call task
/// declaring none could never be resumed by a correlated response.
pub const DISPATCH_CHANNEL_CALL_ALIAS_REQUIRED: &str = "SUTRA.DISPATCH.CHANNEL_CALL.ALIAS_REQUIRED";
/// A timer duration (`<bpmn:timeDuration>` / `<q:timeout duration>`) is missing or is not a
/// parseable ISO-8601 duration.
pub const DISPATCH_TIMER_DURATION_INVALID: &str = "SUTRA.DISPATCH.TIMER.DURATION_INVALID";
/// A `<bpmn:timeDate>` that is not a parseable ISO-8601 datetime with an explicit zone
/// (`Z` or `±HH:MM`). A date in the PAST is deliberately NOT an error — it is simply already
/// due and fires on the first tick that observes it.
pub const DISPATCH_TIMER_DATE_INVALID: &str = "SUTRA.DISPATCH.TIMER.DATE_INVALID";
/// A `<bpmn:timeCycle>` that is ISO-8601-shaped (`R…`) but written wrong — a bad repeat count,
/// a missing/zero interval, or too many `/`-separated parts. A cycle in a form that is out of
/// contract entirely (cron syntax, calendar-length interval) raises the host's
/// unsupported-form code instead, so "written wrong" stays distinguishable from "not done here".
pub const DISPATCH_TIMER_CYCLE_INVALID: &str = "SUTRA.DISPATCH.TIMER.CYCLE_INVALID";
/// A timer definition form this engine does not support: a `timeCycle` anywhere but a START
/// event (a mid-flow token cannot park at a node that fires more than once), a
/// `timerEventDefinition` declaring more than one of `timeDuration`/`timeDate`/`timeCycle`, a
/// non-interrupting timer boundary, or a timer boundary on a non-wait-capable host.
pub const DISPATCH_TIMER_UNSUPPORTED: &str = "SUTRA.DISPATCH.TIMER.UNSUPPORTED";
