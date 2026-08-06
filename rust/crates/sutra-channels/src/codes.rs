//! Stable diagnostic-code strings — the exact `SUTRA.*` diagnostic codes
//! this crate raises.

pub const RESOLVE_CHANNEL_UNKNOWN: &str = "SUTRA.RESOLVE.CHANNEL.UNKNOWN";
pub const RESOLVE_MODULE_NOT_FOUND: &str = "SUTRA.RESOLVE.MODULE.NOT_FOUND";

/// A configuration property held an invalid value (e.g. a negative payload cap) — raised at
/// construction/validation time (the `SUTRA.CONFIG.PROPERTY.INVALID` code).
pub const CONFIG_PROPERTY_INVALID: &str = "SUTRA.CONFIG.PROPERTY.INVALID";

pub const CHANNEL_AUTH_MISSING_SCHEME: &str = "SUTRA.CHANNEL.AUTH.MISSING_SCHEME";
pub const CHANNEL_AUTH_SCHEME_INVALID: &str = "SUTRA.CHANNEL.AUTH.SCHEME_INVALID";
pub const CHANNEL_NAME_COLLISION: &str = "SUTRA.CHANNEL.NAME.COLLISION";
pub const CHANNEL_TRANSPORT_UNKNOWN: &str = "SUTRA.CHANNEL.TRANSPORT.UNKNOWN";

pub const INBOUND_REJECTED_AUTH: &str = "SUTRA.INBOUND.REJECTED.AUTH";
pub const INBOUND_REJECTED_TENANT_CHANNEL_NOT_ALLOWED: &str =
    "SUTRA.INBOUND.REJECTED.TENANT_CHANNEL_NOT_ALLOWED";
/// An inbound HTTP request failed CloudEvents extraction (missing required
/// attribute, malformed envelope, bad `time`, bad `data_base64`) → 400.
pub const INBOUND_REJECTED_CLOUDEVENT: &str = "SUTRA.INBOUND.REJECTED.CLOUDEVENT";
pub const INBOUND_PAYLOAD_TOO_LARGE: &str = "SUTRA.INBOUND.PAYLOAD_TOO_LARGE";
/// A channel's feature-gate expression resolved to false — the dispatcher rejects the
/// inbound before any executor work (the `SUTRA.INBOUND.FEATURE_DISABLED` code).
pub const INBOUND_FEATURE_DISABLED: &str = "SUTRA.INBOUND.FEATURE_DISABLED";
/// Tenant rate-quota (sliding 60 s window) exceeded (the `SUTRA.INBOUND.QUOTA_EXCEEDED_RATE` code).
pub const INBOUND_QUOTA_EXCEEDED_RATE: &str = "SUTRA.INBOUND.QUOTA_EXCEEDED_RATE";
/// Tenant concurrent-instance quota exceeded (the `SUTRA.INBOUND.QUOTA_EXCEEDED_CONCURRENT` code).
pub const INBOUND_QUOTA_EXCEEDED_CONCURRENT: &str = "SUTRA.INBOUND.QUOTA_EXCEEDED_CONCURRENT";
/// A per-channel `max-concurrent-instances` cap is full — the inbound is a busy-signal
/// reject (the `SUTRA.INBOUND.CHANNEL_AT_CAPACITY` code).
pub const INBOUND_CHANNEL_AT_CAPACITY: &str = "SUTRA.INBOUND.CHANNEL_AT_CAPACITY";
pub const INBOUND_CODEC_NOT_FOUND: &str = "SUTRA.INBOUND.CODEC_NOT_FOUND";
pub const INBOUND_CAPABILITY_MISMATCH: &str = "SUTRA.INBOUND.CAPABILITY_MISMATCH";
pub const INBOUND_NO_START_EVENT_FOR_MESSAGE_TYPE: &str =
    "SUTRA.INBOUND.NO_START_EVENT_FOR_MESSAGE_TYPE";
pub const INBOUND_AMBIGUOUS_HANDLER: &str = "SUTRA.INBOUND.AMBIGUOUS_HANDLER";
pub const INBOUND_VALIDATION_REJECT: &str = "SUTRA.INBOUND.VALIDATION_REJECT";
pub const INBOUND_VALIDATION_ERROR: &str = "SUTRA.INBOUND.VALIDATION_ERROR";
pub const INBOUND_PERSISTENCE_REQUIRED: &str = "SUTRA.INBOUND.PERSISTENCE_REQUIRED";
pub const INBOUND_ALIAS_CONFLICT_REJECT: &str = "SUTRA.INBOUND.ALIAS_CONFLICT_REJECT";
/// A NON-IDEMPOTENT process (`<q:process idempotent="false">`, the default) failed during
/// execution. Blind redelivery-and-reprocess would duplicate side effects, so the inbound is
/// CONSUMED (ack, no requeue — at-most-once) and the failure is recorded as a durable incident.
/// An idempotent process (`idempotent="true"`) is retried (NackRequeue) instead.
pub const INBOUND_NON_IDEMPOTENT_FAILURE: &str = "SUTRA.INBOUND.NON_IDEMPOTENT_FAILURE";
pub const INBOUND_ALIAS_FEEL_EVAL_FAILED: &str = "SUTRA.INBOUND.ALIAS_FEEL_EVAL_FAILED";
pub const INBOUND_ALIAS_MULTI_NOT_LIST: &str = "SUTRA.INBOUND.ALIAS_MULTI_NOT_LIST";

// ---- deferred acking (`ack-mode: on-complete`, broker transports) ---------------------------

/// A broker delivery's settle callbacks were registered on the [`crate::DeferredAckRegistry`]
/// — the transport ack is held until the instance's terminal event.
pub const ACK_DEFERRED_REGISTERED: &str = "SUTRA.ACK.DEFERRED_REGISTERED";
/// A deferred ack fired — the registered instance reached `INSTANCE_COMPLETED`.
pub const ACK_DEFERRED_ACKED: &str = "SUTRA.ACK.DEFERRED_ACKED";
/// A deferred nack fired — the registered instance reached `INSTANCE_FAILED` (permanent
/// reject; the broker source executes its `NackDrop`/DLQ posture).
pub const ACK_DEFERRED_NACKED: &str = "SUTRA.ACK.DEFERRED_NACKED";
/// The deferred-ack registry hit its bounded capacity — the OLDEST entry was evicted with
/// a nack (operator-visible failure mode, never a memory leak).
pub const ACK_DEFERRED_OVERFLOW: &str = "SUTRA.ACK.DEFERRED_OVERFLOW";
/// A deferred-ack entry outlived the configured timeout — the sweep nacked it (the broker
/// slot frees; the instance keeps running and inbox dedup absorbs any redelivery).
pub const ACK_DEFERRED_TIMEOUT: &str = "SUTRA.ACK.DEFERRED_TIMEOUT";
/// A broker channel declared `ack-mode: on-complete` on a transport that has no deferred
/// settle path yet — the channel runs `on-persist` and this startup diagnostic says so
/// (loud degrade, never silent).
pub const ACK_ON_COMPLETE_UNSUPPORTED: &str = "SUTRA.ACK.ON_COMPLETE_UNSUPPORTED";

pub const VALIDATE_VALIDATOR_NOT_FOUND: &str = "SUTRA.VALIDATE.VALIDATOR_NOT_FOUND";
pub const RUNTIME_VALIDATOR_UNCAUGHT: &str = "SUTRA.RUNTIME.VALIDATOR.UNCAUGHT";

/// A `<q:redactor ref=…>` names a redactor not present in the RedactorRegistry — fail closed.
pub const VALIDATE_REDACTOR_NOT_FOUND: &str = "SUTRA.VALIDATE.REDACTOR_NOT_FOUND";
/// A redactor threw (`Err` or panic) while locating spans — the engine over-masks the whole
/// payload on every observability surface (fail-closed): never leaking, never crashing intake.
pub const RUNTIME_REDACTOR_UNCAUGHT: &str = "SUTRA.RUNTIME.REDACTOR.UNCAUGHT";
pub const RUNTIME_UNEXPECTED: &str = "SUTRA.RUNTIME.UNEXPECTED";
pub const RUNTIME_RELAY_CORRELATION_NOT_FOUND: &str = "SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND";
pub const RUNTIME_RESUME_INSTANCE_NOT_FOUND: &str = "SUTRA.RUNTIME.RESUME.INSTANCE_NOT_FOUND";
pub const RUNTIME_RESUME_NOT_SUSPENDED: &str = "SUTRA.RUNTIME.RESUME.NOT_SUSPENDED";
/// A parked instance's PINNED deployment id cannot be read back as a deployment id (a
/// corrupt/legacy snapshot column). Both resume paths — relay and timer — refuse the
/// resume rather than fall back to the deployment the delivery arrived under: an instance
/// pinned to one deployment must never silently run on another. The parked instance is
/// untouched, so the delivery is safe to redeliver once the pin is repairable.
/// (The sibling condition — a READABLE pin whose definition is no longer registered — keeps
/// the long-standing `SUTRA.RESOLVE.MODULE.NOT_FOUND` the timer path already raised.)
pub const RUNTIME_RESUME_PIN_UNRESOLVABLE: &str = "SUTRA.RUNTIME.RESUME.PIN_UNRESOLVABLE";
/// Another replica holds this instance's ownership claim, so this resume refuses to
/// rehydrate it (two replicas advancing one instance is the failure this prevents). RETRY-SAFE
/// and stamped as such: nothing was loaded, nothing executed, nothing committed. The relay path
/// tags the diagnostic [`crate::ACK_DISPOSITION_REQUEUE`] so the broker redelivers under its own
/// backoff; the timer path defers the due row and re-fires later. The claim is bounded by
/// `sutra.instance.claim-timeout` — a crashed owner's claim is cleared by the
/// `StuckInstanceScanner`, never held forever.
pub const RUNTIME_RESUME_CLAIM_HELD: &str = "SUTRA.RUNTIME.RESUME.CLAIM_HELD";
pub const OUTBOUND_ENCODE_FAILED: &str = "SUTRA.OUTBOUND.ENCODE_FAILED";
pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.SEND.FAILED";
pub const OUTBOUND_HTTP_AUTH_REF_UNRESOLVED: &str = "SUTRA.OUTBOUND.HTTP.AUTH_REF_UNRESOLVED";
pub const OUTBOUND_HTTP_MTLS_UNSUPPORTED: &str = "SUTRA.OUTBOUND.HTTP.MTLS_UNSUPPORTED";
pub const PARSE_YAML_PARSE_ERROR: &str = "SUTRA.PARSE.YAML.PARSE_ERROR";

// ---- channel-call / timer codes -------------------------------------------------------------

/// A channel-call task parked but NONE of its declared `<q:alias>` expressions resolved to
/// a value — the park would be unresumable, so the step is refused (fail closed).
pub const DISPATCH_CHANNEL_CALL_ALIAS_UNRESOLVED: &str =
    "SUTRA.DISPATCH.CHANNEL_CALL.ALIAS_UNRESOLVED";
/// A channel-call task's output-mapping expression failed to evaluate against the
/// correlated response payload.
pub const DISPATCH_CHANNEL_CALL_OUTPUT_MAPPING_FAILED: &str =
    "SUTRA.DISPATCH.CHANNEL_CALL.OUTPUT_MAPPING_FAILED";
/// A relay correlated to a channel-call node sitting in a `<q:retry>` BACKOFF window: the
/// response belongs to a DEAD attempt (its timeout fired, or its delivery was terminally
/// poisoned, and that failure already consumed one slot of the task's budget). Fail-closed
/// reject — the parked instance is untouched; the backoff re-drive will re-issue the request,
/// and the counterpart's answer to THAT attempt resumes the instance normally. Mirrors the
/// durable-FAILED posture: the alias stays live so the caller gets this honest verdict instead
/// of a "no live instance" miss.
pub const DISPATCH_CHANNEL_CALL_RETRY_PENDING: &str = "SUTRA.DISPATCH.CHANNEL_CALL.RETRY_PENDING";

// ---- durable failure state -------------------------------------------------------------------

/// The addressed instance is durably FAILED (`sutra.status=FAILED`): a fatal step killed it and
/// the failure commit recorded that verdict. Every resume path fails CLOSED on this — a relay
/// correlating to it, and a timer fire claiming one of its rows, both raise this code instead of
/// re-driving a dead instance. Distinct from `SUTRA.RUNTIME.RESUME.NOT_SUSPENDED` (which means
/// "not parked", e.g. mid-flight) precisely so an operator can tell "needs a human" from "try
/// later"; the timer poller also keys on it to resolve the row rather than hot-loop the fire.
/// Recovery is an operator action (inspect + cancel); admin retry/undo of a FAILED instance is not
/// part of this surface.
pub const DISPATCH_INSTANCE_FAILED: &str = "SUTRA.DISPATCH.INSTANCE_FAILED";

/// A `<q:send required>` (or `<q:reply required>`) outbound entry was POISONED by the outbox
/// dispatcher — a permanent delivery failure on a delivery the author declared must not fail
/// silently. Recorded ONCE per entry as a durable incident (the row then sits at the poison
/// horizon, still visible and still redrivable — at-least-once is never traded for silence).
pub const OUTBOUND_REQUIRED_DELIVERY_FAILED: &str = "SUTRA.OUTBOUND.REQUIRED_DELIVERY_FAILED";

/// An outbox entry exhausted `sutra.outbox.retry.max-attempts` and was marked TERMINAL: the
/// dispatcher will not schedule it again.
///
/// This exists because the outbox otherwise retries FOREVER — the historical (and still DEFAULT,
/// when the key is unset) posture, chosen so at-least-once is never traded for silence. The cost
/// of retrying forever is that an undeliverable entry is indistinguishable from a slow one, and it
/// pins its deployment out of quiescence. With a ceiling configured, exhaustion becomes a
/// first-class, observable event instead: the row is flagged `poisoned` (V604) rather than
/// deleted, so it stays visible and redrivable, and ONE durable incident is recorded — composing
/// with the [`OUTBOUND_REQUIRED_DELIVERY_FAILED`] latch so a `required` entry that already alerted
/// does not alert twice, while a non-required entry (which never alerts on poison) finally does.
pub const OUTBOUND_DELIVERY_ATTEMPTS_EXHAUSTED: &str = "SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED";

// ---- the external-task (pull) surface ------------------------------------------------------

/// A worker's fetch-and-lock / complete / failure request was malformed or out of the operator's
/// configured bounds (`sutra.external-task.*`) — a missing `workerId`, an unparsable ISO-8601
/// duration, or a `lockDuration`/`asyncResponseTimeout`/`maxTasks` above its ceiling. Rejected
/// LOUDLY rather than clamped: a worker that thinks it holds a two-hour lock and actually holds a
/// one-minute one is a duplicate-execution bug waiting to happen.
pub const EXTERNAL_TASK_REQUEST_INVALID: &str = "SUTRA.EXTERNAL_TASK.REQUEST_INVALID";

/// No external task with that id is parked on any live deployment.
pub const EXTERNAL_TASK_NOT_FOUND: &str = "SUTRA.EXTERNAL_TASK.NOT_FOUND";

/// The task exists but ANOTHER worker holds an unexpired lock on it. Fail-closed: the caller is
/// not the owner, so its completion/failure is refused rather than allowed to race the owner's.
pub const EXTERNAL_TASK_LOCK_HELD: &str = "SUTRA.EXTERNAL_TASK.LOCK_HELD";

/// The task exists and the caller no longer holds its lock — it expired (so the task is
/// fetchable again, possibly already re-fetched) or was released by a failure. This is the stale
/// worker's answer, and it must fail closed: a lapsed lock is exactly the window in which a
/// second worker may already be executing the same task.
pub const EXTERNAL_TASK_LOCK_LOST: &str = "SUTRA.EXTERNAL_TASK.LOCK_LOST";

/// The task exhausted its retry budget and is TERMINAL — retained with its last error for
/// inspection (the pull-side twin of the outbox's poison horizon), but never fetched, completed
/// or failed again.
pub const EXTERNAL_TASK_TERMINAL: &str = "SUTRA.EXTERNAL_TASK.TERMINAL";

/// The worker held a valid lock, but the engine refused the completion when it re-entered the
/// ordinary inbound path (no correlating instance, a validation reject, or the actor being
/// unavailable). The task row is NOT deleted — it stays locked for the grace window and becomes
/// fetchable again after it, so the work is never silently dropped.
pub const EXTERNAL_TASK_COMPLETION_REJECTED: &str = "SUTRA.EXTERNAL_TASK.COMPLETION_REJECTED";
