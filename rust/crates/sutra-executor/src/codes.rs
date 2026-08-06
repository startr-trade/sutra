//! Executor-side diagnostic code strings — the `SUTRA.*` constants the token executor
//! raises (parse-side codes live in `sutra_bpmn::codes`).

pub const RUNTIME_UNEXPECTED: &str = "SUTRA.RUNTIME.UNEXPECTED";
pub const RUNTIME_TASK_UNCAUGHT: &str = "SUTRA.RUNTIME.TASK.UNCAUGHT";
pub const RUNTIME_ERROR_UNCAUGHT: &str = "SUTRA.RUNTIME.ERROR.UNCAUGHT";
pub const RUNTIME_COMPENSATION_FAILED: &str = "SUTRA.RUNTIME.COMPENSATION.FAILED";
pub const RUNTIME_MULTI_INSTANCE_COMPLETION_FAILED: &str =
    "SUTRA.RUNTIME.MULTI_INSTANCE.COMPLETION_FAILED";
pub const RUNTIME_ADHOC_COMPLETION_FAILED: &str = "SUTRA.RUNTIME.ADHOC.COMPLETION_FAILED";
pub const RUNTIME_DATASTORE_CONFLICT: &str = "SUTRA.RUNTIME.DATASTORE.CONFLICT";

pub const DISPATCH_NO_MATCH: &str = "SUTRA.DISPATCH.NO_MATCH";
pub const DISPATCH_FEEL_EVAL_FAILED: &str = "SUTRA.DISPATCH.FEEL_EVAL_FAILED";
pub const DISPATCH_SUB_PROCESS_NOT_FOUND: &str = "SUTRA.DISPATCH.SUB_PROCESS_NOT_FOUND";

// ---- channel-call codes ----------------------------------------------------------------

/// A channel-call task's timeout fired before the correlated response arrived. Raised as a
/// BPMN error at the host task when the (synthetic `<q:timeout>`) timer boundary has no
/// outgoing route — catchable by an error boundary / event sub-process; uncaught it fails
/// the instance closed.
pub const DISPATCH_CHANNEL_CALL_TIMEOUT: &str = "SUTRA.DISPATCH.CHANNEL_CALL.TIMEOUT";
/// A channel-call task was reached in an execution context that cannot park a durable wait
/// state (multi-instance / ad-hoc / compensation inline runners).
pub const DISPATCH_CHANNEL_CALL_UNSUPPORTED_CONTEXT: &str =
    "SUTRA.DISPATCH.CHANNEL_CALL.UNSUPPORTED_CONTEXT";

// ---- <q:retry> per-task retry policy (P1-1) ---------------------------------------------

/// A `<q:retry>` task ran out of attempts, or failed with one of its declared
/// `@nonRetryableCodes`. TERMINAL: this is the diagnostic the fatal path carries, so the
/// instance's durable `FAILED` snapshot names it and an operator sees "retried and gave up"
/// rather than the bare `SUTRA.RUNTIME.TASK.UNCAUGHT` of a single failed attempt. The
/// underlying task failure is quoted in the message.
pub const RUNTIME_RETRY_EXHAUSTED: &str = "SUTRA.RUNTIME.RETRY.EXHAUSTED";
/// A `<q:retry>` task failed inside an execution context that cannot park the durable timer the
/// backoff needs (a compensation handler, a multi-instance / ad-hoc iteration, an embedded
/// sub-process). The loader refuses the placements it can see statically; this is the runtime
/// floor for the rest. Fail-closed rather than silently downgrading to one attempt: a retry
/// policy that never fires is exactly the kind of silent no-op this engine refuses to ship.
pub const DISPATCH_RETRY_UNSUPPORTED_CONTEXT: &str = "SUTRA.DISPATCH.RETRY.UNSUPPORTED_CONTEXT";

pub const OUTBOUND_REPLY_DEST_REQUIRED_NOT_SET: &str = "SUTRA.OUTBOUND.REPLY_DEST_REQUIRED_NOT_SET";
pub const OUTBOUND_REPLY_AUTH_RESOLVER_NOT_FOUND: &str =
    "SUTRA.OUTBOUND.REPLY_AUTH_RESOLVER_NOT_FOUND";
/// A `<q:header value="<FEEL>">` on a `<q:send>` / `<q:reply>` threw while
/// evaluating against the sending process context at dispatch.
pub const OUTBOUND_HEADER_FEEL_EVAL_FAILED: &str = "SUTRA.OUTBOUND.HEADER_FEEL_EVAL_FAILED";

pub const CONFIG_CHANNEL_OUTBOUND_UNKNOWN: &str = "SUTRA.CONFIG.CHANNEL.OUTBOUND_UNKNOWN";
pub const CONFIG_COVERAGE_STORE_MISSING: &str = "SUTRA.CONFIG.COVERAGE.STORE_MISSING";

pub const RESOLVE_TEMPLATE_UNKNOWN: &str = "SUTRA.RESOLVE.TEMPLATE.UNKNOWN";
pub const RESOLVE_BARE_ID_AMBIGUOUS: &str = "SUTRA.RESOLVE.BARE_ID.AMBIGUOUS";
