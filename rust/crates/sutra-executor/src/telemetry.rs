//! Telemetry names — the FROZEN `sutra.*` span names, metric names and
//! attribute/dimension keys. Only the engine emits these names.
//!
//! This is the telemetry facade: call sites emit plain [`tracing`] spans named
//! by these constants, and the exporter layer wires OTLP as a
//! `tracing` subscriber layer WITHOUT touching any call site. Metric names are constants
//! only — the meters themselves are the exporter's concern, fed by the
//! [`crate::listener::ExecutionListener`] events (the frozen meter set maps 1:1 onto them).
//!
//! Span fields at the call sites use the SAME dotted names as [`ATTR_DEPLOYMENT_ID`] et
//! al., so the exporter's span-attribute mapping is a pass-through.

// ---- waterfall spans (the frozen span names, carried as-is) --------------------------

pub const SPAN_CHANNEL: &str = "sutra.channel";
pub const SPAN_DECODE: &str = "sutra.decode";
pub const SPAN_VALIDATE: &str = "sutra.validate";
pub const SPAN_DISPATCH: &str = "sutra.dispatch";
pub const SPAN_EXECUTE: &str = "sutra.execute";
pub const SPAN_TASK: &str = "sutra.task";
pub const SPAN_TEMPLATE: &str = "sutra.template";
pub const SPAN_SCRIPT: &str = "sutra.script";
pub const SPAN_DECISION: &str = "sutra.decision";
pub const SPAN_ENCODE: &str = "sutra.encode";
pub const SPAN_RESOLVE: &str = "sutra.resolve";
/// Outbox delivery span — opened by the delivery dispatcher per entry, `SpanKind.PRODUCER`,
/// LINKED to the persisted `traceparent` of the enqueueing step (normative).
pub const SPAN_OUTBOX_SEND: &str = "sutra.outbox.send";

// ---- lifecycle spans (`sutra.instance` › `sutra.token` › `sutra.task`) ------------------

pub const SPAN_INSTANCE: &str = "sutra.instance";
pub const SPAN_TOKEN: &str = "sutra.token";

// ---- span events ---------------------------------------------------------------------

pub const EVENT_INSTANCE_SUSPENDED: &str = "sutra.instance.suspended";
pub const EVENT_PATH_COVERED: &str = "coverage.path_covered";

// ---- metric names (frozen; every key carries the `sutra.` prefix) ----------------------

pub const METRIC_INSTANCE_STARTED: &str = "sutra.instance.started";
pub const METRIC_INSTANCE_COMPLETED: &str = "sutra.instance.completed";
pub const METRIC_INSTANCE_SUSPENDED: &str = "sutra.instance.suspended";
pub const METRIC_INSTANCE_RESUMED: &str = "sutra.instance.resumed";
pub const METRIC_TOKEN_ENTERED: &str = "sutra.token.entered";
pub const METRIC_TOKEN_LEFT: &str = "sutra.token.left";
pub const METRIC_TASK_INVOKED: &str = "sutra.task.invoked";
pub const METRIC_TASK_COMPLETED: &str = "sutra.task.completed";
pub const METRIC_TASK_FAILED: &str = "sutra.task.failed";
pub const METRIC_TASK_DURATION: &str = "sutra.task.duration";
pub const METRIC_COVERAGE_PATH_COVERED: &str = "sutra.coverage.path_covered";
/// The per-process coverage-percent gauge
/// (`(covered paths / declared <q:coverage> paths) × 100`). Event-driven: a synchronous
/// gauge the `sutra.coverage.path_covered` listener re-records on each new mark (plus once
/// at activation to reflect pre-existing coverage). NOT one of the original frozen
/// meters — a later addition, but carries the same `sutra.` prefix.
pub const METRIC_COVERAGE_PERCENT: &str = "sutra.coverage.percent";

// ---- shard-router meters (execution scale-out §6.1 — shipped WITH the N-lane feature).
// One point per `shard` label; the engine's exporter reads the router's atomics through
// observable instruments. Later additions like the coverage gauge — same `sutra.` prefix,
// not part of the original frozen set.

/// Per-shard mailbox depth (enqueued, not yet dequeued) — the hot-key skew gauge.
pub const METRIC_ENGINE_SHARD_QUEUE_DEPTH: &str = "sutra.engine.shard.queue-depth";
/// Per-shard work requests drained by the lane's actor (activation swaps excluded).
pub const METRIC_ENGINE_SHARD_DISPATCHES: &str = "sutra.engine.shard.dispatches";
/// Per-shard initial park commits.
pub const METRIC_ENGINE_SHARD_PARKS: &str = "sutra.engine.shard.parks";
/// Per-shard committed resume passes (relay / timer / handoff; terminal or re-park).
pub const METRIC_ENGINE_SHARD_RESUMES: &str = "sutra.engine.shard.resumes";
/// Per-shard relay handoffs (resolved on this lane, owned by another).
pub const METRIC_ENGINE_SHARD_HANDOFFS: &str = "sutra.engine.shard.handoffs";
/// Per-shard `CLAIM_HELD` bounces, split by [`ATTR_CLAIM_BOUNCE_PATH`] (`relay`/`timer`).
/// The mis-route alarm: near zero at a correct N>1 rollout outside genuine
/// cross-replica contention.
pub const METRIC_ENGINE_SHARD_CLAIM_BOUNCES: &str = "sutra.engine.shard.claim-bounces";

// ---- attribute / dimension keys --------------------------------------------------------

/// THE identity dimension — on every meter and span.
pub const ATTR_DEPLOYMENT_ID: &str = "deployment.id";
pub const ATTR_INSTANCE_ID: &str = "instance.id";
pub const ATTR_NODE_ID: &str = "node.id";
pub const ATTR_NODE_TYPE: &str = "node.type";
pub const ATTR_TASK_NAME: &str = "task.name";
pub const ATTR_TASK_DURATION_NS: &str = "task.duration_ns";
/// Stamped on ERROR-status spans.
pub const ATTR_DIAGNOSTIC_CODE: &str = "diagnostic.code";
/// `coverage.path_covered` event attribute.
pub const ATTR_PATH: &str = "path";
/// `sutra.coverage.percent` gauge dimensions. `process.id` + [`ATTR_DEPLOYMENT_ID`]
/// are always attached; `tenant`/`module`/`version` are the authoring namespace of the
/// process's deployment (each attached only when non-empty). These mirror the frozen label
/// allowlist (`tenant,module,version`) so the gauge shares their dimensions.
pub const ATTR_PROCESS_ID: &str = "process.id";
pub const ATTR_TENANT: &str = "tenant";
pub const ATTR_MODULE: &str = "module";
pub const ATTR_VERSION: &str = "version";
/// The shard-lane index dimension on every `sutra.engine.shard.*` meter.
pub const ATTR_SHARD: &str = "shard";
/// The `sutra.engine.shard.claim-bounces` split dimension: `relay` | `timer`.
pub const ATTR_CLAIM_BOUNCE_PATH: &str = "path";

/// The W3C trace-context header persisted on outbox rows at enqueue and restored on the
/// [`SPAN_OUTBOX_SEND`] delivery span (the traceparent bridge is contract, not detail).
pub const TRACEPARENT_HEADER: &str = "traceparent";

// ---- W3C traceparent bridge (outbox delivery joins the enqueueing trace) ---------------
//
// These live HERE (not in the engine crate's exporter module) because the delivery-side
// call site is `sutra-channels`' outbox send span, and the crate dependency direction is
// channels → executor. `sutra_engine::otel` re-exports them unchanged.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Parse a W3C `traceparent` header (`00-<32 hex trace>-<16 hex span>-<2 hex flags>`)
/// into a REMOTE OTel [`SpanContext`]. Returns `None` for anything malformed (unknown
/// version prefixes with the version-00 core shape still parse, per spec).
pub fn parse_traceparent(value: &str) -> Option<SpanContext> {
    let mut parts = value.trim().splitn(4, '-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let span_id = parts.next()?;
    let flags = parts.next()?;

    if version.len() != 2 || !is_lower_hex(version) || version == "ff" {
        return None;
    }
    if trace_id.len() != 32 || !is_lower_hex(trace_id) {
        return None;
    }
    if span_id.len() != 16 || !is_lower_hex(span_id) {
        return None;
    }
    // Future versions may append fields after the flags; accept `xx` or `xx-…`.
    let (flag_hex, _rest) = flags.split_at(flags.len().min(2));
    if flag_hex.len() != 2 || !is_lower_hex(flag_hex) || (flags.len() > 2 && version == "00") {
        return None;
    }

    let trace_id = TraceId::from_hex(trace_id).ok()?;
    let span_id = SpanId::from_hex(span_id).ok()?;
    if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
        return None;
    }
    let flag_bits = u8::from_str_radix(flag_hex, 16).ok()?;
    Some(SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::new(flag_bits),
        true, // remote — restored from the persisted outbox column
        TraceState::default(),
    ))
}

fn is_lower_hex(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Render a [`SpanContext`] as a version-00 `traceparent` value (`None` when invalid).
pub fn format_traceparent(cx: &SpanContext) -> Option<String> {
    if !cx.is_valid() {
        return None;
    }
    Some(format!(
        "00-{}-{}-{:02x}",
        cx.trace_id(),
        cx.span_id(),
        cx.trace_flags().to_u8() & TraceFlags::SAMPLED.to_u8()
    ))
}

/// The `traceparent` of the CURRENT span (the enqueue-side capture: persisted onto
/// outbox rows at `<q:send>` commit). `None` when no OTel span is active.
pub fn current_traceparent() -> Option<String> {
    let cx = tracing::Span::current().context();
    let span = cx.span();
    format_traceparent(span.span_context())
}

/// Delivery-side bridge: parse `traceparent` and attach it as a LINK on `span`
/// (the [`SPAN_OUTBOX_SEND`] span the dispatcher opens with `otel.kind = "producer"`).
/// Returns false (and stays silent) when the value is malformed — fail-open.
pub fn link_span_to_traceparent(span: &tracing::Span, traceparent: &str) -> bool {
    match parse_traceparent(traceparent) {
        Some(cx) => {
            span.add_link(cx);
            true
        }
        None => false,
    }
}

/// [`link_span_to_traceparent`] against the thread's current span.
pub fn link_current_span_to_traceparent(traceparent: &str) -> bool {
    link_span_to_traceparent(&tracing::Span::current(), traceparent)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The contract-frozen span names — a typo here would fan out across every emitting
    /// call site, so they are pinned verbatim.
    #[test]
    fn normative_span_names_are_pinned() {
        assert_eq!(SPAN_DISPATCH, "sutra.dispatch");
        assert_eq!(SPAN_RESOLVE, "sutra.resolve");
        assert_eq!(SPAN_VALIDATE, "sutra.validate");
        assert_eq!(SPAN_EXECUTE, "sutra.execute");
        assert_eq!(SPAN_DECODE, "sutra.decode");
        assert_eq!(SPAN_TEMPLATE, "sutra.template");
        assert_eq!(SPAN_DECISION, "sutra.decision");
        assert_eq!(SPAN_OUTBOX_SEND, "sutra.outbox.send");
    }

    #[test]
    fn metric_names_match_c6_table() {
        for (name, expected) in [
            (METRIC_INSTANCE_STARTED, "sutra.instance.started"),
            (METRIC_INSTANCE_COMPLETED, "sutra.instance.completed"),
            (METRIC_INSTANCE_SUSPENDED, "sutra.instance.suspended"),
            (METRIC_INSTANCE_RESUMED, "sutra.instance.resumed"),
            (METRIC_TOKEN_ENTERED, "sutra.token.entered"),
            (METRIC_TOKEN_LEFT, "sutra.token.left"),
            (METRIC_TASK_INVOKED, "sutra.task.invoked"),
            (METRIC_TASK_COMPLETED, "sutra.task.completed"),
            (METRIC_TASK_FAILED, "sutra.task.failed"),
            (METRIC_TASK_DURATION, "sutra.task.duration"),
            (METRIC_COVERAGE_PATH_COVERED, "sutra.coverage.path_covered"),
            (METRIC_COVERAGE_PERCENT, "sutra.coverage.percent"),
        ] {
            assert_eq!(name, expected);
        }
    }

    /// The shard-router meter names (execution scale-out §6.1) — pinned like the frozen
    /// set above; the `{shard}` dimension is `ATTR_SHARD`.
    #[test]
    fn shard_router_metric_names_are_pinned() {
        for (name, expected) in [
            (
                METRIC_ENGINE_SHARD_QUEUE_DEPTH,
                "sutra.engine.shard.queue-depth",
            ),
            (
                METRIC_ENGINE_SHARD_DISPATCHES,
                "sutra.engine.shard.dispatches",
            ),
            (METRIC_ENGINE_SHARD_PARKS, "sutra.engine.shard.parks"),
            (METRIC_ENGINE_SHARD_RESUMES, "sutra.engine.shard.resumes"),
            (METRIC_ENGINE_SHARD_HANDOFFS, "sutra.engine.shard.handoffs"),
            (
                METRIC_ENGINE_SHARD_CLAIM_BOUNCES,
                "sutra.engine.shard.claim-bounces",
            ),
        ] {
            assert_eq!(name, expected);
        }
        assert_eq!(ATTR_SHARD, "shard");
        assert_eq!(ATTR_CLAIM_BOUNCE_PATH, "path");
    }
}
