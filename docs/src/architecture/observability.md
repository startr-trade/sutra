# Observability

Sutra exports all three signals — traces, metrics, and logs — through OpenTelemetry, and is
fail-open about it: telemetry can never affect message processing. No endpoint configured means no
exporters and zero overhead; a bad config value falls back to a default with a warning; an
exporter failure only logs, it never propagates into intake or dispatch.

## No telemetry, no phone-home

Sutra collects nothing on its own behalf. The engine binary, the `sutra` CLI, and the container
image `sutra-dist` produces send no usage statistics, no error or crash reports, and no telemetry
of any kind to any Sutra-affiliated destination — there isn't one, and no code path sends
anywhere by default. Nothing phones home, in any build, ever.

The *only* way data leaves a running engine to a system outside it is the operator-configured
OpenTelemetry export this chapter describes, and export is opt-in, not opt-out (see
[Configuration reference](../operating/configuration.md#telemetry-otel)). With no OTLP endpoint
configured, the engine emits structured JSON logs to stdout and nothing else — the boot log says so
in plain text (`telemetry export off (no OTLP endpoint configured) — JSON stdout logs only`), and
that posture is enforced in the engine's own telemetry bootstrap
(`rust/crates/sutra-engine/src/otel.rs`), not just asserted here. Point
`sutra.telemetry.otlp.endpoint` (or the standard `OTEL_EXPORTER_OTLP_ENDPOINT`) at a collector you
run and the signals below start exporting there; leave it unset and nothing crosses the process
boundary except whatever you separately configured elsewhere (a channel, a datastore, an audit
sink) — this is a property you can verify by reading the source or watching an unconfigured
engine's own boot log, not a policy statement to take on faith.

## Traces

The engine's existing `tracing` spans (`sutra.dispatch`, `sutra.resolve`, `sutra.decode`,
`sutra.validate`, `sutra.execute`, `sutra.outbox.send`) export through a
`tracing-opentelemetry` layer with no call-site changes — the same spans that back local
`RUST_LOG` debugging (see [Troubleshooting BPMN solutions](../operating/troubleshooting.md)) are
what leaves the process as OTLP.

**A suspended instance does not leak an open span.** Because a wait state can hold an instance for
an arbitrary length of time (see [Wait states and human tasks](../building/wait-states.md)), the
executor fires a listener event at suspend that force-ends every span open for that instance —
otherwise a long park would show up as a pathologically long trace. Each **segment** of a
stateful flow's lifecycle (the initial run, then each resume) gets its own trace with its own
`traceId`; there is no trace-of-traces joining them, since OpenTelemetry traces are flat. What
ties the segments together for a human reading a Gantt view is the **instance id**, stamped as a
plain span attribute (`bpm.instance.id`) on every span belonging to that instance — filter on it
and the segments line up on one timeline, with the waits showing as the gaps between them.

## Metrics

An `ExecutionListener` (the same lifecycle bus described in
[Domain neutrality and the SPI model](neutrality-and-spi.md)) maps instance/token/task events onto
a fixed set of meter names (`sutra.instance.*`, `sutra.token.*`, `sutra.task.*`,
`sutra.coverage.path_covered` — see [Coverage: declared routes as the compliance
signal](../building/coverage.md) for what that one actually tracks), tagged with the deployment id
and a configurable label allowlist. Alongside those, the `sutra.engine.shard.*` family reports
per-execution-lane queue depth, work rates, cross-lane handoffs, and claim bounces — see
[Execution lanes](execution-lanes.md).
Delta vs. cumulative temporality follows the standard
`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` variable — set it to `delta` for an
Elasticsearch-backed collector, which drops cumulative histograms.

## Logs

Structured JSON on stdout, always, with no configuration required — this is the log path every
deployment gets whether or not OTLP is configured. When an OTLP endpoint *is* configured, the same
log records additionally export over OTLP. The field shape (`timestamp`/`level`/`loggerName`/
`message`/`service.name`, plus `traceId`/`spanId` inside a sampled span) is stable and is what any
log-processing pipeline should key off. See [Logging and audit](../operating/logging.md) for the
operator-facing configuration.

## Cardinality discipline

Tenant id is deliberately **not** a tag on high-cardinality metrics (per-task duration
histograms, for instance) — only on instance-counting metrics, where the cardinality stays
bounded by tenant count rather than by tenant × task-name × outcome. This is the same discipline
that keeps the label allowlist above short by default.

## The reference stack

The repo ships a reference EFK-family stack as a set of OpenTofu modules
(`deploy/modules/efk-stack`) — an OTel Collector, Elasticsearch, Kibana, and a Fluent Bit
DaemonSet for host-level logs — deployed alongside the engine module in a dev/small-prod cluster,
or pointed at an external, already-owned observability stack in a larger one. It's a reference,
not a requirement: the engine's only actual contract is the three endpoint inputs (OTLP, an
optional log-forward target, an optional direct Elasticsearch endpoint for the audit fan-out) — any
OTLP-speaking collector on the other end works.

## Next

- **[Logging and audit](../operating/logging.md)** — configuring the endpoints above, and the
  audit trail as a separate, compliance-oriented sink from telemetry.
- **[Troubleshooting BPMN solutions](../operating/troubleshooting.md)** — using traces and audit
  together to retrace what one message actually did.
