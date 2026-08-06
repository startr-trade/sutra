# Logging and audit

Three separate surfaces, deliberately not conflated: engine logs, CLI logs, and the audit trail.
Each has its own destination and its own purpose. None of the three is collected by Sutra itself —
see [No telemetry, no phone-home](../architecture/observability.md#no-telemetry-no-phone-home) for
that guarantee; everything below stays on the host (or goes to a destination *you* configure)
unless you point it somewhere else.

## Engine logs

Structured JSON on **stdout**, always, with no configuration required. The field shape
(`timestamp` / `level` / `loggerName` / `message` / `service.name`, plus `traceId`/`spanId`
inside a sampled span) is stable, so a log-processing pipeline can key off it directly. `RUST_LOG`
filters verbosity using standard `tracing` `EnvFilter` syntax (default `info`) — e.g.
`RUST_LOG=sutra_channels=debug,sutra_engine::deploy=trace` to raise one module without drowning
in the rest. See [Troubleshooting BPMN solutions](troubleshooting.md) for reading these logs
alongside traces and the audit trail when tracking down one message's path through the engine.

When an OTLP endpoint is configured (see [Configuration reference](configuration.md)), the same
log records additionally export over OTLP — stdout is never replaced, only supplemented.

## CLI logs

The `sutra` CLI writes its own logs to **stderr**, opt-in via `-v` / `-vv` / `-vvv` (info / debug /
trace) — never mixed into stdout, which is reserved for report output (`text` or `--format json`).
This is why every command in the [CLI reference](../reference/cli.md) is safe to pipe: `sutra
describe my-process.bpmn --format json | jq .` never has a stray log line corrupt the JSON.

## Audit sinks

The audit trail is a separate, compliance-oriented record of what a process instance did — every
`INSTANCE_STARTED`/`INSTANCE_COMPLETED`/`INSTANCE_SUSPENDED`/`INSTANCE_RESUMED` event, validation
outcomes, and relay/resume decisions — independent of the telemetry pipeline described in
[Observability](../architecture/observability.md). Three sinks, any combination of which can be
active at once:

| Config | env | What it does |
|---|---|---|
| `sutra.audit.jsonl.path` | `SUTRA_AUDIT_JSONL` | Writes one JSONL file (or a per-tenant directory tree) an operator can tail, or feed to `sutra audit-replay` (below) offline. |
| `sutra.audit.otel.endpoint` | `SUTRA_AUDIT_OTEL_ENDPOINT` | Ships audit events over OTLP to a collector. |
| `sutra.audit.sql` | `SUTRA_AUDIT_SQL` | Persists audit rows durably in the engine's own datasource, under the same row-level-security policy as every other engine table — see [Multi-tenancy and isolation](../architecture/multi-tenancy.md). |

## Replaying an instance's audit trail offline

```bash
sutra audit-replay <instance-id> --from-jsonl <path-to-file-or-dir> [--tenant <id>] [--until <EVENT_TYPE>]
```

Walks the JSONL audit stream for one instance id and prints its events in order — useful for
reconstructing what a specific production instance did without needing direct database access.
`--until` stops replay after a given event type (e.g. `INSTANCE_COMPLETED`).

## Sensitive data never appears in the clear

A variable tagged `sensitive` (via `q:variables`, see [The q: namespace](../building/q-namespace.md))
or read from a `dataClass`-tagged data store (see [Data stores](../building/data-stores.md)) is
redacted on every one of the surfaces above — audit sinks, structured logs, and traces alike — by
the same redactor mechanism described in
[Domain neutrality and the SPI model](../architecture/neutrality-and-spi.md). The flow itself
still sees the real value; only what the engine *emits* is masked.

## Next

- **[Observability](../architecture/observability.md)** — the telemetry (trace/metric/log) side
  this page's audit trail is deliberately kept separate from.
- **[Troubleshooting BPMN solutions](troubleshooting.md)** — putting logs, traces, and the audit
  trail together to answer "what happened to this message?"
