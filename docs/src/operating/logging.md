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

A process's events go to one sink: the one its `<q:audit sink="…">` names, or else the
highest-priority sink that is switched on (`sql`, then `jsonl`, then `otel`).

- **JSONL.** Each event is one line, written in a single append, so a file shared by every
  executor lane never holds two events on one line (fixed in 0.6.9).
- **OTLP.** The records are OTLP log records on the instrumentation scope `sutra.audit`, sent
  through a logs pipeline of their own to `SUTRA_AUDIT_OTEL_ENDPOINT`, never through the telemetry
  endpoint. The engine builds that pipeline once at start and flushes it on shutdown. Before
  0.6.9, setting this variable made the engine panic at start.

## Resource attributes on audit records {#resource-attributes-on-audit-records}

The OTLP audit records carry the same resource attributes as the engine's own telemetry, so a
collector can tell which deployment, environment or test run an audit record belongs to:

| Source | Example | On audit records |
|---|---|---|
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=staging,example.run_id=run-42` | yes |
| `sutra.telemetry.resource-attributes` / `SUTRA_TELEMETRY_RESOURCE_ATTRIBUTES` (0.6.12) | `example.from_config=yes` | yes, and on every telemetry signal; wins over `OTEL_RESOURCE_ATTRIBUTES` for the same key |
| `sutra.audit.otel.service-name` / `SUTRA_AUDIT_OTEL_SERVICE_NAME` (0.6.12) | `orders-audit` | the records' `service.name`; default `sutra-audit` |
| `OTEL_SERVICE_NAME` | `orders-engine` | no: it names the engine's telemetry, never the audit stream |

To try it, point the audit pipeline at a collector whose `debug` exporter prints what it receives
(`verbosity: detailed`), and start the engine with:

```bash
SUTRA_AUDIT_OTEL_ENDPOINT=http://collector:4317
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=conformance,example.run_id=run-42
SUTRA_TELEMETRY_RESOURCE_ATTRIBUTES=example.from_config=yes
```

Run any process with `<q:audit sink="otel">`. Each batch of audit records the collector prints
starts with the resource, which includes (with the `telemetry.sdk.*` attributes, in any order):

```text
Resource attributes:
     -> deployment.environment: Str(conformance)
     -> example.run_id: Str(run-42)
     -> example.from_config: Str(yes)
     -> service.name: Str(sutra-audit)
```

The conformance suite `tc_audit_sinks` runs exactly this against a real collector.

## What an audit event captures

A process chooses how much each of its audit events carries, with `<q:audit capture="…">`:

| `capture` | Each event carries |
|---|---|
| `none` | no audit event at all |
| `metadata` | the event, node and diagnostic, with no variable content |
| `payload` (the default) | the redacted variable snapshot |
| `media` (0.6) | metadata, plus the media references found among the variables (`{kind: "media", uri, sha256, …}`), never the media itself |

With `media`, subject erasure and terminal retention also delete the recordings those events
reference, and amend each event to say they are gone. See
[Recordings and media references](../building/recordings-and-media.md) for the mechanism and a
walkthrough.

## Correlation keys on every audit event

From 0.6.9, each audit event carries the instance's correlation keys: every `<q:alias>` the
process declares, with its current value. That lets you find all of one business transaction's
events by the key your systems already use, without first looking up the instance id.

| Sink | Where the keys are |
|---|---|
| JSONL | a `correlation` object on the line: `"correlation":{"draftId":"d-7"}` |
| OTLP | one attribute per key, named `correlation.<alias name>` |
| SQL | not stored; the `audit_event` row is unchanged |

- **Where the values come from.** A start event's aliases are bound into the instance's variables
  under the alias name, and the variables travel with the instance, so every event reads its keys
  from there, including after a resume on another replica. No database read is added per event.
- **Wait-node aliases.** An alias declared on a wait node is evaluated over the current variables.
  Until it has a value, the key is left out.
- **Transport events and unaudited processes.** Transport events carry no keys, and a process that
  is not audited computes none.

Redaction applies to the keys as it does everywhere else:

- A key shows `***REDACTED***` when the alias name, or any variable its expression reads, is
  declared `sensitive`.
- When the expression reads a payload that has a `<q:redactors>` chain, the key comes from the
  masked projection, never from the raw payload. When the expression is more than a plain path,
  the key shows `***REDACTED***`.

### Try it

The `review-loop` example correlates every message by its start event's
`<q:alias name="draftId" expression="payload.draftId">`. Its compose file switches on the SQL
sink; this override sends the audit trail to a JSONL file instead. You need Docker, the `sutra` CLI
and an engine image, both 0.6.9 or later. From the repository root:

```bash
C=examples/review-loop/deploy/compose.yaml
cat > /tmp/audit-jsonl.yaml <<'YAML'
services:
  engine:
    environment:
      SUTRA_AUDIT_SQL: "false"
      SUTRA_AUDIT_JSONL: /tmp/audit/audit.jsonl
YAML
docker compose -f $C -f /tmp/audit-jsonl.yaml up -d --wait
sutra package examples/review-loop/deployments-src/default--review--1.0.0 \
    --out examples/review-loop/deploy/deployments 2>/dev/null
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-review-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done
send() { curl -s -o /dev/null -w "HTTP %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/drafts$1" "${H[@]}" -d "$2"; }
send "" '{"draftId":"d-7"}'
send /review '{"draftId":"d-7","decision":"approve"}'
docker compose -f $C cp engine:/tmp/audit/audit.jsonl - | tar -xO | head -3
docker compose -p sutra-review-loop down -v
```

Both requests answer `HTTP 200`, and every line carries the draft's key (ids and times differ):

```text
{"instanceId":"66ed7453-…","deploymentId":"dep-84173f…","tenant":"default","seq":1,"at":"…","eventType":"INSTANCE_STARTED","payload":{},"correlation":{"draftId":"d-7"}}
{"instanceId":"66ed7453-…","deploymentId":"dep-84173f…","tenant":"default","seq":2,"at":"…","eventType":"NODE_ENTERED","nodeId":"Start","payload":{},"correlation":{"draftId":"d-7"}}
{"instanceId":"66ed7453-…","deploymentId":"dep-84173f…","tenant":"default","seq":3,"at":"…","eventType":"NODE_LEFT","nodeId":"Start","payload":{},"correlation":{"draftId":"d-7"}}
```

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
