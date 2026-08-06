# Limits and quotas

Engine-wide ceilings, each with a safe shipped default and a consistent override shape — the same
canonical `sutra.*` / `SUTRA_*` pair described in
[Configuration reference](configuration.md).

## Inbound payload byte cap

Every inbound channel — broker or HTTP — delivers raw bytes to the engine before a codec ever
parses them, and a producer can publish an arbitrarily large payload. The cap closes that gap: a
message is byte-length-checked *before* `codec.decode()` runs, so nothing is allocated or parsed
for an oversized message.

| Config | Default | Disabled value |
|---|---|---|
| `sutra.codec.max-payload-bytes` (`SUTRA_CODEC_MAX_PAYLOAD_BYTES`) | 10 MiB (`10485760`) | `0` |

A per-channel override lives on the channel's own definition (not as a separate engine-wide
property), and is **not** clamped by the global value — it can raise or lower the effective cap for
just that channel:

```yaml
channels:
  - name: bulk-statements-acme
    payload-cap-bytes: 104857600   # raised for a trusted large-file channel
  - name: heartbeat-acme
    payload-cap-bytes: 4096        # tightened for a strict small-event channel
```

Negative values are rejected at startup (`SUTRA.CONFIG.PROPERTY.INVALID`) — this catches a typo
like `-1` before it silently disables the cap the way some other libraries interpret that value.

**On rejection.** Exceeding the effective cap rejects the message with
`SUTRA.INBOUND.PAYLOAD_TOO_LARGE` (ERROR), carrying the channel id, the actual payload size, and
the effective cap that was applied — enough for an operator to know exactly which config key to
raise. This is a **permanent reject**, not a retryable one:

| Transport | Rejection translates to |
|---|---|
| HTTP | `413` |
| RabbitMQ / AMQP | `basic_nack` with `requeue=false` → the broker's DLX (or dropped if none configured) |
| Kafka | Offset committed — the poison record is skipped, not replayed (wire a dead-letter topic in BPMN if you need one) |
| AWS SQS | `DeleteMessage` — removed, not redelivered |
| GCP Pub/Sub | `message.ack()` — removed, not redelivered |
| File | The source file is moved to the `failed/` sub-directory |

## Per-tenant quotas

Two more admission checks, enforced before a message reaches the executor — see
[Multi-tenancy and isolation](../architecture/multi-tenancy.md) for the full detail:

- **`maxConcurrentInstances`** — a hard cap on simultaneously in-flight instances for a tenant,
  coherent across every replica.
- **`maxInboundRatePerMinute`** — a per-replica sliding 60-second admission window.

Neither is applied unless a tenant opts in — an unconfigured tenant is unlimited on both
dimensions.

## What's not yet a configurable limit

The threat-model backlog names a few more ceilings that aren't wired yet — a FEEL evaluation
wall-clock/memory budget per expression, and a per-tenant audit-write rate cap. Until they land,
the payload cap and the two tenant quotas above are the complete set.

## Next

- **[Configuration reference](configuration.md)** — the full `sutra.*`/`SUTRA_*` key map these
  limits live in.
- **[Troubleshooting BPMN solutions](troubleshooting.md)** — what a rejected-message diagnostic
  looks like in practice, and how to trace it back to the message that triggered it.
