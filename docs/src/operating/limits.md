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
| Dapr | `500` to the sidecar |
| Knative | `500` to the Broker |

**Dapr and Knative are the exception to "permanent" today.** Both answer this rejection — and every
other intake rejection except a malformed CloudEvent — with `500`, and both pushers treat `500` as
retryable: the Dapr sidecar retries under the component's retry policy, and a Knative Broker
redelivers until the Trigger's `delivery` retries are spent. The same oversized message therefore
arrives again until the pusher gives up. Configure a dead-letter topic on the Dapr subscription, or a
`deadLetterSink` on the Trigger, so that it ends somewhere visible.

## Inbound HTTP request body

*(0.8.4)* An HTTP channel reads the request body under a limit of its own, before the payload cap
above applies:

| Config | Default | Range |
|---|---|---|
| `sutra.http.max-body-bytes` (`SUTRA_HTTP_MAX_BODY_BYTES`) | 2 MiB (`2097152`) | 1 KiB..1 GiB; a value outside is clamped, with a WARN |

An inbound HTTP channel overrides it with `max-body-bytes`, clamped the same way. A value that is
not a byte count refuses the channel with `SUTRA.CONFIG.PROPERTY.INVALID`.

```yaml
channels:
  - name: bulk-upload
    transport: http
    bind: "POST /channels/bulk-upload"
    max-body-bytes: 41943040     # 40 MiB for this channel
    payload-cap-bytes: 41943040  # the payload cap (10 MiB by default) applies too
```

The body is read after the route and the caller's credentials are checked. A body over the limit
answers **413** with the problem document `SUTRA.INBOUND.BODY_TOO_LARGE` (its `limitBytes`
attribute names the limit): at once when `Content-Length` declares the size, otherwise as soon as
the streamed bytes cross it. A body that breaks off mid-read answers 400
`SUTRA.INBOUND.BODY_UNREADABLE`. Before 0.8.4 the limit was a fixed 2 MiB, answered with a
plain-text 413.

A body under this limit still meets the payload cap, so a channel raised past 10 MiB needs both
raised. A proxy or ingress in front of the engine has its own limit; keep it at least as large.

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
the HTTP body limit, the payload cap and the two tenant quotas above are the complete set of per-message and
per-tenant limits. Limits on a shared physical resource (lines, seats, connections) are
[capacity pools](capacity-pools.md).

## Next

- **[Configuration reference](configuration.md)** — the full `sutra.*`/`SUTRA_*` key map these
  limits live in.
- **[Troubleshooting BPMN solutions](troubleshooting.md)** — what a rejected-message diagnostic
  looks like in practice, and how to trace it back to the message that triggered it.
