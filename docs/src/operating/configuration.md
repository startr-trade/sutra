# Configuration reference

The engine reads exactly one family of configuration keys — canonical `sutra.*` properties, each
with a canonical `SUTRA_*` environment-variable mirror. There is no separate framework prefix and
no legacy alias layer to reason about.

## Sources and precedence

```
canonical env  >  config file  >  built-in default
```

The config file path comes from `SUTRA_CONFIG` (default `sutra.properties`, read from the
engine's working directory, and only if present); file values may themselves use `${ENV}` /
`env:NAME` indirection. A key that has both a `sutra.*` file entry and its `SUTRA_*` environment
variable set always resolves from the environment.

`rust/crates/sutra-engine/src/config.rs` is the single source of truth for the exact key list —
this page is a map of what's there and how the pieces fit together, not a duplicate of it; treat
that file (and `otel.rs` for the telemetry keys) as authoritative if this page and the running
engine ever disagree.

## Deployment source

| `sutra.*` key | env | meaning |
|---|---|---|
| `sutra.deployment.source` | `SUTRA_DEPLOYMENT_SOURCE` | `dir` (default) — watch a folder of sealed archives — or `db` — the database-backed store, activated only via `POST /admin/deployments`. See [Deployment model](../architecture/deployment-model.md). |
| `sutra.deployments.dir` | `SUTRA_DEPLOYMENTS_DIR` | Required for the `dir` source: the directory of `.sutra` archives the engine watches. |
| `sutra.deployments.poll-interval` | `SUTRA_DEPLOYMENTS_POLL_INTERVAL` | How often the `dir` source rescans (default a few seconds). |
| `sutra.http.port` | `SUTRA_HTTP_PORT` | Listen port; `0` binds an OS-assigned port (always use `0` locally — see [Quickstart](../getting-started/quickstart.md)). |

## The engine's own datasource

| `sutra.*` key | env |
|---|---|
| `sutra.datasource.url` | `SUTRA_DATASOURCE_URL` |
| `sutra.datasource.username` | `SUTRA_DATASOURCE_USERNAME` |
| `sutra.datasource.password` | `SUTRA_DATASOURCE_PASSWORD` |

This is the engine-internal database (instances, outbox, lease, audit, inbox) — never a
package's own business data store, which owns its connection independently in `datastores.yaml`
(see [Data stores](../building/data-stores.md)). Note the CLI's `sutra migrate` reads a
*different*-named set (`SUTRA_DB_URL` / `SUTRA_DB_USERNAME` / `SUTRA_DB_PASSWORD` /
`SUTRA_DB_SCHEMA`) even when pointed at the same database — see the
[CLI reference](../reference/cli.md).

## Outbox and acknowledgement

| `sutra.*` key | env |
|---|---|
| `sutra.outbox.tick-interval` | `SUTRA_OUTBOX_TICK_INTERVAL` |
| `sutra.outbox.retry.base-delay` / `.max-delay` / `.jitter` | `SUTRA_OUTBOX_RETRY_BASE_DELAY` / `_MAX_DELAY` / `_JITTER` |
| `sutra.outbox.retry.max-attempts` | `SUTRA_OUTBOX_RETRY_MAX_ATTEMPTS` |
| `sutra.ack.deferred.capacity` | `SUTRA_ACK_DEFERRED_CAPACITY` |
| `sutra.ack.deferred.timeout` | `SUTRA_ACK_DEFERRED_TIMEOUT` |
| `sutra.ack.deferred.sweep-interval` | `SUTRA_ACK_DEFERRED_SWEEP_INTERVAL` |

Outbound deliveries retry with backoff forever by default. `sutra.outbox.retry.max-attempts`
is the opt-in ceiling: a delivery that exhausts it is marked terminally poisoned — retained
with its last error, never retried again, one incident recorded for a `required` delivery,
and no longer counted by the draining-deployment retirement gate. "We gave up" is a durable,
visible state, never a silent disappearance.

Full explanation of the deferred-ack registry the `sutra.ack.deferred.*` keys tune:
[Acknowledgement modes](ack-modes.md). Configuring the attempt ceiling is also what makes a
poisoned delivery reachable as a `<q:retry>` failure — see
[Retries, history, and schedules](../building/retries-history-schedules.md#the-outbox-poison-ceiling).

## Instance lifecycle — ownership and retention

| `sutra.*` key | env |
|---|---|
| `sutra.instance.sweep-interval` | `SUTRA_INSTANCE_SWEEP_INTERVAL` |
| `sutra.instance.claim-timeout` | `SUTRA_INSTANCE_CLAIM_TIMEOUT` |
| `sutra.instance.retention` | `SUTRA_INSTANCE_RETENTION` |
| `sutra.instance.retention-sweep-interval` | `SUTRA_INSTANCE_RETENTION_SWEEP_INTERVAL` |

Every resume claims the instance first, so two replicas can never advance one instance
concurrently; the stuck-instance sweep (`sweep-interval`, default `PT1M`) clears claims whose
owner has been silent longer than `claim-timeout` (default `PT5M`).

A finished instance is history, not a 404: terminal (completed/terminated) snapshots are
retained for `sutra.instance.retention` (ISO-8601 duration, default `P7D`) and served by
`GET /sutra/instances/{id}` and `GET /admin/instances/{id}/history`; a lease-gated sweeper
purges rows past the window on the `retention-sweep-interval` cadence (default `PT1H`).
`PT0S` restores delete-at-completion. Failed instances are always retained regardless of the
window — they need an operator before their deployment may retire; the operator action is
[instance migration](instance-migration.md). See
[Retries, history, and schedules](../building/retries-history-schedules.md) for what the
retention window makes queryable.

## External tasks (the pull worker surface) {#external-tasks-the-pull-worker-surface}

| `sutra.*` key | env |
|---|---|
| `sutra.external-task.default-lock-duration` | `SUTRA_EXTERNAL_TASK_DEFAULT_LOCK_DURATION` |
| `sutra.external-task.max-lock-duration` | `SUTRA_EXTERNAL_TASK_MAX_LOCK_DURATION` |
| `sutra.external-task.max-async-response-timeout` | `SUTRA_EXTERNAL_TASK_MAX_ASYNC_RESPONSE_TIMEOUT` |
| `sutra.external-task.max-tasks` | `SUTRA_EXTERNAL_TASK_MAX_TASKS` |
| `sutra.external-task.retries` | `SUTRA_EXTERNAL_TASK_RETRIES` |
| `sutra.external-task.retry-timeout` | `SUTRA_EXTERNAL_TASK_RETRY_TIMEOUT` |

A channel declaring `transport: pull` parks its deliveries as fetchable tasks instead of
dialing an endpoint; workers drive them over `POST /sutra/external-tasks/fetch-and-lock`
(bounded long poll) and `.../{id}/complete` / `.../{id}/failure`. The defaults: a `PT30S`
lock when the fetch names none (ceiling `PT1H` — a longer request is rejected, never
clamped), a `PT30S` long-poll ceiling, 100 tasks per fetch, a worker-failure budget of 3
with `PT10S` between attempts. A spent budget turns the task terminal (`failed`) — retained,
never fetchable again. The engine boots fail-closed if `default-lock-duration` exceeds its
own ceiling or is zero. Full treatment:
[External tasks](../building/external-tasks.md).

## Execution lanes

| `sutra.*` key | env | default |
|---|---|---|
| `sutra.engine.shards` | `SUTRA_ENGINE_SHARDS` | `1` |
| `sutra.engine.shard-queue-capacity` | `SUTRA_ENGINE_SHARD_QUEUE_CAPACITY` | unbounded |

`sutra.engine.shards` is the number of identical actor lanes the engine executes on inside one
replica. At `N > 1` all work for one instance — routed by a stable hash of its id — still runs on
**one** lane, in arrival order, one request at a time: **per-instance serialization is the
contract, and it holds at every N.** Values above 1 are accepted; the default stays `1` because
turning it up has one advertised consequence.

**Say it out loud before you raise it.** Incidental *cross-instance* serialization disappears at
`N > 1`. Two concurrent deliveries to two different instances of the same flow never interleave
under a single lane — as a side effect of there being one lane, never as a promise. At `N > 1`
they genuinely run in parallel. A deployment silently leaning on that side effect **will observe
new interleavings.** Every supported concurrency mechanism is unaffected: per-channel `singleton`
/ serial consumption, per-channel and per-tenant admission caps, and optimistic
`expect="unchanged"` / pessimistic `forUpdate` data-store writes.

`shard-queue-capacity` bounds each lane's mailbox (unset = unbounded; zero is rejected — "unset"
is how you say unbounded). A bounded send awaits on the *caller's* task, so backpressure
propagates outward to the transport that offered the work — an in-flight HTTP request, a broker
prefetch window, a poller tick — and never sideways into another lane.

Meters ship with the feature, each carrying the lane index as a dimension:
`sutra.engine.shard.queue-depth` (per-lane backlog and skew), `…dispatches` / `…parks` /
`…resumes`, `…handoffs` (cross-lane relay hops — expected, and rising with lane count by
construction), and `…claim-bounces` split `relay` / `timer`. That last one is the **mis-route
alarm**: on a healthy rollout it reads near zero outside genuine cross-replica contention.

The **live** lane count is readable without reading config: `GET /sutra/health/ready` reports it
under the loader check's `data.shards`, read off the running router rather than echoed back from
configuration.

Full picture: [Execution lanes](../architecture/execution-lanes.md).

## Limits

| `sutra.*` key | env |
|---|---|
| `sutra.codec.max-payload-bytes` | `SUTRA_CODEC_MAX_PAYLOAD_BYTES` |

Full explanation: [Limits and quotas](limits.md).

## Admin API auth

| `sutra.*` key | env |
|---|---|
| `sutra.admin.auth.scheme` | `SUTRA_ADMIN_AUTH_SCHEME` (`apikey` \| `bearer`) |
| `sutra.admin.auth.key-ref` | `SUTRA_ADMIN_AUTH_KEY_REF` |
| `sutra.admin.auth.header` | `SUTRA_ADMIN_AUTH_HEADER` (default `X-API-Key`) |
| `sutra.admin.oidc.issuer` / `.audience` / `.jwks` / `.role-claim` / `.required-role` | `SUTRA_ADMIN_OIDC_*` |
| `sutra.admin.oidc.dev-disabled` | `SUTRA_ADMIN_OIDC_DEV_DISABLED` |

The `/admin/*` surface (deployments, instance inspection, subject erasure) is gated fail-closed:
unconfigured returns `503`, a missing/invalid credential returns `401`, and a valid token missing
the required role/claim returns `403` — never silently open. The auth-key scheme is the same
static-secret model channels use for inbound HTTP auth, and it takes precedence when both are set.
Gating is disabled only via the explicit `sutra.admin.oidc.dev-disabled=true` escape hatch.

## Audit sinks

| `sutra.*` key | env |
|---|---|
| `sutra.audit.jsonl.path` | `SUTRA_AUDIT_JSONL` |
| `sutra.audit.otel.endpoint` | `SUTRA_AUDIT_OTEL_ENDPOINT` |
| `sutra.audit.sql` | `SUTRA_AUDIT_SQL` |

See [Logging and audit](logging.md).

## Telemetry (OTel)

| `sutra.*` key | canonical env | standard `OTEL_*` env also accepted |
|---|---|---|
| `sutra.telemetry.otlp.endpoint` | `SUTRA_TELEMETRY_OTLP_ENDPOINT` | `OTEL_EXPORTER_OTLP_ENDPOINT` |
| `sutra.telemetry.service-name` | `SUTRA_TELEMETRY_SERVICE_NAME` | `OTEL_SERVICE_NAME` |
| `sutra.telemetry.enabled` | `SUTRA_TELEMETRY_ENABLED` | `OTEL_SDK_DISABLED` (inverted) |
| `sutra.telemetry.metrics.export-interval` | `SUTRA_TELEMETRY_METRICS_EXPORT_INTERVAL` | `OTEL_METRIC_EXPORT_INTERVAL` |
| `sutra.telemetry.metrics.temporality-preference` | `SUTRA_TELEMETRY_METRICS_TEMPORALITY_PREFERENCE` | `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` |
| `sutra.telemetry.metric-labels` | `SUTRA_TELEMETRY_METRIC_LABELS` | — (default `tenant,module,version`) |

No OTLP endpoint configured means no exporters at all, and this is the default — telemetry export
is opt-in, never opt-out, and Sutra itself collects nothing regardless: see
[No telemetry, no phone-home](../architecture/observability.md#no-telemetry-no-phone-home) for the
exact guarantee, and [Observability](../architecture/observability.md) for what each signal
actually carries once you do turn export on.

## Secrets — never literal values

Every credential-shaped field across `channels.yaml`, `datastores.yaml`, and the admin-auth keys
above is a **reference**, resolved at startup or channel-activation time — `env:NAME`,
`secret:KEY` (a file under the mounted secrets directory, default `/etc/sutra/secrets` /
`SUTRA_SECRETS_DIR`), `${NAME}` / `${NAME:default}` placeholders, or a vendor scheme
(`vault:…`, `aws-secrets:…`, `azure-kv:…`, `gcp-secret:…`) resolved by whichever
`sutra-envref-<vendor>` crate the binary was built with. A literal secret value in a resource
file is rejected at package-validation time, not just discouraged by convention. See
[Domain neutrality and the SPI model](../architecture/neutrality-and-spi.md#secrets--sutra-envref-spi)
for how a vendor secret backend plugs in.

## Next

- **[Acknowledgement modes](ack-modes.md)**
- **[Limits and quotas](limits.md)**
- **[Logging and audit](logging.md)**
- **[Instance migration](instance-migration.md)**
- **[Execution lanes](../architecture/execution-lanes.md)**
