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
| `sutra.http.max-body-bytes` | `SUTRA_HTTP_MAX_BODY_BYTES` | *(0.8.4)* Inbound HTTP request-body limit, default `2097152` (2 MiB), clamped to 1 KiB..1 GiB with a WARN. An HTTP channel's `max-body-bytes` overrides it; a larger body answers 413 `SUTRA.INBOUND.BODY_TOO_LARGE`. See [Limits and quotas](limits.md#inbound-http-request-body). |

## The engine's own datasource

| `sutra.*` key | env |
|---|---|
| `sutra.datasource.url` | `SUTRA_DATASOURCE_URL` |
| `sutra.datasource.username` | `SUTRA_DATASOURCE_USERNAME` |
| `sutra.datasource.password` | `SUTRA_DATASOURCE_PASSWORD` |
| `sutra.datasource.max-connections` | `SUTRA_DATASOURCE_MAX_CONNECTIONS` |
| `sutra.datasource.dispatch-max-connections` | `SUTRA_DATASOURCE_DISPATCH_MAX_CONNECTIONS` |

*Since 0.6.5* the engine opens two pools over this database. The **main** pool
(`max-connections`, default `8`, the size it always had) serves the execution lanes: intake,
steps and resumes. Size it at least to `sutra.engine.shards`, because each lane holds a
connection while it commits a step. The **dispatch** pool (`dispatch-max-connections`, default
`4` since 0.8.3, `2` before) serves the outbox dispatcher and the timer poller, so a burst of
intake can never leave delivery waiting for a connection. The claimer and the delete batcher hold
one connection each; the others serve the defers and requeues of failed sends in a burst.

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
| `sutra.outbox.workers` | `SUTRA_OUTBOX_WORKERS` |
| `sutra.outbox.destination-concurrency` | `SUTRA_OUTBOX_DESTINATION_CONCURRENCY` |
| `sutra.outbox.max-buffered` | `SUTRA_OUTBOX_MAX_BUFFERED` |
| `sutra.outbox.lag-warn` | `SUTRA_OUTBOX_LAG_WARN` |
| `sutra.outbox.batch-size` | `SUTRA_OUTBOX_BATCH_SIZE` |
| `sutra.outbox.retry.base-delay` / `.max-delay` / `.jitter` | `SUTRA_OUTBOX_RETRY_BASE_DELAY` / `_MAX_DELAY` / `_JITTER` |
| `sutra.outbox.retry.max-attempts` | `SUTRA_OUTBOX_RETRY_MAX_ATTEMPTS` |
| `sutra.ack.deferred.capacity` | `SUTRA_ACK_DEFERRED_CAPACITY` |
| `sutra.ack.deferred.timeout` | `SUTRA_ACK_DEFERRED_TIMEOUT` |
| `sutra.ack.deferred.sweep-interval` | `SUTRA_ACK_DEFERRED_SWEEP_INTERVAL` |

The outbox delivers many rows at once, under three bounds (*since 0.8.3*):

- `sutra.outbox.workers` (default `32`, 1..1024) is the most sends in flight at once, across
  every destination. `1` restores one delivery at a time. Before 0.8.3 it was a count of workers
  rows were spread over by instance, defaulting to the lane count and at least 8.
- `sutra.outbox.destination-concurrency` (default `32`) is the most sends in flight to one
  destination authority: scheme, host and port, the path ignored. It is clamped to `workers` with
  a WARN. A row waiting on a busy destination holds no global slot, so a slow destination does
  not delay the others.
- `sutra.outbox.max-buffered` (default `4096`, from `workers` to 100 000) is the most rows held in
  memory, in flight or queued. Past it the claimer waits.

One instance's deliveries still go out one after another, in the order they were enqueued;
different instances never share a queue. `sutra.outbox.lag-warn` (default `PT10S`, `PT0S` = off)
logs one WARN a minute while delivered rows exceed it, enqueue to delivery. A step that enqueues wakes the dispatcher at once, and a `local://` delivery is handed
to it in-process as soon as the step's commit is durable, so the tick (`tick-interval`, default
`PT5S`) only paces retries that fall due and rows another replica enqueued.
`sutra.outbox.batch-size` (default `50`) is how many due rows one claim takes per deployment.
Delivered rows are deleted in batches. See [Tuning for throughput](throughput-tuning.md).

Outbound deliveries retry with backoff forever by default. `sutra.outbox.retry.base-delay` defaults to `PT1S`:
retry *n* waits about `base-delay × 2ⁿ`, so the first retry comes about 2 s after the failure. Since 0.6.20 each
failed attempt that will be retried logs a WARN with the destination, the cause's code and message, the attempt number
and `next_attempt_at`. `sutra.outbox.retry.max-attempts`
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

With `<q:audit capture="media">`, retention also deletes the recordings the purged instances'
audit events reference, from whichever declared object store holds them (below).

## Capacity pools

| `sutra.*` key | env |
|---|---|
| `sutra.capacity-pools` | `SUTRA_CAPACITY_POOLS` |
| `sutra.capacity-pools.<name>.slots` | `SUTRA_CAPACITY_POOLS_<NAME>_SLOTS` |
| `sutra.capacity-pools.<name>.reserve.inbound` | `SUTRA_CAPACITY_POOLS_<NAME>_RESERVE_INBOUND` |
| `sutra.capacity-pools.<name>.reserve.outbound` | `SUTRA_CAPACITY_POOLS_<NAME>_RESERVE_OUTBOUND` |
| `sutra.capacity-pools.<name>.tenants` | `SUTRA_CAPACITY_POOLS_<NAME>_TENANTS` |

Named, cluster-wide slot counts that channels of any deployment draw from with `pool: <name>`.
Full explanation, defaults and a walkthrough: [Capacity pools and sessions](capacity-pools.md).

## Object stores

| `sutra.*` key | env |
|---|---|
| `sutra.object-stores` | `SUTRA_OBJECT_STORES` |
| `sutra.object-stores.<name>.uri` | `SUTRA_OBJECT_STORES_<NAME>_URI` |
| `sutra.object-stores.<name>.endpoint` | `SUTRA_OBJECT_STORES_<NAME>_ENDPOINT` |
| `sutra.object-stores.<name>.region` | `SUTRA_OBJECT_STORES_<NAME>_REGION` |
| `sutra.object-stores.<name>.access-key` / `.secret-key` | `SUTRA_OBJECT_STORES_<NAME>_ACCESS_KEY` / `_SECRET_KEY` |
| `sutra.object-stores.<name>.encryption` | `SUTRA_OBJECT_STORES_<NAME>_ENCRYPTION` |
| `sutra.object-stores.<name>.kek` | `SUTRA_OBJECT_STORES_<NAME>_KEK` |
| `sutra.object-stores.<name>.key-id` | `SUTRA_OBJECT_STORES_<NAME>_KEY_ID` |

Named S3-compatible buckets the engine reads recorded media from (an `llm` channel's
`media-store`) and deletes it in (erasure and retention). Credentials and the key-encryption
secret are references, resolved at boot; a store that cannot be built refuses startup. Full
explanation and a walkthrough: [Recordings and media references](../building/recordings-and-media.md#object-stores).

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

## Broker connectivity events

| Key | Default | Meaning |
|---|---|---|
| `sutra.engine.connectivity-events.channel` (env `SUTRA_CONNECTIVITY_EVENTS_CHANNEL`) | unset | *(0.8.0)* A channel name. When set, every broker outage a transport sees on one of a deployment's channels is delivered to that deployment as an inbound JSON message on the channel of this name, if the deployment declares one. Unset: nothing is collected. |

Declare the channel in the package as an in-process intake and start a process on it:

```yaml
channels:
  - name: connectivity-in
    transport: local
    codec: json
```

Each outage produces two messages, one when it starts and one when it ends:

```json
{"code":"SUTRA.INBOUND.AMQP.OUTAGE_ENDED","channel":"orders-in","direction":"inbound","state":"up",
 "startedAt":"2026-11-19T10:00:00.120Z","endedAt":"2026-11-19T10:01:00.420Z","ms":60300,
 "detail":"amqp broker mq.example:5672"}
```

- `state` is `down` when the outage starts (no `endedAt` or `ms`) and `up` when it ends.
- `code` is the transport's own log code for the change, so the message and the engine log line
  match.
- `direction: inbound` is an intake losing its connection. `direction: outbound` is a send
  channel whose send could not reach its broker; each send channel that sent during the outage
  reports its own window, ending on its first send that reaches the broker again. A send
  channel that sent nothing during the outage reports nothing.
- A deployment only receives events for its own channels. One that declares no channel of the
  configured name receives nothing.
- Delivery is best effort: up to 1024 events wait per deployment; past that the oldest is
  dropped and `SUTRA.CONNECTIVITY.EVENTS_DROPPED` is logged. An event the deployment refuses is
  logged as `SUTRA.CONNECTIVITY.EVENT_UNDELIVERED` and never retried.
- The AMQP 1.0 transport reports these events. Transports without a long-lived broker
  connection (HTTP, for example) report none.

[Broker outages as messages](connectivity-events.md) runs it against a broker that is stopped and
started.

## Limits

| `sutra.*` key | env |
|---|---|
| `sutra.codec.max-payload-bytes` | `SUTRA_CODEC_MAX_PAYLOAD_BYTES` |
| `sutra.http.max-body-bytes` | `SUTRA_HTTP_MAX_BODY_BYTES` |

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
From 0.6.18 the gate also covers `GET /sutra/instances`, its `?alias=` search and `GET /sutra/instances/{id}`
when admin auth is configured. An engine with no admin configuration at all still answers them without a key,
so keep such an engine where only operators can reach it. See [Upgrading to
0.6.18](upgrading-0-6-18.md#reading-instances-takes-the-admin-key).

## Audit sinks

| Key | Default | Meaning |
|---|---|---|
| `sutra.audit.sql.url` | — (no SQL store) | A `postgres://` URL of the **audit database**, which is never the engine's own (`SUTRA_AUDIT_SQL_URL`). Setting it turns the SQL store on; it serves `/admin/instances/{id}/history`. The URL naming the engine's own database is refused at startup (`SUTRA.CONFIG.AUDIT.SHARES_ENGINE_DB`); the same server with another database is allowed with a warning. The engine applies the audit migrations (V2xx) to it at boot (`SUTRA_AUDIT_DB_MIGRATIONS`, default `SUTRA_DB_MIGRATIONS`); `sutra migrate --audit-url` does it by hand. If it is unreachable at boot, the engine retries every 5 s and the spool holds the events. |
| `sutra.audit.sql.username` / `sutra.audit.sql.password` | the URL's | Override the URL's login; secret references (`env:`, `secret:`, …). |
| `sutra.audit.sql.max-connections` | `4` | The audit pool's size. |
| `sutra.audit.sql` | — | **Deprecated** boolean. `true` without `sutra.audit.sql.url` is the startup error `SUTRA.CONFIG.AUDIT.SQL_URL_REQUIRED`; `false` with a URL is `SUTRA.CONFIG.AUDIT.SQL_CONFLICT`. Set the URL and drop it. |
| `sutra.audit.jsonl.dir` | — (no file store) | Directory of the JSONL file store (`SUTRA_AUDIT_JSONL_DIR`): `audit-<spoolId>-<UTC time>.jsonl`, one event per line, readable by `sutra audit-replay --from-jsonl` and `jq`. Setting it turns the store on. |
| `sutra.audit.jsonl.rotate-bytes` / `.rotate-interval` | `268435456` (256 MiB) / `PT1H` | A file rotates once it is that big or that old. |
| `sutra.audit.jsonl.dedup-window` | `PT15M` | A step re-run after a crash re-emits its events; within this window the second copy is dropped. Readers dedupe on `(deploymentId, instanceId, seq)` beyond it. |
| `sutra.audit.jsonl.retention` | — (keep all) | Closed files older than this are deleted (the open file never). |
| `sutra.audit.jsonl.path` | — | **Deprecated** single-file form of the JSONL sink; setting it together with `sutra.audit.jsonl.dir` is a startup error. |
| `sutra.audit.spool.dir` | `/var/lib/sutra/audit-spool` | The durable spool (`SUTRA_AUDIT_SPOOL_DIR`). Every audit event is appended and fsynced here before the step that produced it commits; a shipper then loads it into each configured store. **Mount it on persistent storage** (a volume per replica; a StatefulSet's PVC), or crash durability is only as good as the container's disk. Opened only when at least one store is configured. A second engine on the same directory fails with `SUTRA.AUDIT.SPOOL.LOCKED`. |
| `sutra.audit.spool.segment-bytes` | `67108864` (64 MiB) | Size of a spool segment. |
| `sutra.audit.spool.sync-linger-ms` | `5` | Group sync: how long the spool waits to batch fsyncs. `0` syncs at once. |
| `sutra.audit.spool.max-bytes` | `8589934592` (8 GiB) | Unshipped bytes the spool accepts. Above it, readiness goes DOWN and step commits fail until the shipper catches up; events are never dropped. |
| `sutra.audit.spool.sync-timeout` | `PT30S` | How long a commit waits for its events to be durable. A failed or timed-out sync fails the commit (readiness is DOWN while the spool is unhealthy), so an event is either durable or its step did not commit: audit is never lost. |
| `sutra.audit.spool.retain` | `PT1H` | How long a fully shipped segment is kept before it is deleted. Unshipped segments are never deleted. |
| `sutra.audit.batch-max` / `sutra.audit.batch-linger-ms` | `256` / `5` | The shipper's batch: up to this many records, or what arrived within the linger. |
| `sutra.audit.default-sink` | the only configured store | The store a process audits into when neither its package nor its manifest names one. With several stores configured and none named, deploy warns (`SUTRA.DEPLOY.AUDIT.SINK_MISSING`) and the events are counted `sutra.audit.undeliverable`, never rerouted. |
| `sutra.audit.shutdown-timeout` | `PT10S` | How long shutdown waits for each store to ship what the spool holds. |
| `sutra.audit.retention` | — (keep all) | How long a row stays in the audit database; the audit sweeper deletes older ones. |
| `sutra.audit.retention-sweep-interval` / `sutra.audit.retention-batch` | `PT10M` / `10000` | How often retention runs (also for the JSONL files), and rows deleted per statement. |
| `sutra.audit.otel.endpoint` | — | The OTLP audit store. |
| `sutra.audit.otel.service-name` | `sutra-audit` | `service.name` of the OTLP audit pipeline's resource (0.6.12). The rest of its resource is the engine telemetry's: `OTEL_RESOURCE_ATTRIBUTES` plus `sutra.telemetry.resource-attributes`. |
| `sutra.admin.auth.*` / `sutra.admin.oidc.*` | fail-closed | The `/admin/*` gate, which also covers `GET /sutra/instances` and `GET /sutra/instances/{id}` once either is configured (those two stay open when admin auth is off): static key/secret (`auth.*`) or OIDC bearer-JWT (`oidc.*`); unconfigured ⇒ `/admin/*` is 503, never open; dev bypass only via the explicit `sutra.admin.oidc.dev-disabled=true`. |

Audit is declared in the BPMN (`<q:audit>`), and these keys say where it goes. Every event is made
durable in the spool before its step commits, then shipped to each configured store. Audit never
shares the engine's database. See [Upgrading to 0.9](upgrading-0-9.md) and [Recording audit
points](../building/recording-audit-points.md).

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
| `sutra.telemetry.resource-attributes` | `SUTRA_TELEMETRY_RESOURCE_ATTRIBUTES` | `OTEL_RESOURCE_ATTRIBUTES` (both apply; the `sutra.*` value wins per key; 0.6.12) |

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
