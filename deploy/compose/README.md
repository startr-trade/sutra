# Local backing-services stack (docker compose)

A self-contained `docker compose` stack of the backing services the Sutra engine
expects — for **dev/prod parity** (15-factor Factor 10). Production ships these as
OpenTofu modules against Kubernetes (`deploy/modules/local-k8s-infra`,
`deploy/modules/local-k8s-cluster`, `deploy/k8s-it/shared-scenario`); this fills the
local gap with the same service set, same image families, same canonical env-var
names and index names, just reachable at `127.0.0.1` instead of a cluster.

The Sutra engine itself is **not** started by this file — see
["Pointing a locally-run engine at this stack"](#pointing-a-locally-run-engine-at-this-stack)
below and the commented `engine` service at the bottom of `docker-compose.yml`.

## Quick start

```bash
docker compose -f deploy/compose/docker-compose.yml up -d
docker compose -f deploy/compose/docker-compose.yml ps      # wait for "healthy"
docker compose -f deploy/compose/docker-compose.yml down    # stop, keep data
docker compose -f deploy/compose/docker-compose.yml down -v # stop, wipe data
```

Every port is published to `127.0.0.1` only. This stack ships fixed dev-only
credentials (`sutra-dev-only`) — never expose it beyond localhost.

## Services and endpoints

| Service | Image | Host endpoint | Notes |
|---|---|---|---|
| `postgres` | `postgres:16-alpine` | `127.0.0.1:5432` | DB `sutra`, user `sutra`. The engine's own datasource (instance/outbox/lease/audit) — never a package's business store. |
| `rabbitmq` | `rabbitmq:3.13-management-alpine` | `127.0.0.1:5672` (AMQP), `127.0.0.1:15672` (mgmt UI) | Real service account, no `guest`. Host `rabbitmq` is what the shipped example `channels.yaml` files already expect. |
| `elasticsearch` | `docker.elastic.co/elasticsearch/elasticsearch:8.15.0` | `127.0.0.1:9200` | Single node, basic auth on (`elastic` / `$ES_PASSWORD`), TLS off. |
| `kibana` | `docker.elastic.co/kibana/kibana:8.15.0` | `127.0.0.1:5601` | Same version as Elasticsearch (ECK pins them together in prod). |
| `otel-collector` | `otel/opentelemetry-collector-contrib:0.114.0` | `127.0.0.1:4317` (gRPC), `127.0.0.1:4318` (HTTP), `127.0.0.1:13133` (health) | One OTLP receiver, ECS-mapped straight into Elasticsearch. |
| `fluent-bit` | `fluent/fluent-bit:3.1.9` | `127.0.0.1:24224` (Forward input), `127.0.0.1:2020` (monitoring) | Ships the *other* services' own container logs into `sutra-logs`. See "Log paths" below. |

Config files: `otel-collector-config.yaml` and `fluent-bit.conf`, both bind-mounted
read-only into their containers — edit and `docker compose restart <service>` to
apply.

## Environment variables

Override any of these before `up` (or drop them in a `deploy/compose/.env` file —
`docker compose` loads it automatically); all default to a fixed dev value.

| Variable | Default | Used for |
|---|---|---|
| `SUTRA_DB_PASSWORD` | `sutra-dev-only` | Postgres `POSTGRES_PASSWORD`. Same name the CLI's `sutra migrate` reads (`SUTRA_DB_URL`/`SUTRA_DB_USERNAME`/`SUTRA_DB_PASSWORD`, see `rust/crates/sutra-cli/src/commands/migrate.rs`) — export it once and a host-run `sutra migrate` agrees with the container. |
| `RABBITMQ_USERNAME` | `sutra` | RabbitMQ `RABBITMQ_DEFAULT_USER` — also the exact env name the engine and `channels.yaml` `${RABBITMQ_USERNAME}` references expect. |
| `RABBITMQ_PASSWORD` | `sutra-dev-only` | RabbitMQ `RABBITMQ_DEFAULT_PASS` — ditto, `${RABBITMQ_PASSWORD}`. |
| `ES_PASSWORD` | `sutra-dev-only` | Elasticsearch `ELASTIC_PASSWORD`, and the same value Kibana / the OTel Collector / Fluent Bit authenticate with. Matches the `ES_PASSWORD` name used throughout `deploy/modules/local-k8s-infra/observability.tf`. |

**Important — the engine's own canonical datasource env vars are a *different* name
than the CLI's.** The Rust engine (`rust/crates/sutra-engine/src/config.rs`) reads
`SUTRA_DATASOURCE_URL` / `SUTRA_DATASOURCE_USERNAME` / `SUTRA_DATASOURCE_PASSWORD` —
not `SUTRA_DB_*` (that's the CLI's `sutra migrate` contract only). Both point at the
same Postgres here; see the table below.

## Pointing a locally-run engine at this stack

Whether you run the engine with `cargo run -p sutra-engine`, its own binary, or a
built Docker image, export the canonical env the engine reads
(`rust/crates/sutra-engine/src/config.rs`, `rust/crates/sutra-engine/src/otel.rs`):

```bash
export SUTRA_HTTP_PORT=0                          # dynamic port — never hardcode 8080 locally
export SUTRA_DEPLOYMENTS_DIR=/path/to/deployments # a directory of sealed .sutra archives
export SUTRA_DATASOURCE_URL=postgresql://127.0.0.1:5432/sutra
export SUTRA_DATASOURCE_USERNAME=sutra
export SUTRA_DATASOURCE_PASSWORD=sutra-dev-only   # matches SUTRA_DB_PASSWORD above
export RABBITMQ_USERNAME=sutra
export RABBITMQ_PASSWORD=sutra-dev-only
export SUTRA_TELEMETRY_OTLP_ENDPOINT=http://127.0.0.1:4317
export SUTRA_TELEMETRY_SERVICE_NAME=sutra-engine
export OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta   # ES exporter drops cumulative histograms
```

If instead you run the engine as a *container in this same Compose project*,
uncomment the `engine` service at the bottom of `docker-compose.yml` — it already
uses the in-network hostnames (`postgres`, `rabbitmq`, `otel-collector`) and the
dynamic-port convention (`"127.0.0.1::8080"`, discover with
`docker compose -f deploy/compose/docker-compose.yml port engine 8080`), the exact
pattern `rust/crates/sutra-cli/assets/app/deploy/compose.yaml` (the `sutra create
app` scaffold) already uses for a single-package app.

## Log paths (why Fluent Bit doesn't tail the engine)

The engine's application logs never travel through Fluent Bit in this codebase —
they export over OTLP straight to `otel-collector` above (`sutra-app-logs` index),
exactly as `deploy/modules/local-k8s-infra/observability.tf` documents for the k8s
DaemonSet and as `rust/crates/sutra-engine/src/otel.rs` implements. Fluent Bit's job
here is the *other* thing the k8s DaemonSet also does: ship the backing services'
own container logs (`sutra-logs` index).

Rather than a filesystem tail of `/var/lib/docker/containers` (only predictable on
Linux hosts running the docker daemon directly, not inside the Docker Desktop VM),
each of `postgres` / `rabbitmq` / `elasticsearch` / `kibana` sets
`logging: driver: fluentd` pointed at Fluent Bit's `24224` Forward listener — the
docker **daemon** (a host-level process) ships those logs directly, no filesystem
path involved. This reuses the identical `log_forward_endpoint` / port-24224 pattern
`deploy/modules/efk-stack/main.tf` already exposes as an "optional forward listener"
for the same reason. Fluent Bit's own container deliberately does *not* opt into the
`fluentd` driver, so there's no self-referential logging loop to exclude (the k8s
config needs an `Exclude_Path` for exactly this; here there is simply nothing to
exclude).

## Differences from prod

This is a *reference* stack for local dev, not a byte-for-byte clone of the k8s
posture:

- **Elasticsearch TLS is off.** Prod's ECK operator auto-issues certs and terminates
  TLS; the compose Elasticsearch runs with `xpack.security.enabled: true` (same
  basic-auth `elastic` superuser) but `xpack.security.http.ssl.enabled: false`, to
  avoid needing a mounted CA locally. If you need TLS parity, flip that flag and
  mount a cert.
- **Kibana authenticates as `elastic`,** not a scoped `kibana_system` service
  account — simpler for a single-user dev box.
- **`otel-collector` / `fluent-bit` have no container healthcheck.** Both images ship
  no shell (`sh`, `curl`, `wget` are all absent — verified: `exec: "sh": executable
  file not found`), so a `CMD-SHELL` healthcheck isn't possible. Poll from the host
  instead: `curl http://127.0.0.1:13133/` (collector) or
  `curl http://127.0.0.1:2020/api/v1/health` (Fluent Bit).
- **No KEDA / MetalLB / ingress-nginx equivalent.** Those are Kubernetes-specific
  autoscaling/networking primitives with no compose analog — irrelevant to a
  single-engine local dev loop.

## Validation

Syntax-checked with `docker compose -f deploy/compose/docker-compose.yml config`
(exit 0, no warnings). `otel-collector-config.yaml` was validated against the pinned
`otel/opentelemetry-collector-contrib:0.114.0` binary's own `validate` subcommand
(exit 0). `fluent-bit.conf` was validated against the pinned
`fluent/fluent-bit:3.1.9` binary's own `--dry-run` (`configuration test is
successful`). `docker compose up` was **not** run as part of producing this stack.
