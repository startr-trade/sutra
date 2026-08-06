# Deployment — OpenTofu modules

OpenTofu (open-source Terraform fork) modules for deploying sutra to Kubernetes. The EFK reference observability stack is a separate, optional module — production environments often share a corporate EFK or use a managed service, in which case only the `sutra` module is deployed and external endpoints are passed in.

> **Use `tofu`, not `terraform`.** OpenTofu is the [Linux Foundation fork](https://opentofu.org) of Terraform 1.5; HCL syntax and state format are compatible up to that point.

## Modules

| Module | Purpose |
|---|---|
| [`modules/sutra/`](modules/sutra/) | Engine Deployment, Service, ServiceAccount, RBAC (Lease watch for timer leader election), PDB, NetworkPolicy, KEDA ScaledObject. Health probes, drain hook, distroless pod-security, topology spread. Optional pre-deploy migration Job using the `sutra-migrate` image (single `sutra_schema_history` table, matches the engine's in-process startup migration). |
| [`modules/efk-stack/`](modules/efk-stack/) | Optional. ECK operator → Elasticsearch + Kibana, OTel Collector (OTLP → ES exporter), Fluent Bit DaemonSet (stdout + audit-JSONL tailing + optional forward listener). The Fluent Bit parser-filter decodes the inner JSON payload from the Docker log envelope so `level` / `loggerName` / `service.name` become first-class fields in ES. ILM policies for logs/metrics/traces/audit. |

## The tier-3 IT harness

[`k8s-it/`](k8s-it/) is the operator-owned local test environment the tier-3 conformance
suites run against: `cluster/` (kind + local registry), `infra/` (MetalLB, ingress-nginx,
KEDA, EFK + the OTel pipeline) and `shared-scenario/` (the ONE engine instance every suite
hot-deploys onto). It lives here, not under an example, because a single cluster serves every
suite. Lifecycle is `make -C deploy/k8s-it {init,ready,shared-apply,destroy}` — see
[`k8s-it/README.md`](k8s-it/README.md). Never applied or destroyed by the ITs themselves.

## Application-scoped deployments live with the application

Each Sutra application owns its own `deploy/` folder that wires these modules
with app-specific values (image SHA, channel YAML mounts, OIDC issuer,
observability endpoints). Per-application deploys live with the application, under
`examples/<app>/deploy/`, in the two shapes these modules compose into:

| Pattern | What it deploys |
|---|---|
| `all-in-one` | Engine + EFK in the same cluster. Dev / staging / small prod. |
| `external-efk` | Engine here, EFK elsewhere (corporate / managed SaaS). |

There is no repo-root `deploy/examples/` folder — those previously-generic templates moved into
each application's own `deploy/` to keep deployment configuration co-located with the application
it deploys. (The worked instances of both patterns travelled with the FedNow application when the
payment-rail material moved to its own repository; the modules here are what they were built from,
and are unchanged.)

## Observability — three independent endpoints

The engine receives three optional endpoints; provide what your EFK exposes:

| Input | What the engine does with it | When to set |
|---|---|---|
| `otlp_endpoint` (required) | OTLP gRPC/HTTP — engine ships traces + metrics | Always |
| `log_forward_endpoint` (optional) | Fluent Bit forward protocol — engine ships JSON logs over the wire | When you don't want to rely on cluster-wide stdout log shippers |
| `elasticsearch_endpoint` (optional) | Direct ES URL — engine's audit fan-out shipper writes JSONL audit lines to ES | When you want the engine itself to manage audit ingest rather than tailing files |

If you only set `otlp_endpoint`, the engine writes JSON logs to stdout and your cluster's existing logging infra (DaemonSet Fluent Bit, Vector, etc.) picks them up. This is the recommended setup in clusters that already standardise on a log shipper — no engine-side log forwarding configuration needed.

## State backend

The examples default to local state for clarity. For team use, configure a remote backend:

```hcl
terraform {
  backend "s3" {
    bucket = "sutra-tofu-state"
    key    = "sutra/prod.tfstate"
    region = "ap-southeast-1"
    dynamodb_table = "sutra-tofu-locks"
    encrypt = true
  }
}
```

OpenTofu also supports `kubernetes`, `gcs`, `azurerm`, `consul`, and HTTP backends.

## Relationship to sutra-cli

When `sutra create app` scaffolds an application repository it includes a `deploy/` directory mirroring this structure, with the application's specific defaults baked in. The engine repo's `deploy/` here is the reference; application repos copy and adapt.

## Local dev — not OpenTofu

For local dev, run the engine against a scaffolded application's own `deploy/compose.yaml` (produced by `sutra create app`) rather than OpenTofu against a real cluster.

For the full backing-services set instead of testcontainers (Postgres + RabbitMQ + EFK + an OTel Collector, mirroring this directory's k8s service topology), see [`compose/`](compose/) — a `docker compose up` reference stack for 15-factor Factor 10 dev/prod parity. The engine itself is not started by it; see `compose/README.md`.

## Files

| Path | Purpose |
|---|---|
| `versions.tf` | OpenTofu + provider version pins |
| `modules/sutra/{variables,main,outputs}.tf` | Engine module |
| `modules/efk-stack/{variables,main,outputs}.tf` | EFK reference stack module |
| `examples/all-in-one/main.tf` | Engine + EFK in one cluster |
| `examples/external-efk/main.tf` | Engine pointing at external EFK |
| `k8s-it/{cluster,infra,shared-scenario}/` | The shared tier-3 IT environment (see above) |
| `compose/docker-compose.yml` | Local `docker compose` reference stack (Postgres + RabbitMQ + EFK + OTel Collector) — not OpenTofu, see above |
