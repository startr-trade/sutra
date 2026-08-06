# Tier-3 Kubernetes IT — the shared local-k8s harness

Operator-owned cluster plumbing for the tier-3 conformance suites — `k8s_money_transfer`
and `k8s_observability` in this repo's `sutra-conformance`, plus the rail suites in the
repository that composes this one as a submodule. ONE kind cluster and ONE EFK/OTel stack
serve **all** of them, which is why this harness lives at the repo root and belongs to no
example.

Three independently-applyable OpenTofu roots live here. The per-example **scenario stage
was RETIRED with R14** — every per-example scenario config is replaced by the single
[`shared-scenario/`](shared-scenario/) instance, onto which the suites hot-deploy their
packages via `sutra deploy` (a k8s-API ConfigMap patch — no per-example tofu, no engine
restart).

| Folder | Lifecycle | What it provisions |
|---|---|---|
| [`cluster/`](cluster/) | **Operator-driven** — `make init` once per local environment | kind cluster (1 cp + 2 workers) + local Docker registry. Provider-light (kind/docker/null only) via the shared [`deploy/modules/local-k8s-cluster`](../modules/local-k8s-cluster/) module. Writes the kubeconfig the other two stages and the conformance harness read. |
| [`infra/`](infra/) | **Operator-driven** — `make init` once per local environment | MetalLB, ingress-nginx, KEDA, EFK (Elasticsearch + Kibana + Fluent Bit) + the OTel observability pipeline (Collector, LoadBalancers, Kibana data views). Via the shared [`deploy/modules/local-k8s-infra`](../modules/local-k8s-infra/) module. |
| [`shared-scenario/`](shared-scenario/) | **Operator- or IT-driven** — applied ONCE, re-applied idempotently by the suites | The R14 shared instance: ONE engine Deployment + postgres + rabbitmq + the empty `sutra-deployments` ConfigMap + the `sutra-secrets` estate Secret + ONE Ingress. Stays up across suites; packages come and go via `sutra deploy`/`undeploy`. |

Per user direction 2026-05-24: cluster init/destroy is operator-driven; the ITs never
re-create or destroy the cluster.

## Operator workflow

```bash
cd deploy/k8s-it
make init       # ~5-10 min — kind cluster + production-realistic deps. ONE TIME per env.
make ready      # show node + prod-dep status

# R14 prereqs (one-time per env):
docker build -t localhost:5000/sutra-engine:k8s-it -f rust/Dockerfile rust/   # from the repo root
docker push localhost:5000/sutra-engine:k8s-it
(cd rust && cargo build -p sutra-cli --release)

make shared-apply    # provision the shared engine instance (or let the first IT do it)
(cd rust && cargo test -p sutra-conformance -- --ignored --test-threads=1 k8s_)  # hot-deploy/undeploy suites
make destroy         # tear cluster + prod deps down when finished
```

`make -C deploy/k8s-it <target>` works from anywhere in the repo.

## The kubeconfig filename

`cluster/sutra-fednow-it-config` is **generated**: the `tehcyx/kind` provider writes
`<cluster_name>-config`, and the cluster has been named `sutra-fednow-it` since it was
created. The name is historical, not a coupling to the FedNow example — the cluster is
shared by every suite. Renaming it would mean recreating the cluster, so only its
directory moved. The harness default is `deploy/k8s-it/cluster/sutra-fednow-it-config`
(`sutra_testkit::conformance::k8s::kubeconfig_path`), overridable via `SUTRA_KUBECONFIG`.

## What a tier-3 suite does

1. Verifies the cluster is reachable (never applies `cluster/` itself beyond `tofu init`).
2. Applies `shared-scenario/` idempotently (its recorder-endpoint vars are the only
   per-suite delta), then waits for the Deployment rollout to converge.
3. Declares its broker queues (per-deployment topology is IT-owned on the shared instance).
4. `sutra package` → `sutra deploy --api` its `.sutra` archives (estate Secret patched
   FIRST, deployments ConfigMap second — kubelet syncs the volume, the engine's watcher
   flips).
5. Asserts through the **Ingress** (ingress-nginx controller LB, port 80) — the engine has
   no per-example NodePort/LB anymore.
6. `sutra undeploy` in teardown — the shared instance stays up for the next suite.

## Why cluster and infra stay split

1. **Speed / lifecycle.** EFK + KEDA Helm rollouts take minutes; the cluster + infra live
   across IT runs (operator-managed lifetime) while packages hot-deploy in seconds.
2. **No provider cycle.** A root that BOTH creates the cluster AND applies in-cluster
   resources must configure its kubernetes/helm/kubectl providers from that cluster's own
   not-yet-created outputs. `cluster/` stays provider-light; `infra/` + the shared scenario
   configure their providers from the kubeconfig FILE the cluster stage wrote.

## Production-realistic deps

The `infra/` stage ships the dep set a real Sutra deployment would expect:

- **MetalLB** — LoadBalancer allocator (the broker LB + the ingress-nginx controller LB).
- **ingress-nginx** — Ingress controller; the shared instance's ONE Ingress rides it, and
  the suites reach the engine through it.
- **KEDA** — Event-driven autoscaler. `deploy/modules/sutra` ships a ScaledObject; without
  KEDA installed that resource never reconciles.
- **EFK** — Elasticsearch + Kibana via ECK + Fluent Bit DaemonSet (in-cluster, per user
  direction 2026-05-24).

Each is gated behind a tofu variable (`install_metallb`, `install_ingress_nginx`,
`install_keda`, `install_efk`); set false to opt out on resource-constrained runners.

## Test tiers

See [`rust/TESTING.md`](../../rust/TESTING.md). Tier-3 is the slowest tier and catches
what the single-container tier-2 suites cannot see: the R14 hot-deploy path (ConfigMap
patch → kubelet sync → watcher flip), Secret mounts, probe shapes, multi-node scheduling,
Ingress exposure, image pull through a registry, and the full OTLP pipeline.
