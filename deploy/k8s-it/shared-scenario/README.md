# Shared-instance k8s harness (R14)

ONE engine Deployment for every example; packages hot-deploy onto it via `sutra deploy`
(ConfigMap patch — no engine management endpoint, no restart). Replaces the retired
per-example scenario configs (the retired per-example `k8s-it/scenario/` and
`deploy/tofu/` folders). `sutra deploy` talks to the cluster with a native Kubernetes
client, not a `kubectl` shell-out.

## Operator flow

```bash
# 0. One-time per environment: cluster + infra (UNCHANGED — never rebuilt by ITs)
cd deploy/k8s-it && make init

# 1. Build + push the Rust engine image to the kind-local registry. Use the FAST `release-it`
#    profile for ITs (no LTO / parallel codegen — ~2x faster; a few MB larger, fine for ITs):
make image-it            # = docker build --build-arg CARGO_PROFILE=release-it ... + push
#    (The SHIPPED release image keeps the size-optimised default profile:
#     docker build -t localhost:5000/sutra-engine:k8s-it -f rust/Dockerfile rust/ )

# 2. Provision the shared instance (engine + postgres + rabbitmq + empty
#    sutra-deployments ConfigMap + sutra-secrets estate Secret + one Ingress)
cd deploy/k8s-it/shared-scenario
tofu init
tofu apply -var "kubeconfig_path=$(tofu -chdir=../cluster output -raw kubeconfig_path)"

# 3. Build the CLI, package an example, hot-deploy it
cargo build -p sutra-cli --release   # from rust/
# money-transfer is a single-variant example: deployments-src/<dir> is already a complete
# package dir, so it is packaged directly. A MULTI-variant example (DRY variants) instead
# overlays shared/ + variants/<name>/ into a standalone dir BEFORE packaging (build-time only —
# the same convention the Rust conformance harness's
# `sutra_testkit::conformance::compose::compose_variant` uses).
V=default--money-transfer--1.0.0
rust/target/release/sutra package examples/money-transfer/deployments-src/$V --out /tmp/pkgs
rust/target/release/sutra deploy /tmp/pkgs/$V.sutra
# `--secret KEY=value` merges estate Secret keys FIRST, ConfigMap second, when a package needs them.

# 4. Exercise it through the Ingress (ingress-nginx controller LB IP, port 80)
INGRESS=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -X POST "http://$INGRESS/channels/balance" \
     -H 'Content-Type: application/json' -H 'X-Api-Key: transfer-demo-key' \
     -d '{"BalanceQuery":{"accountId":"alice"}}'

# 5. Remove it (the engine drains: no new intake, in-flight work finishes)
rust/target/release/sutra undeploy fednow-http--fednow-pacs08--1.0.0.sutra
```

## Ownership split

| Object | tofu owns | CLI owns |
|---|---|---|
| `sutra-deployments` ConfigMap | the object | every `binary_data` entry (`lifecycle.ignore_changes`) |
| `sutra-secrets` Secret | the object + seed keys | merged keys (`lifecycle.ignore_changes`) |
| engine / postgres / rabbitmq / Ingress | everything | — |

## Notes

- **Queues**: per-deployment queue topology is NOT provisioned here. Declare queues
  (mgmt API / AMQP) *before* `sutra deploy` — the engine's broker triggers declare
  passively on the activation flip. The k8s ITs do this themselves.
- **Broker host aliases**: `rabbitmq-mtmx` and `rabbit` are ClusterIP aliases of the one
  broker so every package's `channels.yaml` host resolves unchanged — including the packages
  belonging to the repositories that compose this one as a submodule and drive this same
  harness.
- **~1 MiB ConfigMap ceiling** (R14 posture note): accepted for the examples;
  `sutra deploy` refuses a deploy that would cross it. Real estates split across
  ConfigMaps/instances.
- **Rotation**: estate Secret values live-sync into the pod (tmpfs); `secret:KEY` refs
  re-resolve on the next flip — value rotation applies on next flip, documented posture.
- The engine env pre-provisions the conventional `${ENV}` refs the packages use
  (`RABBITMQ_*`, `ACCOUNTS_DB_*`, and the rail-side `FEDNOW_CALLBACK_HOST`, `MX_DEST_HOST`,
  `MT_CLIENT_HOST`); the recorder-endpoint vars are apply-time inputs (pod env is
  immutable), so each IT class passes its own on its single shared-scenario apply. A suite
  that needs none of them simply omits the `-var`s and takes the placeholder defaults.
- Kafka is not provisioned. The money-transfer package declares a `transfer-topic` kafka
  channel, which therefore stays down — fail-closed per channel, exactly as in tier-2; its
  HTTP channels (which is what the k8s suites drive) serve regardless. Extend here if a
  package ever needs a kafka round trip on the k8s tier.
