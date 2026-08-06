# Deployment — approval-hold

The per-example tofu module that used to live here (**`tofu/`**) is **retired**:
examples no longer carry their own
Kubernetes deployment — ONE shared engine instance is provisioned by
[`deploy/k8s-it/shared-scenario/`](../../../deploy/k8s-it/shared-scenario/) and packages
hot-deploy onto it via `sutra deploy` (a k8s-API ConfigMap patch; the engine's watcher runs
the two-phase flip — no pod restart, no per-example manifests).

## Deploy this example to the shared instance

```bash
# 0. One-time per environment: cluster + infra + shared instance
make -C deploy/k8s-it init shared-apply
(cd rust && cargo build -p sutra-cli --release)

# 1. Package the standalone deployment directory (the authoring unit)
rust/target/release/sutra package \
    examples/approval-hold/deployments-src/default--approval--1.0.0 --out /tmp/pkgs

# 2. Hot-deploy (estate Secret keys merge FIRST, ConfigMap second)
KUBECONFIG=$(tofu -chdir=deploy/k8s-it/cluster output -raw kubeconfig_path) \
    rust/target/release/sutra deploy /tmp/pkgs/default--approval--1.0.0.sutra

# 3. Hit the channel through the shared Ingress
INGRESS=$(kubectl -n ingress-nginx get svc ingress-nginx-controller \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -X POST -H 'Content-Type: application/xml' -H 'X-Api-Key: approval-demo-key' \
    -d @request.xml "http://$INGRESS/channels/approval-request"

# 4. Remove it (the engine drains, then retires it)
rust/target/release/sutra undeploy default--approval--1.0.0.sutra
```

For a production-grade estate (RBAC, PDB, NetworkPolicy, KEDA, migrate-Job) graduate to the
full engine module under the Sutra source tree's `deploy/modules/sutra/`.
