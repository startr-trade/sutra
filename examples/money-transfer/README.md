# money-transfer — an ACID ledger across three transports

A minimal Sutra app (packaged into the R13 standalone-package layout) that demonstrates a
**single writable ledger, made safe under concurrency**: a `TransferRequest` moves funds between
two accounts under Atomicity/Consistency/Isolation/Durability, driven from the SAME BPMN over
three different transports (HTTP, RabbitMQ, Kafka).

## What it shows

- **A module-owned SQL data store.**
  [`datastores.yaml`](deployments-src/default--money-transfer--1.0.0/datastores.yaml) declares
  `accounts` (the ledger: per-account `{balance, frozen}` JSONB, read/written by
  [`transfer.bpmn`](deployments-src/default--money-transfer--1.0.0/bpmn/transfer.bpmn) and
  `balance-query.bpmn`) with its own env-indirected connection and its own idempotent migrations
  under `migrations/accounts/`. It also declares `coverage` — where the path-coverage compliance
  marks are persisted. That one ships no SQL: the declaration chooses the *database* (here, the
  same connection as `accounts`), while the engine owns the coverage *schema* and applies it to
  that connection on first use.
- **Per-channel singleton serialization (M4).** The activation channel `transfer-request` (HTTP)
  and its siblings `transfer-queue` (RabbitMQ) / `transfer-topic` (Kafka) all drive the SAME
  `transfer.bpmn` with `singleton: true` — on a queue transport that is leader-gated across
  replicas (a per-channel DB-lease election); on HTTP the single-writer guarantee instead comes
  from the `<bpmn:transaction>` scope + `FOR UPDATE` row locks. The read-only `balance` channel and
  the `coverage-*` admin channels are NOT singletons — they scale across every replica.
- **A custom module schema.** `schemas/transfer/transfer.xsd` declares `TransferRequest` +
  `BalanceQuery`, registered as the path-derived codec `urn:transfer`.

## Run the scenario

```
cd rust && cargo test -p sutra-conformance -- --ignored tc_money_transfer_acid_ledger
```

Reproduces the retired `MoneyTransferIT`'s seven ordered steps against a real PostgreSQL ledger
(Testcontainers): durability + cross-instance read, insufficient-funds / frozen-account rejection,
atomicity rollback, isolation under concurrent transfers, then the coverage report/reset pair.

## Try it by hand

```
curl -sS -X POST localhost:8080/channels/transfer-request \
  -H 'Content-Type: application/json' -H 'X-Api-Key: transfer-demo-key' \
  -d '{"TransferRequest":{"FromAccount":"alice","ToAccount":"bob","Amount":"25.00"}}'
```

> Demo auth is `apikey`, matching the bundled IT.

## Deploy & hot-deploy

This example is one standalone deployment package —
[`deployments-src/default--money-transfer--1.0.0/`](deployments-src/default--money-transfer--1.0.0/)
is already in the sealed-archive shape (the canonical layout is described in the book's
*Deployment packages* chapter). It ships no bundled
`deploy/` assets of its own, so the recipes below use the generic patterns.

**Seal it** (from the repo root):

```
(cd rust && cargo build -p sutra-cli --release)   # once
rust/target/release/sutra package examples/money-transfer/deployments-src/default--money-transfer--1.0.0 \
    --out /tmp/pkgs
```

**Local (compose).** Run any `sutra-engine` image with `SUTRA_DEPLOYMENTS_DIR` pointing at a
directory it watches, then drop the sealed archive in — the watcher treats the mount as the
source of truth: **adding the `.sutra` deploys it, removing it undeploys it**, hot-reloaded with
no restart. See the scaffolded app's
[`deploy/compose.yaml`](../../rust/crates/sutra-cli/assets/app/deploy/compose.yaml) +
[`deploy/deployments/README.md`](../../rust/crates/sutra-cli/assets/app/deploy/deployments/README.md)
for the reference compose:

```
cp /tmp/pkgs/default--money-transfer--1.0.0.sutra <your-app>/deploy/deployments/
```

**Kubernetes (the shared hot-deploy instance, R14).** ONE shared engine instance backs every
bundled example; `sutra deploy` patches the `sutra-deployments` ConfigMap that instance watches
(no per-example manifests, no engine restart):

```
make -C deploy/k8s-it init shared-apply   # one-time per environment
KUBECONFIG=$(tofu -chdir=deploy/k8s-it/cluster output -raw kubeconfig_path) \
    rust/target/release/sutra deploy /tmp/pkgs/default--money-transfer--1.0.0.sutra \
    --wait --engine-url "http://$INGRESS"
```

> `accounts`' `env:ACCOUNTS_DB_URL/USER/PASSWORD` refs resolve from the engine process
> environment, not from `sutra deploy --secret` (that merges the *estate Secret*, which
> `secret:KEY` refs resolve) — wiring a real Postgres into the shared instance's pod env is a
> tofu/estate concern. The bundled conformance IT supplies them via Testcontainers instead.

Then exercise it through the Ingress:

```
INGRESS=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl -X POST -H 'Content-Type: application/json' -H 'X-Api-Key: transfer-demo-key' \
    -d '{"TransferRequest":{"FromAccount":"alice","ToAccount":"bob","Amount":"25.00"}}' \
    "http://$INGRESS/channels/transfer-request"
```

**Hot-deploy.** Edit a rule/template/BPMN, re-package under the SAME archive file name, and deploy
again — `deploymentId` is the content hash of the archive (a changed archive mints a new id) while
the slot (the ConfigMap key / dropped file name) stays the same, so the engine's watcher flips to
the new version **between requests, no restart**, draining in-flight instances on the old version.

**Rollback.** Re-deploy the previous `.sutra` archive under the same slot name — its still-draining
deploymentId resurrects instead of being rebuilt:

```
rust/target/release/sutra undeploy default--money-transfer--1.0.0.sutra
```
