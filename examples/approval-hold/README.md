# approval-hold — the wait-state relay, end to end

A minimal Sutra app (scaffolded with `sutra create app`, then given a custom XSD + a wait-state
process) that demonstrates the **human-in-the-loop relay**: a payment request **parks** at a
`userTask` until an out-of-band **decision** arrives on a separate channel and **resumes** it — all
correlated by a business key held in the durable, replica-coherent alias index (PostgreSQL).

**Zero application code.** Every flow in this example is pure engine + declarative
resources: BPMN, FEEL data assignments, Handlebars/XSLT templates, Handlebars scripts, a DMN decision
table, channels YAML and an XSD codec. Nothing is compiled or deployed alongside the engine.

## What it shows

- **Custom module schema.** [`schemas/approval/approval.xsd`](deployments-src/default--approval--1.0.0/schemas/approval/approval.xsd)
  declares two root elements (`ApprovalRequest`, `ApprovalDecision`) = two message types. It registers
  as the version-scoped `StructuralCodec` `urn:sutra:module:approval:1.0.0:approval` and validates
  JSON (or XML/YAML) on the wire.
- **Wait-state process.** [`approval-hold.bpmn`](deployments-src/default--approval--1.0.0/bpmn/approval-hold.bpmn):
  `startEvent` (channel `approval-request`) records the correlation alias `e2eId = payload.E2EId`
  (`unique`, `onConflict=correlate`), then a `userTask` (channel `approval-decision`, the **hold**),
  then an `endEvent`.
- **Channel-delivered relay.** An `ApprovalDecision` on `approval-decision` — a channel **no start
  event subscribes to** — is routed by the dispatcher's relay path: same two-tier intake against the
  wait node's `<q:source>`, then correlate by the process's `<q:alias>` via `AliasStore.findLive`, then
  resume the parked instance.

## Declarative showcase flows (no application code)

Alongside the wait-state relay, the module ships five synchronous showcase flows, each on its own
HTTP channel, each replying natively from a template render:

- **[`template-showcase.bpmn`](deployments-src/default--approval--1.0.0/bpmn/template-showcase.bpmn)** — a
  FEEL data-assignment node derives an engine choice, a visible exclusive gateway routes on it, and
  each branch is a template service task (`transform.xsl` / `greeting.hbs`, engine chosen by file
  extension) carrying `<q:reply mode="native">`.
- **[`script-showcase.bpmn`](deployments-src/default--approval--1.0.0/bpmn/script-showcase.bpmn)** —
  two Handlebars `<bpmn:scriptTask>`s render `scripts/` files whose JSON output merges (typed) into
  the instance variables; a Handlebars template echoes them (`autoApprove` stays a boolean).
- **[`subprocess-showcase.bpmn`](deployments-src/default--approval--1.0.0/bpmn/subprocess-showcase.bpmn)**
  — an embedded `<bpmn:subProcess>` expands inline sharing the parent scope; inside it a FEEL
  assignment sets `riskScore` and a `<bpmn:businessRuleTask>` evaluates the DMN decision table
  [`rules/approval-decide.dmn`](deployments-src/default--approval--1.0.0/rules/approval-decide.dmn).
- **[`throw-showcase.bpmn`](deployments-src/default--approval--1.0.0/bpmn/throw-showcase.bpmn)** —
  link throw/catch (intra-process goto), a non-interrupting escalation boundary + handler, and a
  no-subscriber signal throw — all emit-and-continue, evidenced by FEEL-assigned variables in the
  reply.
- **[`datamapping-showcase.bpmn`](deployments-src/default--approval--1.0.0/bpmn/datamapping-showcase.bpmn)**
  — the declarative data-mapping layer: visible FEEL data-assignment nodes (`if/then/else`) plus a scoped
  `<q:param>` feeding the reply template a per-invocation derived input.

## Run the scenario

```
cd rust && cargo test -p sutra-conformance -- --ignored tc_approval_hold   # real PostgreSQL via testcontainers
```

`ApprovalHoldIT` drives one ordered flow and asserts the whole lifecycle:

1. **park** — `ApprovalRequest` → `200` (accept, no business reply), instance parked, alias recorded.
2. **duplicate rejected** — a second `ApprovalRequest` with the same `E2EId` while parked →
   `SUTRA.INBOUND.ALIAS_CONFLICT_REJECT` (the durable unique-alias correlate guard).
3. **relay resume** — `ApprovalDecision` on `approval-decision` → `200`; the parked instance is
   correlated by `E2EId`, resumed, and completed.
4. **retire** — the same `E2EId` is accepted again, proving the alias was retired at completion.
5. **uncorrelated relay is safe** — a decision for an unknown `E2EId` →
   `SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND`; parked instances are untouched (the wait is the safe
   state).

## Try it by hand

```
# park
curl -sS -X POST localhost:8080/channels/approval-request \
  -H 'Content-Type: application/json' -H 'X-Api-Key: approval-demo-key' \
  -d '{"ApprovalRequest":{"E2EId":"E2E-1","Amount":"1500.00"}}'

# resume it with a decision
curl -sS -X POST localhost:8080/channels/approval-decision \
  -H 'Content-Type: application/json' -H 'X-Api-Key: approval-demo-key' \
  -d '{"ApprovalDecision":{"E2EId":"E2E-1","Decision":"APPROVE"}}'
```

> Demo auth is `apikey` so the bundled IT can authenticate without a certificate. Real deployments
> should use mTLS or JWT instead.

## Deploy & hot-deploy

[`deployments-src/default--approval--1.0.0/`](deployments-src/default--approval--1.0.0/) is this
example's one standalone deployment package — the authoring unit `sutra package` seals; the layout is
described in the book's *Deployment packages* chapter.

**Local (compose).** Seal it, run any `sutra-engine` image with `SUTRA_DEPLOYMENTS_DIR` pointing at
a watched directory, then drop the archive in — adding the `.sutra` deploys it, removing it
undeploys it, hot-reloaded with no restart:

```
rust/target/release/sutra package examples/approval-hold/deployments-src/default--approval--1.0.0 \
    --out /tmp/pkgs
cp /tmp/pkgs/default--approval--1.0.0.sutra <your-app>/deploy/deployments/
```

See the scaffolded app's
[`deploy/compose.yaml`](../../rust/crates/sutra-cli/assets/app/deploy/compose.yaml) +
[`deploy/deployments/README.md`](../../rust/crates/sutra-cli/assets/app/deploy/deployments/README.md)
for the reference compose.

**Kubernetes** — hot-deploy this package onto the shared instance via `sutra deploy` (a ConfigMap
patch, no per-example manifests, no engine restart): see [`deploy/README.md`](deploy/README.md)
for the full recipe, including `--wait --engine-url <URL>` to block until the engine reports the
deployment `Active` (polls `GET /sutra/deployments/{id}`).

**Hot-deploy & rollback.** Re-package under the SAME archive file name and re-deploy:
`deploymentId` is the content hash of the archive (a changed archive mints a new id; the file
name/ConfigMap key is the stable slot), so the engine's watcher flips to the new version between
requests — no restart, in-flight instances on the old version drain in the background. Rollback =
re-deploy the previous archive under the same slot name; its still-draining deploymentId
resurrects instead of being rebuilt.
