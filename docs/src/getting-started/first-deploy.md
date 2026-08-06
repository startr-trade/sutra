# Your first deployment

`sutra deploy` has two paths onto a running engine: a **ConfigMap patch** (the default, for a
Kubernetes deployment source) and the **synchronous API** (`--api`, against an engine running the
`db` deployment source, or any engine you can reach directly). This page walks the API path,
because it is the deterministic one — the call returns only once the deployment is `Active`, so
what you see on the wire matches what actually happened.

## What "deploy" means

The deploy unit is one sealed `.sutra` archive. `sutra package` runs the full fail-closed
validation suite and derives the archive's manifest — including its content-addressed
`deploymentId = sha256(manifest)` — from the package directory; nothing in the manifest is
hand-authored. Deploying that archive is idempotent: re-deploying identical bytes is a no-op,
and a changed archive under the same **slot** (its stable `tenant--module--version` key) replaces
the slot's active revision in one transaction — a hot-deploy, not a restart.

## Deploy over the API

Point the CLI at a reachable engine and hand it a sealed archive:

```bash
sutra deploy my-first-app-main.sutra --api --engine-url http://localhost:<port>
```

What happens, in order:

1. The CLI uploads the archive bytes to `POST /admin/deployments`.
2. The engine re-verifies the archive fail-closed (`sutra_loader::read_archive`) — a corrupt or
   invalid archive is rejected here, before anything is stored.
3. The engine stores the archive as the new active revision for its slot, in its own datasource.
4. The engine runs its in-process two-phase activation flip (drain the old revision if one
   exists, activate the new one).
5. The call returns **synchronously**: `200 {deploymentId, phase: "Active"}` on success, or a
   `4xx` carrying the `SUTRA.DEPLOY.*` reject diagnostic on failure.

There is no propagation window to wait out — the HTTP response *is* the "it's live" signal. For
a deployment large enough that the engine's own plan-and-flip work risks a long-held request
(mainly a concern behind a Kubernetes ingress with a short `proxy-read-timeout`), the same
endpoint accepts an async mode: it returns `202 {deploymentId, status: "Pending"}` immediately and
you poll `GET /sutra/deployments/{id}` (or request a completion webhook/broker notification) until
it flips to `Active` or `Failed`. Small local deploys — everything in this book's examples — use
the synchronous form.

## What you see

A successful deploy leaves you with:

- **A running deployment.** `GET /sutra/deployments/{id}` reports `Active`, the same status
  `sutra deploy --wait` polls on the ConfigMap path.
- **A live channel.** Whatever channels the package declared in `channels.yaml` are now bound and
  serving — an HTTP channel accepts requests immediately; a broker channel's consumer is running
  (or, for a `singleton: true` channel, running on whichever replica currently holds the
  per-channel lease).
- **Nothing extra.** Deploying does not create infrastructure — no database, no broker, no
  ingress. Those are provisioned separately (locally via `docker compose`, in a cluster via the
  OpenTofu modules under `deploy/`); the deploy call only ever registers and activates the
  package's processes, channels, and data-store bindings against infrastructure that already
  exists. See [Deployment model](../architecture/deployment-model.md) for the full model,
  including how a fleet of replicas converges on the same active set.

## Hot-deploy and rollback

Edit a package's source, re-package under the **same** slot, and re-deploy — the archive gets a
new content-addressed `deploymentId`, but the slot name doesn't change, so the flip happens
in-process with in-flight instances on the old revision left to drain. Rolling back is the
identical operation in reverse: re-deploy the previous archive under the same slot. See
[Deploy, hot-deploy, and rollback](../operating/deploy-rollback.md) for the operator-facing detail.

## Next

You've scaffolded an app, packaged it, and deployed it. **[Building BPMN
solutions](../building/concepts.md)** picks up from here — the ideas and file formats behind what
you just deployed.
