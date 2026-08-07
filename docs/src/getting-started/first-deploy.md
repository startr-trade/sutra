# Your first deployment

The [quickstart](quickstart.md) deployed by dropping an archive into a watched directory. That
is one of three ways in, and the right one for local work. This page covers the other two and
what they have in common — because "deployed" means something precise here.

## What a deploy actually is

The deploy unit is one sealed `.sutra` archive. `sutra package` runs the fail-closed validation
suite and derives the manifest — including the content-addressed `deploymentId = sha256(manifest)`
— from the package directory; nothing in the manifest is hand-authored.

Two consequences worth internalizing:

- **Deploying is idempotent.** Re-deploying identical bytes is a no-op, because the bytes *are*
  the identity. There is no "did that apply?" ambiguity.
- **A slot holds one active revision.** The slot is the package's stable
  `tenant--module--version`. A changed archive under the same slot replaces the active revision
  in one transaction — a hot-deploy, not a restart — while instances already running stay pinned
  to the revision they started on.

## Three ways in

| Path | Use it when | How |
|---|---|---|
| **Watched directory** | local dev, air-gapped hosts | write the `.sutra` into the directory the engine watches |
| **Synchronous API** | CI/CD, any engine you can reach | `sutra deploy … --api --engine-url …` |
| **ConfigMap patch** | Kubernetes | `sutra deploy …` (the default when a cluster context is set) |

All three end at the same place: the archive is stored, validated, and activated by the same
two-phase flip. They differ only in how the bytes arrive.

### The synchronous API

```bash
sutra deploy my-first-app-main.sutra --api --engine-url http://localhost:<port>
```

This is the deterministic path — the call returns only once the deployment is live, so what you
see on the wire is what happened:

1. The CLI uploads the archive to `POST /admin/deployments`.
2. The engine re-verifies it fail-closed — a corrupt or invalid archive is rejected here, before
   anything is stored.
3. The archive becomes the new active revision for its slot, in the engine's own datasource.
4. The engine runs its two-phase activation flip: drain the old revision if there is one,
   activate the new one.
5. The call returns **synchronously** — `200 {deploymentId, phase: "Active"}`, or a `4xx`
   carrying the `SUTRA.DEPLOY.*` diagnostic that explains the refusal.

There is no propagation window to wait out; the HTTP response *is* the "it's live" signal. For a
deployment large enough that the flip risks a long-held request (mainly behind an ingress with a
short read timeout), the same endpoint takes an async mode: `202 {deploymentId, status:
"Pending"}`, then poll `GET /sutra/deployments/{id}` until it reads `Active` or `Failed`.

In CI, that exit code is your gate — a failed validation fails the pipeline, not the release.

## What you get, and what you don't

A successful deploy leaves you with:

- **A live deployment.** `GET /sutra/deployments/{id}` reports `Active`.
- **Live channels.** Whatever `channels.yaml` declared is bound and serving — an HTTP channel
  accepts requests immediately; a broker channel's consumer is running (for a `singleton: true`
  channel, on whichever replica holds the per-channel lease).
- **Nothing else.** Deploying creates no infrastructure — no database, no broker, no ingress.
  Those are provisioned separately (locally by `docker compose`, in a cluster by the OpenTofu
  modules under `deploy/`). A deploy only registers and activates the package's processes,
  channels, and store bindings against infrastructure that already exists.

## Hot-deploy and rollback

Re-package under the **same** slot and deploy again. The archive gets a new content-addressed
id, the slot name does not, so the engine flips in-process and lets in-flight instances drain on
the old revision. Rollback is the identical operation with the previous archive — there is no
separate rollback mode to learn, and no "undo" that behaves differently from a deploy.

For the operator-facing detail — draining, retirement, migrating an instance off a pinned
revision — see [Deploy, hot-deploy, and rollback](../operating/deploy-rollback.md).

## Next

You have installed the tools, run a message through an engine, understood the package, and
deployed it for real. **[Building BPMN solutions](../building/concepts.md)** picks up from here
with the ideas and file formats behind what you just shipped.
