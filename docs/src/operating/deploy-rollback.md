# Deploy, hot-deploy, and rollback

The mechanics behind this page are covered in full in
[Deployment model](../architecture/deployment-model.md); this is the operator-facing walkthrough
of the same three operations.

## Deploy

```bash
sutra deploy my-app.sutra --api --engine-url http://localhost:<port>
```

`sutra deploy` has two paths: the **API** path (`--api`, against an engine running the `db`
deployment source) is synchronous — it returns only once the deployment is `Active`, or fails fast
with the `SUTRA.DEPLOY.*` reject diagnostic. The **ConfigMap** path (the default, for a Kubernetes
`dir` deployment source) patches the deployments ConfigMap and is asynchronous — the engine's own
watcher picks up the change on its next poll; add `--wait` to have the CLI poll
`GET /sutra/deployments/{id}` until it reports `Active`.

For a large deployment where a synchronous call risks a long-held request behind an ingress,
`--async` (API path) submits and returns `202 {deploymentId, Pending}` immediately, then the CLI
polls to completion itself.

## Hot-deploy

A hot-deploy is just a normal deploy call against an existing **slot** (the archive's stable
`tenant--module--version` key): re-package the same source directory, and re-deploy. The new
archive gets a new content-addressed `deploymentId`, but the slot name is unchanged, so the engine
replaces the slot's active revision in one transaction and runs its two-phase activation flip —
drain the old revision, activate the new one — with no restart. In-flight instances on the old
revision keep running to completion in the background; new inbound picks up the new revision
immediately.

```bash
# edit the package source, then:
sutra package packages/my-app --out /tmp/pkgs
sutra deploy /tmp/pkgs/my-app.sutra --api --engine-url http://localhost:<port>
```

For the edit-save-redeploy loop while developing, skip the manual re-package step entirely:

```bash
sutra deploy --watch packages/my-app --engine-url http://localhost:<port>
```

Each save is re-packaged and **validated first** — the same static validation `sutra lint` runs.
Deployment only fires if validation passes; on a finding, the CLI reports it and skips the deploy,
so a broken edit never reaches the engine.

## Rollback

Rollback is the identical operation in reverse: re-package (or simply keep) the **previous**
archive for the same slot, and re-deploy it. Its still-draining `deploymentId` resurrects rather
than being rebuilt from scratch:

```bash
sutra deploy /tmp/pkgs/my-app-previous.sutra --api --engine-url http://localhost:<port>
```

## Removing a deployment

```bash
sutra undeploy my-app.sutra --api --engine-url http://localhost:<port>
```

The engine drains it — refuses new intake, lets in-flight instances finish, retires the slot once
it reaches zero instances and zero pending outbox entries. On the ConfigMap path, pair this with
`sutra deployments list <dir>` to find the right archive/deploymentId first.

Two drain behaviors worth knowing before you need them:

- **Inbound routes always follow the *active* set.** A slot whose only revisions are draining
  serves no inbound routes at all (`SUTRA.RESOLVE.CHANNEL.UNKNOWN` on its paths); its parked
  instances still resume through relay correlation the moment you deploy a new active revision
  into the slot — which is also the recovery move when an undeploy left work parked.
- **Accumulated drains are safe.** Several draining revisions of one slot are a legal store
  state (interrupted drains accumulate across restarts); boot registers each channel key once,
  newest draining revision first, and instances pinned to *any* of the revisions remain
  resumable. No cleanup is required before a restart.

## Checking what's live

```bash
sutra deployments list <dir> [--label KEY=VALUE]...   # ConfigMap/dir source: what's on disk
```

Against a running engine, `GET /sutra/deployments/{id}` is the authoritative live status —
`Active`, `Pending`, `Draining`, or `Failed` with a reason.

## Next

- **[Deployment model](../architecture/deployment-model.md)** — why this is deterministic rather
  than eventually consistent.
- **[Instance migration](instance-migration.md)** — the sanctioned way to move an instance off the
  pin it is stuck on, so a draining deployment can finally retire.
- **[Reference: the `sutra` CLI](../reference/cli.md)** — every flag on `deploy`/`undeploy` in
  full.
