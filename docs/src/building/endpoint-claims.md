# One endpoint, one deployment

Several deployments can share one engine, but most inbound endpoints hand each message to a single
receiver. If two deployments claimed one, the second would either never receive anything or silently
compete with the first for messages. So since 0.6.18 the engine refuses the second claim at deploy,
with the first deployment still serving.

| Endpoint | Who can hold it |
|---|---|
| HTTP `METHOD path` | one deployment |
| Queue (RabbitMQ, SQS, AMQP 1.0) | one deployment |
| Pub/Sub subscription | one deployment |
| Kafka consumer group, on any topic | one deployment |
| File spool directory | one deployment |
| LLM callback route | one deployment |
| A channel's name, under a package's tenant, module and version | one deployment |
| Topic (Dapr, Knative subscription, AMQP 1.0) | any number; each receives its own copy |

Two deployments can read one Kafka topic if they use different `group.id` values, and one Pub/Sub
topic through different subscriptions. Each then gets every message. An unset `group.id` defaults from
the channel name, so the same channel in two deployments lands in one group and is refused.

This walkthrough deploys two packages that want the same route, then fixes the second.

**What was and was not run.** The walkthrough ran an HTTP route end to end on the `db` deployment source.
RabbitMQ was run end to end too. Claim identities for Kafka, SQS, Pub/Sub, AMQP and file have unit tests with
no broker behind them. Dapr fan-out was driven by a direct POST with no sidecar, Knative fan-out has unit tests
only, and the advisory lock has no concurrent-replica race test.

## Before you start

You need a running engine on the `db` deployment source, with a database role that can't bypass
row-level security (the engine refuses to boot on a superuser or a `BYPASSRLS` role with
`SUTRA.STARTUP.RLS_BYPASS_RISK`), and an admin gate, or the admin surface answers `503`. This page uses a key:
`SUTRA_ADMIN_AUTH_SCHEME=apikey` and `SUTRA_ADMIN_AUTH_KEY_REF=env:<a variable holding the key>` on the engine,
and `SUTRA_ADMIN_API_KEY` for the CLI. See [Configuration reference](../operating/configuration.md).

You also need two package directories, `orders/` and `notes/`, each with a one-step flow that answers a
request. They differ only in the module name. This is the part that matters, `channels.yaml`, in both (the
channel is `notes-in` in `notes/`):

```yaml
channels:
  - name: orders-in
    transport: http
    bind: "POST /orders"
    auth:
      scheme: apikey
      apikey:
        value: env:ORDERS_KEY
```

Both also declare a data store on one connection, `datastores.yaml`:

```yaml
datastores:
  - name: accounts
    type: sql
    sql:
      url-ref: env:SHARED_DB_URL
```

## 1. Deploy the first package

```console
$ sutra package orders --out out-orders
packaged out-orders/orders.sutra (deploymentId dep-7a4cb8e71418102e546ce735)
$ sutra deploy out-orders/orders.sutra --api --engine-url http://localhost:8080
deployed dep-7a4cb8e71418102e546ce735 (slot acme--orders--1.0.0, rev 1) — Active
$ curl -X POST http://localhost:8080/orders -H 'X-API-Key: <your key>' -H 'Content-Type: application/json' -d '{}'
hello from orders
```

## 2. Check the second package before deploying it

`sutra lint` runs the deploy's check when you give it a target. `--engine-url` asks the running engine:

```console
$ sutra lint notes --engine-url http://localhost:8080
[ERROR] SUTRA.DEPLOY.ENDPOINT.COLLISION — http endpoint 'POST /orders' is claimed by channel 'orders-in' of deployment acme--orders--1.0.0 and by channel 'notes-in' of deployment acme--notes--1.0.0; it can belong to only one deployment
[WARN] SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION — deployment acme--notes--1.0.0 and deployment acme--orders--1.0.0 use the same connection for data store(s) accounts, so they share those stores' rows. If that is not intended, give this deployment its own connection in its datastores.yaml
1 error(s), 2 warning(s)
```

The count includes a schema warning about these minimal packages, left out here. The call needs the admin
surface, so pass `--api-key` if your engine gates it. With no engine to reach, `--active-set` takes sealed
archives, or a directory of them, instead. That form runs inside the CLI, which links only the HTTP transport,
so it reports a channel on any other transport as `SUTRA.DEPLOY.ENDPOINT.UNCHECKED` rather than judging it.
See the [CLI reference](../reference/cli.md).

## 3. Try the deploy anyway

```console
$ sutra package notes --out out-notes
$ sutra deploy out-notes/notes.sutra --api --engine-url http://localhost:8080
[ERROR] SUTRA.DEPLOY.ENDPOINT.COLLISION — engine rejected the deployment (HTTP 409 Conflict): ...
$ echo $?
1
```

Nothing changed. The route still answers from the first deployment, and the second was not stored:

```console
$ curl -X POST http://localhost:8080/orders -H 'X-API-Key: <your key>' -H 'Content-Type: application/json' -d '{}'
hello from orders
$ curl http://localhost:8080/sutra/deployments
{"active":[{"deploymentId":"dep-7a4cb8e71418102e546ce735","slot":"acme--orders--1.0.0","phase":"Active","ready":true}],"pending":[],"draining":[],"failed":[]}
```

## 4. Give the second package its own route

Change the bind in `notes/channels.yaml` to `POST /notes`, then package and deploy again:

```console
$ sutra deploy out-notes/notes.sutra --api --engine-url http://localhost:8080
[WARN] SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION — deployment acme--notes--1.0.0 and deployment acme--orders--1.0.0 use the same connection for data store(s) accounts, so they share those stores' rows. If that is not intended, give this deployment its own connection in its datastores.yaml
deployed dep-b7a5226eb282e88b23a075b7 (slot acme--notes--1.0.0, rev 1) — Active
```

The deploy went through, and the warning stayed. Both deployments declare a store named `accounts` on one
connection, and rows are keyed by store name and key, so they read and write each other's rows. Sharing a
connection is sometimes right, so this never refuses a deploy. If you didn't mean it, give each deployment
its own connection in its `datastores.yaml`. The warning names the other deployment and the stores, never the
connection. `sutra lint` with no target also raises it when a `sql` store's connection reference names
neither the tenant nor the module, because any co-hosted copy of the package would share it.

## How the other deployment paths behave

**A hot deploy into the same slot is not a collision.** New content for `acme--orders--1.0.0` replaces it.

**A new version takes over from an old one.** Versions of one tenant and module count as one owner, so
`acme--orders--2.0.0` on 1.0.0's route is accepted, and `SUTRA.DEPLOY.ENDPOINT.SUPERSEDED` warns. New requests
reach 2.0.0, and 1.0.0 drains. In 0.6.18 it drains whole: every channel of 1.0.0 stops taking new work,
including one that 2.0.0 doesn't have. The warning names what stops being served, so check it before you
deploy.

**A package with no `tenant` or `module` label owns only itself.** It can't take a route, or a channel
name, from anything. Moving it to a new route is refused until you undeploy the old one. Label the package and
new versions replace old ones for you.

**A folder source (`sutra.deployments.dir`).** A refused archive is listed under `failed` in
`GET /sutra/deployments`, and the engine serves the rest. File names and arrival order decide nothing:

- The holder keeps the endpoint across restarts and on new pods. The engine records it in its database, so
  every replica reads the same answer.
- Two archives on one endpoint with no record of a holder both report `Failed` and neither deploys. Remove one
  and the other lands. With no database there is no record, so this is also what a restart gives you if a
  refused file is still there.
- The highest version of one tenant and module wins, comparing versions rather than text (`1.10.0` beats
  `1.9.0`). To roll back, remove the newer file.
- A pod that can't verify the holder's file, because it is damaged or a secret is missing there, doesn't give
  the endpoint away. It passes on only when the holder's file is gone.

**`sutra deploy` to a ConfigMap.** The CLI judges the archive against the ConfigMap's other entries and
refuses before patching it. Add `--engine-url` to have the engine judge it too, which covers every transport.

## Upgrading

- **Two deployments already on one queue.** The one activated first keeps it. The later one stops being active
  and is listed under `failed` until the next deployment change. Its running instances keep their definition.
- **Kafka groups are different.** Deployments already sharing a consumer group keep running, with
  `SUTRA.DEPLOY.ENDPOINT.ALREADY_SHARED` logged as an error. Any new deploy claiming that group is refused,
  including new content for one of them. Give each channel its own `group.id` the next time you change it, and set
  `auto.offset.reset` for what a new group should read first, because it starts from its own offsets:

  ```yaml
  channels:
    - name: orders-in
      transport: kafka
      bootstrap.servers: kafka:9092
      topic: orders
      group.id: orders-consumers
  ```

- **Topics.** Dapr and Knative channels on one topic now all receive each event, where a second claim used to be
  refused. The engine creates a `deployment_endpoint_holder` table at boot.
