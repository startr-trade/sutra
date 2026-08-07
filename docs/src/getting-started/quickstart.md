# Quickstart

A running engine and your first message flowing through it. About five minutes, and the only
prerequisites are the [`sutra` CLI](installation.md) and Docker.

What you will have at the end: a BPMN process, bound to an HTTP channel, that decodes and
schema-validates an XML message, branches on the result, and replies — running on the real
engine against a real PostgreSQL, not a simulator.

## 1. Scaffold an app

```bash
sutra create app my-first-app
cd my-first-app
```

That is a complete, deployable application, not a hello-world stub:

```
packages/my-first-app-main/     the deployment package (the unit you ship)
  package.yaml                  identity: tenant, module, version
  channels.yaml                 the HTTP channel the process listens on
  bpmn/sample.bpmn              the process
  schemas/sample/sample.xsd     the message contract
  templates/*.hbs               the reply bodies
deploy/compose.yaml             engine + PostgreSQL
deploy/deployments/             what the engine watches
deploy/smoke.sh                 a health-gated end-to-end check
```

## 2. Package it

```bash
sutra package packages/my-first-app-main --out deploy/deployments
```

`package` seals the directory into a single `.sutra` archive: content-addressed, with a
manifest hash over every entry. It also **lints on the way in** — a template that navigates a
field the schema does not declare, or a channel with no process bound to it, fails here rather
than at 2 a.m. That archive is now the artifact you would promote through environments.

## 3. Start the engine

```bash
docker compose -f deploy/compose.yaml up -d
```

Compose starts the engine image alongside its own PostgreSQL and mounts `deploy/deployments`,
which the engine watches. The archive you just wrote is picked up and activated on boot.

The host port is deliberately dynamic — nothing assumes port 8080 is free on your machine:

```bash
ENGINE=$(docker compose -f deploy/compose.yaml port engine 8080)
curl -s http://$ENGINE/sutra/health/ready
```

```json
{"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deployments":1,"shards":1}}]}
```

`deployments: 1` is your archive, live.

## 4. Send a message

```bash
curl -s -X POST http://$ENGINE/channels/sample-in \
  -H 'Content-Type: application/xml' \
  --data '<SampleRequest><note>hello</note></SampleRequest>'
```

```xml
<Accepted><note>hello</note></Accepted>
```

That reply came out the other end of a real process: the channel decoded the XML, validated it
against `sample.xsd`, started an instance, ran a gateway on the validation outcome, rendered a
template, and replied on the inbound connection.

Now watch it reject something. Send a payload the schema forbids:

```bash
curl -s -X POST http://$ENGINE/channels/sample-in \
  -H 'Content-Type: application/xml' \
  --data '<SampleRequest><wrong>hello</wrong></SampleRequest>'
```

You get the rejection branch, not a stack trace and not a 500. **That is the point of the
whole design**: a schema violation is a routable outcome your diagram models, so the failure
path is as reviewable as the happy path.

The bundled check runs both of those for you:

```bash
./deploy/smoke.sh
```

## 5. Look inside

```bash
# What is deployed, and what state is it in?
curl -s http://$ENGINE/sutra/deployments | jq

# What does the engine think this package binds?
sutra describe packages/my-first-app-main

# Will a message actually reach a process? (no engine needed)
sutra simulate --channel sample-in --dry-run packages/my-first-app-main
```

## 6. Change something

Edit `templates/sample-accepted.hbs`, then re-package into the watched directory:

```bash
sutra package packages/my-first-app-main --out deploy/deployments
```

The engine picks up the new archive and **flips atomically**: in-flight instances finish on
the version they started on, new messages land on the new one. No restart, no dropped request.
That is the same mechanism you would use in production — there is no separate "dev mode".

## Clean up

```bash
docker compose -f deploy/compose.yaml down -v
```

## Next

- **[Your first deployment](first-deploy.md)** — deploying over the API instead of a watched
  directory, and what "Active" actually means.
- **[Concepts](../building/concepts.md)** — why the *message*, not a REST call, is the contract.
- **[Channels](../building/channels.md)** — the other eight transports (Kafka, RabbitMQ, SQS,
  Pub/Sub, AMQP, …) your process can listen on without changing the process.
