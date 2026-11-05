# References in connection properties

A channel's broker address, queue or spool directory can come from the environment or a mounted secret,
so one package runs unchanged in each environment. This walkthrough runs a RabbitMQ channel whose whole
connection is references, then shows what happens when one doesn't resolve.

You need the engine, PostgreSQL and a broker. This was run on PostgreSQL 16 and RabbitMQ 3.13.

## Which properties, and which forms

| Transport | Properties that may be a reference |
|---|---|
| `kafka` | `bootstrap.servers`, `topic`, `group.id` |
| `rabbitmq` | `host`, `port`, `virtual-host`, `queue`, `exchange` |
| `amqp` | `host`, `port`, `queue`, `topic` |
| `gcp-pubsub` | `project-id`, `subscription`, `topic`, `endpoint-override` |
| `file` | `spool.dir`, `directory` |
| `aws-sqs` | `region`, `queue.url`, `endpoint-override`, `wait-time-seconds`, `max-messages`, `visibility-timeout-seconds` |

| Form | Reads |
|---|---|
| `${NAME}` | the environment variable, and it can sit inside other text: `${BROKER_HOST}:9092` |
| `${NAME:default}` | the same, or the default when it isn't set |
| `env:NAME` | the environment variable; the whole value only |
| `secret:KEY`, `${secret:KEY}` | the file `KEY` in `SUTRA_SECRETS_DIR`, default `/etc/sutra/secrets` |
| `vault:`, `aws-secrets:`, `azure-kv:`, `gcp-secret:` | the vendor store, in a build that links it; the whole value only, or inside `${…}` |

Other properties of these channels, such as `prefetch-count`, stay literal.

**Credentials follow their own rule.** On `rabbitmq` and `amqp`, `username` and `password` must be `${NAME}`,
`env:NAME` or `k8s:secret/<name>#<key>`. A `secret:KEY` or `${secret:KEY}` there is refused with
`SUTRA.STARTUP.REFUSED.HARDCODED_SECRET`. To use a mounted secret as a broker password, expose it to the
engine as an environment variable.

## 1. The channel

`channels.yaml`. The host and port come from the environment, the port with a default, the virtual host from
`env:`, and the queue name from a mounted secret file:

```yaml
channels:
  - name: note-in
    transport: rabbitmq
    host: ${BROKER_HOST}
    port: ${BROKER_PORT:5672}
    virtual-host: env:BROKER_VHOST
    queue: secret:notes-queue
    username: ${BROKER_USER}
    password: ${BROKER_PASSWORD}
    ack-mode: on-persist
    cloudevents-mode: none
```

The flow is `note-keep` from [Keeping a message as text](../building/data-stores.md#keeping-a-message-as-text), which writes the
message to a `notes` table. `secrets/notes-queue` holds the queue's name, `notes-from-a-secret`. Create that
queue on the broker in a virtual host called `refs`.

```bash
sutra lint refnotes
sutra package refnotes -o out
```

## 2. A reference that doesn't resolve

Start the engine with `SUTRA_DEPLOYMENTS_DIR` pointing at `out`, the secrets folder mounted, and every setting
except `BROKER_HOST`. It stops with exit code 1 and this error:

```text
rabbitmq channel wiring failed: [SUTRA.INBOUND.RABBITMQ.CONFIG_INVALID] channel 'note-in' property 'host' has an unresolved reference: placeholder '${BROKER_HOST}' resolves to no value (environment variable 'BROKER_HOST' is not set and no default is given)
```

The message names the channel, the property and the variable.

Now set `BROKER_HOST`, and set `BROKER_PORT` to text that is no port. It also stops with exit code 1, and the
message doesn't repeat what the variable held:

```text
rabbitmq channel wiring failed: [SUTRA.INBOUND.RABBITMQ.CONFIG_INVALID] channel 'note-in' property 'port' has a reference that does not resolve to an integer in 1..=65535
```

## 3. All of them set

Set `BROKER_HOST` and leave `BROKER_PORT` unset, so that the default applies. The engine starts, and the queue
named by the secret has one consumer:

```bash
rabbitmqctl -q list_queues -p refs name consumers --no-table-headers
```

```text
notes-from-a-secret	1
```

Publish a message to that queue with a `message_id` of `r-1` and a content type of `application/json`. The row
is there:

```sql
SELECT store_key, content_type, body FROM notes;
```

```text
 store_key |   content_type   |              body
-----------+------------------+---------------------------------
 r-1       | application/json | {"title":"through a reference"}
(1 row)
```

The engine's log names the queue it reached:

```json
{"channel":"note-in","queue":"notes-from-a-secret","level":"INFO","message":"rabbitmq consumer up"}
```

So a value you read from a secret store shows up in connection log lines, as a literal one would. These are
connection settings. Keep credentials in the credential properties.

## 4. An activation in a running engine

An activation is not refused for this. With the engine on the `db` deployment source, version `1.0.0` deployed
and consuming, deploy a `1.1.0` whose queue is `${NOTES_QUEUE_NOT_SET}`:

```bash
sutra deploy out2/refnotes.sutra --api --engine-url "$B"
```

```text
deployed dep-f26c5a0b3b788bca8f9833e5 (slot default--refnotes--1.1.0, rev 1) — Active
```

`1.1.0` reports `Active` and `ready`, and has no consumer. The engine logs a WARN:

```json
{"code":"SUTRA.INBOUND.RABBITMQ.CONFIG_INVALID","level":"WARN","message":"rabbitmq consumer NOT started during flip: channel 'note-in' property 'queue' has an unresolved reference: placeholder '${NOTES_QUEUE_NOT_SET}' resolves to no value (environment variable 'NOTES_QUEUE_NOT_SET' is not set and no default is given)"}
```

No queue with that name was created, and `1.0.0` keeps its consumer. So watch for this code after a deploy,
because the deploy's own answer won't show it.

If the channel with the bad reference already had a running consumer, that consumer stops. The next activation
in which the reference resolves starts it again.

## What to watch for

- **A reference that resolves to blank is the property left unset,** so `${NAME:}` writes an optional setting. A
  variable that is set but empty counts too. A required property is then refused: Kafka `bootstrap.servers` and
  `topic`, RabbitMQ `queue`, AMQP `host`, Pub/Sub `project-id`, the file spool directory. One with a default takes
  it with no warning: RabbitMQ `host` becomes `localhost`, and Kafka `group.id` becomes `sutra-<channel>`.
- **A value that starts with a secret scheme and a colon is a reference.** A Kafka broker called `vault`, written
  `bootstrap.servers: vault:9092`, fails to resolve. Write it `${BROKER:vault:9092}`. `kafka:9092` is fine.
- **A changed value restarts the consumer at the next activation.** An activation is what reads it again; nothing
  watches it in between.
- **Endpoint claims compare references as written,** so two deployments on one queue under different names aren't
  caught. See [One endpoint, one deployment](../building/endpoint-claims.md#references-compare-as-written).
