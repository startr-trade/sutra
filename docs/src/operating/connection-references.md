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

**Credentials** (`username`, `password` on `rabbitmq` and `amqp`) take the whole-value forms above, `${secret:KEY}`
included since 0.6.20, and `k8s:secret/<name>#<key>`. A literal, or text with a reference inside it such as `pre${X}`,
is refused with `SUTRA.STARTUP.REFUSED.HARDCODED_SECRET`. See [section 5](#5-broker-credentials-from-a-secret-and-a-blank-reference).

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

## 5. Broker credentials from a secret, and a blank reference

Since 0.6.20, `username` and `password` on `rabbitmq` and `amqp` take `secret:KEY` and `${secret:KEY}`, besides
`${NAME}`, `env:NAME` and `k8s:secret/<name>#<key>`. A literal is still refused with
`SUTRA.STARTUP.REFUSED.HARDCODED_SECRET`.

A reference that resolves to blank now warns `SUTRA.CONFIG.PROPERTY.BLANK_REFERENCE`, naming the channel, the property
and the reference as written, never a value. Write `${NAME:}` for an optional setting: it does not warn.

A RabbitMQ channel whose credentials come from secret files (`/etc/sutra/secrets/broker-user`,
`/etc/sutra/secrets/broker-password`), and whose `exchange` is `${H}` with `H` set but empty. The
broker has only the user in that file, so the consumer connects only if the credentials resolved.
Run B is the same package with `exchange: ${H:}`.

```text
using sutra 0.6.20
$ cat refnotes/channels.yaml
channels:
  - name: note-in
    transport: rabbitmq
    host: ${BROKER_HOST}
    virtual-host: env:BROKER_VHOST
    queue: secret:notes-queue
    exchange: ${H}
    username: secret:broker-user
    password: ${secret:broker-password}
    ack-mode: on-persist
    cloudevents-mode: none
$ sutra lint refnotes
[WARN] SUTRA.CHANNEL.NO_SCHEMA — deployment default/refnotes/1.0.0 (dep-ffffffffffffffffffffffff): channel 'note-in' has 1 subscriber(s) but its codec (none) declares no message types — no type can be derived, so only a catch-all <q:source> can match. Schema-less ingress is legitimate; this warns so
[WARN] SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION — the connection of data store(s) notes names neither this package's tenant nor its module, so a copy co-hosted on one engine resolves the same connection and shares those stores' rows. If that is not intended, give each variant its own reference in it
0 error(s), 2 warning(s)
[exit 0]
$ sutra package refnotes -o out-a
[WARN] SUTRA.CHANNEL.NO_SCHEMA — deployment default/refnotes/1.0.0 (dep-ffffffffffffffffffffffff): channel 'note-in' has 1 subscriber(s) but its codec (none) declares no message types — no type can be derived, so only a catch-all <q:source> can match. Schema-less ingress is legitimate; this warns so it is explicit.
packaged out-a/refnotes.sutra (deploymentId dep-11d6a5f464d563879221824f)
[exit 0]
package refnotes-b exit=0
broker up: 1

== Run A: secret: credentials, exchange: ${H} with H set but empty
ready: {"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deploymen
queue, consumers: notes-from-a-secret	1
Message published
 store_key |                    body                    
-----------+--------------------------------------------
 s-1       | {"title":"credentials from a secret file"}
(1 row)

BLANK_REFERENCE lines:
{"code":"SUTRA.CONFIG.PROPERTY.BLANK_REFERENCE","channel":"note-in","property":"exchange","timestamp":"2026-11-10T05:26:09.248+00:00","sequence":6,"level":"WARN","loggerName":"sutra_transport_spi","message":"channel 'note-in' property 'exchange' reference '${H}' resolved to blank, so the property is unset: the transport's built-in default applies, or a required property is refused. Write the refer
HARDCODED_SECRET lines: 0
broker password in the log: 0

== Run B: exchange: ${H:} with H set but empty
ready: {"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deploymen
queue, consumers: notes-from-a-secret	1
Message published
 store_key |              body               
-----------+---------------------------------
 s-2       | {"title":"an optional setting"}
(1 row)

BLANK_REFERENCE lines: 0
broker password in the log: 0
removed, left: 0
```

The full warning text, one line in the log:

```text
channel 'note-in' property 'exchange' reference '${H}' resolved to blank, so the property is unset: the transport's built-in default applies, or a required property is refused. Write the reference with an empty default, such as ${NAME:}, to leave it blank on purpose
```

## What to watch for

- **A reference that resolves to blank is the property left unset,** so `${NAME:}` writes an optional setting. A
  variable that is set but empty counts too. A required property is then refused: Kafka `bootstrap.servers` and
  `topic`, RabbitMQ `queue`, AMQP `host`, Pub/Sub `project-id`, the file spool directory. One with a default takes
  it: RabbitMQ `host` becomes `localhost`, and Kafka `group.id` becomes `sutra-<channel>`. Since 0.6.20 the engine logs
  `SUTRA.CONFIG.PROPERTY.BLANK_REFERENCE` first (0.6.19 was silent); `${NAME:}` is the explicit form and does not warn.
- **A value that starts with a secret scheme and a colon is a reference.** A Kafka broker called `vault`, written
  `bootstrap.servers: vault:9092`, fails to resolve. Write it `${BROKER:vault:9092}`. `kafka:9092` is fine.
- **A changed value restarts the consumer at the next activation.** An activation is what reads it again; nothing
  watches it in between.
- **Endpoint claims compare references as written,** so two deployments on one queue under different names aren't
  caught. See [One endpoint, one deployment](../building/endpoint-claims.md#references-compare-as-written).
