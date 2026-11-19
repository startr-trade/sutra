# Connecting the engine to IBM MQ

The engine reaches IBM MQ through the queue manager's AMQP 1.0 channel, with the `amqp` transport.
There is no MQ client library in the engine: the queue manager speaks AMQP on a port of its own, and
MQ applications and other queue managers see ordinary MQ messages on ordinary MQ queues.

**What this was tested against.** IBM MQ Advanced for Developers 10.0.1.0 in containers, on one
machine: two queue managers joined by a sender/receiver channel pair over mutual TLS, local queues, a
remote queue definition, a topic with administrative subscriptions, the AMQP channel in plain text and
with TLS, SASL PLAIN, concurrent sends, and the queue manager stopped and started while messages were
being sent. IBM MQ on
z/OS, MQ appliances, IBM MQ as a service and the native MQI client have not been tested and are not
claimed to work.

## What MQ needs from a channel

MQ maps AMQP onto its own message model, and two of its rules decide how a channel is written:

- **An address without a capability is a topic.** MQ reads an AMQP address as a topic string unless the
  link says it names a queue. Set `address-type: queue` on an inbound channel and `?type=queue` on a
  destination. Without it a send goes to the topic of that name (with no subscriber it reaches no one,
  and a user without publish authority there is refused with `AMQXR0102E`), and an inbound channel
  subscribes to that topic instead of reading the queue.
- **The body section decides the MQ format.** A message that comes over AMQP is stored as `MQSTR`
  when its body is one AMQP string, as `MQFMT_NONE` when it is one AMQP binary value, and as an
  embedded AMQP message (`MQAMQP`) otherwise, which includes the engine's default `data` section. Add
  `body=text` for XML, JSON and other text, or `body=binary` for bytes, to every destination an MQ
  application reads. An engine reading the queue over AMQP gets the same bytes either way.

Two more rules the transport follows by itself since 0.7.0: every message names its
destination in its `to` property, which MQ requires (`AMQXR0032E` otherwise), and every connection has
its own container id, because MQ treats the container id as the client's identity and closes the
older of two connections that share one.

## The settings

An inbound channel takes the `amqp` properties described in
[Connecting to a TLS and SASL broker](broker-tls-sasl.md), and one more:

| Property | Meaning |
|---|---|
| `address-type` | `queue` or `topic`, sent to the broker as the link's capability. Unset sends none |

A destination (`amqp10://` or `amqp10s://host[:port]/<address>`) takes these query options. Each is off
unless you set it, so an existing destination sends what it always did:

| Option | Values | Effect |
|---|---|---|
| `type` | `queue`, `topic` | The link's capability |
| `durable` | `true`, `false` | The message's `durable` flag: MQ's persistent / not persistent |
| `ttl-ms` | milliseconds | The message's time to live: MQ's expiry, in tenths of a second |
| `body` | `data` (default), `binary`, `text` | `data` is one AMQP data section, `binary` one AMQP binary value, `text` one AMQP string. With `text` a payload that is not UTF-8 is refused with `SUTRA.OUTBOUND.AMQP.CONFIG_INVALID` |
| `application-properties` | `all` (default), `none` | `none` sends no application properties: no `sutra-outbox-key`, no `content-type`, no headers. The outbox key is still the `message-id` |
| `property-names` | `verbatim` (default), `identifier` | `identifier` rewrites every application-property name to `[A-Za-z_$][A-Za-z0-9_$]*`: `sutra-outbox-key` becomes `sutra_outbox_key`, which the engine reads too. IBM MQ 10.0.1 kept names with `-` as they were; IBM documents earlier releases as refusing them, and JMS applications can read only identifier names |

A value an option cannot take stops the send permanently with `SUTRA.OUTBOUND.AMQP.CONFIG_INVALID`.

Four headers carry AMQP message properties both ways. A received message has them set from the message,
and an outbound message that sets them sends them as those properties rather than as application
properties:

| Header | AMQP property | In MQ |
|---|---|---|
| `x-amqp-message-id` | `message-id` | Inbound: MQ's `MsgId`, 24 bytes as 48 hex digits. Outbound: kept in the `AMQPMsgId` property; MQ makes its own `MsgId`. The engine sends the outbox key when the header is unset |
| `x-amqp-correlation-id` | `correlation-id` | `CorrelId` |
| `x-amqp-reply-to` | `reply-to` | `ReplyToQ` |
| `x-amqp-subject` | `subject` | the `AMQPSubject` property |

## How a message looks to MQ

What an MQ application reading the queue saw, from the queue manager's own `amqsbcg`:

| MQ field | From the engine |
|---|---|
| `MsgType` | `MQMT_DATAGRAM` (8), always |
| `Format` | `MQSTR` with `body=text`, blank (`MQFMT_NONE`) with `body=binary`, `MQAMQP` with the default `data` section |
| `CodedCharSetId` | 1208, always; the text is the engine's UTF-8 bytes unchanged |
| `Persistence` | 1 with `durable=true`, 0 otherwise: IBM documents a message without the flag as not persistent, whatever the queue's `DEFPSIST` |
| `Expiry` | `ttl-ms` in tenths of a second, unlimited (-1) without it |
| `MsgId` | made by the queue manager; the engine's message id is kept in the `AMQPMsgId` property |
| `CorrelId` | the `x-amqp-correlation-id` text, padded with zero bytes to 24 |
| `ReplyToQ`, `ReplyToQMgr` | the `x-amqp-reply-to` queue, and the queue manager the engine is connected to |
| `UserIdentifier` | the user the AMQP channel authenticated |
| `PutApplName` | `IBM MQ AMQP Support 10.0.1.0` |

**Message properties and MQRFH2.** The AMQP header and properties become MQ message properties in an
`mq_amqp` folder, a reply-to also in a `jms` folder, and the application properties in `usr`. An
application that reads a queue with the default `PROPCTL(COMPAT)` and does not ask for properties gets
them as an MQRFH2 header in front of the body, with `Format` `MQHRF2`, when the message has a `usr` or
`jms` folder; the `mq_amqp` folder alone is not handed to it. So for a reader that expects a plain
message, either send it with `application-properties=none` and no `x-amqp-reply-to`, or set
`PROPCTL(NONE)` on the queue it reads. The sender channel's own `PROPCTL` does not help: it applies only
to queue managers of version 6 and earlier.

Going the other way, a message an MQ application put arrives with its `MsgId` in
`x-amqp-message-id` (48 hex digits), its `ReplyToQ` in `x-amqp-reply-to`, and an `MQSTR` body as text.
The `MsgId` is also the idempotency key when the message has no `sutra-outbox-key`.

## When the queue manager goes away

An inbound channel reconnects by itself, backing off from 1 s to 30 s, so it is attached again at most
about 30 s after the queue manager is back. An outbound send that cannot reach the queue manager fails
as retryable and the outbox sends it again. Both log the outage once
when it starts and once when it ends, with its start, end and length:

| Code | Logged |
|---|---|
| `SUTRA.INBOUND.AMQP.OUTAGE_STARTED` | WARN, when a consumer loses its connection or cannot open one; `outage_started` |
| `SUTRA.INBOUND.AMQP.OUTAGE_ENDED` | INFO, when it is attached again; `outage_started`, `outage_ended`, `outage_ms` |
| `SUTRA.OUTBOUND.AMQP.OUTAGE_STARTED` | WARN, per broker (`host:port`), at the first send that cannot connect |
| `SUTRA.OUTBOUND.AMQP.OUTAGE_ENDED` | INFO, at the first send that connects again |

The walkthrough below shows both lines from a real restart.

The same outage can also reach the package as messages, so a deployment can record its own
outage windows: set `SUTRA_CONNECTIVITY_EVENTS_CHANNEL` and declare that channel
(see [Broker connectivity events](configuration.md#broker-connectivity-events) and
[Broker outages as messages](connectivity-events.md)).

An AMQP channel that was running when the queue manager stopped starts again with it. What MQ keeps
across the restart is MQ's rule, not the engine's: persistent messages (`durable=true`) are kept, and
so are non-persistent ones on a queue defined with `NPMCLASS(HIGH)` after a normal shutdown; other
non-persistent messages are lost. A message the engine had received but not yet settled is delivered
again after the reconnect, and the engine's inbox drops the duplicate.

## What the AMQP channel cannot do

These are limits of MQ's AMQP support, not settings:

- **`MQPER_PERSISTENCE_AS_Q_DEF`.** A message is persistent or not, by `durable`; the queue's default
  does not apply. Set `durable` to match the queues a destination writes to.
- **`ReplyToQMgr`.** It is always the queue manager the engine is connected to, and the engine
  addresses only queues that queue manager can open: its local queues, aliases and remote queue
  definitions. To answer a `ReplyToQ` on another queue manager, define a remote queue definition for it.
- **A binary `CorrelId` or `MsgId`.** An outbound `x-amqp-correlation-id` is text, at most 24 bytes
  as MQ keeps it. The engine cannot set a `MsgId`: the queue manager makes one.
- **Other descriptor fields.** `Report`, `Feedback`, `GroupId`, `MsgSeqNumber`, `Offset` and `MsgFlags`
  keep their defaults, and segmentation and message groups are not available.
- **One AMQP session per connection.** The engine opens one connection, with one session, per inbound
  channel and per outbound send, which fits.
- **Message size.** An AMQP channel accepts messages up to its `MAXMSGL` (4 MB unless set); raise it,
  and the queues', for larger messages.

Where a peer needs exact control of the message descriptor, the AMQP channel is the wrong road: that
takes an MQI client, which the engine does not have.

## Walkthrough: a message through IBM MQ

You need Docker, `openssl`, the `sutra` CLI and an empty working directory. This builds an IBM MQ
developer image with AMQP, gives the queue manager a certificate, defines its AMQP channel, queues and
a topic, runs a package that answers each message, and restarts the queue manager under it. It was
run as written with IBM MQ Advanced for Developers 10.0.1.0, `postgres:16-alpine` and the engine built
from this release; the output shown is from that run.

**1. Build an IBM MQ image with AMQP.** The published developer image leaves out MQ's AMQP component.
Copy it in from the developer archive of the same release. The archive is about 550 MB and IBM's
download site can be slow on one connection; only its small `amqp/` directory is used:

```bash
curl -fLO https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqadv/10.0.1.0-IBM-MQ-Advanced-for-Developers-Non-Install-LinuxX64.tar.gz
mkdir -p mq-amqp
tar xzf 10.0.1.0-IBM-MQ-Advanced-for-Developers-Non-Install-LinuxX64.tar.gz -C mq-amqp amqp
printf 'FROM icr.io/ibm-messaging/mq:10.0.1.0-r1\nCOPY --chown=1001:0 amqp/ /opt/mqm/amqp/\n' > mq-amqp/Dockerfile
docker build -q -t mq-amqp:10.0.1.0 mq-amqp
```

Running the image means accepting the IBM MQ Advanced for Developers licence (`LICENSE=accept`
below). It is for development and testing only.

**2. Create the certificates.** The queue manager's certificate must name the host the engine connects
to. The AMQP channel reads it from a PKCS#12 key repository with a password; it cannot use the CMS
repository the image builds from `/etc/mqm/pki`.

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 7 \
  -subj "/CN=demo MQ CA" -keyout ca.key -out ca.pem 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=mqdemo-qm" -keyout qm.key -out qm.csr 2>/dev/null
openssl x509 -req -in qm.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 7 \
  -extfile <(printf "subjectAltName=DNS:mqdemo-qm") -out qm.crt 2>/dev/null
KEYRING_PASSWORD=$(openssl rand -hex 16)
MQ_PASSWORD=$(openssl rand -hex 16)
mkdir -p tls
openssl pkcs12 -export -name qm -in qm.crt -inkey qm.key -certfile ca.pem \
  -out tls/qm.p12 -passout "pass:$KEYRING_PASSWORD"
chmod 644 tls/qm.p12 ca.pem
docker network create mqdemo-net
```

**3. Define the queue manager's objects.** An AMQP channel with TLS that requires a user and
password, an inbound and an outbound queue, a topic, and an administrative subscription that copies
every publication to a queue. The key repository is set before the AMQP service starts, because the
service reads it then.

```bash
cat > qm.mqsc <<EOF
ALTER QMGR SSLKEYR('/tls/qm.p12') KEYRPWD('$KEYRING_PASSWORD') CERTLABL('qm')
REFRESH SECURITY TYPE(SSL)
DEFINE CHANNEL('ENGINE.AMQPS') CHLTYPE(AMQP) PORT(5671) MCAUSER('app') +
       SSLCIPH('ANY_TLS12_OR_HIGHER') SSLCAUTH(OPTIONAL) REPLACE
SET CHLAUTH('ENGINE.AMQPS') TYPE(ADDRESSMAP) ADDRESS('*') USERSRC(CHANNEL) CHCKCLNT(REQUIRED) +
    ACTION(REPLACE)
DEFINE QLOCAL('ORDERS.IN') REPLACE
DEFINE QLOCAL('ORDERS.OUT') REPLACE
DEFINE TOPIC('ORDERS.EVENTS') TOPICSTR('orders/events') REPLACE
DEFINE QLOCAL('ORDERS.EVENTS.COPY') REPLACE
DEFINE SUB('ORDERS.EVENTS.COPY') TOPICSTR('orders/events') DEST('ORDERS.EVENTS.COPY') REPLACE
SET AUTHREC PROFILE('ORDERS.**') PRINCIPAL('app') OBJTYPE(QUEUE) AUTHADD(BROWSE,GET,INQ,PUT)
SET AUTHREC PROFILE('ORDERS.**') PRINCIPAL('app') OBJTYPE(TOPIC) AUTHADD(PUB,SUB)
ALTER SERVICE(SYSTEM.AMQP.SERVICE) CONTROL(QMGR)
START SERVICE(SYSTEM.AMQP.SERVICE)
EOF
chmod 644 qm.mqsc
```

**4. Start the queue manager**, then the AMQP channel. The channel can start only once the AMQP
service is up, so retry until it runs. After a restart it starts again by itself.

```bash
docker run -d --name mqdemo-qm --network mqdemo-net \
  -e LICENSE=accept -e MQ_QMGR_NAME=QM1 -e MQ_APP_PASSWORD="$MQ_PASSWORD" \
  -v "$PWD/qm.mqsc:/etc/mqm/30-demo.mqsc:ro" -v "$PWD/tls:/tls:ro" \
  mq-amqp:10.0.1.0
until docker logs mqdemo-qm 2>&1 | grep -q 'Started queue manager'; do sleep 1; done
until docker exec mqdemo-qm bash -c "echo \"START CHANNEL('ENGINE.AMQPS')
DISPLAY CHSTATUS('ENGINE.AMQPS') CHLTYPE(AMQP)\" | runmqsc QM1" | grep -q 'STATUS(RUNNING)'; do
  sleep 1
done
```

**5. Write the package** in `mq-relay/` (`mkdir -p mq-relay/bpmn`). `mq-relay/package.yaml`:

```yaml
labels:
  "tenant": "default"
  "module": "mq-relay"
  "version": "1.0.0"
engine:
  minContract: 1
```

`mq-relay/channels.yaml`. The inbound channel names its queue as a queue; both outbound destinations
send the payload as text and no application properties, so an MQ application reading the default
queues gets a plain `MQSTR` message:

```yaml
channels:
  - name: mq-in
    transport: amqp
    codec: json
    cloudevents-mode: none
    host: mqdemo-qm
    tls: true
    tls-ca-pem: secret:MQ_CA
    username: env:MQ_USER
    password: secret:MQ_PASSWORD
    queue: ORDERS.IN
    address-type: queue
  - name: mq-out
    direction: outbound
    transport: amqp
    bind: amqp10s://mqdemo-qm/ORDERS.OUT?type=queue&body=text&application-properties=none
    cloudevents-mode: none
  - name: mq-events
    direction: outbound
    transport: amqp
    bind: amqp10s://mqdemo-qm/orders/events?type=topic&body=text&application-properties=none
    cloudevents-mode: none
```

`mq-relay/bpmn/mq-relay.bpmn` answers each message, then announces it on the topic:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="Defs"
                  targetNamespace="urn:sutra:module:mq-relay:1.0.0">
  <bpmn:process id="mq-relay" isExecutable="true">
    <bpmn:extensionElements>
      <q:variables>
        <q:variable name="responseBody" type="object"/>
      </q:variables>
    </bpmn:extensionElements>
    <bpmn:startEvent id="start">
      <bpmn:extensionElements><q:source channel="mq-in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="build"/>
    <bpmn:serviceTask id="build">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation>
        <bpmn:assignment>
          <bpmn:from>{ref: payload.ref, status: "ACCEPTED"}</bpmn:from>
          <bpmn:to>responseBody</bpmn:to>
        </bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="build" targetRef="answer"/>
    <bpmn:sendTask id="answer">
      <bpmn:extensionElements><q:send channel="mq-out"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f3" sourceRef="answer" targetRef="announce"/>
    <bpmn:sendTask id="announce">
      <bpmn:extensionElements><q:send channel="mq-events"/></bpmn:extensionElements>
      <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f4" sourceRef="announce" targetRef="end"/>
    <bpmn:endEvent id="end"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
```

```bash
mkdir -p deployments secrets
sutra lint mq-relay
sutra package mq-relay --out deployments
```

```text
[WARN] SUTRA.CHANNEL.NO_SCHEMA — deployment default/mq-relay/1.0.0 (...): channel 'mq-in' has 1 subscriber(s) but its codec (json) declares no message types — ...
0 error(s), 1 warning(s)
packaged deployments/mq-relay.sutra (deploymentId dep-b95d811099f0245b07e2d9c1)
```

**6. Start the engine.** The inbound channel reads the password and the CA from the mounted secrets
directory; the outbound sends take theirs from the environment.

```bash
printf '%s' "$MQ_PASSWORD" > secrets/MQ_PASSWORD
cp ca.pem secrets/MQ_CA
chmod -R a+rX deployments secrets
docker run -d --name mqdemo-db --network mqdemo-net -e POSTGRES_PASSWORD=dev-only postgres:16-alpine
until docker exec mqdemo-db pg_isready -U postgres -h 127.0.0.1; do sleep 1; done
sleep 2
docker exec mqdemo-db psql -U postgres \
  -c "CREATE ROLE sutra_engine LOGIN PASSWORD 'dev-only' NOSUPERUSER NOBYPASSRLS" \
  -c "ALTER SCHEMA public OWNER TO sutra_engine"
docker run -d --name mqdemo-engine --network mqdemo-net -p 127.0.0.1::8080 \
  -v "$PWD/deployments:/deployments:ro" -e SUTRA_DEPLOYMENTS_DIR=/deployments \
  -v "$PWD/secrets:/etc/sutra/secrets:ro" \
  -v "$PWD/ca.pem:/etc/sutra/mq-ca.pem:ro" \
  -e SUTRA_DATASOURCE_URL=postgres://mqdemo-db:5432/postgres \
  -e SUTRA_DATASOURCE_USERNAME=sutra_engine -e SUTRA_DATASOURCE_PASSWORD=dev-only \
  -e MQ_USER=app \
  -e SUTRA_SINK_AMQP_USERNAME=app \
  -e SUTRA_SINK_AMQP_PASSWORD="$MQ_PASSWORD" \
  -e SUTRA_SINK_AMQP_TLS_CA_FILE=/etc/sutra/mq-ca.pem \
  ghcr.io/startr-trade/sutra:0.8.1
PORT=$(docker port mqdemo-engine 8080 | head -1 | cut -d: -f2)
until curl -sf "http://127.0.0.1:$PORT/sutra/health/ready"; do sleep 1; done
```

```json
{"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deployments":1,"shards":1}}]}
```

**7. Put a message as an MQ application would, and read the answer and the announcement.**

```bash
docker exec mqdemo-qm bash -c \
  "printf '%s\n\n' '{\"ref\":\"mq-1\"}' | /opt/mqm/samp/bin/amqsput ORDERS.IN QM1"
until [ "$(docker exec mqdemo-qm bash -c "echo \"DISPLAY QLOCAL('ORDERS.OUT') CURDEPTH\" | runmqsc QM1" | grep -o 'CURDEPTH([0-9]*)')" = "CURDEPTH(1)" ]; do sleep 1; done
docker exec mqdemo-qm /opt/mqm/samp/bin/amqsbcg ORDERS.OUT QM1 | grep -E "Format|CodedCharSetId"
docker exec mqdemo-qm /opt/mqm/samp/bin/amqsget ORDERS.OUT QM1
until [ "$(docker exec mqdemo-qm bash -c "echo \"DISPLAY QLOCAL('ORDERS.EVENTS.COPY') CURDEPTH\" | runmqsc QM1" | grep -o 'CURDEPTH([0-9]*)')" = "CURDEPTH(1)" ]; do sleep 1; done
docker exec mqdemo-qm /opt/mqm/samp/bin/amqsget ORDERS.EVENTS.COPY QM1
```

```text
Sample AMQSPUT0 start
target queue is ORDERS.IN
Sample AMQSPUT0 end
  Encoding : 273  CodedCharSetId : 1208
  Format : 'MQSTR   '
Sample AMQSGET0 start
message <{"ref":"mq-1","status":"ACCEPTED"}>
no more messages
Sample AMQSGET0 end
Sample AMQSGET0 start
message <{"ref":"mq-1","status":"ACCEPTED"}>
no more messages
Sample AMQSGET0 end
```

`amqsput` put an `MQSTR` message; the engine read it over the AMQP channel with TLS and SASL PLAIN and
answered on `ORDERS.OUT` as `MQSTR` in CCSID 1208, with no MQRFH2 in front of it. Its one publication
to `orders/events` reached `ORDERS.EVENTS.COPY` through the subscription.

**8. Restart the queue manager** and send another message once it is back:

```bash
docker restart mqdemo-qm
until [ "$(docker logs mqdemo-qm 2>&1 | grep -c 'Started queue manager')" -ge 2 ]; do sleep 1; done
docker exec mqdemo-qm bash -c \
  "printf '%s\n\n' '{\"ref\":\"mq-2\"}' | /opt/mqm/samp/bin/amqsput ORDERS.IN QM1"
until [ "$(docker exec mqdemo-qm bash -c "echo \"DISPLAY QLOCAL('ORDERS.OUT') CURDEPTH\" | runmqsc QM1" | grep -o 'CURDEPTH([0-9]*)')" = "CURDEPTH(1)" ]; do sleep 1; done
docker exec mqdemo-qm /opt/mqm/samp/bin/amqsget ORDERS.OUT QM1
docker logs mqdemo-engine 2>&1 | grep -E 'OUTAGE_(STARTED|ENDED)'
```

```text
Sample AMQSPUT0 start
target queue is ORDERS.IN
Sample AMQSPUT0 end
Sample AMQSGET0 start
message <{"ref":"mq-2","status":"ACCEPTED"}>
no more messages
Sample AMQSGET0 end
{"channel":"mq-in","code":"SUTRA.INBOUND.AMQP.OUTAGE_STARTED","outage_started":"2026-11-19T04:24:51.086333388Z",...,"level":"WARN",...,"message":"amqp consumer stream failed — reconnecting",...}
{"channel":"mq-in","code":"SUTRA.INBOUND.AMQP.OUTAGE_ENDED","outage_started":"2026-11-19T04:24:51.086333388Z","outage_ended":"2026-11-19T04:25:06.679943301Z","outage_ms":15593,...,"level":"INFO",...,"message":"amqp consumer reconnected after an outage",...}
```

The consumer lost the queue manager when it stopped and was attached again 15.6 s later, by itself.
Part of that is the queue manager stopping and starting; the rest is the consumer's backoff, which
waits up to 30 s between attempts, so the gap differs from run to run (31.7 s in an earlier run). The engine's log held the queue manager password 0 times.

**9. Clean up.**

```bash
docker rm -f mqdemo-engine mqdemo-db mqdemo-qm
docker network rm mqdemo-net
```
