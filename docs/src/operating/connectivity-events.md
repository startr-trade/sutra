# Broker outages as messages

*Since 0.8.0.* The engine logs every broker outage it sees (see
[When the queue manager goes away](ibm-mq.md#when-the-queue-manager-goes-away)). With
`sutra.engine.connectivity-events.channel` set, the same outage also reaches the deployment that
owns the channel, as a JSON message on a channel of that name, so a process can record its own
outage windows, alert, or hold work. The keys and the message are described in
[Broker connectivity events](configuration.md#broker-connectivity-events).

This walkthrough runs an AMQP 1.0 broker (Apache ActiveMQ Artemis), a package that consumes one
queue and sends to another, and a small HTTP server that prints what the package records. It stops
the broker, sends while it is down, and starts it again. You need Docker, `curl`, the `sutra` CLI and
an empty working directory. It was run as written with the engine built from this release,
`apache/activemq-artemis:latest-alpine`, `python:3.12-alpine` and `postgres:16-alpine`; the output
shown is from that run.

## 1. Start the broker and a capture server

`capture.py` prints the body of every POST it receives:

```python
from http.server import BaseHTTPRequestHandler, HTTPServer

class Capture(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        print(body.decode().strip(), flush=True)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass

HTTPServer(("", 8000), Capture).serve_forever()
```

```bash
docker network create outages-net
docker run -d --name outages-broker --network outages-net \
  -e ARTEMIS_USER=demo -e ARTEMIS_PASSWORD=demo-only \
  apache/activemq-artemis:latest-alpine
until docker logs outages-broker 2>&1 | grep -q 'Server is now active'; do sleep 1; done
docker run -d --name outages-capture --network outages-net \
  -v "$PWD/capture.py:/capture.py:ro" python:3.12-alpine python -u /capture.py
```

## 2. Write the package

`outages/package.yaml`:

```yaml
labels:
  "tenant": "default"
  "module": "outages"
  "version": "1.0.0"
engine:
  minContract: 1
```

`outages/channels.yaml`. `orders-in` and `orders-out` are the AMQP channels whose outages are
reported. `connectivity-in` is the channel the events arrive on: an in-process intake with the JSON
codec. `record-out` sends each event to the capture server:

```yaml
channels:
  - name: orders-in
    transport: amqp
    codec: json
    cloudevents-mode: none
    host: outages-broker
    username: env:BROKER_USER
    password: env:BROKER_PASSWORD
    queue: orders.in
  - name: orders-out
    direction: outbound
    transport: amqp
    bind: amqp10://outages-broker/orders.out
    cloudevents-mode: none
  - name: emit
    transport: http
    bind: "POST /channels/emit"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: env:OUTAGES_API_KEY
        header: X-Api-Key
  - name: connectivity-in
    transport: local
    codec: json
  - name: record-out
    direction: outbound
    transport: http
    bind: "http://outages-capture:8000/connectivity"
```

`outages/templates/out.hbs`:

```text
{"n":"{{payload.n}}"}
```

`outages/templates/record.hbs` copies the event's fields:

```text
{"code":"{{payload.code}}","channel":"{{payload.channel}}","direction":"{{payload.direction}}","state":"{{payload.state}}","startedAt":"{{payload.startedAt}}"{{#if payload.endedAt}},"endedAt":"{{payload.endedAt}}","ms":{{payload.ms}}{{/if}}}
```

`outages/bpmn/outages.bpmn`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:outages:1.0.0">
  <!-- consumes orders-in, so the intake is attached to the broker -->
  <bpmn:process id="listen" isExecutable="true">
    <bpmn:startEvent id="S1">
      <bpmn:extensionElements><q:source channel="orders-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="l1" sourceRef="S1" targetRef="E1"/>
    <bpmn:endEvent id="E1"/>
  </bpmn:process>

  <!-- POST /channels/emit sends one message on orders-out -->
  <bpmn:process id="emit" isExecutable="true">
    <bpmn:startEvent id="S2">
      <bpmn:extensionElements><q:source channel="emit" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="e1" sourceRef="S2" targetRef="Send"/>
    <bpmn:serviceTask id="Send" implementation="out.hbs">
      <bpmn:extensionElements>
        <q:send channel="orders-out" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="e2" sourceRef="Send" targetRef="E2"/>
    <bpmn:endEvent id="E2"/>
  </bpmn:process>

  <!-- each connectivity event starts an instance, which records it -->
  <bpmn:process id="record" isExecutable="true">
    <bpmn:startEvent id="S3">
      <bpmn:extensionElements><q:source channel="connectivity-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="r1" sourceRef="S3" targetRef="Record"/>
    <bpmn:serviceTask id="Record" implementation="record.hbs">
      <bpmn:extensionElements>
        <q:send channel="record-out" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="r2" sourceRef="Record" targetRef="E3"/>
    <bpmn:endEvent id="E3"/>
  </bpmn:process>
</bpmn:definitions>
```

```bash
sutra lint outages
mkdir -p deployments
sutra package outages --out deployments
chmod -R a+rX deployments
```

Lint reports 0 errors and 11 warnings: three `SUTRA.CHANNEL.NO_SCHEMA` and eight
`SUTRA.CONFIG.TEMPLATE.FIELD_UNVERIFIABLE`, because the JSON codec declares no schema.

## 3. Start the engine with the events channel set

`SUTRA_CONNECTIVITY_EVENTS_CHANNEL=connectivity-in` names the channel. A deployment that declares
no channel of that name receives nothing.

```bash
docker run -d --name outages-db --network outages-net -e POSTGRES_PASSWORD=dev-only postgres:16-alpine
until docker exec outages-db pg_isready -U postgres -h 127.0.0.1; do sleep 1; done
sleep 2
docker exec outages-db psql -U postgres \
  -c "CREATE ROLE sutra_engine LOGIN PASSWORD 'dev-only' NOSUPERUSER NOBYPASSRLS" \
  -c "ALTER SCHEMA public OWNER TO sutra_engine"
docker run -d --name outages-engine --network outages-net -p 127.0.0.1::8080 \
  -v "$PWD/deployments:/deployments:ro" -e SUTRA_DEPLOYMENTS_DIR=/deployments \
  -e SUTRA_DATASOURCE_URL=postgres://outages-db:5432/postgres \
  -e SUTRA_DATASOURCE_USERNAME=sutra_engine -e SUTRA_DATASOURCE_PASSWORD=dev-only \
  -e SUTRA_CONNECTIVITY_EVENTS_CHANNEL=connectivity-in \
  -e BROKER_USER=demo -e BROKER_PASSWORD=demo-only \
  -e SUTRA_SINK_AMQP_USERNAME=demo -e SUTRA_SINK_AMQP_PASSWORD=demo-only \
  -e OUTAGES_API_KEY=demo-key \
  ghcr.io/startr-trade/sutra:0.8.4
PORT=$(docker port outages-engine 8080 | head -1 | cut -d: -f2)
until curl -sf "http://127.0.0.1:$PORT/sutra/health/ready"; do sleep 1; done
```

```json
{"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deployments":1,"shards":1}}]}
```

Send one message while the broker is up, so the send channel has connected. Nothing is recorded:

```bash
curl -s -o /dev/null -w '%{http_code}\n' -X POST "localhost:$PORT/channels/emit" -H 'X-Api-Key: demo-key' \
  -H 'Content-Type: application/json' -d '{"n":"1"}'
sleep 3
docker logs outages-capture
```

```text
200
```

## 4. Stop the broker, send during the outage, start it again

```bash
docker stop outages-broker
until docker logs outages-capture 2>&1 | grep -q '"orders-in".*"down"'; do sleep 1; done
curl -s -o /dev/null -w '%{http_code}\n' -X POST "localhost:$PORT/channels/emit" -H 'X-Api-Key: demo-key' \
  -H 'Content-Type: application/json' -d '{"n":"2"}'
until docker logs outages-capture 2>&1 | grep -q '"orders-out".*"down"'; do sleep 1; done
sleep 5
docker start outages-broker
until [ "$(docker logs outages-capture 2>&1 | grep -c '"up"')" -ge 2 ]; do sleep 1; done
docker logs outages-capture
```

The send during the outage is accepted (`200`): it waits in the outbox and goes out once the broker
is back. What the package recorded:

```text
{"code":"SUTRA.INBOUND.AMQP.OUTAGE_STARTED","channel":"orders-in","direction":"inbound","state":"down","startedAt":"2026-11-19T09:15:09.146540438Z"}
{"code":"SUTRA.OUTBOUND.AMQP.OUTAGE_STARTED","channel":"orders-out","direction":"outbound","state":"down","startedAt":"2026-11-19T09:15:09.521903086Z"}
{"code":"SUTRA.OUTBOUND.AMQP.OUTAGE_ENDED","channel":"orders-out","direction":"outbound","state":"up","startedAt":"2026-11-19T09:15:09.521903086Z","endedAt":"2026-11-19T09:15:21.185206554Z","ms":11663}
{"code":"SUTRA.INBOUND.AMQP.OUTAGE_ENDED","channel":"orders-in","direction":"inbound","state":"up","startedAt":"2026-11-19T09:15:09.146540438Z","endedAt":"2026-11-19T09:15:24.169169142Z","ms":15022}
```

Two windows, one per channel. The intake's started when it lost its connection and ended when it
was attached again; the send channel's started at the send that could not connect and ended at the
outbox's retry that reached the broker. Each `up` repeats its `startedAt`, so the two messages of a
window pair up. The engine's own log has the same four changes with the same codes and times:

```bash
docker logs outages-engine 2>&1 | grep -E 'OUTAGE_(STARTED|ENDED)'
```

```text
{"channel":"orders-in","code":"SUTRA.INBOUND.AMQP.OUTAGE_STARTED","outage_started":"2026-11-19T09:15:09.146540438Z",...,"level":"WARN",...}
{"code":"SUTRA.OUTBOUND.AMQP.OUTAGE_STARTED","broker":"outages-broker:5672","destination":"amqp10://outages-broker/orders.out","outage_started":"2026-11-19T09:15:09.521911293Z",...,"level":"WARN",...}
{"code":"SUTRA.OUTBOUND.AMQP.OUTAGE_ENDED","broker":"outages-broker:5672","destination":"amqp10://outages-broker/orders.out","outage_started":"2026-11-19T09:15:09.521911293Z","outage_ended":"2026-11-19T09:15:21.185210334Z","outage_ms":11663,...,"level":"INFO",...}
{"channel":"orders-in","code":"SUTRA.INBOUND.AMQP.OUTAGE_ENDED","outage_started":"2026-11-19T09:15:09.146540438Z","outage_ended":"2026-11-19T09:15:24.169169142Z","outage_ms":15022,...,"level":"INFO",...}
```

How long each window lasts depends on the broker's restart and on the intake's reconnect backoff,
which waits up to 30 s between attempts, so it differs from run to run.

## 5. Clean up

```bash
docker rm -f outages-engine outages-db outages-broker outages-capture
docker network rm outages-net
```
