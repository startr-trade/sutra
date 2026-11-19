# Answering with a status and headers

Since 0.8.0 a flow that answers a synchronous HTTP request chooses the status and the response
headers, so it can behave like a REST API (201 with `Location`, 404, 409) or like an auth check for
a reverse proxy (200 with identity headers, or 401 with a challenge). The reference is
[Status and response headers](q-namespace.md#status-and-response-headers-080). This walkthrough
deploys one package with four endpoints and calls each with `curl`.

You need Docker, `curl`, the `sutra` CLI and an empty working directory. It was run as written with
the engine built from this release and `postgres:16-alpine`; the output shown is from that run.

## 1. Write the package

`replies/package.yaml`:

```yaml
labels:
  "tenant": "default"
  "module": "replies"
  "version": "1.0.0"
engine:
  minContract: 1
```

`replies/channels.yaml`: four synchronous HTTP channels with the same key on each:

```yaml
channels:
  - name: items
    transport: http
    bind: "POST /channels/items"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: env:REPLIES_API_KEY
        header: X-Api-Key
  - name: lookup
    transport: http
    bind: "POST /channels/lookup"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: env:REPLIES_API_KEY
        header: X-Api-Key
  - name: check
    transport: http
    bind: "POST /channels/check"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: env:REPLIES_API_KEY
        header: X-Api-Key
  - name: bad
    transport: http
    bind: "POST /channels/bad"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: env:REPLIES_API_KEY
        header: X-Api-Key
```

`replies/templates/item.hbs`:

```text
{"id":"{{req.id}}"}
```

`replies/bpmn/reply-status.bpmn`, four processes:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:replies:1.0.0">
  <!-- 201 with a Location -->
  <bpmn:process id="create" isExecutable="true">
    <bpmn:startEvent id="CreateStart">
      <bpmn:extensionElements><q:source channel="items" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="C1" sourceRef="CreateStart" targetRef="Create"/>
    <bpmn:serviceTask id="Create" implementation="item.hbs">
      <bpmn:extensionElements>
        <q:reply contentType="application/json" status="201">
          <q:header name="Location" value="'/items/' + req.id"/>
          <q:header name="X-Request-Id" value="req.id"/>
        </q:reply>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="C2" sourceRef="Create" targetRef="CreateDone"/>
    <bpmn:endEvent id="CreateDone"/>
  </bpmn:process>

  <!-- a status chosen by FEEL -->
  <bpmn:process id="lookup" isExecutable="true">
    <bpmn:startEvent id="LookupStart">
      <bpmn:extensionElements><q:source channel="lookup" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="L1" sourceRef="LookupStart" targetRef="Lookup"/>
    <bpmn:serviceTask id="Lookup" implementation="item.hbs">
      <bpmn:extensionElements>
        <q:reply contentType="application/json" status="if req.id = '42' then 200 else 404"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="L2" sourceRef="Lookup" targetRef="LookupDone"/>
    <bpmn:endEvent id="LookupDone"/>
  </bpmn:process>

  <!-- an auth check: no body, 200 with identity headers or 401 with a challenge -->
  <bpmn:process id="check" isExecutable="true">
    <bpmn:startEvent id="CheckStart">
      <bpmn:extensionElements><q:source channel="check" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="K1" sourceRef="CheckStart" targetRef="CheckDone"/>
    <bpmn:endEvent id="CheckDone">
      <bpmn:extensionElements>
        <q:reply status="if req.token = 'good' then 200 else 401">
          <q:header name="X-User-Id" value="if req.token = 'good' then req.user else null"/>
          <q:header name="WWW-Authenticate" value="if req.token = 'good' then null else 'Bearer realm=&quot;sim&quot;'"/>
        </q:reply>
      </bpmn:extensionElements>
    </bpmn:endEvent>
  </bpmn:process>

  <!-- whatever status the caller sends: shows the refusal -->
  <bpmn:process id="bad" isExecutable="true">
    <bpmn:startEvent id="BadStart">
      <bpmn:extensionElements><q:source channel="bad" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="B1" sourceRef="BadStart" targetRef="BadDone"/>
    <bpmn:endEvent id="BadDone">
      <bpmn:extensionElements><q:reply status="req.code"/></bpmn:extensionElements>
    </bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
```

`create` answers 201 with two headers and the template's body. `lookup` picks its status with FEEL.
`check` renders no body: its end event answers 200 with `X-User-Id` for the right token, else 401
with `WWW-Authenticate`; a header whose value is `null` is left out. `bad` sends whatever status the
caller asks for, to show what happens when it cannot be sent.

## 2. Check it

```bash
sutra lint replies/
```

Only the schema-less-channel warnings appear:

```text
[WARN] SUTRA.CHANNEL.NO_SCHEMA — deployment default/replies/1.0.0 (dep-ffffffffffffffffffffffff): channel 'items' has 1 subscriber(s) but its codec (json) declares no message types — ...
...
0 error(s), 4 warning(s)
```

A literal status is checked here, before anything is deployed. Change `lookup`'s status to
`status="302"` and lint refuses it, because a redirect needs a `Location`; `status="99"` is refused
because a 1xx is not a final answer:

```text
[ERROR] SUTRA.OUTBOUND.REPLY_STATUS_INVALID — deployment default/replies/1.0.0 (dep-ffffffffffffffffffffffff): process 'lookup' node 'Lookup' <q:reply>: status 302 is a redirect, which needs a Location header (declare <q:header name="Location">, and make sure it is not null).
1 error(s), 4 warning(s)
```

```text
[ERROR] SUTRA.OUTBOUND.REPLY_STATUS_INVALID — deployment default/replies/1.0.0 (dep-ffffffffffffffffffffffff): process 'lookup' node 'Lookup' <q:reply>: status 99 is outside 200-599; a reply is a final answer, so a 1xx is refused too.
1 error(s), 4 warning(s)
```

Change it back.

## 3. Package it and start the engine

```bash
mkdir -p deployments
sutra package replies --out deployments
chmod -R a+rX deployments
docker network create replies-net
docker run -d --name replies-db --network replies-net -e POSTGRES_PASSWORD=dev-only postgres:16-alpine
until docker exec replies-db pg_isready -U postgres -h 127.0.0.1; do sleep 1; done
sleep 2
docker exec replies-db psql -U postgres \
  -c "CREATE ROLE sutra_engine LOGIN PASSWORD 'dev-only' NOSUPERUSER NOBYPASSRLS" \
  -c "ALTER SCHEMA public OWNER TO sutra_engine"
docker run -d --name replies-engine --network replies-net -p 127.0.0.1::8080 \
  -v "$PWD/deployments:/deployments:ro" -e SUTRA_DEPLOYMENTS_DIR=/deployments \
  -e SUTRA_DATASOURCE_URL=postgres://replies-db:5432/postgres \
  -e SUTRA_DATASOURCE_USERNAME=sutra_engine -e SUTRA_DATASOURCE_PASSWORD=dev-only \
  -e REPLIES_API_KEY=demo-key \
  ghcr.io/startr-trade/sutra:0.8.1
PORT=$(docker port replies-engine 8080 | head -1 | cut -d: -f2)
until curl -sf "http://127.0.0.1:$PORT/sutra/health/ready"; do sleep 1; done
```

```json
{"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deployments":1,"shards":1}}]}
```

## 4. Call it

A create answers 201 with its `Location`:

```bash
curl -si -X POST "localhost:$PORT/channels/items" -H 'X-Api-Key: demo-key' \
  -H 'Content-Type: application/json' -d '{"id":"7"}'
```

```text
HTTP/1.1 201 Created
content-type: application/json
location: /items/7
x-request-id: 7
content-length: 11
date: Thu, 19 Nov 2026 09:13:19 GMT

{"id":"7"}
```

A lookup answers 200 for `{"id":"42"}` and 404 for anything else, with the flow's body either way:

```bash
curl -si -X POST "localhost:$PORT/channels/lookup" -H 'X-Api-Key: demo-key' \
  -H 'Content-Type: application/json' -d '{"id":"41"}'
```

```text
HTTP/1.1 404 Not Found
content-type: application/json
content-length: 12
date: Thu, 19 Nov 2026 09:13:19 GMT

{"id":"41"}
```

The auth check, with the right token and then a wrong one:

```bash
curl -si -X POST "localhost:$PORT/channels/check" -H 'X-Api-Key: demo-key' \
  -H 'Content-Type: application/json' -d '{"token":"good","user":"u-1"}'
curl -si -X POST "localhost:$PORT/channels/check" -H 'X-Api-Key: demo-key' \
  -H 'Content-Type: application/json' -d '{"token":"bad"}'
```

```text
HTTP/1.1 200 OK
content-type: application/json
x-user-id: u-1
content-length: 0
date: Thu, 19 Nov 2026 09:13:19 GMT

HTTP/1.1 401 Unauthorized
content-type: application/json
www-authenticate: Bearer realm="sim"
content-length: 0
date: Thu, 19 Nov 2026 09:13:19 GMT
```

Each answer carries only the header whose value was not `null`. A reverse proxy that runs a
subrequest per call (an `auth_request`-style check) can pass `X-User-Id` on to the upstream.

## 5. A status that cannot be sent

A computed status is checked when the answer is sent. `bad` with `{"code":700}`:

```bash
curl -si -X POST "localhost:$PORT/channels/bad" -H 'X-Api-Key: demo-key' \
  -H 'Content-Type: application/json' -d '{"code":700}'
```

```text
HTTP/1.1 500 Internal Server Error
content-type: application/problem+json
content-length: 355
date: Thu, 19 Nov 2026 09:13:19 GMT

{"type":"urn:bpm:diag:SUTRA.OUTBOUND.REPLY_STATUS_INVALID","title":"SUTRA.OUTBOUND.REPLY_STATUS_INVALID","status":500,"detail":"The reply of process 'bad' at node 'BadDone': status 700 is outside 200-599; a reply is a final answer, so a 1xx is refused too.","code":"SUTRA.OUTBOUND.REPLY_STATUS_INVALID","attributes":{"nodeId":"BadDone","processId":"bad"}}
```

The same call with `{"code":409}` answers `HTTP/1.1 409 Conflict` with an empty body. A header value
with a line break or another control character is refused the same way, with
`SUTRA.OUTBOUND.REPLY_HEADER_INVALID`; the problem names the header, never its value.

## 6. Clean up

```bash
docker rm -f replies-engine replies-db
docker network rm replies-net
```

## Where it does not apply

- An `ack-mode: on-persist` channel always answers 202, the acceptance; the reply's headers are
  added to it.
- A broker intake (Kafka, AMQP, …) has no status.
- `sutra lint` warns about both with `SUTRA.CHANNEL.REPLY_STATUS_IGNORED`.
- A `<q:reply>` with a `destination` goes out of band, and its headers travel with that message.
