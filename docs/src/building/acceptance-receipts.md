# Acceptance receipts

An HTTP channel with `ack-mode: on-persist` answers `202 Accepted` before the work finishes. Since 0.6.18
the engine tells the caller what it accepted, so nobody has to poll the instance list and match a variable.
This walkthrough starts an instance, reads its receipt, repeats the request, and finds the instance again by
its alias.

You need the engine and a PostgreSQL database; this was run on PostgreSQL 16. Only the HTTP path was run.
Queue transports have no synchronous answer, so they get no receipt.

## 1. Write the package

`package.yaml`:

```yaml
labels:
  "tenant": "default"
  "module": "cases"
  "version": "1.0.0"
engine:
  minContract: 1
```

`channels.yaml`. `Idempotency-Key` is the header that makes a retry the same request:

```yaml
channels:
  - name: case-open
    transport: http
    bind: "POST /channels/case-open"
    codec: json
    ack-mode: on-persist
    idempotency-key-header: Idempotency-Key
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: env:CASES_API_KEY
        header: X-Api-Key
  - name: case-release
    transport: http
    bind: "POST /channels/case-release"
    codec: json
    ack-mode: on-persist
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: env:CASES_API_KEY
        header: X-Api-Key
```

`bpmn/case-open.bpmn`. `<q:receipt>` names the values this flow chooses to expose. Without it, the receipt
carries no flow values of its own:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_case_open"
                  targetNamespace="urn:sutra:module:cases:1.0.0">
  <bpmn:process id="case-open" isExecutable="true">
    <bpmn:extensionElements>
      <q:variables>
        <q:variable name="holderId" type="string" sensitive="true"/>
      </q:variables>
      <q:receipt>
        <q:attribute name="caseId" expression="req.caseId"/>
        <q:attribute name="holder" expression="holderId"/>
      </q:receipt>
    </bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="case-open" name="req"/>
        <q:alias name="orderId" expression="req.orderId" unique="true" onConflict="correlate"/>
        <q:alias name="holderId" expression="req.holderId"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Release"/>
    <bpmn:receiveTask id="Release">
      <bpmn:extensionElements><q:source channel="case-release" name="release"/></bpmn:extensionElements>
      <bpmn:incoming>F1</bpmn:incoming><bpmn:outgoing>F2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="F2" sourceRef="Release" targetRef="Done"/>
    <bpmn:endEvent id="Done"><bpmn:incoming>F2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
```

## 2. Lint and package it

```bash
sutra lint cases
sutra package cases -o deployments
```

Lint warns about the `holder` attribute, because it reads a sensitive variable and the caller would only
ever see the mask. It is kept here to show the masking. A real flow would drop it.

```text
[WARN] SUTRA.CONFIG.RECEIPT.ATTRIBUTE_SENSITIVE — ... process 'case-open' <q:receipt> attribute 'holder' reads sensitive variable 'holderId', so every receipt shows it as ***REDACTED***. Expose a value that is safe for the caller to see, or remove the attribute.
```

## 3. Start the engine

Give the engine a PostgreSQL role that isn't a superuser, and a database that role owns:

```sql
CREATE ROLE sutra_app LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '<a password>';
CREATE DATABASE sutra OWNER sutra_app;
```

Start the engine with `SUTRA_DATASOURCE_URL` pointing at that database as `sutra_app`,
`SUTRA_DEPLOYMENTS_DIR` pointing at `deployments`, and `CASES_API_KEY`. Turn admin auth on with
`SUTRA_ADMIN_AUTH_SCHEME=apikey`, `SUTRA_ADMIN_AUTH_KEY_REF=env:SUTRA_ADMIN_API_KEY` and
`SUTRA_ADMIN_API_KEY`, because step 6 reads instances. Because this role owns its tables, the engine logs
`SUTRA.STARTUP.RLS_INERT_POSTURE` at startup: row-level security is on but doesn't apply to the table owner.
Before running several tenants on one database, give the engine a role that doesn't own its tables. Set `B` to
the engine's address, then wait for it:

```bash
curl -s "$B/sutra/health/ready"
```

```json
{"status":"UP","checks":[{"name":"sutra-loader","status":"UP","data":{"deployments":1,"shards":1}}]}
```

## 4. Start a case and read the receipt

```bash
curl -si -X POST "$B/channels/case-open" \
  -H 'X-Api-Key: <your key>' -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: try-1' \
  -d '{"orderId":"O-100","caseId":"C-7","holderId":"H-SECRET"}'
```

```text
HTTP/1.1 202 Accepted
content-type: application/json
sutra-instance-id: 315001d4-74f0-42bc-8b97-8dabe7511b35
sutra-receipt: {"deploymentId":"dep-dceba0e28896cca13aa92d0a","labels":{"module":"cases","tenant":"default","version":"1.0.0"},"duplicate":false,"instanceId":"315001d4-74f0-42bc-8b97-8dabe7511b35","aliases":{"holderId":"***REDACTED***","orderId":"O-100"},"attributes":{"caseId":"C-7","holder":"***REDACTED***"},"idempotencyKey":"try-1"}

{"deploymentId":"dep-dceba0e28896cca13aa92d0a","labels":{"module":"cases","tenant":"default","version":"1.0.0"},"duplicate":false,"instanceId":"315001d4-74f0-42bc-8b97-8dabe7511b35","aliases":{"holderId":"***REDACTED***","orderId":"O-100"},"attributes":{"caseId":"C-7","holder":"***REDACTED***"},"idempotencyKey":"try-1"}
```

The flow replied nothing, so the receipt is also the body. A flow that replies with
`<q:reply continue="true">` keeps its own body byte for byte, and the receipt rides only in the headers.
`H-SECRET` appears nowhere: `holderId` is declared `sensitive`, so both the alias and the attribute show the
mask. An alias built from the payload does show that value to the caller, so alias what you are willing to
return.

## 5. Send it again

Repeat the same request, with the same `Idempotency-Key`:

```text
HTTP/1.1 202 Accepted
content-type: application/json
sutra-instance-id: 315001d4-74f0-42bc-8b97-8dabe7511b35

{"deploymentId":"dep-dceba0e28896cca13aa92d0a","labels":{"module":"cases","tenant":"default","version":"1.0.0"},"duplicate":true,"instanceId":"315001d4-74f0-42bc-8b97-8dabe7511b35","originalInstanceId":"315001d4-74f0-42bc-8b97-8dabe7511b35","aliases":{},"attributes":{},"idempotencyKey":"try-1"}
```

Nothing new started. The answer names the instance the first request started. It has no aliases or
attributes because nothing ran to produce them. A duplicate on any HTTP channel, including an `on-complete`
one, gets this `202`.

## 6. Find the instance after losing the 202

With admin auth on, reading instances takes the admin key. Without it, or with a wrong one, the engine refuses:

```bash
curl -si "$B/sutra/instances?alias=orderId:O-100"
```

```text
HTTP/1.1 401 Unauthorized

{"error":"unauthorized","reason":"missing or invalid admin auth key/secret"}
```

Send the key, in `X-API-Key` unless you set `SUTRA_ADMIN_AUTH_HEADER`:

```bash
curl -s -H "X-API-Key: $SUTRA_ADMIN_API_KEY" "$B/sutra/instances?alias=orderId:O-100"
```

```json
{"instances":[{"instanceId":"315001d4-74f0-42bc-8b97-8dabe7511b35","deploymentId":"dep-dceba0e28896cca13aa92d0a","status":"SUSPENDED"}]}
```

An alias nothing is bound to gives `{"instances":[]}`. The lookup finds live instances. A finished instance's
aliases have retired, so add `includeTerminal=true` to find it. That form walks the deployment's alias rows, so
don't poll it. The same gate covers the plain list and `GET /sutra/instances/{id}`; see
[Upgrading to 0.6.18](../operating/upgrading-0-6-18.md#reading-instances-takes-the-admin-key) for the case where
admin auth is off.

## What to know

- `idempotencyKey` appears only when the caller supplied a key or a `<q:source dedupKey>` derived one. Without
  one, a repeated request starts a new instance, so nothing is echoed.
- Attribute values are strings. A null is left out. An expression that fails to evaluate is left out and
  logged, because the work is already accepted.
- `Sutra-Receipt` is one line of ASCII JSON. Past 4 KiB it keeps only the identifying fields and says
  `"truncated": true`. `Sutra-Instance-Id` is always there.
- A duplicate names no instance if the first delivery ended without one, or was recorded before 0.6.18.
- Two deliveries racing before either commits behave as before: the loser gets
  `SUTRA.INBOUND.DUPLICATE_CONCURRENT`.
- A `<q:attribute>` with a repeated name is the lint error `SUTRA.CONFIG.RECEIPT.ATTRIBUTE_DUPLICATE`; one
  missing its name or expression fails at load.
