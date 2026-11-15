# A reply answers the instance waiting for it

From 0.6.22, when a message arrives on a channel, the engine first asks whether it answers an
instance that is parked waiting for it, and only then whether it starts a new one. This holds with
one version of a package and across versions while an older one drains:

- A message on a channel with both a start event and a wait resumes the instance it correlates to,
  and starts nothing. An uncorrelated message starts, as before.
- While version 1 drains, a reply for one of its parked instances resumes that instance on version
  1, even when version 2 has a start event on the channel, and it is processed with version 1's own
  codec, validators and reply.
- A retry of a message that ran on version 1 is a duplicate on version 2.

This walkthrough shows the first with one package; the tier-2 suite covers the other two. It needs the setup of
[One endpoint, one deployment](endpoint-claims.md#before-you-start): an engine on the `db`
deployment source, a database role that can't bypass row-level security, and an admin key.

## The package

`orders/bpmn/flow.bpmn` has one process. It starts on `order-in`, keeps the order's key as its
correlation alias, and parks on `order-in` again until a message with the same key arrives:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="Definitions_orders"
                  targetNamespace="urn:sutra:module:orders:1.0.0">
  <bpmn:process id="order" name="Order" isExecutable="true">
    <bpmn:startEvent id="Received">
      <bpmn:extensionElements>
        <q:source channel="order-in"/>
        <q:alias name="orderKey" expression="payload.key" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Received" targetRef="AwaitConfirmation"/>
    <bpmn:receiveTask id="AwaitConfirmation" name="Await confirmation">
      <bpmn:extensionElements>
        <q:source channel="order-in" name="confirmation"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f2" sourceRef="AwaitConfirmation" targetRef="Done"/>
    <bpmn:endEvent id="Done"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
```

`orders/channels.yaml` binds the channel once:

```yaml
channels:
  - name: order-in
    transport: http
    bind: "POST /orders"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: env:ORDERS_KEY
```

`sutra lint orders` warns `SUTRA.CONFIG.CHANNEL.START_AND_WAIT`:

```text
[WARN] SUTRA.CONFIG.CHANNEL.START_AND_WAIT — deployment acme/orders/1.0.0 (dep-…): process 'order' start event 'Received' and the wait at order/AwaitConfirmation are both on channel 'order-in' with message types that can overlap. A message that correlates to a parked instance answers its wait and starts nothing, so a repeated key never starts a second instance; give them disjoint message types or separate channels if one is meant
```

That is what this walkthrough relies on; the warning is there for a flow that meant to start a
second instance with a repeated key. `package.yaml` labels the package `acme`/`orders`/`1.0.0`, and
the engine has `ORDERS_KEY` set to the key the channel checks.

## 1. Deploy and start one order

```console
$ sutra package orders --out out-orders
packaged out-orders/orders.sutra (deploymentId dep-<id>)
$ sutra deploy out-orders/orders.sutra --api --engine-url http://localhost:8080
deployed dep-<id> (slot acme--orders--1.0.0, rev 1) — Active
$ curl -X POST http://localhost:8080/orders -H 'X-API-Key: <your key>' \
    -H 'Content-Type: application/json' -d '{"key":"A-1"}'
```

The instance parks at `AwaitConfirmation`, findable by its key:

```console
$ curl -s -H "X-API-Key: $SUTRA_ADMIN_API_KEY" 'http://localhost:8080/sutra/instances?alias=orderKey:A-1'
{"instances":[{"instanceId":"…","deploymentId":"dep-<id>","status":"SUSPENDED"}]}
```

## 2. Send the confirmation

```console
$ curl -X POST http://localhost:8080/orders -H 'X-API-Key: <your key>' \
    -H 'Content-Type: application/json' -d '{"key":"A-1"}'
```

The same channel, the same key: the message resumes the parked instance, which completes. No second
instance starts: the same query now answers `{"instances":[]}`.

## 3. Start another order

```console
$ curl -X POST http://localhost:8080/orders -H 'X-API-Key: <your key>' \
    -H 'Content-Type: application/json' -d '{"key":"B-2"}'
```

No instance waits for `B-2`, so this starts a new one, which parks as in step 1:
`/sutra/instances?alias=orderKey:B-2` lists it, `SUSPENDED`.

## Broadcast channels

On a channel with `broadcast: true`, a correlated message resumes the process that waits for it,
and that process does not also start. Every other process with a start event on the channel still
receives the message, as broadcast promises.

## What it costs

On a channel no version waits on, nothing: the engine knows from its index which channels have
waits, and answers in a few hash lookups. On a channel with a wait that can take the message's type,
each message costs one correlation lookup per live version that waits. A message whose type no wait
accepts (for example a start type and a reply type on one channel) is never looked up.

## Upgrading to 0.6.22

- A flow that re-sent a business key on a start-and-wait channel to spawn a second instance now
  answers the parked one.
- A message whose key names a running instance that has not reached the wait yet is refused with a
  retryable `WAIT_PENDING` answer, not started.
- An engine database runs two additive migrations at first boot: V305 and V306. V306 builds an
  index on `inbox_seen` concurrently; it adds I/O in proportion to the table and blocks nothing. If
  the engine connects with a non-owning role, grant it `SELECT, INSERT` on the new
  `deployment_owner` table.
- Dedup records written before the upgrade dedupe within their own deployment only, so a retry that
  spans both the engine upgrade and a version flip can run again.
