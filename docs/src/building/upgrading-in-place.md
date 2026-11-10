# Upgrading a deployment in place

When a new version of a tenant and module takes over, the old version drains: its running instances
finish on it. Since 0.6.20, a channel only the old version declares stays served by it, starts
included, until it retires at zero running instances. Another owner cannot take such an endpoint
meanwhile.

## The packages

- `orders` 1.0.0: `start` (`POST /orders`) parks an order on a receive task waiting on `release`
  (`POST /orders/release`, correlated by `id`). A second process starts on `legacy`
  (`POST /orders/legacy`) and replies `legacy handled by orders 1.0.0`.
- `orders` 2.0.0: keeps `start`, waits on `confirm` (`POST /orders/confirm`) instead, and has no
  `release` and no `legacy`.
- `audit` 1.0.0, another module (another owner), binds `POST /orders/release`.

### Steps

1. Package and deploy `orders` 1.0.0; start two orders, `A-1` and `A-2`. Both park on `release`.
2. Deploy `orders` 2.0.0. `SUTRA.DEPLOY.ENDPOINT.SUPERSEDED` says 1.0.0 drains, its instances
   finish on it, and names the endpoints it keeps: `POST /orders/release, POST /orders/legacy`.
3. `GET /sutra/deployments`: 1.0.0 is under `draining`, with 2 running instances under
   `drainingInstances`.
4. `POST /orders/legacy` still runs, on 1.0.0.
5. Deploy `audit`: refused with `409`, `SUTRA.DEPLOY.ENDPOINT.COLLISION`, naming 1.0.0 and its 2
   running instances. Nothing is stored.
6. Release `A-1` and `A-2` on `POST /orders/release`: both finish on 1.0.0.
7. On the next sweep 1.0.0 retires: `draining` is empty, and `POST /orders/legacy` is `404`.
8. Deploy `audit` again: it lands.
9. `sutra lint` on a package that sends `q:send destination="local://acme/orders/2.0.0/start"`:
   `SUTRA.CONFIG.SEND.LOCAL_DESTINATION_CROSSES_PACKAGE`, exit 1. Send on an outbound channel the
   other package listens on instead.

```text
$ sutra package orders-1 --out out1
packaged out1/orders-1.sutra (deploymentId dep-9f805df1cff68d20777326e9)
[exit 0]

$ sutra deploy out1/orders-1.sutra --api --engine-url http://localhost:8080
deployed dep-9f805df1cff68d20777326e9 (slot acme--orders--1.0.0, rev 1) — Active
[exit 0]

$ curl -X POST /orders -H 'X-API-Key: walk-key' -d '{"id":"A-1"}'
 [HTTP 200]

$ curl -X POST /orders -H 'X-API-Key: walk-key' -d '{"id":"A-2"}'
 [HTTP 200]

$ sutra package orders-2 --out out2
packaged out2/orders-2.sutra (deploymentId dep-200e1644e8045c4c02c0e3d0)
[exit 0]

$ sutra deploy out2/orders-2.sutra --api --engine-url http://localhost:8080
[WARN] SUTRA.DEPLOY.ENDPOINT.SUPERSEDED — deployment acme--orders--2.0.0 takes over from acme--orders--1.0.0, an earlier deployment of the same tenant and module. acme--orders--1.0.0 drains: it takes no new work, and its running instances finish on it. Channels only it declares stay served by it until it retires: POST /orders/release, POST /orders/legacy.
deployed dep-200e1644e8045c4c02c0e3d0 (slot acme--orders--2.0.0, rev 1) — Active
[exit 0]

$ curl /sutra/deployments
{"active":[{"deploymentId":"dep-200e1644e8045c4c02c0e3d0","slot":"acme--orders--2.0.0","phase":"Active","ready":true}],"pending":[],"draining":["dep-9f805df1cff68d20777326e9"],"drainingInstances":{"dep-9f805df1cff68d20777326e9":2},"failed":[]}

$ curl -X POST /orders/legacy -H 'X-API-Key: walk-key' -d '{}'
legacy handled by orders 1.0.0
 [HTTP 200]

$ sutra package audit --out outa
packaged outa/audit.sutra (deploymentId dep-dbd1c12988671263d273fbe1)
[exit 0]

$ sutra deploy outa/audit.sutra --api --engine-url http://localhost:8080
[ERROR] SUTRA.DEPLOY.ENDPOINT.COLLISION — engine rejected the deployment (HTTP 409 Conflict): {"code":"SUTRA.DEPLOY.ENDPOINT.COLLISION","error":"[SUTRA.DEPLOY.ENDPOINT.COLLISION] http endpoint 'POST /orders/release' stays with draining deployment acme--orders--1.0.0 (2 running instances) until it retires"}
[exit 1]

$ curl -X POST /orders/release -H 'X-API-Key: walk-key' -d '{"id":"A-1"}'
 [HTTP 200]

$ curl -X POST /orders/release -H 'X-API-Key: walk-key' -d '{"id":"A-2"}'
 [HTTP 200]

$ # wait for the sweep

$ curl /sutra/deployments
{"active":[{"deploymentId":"dep-200e1644e8045c4c02c0e3d0","slot":"acme--orders--2.0.0","phase":"Active","ready":true}],"pending":[],"draining":[],"drainingInstances":{},"failed":[]}

$ curl -X POST /orders/legacy -H 'X-API-Key: walk-key' -d '{}'
{"type":"urn:bpm:diag:SUTRA.RESOLVE.CHANNEL.UNKNOWN","title":"SUTRA.RESOLVE.CHANNEL.UNKNOWN","status":404,"detail":"No channel bound at 'POST /orders/legacy'","code":"SUTRA.RESOLVE.CHANNEL.UNKNOWN"} [HTTP 404]

$ sutra deploy outa/audit.sutra --api --engine-url http://localhost:8080
deployed dep-dbd1c12988671263d273fbe1 (slot acme--audit--1.0.0, rev 1) — Active
[exit 0]

$ sutra lint crossing
[ERROR] SUTRA.CONFIG.SEND.LOCAL_DESTINATION_CROSSES_PACKAGE — deployment acme/sender/1.0.0 (dep-ffffffffffffffffffffffff): process 'send' node 'Send' sends to 'local://acme/orders/2.0.0/start', which is not a 'transport: local' inbound channel of this package (local://acme/sender/1.0.0/<channel>). A local address does not cross packages, versions of one package included: send on an outbound channel the other package listens on
1 error(s), 1 warning(s)
[exit 1]
```

### If you undeploy the new version

Undeploying 2.0.0 gives 1.0.0 back every channel it declares, the ones it shared with 2.0.0
included, until 1.0.0 retires. Here a new order `C-1` on the shared `POST /orders` starts on 1.0.0
(the admin instance list for 1.0.0 counts 2), both orders finish on it, and once it retires
`POST /orders` is `404`, because nothing serves it any more.

```text
$ sutra package orders-1 --out out1
packaged out1/orders-1.sutra (deploymentId dep-9f805df1cff68d20777326e9)
[exit 0]

$ sutra deploy out1/orders-1.sutra --api --engine-url http://localhost:8080
deployed dep-9f805df1cff68d20777326e9 (slot acme--orders--1.0.0, rev 1) — Active
[exit 0]

$ curl -X POST /orders -H 'X-API-Key: walk-key' -d '{"id":"A-1"}'
 [HTTP 200]

$ sutra package orders-2 --out out2
packaged out2/orders-2.sutra (deploymentId dep-200e1644e8045c4c02c0e3d0)
[exit 0]

$ sutra deploy out2/orders-2.sutra --api --engine-url http://localhost:8080
[WARN] SUTRA.DEPLOY.ENDPOINT.SUPERSEDED — deployment acme--orders--2.0.0 takes over from acme--orders--1.0.0, an earlier deployment of the same tenant and module. acme--orders--1.0.0 drains: it takes no new work, and its running instances finish on it. Channels only it declares stay served by it until it retires: POST /orders/release, POST /orders/legacy.
deployed dep-200e1644e8045c4c02c0e3d0 (slot acme--orders--2.0.0, rev 1) — Active
[exit 0]

$ sutra undeploy acme--orders--2.0.0 --api --engine-url http://localhost:8080
undeployed slot acme--orders--2.0.0 — Draining
[exit 0]

$ curl /sutra/deployments
{"active":[],"pending":[],"draining":["dep-9f805df1cff68d20777326e9"],"drainingInstances":{"dep-9f805df1cff68d20777326e9":1},"failed":[]}

$ curl -X POST /orders -H 'X-API-Key: walk-key' -d '{"id":"C-1"}'
 [HTTP 200]

$ # instances on 1.0.0 (dep-9f805df1cff68d20777326e9)
2

$ curl -X POST /orders/release -H 'X-API-Key: walk-key' -d '{"id":"A-1"}'
 [HTTP 200]

$ curl -X POST /orders/release -H 'X-API-Key: walk-key' -d '{"id":"C-1"}'
 [HTTP 200]

$ # wait for the sweep

$ curl /sutra/deployments
{"active":[],"pending":[],"draining":[],"drainingInstances":{},"failed":[]}

$ curl -X POST /orders -H 'X-API-Key: walk-key' -d '{"id":"D-1"}'
{"type":"urn:bpm:diag:SUTRA.RESOLVE.CHANNEL.UNKNOWN","title":"SUTRA.RESOLVE.CHANNEL.UNKNOWN","status":404,"detail":"No channel bound at 'POST /orders'","code":"SUTRA.RESOLVE.CHANNEL.UNKNOWN"} [HTTP 404]
```

### What to watch for

- A version that keeps starting work on the channels it kept never retires. Watch
  `drainingInstances`.
- In a deployments folder, removing 1.0.0's file stops its channels.
- Versions already draining when you upgrade the engine to 0.6.20 stop serving as before.
- `SUTRA.DEPLOY.ENDPOINT.CHANNEL_CHANGED` and `START_ON_WAIT` warn at deploy when 2.0.0 changes a
  channel 1.0.0 has instances parked on (not shown in this run; the tier-2 suite covers them).

See also [One endpoint, one deployment](endpoint-claims.md), which covers how the other deployment paths behave.
