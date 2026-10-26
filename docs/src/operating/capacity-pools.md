# Capacity pools and sessions

Some limits are not per channel or per tenant but physical: a number of lines, licences, seats or
connections to a downstream system, shared by every deployment that uses them. 0.6 adds two
things for that:

- **Capacity pools**: named, cluster-wide counts of slots, declared in the engine configuration,
  which inbound and outbound channels of any deployment draw from with `pool: <name>`.
- **`session-end:`**: an inbound channel can end an instance's session (give back its slots)
  before the instance reaches its end, while the instance runs on.

## Active is not incomplete

`max-concurrent-instances` on an inbound channel caps how many of the instances it admitted may
be active at once. With `use-only-in-flight-for-concurrency-cap: false`, a parked instance
(waiting on a user task, a model job, a reply) still counts. Normally an instance stops counting
when it ends. `session-end:` lets it stop earlier, for a flow whose session (a connection, a
conversation, a line) can finish before its follow-up work does:

```yaml
channels:
  - name: session-start
    transport: http
    bind: "POST /sessions"
    max-concurrent-instances: 200
    use-only-in-flight-for-concurrency-cap: false
    pool: lines                      # optional: also draw from a capacity pool
    pool-call-key: header.x-call-id  # optional: which call a message belongs to
    session-end:
      on-message: session-closed     # a correlated message on this channel ends the session
      on-reply: true                 # and/or: the flow's reply on session-start ends it
  - name: session-closed
    transport: http
    bind: "POST /sessions/closed"
```

| Key | Meaning |
|---|---|
| `session-end.on-message` | Another **inbound** channel of the same file. A message on it is correlated to an instance through the `<q:alias onConflict="correlate">` of the process that starts on the session channel. When the session channel declares `pool-call-key: header.<name>` and the message carries that header, only the call it names is ended; otherwise the instance's whole session is released. If a wait in the flow subscribes to the channel too, the message is also delivered as usual. If none does, the message is accepted for the release alone, and a message that matches no live instance is accepted and does nothing. |
| `session-end.on-reply` | `true`: the flow's `<q:reply>` on the session channel (including `continue="true"`) ends the replying instance's inbound call: its channel slot and the pool slot that call took. Pool slots the instance holds for outbound calls stay. |

A release is idempotent, and nothing takes a released slot back: not a later park, not a resume,
not an outbound request. A request the instance put in the outbox that is still waiting for a
slot when its session ends (or its call, for a call-scoped end), or one it sends afterwards, is
still sent, without a slot.
End, failure or a cancel still release whatever nothing released earlier. A malformed block (on
an outbound channel, an unknown key, an `on-message` that is not another inbound channel of the
file) fails to load with `SUTRA.CONFIG.CHANNEL.SESSION_END_INVALID`.

## Capacity pools

A pool is engine configuration:

| Key | Default | Meaning |
|---|---|---|
| `sutra.capacity-pools` | — | Comma-separated pool names; the way to declare a pool purely by environment variables. |
| `sutra.capacity-pools.<name>.slots` | required | Total slots. At least 1. |
| `sutra.capacity-pools.<name>.reserve.inbound` | `0` | Slots outbound may never take while inbound holds fewer than this many. |
| `sutra.capacity-pools.<name>.reserve.outbound` | `0` | The same, kept for outbound. The two reserves together may not exceed `slots`. |
| `sutra.capacity-pools.<name>.tenants` | every tenant | Comma-separated allow-list of tenants whose channels may name the pool. |

The env vars are `SUTRA_CAPACITY_POOLS_<NAME>_SLOTS`, `…_RESERVE_INBOUND`, `…_RESERVE_OUTBOUND` and
`…_TENANTS`, with `<NAME>` upper-cased and `-` written as `_`. Pool names are lowercase letters,
digits and dashes.

```properties
sutra.capacity-pools.lines.slots=30
sutra.capacity-pools.lines.reserve.inbound=6
sutra.capacity-pools.lines.tenants=default
```

The count is kept in the engine database (the `capacity_pool` and `capacity_slot` tables), so
every replica sees the same pool. Without a datasource it is counted per process. Stranded slots
(an instance that finished without releasing, a replica that died mid-call) are returned by the
instance sweep (`sutra.instance.sweep-interval`).

A channel draws from a pool with `pool: <name>`:

- **Inbound.** A new instance takes a slot for its call before its first step runs, or is
  rejected as busy with `SUTRA.INBOUND.POOL.EXHAUSTED` (HTTP 503), and no instance is created. A
  relay or a timer that resumes an existing instance takes no slot.
- **Outbound.** A request (a `<q:send>` or a channel call) on a pooled channel takes a slot when
  the outbox sends it. While the pool is full the request **waits**. It is put back without
  counting a delivery attempt (its last diagnostic reads `SUTRA.OUTBOUND.POOL.WAITING`) and
  re-checked on every outbox tick, oldest first. The wait never blocks the flow, and a channel
  call's `<q:timeout>` still fires. A channel-call request still waiting when its instance has
  finished is abandoned rather than sent (`SUTRA.OUTBOUND.POOL.ABANDONED`). A pooled outbound
  channel is never synchronous: `ack-mode: on-complete` on it takes the outbox path, since only
  the outbox can hold a request back without holding the caller's connection.

**One slot per active call.** An instance holds a slot for one call, identified by its key: on an
inbound start, the value of the channel's `pool-call-key` header, or else the delivery's own
identity; on an outbound request, the outbound channel's `pool-call-key` header, or else the
outbox entry's identity. So an instance that bridges its caller to a second party holds two slots,
one per leg, and requests that carry the same call key share one slot. A call's slot is released
when that call ends: a `session-end: on-message` naming its key (the far leg hanging up frees
that leg's slot alone), or `on-reply` for the inbound call. When the instance ends, fails or is
cancelled, every slot it still holds is released. **A park never releases a slot.**

`pool-call-key` takes the form `header.<name>`, matched case-insensitively, and needs a `pool:`
on the same channel.

**Reserves.** `reserve.inbound: R` keeps `R` slots that outbound may not take while inbound holds
fewer than `R` (and the same the other way round). A direction may take a slot when
`held + 1 + max(0, reserve(other) − held(other)) ≤ slots`. If the pool's store fails, inbound is
admitted (as the channel cap's count is) and outbound waits.

| Load-time error | When |
|---|---|
| `SUTRA.CONFIG.CHANNEL.POOL_UNDECLARED` | a channel names a pool the engine configuration does not declare |
| `SUTRA.CONFIG.CHANNEL.POOL_TENANT_NOT_ALLOWED` | the pool's `tenants` list excludes the channel's tenant |
| `SUTRA.CONFIG.CHANNEL.POOL_CALL_KEY_INVALID` | a malformed `pool-call-key`, or one on a channel with no `pool:` |

## Walkthrough: two lines, one callback slot

The `examples/capacity-pools` package runs one flow, `session.bpmn`:

```
POST /channels/sessions          { sessionId }   takes a `lines` slot (or 503)
  BuildCallback   responseBody := { sessionId, note }
  Notify          send on agent-callback          takes a `callbacks` slot when sent (or waits)
  AwaitWrapUp     parks until POST /channels/sessions/wrap-up { sessionId }
  End             releases whatever is still held

POST /channels/sessions/closed   { sessionId }   session-end: releases the session's slots
```

Its channels name the pools; nothing in the BPMN does:

```yaml
  - name: session-start
    transport: http
    bind: "POST /channels/sessions"
    codec: json
    cloudevents-mode: none
    pool: lines
    session-end:
      on-message: session-closed     # a correlated message there releases the session's slots
    auth: …

  - name: agent-callback
    direction: outbound
    transport: http
    bind: "http://sink:8000/sink"
    cloudevents-mode: none
    pool: callbacks
```

`deploy/compose.yaml` declares the pools on the engine, deliberately small so that three requests
fill them:

```yaml
      SUTRA_CAPACITY_POOLS: lines,callbacks
      SUTRA_CAPACITY_POOLS_LINES_SLOTS: "2"
      SUTRA_CAPACITY_POOLS_CALLBACKS_SLOTS: "1"
      SUTRA_OUTBOX_TICK_INTERVAL: PT1S     # a waiting request is re-checked every second
```

The `sink` service is the stub server from `examples/ai-summarize`, used only for its `/sink`
route, which records every body it is sent.

**Prerequisites:** [Before you try a walkthrough](../whats-new-0-6.md#before-you-try-a-walkthrough).

**1. Start the stack and deploy the package:**

```bash
C=examples/capacity-pools/deploy/compose.yaml
docker compose -f $C up -d --wait
sutra package examples/capacity-pools/deployments-src/default--lines--1.0.0 \
    --out examples/capacity-pools/deploy/deployments 2>/dev/null
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-lines-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done
post() { curl -s -w " HTTP %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/$1" "${H[@]}" -d "{\"sessionId\":\"$2\"}"; }
slots() {
  docker compose -f $C exec -T engine-db psql -U postgres -d sutra -c \
    "select pool, direction, count(*) from capacity_slot group by pool, direction order by pool"
}
```

```text
packaged examples/capacity-pools/deploy/deployments/default--lines--1.0.0.sutra (deploymentId dep-a833cc0807faa199083bcb7b)
```

**2. Open two sessions**, and wait until the first one's callback has been delivered. Both
lines are now taken. The first session's callback took the only `callbacks` slot, so the second
one's callback waits:

```bash
post sessions s-1
post sessions s-2
until docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink | grep -q s-1; do sleep 1; done
docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink; echo
slots
```

```text
 HTTP 200
 HTTP 200
["{\"note\":\"call the session back\",\"sessionId\":\"s-1\"}"]
   pool    | direction | count
-----------+-----------+-------
 callbacks | OUTBOUND  |     1
 lines     | INBOUND   |     2
(2 rows)
```

`s-1`'s callback holds the `callbacks` slot until `s-1`'s instance ends, or its session does.

**3. The waiting request** sits in the outbox, with no attempt counted:

```bash
docker compose -f $C exec -T engine-db psql -U postgres -d sutra -At -c \
    "select convert_from(body, 'UTF8'), attempt_count, last_diagnostic_json::jsonb->>'code' from outbox_entry"
```

```text
{"note":"call the session back","sessionId":"s-2"}|0|SUTRA.OUTBOUND.POOL.WAITING
```

**4. A third session is turned away**, because the pool is full:

```bash
post sessions s-3
```

```text
{"type":"urn:bpm:diag:SUTRA.INBOUND.POOL.EXHAUSTED","title":"SUTRA.INBOUND.POOL.EXHAUSTED","status":503,"detail":"Channel 'session-start' draws from capacity pool 'lines', which has no free slot (2 inbound + 0 outbound of 2); the inbound is rejected (busy).","code":"SUTRA.INBOUND.POOL.EXHAUSTED","attributes":{"channel":"session-start","pool":"lines","slots":"2"}} HTTP 503
```

**5. End `s-2`'s session while its callback waits.** `s-2` gives back its line. Its callback is
still sent, at once, although `s-1` still holds the only `callbacks` slot: an ended session takes
no slot and holds none. A third session now fits, and its callback waits for the slot `s-1`
holds. The lines are now `s-1`'s and `s-3`'s, and the callback slot is still `s-1`'s:

```bash
post sessions/closed s-2
until docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink | grep -q s-2; do sleep 1; done
docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink; echo
post sessions s-3
slots
curl -s "http://127.0.0.1:$PORT/admin/instances" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["instances"]), "instances parked")'
```

```text
 HTTP 200
["{\"note\":\"call the session back\",\"sessionId\":\"s-1\"}", "{\"note\":\"call the session back\",\"sessionId\":\"s-2\"}"]
 HTTP 200
   pool    | direction | count
-----------+-----------+-------
 callbacks | OUTBOUND  |     1
 lines     | INBOUND   |     2
(2 rows)

3 instances parked
```

`s-2`'s instance is still parked at `AwaitWrapUp`: a session end releases slots, and the instance
runs on.

**6. Wrap up `s-1`.** It ends, and gives back its line and the callback slot. `s-3`'s waiting
callback takes the slot, so one line (`s-3`'s) and the callback slot (also `s-3`'s) are held:

```bash
post sessions/wrap-up s-1
until docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink | grep -q s-3; do sleep 1; done
slots
```

```text
 HTTP 200
   pool    | direction | count
-----------+-----------+-------
 callbacks | OUTBOUND  |     1
 lines     | INBOUND   |     1
(2 rows)
```

**7. Clean up:**

```bash
docker compose -p sutra-capacity-pools down -v
```

## Next

- **[Limits and quotas](limits.md)**: the payload cap and the per-tenant quotas.
- **[Configuration reference](configuration.md)**: every `sutra.*` key.
