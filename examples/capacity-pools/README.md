# capacity-pools — a shared count of slots, and sessions that end before their instance

Two engine-declared pools and one flow that draws from both:

| Pool | Slots | Drawn by | When full |
|---|---|---|---|
| `lines` | 2 | `session-start` (inbound): each new session takes one | the next start is answered **503** `SUTRA.INBOUND.POOL.EXHAUSTED`; no instance is created |
| `callbacks` | 1 | `agent-callback` (outbound): each session's callback request takes one when it is sent | the request **waits** in the outbox (`SUTRA.OUTBOUND.POOL.WAITING`), no attempt counted, until a slot frees |

`session-end: on-message: session-closed` on `session-start` gives a session's slots back while
its instance keeps running: the instance stays parked at its wrap-up wait, holding nothing.

The pools are engine configuration (`SUTRA_CAPACITY_POOLS_*` in `deploy/compose.yaml`); the
channels only name them. The count lives in the engine database, so every replica sees one pool.

## The flow

```
POST /channels/sessions          { sessionId }   takes a `lines` slot (or 503)
  BuildCallback   responseBody := { sessionId, note }
  Notify          send on agent-callback          takes a `callbacks` slot when sent (or waits)
  AwaitWrapUp     parks until POST /channels/sessions/wrap-up { sessionId }
  End             releases whatever is still held

POST /channels/sessions/closed   { sessionId }   releases the session's slots; the instance runs on
```

## Run it

Needs Docker, the `sutra` CLI and an engine image, both 0.6.0 or later (set `SUTRA_ENGINE_IMAGE`
for a locally built image). From the repository root:

```bash
C=examples/capacity-pools/deploy/compose.yaml
docker compose -f $C up -d --wait
sutra package examples/capacity-pools/deployments-src/default--lines--1.0.0 \
    --out examples/capacity-pools/deploy/deployments
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-lines-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done
post() { curl -s -w " HTTP %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/$1" "${H[@]}" -d "{\"sessionId\":\"$2\"}"; }

post sessions s-1; post sessions s-2     # both lines taken; s-2's callback waits
until docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink | grep -q s-1; do sleep 1; done
post sessions s-3                        # => SUTRA.INBOUND.POOL.EXHAUSTED ... HTTP 503
post sessions/closed s-1                 # s-1 gives its line and callback slot back
until docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink | grep -q s-2; do sleep 1; done
docker compose -f $C exec sink wget -qO- http://127.0.0.1:8000/sink; echo   # s-2's callback arrived
post sessions s-3                        # => HTTP 200
docker compose -p sutra-capacity-pools down -v
```

The book's *Capacity pools and sessions* chapter walks through every step with the output to
expect, including the waiting outbox row.

## Notes

- `session-closed` has no `<q:source>` subscriber; `session-end: on-message` takes its messages,
  and `sutra lint` knows that, so it reports no warning for it.
- A callback still queued in the outbox when its session ends is still sent, but takes no
  `callbacks` slot and holds none: an ended session gives its slots back for good.
- `/admin/*` is served without authentication in this compose file
  (`SUTRA_ADMIN_OIDC_DEV_DISABLED`). Never do that in anything real.
