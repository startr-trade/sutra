# review-loop — a process that waits at the same nodes again and again

A draft goes round **review → revision → cool-off** until a reviewer approves it. The loop brings
the token back to the same three wait nodes every round — a user task (`AwaitReview`), a message
catch (`AwaitRevision`) and a timer (`CoolOff`, 2 s) — and each pass parks a **new** wait: its own
wait row and timer. The step that answers a round retires its wait, so a message for a wait the
instance has already left is refused (`SUTRA.RUNTIME.RESUME.NOT_WAITING`) before anything runs.

## The flow

```
POST /channels/drafts            { draftId }                     rounds := 0
  AwaitReview    user task, answered on POST /channels/drafts/review { draftId, decision }
  Count          rounds := rounds + 1
  Decide         review.decision = "approve" -> End
                 otherwise -> AwaitRevision (POST /channels/drafts/revision { draftId })
                           -> CoolOff (timer, PT2S) -> back to AwaitReview
```

Every message after the first finds its instance through the start event's
`<q:alias name="draftId" expression="payload.draftId" onConflict="correlate">`, although the waits
bind their messages under names of their own (`review`, `revision`). The process is audited at
metadata level, so `GET /admin/instances/{id}/history` lists every round.

## Run it

Needs Docker, the `sutra` CLI and an engine image, both 0.6.0 or later (set `SUTRA_ENGINE_IMAGE`
for a locally built image). From the repository root:

```bash
C=examples/review-loop/deploy/compose.yaml
docker compose -f $C up -d --wait
sutra package examples/review-loop/deployments-src/default--review--1.0.0 \
    --out examples/review-loop/deploy/deployments
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-review-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done
send() { curl -s -w " HTTP %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/drafts$1" "${H[@]}" -d "$2"; }

send "" '{"draftId":"d-1"}'
send /review '{"draftId":"d-1","decision":"revise"}'
send /revision '{"draftId":"d-1"}'                      # then 2 s of cool-off
sleep 3
send /review '{"draftId":"d-1","decision":"approve"}'   # round 2: the same user task, a new wait
docker compose -p sutra-review-loop down -v
```

The book's *Wait states and human tasks* chapter walks through it with the output to expect,
including the wait rows re-armed each round and a refused message for a wait already left.
