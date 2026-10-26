# media-recordings — media references, audited by reference, deleted on erasure and retention

A process that never holds the audio it works on. A caller names a recording that already sits
in an object store; the model channel reads it from there, checks it, and sends it to an audio
model; the audit trail records the reference, never the bytes; and when the instance's data has
to go — a data subject's erasure, or the end of terminal retention — the engine deletes the
recording from the store and amends the audit events to say so.

| Piece | Where |
|---|---|
| The object store (`recordings`: bucket `recordings`, prefix `calls`, on MinIO) | engine configuration, `deploy/compose.yaml` (`SUTRA_OBJECT_STORES_RECORDINGS_*`) |
| A media reference `{kind: "media", uri, sha256, format}` in the request | the caller's JSON |
| `audio: {ref, sha256, format}` on the model request | `bpmn/recording-summary.bpmn`, BuildRequest |
| `llm.media-store`, `llm.media-max-bytes` | `channels.yaml`, `summary-model` |
| `<q:audit capture="media">` | `bpmn/recording-summary.bpmn` |
| `callerId` as the data subject (`subjectKey="true"`) | `bpmn/recording-summary.bpmn` |
| Short retention (`PT2M`, swept every 15 s) so the delete is visible | `deploy/compose.yaml` |

## The flow

```
POST /channels/recording-summary   { recordingId, callerId, recording: {kind, uri, sha256, format} }
  BuildRequest   requestBody := { messages: [{ content, audio: { ref, sha256, format } }] }
  AskModel       summary-model (sync) — reads the object, checks digest and size, sends it as audio
  BuildReply     { summary, provider, audioPreserved }   <q:reply continue="true">: answered here
  AwaitClose     parks until POST /channels/recording-closed { recordingId }
  End
```

## Run it

Needs Docker, the `sutra` CLI and an engine image, both 0.6.0 or later (set `SUTRA_ENGINE_IMAGE`
for a locally built image). From the repository root:

```bash
C=examples/media-recordings/deploy/compose.yaml
docker compose -f $C up -d --wait
sutra package examples/media-recordings/deployments-src/default--recordings--1.0.0 \
    --out examples/media-recordings/deploy/deployments
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-recordings-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done

# Put the sample recording in the store, as a recording transport would.
docker compose -f $C exec store mc mb local/recordings
docker compose -f $C exec -T store mc pipe local/recordings/calls/q-1.wav < examples/media-recordings/sample/question.wav
SHA=$(sha256sum examples/media-recordings/sample/question.wav | cut -d' ' -f1)

curl -s -X POST "http://127.0.0.1:$PORT/channels/recording-summary" "${H[@]}" \
    -d "{\"recordingId\":\"q-1\",\"callerId\":\"caller-1\",\"recording\":{\"kind\":\"media\",\"uri\":\"s3://recordings/calls/q-1.wav\",\"sha256\":\"$SHA\",\"format\":\"audio/wav\"}}"; echo
# => {"audioPreserved":true,"provider":"openai","summary":"STUB SUMMARY: ..."}
```

The engine's audit trail (`GET /admin/instances/{id}/history`) now carries the reference on each
event; `POST /admin/subjects/erase` for `callerId` = `caller-1` deletes `calls/q-1.wav` and marks
those events `erased`. A recording whose instance finishes is deleted by retention two minutes
later. The book's *Recordings and media references* chapter walks through every step with the
output to expect.

```bash
docker compose -p sutra-media-recordings down -v
```

## Notes

- The sample uploads plain objects (`encryption: none`). A recording transport writes sealed
  objects (the default, `encryption: envelope`), which the store opens with its `kek` secret.
- `/admin/*` is served without authentication in this compose file
  (`SUTRA_ADMIN_OIDC_DEV_DISABLED`). Never do that in anything real.
- Tenant-wide erasure (no `deploymentId`) covers every live deployment of the tenant, including
  ones from a watched deployments directory, as here. Pass `deploymentId` only to narrow it.
- `SUTRA_CRYPTO_MASTER_KEY` is a secret reference (`env:RECORDINGS_MASTER_KEY` here), resolved
  like any other secret. A literal value still works, as the raw key, and logs a warning.
