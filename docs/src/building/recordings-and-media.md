# Recordings and media references

A process should not carry audio through its variables. A recording is large and personal, and
once it is inside a snapshot, an audit row or a log it is copied everywhere the engine writes. In
0.6 the media stays in an **object store**, and the engine passes a **media reference** around
in its place:

```json
{ "kind": "media", "uri": "s3://recordings/calls/q-1.wav", "sha256": "ee6497…", "format": "audio/wav" }
```

Four pieces work together:

| Piece | What it does |
|---|---|
| [Named object stores](#object-stores) | Engine configuration: a bucket (and optional key prefix) the engine may read and delete in. |
| [Media references in a model call](#media-references-in-a-model-call) | An `audio`, `video` or `image` part gives `{ref, sha256}` instead of `data`, and the `llm` channel reads the object from its `media-store`. |
| [`q:audit capture="media"`](#the-audit-trail-carries-references-never-media) | Audit events carry the references found among the instance's variables, never the media. |
| [Erasure and retention](#erasure-and-retention-delete-the-recording) | When an instance's data has to go, the engine deletes the referenced objects and amends the audit events to say so. |

A media transport (a telephony transport, for example) typically writes the recording and hands
the process its reference. In the walkthrough below you play that part by uploading a file.

## Object stores

A store is declared in the engine configuration, never in a package, by any
`sutra.object-stores.<name>.*` key in the file, or by naming it in `sutra.object-stores`:

| Key | Default | Meaning |
|---|---|---|
| `sutra.object-stores` | — | Comma-separated store names; the way to declare a store purely by environment variables. |
| `sutra.object-stores.<name>.uri` | required | `s3://<bucket>[/<prefix>]` (or `memory://<bucket>` for tests: in process, nothing survives a restart). |
| `sutra.object-stores.<name>.endpoint` | AWS | A self-hosted or non-AWS store's base URL (`http://store:9000`), addressed path-style. |
| `sutra.object-stores.<name>.region` | `us-east-1` | Signing region. |
| `sutra.object-stores.<name>.access-key`, `.secret-key` | required for `s3://` | Secret references (`env:`, `secret:`, `vault:`, …), resolved at boot. |
| `sutra.object-stores.<name>.encryption` | `envelope` | `envelope`: objects are sealed (encrypted at rest with a fresh data key wrapped under `kek`), as a recording transport writes them; `none`: stored as they are. |
| `sutra.object-stores.<name>.kek` | required with `envelope` | Secret reference to the key-encryption secret the objects were sealed under. |
| `sutra.object-stores.<name>.key-id` | `recordings` | The key id sealed objects name. |

Each key has an env var `SUTRA_OBJECT_STORES_<NAME>_<FIELD>`, with `<NAME>` upper-cased and `-`
written as `_` (for example `SUTRA_OBJECT_STORES_RECORDINGS_SECRET_KEY`). A store that cannot be
built refuses startup. The client speaks the S3 API with Signature V4, so any S3-compatible store
works.

```properties
sutra.object-stores.recordings.uri=s3://recordings/calls
sutra.object-stores.recordings.endpoint=http://store:9000
sutra.object-stores.recordings.access-key=env:STORE_ACCESS_KEY
sutra.object-stores.recordings.secret-key=env:STORE_SECRET_KEY
sutra.object-stores.recordings.kek=secret:recordings-kek
```

## Media references in a model call

Any `audio`, `video` (the clip, or each frame) or `image` part of an `llm` request may give
`ref` and `sha256` instead of `data`:

```json
{
  "messages": [{ "role": "user", "content": "Summarise this recording in one sentence.",
                 "audio": { "ref": "s3://recordings/calls/q-1.wav",
                            "sha256": "ee649768a64c…", "format": "audio/wav" } }]
}
```

The channel names the one store it may read from:

```yaml
  llm:
    media-store: recordings        # sutra.object-stores.recordings
    media-max-bytes: 1048576       # optional; 20 MiB by default
    candidates: […]
```

Before the call is routed, the transport reads each referenced object. It checks the stored
bytes against `sha256` and `media-max-bytes`, opens the object when it is sealed, and sends the
content to the provider as if it had been inline. The reply's echoed `request` keeps the
reference, never the content.

| What went wrong | Result |
|---|---|
| The channel has no `media-store`; the reference is outside that store (another store, another bucket, or a key outside its prefix); the object is missing or over the limit; the digest does not match; a sealed object does not open | permanent failure, `SUTRA.LLM.MEDIA.REFUSED` |
| `media-store` names a store the engine configuration does not declare | `SUTRA.LLM.CONFIG.INVALID` when the package loads |
| The store cannot be reached | a retryable failure |

## The audit trail carries references, never media

`<q:audit capture="media">` on a process selects metadata-level audit events: no variable
snapshot. Each event instead carries, under `media`, every media reference found among the
instance's variables (any object with `kind: "media"`, a `uri` and a `sha256`, at any depth). A
variable marked `sensitive` is not searched. The capture also marks every request the process
sends (the `sutra-audit-capture: media` header), so a media transport knows to record the
session.

```xml
<bpmn:process id="recording-summary" isExecutable="true">
  <bpmn:extensionElements>
    <q:audit capture="media"/>
    …
```

`media` is an additive value of the `capture` attribute, alongside `none`, `metadata` and
`payload`, and a package's `audit.capture` accepts it too.

## Erasure and retention delete the recording

When an instance's data has to go, its recordings go with it:

- **Subject erasure** (`POST /admin/subjects/erase`) deletes every object that the erased
  instances' media events reference.
- **Terminal retention** (`sutra.instance.retention`) deletes them for the instances it purges:
  retained rows past the window, and finished instances whose row is already gone.

Each media event is then amended to say what was recorded and that it is gone:

```json
{"media": [{"kind": "media", "uri": "…", "sha256": "…", "erased": true}],
 "mediaErasure": {"at": "<time>", "reason": "subject-erasure | retention", "objects": 1}}
```

A recording is never skipped silently. If no declared store holds it, or the store refuses the
delete, erasure answers `500` with the references and changes nothing for that instance, so it
can be re-run once that is fixed. Retention keeps the instance and retries on its next sweep,
with a `WARN` in the log.

## Walkthrough: a recording summarised, erased, and retired

The `examples/media-recordings` package has one flow, `recording-summary.bpmn`:

```
POST /channels/recording-summary   { recordingId, callerId, recording: {kind, uri, sha256, format} }
  BuildRequest   requestBody := { messages: [{ content, audio: { ref, sha256, format } }] }
  AskModel       summary-model (sync): reads the object, checks digest and size, sends it as audio
  BuildReply     { summary, provider, audioPreserved }, answered at once (<q:reply continue="true">)
  AwaitClose     parks until POST /channels/recording-closed { recordingId }
  End
```

`callerId` is declared `<q:variable name="callerId" type="string" sensitive="true"
subjectKey="true"/>`, which makes it the data subject that erasure finds the instance by. The
model channel:

```yaml
  - name: summary-model
    direction: outbound
    transport: llm
    bind: "llm://summary-model"
    ack-mode: on-complete
    llm:
      media-store: recordings          # sutra.object-stores.recordings (engine configuration)
      media-max-bytes: 1048576         # refuse any referenced object over 1 MiB (default 20 MiB)
      require: [audio-understanding]   # only a candidate that hears the voice itself
      candidates:
        - provider: openai
          model: gpt-audio-stub
          base-url: http://model-stub:8000/v1
          api-key: env:LLM_API_KEY     # a real deployment: env:OPENAI_API_KEY, no base-url
      timeout-seconds: 30
```

`deploy/compose.yaml` runs the engine, PostgreSQL, MinIO (pinned by digest) and the stub model
server. It sets on the engine:

| Setting | Why |
|---|---|
| `SUTRA_OBJECT_STORES=recordings`, `…_URI=s3://recordings/calls`, `…_ENDPOINT=http://store:9000`, `…_ACCESS_KEY=env:STORE_ACCESS_KEY`, `…_SECRET_KEY=env:STORE_SECRET_KEY`, `…_ENCRYPTION=none` | the store; the sample uploads plain objects, where a recording transport would write sealed ones |
| `SUTRA_AUDIT_SQL=true` | audit events go to the engine database, which `GET /admin/instances/{id}/history` reads |
| `SUTRA_CRYPTO_MASTER_KEY=env:RECORDINGS_MASTER_KEY` (a dev-only placeholder) | blind-indexes `subjectKey` variables so erasure can find the instance; it also encrypts `sensitive` variables at rest. It is a secret reference like every other secret key (`env:`, `secret:`, `vault:`, …); a literal value is still used as the raw key, with a warning at boot |
| `SUTRA_INSTANCE_RETENTION=PT2M`, `SUTRA_INSTANCE_RETENTION_SWEEP_INTERVAL=PT15S` | a finished instance is purged about two minutes after it ends, instead of the default seven days |
| `SUTRA_ADMIN_OIDC_DEV_DISABLED=true` | `/admin/*` without authentication. **Dev only.** |

**Prerequisites:** [Before you try a walkthrough](../whats-new-0-6.md#before-you-try-a-walkthrough).
No key is needed.

**1. Start the stack and deploy the package:**

```bash
C=examples/media-recordings/deploy/compose.yaml
docker compose -f $C up -d --wait
sutra package examples/media-recordings/deployments-src/default--recordings--1.0.0 \
    --out examples/media-recordings/deploy/deployments 2>/dev/null
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-recordings-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done
```

```text
packaged examples/media-recordings/deploy/deployments/default--recordings--1.0.0.sutra (deploymentId dep-4314895f6198e60f7bf51a20)
```

(`sutra package` also prints two warnings about the schema-less JSON channel, which you can
ignore here.)

**2. Put two recordings in the store**, as a recording transport would. The MinIO image carries
the `mc` client, already pointed at the server:

```bash
docker compose -f $C exec store mc mb local/recordings
for id in q-1 q-2; do
  docker compose -f $C exec -T store mc pipe local/recordings/calls/$id.wav < examples/media-recordings/sample/question.wav
done
SHA=$(sha256sum examples/media-recordings/sample/question.wav | cut -d' ' -f1)
```

```text
Bucket created successfully `local/recordings`.
 0 B / ? 8044 bytes -> `local/recordings/calls/q-1.wav`
 0 B / ? 8044 bytes -> `local/recordings/calls/q-2.wav`
```

**3. Summarise the first recording by reference:**

```bash
ask() {  # ask <recordingId> <callerId> <sha256>
  curl -s -w " HTTP %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/recording-summary" "${H[@]}" \
    -d "{\"recordingId\":\"$1\",\"callerId\":\"$2\",\"recording\":{\"kind\":\"media\",\"uri\":\"s3://recordings/calls/$1.wav\",\"sha256\":\"$3\",\"format\":\"audio/wav\"}}"
}
ask q-1 caller-1 "$SHA"
```

```text
{"audioPreserved":true,"provider":"openai","summary":"STUB SUMMARY: the customer reports a problem and asks for it to be looked at."} HTTP 200
```

The engine read `calls/q-1.wav`, checked its digest, and sent it to the audio model. The instance
is now parked at `AwaitClose`.

**4. A reference the store cannot honour is refused.** There is no `q-9.wav`:

```bash
ask q-9 caller-9 "$SHA"
```

```text
{"type":"urn:bpm:diag:SUTRA.LLM.MEDIA.REFUSED","title":"SUTRA.LLM.MEDIA.REFUSED","status":422,"detail":"'s3://recordings/calls/q-9.wav' does not exist","code":"SUTRA.LLM.MEDIA.REFUSED","attributes":{"incident":"SUTRA.INBOUND.NON_IDEMPOTENT_FAILURE"}} HTTP 422
```

The model call failed permanently with `SUTRA.LLM.MEDIA.REFUSED`, and the caller is told so, with
`422`: the request named an object that cannot be read, so sending it again cannot succeed. The
detail names the reference, never the store's credentials or the object's content. This process
does not declare itself idempotent, so the inbound is consumed and recorded as an incident instead
of being redelivered; `incident` carries that code. A wrong `sha256` on an existing object is
refused the same way, with `does not match its SHA-256`.

**5. Read the audit trail.** Every event carries the reference, and none carries the audio:

```bash
curl -s "http://127.0.0.1:$PORT/admin/instances"; echo
ID=$(curl -s "http://127.0.0.1:$PORT/admin/instances" | python3 -c 'import json,sys; print([i["instanceId"] for i in json.load(sys.stdin)["instances"] if i["status"] == "SUSPENDED"][0])')
history() {
  curl -s "http://127.0.0.1:$PORT/admin/instances/$1/history" | python3 -c '
import json, sys
for e in json.load(sys.stdin)["events"]:
    print(e["seq"], e["eventType"], e["nodeId"] or "", json.dumps(e["payload"]) if e["payload"] else "")'
}
history "$ID"
```

```text
{"instances":[{"instanceId":"f67c1b1e-4c95-4317-9f0f-6fb3ede9f5a2","deploymentId":"dep-4314895f6198e60f7bf51a20","status":"SUSPENDED"}]}
1 INSTANCE_STARTED
2 NODE_ENTERED Start {"media": [{"format": "audio/wav", "kind": "media", "sha256": "ee649768a64c8d99984c36213c629f4a5c84d10fac2d872275f2f11bcafa1e4c", "uri": "s3://recordings/calls/q-1.wav"}]}
3 NODE_LEFT Start
4 NODE_ENTERED BuildRequest {"media": [{"format": "audio/wav", "kind": "media", "sha256": "ee649768a64c8d99984c36213c629f4a5c84d10fac2d872275f2f11bcafa1e4c", "uri": "s3://recordings/calls/q-1.wav"}]}
5 NODE_LEFT BuildRequest
6 NODE_ENTERED AskModel {"media": [{"format": "audio/wav", "kind": "media", "sha256": "ee649768a64c8d99984c36213c629f4a5c84d10fac2d872275f2f11bcafa1e4c", "uri": "s3://recordings/calls/q-1.wav"}]}
7 INSTANCE_SUSPENDED
8 INSTANCE_RESUMED
9 NODE_LEFT AskModel
10 NODE_ENTERED BuildReply {"media": [{"format": "audio/wav", "kind": "media", "sha256": "ee649768a64c8d99984c36213c629f4a5c84d10fac2d872275f2f11bcafa1e4c", "uri": "s3://recordings/calls/q-1.wav"}]}
11 NODE_LEFT BuildReply
12 NODE_ENTERED AwaitClose {"media": [{"format": "audio/wav", "kind": "media", "sha256": "ee649768a64c8d99984c36213c629f4a5c84d10fac2d872275f2f11bcafa1e4c", "uri": "s3://recordings/calls/q-1.wav"}]}
13 INSTANCE_SUSPENDED
```

The refused instance is not listed: `/admin/instances` lists live instances. Inspecting the
parked one shows its variables, with `callerId` redacted: it is `sensitive`, encrypted at rest,
and the endpoint decrypts it only to apply the same redaction as everywhere else:

```bash
curl -s "http://127.0.0.1:$PORT/admin/instances/$ID" | python3 -c 'import json,sys; r=json.load(sys.stdin); print(r["status"], r["sensitive"], r["variables"]["callerId"])'
```

```text
SUSPENDED ['callerId'] ***REDACTED***
```

**6. Erase the data subject `caller-1`.** `keyId` is the tenant and `subjectName` is the
`subjectKey` variable. With no `deploymentId`, erasure covers every live deployment of the
tenant, wherever it was deployed from (a watched directory, as here, or the database):

```bash
curl -s -X POST "http://127.0.0.1:$PORT/admin/subjects/erase" -H 'Content-Type: application/json' \
    -d '{"keyId":"default","subjectName":"callerId","value":"caller-1"}'; echo
docker compose -f $C exec store mc ls -r local/recordings
history "$ID" | sed -n 2p
```

```text
{"subjectName":"callerId","tenantWide":true,"deploymentsScanned":1,"erased":true,"matched":1,"erasedCount":1,"instanceIds":["f67c1b1e-4c95-4317-9f0f-6fb3ede9f5a2"]}
[2026-10-26 06:52:26 UTC] 7.9KiB STANDARD calls/q-2.wav
2 NODE_ENTERED Start {"media": [{"kind": "media", "uri": "s3://recordings/calls/q-1.wav", "erased": true, "sha256": "ee649768a64c8d99984c36213c629f4a5c84d10fac2d872275f2f11bcafa1e4c"}], "mediaErasure": {"at": "2026-10-26T06:52:26.726921625Z", "reason": "subject-erasure", "objects": 1}}
```

`calls/q-1.wav` is gone from the store, and the audit events still say what was recorded, now
marked `erased`.

**7. Let retention retire the second recording.** Summarise `q-2` and close it, so the instance
completes:

```bash
ask q-2 caller-2 "$SHA"
curl -s -w "HTTP %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/recording-closed" "${H[@]}" -d '{"recordingId":"q-2"}'
```

```text
{"audioPreserved":true,"provider":"openai","summary":"STUB SUMMARY: the customer reports a problem and asks for it to be looked at."} HTTP 200
HTTP 200
```

Then wait for the purge. With a two-minute retention and a 15-second sweep it takes about two
and a half minutes:

```bash
until [ -z "$(docker compose -f $C exec -T store mc ls local/recordings/calls/q-2.wav)" ]; do sleep 5; done
docker compose -f $C logs engine | grep -o 'terminal retention deleted the recorded media[^"]*'
docker compose -f $C exec -T engine-db psql -U postgres -d sutra -At \
    -c "select seq, event_type, payload_json from audit_event where payload_json like '%q-2.wav%' order by seq limit 1"
```

```text
terminal retention deleted the recorded media of finished instances
2|NODE_ENTERED|{"media":[{"kind":"media","uri":"s3://recordings/calls/q-2.wav","erased":true,"sha256":"ee649768a64c8d99984c36213c629f4a5c84d10fac2d872275f2f11bcafa1e4c"}],"mediaErasure":{"at":"2026-10-26T06:54:34.352446787Z","reason":"retention","objects":1}}
```

The loop ends when `mc ls` no longer lists the object. The purged instance's audit rows stay,
amended with `"reason":"retention"`.

**8. Clean up:**

```bash
docker compose -p sutra-media-recordings down -v
```

## Next

- **[Calling a language model](llm-channels.md#audio-images-and-video)**: audio, images and
  video in model calls.
- **[Logging and audit](../operating/logging.md)**: the audit sinks and capture levels.
- **[Configuration reference](../operating/configuration.md)**: every `sutra.*` key.
