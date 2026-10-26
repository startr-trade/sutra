# ai-advanced — fallback, JSON mode, tools, provider jobs, webhooks, audio and vision

The companion to `examples/ai-summarize`: nine flows for the parts of the `llm` transport beyond a
plain call. Every model sits behind an ordinary outbound channel and is called by an
ordinary channel-call task, so nothing in the BPMN is specific to models.

| Flow (`shared/bpmn/`) | Inbound channel | Model channel | What it shows |
|---|---|---|---|
| `fallback.bpmn` | `POST /channels/fallback` (sync) | `fallback-model` | **Cross-provider fallback**: Anthropic first, OpenAI second. The reply names the provider that answered. |
| `classify.bpmn` | `POST /channels/classify` (sync) | `classify-model` | **JSON mode**: the request says `json: true`; the answer is one JSON object. |
| `weather.bpmn` | `POST /channels/weather` (sync) | `weather-model` | **A tool call**: the request offers `get_weather`; the reply is `finish: tool-calls` plus the call. |
| `job-poll.bpmn` | `POST /channels/summarize-job` (202) | `job-model` | **An OpenAI background job**, `mode: job`, found by polling. |
| `job-webhook.bpmn` | `POST /channels/summarize-webhook` (202) | `webhook-model` | **An OpenAI background job completed by a signed webhook** on `POST /llm/openai-hooks`. |
| `batch.bpmn` | `POST /channels/summarize-batch` (202) | `batch-model` | **An Anthropic Message Batch**, `mode: job`, polled. |
| `voice.bpmn` | `POST /channels/voice` (sync) | `voice-model` | **Audio in, a spoken answer out**: an uploaded WAV recording to an audio model, the spoken answer in the format the request asked for; `audioPreserved: true`. |
| `voice-pipeline.bpmn` | `POST /channels/voice-pipeline` (sync) | `voice-pipeline-model` | **`voice: pipeline`**: transcribed, answered by a text model (Anthropic), spoken; `audioPreserved: false`. |
| `vision.bpmn` | `POST /channels/vision` (sync) | `vision-model` | **Images and video frames** (preview) to the native `gemini` provider with `json: true`; the flow reads the parsed answer under `json`. |

The three job flows park on their task, are resumed by the job's result on the channel's
`reply-channel` (correlated by `<q:alias>` on the echoed `ticketId`), and hand the summary to
`summary-sink`.

## Layout

| Path | What it is |
|---|---|
| `shared/bpmn/` | the nine flows (commented, with diagram interchange) |
| `sample/` | `question.wav` (a 0.25 s tone) and two 8×8 PNG frames, for the audio and vision flows; for the real-provider checks, `spoken-question.wav` (a synthetic voice asking "What is the capital of France?", 16 kHz mono) and `moving-square.mp4` (a 2 s clip) |
| `variants/stub/` | `package.yaml` + `channels.yaml`: every model is the stub. Needs no key; the engine's tier-2 tests deploy this one. |
| `variants/live/` | the same channels against the real OpenAI, Anthropic and Gemini APIs, plus two Gemini-only flows in `variants/live/bpmn/`: `hear.bpmn` (audio in, inline or as a media reference, a text answer out) and `extract.bpmn` (`json: true`, the parsed answer out) |
| `deploy/compose.yaml` | engine + PostgreSQL + the stub (from `examples/ai-summarize/stub`), project name `sutra-ai-advanced` |
| `deploy/compose.media.yaml` | adds a MinIO object store, declared to the engine as the store `media`, which `vision-model` (stub) and `hear-model` (live) read media references `{ref, sha256, format}` from |
| `live-check.sh` | runs both examples against the real providers with your keys |

A variant is one `channels.yaml` (plus its `package.yaml`, which an overlay layer may not carry);
`sutra package <variant> --overlay shared` composes it over the flows.

## Run it against the stub

```bash
sutra package examples/ai-advanced/variants/stub --overlay examples/ai-advanced/shared \
    --out examples/ai-advanced/deploy/deployments
docker compose -f examples/ai-advanced/deploy/compose.yaml up -d
PORT=$(docker compose -f examples/ai-advanced/deploy/compose.yaml port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-advanced-key')

# Fallback. The stub's Anthropic candidate sits behind its /flaky gate, down by default:
curl -s -X POST "http://127.0.0.1:$PORT/channels/fallback" "${H[@]}" -d '{}'
# => {"model":"test-model","provider":"openai","text":"STUB SUMMARY: ..."}
# Bring Anthropic up and ask again:
docker compose -f examples/ai-advanced/deploy/compose.yaml exec model-stub \
    wget -qO- --post-data '{"on": false}' http://127.0.0.1:8000/control/outage
curl -s -X POST "http://127.0.0.1:$PORT/channels/fallback" "${H[@]}" -d '{}'
# => {"model":"test-model","provider":"anthropic","text":"STUB SUMMARY: ..."}

# JSON mode and a tool call.
curl -s -X POST "http://127.0.0.1:$PORT/channels/classify" "${H[@]}" \
    -d '{"ticketId":"T-1","description":"Checkout crashes for everyone."}'
# => {"finish":"stop","text":"{\"category\": \"outage\", \"urgency\": \"high\"}","ticketId":"T-1"}
curl -s -X POST "http://127.0.0.1:$PORT/channels/weather" "${H[@]}" -d '{}'
# => {"finish":"tool-calls","toolCalls":[{"arguments":{"city":"Paris"},"id":"toolu_stub_1","name":"get_weather"}]}

# Audio: a WAV recording in, 16 kHz PCM out; then the same through the pipeline.
AUDIO=$(base64 -w0 examples/ai-advanced/sample/question.wav)   # or any WAV file
curl -s -X POST "http://127.0.0.1:$PORT/channels/voice" "${H[@]}" \
    -d "{\"requestId\":\"Q-1\",\"audio\":{\"data\":\"$AUDIO\",\"format\":\"audio/wav\"},\"audioOutput\":\"audio/L16;rate=16000\"}" \
    | python3 -c 'import json,sys; r=json.load(sys.stdin); r["audio"]["data"]="…"; print(r)'
# => {'audio': {'data': '…', 'format': 'audio/L16;rate=16000'}, 'audioPreserved': True,
#     'requestId': 'Q-1', 'provider': 'openai', 'text': 'STUB SPOKEN ANSWER: ...', ...}
# /channels/voice-pipeline takes the same body and answers with 'audioPreserved': False,
# 'provider': 'anthropic'.

# Vision: one image, or sampled frames, to the native Gemini provider; a parsed JSON answer.
IMG=$(base64 -w0 examples/ai-advanced/sample/frame-1.png)
curl -s -X POST "http://127.0.0.1:$PORT/channels/vision" "${H[@]}" \
    -d "{\"requestId\":\"V-1\",\"image\":{\"data\":\"$IMG\",\"format\":\"image/png\"}}"
# => {"framesSeen":1,"objects":["bicycle","door"],"provider":"gemini","requestId":"V-1","scene":"STUB SCENE: ..."}

# The three jobs: 202 at once, the summaries reach the sink a few seconds later.
for c in summarize-job summarize-webhook summarize-batch; do
  curl -s -o /dev/null -w "$c %{http_code}\n" -X POST "http://127.0.0.1:$PORT/channels/$c" "${H[@]}" \
      -d "{\"ticketId\":\"$c-1\",\"description\":\"My parcel never arrived.\"}"
done
docker compose -f examples/ai-advanced/deploy/compose.yaml logs model-stub | grep -E 'webhook|SINK'
# => model-stub: webhook for resp_stub_2 -> HTTP 200
#    model-stub: SINK received {"summary":"STUB SUMMARY: ...","ticketId":"summarize-webhook-1","via":"job-webhook"}
#    ... (job-poll and batch likewise)

docker compose -p sutra-ai-advanced down -v
```

What the stub does for each: a fixed answer per wire format; a background job or batch that is in
progress on the first poll and done on the next; for a job whose model id ends in `-webhook`, a
`response.completed` event signed the Standard Webhooks way with the dev-only secret both
containers get from `compose.yaml`, POSTed back to the engine about a second after the submit (the
candidate's safety-net poll is 10 minutes, so it is the webhook that finishes it). The script's
header lists every route.

## Run it against the real providers

`variants/live` uses `gpt-4o-mini`, `claude-haiku-5-5` and, for the vision flow, `gemini-2.5-flash` (plain strings in its `channels.yaml`:
edit them to change models). Its Anthropic fallback candidate reaches `api.anthropic.com` through
the stub's `/relay/anthropic` gate, so the same outage switch takes a real provider down and up and
both fallback answers are real.

```bash
export OPENAI_API_KEY=... ANTHROPIC_API_KEY=...     # GEMINI_API_KEY too, for ai-summarize/gemini
examples/ai-advanced/live-check.sh
```

`live-check.sh` packages each variant with `sutra package <variant> --overlay <shared>` into a
temporary directory, starts the stack, runs every check and tears it down; it prints one line per
check and never prints a key. It runs the three real `ai-summarize` variants for whichever keys you
exported, then the `live` variant here when an OpenAI key is present (its fallback, tool-call and
Message Batch checks need the Anthropic key too).

**Just the Gemini paths.** With only `GEMINI_API_KEY`, `ONLY=gemini` runs the ai-summarize
`gemini` variant and every native-Gemini check: image, video frames and an MP4 clip, audio
understanding (the answer must say Paris), a parsed JSON answer, the audio as a media reference
read from MinIO, a wrong SHA-256 refused before any provider call, and a Gemini Live session
(ephemeral token, constrained WebSocket, the WAV up, audio and transcripts back, no credential in
any URL). They need an engine image and CLI from this tree:

```bash
make image && (cd rust && cargo build -p sutra-cli)
GEMINI_API_KEY=... ONLY=gemini SUTRA_ENGINE_IMAGE=sutra-rust-engine:dev \
    SUTRA_BIN=$PWD/rust/target/debug/sutra examples/ai-advanced/live-check.sh
```

The Live model is `GEMINI_LIVE_MODEL` (default `gemini-3.8-live`); the script's
header says where to change it. The round trip under Gemini's own turn detection uses
`gemini-2.5-flash-native-audio-latest` by default: the sample question is synthetic speech, and
`gemini-3.8-live`'s voice-activity detection takes only its first word for speech, while it handles a
real voice. To check `gemini-3.8-live` there, record your own question
(`arecord -f S16_LE -r 16000 -c 1 -d 4 q.wav`) and set `GEMINI_LIVE_WAV` to it with
`GEMINI_LIVE_MODEL=gemini-3.8-live`.

**Just the OpenAI paths.** With only `OPENAI_API_KEY`, `ONLY=openai` runs every check that needs
nothing else: the ai-summarize `openai` variant, JSON mode, the background job (polled, and by
webhook when `OPENAI_WEBHOOK_SECRET` is set), audio understanding with a spoken answer (`voice`:
the answer must say Paris, audio back, `audioPreserved: true`), the voice pipeline (transcribed,
answered by `gpt-4o-mini` in place of Anthropic, spoken; `audioPreserved: false`), the audio as a
media reference read from MinIO (and a wrong SHA-256, refused before any call), and an OpenAI
Realtime session through the adapter (the WAV up, audio and transcripts back, the key only in the
`Authorization` header). The cross-provider checks are skipped.

```bash
OPENAI_API_KEY=... ONLY=openai SUTRA_ENGINE_IMAGE=sutra-rust-engine:dev \
    SUTRA_BIN=$PWD/rust/target/debug/sutra examples/ai-advanced/live-check.sh
```

The model ids may not match what your account offers; each is overridable without editing
`channels.yaml` (the script substitutes them into the copy it deploys): `OPENAI_AUDIO_MODEL`
(default `gpt-audio-1.5`), `OPENAI_TRANSCRIBE_MODEL` (`gpt-transcribe`), `OPENAI_TTS_MODEL`
(`gpt-4o-mini-tts`) and `OPENAI_REALTIME_MODEL` (`gpt-realtime`). A model-not-found failure names
the variable to set.

**The webhook needs a tunnel.** OpenAI delivers webhooks to a public HTTPS URL, so:

1. start a tunnel to `127.0.0.1:18080` (any tunnelling tool);
2. in your OpenAI project, add a webhook endpoint `https://<tunnel-host>/llm/openai-hooks` for
   `response.completed`, and copy its signing secret;
3. `export OPENAI_WEBHOOK_SECRET=whsec_...` and run `ENGINE_HOST_PORT=18080
   examples/ai-advanced/live-check.sh` (the compose file publishes the engine on that fixed
   loopback port when `ENGINE_HOST_PORT` is set).

Without the secret the check is skipped. The package still loads either way: the compose file
always defines `OPENAI_WEBHOOK_SECRET` (an `env:` reference to an unset variable would fail the
channel at load), and with the placeholder no webhook verifies — each is answered `401` — while the
job still completes by its safety-net poll.

## Notes

- `mode: job` needs a database and the outbound default `ack-mode` (`on-persist`). The job is a
  row in `llm_job` from submit until its result is delivered; then the row is deleted.
- The webhook endpoint answers `401` for a bad signature and `503` when the job row cannot be
  claimed (so the provider retries); the same job is delivered once, whether the webhook or the
  poller gets there first.
- Batches are cheap and slow: they usually end in minutes but may take up to a day, so the batch
  flow's `<q:timeout>` is `PT24H`.
- Prompts and answers are never written to logs or spans, here or in the stub.
