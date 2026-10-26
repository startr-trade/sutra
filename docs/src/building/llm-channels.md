# Calling a language model

A process calls a language model the same way it calls any other counterpart: with a
**channel-call task**. The model sits behind an outbound channel with `transport: llm`. Whether
the call is synchronous or asynchronous is the channel's `ack-mode` (see
[Sync or async](#sync-or-async)). No BPMN element is specific to models, so everything a channel
call already has applies unchanged — `<q:timeout>`, `<q:retry>`, timer boundaries, error
boundaries.

## Sync or async

| Outbound `ack-mode` | Pattern | What happens |
|---|---|---|
| `on-complete` | **sync** | The model is called inline and the answer is merged into the instance's variables; the flow continues. No outbox row, no park, no database write for the hop, no `reply-channel`, no `<q:alias>`. |
| `on-persist` (the outbound default) | **async** | The request goes through the outbox, the instance parks, and the answer arrives as a message on `reply-channel`, resuming the instance by `<q:alias>` correlation. |

Only an `ack-mode` you declare on the outbound channel counts; leave it out and the channel is
async, exactly as before.

### Sync

```yaml
- name: summarize-sync
  direction: outbound
  transport: llm
  bind: "llm://summarize-sync"
  ack-mode: on-complete                 # no reply-channel
  llm:
    candidates:
      - { provider: anthropic, model: <model-id>, api-key: "secret:llm/anthropic" }
    timeout-seconds: 30
```

```xml
<serviceTask id="Summarize" implementation="channel:summarize-sync">
  <extensionElements>
    <q:timeout duration="PT45S"/>
  </extensionElements>
  <dataOutputAssociation>
    <assignment><from>payload.text</from><to>summary</to></assignment>
  </dataOutputAssociation>
</serviceTask>
```

The task sends its `requestBody` variable (see [The request](#the-request)). The answer is the
[reply message](#the-reply) below, and it reaches variables only through the task's output
mapping: `payload.text` is the answer text, `payload.finish`, `payload.usage.outputTokens`,
`payload.model` and so on are the rest; `header.x-llm-provider` and `header.x-llm-model` carry
the provider and model that answered. Anything you do not map is dropped.

Parallel branches' sync calls run concurrently; each is bounded by its `<q:timeout>`.

### Async

Add `reply-channel` and an inbound channel for the answer, and give the task a `<q:alias>` whose
value is unique per call in flight (the next section shows the pair). A channel-call to an async
channel with no `<q:alias>` is rejected at deploy time with
`SUTRA.DISPATCH.CHANNEL_CALL.ALIAS_REQUIRED`.

### Replying to the caller

When the process was started by (or is being resumed through) an inbound HTTP channel with
`ack-mode: on-complete`, the caller is waiting and the flow's `<q:reply>` answers it:

- A **relayed turn** (a message that resumes a parked instance) answers with the `<q:reply>` its
  own pass reaches, whether that reply is `continue="true"` before the instance parks again or
  sits on the way to the end. Replies the instance passed in earlier turns are not repeated. A
  turn that reaches no reply answers `200` with an empty body.
- A plain `<q:reply>` followed by a park answers nothing — the request is answered when the flow
  ends, or by a `continue="true"` reply.
- An inbound HTTP channel on `on-persist` answers `202`, with the reply as the receipt body.

## The two channels (async)

```yaml
channels:
  # The request side: an outbound channel the process calls.
  - name: summarize
    direction: outbound
    transport: llm
    bind: "llm://summarize"
    llm:
      reply-channel: summarize-reply        # where the answer is delivered
      candidates:                           # tried in order
        - provider: anthropic
          model: <model-id>
          api-key: secret:llm/anthropic
        - provider: openai
          model: <model-id>
          base-url: https://models.example.test/v1   # any compatible server
          api-key: env:LLM_FALLBACK_KEY
      system: "You summarize support tickets in three sentences."
      max-output-tokens: 512
      temperature: 0
      timeout-seconds: 60
      concurrency: 4
      fallback-on: [rate-limited, unavailable, timeout]
      require: [json-output]
    # ack-mode: on-persist is the outbound default

  # The reply side: an inbound channel the answer arrives on.
  - name: summarize-reply
    transport: local
    codec: json
```

| Key | Default | Meaning |
|---|---|---|
| `candidates` | *required* | Ordered `provider` + `model` pairs, each with optional `base-url` and `api-key`. `api-key` is a reference (`env:`, `secret:`, `vault:`, `${…}`), never a literal. A candidate may also name a `transcription-model`, used when a [live audio session](#live-audio-sessions-preview) is opened on it. |
| `reply-channel` | *required* unless `ack-mode: on-complete` | The inbound channel, in the same package, the answer is delivered on. A sync channel has none. |
| `mode` | `call` | `call` holds the request open; `job` submits and completes later (see [Jobs and webhooks](#jobs-and-webhooks)). Refused on `ack-mode: on-complete`. |
| `system` | none | System prompt. A request may override it. |
| `max-output-tokens`, `temperature` | provider default | Passed through. A request may override them. |
| `timeout-seconds` | `60` | Deadline for one candidate's call (for a job: the submit call). |
| `concurrency` | `4` | Calls in flight on this channel, per engine replica. |
| `fallback-on` | `[rate-limited, unavailable, timeout]` | Which transient failures move on to the next candidate. A rejected request or a filtered answer never does. |
| `require` | none | `tools`, `json-output`, `audio-input`, `audio-output`, `audio-understanding`, `video-understanding` or `realtime`: skip candidates that cannot do it (see [Audio, images and video](#audio-images-and-video)). Applies to every call on the channel. |
| `output-voice` | provider default | The voice of a spoken answer. A request's `outputVoice` overrides it. |
| `voice` | `native` | `native` sends audio to the model as audio; `pipeline` transcribes it, asks a text model, and speaks the answer (see [The voice pipeline](#the-voice-pipeline)). |
| `speech` | none | The speech provider of a `voice: pipeline` channel: `provider`, `api-key`, `base-url`, `transcribe-model`, `speech-model`, optional `voice` and `language`. |
| `media-store` | none | The object store (`sutra.object-stores.<name>`, engine configuration) this channel reads [media references](recordings-and-media.md#media-references-in-a-model-call) from. Without it, a request carrying a reference is refused. |
| `media-max-bytes` | `20971520` (20 MiB) | The largest object one media reference may name. |

## Providers

| `provider:` | Service | Default endpoint | Notes |
|---|---|---|---|
| `openai` | OpenAI | `https://api.openai.com/v1` | Also any compatible server via `base-url`. With an audio chat model (a model id containing `audio`), audio in and spoken answers out. |
| `anthropic` | Anthropic | `https://api.anthropic.com` | |
| `aws-bedrock` | Amazon Bedrock (Converse API) | none — set `base-url: https://bedrock-runtime.<region>.amazonaws.com` | Authenticates with a Bedrock API key; IAM request signing is not supported. |
| `gemini` | Google Gemini, native API | `https://generativelanguage.googleapis.com` | Gemini's own `generateContent` API: audio, images and video in, JSON answers, tools, streaming. The key travels in the `x-goog-api-key` header. |
| `google-gemini` | Google Gemini | `https://generativelanguage.googleapis.com/v1beta/openai` | Gemini's Chat Completions-compatible endpoint: text only. Use `gemini` for audio or video. |
| `mistral` | Mistral AI | `https://api.mistral.ai/v1` | |
| `deepseek` | DeepSeek | `https://api.deepseek.com` | |
| `xai` | xAI | `https://api.x.ai/v1` | |
| `kimi` | Moonshot AI (Kimi) | `https://api.moonshot.ai/v1` | |
| `meta-llama` | Meta Llama API | `https://api.llama.com/compat/v1` | |

Every service except `anthropic`, `aws-bedrock` and `gemini` speaks the Chat Completions wire
format and is served by one adapter; the `provider:` id only picks the default endpoint. Override `base-url` for
a regional host, a proxy, or a self-hosted server.

Which providers exist depends on how the engine binary was built (`llm-openai`, `llm-anthropic`,
`llm-bedrock`, `llm-gemini` cargo features; the standard image includes all four). A channel naming a provider
the binary does not include fails at load with `SUTRA.LLM.PROVIDER.UNKNOWN`.

## Jobs and webhooks

Some providers accept a request and finish it later (background responses, batch jobs). With
`mode: job` the channel submits the request, the provider holds the work, and the result is
delivered on `reply-channel` when it completes — found by polling, by a webhook, or both. The
flow is the async one above: send, park, resume by `<q:alias>`. Jobs need `ack-mode: on-persist`
and an engine with a database.

```yaml
- name: summarize
  direction: outbound
  transport: llm
  bind: "llm://summarize"
  llm:
    mode: job
    reply-channel: summarize-reply
    candidates:
      - provider: openai
        model: <model-id>
        api-key: secret:llm/openai
        completion: callback              # the provider notifies us
        callback-channel: openai-hooks
        poll-interval-seconds: 300        # safety net: a webhook can be lost or late
        job-timeout-seconds: 86400
      - provider: anthropic
        model: <model-id>
        api-key: secret:llm/anthropic
        completion: poll                  # no webhooks: poll
        poll-interval-seconds: 30

# The webhook endpoint: an inbound llm channel.
- name: openai-hooks
  transport: llm
  bind: "POST /llm/openai"                # always POST /llm/<path>, on the shared HTTP listener
  llm:
    callback-for: openai
    callback-secret: secret:llm/openai-webhook
```

| Key | Where | Default | Meaning |
|---|---|---|---|
| `completion` | candidate | `poll` | `poll` or `callback`. |
| `callback-channel` | candidate | none | Required with `completion: callback`, and only then. Names an inbound `transport: llm` channel in the same package whose `callback-for` is this candidate's provider. |
| `poll-interval-seconds` | candidate | `30` (`poll`), `300` (`callback`) | Poll cadence; with `callback` it is a safety-net sweep. |
| `job-timeout-seconds` | candidate | `86400` | A job not finished by then is marked failed. |
| `callback-secret` | candidate or callback channel | none | Secret reference used to verify the webhook signature. A callback is accepted if the callback channel's secret or any referencing candidate's secret verifies, so rotation is a two-step change. At least one is required. |
| `callback-for` | callback channel | none | Required: the provider whose webhooks this endpoint takes. |

The candidate job keys are refused on a `mode: call` channel. Candidates that cannot do jobs are
skipped at submit, like a candidate that lacks a `require:` capability.

The webhook endpoint answers `404` for an unknown path, `401` for a bad signature, `400` for a
body the provider adapter cannot parse, `503` when the job row cannot be claimed or the database is
unavailable (so the provider retries the webhook), and `200` otherwise (also when it had nothing
left to deliver — the same job is delivered once, whether the poller or the webhook finishes it
first).
A job that fails, is rejected, or passes `job-timeout-seconds` delivers nothing and logs
`SUTRA.LLM.CALL.FAILED`; the flow's own `<q:timeout>` handles the missing answer. Broker-routed
callbacks are not supported.

Misconfiguration fails at load with `SUTRA.LLM.CONFIG.INVALID`: `mode: job` on an `on-complete`
channel or without a database, no candidate that supports jobs, `completion: callback` on a
provider without webhooks, a missing or dangling `callback-channel`, a provider mismatch, no
secret, or a callback bind that is not `POST /llm/<path>` or reuses a path.

### What each provider supports

| `provider:` | Streaming | Jobs | Webhooks |
|---|---|---|---|
| `openai` | yes | yes | yes |
| `anthropic` | yes | yes (polled) | no |
| `gemini` | yes | no | no |
| `google-gemini`, `mistral`, `deepseek`, `xai`, `kimi`, `meta-llama` (Chat Completions services) | yes | no | no |
| `aws-bedrock` | no | no | no |

## Streaming to the caller

A token stream means nothing inside a flow — a variable is useful only once complete — but it
matters to the caller waiting on the last step. Two switches turn it on:

```yaml
- name: ask
  transport: http
  bind: "POST /ask"
  ack-mode: on-complete
  reply-stream: sse            # inbound HTTP channel: answer as server-sent events
```

```xml
<serviceTask id="Answer" implementation="channel:summarize-sync" q:stream="true"> ... </serviceTask>
```

The call must go to a sync (`ack-mode: on-complete`) `transport: llm` channel, and the provider
must support streaming. The response becomes `text/event-stream`:

```
event: chunk
data: Hel

event: chunk
data: lo

event: reply
data: {"summary":"Hello"}
```

| Event | Data | Meaning |
|---|---|---|
| `chunk` | a piece of the answer text | Repeats. A multi-line piece uses one `data:` line per line. |
| `reset` | empty | A candidate failed mid-answer and the next one started over: discard the chunks received so far. |
| `reply` | the flow's reply body | The flow finished; the stream then closes. |
| `error` | the diagnostic code only (never its message) | The flow failed; the stream then closes. |

Chunks go to the caller only. The flow continues with the whole accumulated answer in variables
and audit, exactly as for a non-streamed call. If the flow streams nothing, or the channel has no
`reply-stream`, the request is answered as an ordinary response and `q:stream` is ignored.

Deploy-time errors: `SUTRA.CONFIG.STREAM.TARGET_NOT_SYNC_LLM` for `q:stream="true"` on anything
but a channel-call to a sync `llm` channel, and `SUTRA.CONFIG.STREAM.REPLY_STREAM_UNSUPPORTED`
for `reply-stream: sse` on anything but an inbound `http` channel with `ack-mode: on-complete`.

## The request

The channel-call task's input is a JSON object:

| Field | Meaning |
|---|---|
| `prompt` | A single user message. |
| `messages` | A conversation: `[{ "role": "user" \| "assistant" \| "tool", "content": "…", "toolCallId": "…" }]`. Use `prompt`, `messages`, or both (`prompt` is appended). |
| `system` | Overrides the channel's `system`. |
| `json` | `true` asks for a JSON object as the answer. |
| `maxOutputTokens`, `temperature` | Override the channel's values. |
| `tools` | `[{ "name", "description", "parameters": <JSON Schema> }]` the model may call. |
| `messages[].audio` | Audio on a user turn: `{ "data": <base64>, "format": <media type> }`. See [Audio, images and video](#audio-images-and-video). |
| `messages[].video` | Video on a user turn: a clip `{ "data", "format": "video/…" }`, or sampled frames `{ "frames": [{ "data", "format": "image/…", "atMs" }] }`. |
| `messages[].image` | One image on a user turn, `{ "data", "format": "image/…" }`, read as a one-frame video (the candidate needs `video-understanding`). |
| `ref`, `sha256` | Any audio, video, frame or image part may give `{ "ref": <object URI>, "sha256": <hex> }` instead of `data`: a [media reference](recordings-and-media.md#media-references-in-a-model-call). |
| `audioOutput` | Ask for a spoken answer in this format, for example `"audio/L16;rate=8000"`. |
| `outputVoice` | The spoken answer's voice; overrides the channel's `output-voice`. |

Build the prompt in the process — a template task, a FEEL string, or a `<q:param>` — so it is
versioned with the flow that uses it.

## The reply

The answer is this JSON message — delivered on `reply-channel` for an async call, the response
itself for a sync one:

```json
{
  "text": "…",
  "toolCalls": [{ "id": "…", "name": "…", "arguments": { } }],
  "finish": "stop | length | tool-calls | filtered | …",
  "usage": { "inputTokens": 812, "outputTokens": 96 },
  "provider": "anthropic",
  "model": "<model-id>",
  "providerRequestId": "…",
  "request": { }
}
```

`request` echoes the request body, so an async task's `<q:alias>` can correlate on a field the
process put there (for example `body.request.ticketId`). The headers of the request ride along
too, plus `x-llm-provider` and `x-llm-model`.

A `finish` of `filtered` means the provider withheld the answer. It is a result, not a failure:
route on it with a gateway rather than retrying.

**Structured answers.** A call that asked for JSON (`json: true`) adds `json`: the answer parsed
into an object or array (a fenced code block is unwrapped), so the flow reads its fields
directly, as `payload.json.category`, instead of parsing `text`. When the answer does not parse,
`json` is absent and `jsonError` says why: either the answer was cut off at `max-output-tokens`
(`finish: "length"`), or it is not a JSON object or array. Reasoning models such as
`gemini-2.5-flash` think before they answer, and the thinking counts against
`max-output-tokens`, so give a JSON channel on such a model room (the examples use 2048).

**Audio.** A call involving audio adds `audio` (`{ "data": <base64>, "format": <the audioOutput
asked for> }`), `transcript` (what the spoken answer says) and `audioPreserved` (`true` when the
model heard the audio itself, `false` when it went through the [pipeline](#the-voice-pipeline)).
In the echoed `request`, every audio or video part keeps its `format` and has its `data` replaced
by `bytes` (the decoded size), so the reply never carries the media back.

## Audio, images and video

> **Preview.** Video understanding (clips, frames, images) and
> [live audio sessions](#live-audio-sessions-preview) are **preview** in 0.6: they have been run
> against the real Gemini and OpenAI APIs as well as against in-process stand-ins of them, and
> their shapes may still change. Audio on a turn-based call (`audio`, `audioOutput`,
> `voice: pipeline`) is not preview.

**Audio stays audio.** A user turn's audio goes to the model as audio, so an audio-native model
hears the voice itself (tone, stress, hesitation), not a transcript of it. Speech-to-text in
front of a text model happens only when a channel asks for it with
[`voice: pipeline`](#the-voice-pipeline).

```yaml
- name: voice-model
  direction: outbound
  transport: llm
  bind: "llm://voice-model"
  ack-mode: on-complete
  llm:
    candidates:
      - provider: openai
        model: <audio-model-id>          # an audio chat model: its id contains `audio`
        api-key: env:OPENAI_API_KEY
    require: [audio-understanding]       # skip any candidate that would flatten the voice
    output-voice: alloy
```

The request names the format it has and the format it wants back:

```json
{
  "messages": [{ "role": "user", "content": "Answer the caller.",
                 "audio": { "data": "<base64>", "format": "audio/PCMU" } }],
  "audioOutput": "audio/L16;rate=8000"
}
```

**Formats are media types.** `audio/wav`; `audio/L16;rate=<hz>[;channels=<n>]` (also written
`audio/pcm;rate=<hz>`: signed 16-bit little-endian, mono unless stated); `audio/PCMU` and
`audio/PCMA` (G.711 µ-law and A-law, 8 kHz); and the compressed `audio/mpeg`, `audio/flac`,
`audio/opus`, `audio/ogg`, `audio/aac`, `audio/webm` and `audio/mp4`. The transport converts
between the call's formats and the provider's in-process: WAV wrap and unwrap, G.711, channel
mixing and linear resampling. A compressed format is passed through, never converted. If the
provider does not take it as it is, the call fails with `SUTRA.LLM.AUDIO.UNSUPPORTED` in the
diagnostic.

**Audio is never dropped.** A candidate that cannot take what the call carries (audio input, a
spoken answer, video) is skipped, exactly like a candidate without tools. If none can, the call
fails permanently. A `mode: job` channel carries no audio or video and refuses such a call.

| `require:` value | A candidate must… |
|---|---|
| `audio-input` | accept audio on a message |
| `audio-output` | answer in audio |
| `audio-understanding` | hear the voice itself (emotion, tone), not a transcript |
| `video-understanding` | read video: a clip, sampled frames or an image |
| `realtime` | open a [live audio session](#live-audio-sessions-preview) |

| `provider:` | Audio in | Spoken answer | Images and video |
|---|---|---|---|
| `openai`, with an audio chat model (id containing `audio`) | WAV, MP3 | WAV, MP3, FLAC, Opus, AAC | no |
| `gemini` | WAV, MP3, FLAC, AAC, Ogg, Opus, M4A, WebM | no | clips, frames, images |
| every other provider | no | no | no |

**Images and video** (preview). A user turn may carry `video`, either a clip (`{ "data",
"format": "video/mp4" }`) or a run of sampled frames (`{ "frames": [{ "data", "format":
"image/png", "atMs": 0 }, …] }`), or one `image` (`{ "data", "format": "image/png" }`), which is
read as a one-frame video. Only `gemini` reads them today. Combined with `json: true`, the flow
gets the answer parsed under `json`:

```yaml
- name: vision-model
  direction: outbound
  transport: llm
  bind: "llm://vision-model"
  ack-mode: on-complete
  llm:
    candidates:
      - provider: gemini
        model: <gemini-model-id>
        api-key: env:GEMINI_API_KEY
    require: [video-understanding, json-output]
```

### The voice pipeline

For a model without audio, or for cost, a channel can transcribe, answer in text and speak the
answer, explicitly:

```yaml
  llm:
    voice: pipeline
    speech:
      provider: openai
      api-key: env:OPENAI_API_KEY
      transcribe-model: <transcription-model-id>
      speech-model: <speech-model-id>
      voice: alloy                  # optional; a request's outputVoice overrides it
      language: en                  # optional transcription hint
    candidates:
      - provider: anthropic
        model: <model-id>
        api-key: env:ANTHROPIC_API_KEY
```

Each audio turn is transcribed by `speech` and becomes text, appended to the turn's `content`.
The candidates answer as text. With `audioOutput`, `speech` speaks the answer, and the reply's
`transcript` is the answer text. The reply then says **`audioPreserved: false`**: the voice never
reached the model, so a flow that needs it (an emotion analysis, say) can refuse the answer. A
native answer says `audioPreserved: true`. A text-only call on a pipeline channel is not touched.

`voice` is `native` (the default) or `pipeline`. `llm.speech` is required with `pipeline` and
refused without it. The speech provider must do both transcription and speech synthesis (today,
`openai`). A pipeline channel cannot `require` an audio capability and cannot be `mode: job`.
Each of these is a `SUTRA.LLM.CONFIG.INVALID` at load.

### Media references

Any `audio`, `video` (the clip, or each frame) or `image` part may name an object in an object
store, `{ "ref": "s3://…", "sha256": "<hex>" }`, instead of inlining `data`. The channel's
`media-store` names the store it may read. The transport reads the object, checks its SHA-256
and size, and sends it as if it had been inline; variables, snapshots and audit only ever hold
the reference. [Recordings and media references](recordings-and-media.md) covers the object
store, the refusals and a walkthrough.

### Try it: audio, the pipeline and vision

This runs three flows of the `examples/ai-advanced` package against the stub model server:

| Flow | Channel | Model channel | Candidate |
|---|---|---|---|
| `shared/bpmn/voice.bpmn` | `POST /channels/voice` | `voice-model` | `openai`, audio model, `require: [audio-understanding]` |
| `shared/bpmn/voice-pipeline.bpmn` | `POST /channels/voice-pipeline` | `voice-pipeline-model` | `voice: pipeline`: `openai` speech, `anthropic` text model |
| `shared/bpmn/vision.bpmn` | `POST /channels/vision` | `vision-model` | `gemini`, `require: [video-understanding, json-output]` |

**Prerequisites:** [Before you try a walkthrough](../whats-new-0-6.md#before-you-try-a-walkthrough).
No key is needed. In `variants/live/channels.yaml` the same channels name real models, with
their keys as `env:OPENAI_API_KEY`, `env:ANTHROPIC_API_KEY` and `env:GEMINI_API_KEY`, resolved
from the engine's environment.

The vision flow builds its request in one FEEL data task. It sends either the image or the
frames, and asks for JSON:

```xml
<bpmn:assignment>
  <bpmn:from>{messages: [if payload.image != null then {role: "user", content: "Describe the image as one JSON object: scene (one sentence), objects (a list of short names) and framesSeen (1).", image: payload.image} else {role: "user", content: "These are the frames of one short clip, in order. Describe the whole clip as ONE JSON object, not one per frame: scene (one sentence), objects (a list of short names) and framesSeen (how many frames you received).", video: payload.video}], json: true}</bpmn:from>
  <bpmn:to>requestBody</bpmn:to>
</bpmn:assignment>
```

It then reads the parsed answer's fields directly:

```xml
<bpmn:from>{requestId: payload.requestId, scene: answer.json.scene, objects: answer.json.objects, framesSeen: answer.json.framesSeen, provider: answer.provider}</bpmn:from>
```

**1. Package the stub variant and start the stack** (engine, PostgreSQL, the stub):

```bash
C=examples/ai-advanced/deploy/compose.yaml
sutra package examples/ai-advanced/variants/stub --overlay examples/ai-advanced/shared \
    --out examples/ai-advanced/deploy/deployments
docker compose -f $C up -d --wait
PORT=$(docker compose -f $C port engine 8080 | cut -d: -f2)
H=(-H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-advanced-key')
until curl -sf "http://127.0.0.1:$PORT/sutra/deployments" | grep -q '"phase":"Active"'; do sleep 1; done
```

`sutra package` prints one `SUTRA.CHANNEL.NO_SCHEMA` warning per schema-less JSON channel, and
then:

```text
packaged examples/ai-advanced/deploy/deployments/stub.sutra (deploymentId dep-8846c16308ee3b0f46c89bbd)
```

**2. Audio in, a spoken answer out.** The sample recording is a quarter-second WAV. The request
asks for 16 kHz linear PCM back. The helper shortens the returned audio for display:

```bash
AUDIO=$(base64 -w0 examples/ai-advanced/sample/question.wav)
short() { python3 -c 'import json,sys; r=json.load(sys.stdin); r["audio"]["data"]=r["audio"]["data"][:12]+"..."; print(json.dumps(r, indent=2))'; }
curl -s -X POST "http://127.0.0.1:$PORT/channels/voice" "${H[@]}" \
    -d "{\"requestId\":\"Q-1\",\"audio\":{\"data\":\"$AUDIO\",\"format\":\"audio/wav\"},\"audioOutput\":\"audio/L16;rate=16000\"}" | short
```

```json
{
  "audio": {
    "data": "oA+gD6APoA+g...",
    "format": "audio/L16;rate=16000"
  },
  "audioPreserved": true,
  "provider": "openai",
  "requestId": "Q-1",
  "text": "STUB SPOKEN ANSWER: your appointment is confirmed for Tuesday.",
  "transcript": "STUB SPOKEN ANSWER: your appointment is confirmed for Tuesday."
}
```

The stub spoke a 24 kHz WAV, and the transport unwrapped and resampled it to the
`audio/L16;rate=16000` the request asked for.

**3. The same question through the pipeline**, asking for G.711 µ-law back:

```bash
curl -s -X POST "http://127.0.0.1:$PORT/channels/voice-pipeline" "${H[@]}" \
    -d "{\"requestId\":\"Q-2\",\"audio\":{\"data\":\"$AUDIO\",\"format\":\"audio/wav\"},\"audioOutput\":\"audio/PCMU\"}" | short
```

```json
{
  "audio": {
    "data": "r6+vr6+vr6+v...",
    "format": "audio/PCMU"
  },
  "audioPreserved": false,
  "provider": "anthropic",
  "requestId": "Q-2",
  "text": "STUB SUMMARY: the customer reports a problem and asks for it to be looked at.",
  "transcript": "STUB SUMMARY: the customer reports a problem and asks for it to be looked at."
}
```

The text model (Anthropic) answered, and `audioPreserved: false` says it never heard the voice.

**4. One image to the native Gemini provider:**

```bash
IMG=$(base64 -w0 examples/ai-advanced/sample/frame-1.png)
curl -s -X POST "http://127.0.0.1:$PORT/channels/vision" "${H[@]}" \
    -d "{\"requestId\":\"V-1\",\"image\":{\"data\":\"$IMG\",\"format\":\"image/png\"}}"; echo
```

```json
{"framesSeen":1,"objects":["bicycle","door"],"provider":"gemini","requestId":"V-1","scene":"STUB SCENE: a bicycle leans against a blue door."}
```

**5. Two sampled video frames:**

```bash
F2=$(base64 -w0 examples/ai-advanced/sample/frame-2.png)
curl -s -X POST "http://127.0.0.1:$PORT/channels/vision" "${H[@]}" \
    -d "{\"requestId\":\"V-2\",\"video\":{\"frames\":[{\"data\":\"$IMG\",\"format\":\"image/png\",\"atMs\":0},{\"data\":\"$F2\",\"format\":\"image/png\",\"atMs\":500}]}}"; echo
```

```json
{"framesSeen":2,"objects":["bicycle","door"],"provider":"gemini","requestId":"V-2","scene":"STUB SCENE: a bicycle leans against a blue door."}
```

`scene`, `objects` and `framesSeen` came from `answer.json`: the flow never parsed `text`.

**6. What the stub was asked**, by route:

```bash
docker compose -f $C exec model-stub wget -qO- http://127.0.0.1:8000/stats; echo
```

```json
{"chat": 1, "chat-audio-in": 1, "chat-audio-out": 1, "transcriptions": 1, "messages": 1, "speech": 1, "gemini-generate": 2}
```

The native call is one audio chat completion. The pipeline is one transcription, one Anthropic
message and one speech synthesis. The two vision calls are two Gemini `generateContent` requests.

**7. Clean up:**

```bash
docker compose -p sutra-ai-advanced down -v
```

### Live audio sessions (preview)

A provider adapter can also open a **live audio session** (`AudioSession`): audio frames in,
transcript deltas and the model's audio and text out. The `openai` adapter does it over the
Realtime WebSocket API for a model id containing `realtime`, and the `gemini` adapter does it over
the Live API for a model id containing `live`. The `gemini` adapter never puts the API key in a
URL. It mints a single-use ephemeral token with the key (sent in the `x-goog-api-key` header) and
opens the session with that token in the handshake's `Authorization` header.

A process never opens a session. A **media transport** does, such as the telephony transport of
[sutra-voice](https://github.com/startr-trade/sutra-voice), and it opens it through an `llm`
channel of its own deployment, so it gets the same routing as a process's call (ordered
candidates, `fallback-on`, `require:`, key references, `concurrency`, and the `sutra.llm.call`
span, with `session = true`). The media goes from that transport to the provider directly and
never passes through a process. Such a channel answers inline, so it declares
`ack-mode: on-complete` and no reply channel:

```yaml
  - name: live-transcription
    direction: outbound
    transport: llm
    bind: "llm://live-transcription"
    ack-mode: on-complete
    llm:
      require: [realtime]            # only candidates with live audio sessions
      concurrency: 16                # an open session holds one permit for its life
      candidates:
        - provider: openai
          model: <realtime-model-id>
          transcription-model: <transcription-model-id>
          api-key: env:OPENAI_API_KEY
        - provider: gemini           # the fallback
          model: <live-model-id>
          api-key: env:GEMINI_API_KEY
```

A session opens on the first candidate that has `realtime` and what `require:` names. A failure
listed in `fallback-on` moves it to the next candidate, exactly as for a call. A transport may
also ask for a **transcription-only** session: on `openai` that is a Realtime transcription
session, which reports the start and end of each stretch of speech as offsets into the audio.
Gemini Live has no listen-only session, so it is asked to stay silent and anything it says is
ignored. A `mode: job` channel cannot serve a transport.

**Turn detection.** A session either lets the provider's voice-activity detection decide where
the caller's turn ends (the default), or the transport marks each turn itself (manual turns).
With the `gemini` adapter, provider detection runs at a low end-of-speech sensitivity with 800 ms
of silence, so a pause inside a sentence does not end the turn. Provider detection is tuned for
real speech: in our checks, `gemini-3.8-live` took only the first word of a synthetic (text-to-
speech) question for speech and answered that, while it handled a recorded human voice, and
`gemini-2.5-flash-native-audio-latest` handled both. Manual turns handled every case. If your
callers' audio is unusual (synthetic prompts, heavy noise), test with your own recordings or use
manual turns.

There is no walkthrough here, because a live session needs a media transport to open it and the
engine in this repository ships none. The sutra-voice repository runs it end to end.
`examples/ai-advanced/live-check.sh` checks the adapters themselves against the real APIs.

## Failures

| What happened | Sync (`on-complete`) | Async (`on-persist`) |
|---|---|---|
| Every candidate failed transiently, or the call timed out | With `<q:retry>`: the call is re-issued inline with the policy's backoff, all within the `<q:timeout>` budget; once spent, the task fails. Without it: a timeout fires the task's `#timeout` boundary; any other failure fails the instance. | The delivery is retried with the outbox's backoff. If it never succeeds, the task's `<q:timeout>` fires, or the outbox attempt ceiling poisons it — both are retryable failures under `<q:retry>`. |
| The request was rejected (bad key, unknown model, malformed request) | A permanent failure: no retry; the task fails (an error boundary can catch it). | The delivery is poisoned (`SUTRA.LLM.CALL.FAILED`), which `<q:retry>` treats as a channel-call failure. |

A sync retry never parks a durable backoff; a flow that needs one should use the async channel.

Duplicate calls. Async delivery is at-least-once: a crash after the model answered but before
the delivery was recorded calls the model again; inbox deduplication absorbs the duplicate reply,
but the call is paid for twice (a job channel likewise risks a duplicate provider job if the
submit's record fails). A sync call lives only in memory: a crash mid-call loses it, and the
client's retry re-runs the flow from the start, calling the model again. Sync calls made by a
timer or scheduled start run inline too; only a broadcast fan-out degrades to the async path,
with a `SUTRA.CHANNEL.SYNC_CALL_DEGRADED` warning.

## Tools and agent loops

The transport never runs tools. When `finish` is `tool-calls`, route the token to tasks that run
the requested tools (other channels), append their results to `messages` as `tool` turns, and
loop back to the model task. Bound the loop with a counter and a non-interrupting timer boundary.

## What is not logged

Prompts and answers are never written to spans, logs or diagnostics. Each call opens a
`sutra.llm.call` span carrying the channel, provider, model, finish reason and token counts.

## Examples

Runnable packages in the source tree. The two `ai-` examples keep their flows once under
`shared/` and one `channels.yaml` per provider under `variants/`, composed by
`sutra package --overlay`:

| Example | Variants | Flows |
|---|---|---|
| `examples/ai-summarize` | `stub-openai`, `stub-anthropic`, `openai`, `anthropic`, `gemini` | sync, streamed (SSE), async |
| `examples/ai-advanced` | `stub`, `live` | cross-provider fallback (Anthropic first, OpenAI second), JSON mode, a tool call, an OpenAI background job (polled), an OpenAI background job (webhook), an Anthropic Message Batch, audio in and out, the voice pipeline, images and video frames to native Gemini |
| `examples/media-recordings` | — | a stored recording read by media reference from an object store, audited by reference, deleted on erasure and retention ([walkthrough](recordings-and-media.md)) |

```bash
sutra package examples/ai-summarize/variants/anthropic --overlay examples/ai-summarize/shared \
    --out examples/ai-summarize/deploy/deployments
sutra package examples/ai-advanced/variants/stub --overlay examples/ai-advanced/shared \
    --out examples/ai-advanced/deploy/deployments
```

They share a stub model server (`examples/ai-summarize/stub/model-stub.py`) that speaks the
OpenAI, Anthropic and native Gemini wire formats with fixed answers, including streaming, tools,
JSON mode, audio, speech, transcription, background jobs, batches, signed webhooks and a
switchable outage for fallback.

- **CI (tier 2, no keys):** the `stub-openai`, `stub-anthropic` and ai-advanced `stub` variants,
  composed through the overlay path, run against the stub on a real engine and PostgreSQL — every
  flow above except `vision` (exercised by the walkthrough in this chapter), with the replies, the SSE frames, the sink deliveries and the `instance_state` and
  `llm_job` rows checked. Every variant, the real-provider ones included, is also linted in tier 1.
- **Live (your keys):** `examples/ai-advanced/live-check.sh` runs the `openai`, `anthropic` and
  `gemini` variants and the ai-advanced `live` variant against the real APIs, including a fallback
  where both the failing Anthropic candidate and the answering OpenAI one are real, and — with a
  tunnel and `OPENAI_WEBHOOK_SECRET` — a webhook-completed job.
