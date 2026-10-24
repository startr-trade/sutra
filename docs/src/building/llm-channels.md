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
| `candidates` | *required* | Ordered `provider` + `model` pairs, each with optional `base-url` and `api-key`. `api-key` is a reference (`env:`, `secret:`, `vault:`, `${…}`), never a literal. |
| `reply-channel` | *required* unless `ack-mode: on-complete` | The inbound channel, in the same package, the answer is delivered on. A sync channel has none. |
| `mode` | `call` | `call` holds the request open; `job` submits and completes later (see [Jobs and webhooks](#jobs-and-webhooks)). Refused on `ack-mode: on-complete`. |
| `system` | none | System prompt. A request may override it. |
| `max-output-tokens`, `temperature` | provider default | Passed through. A request may override them. |
| `timeout-seconds` | `60` | Deadline for one candidate's call (for a job: the submit call). |
| `concurrency` | `4` | Calls in flight on this channel, per engine replica. |
| `fallback-on` | `[rate-limited, unavailable, timeout]` | Which transient failures move on to the next candidate. A rejected request or a filtered answer never does. |
| `require` | none | `tools` and/or `json-output`: skip candidates that cannot do it. |

## Providers

| `provider:` | Service | Default endpoint | Notes |
|---|---|---|---|
| `openai` | OpenAI | `https://api.openai.com/v1` | Also any compatible server via `base-url`. |
| `anthropic` | Anthropic | `https://api.anthropic.com` | |
| `aws-bedrock` | Amazon Bedrock (Converse API) | none — set `base-url: https://bedrock-runtime.<region>.amazonaws.com` | Authenticates with a Bedrock API key; IAM request signing is not supported. |
| `google-gemini` | Google Gemini | `https://generativelanguage.googleapis.com/v1beta/openai` | Gemini's compatible endpoint. |
| `mistral` | Mistral AI | `https://api.mistral.ai/v1` | |
| `deepseek` | DeepSeek | `https://api.deepseek.com` | |
| `xai` | xAI | `https://api.x.ai/v1` | |
| `kimi` | Moonshot AI (Kimi) | `https://api.moonshot.ai/v1` | |
| `meta-llama` | Meta Llama API | `https://api.llama.com/compat/v1` | |

Every service except `anthropic` and `aws-bedrock` speaks the Chat Completions wire format and is
served by one adapter; the `provider:` id only picks the default endpoint. Override `base-url` for
a regional host, a proxy, or a self-hosted server.

Which providers exist depends on how the engine binary was built (`llm-openai`, `llm-anthropic`,
`llm-bedrock` cargo features; the standard image includes all three). A channel naming a provider
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
