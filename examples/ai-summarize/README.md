# ai-summarize — a process that calls a language model, synchronously and asynchronously

A worked deployment package for the v0.5.0 `llm` transport, in five provider variants. A support ticket is POSTed as JSON; a
FEEL data task builds a prompt from it; a **channel-call task** sends the prompt to a model behind
an outbound `transport: llm` channel. No BPMN element is specific to models — the call is an
ordinary channel call, so `<q:timeout>`, `<q:retry>` and boundaries all apply.

The same model is called two ways, and the outbound channel's `ack-mode` is what picks:

```
SYNC   POST /channels/ticket
          ticket (on-complete) ─► BuildPrompt ─► Summarize ──────────────► BuildReply ─► End
                                                  channel:summarize-sync                  │
                                                  (ack-mode: on-complete: inline)         ▼
                                                                          200 { ticketId, summary }

ASYNC  POST /channels/ticket-async
          ticket-async (on-persist) ─► 202 Accepted at once
          BuildPrompt ─► Summarize ─► (parked) ─► BuildNote ─► Deliver ─► End
                          channel:summarize-async    ▲           q:send summary-sink
                          request via the outbox     │
                                  answer on summarize-reply, correlated by body.request.ticketId
```

| | sync | async |
|---|---|---|
| Inbound channel | `ticket`, `ack-mode: on-complete` | `ticket-async`, `ack-mode: on-persist` |
| Outbound model channel | `summarize-sync`, `ack-mode: on-complete` | `summarize-async` (default `on-persist`) |
| `reply-channel` | none — the answer is the call's return value | `summarize-reply` (an inbound `local` / `json` channel) |
| Instance while the model works | in memory, no row, no outbox | durably parked |
| Caller sees | the summary on the same connection | `202`, then the summary is POSTed to `summary-sink` |
| Correlation | none needed | `<q:alias expression="body.request.ticketId">` — the reply echoes the request under `request` |

## Layout

The flows live once; each provider is one small layer on top. `sutra package <variant> --overlay
shared` composes the two into one package (the variant's files win; an overlay layer may not carry
`package.yaml`, so each variant has its own copy, with the same labels).

| Path | What it is |
|---|---|
| `shared/bpmn/` | `summarize-sync.bpmn`, `summarize-stream.bpmn`, `summarize-async.bpmn` (with diagram interchange) |
| `variants/stub-openai/` | `package.yaml` + `channels.yaml` (commented): the stub, OpenAI Chat Completions wire. The default. |
| `variants/stub-anthropic/` | the same, over the Anthropic Messages wire |
| `variants/openai/`, `variants/anthropic/`, `variants/gemini/` | the real services (`gpt-4o-mini`, `claude-haiku-5-5`, `gemini-2.5-flash`) |
| `stub/model-stub.py` | a Python 3 stdlib server with fixed answers: the OpenAI and Anthropic wire formats (incl. streaming, tools, JSON mode, jobs, batches, webhooks) plus a `/sink` that records. Shared with `examples/ai-advanced`. |
| `deploy/compose.yaml` | engine + PostgreSQL + the stub, project name `sutra-ai-summarize` |

The variants differ only in the two `candidates` blocks of `channels.yaml`:

| Variant | `provider` | `model` | `base-url` | `api-key` |
|---|---|---|---|---|
| `stub-openai` | `openai` | `test-model` | `http://model-stub:8000/v1` | `env:LLM_API_KEY` |
| `stub-anthropic` | `anthropic` | `test-model` | `http://model-stub:8000` | `env:LLM_API_KEY` |
| `openai` | `openai` | `gpt-4o-mini` | default | `env:OPENAI_API_KEY` |
| `anthropic` | `anthropic` | `claude-haiku-5-5` | default | `env:ANTHROPIC_API_KEY` |
| `gemini` | `google-gemini` | `gemini-2.5-flash` | default | `env:GEMINI_API_KEY` |

## Run it against the stub

```bash
# 1. Compose the variant over the shared flows and seal it into the directory the engine watches.
#    Package ONE variant there at a time: they all bind the same /channels/... paths.
sutra package examples/ai-summarize/variants/stub-openai --overlay examples/ai-summarize/shared \
    --out examples/ai-summarize/deploy/deployments

# 2. Start the engine, its database and the stub.
docker compose -f examples/ai-summarize/deploy/compose.yaml up -d
PORT=$(docker compose -f examples/ai-summarize/deploy/compose.yaml port engine 8080 | cut -d: -f2)

# 3. Sync: the summary comes back on the same request.
curl -s -X POST "http://127.0.0.1:$PORT/channels/ticket" \
  -H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-summarize-key' \
  -d '{"ticketId":"T-1001","subject":"Cannot log in","description":"Password reset email never arrives."}'
# => {"summary":"STUB SUMMARY: the customer reports a problem and asks for it to be looked at.","ticketId":"T-1001"}

# 3b. Streamed: the same summary as server-sent events, one chunk per piece, then the reply.
curl -s -N -X POST "http://127.0.0.1:$PORT/channels/ticket-stream" \
  -H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-summarize-key' \
  -d '{"ticketId":"T-1003","subject":"Slow app","description":"Pages take ten seconds."}'
# => event: chunk / data: STUB   … event: chunk / data:  at.
#    event: reply / data: {"summary":"STUB SUMMARY: …","ticketId":"T-1003"}

# 4. Async: 202 immediately; the summary reaches the sink a few seconds later.
curl -s -i -X POST "http://127.0.0.1:$PORT/channels/ticket-async" \
  -H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-summarize-key' \
  -d '{"ticketId":"T-1002","subject":"Invoice looks wrong","description":"Total differs from the quote."}'
# => HTTP/1.1 202 Accepted
docker compose -f examples/ai-summarize/deploy/compose.yaml logs model-stub | grep SINK
# => model-stub: SINK received {"summary":"STUB SUMMARY: ...","ticketId":"T-1002"}

# 5. Tear down by explicit project name.
docker compose -p sutra-ai-summarize down -v
```

For the Anthropic wire, package `variants/stub-anthropic` instead (remove the previous archive from
`deploy/deployments` first); the answers are the same. The engine image is
`ghcr.io/startr-trade/sutra:0.6.11`; set `SUTRA_ENGINE_IMAGE` to run a locally built tag. Validate a
variant without Docker:

```bash
sutra lint examples/ai-summarize/variants/stub-openai --overlay examples/ai-summarize/shared
```

## Run it against a real provider

Pick the variant, export its key, and run the same requests. `api-key` is an `env:` reference
resolved from the engine's environment, so the key never appears in the package; the compose file
passes `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` and `GEMINI_API_KEY` through (each defaults to a
placeholder, because an `env:` reference to an unset variable fails the channel at load).

```bash
export ANTHROPIC_API_KEY=...          # or OPENAI_API_KEY / GEMINI_API_KEY
sutra package examples/ai-summarize/variants/anthropic --overlay examples/ai-summarize/shared \
    --out examples/ai-summarize/deploy/deployments
docker compose -f examples/ai-summarize/deploy/compose.yaml up -d
```

`provider`, `model` and `base-url` are plain strings in the variant's `channels.yaml` (placeholder
substitution applies to the key, not to these): to change the model, edit the two `candidates`
blocks of that variant. A new provider is a new variant directory: copy one, change its candidates.
The async flow's `summary-sink` still points at the stub's `/sink`; change its `bind:` to your own
webhook. Never put a literal key in the package: a literal secret is rejected at package-validation
time.

`examples/ai-advanced/live-check.sh` runs all three real variants (for each key you export) plus
the advanced flows, and prints a pass/fail line per check.

## Notes

- The request body is the process's `requestBody` variable: `{ "prompt": "...", "ticketId": "..." }`.
  The transport reads `prompt` (also `messages`, `system`, `json`, `tools`, ...) and the rest rides
  along and is echoed back as `request` on the async reply.
- A slow model makes the sync caller wait: `<q:timeout duration="PT45S">` bounds the call, and
  `timeout-seconds: 30` bounds each candidate. For anything slower, use the async pair.
- Delivery is at-least-once: a crash after the model answered but before the delivery was recorded
  calls the model again. Prompts and answers are never written to logs or spans.
