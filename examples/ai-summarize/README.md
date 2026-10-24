# ai-summarize — a process that calls a language model, synchronously and asynchronously

A worked deployment package for the v0.5.0 `llm` transport. A support ticket is POSTed as JSON; a
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

| Path | What it is |
|---|---|
| `deployments-src/default--ai-summarize--1.0.0/` | the package: `package.yaml`, `channels.yaml` (commented), `bpmn/summarize-sync.bpmn`, `bpmn/summarize-async.bpmn` (both with diagram interchange) |
| `stub/model-stub.py` | a ~50-line Python 3 stdlib server: a fixed `/v1/chat/completions` answer plus a `/sink` that logs |
| `deploy/compose.yaml` | engine + PostgreSQL + the stub, project name `sutra-ai-summarize` |

## Run it against the stub

```bash
# 1. Seal the package into the directory the engine watches.
sutra package examples/ai-summarize/deployments-src/default--ai-summarize--1.0.0 \
    --out examples/ai-summarize/deploy/deployments

# 2. Start the engine, its database and the stub.
docker compose -f examples/ai-summarize/deploy/compose.yaml up -d
PORT=$(docker compose -f examples/ai-summarize/deploy/compose.yaml port engine 8080 | cut -d: -f2)

# 3. Sync: the summary comes back on the same request.
curl -s -X POST "http://127.0.0.1:$PORT/channels/ticket" \
  -H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-summarize-key' \
  -d '{"ticketId":"T-1001","subject":"Cannot log in","description":"Password reset email never arrives."}'
# => {"ticketId":"T-1001","summary":"STUB SUMMARY: the customer reports a problem and asks for it to be looked at."}

# 3b. Streamed: the same summary as server-sent events, one chunk per piece, then the reply.
curl -s -N -X POST "http://127.0.0.1:$PORT/channels/ticket-stream" \
  -H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-summarize-key' \
  -d '{"ticketId":"T-1003","subject":"Slow app","description":"Pages take ten seconds."}'
# => event: chunk / data: STUB   … event: chunk / data:  at.
#    event: reply / data: {"summary":"STUB SUMMARY: …","ticketId":"T-1003"}

# 4. Async: 202 immediately; the summary reaches the sink a moment later.
curl -s -i -X POST "http://127.0.0.1:$PORT/channels/ticket-async" \
  -H 'Content-Type: application/json' -H 'X-Api-Key: dev-only-summarize-key' \
  -d '{"ticketId":"T-1002","subject":"Invoice looks wrong","description":"Total differs from the quote."}'
# => HTTP/1.1 202 Accepted
docker compose -f examples/ai-summarize/deploy/compose.yaml logs model-stub | grep SINK
# => model-stub: SINK received {"ticketId":"T-1002","summary":"STUB SUMMARY: ..."}

# 5. Tear down by explicit project name.
docker compose -p sutra-ai-summarize down -v
```

The engine image is `ghcr.io/startr-trade/sutra:0.5.0`; set `SUTRA_ENGINE_IMAGE` to run a locally
built tag. Validate the package without Docker: `sutra lint examples/ai-summarize/deployments-src/default--ai-summarize--1.0.0`.

## Run it against a real provider

`api-key` is resolved from the engine's environment (`env:LLM_API_KEY`), so the key never appears in
the package. `provider`, `model` and `base-url` are **plain strings in `channels.yaml`** (placeholder
substitution applies to the key, not to these), so switching providers is an edit to the two
`candidates` blocks (`summarize-sync` and `summarize-async`), then re-run `sutra package`:

1. `model: test-model` becomes a real `<model-id>` from your provider.
2. Delete the `base-url:` line to use the provider's default endpoint (for `provider: openai`,
   `https://api.openai.com/v1`), or point it at any OpenAI-compatible server. Other `provider:` ids
   (`anthropic`, `mistral`, ...) are listed in `docs/guide/llm-channels.md`.
3. `export LLM_API_KEY=<your key>` before `docker compose up` (the compose file passes it through),
   and optionally remove the `model-stub` service. The async flow's `summary-sink` still points at
   the stub's `/sink`; change its `bind:` to your own webhook.

Never put a literal key in the package: a literal secret is rejected at package-validation time.

## Notes

- The request body is the process's `requestBody` variable: `{ "prompt": "...", "ticketId": "..." }`.
  The transport reads `prompt` (also `messages`, `system`, `json`, `tools`, ...) and the rest rides
  along and is echoed back as `request` on the async reply.
- A slow model makes the sync caller wait: `<q:timeout duration="PT45S">` bounds the call, and
  `timeout-seconds: 30` bounds each candidate. For anything slower, use the async pair.
- The sync task still carries a `<q:alias>` because the loader requires one on every channel call; the
  engine never evaluates it for an inline call.
- Delivery is at-least-once: a crash after the model answered but before the delivery was recorded
  calls the model again. Prompts and answers are never written to logs or spans.
