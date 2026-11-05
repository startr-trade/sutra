# What's new in 0.6

Engine 0.6.0 lets a process work with sound, images and stored recordings, share a limited
number of slots across deployments, and loop back to a wait as many times as it needs. Every
feature below has a walkthrough you can run on your own machine with the bundled stub model
server, so no provider key is needed.

| Feature | What it gives you | Try it |
|---|---|---|
| **Audio in model calls** | `audio` parts on a user turn, formats written as media types, `audioOutput` for a spoken answer, and `audio` / `transcript` / `audioPreserved` on the reply. `require:` gains `audio-input`, `audio-output`, `audio-understanding` and `video-understanding`. | [Audio, images and video](building/llm-channels.md#audio-images-and-video) |
| **`voice: pipeline`** | Speech-to-text, then a text model, then text-to-speech, through an `llm.speech` provider, for models that take no audio. | [The voice pipeline](building/llm-channels.md#the-voice-pipeline) |
| **Video and image understanding** (preview) | Video clips, sampled frames and a one-frame `image` part. | [Try it](building/llm-channels.md#try-it-audio-the-pipeline-and-vision) |
| **Native `gemini` provider** | Gemini's own API, which carries audio, images and video. | [Providers](building/llm-channels.md#providers) |
| **Structured answers** | A call with `json: true` gets the parsed answer under `json`, so a flow reads its fields directly. | [The reply](building/llm-channels.md#the-reply) |
| **Media references and object stores** | Named object stores in the engine configuration. A media part gives `{ref, sha256}` instead of inline data, and the channel reads it from `llm.media-store`. | [Recordings and media references](building/recordings-and-media.md) |
| **`q:audit capture="media"`** | Audit events carry media references, never the media. | [Recordings: the audit trail](building/recordings-and-media.md#the-audit-trail-carries-references-never-media) |
| **Erasure and retention delete recordings** | Subject erasure and terminal retention delete the referenced objects and amend the audit events. | [Recordings: erasure and retention](building/recordings-and-media.md#erasure-and-retention-delete-the-recording) |
| **Capacity pools and `session-end:`** | Named, cluster-wide slot counts drawn from by inbound and outbound channels. A full pool answers 503 inbound and holds outbound requests in the outbox. A session can end before its instance does. | [Capacity pools and sessions](operating/capacity-pools.md) |
| **Re-enterable waits** | A loop back to a message catch, user task, timer or channel call parks a new wait each time round. | [Waiting at the same node again](building/wait-states.md#waiting-at-the-same-node-again-loops) |
| **Timer intervals from FEEL** (0.6.4) | A `timeDuration`, `timeDate` or `timeCycle`, a `q:timeout` and the `q:retry` delays can each be a FEEL expression over the instance's variables and `config.<key>`. Deployment `properties:` in `package.yaml` hold per-environment values. | [Timer intervals from FEEL](building/retries-history-schedules.md#timer-intervals-from-feel) |
| **Durable throughput** (0.6.5) | A concurrent outbox dispatcher, in-process hand-off of `local://` deliveries, fewer durable commits per step, separate pools for intake and delivery, and an opt-in data-store read cache. On one laptop with durable PostgreSQL a two-process relay went from 14 to 499 completions per second, p99 250 ms. | [Tuning for throughput](operating/throughput-tuning.md) |
| **Answers that beat their wait; less CPU** (0.6.6) | A `local://` answer that reaches a process before it has parked at the wait for it is held and retried instead of dropped. HTTP clients read the CA bundle once. About a fifth less CPU per completed relay. | [An answer that arrives before its wait](building/wait-states.md#an-answer-that-arrives-before-its-wait), [Tuning for throughput](operating/throughput-tuning.md) |
| **Binary snapshots; restart-proof local deliveries** (0.6.7) | Instance snapshots are stored in a binary container — **an older release cannot read it, so upgrade every replica at once**. A local delivery that fails because the database restarted is retried instead of poisoned. | [The binary container](internals/durable-execution.md#the-binary-container) |
| **Replicas no longer send twice** (0.6.8) | Engines sharing one database each claimed outbox rows another was still delivering, so 0.6.5–0.6.7 sent about half of all outbound messages twice under multi-replica load. The claim now leases its rows (30 s, renewed while in flight). | [Outbox processing](architecture/replicas.md#outbox-processing-skip-locked-and-a-claim-lease) |
| **Autovacuum for the busy tables** (0.6.8) | On PostgreSQL the outbox queue and the other high-churn tables have their own autovacuum settings, so dead rows no longer stall the outbox claim at high rates. Lower `autovacuum_naptime` on the server for high-rate deployments. | [PostgreSQL autovacuum](operating/throughput-tuning.md#postgresql-autovacuum) |
| **Correlation keys on audit events** (0.6.9) | Every audit event in the JSONL and OTLP sinks carries the instance's `<q:alias>` keys and values, redacted like any other audit content. `SUTRA_AUDIT_OTEL_ENDPOINT` no longer panics the engine at start, and the JSONL sink writes exactly one event per line. 0.6.9 is also the first release of the 0.6.8 changes above. | [Correlation keys on every audit event](operating/logging.md#correlation-keys-on-every-audit-event) |
| **Key queries on a data store** (0.6.10) | A data-task read returns the rows whose key starts with a prefix or lies in a range, in key order and paged, with count and exact-decimal sum aggregates over every matching row, optionally grouped. One index range scan, no locks. `expect="absent"` claims a key once across replicas, and a data-store conflict can be caught by an error boundary. | [Querying rows by key](building/data-stores.md#querying-rows-by-key) |
| **Relays honour a wait's message type** (0.6.10) | Several waits on one channel are told apart by `messageTypeValue` / `messageTypePattern`, like start events. Also in 0.6.10: FEEL builtins such as `context put` and `list contains` parse in the package lint, and the lint checks assignment expressions and multi-instance loop items. | [Several waits on one channel](building/q-namespace.md#several-waits-on-one-channel) |
| **A failed send stays with its failed step** (0.6.11) | A process that sent and then failed on its first pass left that send for the next request on the same engine lane, which then failed with an HTTP 500 or delivered the failed process's send. Every release up to 0.6.10 is affected. | [Each step's sends are its own](architecture/execution-lanes.md#each-steps-sends-are-its-own) |
| **Outbox renewal never waits; audit resource attributes** (0.6.12) | The claim-lease renewal no longer deadlocks with the batch delete of delivered rows (0.6.8–0.6.11) and no longer takes seconds when the engine falls behind. Data stores that share a table no longer race their first use. OTLP audit records carry `OTEL_RESOURCE_ATTRIBUTES` and the new `sutra.telemetry.resource-attributes`. | [Outbox processing](architecture/replicas.md#outbox-processing-skip-locked-and-a-claim-lease), [Resource attributes on audit records](operating/logging.md#resource-attributes-on-audit-records) |
| **Typed-store queries, a migration ledger, deployment manifests** (0.6.13) | `<q:store query=…>` and `<q:aggregate>` run on typed-column stores as SQL aggregates. Module-store migrations run once, recorded in `sutra_store_migrations`, refuse an edited script, and take `${package.*}`/`${store.*}` placeholders. `sutra lint`/`package`/`deploy` accept a `deploy/<name>.yaml` manifest that stacks layers, with per-layer `.ignore`. | [Querying a typed-column store](building/data-stores.md#querying-a-typed-column-store), [Migrations run once](building/data-stores.md#migrations-run-once), [Deployment manifests](building/deployment-packages.md#deployment-manifests-customers-editions-environments) |
| **JSON from OpenAI audio models; the reason for a failed model call** (0.6.14) | An OpenAI audio model (`gpt-audio…`) asked for JSON is asked in words, since it refuses `response_format`; a fenced JSON answer inside prose is parsed. A failed `llm` call logs the provider's HTTP status, error code and message at `WARN` under `SUTRA.LLM.CALL.FAILED`, keys redacted, and the failed instance's stored diagnostic carries it. | [How JSON is asked for](building/llm-channels.md#how-json-is-asked-for), [Finding the reason for a failed call](building/llm-channels.md#finding-the-reason-for-a-failed-call) |
| **A failed synchronous channel call can be caught** (0.6.15) | A permanent failure of a synchronous channel call is a BPMN error on its task, under the failure's own code: a boundary or an error event sub-process catches it, and the catching path reads `callFailure` (`code`, `message`, `node`). Uncaught, it fails as before. | [Catching a failed call](building/llm-channels.md#catching-a-failed-call) |
| **A delivery that can't be checked is refused; `event.body` is masked** (0.6.17) | A delivery with an explicit event id or a declared `dedupKey` is refused and retried while its duplicate check can't answer, instead of running unchecked. See [Acknowledgement modes](operating/ack-modes.md#when-a-delivery-cant-be-checked-for-a-duplicate). A flow that declares masking now masks `event.body` and every inbound header too: it used to show them in audit, inspect and snapshots. |
| **One endpoint, one deployment; acceptance receipts; refused deliveries** (0.6.18) | **A second deployment claiming a taken endpoint is refused at deploy,** with the first still serving; a new version may take a route from an older one, which then drains whole. **An `on-persist` HTTP channel answers with an acceptance receipt** naming the instance, and a duplicate names its original. **A declared `<q:source dedupKey>` always dedupes,** and an unrecognised form is refused. **Reading `GET /sutra/instances` takes the admin key** when admin auth is on. **A broker message with no content type is decoded by the channel's codec,** and a permanent refusal now leaves a WARN and an audit record. **A synchronous flow with no `<q:reply>` answers an empty body.** | [One endpoint, one deployment](building/endpoint-claims.md), [Acceptance receipts](building/acceptance-receipts.md), [Refused deliveries](operating/inbound-refusals.md), [Upgrading to 0.6.18](operating/upgrading-0-6-18.md) |
| **Keep a message as text; references in connection properties** (0.6.19) | A flow can keep a message as raw text in a typed store column and read it back decoded with the new `<q:decode>` data-task step. `event.bodyLossy` marks a body whose bytes were not valid UTF-8. The connection properties of `kafka`, `rabbitmq`, `amqp`, `gcp-pubsub` and `file` channels now expand references, where 0.6.18 refused them. | [Keeping a message as text](building/data-stores.md#keeping-a-message-as-text), [References in connection properties](operating/connection-references.md), [Upgrading to 0.6.19](operating/upgrading-0-6-19.md) |
| **A draining deployment's instances stay its owner's** (0.6.17) | A message only falls back to draining deployments with the same tenant and module labels as its channel's deployment. It could previously resume another owner's draining instance. A package without both labels no longer falls back at all. |
| **Live audio sessions** (preview) | `AudioSession` and `require: [realtime]`, opened through an `llm` channel by a media transport, not by a process. | [Live audio sessions](building/llm-channels.md#live-audio-sessions-preview) |

Telephony, IVRs, meetings and WebRTC are not part of this engine. They live in the separate
open-source repository [sutra-voice](https://github.com/startr-trade/sutra-voice), which builds
on the pieces above.

## Before you try a walkthrough

Every walkthrough runs from the root of a checkout of this repository. It needs:

- Docker with Compose v2, plus `curl`, `python3`, `sha256sum` and GNU `base64` (on macOS, use
  `base64 -i <file>` wherever a walkthrough says `base64 -w0 <file>`);
- an engine image and the `sutra` CLI, both **0.6.0 or later**.

The compose files default to the published `ghcr.io/startr-trade/sutra:0.6.19` image, and
[Installation](getting-started/installation.md) installs the CLI. To run a walkthrough against
your own checkout instead, build both from it:

```bash
make image                                        # builds sutra-rust-engine:dev
(cd rust && cargo build --profile release-it -p sutra-cli)
export PATH="$PWD/rust/target/release-it:$PATH"   # the sutra CLI you just built
export SUTRA_ENGINE_IMAGE=sutra-rust-engine:dev   # every example's compose file reads this
```

Each example's compose file names its own Compose project (`sutra-ai-advanced`,
`sutra-media-recordings`, `sutra-capacity-pools`, `sutra-review-loop`) and publishes the engine
on a dynamic loopback port, so the stacks never collide with each other or with anything you
already run. Each walkthrough ends by removing its own stack with `docker compose -p <project>
down -v`.

The output shown in each walkthrough was copied from a real run against the stub. Instance ids,
timestamps and audio bytes differ on every run.
