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
| **Live audio sessions** (preview) | `AudioSession` and `require: [realtime]`, opened through an `llm` channel by a media transport, not by a process. | [Live audio sessions](building/llm-channels.md#live-audio-sessions-preview) |

Telephony, IVRs, meetings and WebRTC are not part of this engine. They live in the separate
open-source repository [sutra-voice](https://github.com/startr-trade/sutra-voice), which builds
on the pieces above.

## Before you try a walkthrough

Every walkthrough runs from the root of a checkout of this repository. It needs:

- Docker with Compose v2, plus `curl`, `python3`, `sha256sum` and GNU `base64` (on macOS, use
  `base64 -i <file>` wherever a walkthrough says `base64 -w0 <file>`);
- an engine image and the `sutra` CLI, both **0.6.0 or later**.

The compose files default to the published `ghcr.io/startr-trade/sutra:0.6.5` image, and
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
