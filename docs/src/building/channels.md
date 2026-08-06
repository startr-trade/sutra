# Channels and transports

Processes don't get called — they're **triggered by messages arriving on channels**.
`channels.yaml` (one per package, see [Deployment packages](deployment-packages.md)) declares each
channel: which transport it rides, which codec decodes it, how it acknowledges receipt, and who
may send to it.

## Transports

One neutral transport SPI, seven implementations, all self-registering behind the same lifecycle
trait — the engine binds, activates, and drains every one of them through a single generic path
with no `if transport == "..."` branching anywhere:

| `transport:` | Notes |
|---|---|
| `http` | The universal baseline — always bundled. Also serves `/sutra/health/*`. |
| `kafka` | `rdkafka`. |
| `rabbitmq` | `lapin` (AMQP 0.9.1). |
| `aws-sqs` | AWS SDK. |
| `gcp-pubsub` | Google Cloud client. |
| `amqp` | `fe2o3-amqp` (AMQP 1.0). |
| `file` | Air-gapped: file-spool inbound + `file://` outbound sink, no network dependency. |

Two further `transport:` values are engine-internal rather than vendor clients — they have no wire
protocol and no listener of their own. `local` delivers in-process to another channel; `pull` parks
the delivery as a task a worker fetches instead of dialing anything, which is the
[external-task surface](external-tasks.md).

Dapr and Knative Eventing ride the HTTP transport as integration patterns rather than dedicated
crates — the engine speaks plain HTTP (+ CloudEvents) to a Dapr sidecar or a Knative broker, so no
broker vendor client ever links into the engine for either.

A hardened or air-gapped build selects a subset of transports at compile time via Cargo features
(`cargo build -p sutra-engine --no-default-features --features file`), so the unlinked vendor
clients (`rdkafka`, the AWS/GCP SDKs, `lapin`, `fe2o3-amqp`) are not compiled in at all. An
operator can additionally restrict which transports a *running* binary accepts via
`SUTRA_ALLOWED_TRANSPORTS` — a channel declaring a disallowed transport fails the deployment with
a clear diagnostic, not a silent no-op.

## Binding a channel

```yaml
# examples/money-transfer/.../channels.yaml (abridged)
channels:
  - name: transfer-request
    transport: http
    bind: "POST /channels/transfer-request"
    codec: urn:transfer
    cloudevents-mode: none
    ack-mode: on-complete
    auth:
      scheme: apikey
      apikey:
        value: transfer-demo-key
        header: X-Api-Key
```

A channel does not name a process. **Processes subscribe to channels**: a start event's
`<q:source channel="transfer-request" messageTypeValue="TransferRequest"/>` is what routes a
decoded inbound to it (see [The q: namespace](q-namespace.md)). This is what lets one channel feed
several processes and one process listen on several channels — the money-transfer example runs
the *same* transfer flow off three different intake channels (`http`, `rabbitmq`, `kafka`), each a
separate `<q:source>` on the same underlying `<bpmn:transaction>`.

**Fan-out.** By default a channel is point-to-point: for a given `(channel, messageType)`, exactly
one subscribing process is allowed (enforced at deploy time). Setting `broadcast: true` fans a
decoded message out to *every* subscribing process, one instance each — genuine pub/sub.

**Concurrency admission.** A channel may optionally declare `maxConcurrentInstances` — an
admission cap on simultaneously active instances from that channel (a suspended instance still
holds its slot). Absent, the channel is unbounded.

## Codecs — format × schema

A **codec** is a channel-facing named decoder: a parser (`json` / `xml` / `yaml` / `csv` / a
domain wire format) plus an *optional* schema. The schema is the load-bearing part — it's what
yields a `messageType` and structural validation. A codec with no schema (a media codec, or a
channel with no codec at all) still decodes, but only a catch-all `<q:source>` (no
`messageTypeValue`/`messageTypePattern`) can subscribe to it, and `sutra lint` warns.

Codec names share one `urn:sutra:codec:<name>` namespace, and `codec:` in `channels.yaml` looks
identical whichever tier the decoder came from. There are three:

| Tier | Example binding | Where the decoder comes from | What it takes to have one |
|---|---|---|---|
| Built-in format | `codec: urn:sutra:codec:json` | Linked into every engine binary | Nothing — it is always there |
| Package-supplied codec | `codec: urn:transfer` | `schemas/transfer/*.xsd` inside the archive, compiled when the package deploys | Author the XSDs — no Rust, no engine build |
| Extension-crate codec | `codec: urn:sutra:codec:<name>` | A crate implementing `PayloadCodec`, force-linked by a composition root | One Cargo dependency and one line in that composition root |

**A built-in format** is a pure parser — it decodes, but carries no schema, so it lands in the
catch-all-subscription case above. This distribution bundles six of them — `json`, `xml`, `yaml`,
`csv`, `raw-text`, `raw-bytes` — and no domain codec at all. Bind one when the payload genuinely
has no contract to check, or when that contract is enforced somewhere else.

**A package-supplied codec** is the zero-install path to a *typed* contract, and the one both
example apps take. `schemas/transfer/` holds the XSDs plus a `codec-manifest.yaml` declaring
`schemaKind: xsd` and the formats the codec accepts; the engine compiles them at deploy time and
names the codec after the folder path (`urn:transfer` — see
[Deployment packages](deployment-packages.md) for the exact folding rule). Nothing about it is
second-class: it yields message types, structural validation, and the shape every FEEL path is
checked against at load time, exactly as a codec written in Rust does. The engine cannot tell that
the schema arrived in an archive rather than a crate.

**An extension-crate codec** is what a wire format needs when a folder of schemas cannot express
it — a grammar that isn't XML or JSON at all (a fixed-width block structure, or a delimited segment
stream), or a whole *profile*: an envelope grammar, a mapping from wire-level message names to
schemas, and versioned editions revved on the standard's own release cadence. It implements
`PayloadCodec` from `sutra-codec-spi`, `inventory::submit!`s a `BuiltinCodec` next to the impl, and
claims its name in the same namespace; a distribution that wants it adds the dependency and
force-links it from its own composition root. Every message standard is served this way, by
**proprietary extension crates built outside this repository**. An engine binary that links one
resolves it exactly like a built-in; see
[Domain neutrality and the SPI model](../architecture/neutrality-and-spi.md) for why none of them
lives here.

### What an extension codec can express — an enveloped profile, generically

The clearest illustration of what the codec SPI has to be able to carry is a market venue's own
*profile* of a standard rather than the bare standard itself. A generic-format codec is the easy
case: one instance document validated against a schema, with the message type read straight off
the document's own root element or namespace. A profile codec decodes a family of messages for a
venue that doesn't fit that shape — every message travels inside a venue-specific envelope,
wrapping a header plus a body, and the venue pins its own schema versions on its own release
cadence. Worse for identification purposes, the same underlying message can sometimes back several
different wrappers, so a bare schema namespace can't tell you which one you're looking at.

Such a codec's message type is therefore **the wrapper element's local name**, not a schema
namespace slug — which lets `declared_message_types()` return the venue's own **closed** set of
wrapper names, rather than the open type set a bare-standard codec declares, so a message-type
applicability check (a `rules-manifest.yaml` entry, a `q:source messageTypeValue` pin) actually
fires for a profile-bound module rather than silently no-op.

Decoding validates the envelope grammar, the header, and the body against the venue-pinned base
schemas the codec crate carries, out of the box. The payload view projects the body exactly as a
bare-standard document would be — a compatibility guarantee: every alias, DMN input, or fixture
written against the underlying message shape reads identically whether the underlying codec is the
profile variant or the plain standard. Outbound can be a template-rendered passthrough — `encode()`
returning the rendered envelope bytes verbatim rather than assembling one — so template drift
against the venue's schemas has to be caught by validating rendered output in tests, not by a
runtime encode path. A second venue following the identical envelope/wrapper/edition pattern
reuses the same machinery rather than re-implementing it.

None of that requires a change to the engine, the channel layer, or this repository — which is the
point of the codec SPI. A deployment can go one step further still and override a codec's own
schemas: see
[Deployment packages](deployment-packages.md#schema-bundles-when-a-codec-is-a-whole-profile) for
the `schemaKind` bundle mechanism that lets an archive map its own schema editions per wrapper.

## Acknowledgement modes

`ack-mode` decides **when** the engine acknowledges an inbound relative to processing:

- **`on-persist`** — ack as soon as the message is durably captured, before the process runs.
  Broker default. On HTTP this is what makes a channel asynchronous (`202 Accepted`, no business
  body).
- **`on-complete`** — ack only once the instance reaches a terminal state. HTTP default (classic
  synchronous request/reply — hold the connection, return the reply body). On a broker, this
  defers the ack via the engine's `DeferredAckRegistry` until the instance completes or fails.

The full per-transport wiring matrix, the bounded-registry knobs, and when to pick which mode live
in [Acknowledgement modes](../operating/ack-modes.md) — that's the operating-chapter deep dive this
summary points at.

## Secrets on a channel

A channel's credentials (broker username/password, an API key) are never literal values in
`channels.yaml` — they're scheme references (`env:NAME`, `secret:…`, `vault:…`,
`aws-secrets:…`), resolved at channel startup through one vendor-neutral resolver SPI. Package-time
validation rejects a literal secret outright.

## Next

- **[The q: namespace](q-namespace.md)** — how a BPMN process subscribes to a channel.
- **[External tasks: the pull worker surface](external-tasks.md)** — the `pull` transport in full.
- **[Acknowledgement modes](../operating/ack-modes.md)** — the operator-facing deep dive.
