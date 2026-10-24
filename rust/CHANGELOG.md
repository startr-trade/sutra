# Changelog

All notable changes to the Sutra Rust workspace are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This is the changelog for the **Rust-native** engine (`rust/` workspace). It begins at
`0.2.0-rc.1`, the first release candidate of the ground-up Rust rewrite. The earlier
`v0.1.0` line belonged to the retired reference baseline and is unrelated version history.

## [Unreleased]

Nothing yet.

## [0.5.0] — 2026-10-24

A **minor** release: a process can call a language model, synchronously or asynchronously, and
stream the answer to its caller. Model calls are ordinary channel calls, so they get `<q:timeout>`,
`<q:retry>`, boundary events and correlation with no new BPMN element. One additive `q:` attribute
(`q:stream`).

### Added

- **Sync and async outbound calls, chosen by the outbound channel's declared `ack-mode`.**
  `on-complete` = **sync**: the call is made inline, its response is merged into the variables and the
  flow continues — no park, no outbox row, no database write for the hop. Parallel branches' sync
  calls are in flight together; responses are applied one at a time. Works on a start, on a relayed
  turn, on a timer-driven pass and on a scheduled start. `on-persist` (the outbound default) =
  **async**: outbox, durable park, the callback arrives on a declared inbound channel and resumes by
  correlation. Only an explicitly declared `ack-mode` counts on an outbound channel, so no existing
  channel changes behaviour.
- **`InlineCaller`** SPI (`sutra-channels`, registered through `sutra-transport-spi`'s
  `InlineCallerFactory`), implemented by the `llm` transport and by the `http` transport (sync REST).
  A sync channel on a transport without one fails at load (`SUTRA.CHANNEL.SYNC_CALLER_MISSING`).
- **The `llm` transport** (`sutra-transport-llm`): ordered model candidates with fallback on
  rate-limit / outage / timeout, `require` capability filtering, `system`, `max-output-tokens`,
  `temperature`, `timeout-seconds`, `concurrency`. Sync channels need no reply channel; async ones
  deliver to `reply-channel` with the request echoed so `<q:alias>` can correlate on it.
- **Provider-native async jobs** (`llm.mode: job`): submit and return at once; completion by
  polling or by **signed webhooks** (served under `/llm/…`); a durable, row-claimed `llm_job` table.
- **Streaming to the caller**: an inbound HTTP channel declaring `reply-stream: sse` streams a
  `q:stream="true"` model call as `event: chunk` frames, then `event: reply` (or `event: error`, code
  only); `event: reset` if a fallback candidate takes over mid-stream. Chunks are never logged.
- **`q:stream`** attribute on `<bpmn:serviceTask>` (additive to the frozen `q:` namespace).
- **A provider SPI** (`sutra-llm-spi`) — neutral request/response/failure types, optional
  streaming, job and callback methods, a shared JSON/SSE HTTP client — held to the domain-neutrality
  gate. **Providers:** `openai`, `anthropic`, `aws-bedrock` (Converse API, Bedrock API-key auth),
  and through the Chat Completions adapter `google-gemini`, `mistral`, `deepseek`, `xai`, `kimi`,
  `meta-llama`. Jobs: `openai` (background responses + webhooks), `anthropic` (Message Batches,
  polled). Streaming: all except `aws-bedrock`. `sutra-dist` features `llm-openai`,
  `llm-anthropic`, `llm-bedrock`, all in the default build.
- **A relayed turn on an `on-complete` HTTP channel answers with that turn's reply**, so a
  multi-turn conversation over HTTP gets each answer on its own request.
- **`ReplySinkFactory`** hook in `sutra-transport-spi` for a sink that answers by re-entering the
  engine; bare `<scheme>://<channel>` binds are qualified for it as for `local://`.
- **Crash recovery for in-memory chains**: an `inflight_sync_chain` marker and a sweep release a
  crashed chain's inbox-dedup rows and admission slot (`sutra.instance.inflight-chain-max-age`,
  default PT15M). A failed chain releases its dedup row at once, so a client retry re-runs.
- `sutra-docgen` documents `llm:` channels (secret references shown only by scheme).
- Example package `examples/ai-summarize` (sync and async summaries against a stub model).

### Changed

- **A destination-less `<q:reply>` reached before a park now answers the request**, for a start
  and for a relayed turn; previously only `continue="true"` did, and a start that replied then
  parked returned `200` with an empty body.
- `<q:alias>` is no longer required on a channel-call task by the BPMN loader; the deploy-time lint
  requires it only when the target channel is async (same `ALIAS_REQUIRED` code).
- An instance inside an in-memory sync chain holds its `max-concurrent-instances` slot from the
  first call until the chain ends.
- **A structured request or send body is sent as JSON.** A channel-call `requestBody`, and a
  `<q:send>` / destination-bearing `<q:reply>` body, that is a FEEL context or list is now serialised as
  JSON instead of FEEL's canonical text (`{a=1}`), which no counterpart could parse. String bodies
  are unchanged.

### Known limitations

- Async held calls (`mode: call` on an `on-persist` channel) still hold the outbox's serial
  delivery for the call's duration; use `mode: job` or a sync channel to avoid it.
- A crash after a model answered but before the result was recorded can call the model again.
- No token budget or rate limiter beyond `concurrency`; no IAM request signing for Bedrock;
  broker-routed job callbacks and Bedrock/Gemini jobs are not implemented; broadcast fan-out keeps
  sync calls on the async path. The `llm_job` and in-flight-marker stores are PostgreSQL-only.
- A failed sync call without `<q:retry>` fails the instance rather than routing to a boundary.

### Fixed

- **Anthropic: a rejected `temperature` is retried once without it.** Newer Claude models reject
  sampling parameters with HTTP 400 ("`temperature` is deprecated for this model"); a channel's
  `temperature` is a preference, so the adapter drops it rather than failing the call.

### Verified against real providers

- `examples/ai-summarize` (sync, streamed, async) passed end to end against OpenAI
  (`gpt-4o-mini`), Anthropic (`claude-haiku-5-5`) and Google Gemini (`gemini-2.5-flash`) on
  2026-10-24; against OpenAI also cross-candidate fallback, JSON mode, tool calls, and an OpenAI
  background job completed both by polling and by a signed webhook; and an Anthropic Message
  Batches job (`mode: job`, polled) end to end.

## [0.4.0] — 2026-10-23

A **minor** release: BPMN standard convergence. Where BPMN 2.0 already spells something the
engine does, the engine now accepts the standard spelling and maps it onto existing behaviour.
Four of the five flow nodes it used to reject now load, and the rejected list is down to one —
`<event>`, which is `abstract="true"` in the BPMN 2.0 XSD and so cannot be implemented by anyone.

Purely additive: no element that loaded before loads differently, and nothing in the `q:` surface
changed (it is frozen; a change there would be semver-major). What is new is that conformant models
which previously failed to load now work, and that a model authored in a BPMN tool stops losing its
data declarations silently.

### Added

- `<task>` and `<implicitThrowEvent>` load (both concrete in the BPMN 2.0 XSD, so rejecting them
  was a conformance gap). `Node::ManualTask` gained `untyped` to carry the authored spelling to
  `sutra lint`, which warns on the untyped form — the loader has no warning channel of its own.
- `Node::ReceiveTask` — a wait-capable ACTIVITY, so timer boundaries attach.
- `Node::EventBasedGateway` — arms its targets rather than parking itself; first-wins retirement
  widens `resolved_waits_for`, so the losing arms resolve in the same transaction as the winner.
  The wait group is static model structure (the gateway's outgoing targets), so nothing has to
  survive the park: no snapshot field, no counter.
- Non-interrupting timer boundaries, and unbounded `<timeCycle>` on them, reusing
  `advance_cycle()` so the boundary and the schedule poller share one catch-up policy.
- BPMN data declarations (`<property>`, `<dataObject>`/`<dataObjectReference>`,
  `itemDefinition`) merge into `ProcessDefinition::declared_variables`;
  `shadowed_bpmn_variables` records a collision for lint.

### Fixed

- `bpmn-support.md`'s REJECTED table claimed to be "verbatim from the loader's reject arm" and
  nothing checked it — the existing tests pin the `Node` enum against the page, not the LOADER
  against it, so when two elements began loading the page went on calling them rejected while
  every gate stayed green. A test now loads a minimal process per listed element and requires the
  rejection, with the converse for the accepted spellings.

## [0.3.0] — 2026-10-23

**Minor, not patch**, because of the second item: it changes what an existing package returns to
its caller.

### Added

- **A start event can select on codec-reported message attributes** — `<q:source
  filterExpression>`, a FEEL predicate over attributes a codec reports per message, composing
  with the message-type tiers by conjunction. Codecs declare their vocabulary through
  `PayloadCodec::declared_attributes` (defaulted, so no codec is forced to change) and report
  per-message facts through `DecodeResult.attributes`. `sutra lint` refuses a predicate reading a
  key the bound codec can never emit, and refuses two subscribers it cannot prove disjoint.
- **A message a flow produces is validated before it leaves** — the codec's decode step is its
  validator, so the engine reads back what the flow produced. `<q:send codec>` / `<q:reply codec>`
  state the governing contract. What may be INFERRED is asymmetric: a send infers the codec of the
  channel it names, a reply infers nothing (a channel's codec is the contract its requests
  satisfy, and a reply is a different message).
- `sutra lint` classifies a template's own unrendered text against the governing codec and
  compares it to `template-manifest.yaml` (`outputMessageType`, `outputAttributes`).

### Changed

- **A render is no longer a synchronous reply unless `<q:reply>` is declared.** `responseBody` is
  the only slot the executor has for a render, so a terminal task rendering bytes for a
  `<q:send>` filled it without asking to answer anyone — and since an on-persist ack began
  carrying a flow's receipt, that made an asynchronous channel deliver the same message twice.
  A package whose caller depended on the implicit reply will see its response body change.
- Outbound validation is fail-closed by default on contract-bearing egress
  (`SUTRA.OUTBOUND.VALIDATION_REJECT`); schema-less egress is unaffected.

### Security

- `zip` 8 → 9 (a high-severity archive-extraction advisory the engine is exposed to, since it
  extracts `.sutra` archives), plus quinn-proto, rustls 0.23.46, rustls-webpki 0.103.16,
  webpki-roots/root-certs and ctutils 0.4.5 (RUSTSEC-2026-0385).

## [0.2.0-rc.4] - 2026-10-12

### Added — macOS CLI binaries, both architectures

`aarch64-apple-darwin` and `x86_64-apple-darwin` join the published targets, built natively on a
runner per architecture (the Apple SDK is not redistributable, so cross-building from Linux would
need osxcross and a self-supplied SDK). No source change was required: the CLI's only native-code
dependency is `ring`, and its platform-specific paths are `#[cfg(unix)]`, which macOS satisfies.

`scripts/install.sh` resolves `Darwin/arm64` and `Darwin/x86_64` to those targets, where it
previously refused macOS outright. The engine image is unchanged and remains Linux-only.

## [0.2.0-rc.3] - 2026-10-12

### Added — a package can be composed from layers (`--overlay`)

`sutra package <variant> --overlay <shared>` and `sutra lint <variant> --overlay <shared>` compose
the layers first and validate and seal the composed tree. Each `--overlay` is a BASE layer applied
in the order given; the package directory is applied last and wins, and a later overlay wins over
an earlier one. The archive is sealed under the package directory's own name and is
**byte-identical** to one sealed from a hand-merged directory of the same content, so the
`deploymentId` does not depend on how the tree was assembled.

Fail-closed: an overlay may not carry `package.yaml`, no layer may contain a symlink, and a
file-versus-directory clash between layers is an error. Composition only adds and replaces — a
layer cannot delete a file a lower layer provides. Every shadowed path is reported as
`SUTRA.DEPLOY.PACKAGE.OVERLAY_OVERRIDE`, naming the layer that won.

New in `sutra-loader`: `compose_overlays`, `assemble_dir_with_overlays`, `lint_dir_with_overlays`
and `ComposedPackage`. `sutra-testkit`'s `compose_variant` delegates to the same code, so the
conformance suites and the CLI compose a variant exactly one way.

### Added — a BKM can be invoked directly

`Drg::evaluate_bkm` evaluates a business knowledge model by name. A BKM that no decision
references was previously unreachable from outside the decision graph — exactly the case a
conformance harness has to exercise.

### Added — the security scanners are local gates

`make scan-deps` (Trivy over the source tree), `make scan-image` (Trivy over the engine image) and
`make codeql` (CodeQL for Rust) fail on any finding, unlike the scheduled workflows that only
report. Tiered by cost like the test tiers; CodeQL's scope is `scripts/codeql-config.yml`, which
excludes test code because it never ships.

### Changed — a `date` is accepted where a `date and time` is declared

FEEL's shape coercion refused the value; DMN 1.5 requires the implicit conversion (midnight, UTC).
A decision typed `date and time` now accepts a `date` input instead of failing the invocation.

### Changed — a secret reference's fields are named for what they are

In the GCP and Azure `envref` crates, `secret` and `secret_url` read as though they held secret
material; they hold a name and a URL, and they are `name` and `access_url` now. Positional
construction is unaffected; a named-field construction needs the new names.

### Changed — a scaffolded app's compose stack is scoped to that app

Every app from `sutra create app` resolved to the same compose project name, so
`docker compose down -v` in one app's deploy directory could tear down another app's stack and its
volumes.

### Fixed — DMN model imports and `functionItem` output types

A named import carries the imported model's input data under its alias, an import with an empty
name is merged unqualified, and a diamond import resolves once. A BKM whose `functionItem`
declares an `outputTypeRef` conforms its result to that type. DMN-TCK standing against the current
corpus: 3365 of 3385 assertions at compliance level 3 (99.4%), the 20 skipped ones being 14 cases
that need an external Java function and 6 with a nine-digit year.

### Fixed — the tier-3 Elasticsearch used percentage disk watermarks

A single node on a developer filesystem can sit above Elasticsearch's 85% low watermark with over
100 GB free; Elasticsearch then refuses to allocate shards, the cluster goes red, fluent-bit's
writes fail, and the k8s observability suite fails as "timed out waiting for all three OTLP
signals". The IT cluster sets absolute watermarks now (20 GB / 10 GB / 5 GB).

### Fixed — the fixture reaper reaps only fixtures

`make docker-clean` had host-wide prunes that would remove a developer's stopped compose stack and
its volumes. Removal now requires both a fixture-image match and the absence of a
`com.docker.compose.project` label; the host-wide prunes are behind an explicit `--deep`.

### Security

- **rustls 0.23.45** for RUSTSEC-2026-0285, with `rustls-webpki` 0.103.15.
- **Base-image CVEs re-triaged**, with reachability checked against the shipped binary's own
  tables (`readelf -d`, `nm -D --undefined-only`) rather than only against the distribution's
  "no fix" status. Every suppression is time-boxed.
- **Developer-tooling lockfiles** updated — `brace-expansion` 2.1.7, `js-yaml` 4.3.2,
  `source-map-js` 1.2.2, `vitest` 4.1.11 — clearing eight advisories.

## [0.2.0-rc.2] - 2026-09-06

Sectioned after the fact: the rc.2 cut wrote release notes but left this file's entries under
*Unreleased*. Everything below shipped in rc.2.

### Fixed — three defects the `call-log-load` example only exposed when it was actually run

All three sat behind the same blind spot: the example's end-to-end test booted the engine with no
datasource, so every valid upload stopped at the persistence check and nothing downstream of intake
ran. The codec half was well covered; the load half was structurally unreachable. A new tier-2
suite (`call_log_load_it`) now boots the same archive against a real PostgreSQL and asserts the
receipt, the transformed rows, and that a re-uploaded batch converges.

- **A `<bpmn:dataObject>` declared on a process was invisible inside its sub-processes**, so a data
  association referencing one from a nested scope silently resolved to the raw element id instead of
  the variable name. BPMN scopes a data object to its container *and everything nested in it*; the
  loader indexed each container in isolation. A store write inside a multi-instance loop therefore
  wrote `null` for every item — and reported it as `VALUE_NOT_A_RECORD` against a correctly
  resolved key, pointing at the transform rather than at the association that dropped the value.

- **`ack-mode: on-persist` discarded a respond-and-continue reply.** The ack mode settles *when* the
  caller is answered; it was also silently settling *what* with, throwing away a `<q:reply
  continue="true">` render that a process had produced deliberately before parking. An HTTP channel
  under `on-persist` now answers `202 Accepted` carrying that receipt, and a bare `202` only when
  the process replied nothing. This makes the two declarations composable, which is what a
  long-running intake needs: answer immediately, with a document, and load detached.

- **Every intake rejection that was the caller's fault answered `500`.** The status map keyed on the
  literal `REJECTED.` segment, which `SUTRA.INBOUND.VALIDATION_REJECT` does not carry — so a batch
  refused for malformed cells told the sender the engine had broken and the identical bytes were
  worth retrying. Caller faults are now `400`, with `409` for an alias conflict, `413` for an
  oversized payload, `429` for quota, and `503` for channel capacity. Anything unmapped keeps `500`:
  a wrong `4xx` is worse than an honest `5xx`.

### Changed — the generators moved under one `generate` verb (BREAKING)

`sutra docgen`, `sutra catalog` and `sutra schemagen` are replaced by

```
sutra generate docs           --input <folder>  [--output <dir>] [--check]
sutra generate catalog        --repo-root <dir> [--output <dir>] [--check]
sutra generate schema-handler <schemas> <out>   [--full] [--check]
```

**No aliases are kept** — the old spellings are gone, so any Makefile, pre-commit hook or
pipeline that invokes them must move. Every invoker in this repository already has.

The three are siblings in the way that matters to a caller: each recomputes output that is
derived rather than authored, and each offers the same `--check` drift gate. Grouping them also
draws the line against `sutra create`, which scaffolds files that become *yours* — a scaffold is
headed "edit freely — this file is yours" and needs `--force` to overwrite your edits, while a
generated page is headed "Do not edit above the MANUAL NOTES sentinel" and `--check` fails the
build if you edited it. Opposite guarantees, so they stay under different verbs.

`schema-handler` also drops its `generate` / `check` sub-verbs for the shared `--check` flag —
under the new parent they would have read `sutra generate schema-handler generate …`. The
`SUTRA.DOCGEN.*` / `SUTRA.CATALOG.*` / `SUTRA.SCHEMAGEN.*` diagnostic codes are unchanged: they
are an output contract, not a command name.

### Added — BPMN diagrams in the generated catalog

`sutra generate docs` now opens each BPMN page with a diagram in BPMN notation, auto-laid-out
from the process graph (authored BPMN carries no `<bpmndi:BPMNDiagram>`, and needs none). The SVG
is embedded scaled to the column and linked to a full-size copy written beside the page.

### Added — a schema codec can bind any non-opaque built-in format (csv included)

The `format × schema` composition the SPI already carried (`MessageFormat` + `MessageSchema` →
`SchemaBoundCodec`) is now reachable from a `codec-manifest.yaml`. Previously the two builders
were hardcoded — an XSD codec accepted `xml`/`json`/`yaml` and a JSON-schema codec was pinned to
`JsonNodeFormat` — so a CSV upload could only be a bare format, i.e. a bag of untyped strings with
nothing asserted. Design: ``.

- **`formats:` widens.** `schemaKind: xsd` accepts `xml`/`json`/`yaml`/**`csv`**/**`fixed-width`**;
  `schemaKind: json-schema` accepts `json`/`yaml`/**`csv`**/**`fixed-width`** — deliberately not `xml`, which belongs to XSD
  under the standing two-kinds ruling. An `Opaque`-shaped format (`raw-text`, `raw-bytes`) is
  refused with a message saying why: there is no map under raw bytes for a schema to type.
- **A tabular body is a BATCH, validated row-wise.** Each row is checked as one instance of the
  declared root, in a single decode before any process runs, and each violation carries a
  row-indexed path (`value[3].durationSec`). An unparseable *file* is `FATAL`; any row's violation
  is `SOFT_ERRORS`, so the payload still projects and `<q:onValidation>` decides the posture. The
  batch projects under `value`, matching the JSON-schema path.
- **Format layout is manifest config, not schema.** An optional `csv:` block carries `delimiter` /
  `header`; both default, so a header-bearing comma file needs no block. This is the home the
  deleted fixed-width codec lacked.
- **New:** `PayloadCodecFormat` (any `PayloadCodec` as a `MessageFormat`, so a format added later
  is bindable without a new impl), `JsonSchemaCodec::with_formats` (content-type negotiation
  across several parsers), `StructuralCodec::compile_with_layout`.
- **Moved:** `content_type::accepts` from `sutra-channels` to `sutra-codec-spi` (re-exported), so
  the codec layer negotiates by content-type the same way a channel does.

### Fixed — the engine ignored `codec-manifest.yaml` entirely

The assembly hardcoded `["xml","json","yaml"]` and never opened the manifest, so a
`formats: [csv]` (or `[fixed-width]`) codec was inert at RUNTIME: the package sealed cleanly,
lint passed, the codec unit tests passed — and a running engine then refused every upload as an
unsupported content-type. `schema_codec_loader::compile_module_codec` is now the single entry
point both `sutra lint` and the engine assembly build through, so what a package declares is what
a deployment honours. Guarded end to end by `call_log_csv_e2e`, which boots a real engine on the
committed example archive and posts both samples.

Consequences:

- **A manifest fault is a package-time ERROR** (`SUTRA.CONFIG.CODEC_MANIFEST.REJECTED`): an
  unknown format, a tabular format declared without its layout block, or a fixed-width layout
  whose columns disagree with the bound type. Lint had no other owner for these — its codec passes
  read the XSDs and never opened the manifest.
- **A layout fault is distinguished from an unsupported-XSD fault** (`LayoutCompileError`), so the
  "serve a shape-only codec" fallback that legitimately covers the latter cannot swallow the
  former.

### Changed — a codec may declare BOTH tabular formats

`formats: [csv, fixed-width]` is supported: their content types are disjoint (`text/csv` /
`application/csv` against `text/plain` / `application/x-fixed-width`), so an inbound body selects
its parser unambiguously and one schema serves a CSV feed and a fixed-width feed over the same
channel. Declaration order decides only the no-content-type fallback. (An earlier cut refused
this on the incorrect premise that both were indistinguishable plain text.)

### Added — `fixed-width` is reinstated, schema-bound

The format deleted in the `sutra-formats` carve-out is back, with the manifest layout block it
lacked — the stated reason for its removal was "no xsd/json way to express its column layout".

```yaml
schemaKind: xsd
formats: [fixed-width]
fixed-width:
  fields: [{name: recordId, width: 12}, {name: msisdn, width: 16}]
```

It registers **no** `BuiltinFormat` and so cannot be bound bare on a channel: without the widths a
line is an undifferentiated string. Declaring the format without the block is a manifest error.
`encode` is implemented (left-aligned, space-padded); an overlong value is an error rather than a
truncation, because truncating a fixed-width field shifts every column after it.

**The layout is verified against the schema at package time** — the one check csv cannot have,
since a fixed-width record's columns are its only field names. A column the bound type does not
declare, or a required element with no column, fails to compile.

### Fixed — an empty tabular cell is absence, not an empty value

A tabular row has a cell for every column whether or not it carries a value. An empty cell for an
element the type declares `minOccurs="0"` now reads as ABSENT; previously it emitted `<x></x>` and
failed the element's own facets, which made optional elements unusable with any tabular format. An
empty cell for a REQUIRED element is untouched and still reported.

Found by decoding the `call-log-load` example's own sample against its own schema — the package
lints clean, because lint never decodes anything. That round-trip is now a test.

### Fixed — a batch issue path no longer leaks a synthesised-XML offset

The XSD validator reports a position (`line 1:362`) into the one-line fragment the batch decoder
synthesises for a row. That offset is meaningless to whoever sent the file, so it is dropped rather
than appended: the path is `value[3]`, and the message already names the element.

### Added — `csv` encodes

`CsvCodec::encode` is implemented (rows → RFC 4180 bytes), so a csv channel can answer in csv and
an error can come back as a table. Column order is alphabetical: a decoded row is a `BTreeMap`, so
the source header's order is already gone — `decode(encode(rows)) == rows` holds, while
`encode(decode(bytes))` returns the same table with columns sorted.

### Changed — validation fails CLOSED when the flow declares no posture

An intake carrying a validation **contract** — a schema-backed codec, or a declared
`<q:validators>` chain — that declares no `<q:onValidation>` now **refuses** a failing payload
instead of passing it to the flow. Previously `apply_decoded` returned `Proceed` whenever no
policy was declared, whatever the issues were; with no `DecodeOutcome` check anywhere in the
dispatch path, that included a `FATAL` decode starting a process with a null payload.

The default is `reject`, not `error`: `error` raises a BPMN error into a process that by
definition declared no handler for it, reporting "uncaught BPMN error" instead of naming the
offending field.

**Migration:** declare `<q:onValidation mode="route"/>` to keep the previous pass-through, or
`mode="reject"` to state the new behaviour. `sutra lint` emits
`SUTRA.CONFIG.VALIDATION.POSTURE_UNDECLARED` (WARNING) on every affected intake, naming both.
**Schema-less ingress is unaffected**: a bare format has no contract to fail, and the format layer
is fail-open by construction.

### Changed — the RFC 7807 problem document is rendered in the caller's format

The problem *model* is unchanged and remains the contract. Its *serialisation* now follows the
inbound content-type: `application/problem+json` (default and fallback), `application/problem+xml`,
`application/problem+yaml`, and `text/csv` — where a batch's per-cell violations come back as a
table the sender can diff against the file they posted, instead of JSON they must re-parse to find
row 4,217.

### Fixed — multi-instance loop variables are visible to the linter

`sutra-loader`'s `variable_writers` did not enumerate `Node::MultiInstance`'s
`<bpmn:inputDataItem>` or `loopCounter`, so declaring the loop item variable — which is
*mandatory*, since an undeclared root fails the template-input check — produced a false
`SUTRA.CONFIG.VARIABLE.NEVER_INITIALIZED`. The two names now live in `sutra-bpmn`
(`DEFAULT_LOOP_ITEM_VARIABLE`, `LOOP_COUNTER_VARIABLE`) and are read by both the executor that
binds them and the linter that checks them, so they cannot drift. A cardinality-only loop binds no
item variable, so the warning still fires there.

### Added — the `call-log-load` example

A third public example: a CSV batch of call detail records, validated whole against an inbound
XSD by the codec, transformed record by record by a Handlebars script task, and written to a
projected data store whose columns are a second XSD's type. One channel, one process, two schemas.
Covered by the examples packaging gate.

### Changed — the workspace ships no message-standard codec

Every business codec left this workspace for a proprietary repository that composes it as a
submodule: the message-standard codecs, their schema corpora, the financial validators and the
domain redactors. **`builtin_codecs()` is now empty in `sutra-dist`** — the built-in set is the
six schema-less formats (`json`, `xml`, `yaml`, `csv`, `raw-text`, `raw-bytes`), and
`sutra-dist`'s bundle test asserts that emptiness as the link-time half of the neutrality
boundary. Nothing in the SPI changed; that is the point of it.

A deployment package still gets a typed contract with zero installation by bringing its own
XSD (`schemas/<codec>/`, referenced as `urn:<path>`), which is what both remaining examples
(money-transfer, approval-hold) do. A distribution that needs a message standard adds a Cargo
dependency and one `use <crate> as _;` line to its own composition root.

Consequences for this workspace:

- `make test-generated` is **removed**, and every `--exclude` flag for the generated binding
  crate is gone: `make test` is `cargo test --workspace` and `make lint` is
  `cargo clippy --workspace --all-targets -- -D warnings`. No crate is carved out of the
  routine tiers any more.
- `sutra-schema-gen` (shipped as `sutra schemagen generate|check`) stays — it is a neutral
  tool over arbitrary corpus and output paths, and its golden gate now compares against a
  vendored emission under `crates/sutra-schema-gen/tests/data/`.
- `sutra-xsd` stays and is test-self-contained: nine schema-corpus fixtures are vendored
  under `crates/sutra-xsd/tests/data/schemas/` (with provenance), and the full-corpus compile
  sweep moved to the repository that owns the corpus.
- Tier-3 is `k8s_money_transfer` (deploy smoke) + `k8s_observability` (OTLP signals), both
  driving the money-transfer package.

## [0.2.0-rc.1] - 2026-07-13

First release candidate of the Rust-native engine. The workspace is a from-scratch
rewrite that reproduces the reference baseline's behaviour, proven against the same example
integration suites (money-transfer, approval-hold, and two now-retired message-standard
examples) on tier-2 (Testcontainers) and tier-3 (k8s) at each phase gate.

### Added

- **Kernel crates** — `sutra-feel` (FEEL subset lexer/parser/evaluator, DECIMAL64
  arithmetic mirrored from the reference baseline, determinism guard + denylist), `sutra-dmn`
  (decision tables, COLLECT hit policy, per-rule codes), `sutra-bpmn` (token executor:
  start/service/call-activity/exclusive+parallel+inclusive gateways/error+compensation/
  multi-instance), `sutra-templates` (Handlebars, byte-parity with the reference
  baseline's rendered output, strict `helperMissing`), `sutra-executor` (the step engine).
- **Persistence** — `sutra-persistence`: `sqlx` PostgreSQL stores (7 store families),
  snapshot v2 golden-locked against the reference bytes, the strict transactional step primitive with
  crash-injection proofs, and RLS via the `sutra.deployment_id` GUC. Dialect stores for
  MySQL/MariaDB/MSSQL (SKIP LOCKED / READPAST, unique-live strategies, per-dialect
  strict-step crash proofs). Vendored shipped migrations sharing `sutra_schema_history`
  (ledger-interoperable with the reference baseline's `sutra-migrate`).
- **Channels & codecs** — `sutra-channels` + `sutra-codecs`: axum HTTP intake, `q:reply`
  outbound, `X-Api-Key` intake auth; RabbitMQ (`lapin`, AMQP 0.9.1) and Kafka (`rdkafka`,
  PLAINTEXT) sources/sinks with leader-gated supervisors and ack→settle mapping; codecs
  for the message-standard formats (protocol-specific parsers/validators, later split to a
  separate repository), plus builtin csv / fixed-width / raw / xml / json / yaml
  (XXE-hardened).
- **Loader & packaging** — `sutra-loader`: deterministic `.sutra` archive writer +
  verifying (sealed) reader, manifest-hash `deploymentId` (`dep-<24hex>`), fail-closed
  lint with 16 `SUTRA.DEPLOY.*` codes, archive-mode activation with a two-phase flip and a
  deployments-dir watcher (DRAINING→retire), authoring-input package directories.
- **Datastore** — `sutra-datastore`: env-ref connection resolution (`env:` / `secret:`),
  revision compare-and-set.
- **Engine** — `sutra-engine`: canonical `SUTRA_*` configuration, `/sutra/health/ready`, structured
  JSON logs, OTLP telemetry (traces/metrics/logs over gRPC, fail-open), the outbox
  dispatcher with exact backoff arithmetic and poison-arming, per-broker CloudEvents
  binding prefixes, graceful SIGTERM drain.
- **CLI** — `sutra`: `migrate` / `status` / `verify` (checksummed migration ledger),
  `deploy` / `undeploy` on kube-rs (Secret-FIRST ordering, `--gc-secrets`, 1MiB ConfigMap
  ceiling), `deployments list [--label]`, `create app` / `deployment` / `bpmn` (fully
  declarative scaffold — no application code), `coverage init` / `check`, `describe`,
  `dispatch-graph`, `compat-baseline`,
  `explain`, `audit-replay`, `simulate --dry-run`.
- **Schema generator** — `sutra-schema-gen`: document-order XSD→Rust generator (116
  schemas byte-identical, zero binding-file config), producing the generated
  binding crate; a `check` CLI gate proves drift cheaply (no `rustc`).
- **XSD validator** — `sutra-xsd`: Tier-1 subset validator + shape emission (0 divergences
  over 206 differential cases against an independent reference validator).
- **Catalog generator** — `sutra-catalog-gen`: `syn`-parsed `rust/**`
  catalog pages (bidirectional Relationships, deterministic `--check`).
- **Test harness** — `sutra-testkit`: Testcontainers engine + CLI drivers, container
  reaper.
- **Deployment** — k8s shared-scenario OpenTofu (one engine + estate `Secret` at mode
  `0400` + CLI-owned ConfigMap + one Ingress), with a live hot-deploy replace-in-place
  path.

### Changed

- **Namespace root** is `sutra` — every carried-forward `trade.startr.` name removed; the
  Rust tree has zero remaining occurrences (test-enforced ban).
- **Wire vocabulary** — CloudEvents `sutra.channel.reply`; carriers `sutra-outbox-key`
  / `sutra-reply-to`; datasource `type: sql` with `sql.*` properties; `q:audit`
  `sink="sql"`; `auditRetentionDays`; templates are Handlebars (`.hbs`).
- **Configuration** — the engine, testkit, and deploy tooling read canonical `SUTRA_*`
  configuration only; no other environment prefix is consulted.
- **Identity** — `deploymentId` rekeyed to `dep-<24hex>` derived at the authoring boundary;
  snapshot v2 is `sutra.*`-native; RLS moved to the `sutra.deployment_id` GUC.

### Security

- **Secret handling** — the `secret:` env-ref scheme resolves through tmpfs `Secret` mounts
  with flip-abort parity; a vocabulary-agnostic `SUTRA.DEPLOY.CREDENTIALS.LITERAL` lint
  rejects inline credentials; `sutra deploy` writes the estate `Secret` FIRST (mocked-API
  ordering test) at mode `0400`, honours the 1MiB ConfigMap ceiling, and supports
  `--gc-secrets`.
- **Fail-closed posture** — the XSLT-subset transform engine and the sealed archive reader
  both fail closed; boot refuses an RLS-bypassing database role.
- A full STRIDE pass over the Rust surface backs this posture; the supply-chain gate
  (`cargo-audit` + `cargo-deny`) is `make audit` with `rust/deny.toml`.

### Notes

- The generated binding crate (707k lines) is excluded from routine `make test` /
  `make lint`; its own gates run via `make test-generated` only after a regeneration.

[Unreleased]: https://github.com/startr-trade/sutra/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/startr-trade/sutra/releases/tag/v0.4.0
[0.3.0]: https://github.com/startr-trade/sutra/releases/tag/v0.3.0
[0.2.0-rc.4]: https://github.com/startr-trade/sutra/releases/tag/v0.2.0-rc.4
[0.2.0-rc.3]: https://github.com/startr-trade/sutra/releases/tag/v0.2.0-rc.3
[0.2.0-rc.2]: https://github.com/startr-trade/sutra/releases/tag/v0.2.0-rc.2
[0.2.0-rc.1]: https://github.com/startr-trade/sutra/releases/tag/v0.2.0-rc.1
