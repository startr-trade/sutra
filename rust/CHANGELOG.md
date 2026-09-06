# Changelog

All notable changes to the Sutra Rust workspace are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This is the changelog for the **Rust-native** engine (`rust/` workspace). It begins at
`0.2.0-rc.1`, the first release candidate of the ground-up Rust rewrite. The earlier
`v0.1.0` line belonged to the retired reference baseline and is unrelated version history.

## [Unreleased]

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
nothing asserted. Design: `docs/design/schema-format-binding.md`.

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

### In flight

The Rust-native GA cycle is in flight on top of this candidate: the retired reference
baseline moves out of the product tree, the example ITs become a Rust conformance
harness, three additional brokers (SQS, GCP Pub/Sub, AMQP 1.0) land, and the deferred
feature gaps (message-level broker auth, inbound CloudEvents over HTTP, inbound bearer,
the `envref` resolver registry with a Vault KV-v2 resolver) close.

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
  artifact-documentation pages (bidirectional Relationships, deterministic `--check`).
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

[Unreleased]: https://github.com/startr-trade/sutra/compare/v0.2.0-rc.1...HEAD
[0.2.0-rc.1]: https://github.com/startr-trade/sutra/releases/tag/v0.2.0-rc.1
