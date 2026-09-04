# The Sutra Rust workspace

Everything that compiles lives here: the engine library and its binary, the `sutra` CLI, the
expression/decision/template languages, the persistence layer, the transports, and the
developer tooling. It is one Cargo workspace with ~44 crates and no build script magic —
`cargo build` from this directory is the whole story.

```bash
cargo build                # the workspace
cargo test                 # tier-1 tests (no Docker) — see TESTING.md
cargo run -p sutra-cli -- --help
```

Rust 1.96 or newer. The repository root `Makefile` wraps the canonical commands
(`make help` lists them); [`TESTING.md`](TESTING.md) explains the three test tiers.

## How the crates group

The layering — how a message travels from a channel through the executor to durable state — is
described in the book's *Engine layering* chapter. This table is the flat index.

### Runtime core

| Crate | What it is |
|---|---|
| `sutra-bpmn` | The BPMN 2.0 + `q:` extension model and loader: node variants, `ProcessDefinition`, `QBindings`, `CoveragePath`, every load-time validation with its `SUTRA.*` diagnostic code, and the XXE-hardened parse posture (no DTD, no external entities). |
| `sutra-executor` | The token executor: data tasks, all four gateway kinds, `serviceTask` routing, embedded/transaction/ad-hoc/event sub-processes, link/escalation/error events, compensation, emissions, and path-coverage marking. Runs both the synchronous and the suspend/resume paths. |
| `sutra-channels` | Channel binding and the intake pipeline — inbox dedup, decode, two-tier validation, alias materialisation, start-event routing, dispatch, ack — plus the outbox delivery spine. Protocol-neutral: it names no broker. |
| `sutra-persistence` | Durable state on SQL: instance snapshots, outbox, inbox dedup, aliases, wait states, lease, audit, incidents. PostgreSQL is the reference dialect; MySQL/MariaDB and SQL Server follow the same store traits. |
| `sutra-datastore` | Business data stores (`<q:store>`) — the providers a process reads and writes through, across the same four dialects. |
| `sutra-engine` | The engine library: configuration, deployment loading and activation, the platform API, OpenTelemetry wiring, audit sinks, the outbox tick loop, health endpoints. Domain-neutral — it collects codecs, transports and secret resolvers generically through their SPIs and names none of them. |
| `sutra-dist` | The composition root. Force-links the concrete built-ins and produces the `sutra-engine` binary the container image ships. |

### Languages

| Crate | What it is |
|---|---|
| `sutra-feel` | The FEEL subset — lexer, parser, AST, evaluator, path extraction, source positions. Map/data-only (no host-object introspection) and deterministic: `now`/`today`/`random`/`uuid` are refused, and evaluation time arrives as an injected input. Numbers are DECIMAL64 (16 significant digits, half-even rounding). |
| `sutra-dmn` | The DMN 1.5 decision-table core over that evaluator: file loader, model, unary-test translator, DRG evaluation, ruleset validation with all seven hit policies. |
| `sutra-srl` | The `.srl` rule language — a `rule / when / then` DSL that compiles onto the same FEEL evaluator, adding only the rule framing (declaration, salience, activation groups, two side-effecting verbs). Stateless: there is no working memory, no `insert`/`retract`, no re-activation. |
| `sutra-templates` | Strict-mode Handlebars: a missing value fails the render (missing simple variables, dotted paths and null-mid-path all fail; helper parameters stay null-tolerant), NOOP escaping, a fixed helper set, and a static template analyzer. `uuid`/`now` are render-context suppliers injected by the caller — never wall-clock reads inside the engine. |

### Payload contracts

| Crate | What it is |
|---|---|
| `sutra-codec-spi` | The neutral codec SPI: `PayloadCodec` / `MessageFormat` / `MessageSchema`, the shape/result/issue types, and the self-registration registry every codec joins. |
| `sutra-formats` | The schema-less built-ins — `json`, `xml`, `yaml`, `csv`, `raw-text`, `raw-bytes`. |
| `sutra-codec-schema` | Schema-backed codecs built on demand from a deployment archive's own schemas (XSD structural, JSON Schema). |
| `sutra-xsd` | An XSD-subset compiler with two back-ends: a streaming instance validator that collects every violation with `line:col`, and navigation-shape emission that lets template and FEEL paths be checked at deploy time. |
| `sutra-schema-gen` | The offline generator (`sutra generate schema-handler`): parses a directory of message-definition XSDs and emits Rust binding sources — model, lenient decoder, canonical map projection, shape metadata. |
| `sutra-redactor-spi`, `sutra-redactor-template` | The content-redactor SPI and the archive-supplied Handlebars redactor built on it. |
| `sutra-crypto` | The encryption-at-rest seam: an AES-256-GCM payload cipher plus an envelope key provider, so `@sensitive` variables persist as ciphertext. |

### Transports and secret resolvers

`sutra-transport-spi` holds the contract — the lifecycle trait, the inventory-collected factory,
the DB-lease leader election, and the engine intake adapter. Each concrete transport is its own
crate that self-registers through it: `sutra-transport-http` (native and always present),
`-kafka`, `-rabbitmq`, `-sqs`, `-gcp-pubsub`, `-amqp`, `-dapr`, `-knative`, and `-file` for
air-gapped spooling. Secret references resolve the same way — `sutra-envref-spi` plus
`-vault`, `-aws`, `-azure`, `-gcp` — so the engine core names no vendor.

### Tooling

| Crate | What it is |
|---|---|
| `sutra-cli` | The `sutra` binary: `package`, `lint`, `deploy`, `migrate`, `create`, `coverage`, `openapi`, `schemagen`, `docgen`, and the read-only inspectors (`describe`, `dispatch-graph`, `simulate`, `explain`). Not linked into the engine binary. |
| `sutra-loader` | The `.sutra` archive format owner — authoring-tree resolver, package/lint library, archive writer and reader, shared by CLI and engine. |
| `sutra-lint-core` | The pure, I/O-free deploy-time lint core, carved out so it compiles to WASM and the VS Code extension runs the *same* checks as `sutra lint`. |
| `sutra-openapi` | Projects a deployment's parsed manifest into an OpenAPI 3.1 document, served live per deployment and emitted offline by the CLI. |
| `sutra-catalog-gen`, `sutra-docgen` | Documentation generators: the source-file impact-analysis catalog, and the authored-artifact pages (BPMN, rules, templates, channel and package manifests). |
| `sutra-testkit` | Shared test harness — container fixtures, the exit-time reaper, and the conformance helpers the integration suites reuse. |
| `sutra-conformance` | The cross-cutting integration suites: container-backed (tier-2) and Kubernetes (tier-3). |
| `sutra-archtest` | The domain-neutrality gate. A structural test that fails the build when a business-domain literal leaks into the neutral core — it must name the words it bans, which is why they appear in that crate and nowhere else. |
| `sutra-bench-packager` | A benchmark-harness helper that seals a package directory into a `.sutra` archive without force-linking the built-in codec set. |

## Domain neutrality

The engine core carries no business vertical. Message standards — industry wire formats with
their own envelope grammars and schema editions — are **extension crates built outside this
repository**: they implement the codec, validator and redactor SPIs, register through the same
inventory mechanism, and are force-linked by their own composition root. `sutra-archtest`
enforces the boundary on every build, and the book's *Domain neutrality and the SPI model*
chapter describes exactly what a third party has to write.

## Where to go next

- [`TESTING.md`](TESTING.md) — the three test tiers, the fixture reaper, and the conventions for
  adding a test.
- The book under `docs/` — concepts, guides, architecture, and the operator reference.
- `CONTRIBUTING.md` at the repository root — how to propose a change.
