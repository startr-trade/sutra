# Sutra

**A Rust-native, message-native workflow engine built on BPMN 2.0.** Sutra turns a standard
BPMN process into a declarative way to **consume typed, schema-validated messages off any
channel**, route and correlate them, **pause for human decisions**, and reply — a single
statically-compiled binary (no JVM, no GC) that is multi-tenant, content-addressed, and
hot-deployable.

Most workflow engines treat the payload as an opaque blob and start a process with a REST
call. Sutra makes the **message a first-class, typed contract**: a start event binds to a
*channel* (HTTP or a broker) and a *message type*; the engine **decodes the real wire format,
validates it against its schema on the way in**, and drives the process with the typed
payload, surfacing violations as *routable soft errors* rather than exceptions. The engine
ships the structural formats (JSON / XML / YAML / CSV / raw text and bytes); a typed contract
comes from the deployment package's own XSDs, compiled at deploy time, or from an extension
crate against the codec SPI — which is how a message standard (an industry wire format
with its own envelope grammar and schema editions) is served. The engine core is
**domain-neutral**: no business vertical lives in the engine itself — it lives entirely in
codecs, channels, and modules.

Nearly every production-grade BPMN 2.0 engine is JVM-based; the production-grade native-language
orchestrators (Temporal, Cadence, Argo) deliberately *aren't* BPMN. Sutra is built for that gap.

## What makes Sutra different

- **Rust-native — no JVM, no GC, no GraalVM.** One statically-compiled binary; no managed
  runtime, no reflection, no CDI.
- **Typed message contracts — codec = format × schema.** Decode *and* schema-validate real
  wire formats at intake, with a two-tier check (structural codec + business rules). Six
  formats are built in; a schema-bound codec comes from the package's own XSDs (no Rust, no
  engine build) or from an extension crate against the public codec SPI.
- **Message- and channel-native.** Processes are triggered by messages arriving on channels —
  HTTP, five brokers (Kafka, RabbitMQ, AWS SQS, Google Pub/Sub, AMQP 1.0), and an air-gapped
  file transport — not by a "start process" API.
- **Stateful, human-in-the-loop — correlated by *your* business key.** Wait states
  **suspend → persist → rehydrate → resume**; a decision arriving on a channel is correlated to
  the parked instance by a business key you name (`<q:alias>`), durable on PostgreSQL.
- **Rules the way developers want them — DMN + `.srl` + FEEL.** A `businessRuleTask` binds a
  `.dmn` decision table **or** a `.srl` ruleset (a Drools-inspired `rule / when / then` DSL),
  both compiling onto one shared FEEL evaluator. No JVM, no Rete runtime.
- **A declarative `q:` vocabulary on standard BPMN.** `<q:dispatch>` / `<q:case>` routing
  tables, `<q:validators>`, `<q:alias>`, `<q:reply>`, `<q:variables>`, `<q:audit>`,
  `<q:coverage>` — extensions that keep diagrams standard while removing boilerplate.
- **Static validation — schema & path errors at deploy, not at 3 a.m.** Every FEEL path is
  checked against the codec's schema at load time; `sutra lint` runs the same gate in CI. The
  VS Code extension runs *the same Rust core compiled to WASM*.
- **Content-addressed deployment packages — idempotent deploy + hot-reload.** The deploy unit
  is one sealed `.sutra` archive; runtime identity is `deploymentId = sha256(manifest)`.
  Secrets never live in the archive — channels reference them by scheme, resolved at runtime
  through one vendor-neutral SPI.
- **Multi-tenant, cloud-native & observable.** Per-deployment row-level security across PG /
  MySQL / MariaDB / MSSQL; OTLP traces / metrics / logs to any collector; KEDA autoscaling and
  leader election. `sutra create app` scaffolds an app + OpenTofu deploy.

## Repository layout

The engine, tooling, and libraries are a single Cargo workspace under `rust/`.

| Path | What it is |
|---|---|
| **`rust/`** | The Cargo workspace — the engine, CLI, libraries and tools. Start at [`rust/README.md`](rust/README.md); build & test tiers in [`rust/TESTING.md`](rust/TESTING.md). |
| **`docs/`** | The documentation site ([mdBook](https://rust-lang.github.io/mdBook/)) — build with `mdbook serve docs`. |
| **`catalog/`** | Generated impact-analysis catalog — one page per source file, with dependency relationships (git-ignored; run `make catalog` to populate). |
| **`examples/`** | End-to-end example apps (money-transfer, approval-hold). |
| **`deploy/`** | Reusable OpenTofu deployment modules. |
| **`xsd/`** | Authoritative XSD schemas for BPMN, DMN, and the Sutra `q:` extension namespace. |
| **`openapi/`** | API specifications. |
| **`scripts/`** | Repository-level dev / ops scripts. |

## Quickstart

Prerequisites: a Rust toolchain, Docker (for the tier-2 / conformance suites), and `tofu` + a
kind cluster (for the tier-3 k8s suites only).

```bash
# Build + test the workspace (tier-1: no Docker)
make test
make lint

# Build the engine container image
docker build -t sutra-engine:dev -f rust/Dockerfile rust/

# The CLI packages, deploys, and inspects deployments
cd rust && cargo run -p sutra-cli -- --help
```

A one-line installer for the released `sutra` CLI and a published container image will
accompany the first tagged release. See the [documentation](docs/) to get started.

## Documentation

- **[The Sutra book](docs/)** — Getting Started, Concepts, Guides, and Reference (mdBook).
- **[Examples](examples/)** — runnable end-to-end apps with per-example READMEs.
- **[Contributing](CONTRIBUTING.md)** · **[Security policy](SECURITY.md)** ·
  **[Code of Conduct](CODE_OF_CONDUCT.md)**

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in Sutra by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without
any additional terms or conditions.
