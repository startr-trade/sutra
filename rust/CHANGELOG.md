# Changelog

All notable changes to the Sutra Rust workspace are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This is the changelog for the **Rust-native** engine (`rust/` workspace). It begins at
`0.2.0-rc.1`, the first release candidate of the ground-up Rust rewrite. The earlier
`v0.1.0` line belonged to the retired reference baseline and is unrelated version history.

## [Unreleased]

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
