# CLAUDE.md

Guidance for an AI agent (Claude or otherwise) working cold in this repository. The developer
book under `docs/` is the narrative version of everything below — read it if you need context
beyond what fits here; this file is the mechanical quick-reference.

## What this repo is

Sutra: a Rust-native, message-native BPMN 2.0 + DMN workflow engine. One Cargo workspace
(`rust/`), a set of end-to-end example apps (`examples/`), OpenTofu deployment modules
(`deploy/`), authoritative XSD schemas (`xsd/`), and this mdBook (`docs/`).

## Repository layout

| Path | What it is |
|---|---|
| `rust/crates/` | The whole engine, one crate per concern — see "Crate layering" below. |
| `docs/src/` | This book. `docs/src/SUMMARY.md` is the table of contents; every page must resolve as a relative link from where it's referenced. |
| `examples/` | Runnable deployment packages (money-transfer, approval-hold). Each has its own README with exact run/deploy commands. |
| `deploy/` | OpenTofu modules (`modules/sutra`, `modules/efk-stack`) + local dev compose (`deploy/compose/`). |
| `xsd/q.xsd` | The authoritative shape of every `<q:*>` BPMN extension element. Read this, not a design doc, when you need the exact attributes/types a `q:` element supports. |
| `tools/` | `sutra-vscode`, `sutra-modeler-plugin`, `sutra-load-test` — user-facing tooling outside the Cargo workspace. |

## Crate layering (do not violate this)

```
sutra-dist        composition root — the ONLY crate allowed to name concrete codecs/
                   transports/secret-resolvers. Force-links them, produces the `sutra-engine`
                   binary (docker build -f rust/Dockerfile rust/).
sutra-engine       the engine library. Domain-neutral: collects codecs/transports/resolvers
                   generically via their SPI, names none of them.
sutra-channels     protocol-neutral channel binding + dispatch.
sutra-executor     the token executor (BPMN gateways, sub-processes, data associations, …).
sutra-bpmn         the BPMN 2.0 + q: model and loader.
sutra-feel / sutra-dmn / sutra-srl / sutra-templates
                   the expression / decision / template languages. sutra-feel and sutra-dmn
                   are ALSO published standalone crates (crates.io) — keep their public API
                   changes semver-honest independent of the rest of the workspace.
sutra-persistence  durable state (instances, outbox, inbox dedup, lease, audit).
```

Concrete extensions (one crate per concept) plug into this via a neutral SPI crate:
`sutra-codec-spi` (+ `sutra-formats` and `sutra-codec-schema` here; `sutra-codec-<standard>`
implementations out of tree), `sutra-transport-spi` (+ `sutra-transport-<vendor>`),
`sutra-datastore`, `sutra-envref-spi` (+ `sutra-envref-<vendor>`), `sutra-redactor-spi`
(+ `sutra-redactor-<standard>`). Each
implementation crate `inventory::submit!`s its registration next to its impl — implementing an
extension *is* registering it. Never add a name-check for a specific vendor/standard anywhere in
`sutra-engine` or `sutra-channels`; that's what the SPI is for. Full detail:
`docs/src/architecture/neutrality-and-spi.md`.

## Load-bearing rules

1. **The engine core never names a business domain, a vendor, or a wire format.**
   `rust/crates/sutra-archtest` mechanically enforces this: it fails the build on any of
   `swift fednow fedwire pacs camt nacha edifact hl7 iso20022 x12` appearing as an identifier or
   string literal (not a comment) inside the neutral-core and assembly crates
   (`NEUTRAL_CORE_CRATES` + `GATED_ASSEMBLY_CRATES` in that crate's `lib.rs` — read it for the
   exact, current list rather than assuming). `sutra-dist` and the concrete extension crates are
   deliberately excluded — that's the legitimate wiring boundary, not a leak. Run `make lint` to
   check yourself before it's a CI failure.
2. **Zero-warning clippy is a hard gate, not a preference.**
   `cargo clippy --workspace --all-targets -- -D warnings` must be clean. `make lint` runs this
   plus the neutrality gate above.
3. **The `q:` XML namespace is `urn:sutra:q:1.0`.** Frozen surface — `xsd/q.xsd` is authoritative;
   changing it is semver-major.
4. **Vendor identity: none.** This is a community/OSS project (MIT OR Apache-2.0, copyright "The
   Sutra Authors"). Don't attribute the project to a company name anywhere you touch — there
   isn't one to name.
5. **The engine ships no domain codec.** The built-in codec set is the schema-less formats in
   `sutra-formats` (`json`, `xml`, `yaml`, `csv`, `raw-text`, `raw-bytes`); `sutra-dist`
   force-links nothing else that registers one. Every message standard (a payments-network
   format, a clinical-data interchange standard, an EDI dialect, …) is an extension crate
   maintained outside this repository — don't add one here. A deployment package that needs a
   typed contract supplies its own XSDs under `schemas/<name>/`, compiled at deploy time by
   `sutra-codec-schema`.
6. **Secrets are references, never literals.** `env:NAME`, `secret:KEY`, `${NAME}`, or a vendor
   scheme (`vault:…`, `aws-secrets:…`) — a literal secret in a resource file (`channels.yaml`,
   `datastores.yaml`) is rejected at package-validation time, not just discouraged.
7. **A deployment package is self-contained.** No shared resource tree, no inheritance between
   packages — `tenant`/`module`/`version` are opaque `package.yaml` labels, not a folder
   hierarchy. Don't reintroduce a `tenants/<id>/modules/<m>/<v>/` layout; that model is retired
   (see `docs/src/building/deployment-packages.md`).

## Test tiers — reproduce at the lowest one that shows the bug

| Tier | Command | Needs |
|---|---|---|
| 1 | `make test` | Rust toolchain only — the default while iterating |
| 2 | `make test-docker` (`P=<crate>` narrows it) | Docker daemon |
| 1+2 | `make test-all` | Docker daemon |
| 3 | `make test-k8s` | A running `kind` cluster (`make -C deploy/k8s-it init` first) |

Add a new test as a module in the crate's `tests/all.rs` (a new file under `tests/all/` + one
`mod <name>;` line), not a new top-level `tests/*.rs`. A container-spawning test gets
`#[ignore = "docker"]` and must call `sutra_testkit::reap_on_exit(container.id())`. Full detail:
`rust/TESTING.md` and `docs/src/debugging-the-engine.md`.

## How to verify a change before calling it done

```bash
cd rust
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test  --workspace
```

Equivalently, from the repo root: `make lint && make test`. This is exactly what
`.github/workflows/ci.yml` runs on every PR — green locally means green in CI. If your change
touches Docker-dependent behavior (a transport, persistence, replica semantics), also run
`make test-docker P=<the crate you touched>` before considering it verified; don't rely on the
nightly tier-2 run to catch it first.

If your change adds or removes a public type, an SPI trait, a diagnostic code, or a config key,
regenerate the impact-analysis catalog to see every affected call site: `make catalog` (output is
git-ignored under `catalog/` — never commit it).

## Where designs live

Deeper rationale than the book covers — why a mechanism is shaped the way it is, staged plans for
work in flight — lives in `docs/design/` in the private working repository this public repo is
curated from. This public repo does not carry that tree; if you need "why," check the relevant
book chapter first (each links back to the concept it's explaining), and if it's still not
answered, that's a gap worth flagging rather than guessing at.

## Docs

This book (`docs/`) is built with [mdBook](https://rust-lang.github.io/mdBook/):
`mdbook build docs` or `mdbook serve docs` (the book's diagrams are ```mermaid blocks, so
`mdbook-mermaid` must be on PATH or they render as code listings). Every relative link in `docs/src/**/*.md` must resolve
— check with `mdbook build` before treating a docs change as complete if mdBook is installed
locally; if it isn't, at minimum grep for the link targets you added/changed and confirm the
files exist at those relative paths.
