# Contributing

Contributions are welcome! This page covers the repo map, the day-to-day dev workflow, the code
style and PR bar, and the license. If you're an AI agent working on this codebase, also read
[`CLAUDE.md`](https://github.com/startr-trade/sutra/blob/main/CLAUDE.md) at the repository root —
it's a shorter, more mechanical companion to this page written specifically for that audience.

## Repository map

The engine, tooling, and libraries are a single Cargo workspace under `rust/`:

| Path | What it is |
|---|---|
| `rust/` | The Cargo workspace — engine, CLI, libraries, tools. Start at `rust/README.md`; build/test tiers in `rust/TESTING.md`. |
| `docs/` | This book (mdBook) — `book.toml` + `src/SUMMARY.md`. |
| `examples/` | End-to-end example apps (money-transfer, approval-hold). See [Worked example: money-transfer](building/worked-example.md). |
| `deploy/` | Reusable OpenTofu deployment modules. |
| `xsd/` | Authoritative XSD schemas for BPMN, DMN, and the `q:` extension namespace. |
| `openapi/` | API specifications. |
| `scripts/` | Repository-level dev/ops scripts. |
| `tools/` | User-facing tooling: `sutra-vscode`, `sutra-modeler-plugin`, `sutra-load-test`. |

Within `rust/crates/`, the layering described in
[Engine layering](architecture/overview.md) maps directly onto directories — `sutra-engine` (the
library), `sutra-dist` (the composition root that builds the actual binary), `sutra-channels`,
`sutra-executor`, `sutra-bpmn`, `sutra-feel`/`sutra-dmn`/`sutra-srl`/`sutra-templates`,
`sutra-persistence`, `sutra-formats` (the built-in formats) alongside `sutra-codec-schema`, one
`sutra-transport-<vendor>` crate per broker, and the SPI crates each of those builds against
(`sutra-codec-spi`, `sutra-transport-spi`, `sutra-datastore`, `sutra-envref-spi`,
`sutra-redactor-spi`) — see
[Domain neutrality and the SPI model](architecture/neutrality-and-spi.md) for exactly how the
pieces fit and how to add a new one.

## Development workflow

You need a stable Rust toolchain ([rustup](https://rustup.rs)); Docker is only needed for the
container/integration test tiers, and `tofu` plus a `kind` cluster only for the Kubernetes tier.
`make help` (from the repo root) lists every target; full detail lives in `rust/TESTING.md`.

```bash
cd rust && cargo build                 # whole workspace, debug
cargo build --release -p sutra-cli     # the `sutra` CLI binary
cargo build --release -p sutra-dist    # the `sutra-engine` binary (the composition root)
```

### Test tiers

| Tier | `make` target | What it needs |
|---|---|---|
| 1 | `make test` | Rust toolchain only — the default gate while iterating |
| 2 | `make test-docker` (`P=<crate>` narrows to one crate) | A Docker daemon |
| 1+2 | `make test-all` | A Docker daemon |
| 3 | `make test-k8s` | A running `kind` cluster (`make -C deploy/k8s-it init` first) |

The CI workflow (`.github/workflows/ci.yml`) mirrors the local gate chain exactly — `cargo fmt
--all --check`, `make lint`, `make test`, `make audit` — so a contributor who is green locally is
green in CI. Tier-2 runs nightly rather than per-PR (a Docker-heavy suite is slow and
resource-flaky on a small hosted runner for signal tier-1 already covers on most changes); tier-3
stays a local/milestone gate, since it needs a provisioned `kind` cluster CI doesn't have.
CodeQL and Trivy scans run on their own schedules alongside CI.

Tier-1 covers the whole workspace — no crate is held out of it, so a green `make test` means the
whole workspace is green, not a subset of it.

### Lint and supply-chain gates

```bash
make lint    # cargo clippy --workspace --all-targets -- -D warnings,
             #   plus the sutra-archtest domain-neutrality suite
make audit   # cargo audit (RustSec advisories) + cargo deny check (rust/deny.toml)
```

`make lint` failing on a domain-neutrality violation means a business term landed in a crate
[the gate enforces](architecture/neutrality-and-spi.md#the-gate-sutra-archtest) — move the
concrete logic into its own extension crate instead.

### The generated catalog (on demand, not committed)

```bash
make catalog         # regenerate the artifact-documentation catalog under catalog/
make catalog-check   # verify it's in sync
```

The catalog — one page per source file plus its dependency relationships, produced by
`sutra-catalog-gen` — is generated on demand and **git-ignored**; there's no committed baseline
to diff against, so it isn't part of the CI gate today. Run it locally when you want the
dependency picture for a change you're making; `make install-hooks` wires an optional pre-commit
regeneration hook if you want it kept fresh automatically.

## Code style and the PR bar

1. **Discuss anything large first.** A new transport, a new codec, an architectural change —
   open an issue before you invest the time.
2. **Keep PRs focused.** One logical change reviews faster than a bundle of unrelated ones.
3. **Be green locally before pushing:**
   ```bash
   cd rust
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test  --workspace
   ```
4. **Add a `CHANGELOG.md` entry** under `[Unreleased]` for a user-visible change.
5. **Commit messages** are short, imperative, area-prefixed:
   ```
   feat(channels): add AMQP 1.0 outbound reply path
   fix(feel): correct DECIMAL64 rounding on division
   docs(concepts): clarify wait-state correlation keys
   ```

The zero-warning clippy bar and the domain-neutrality gate are both hard requirements, not style
preferences — see [Domain neutrality and the SPI model](architecture/neutrality-and-spi.md) for
why the latter exists.

### Diagrams in this book

Diagrams are [mermaid](https://mermaid.js.org) in fenced ```` ```mermaid ```` blocks, rendered by
the `mdbook-mermaid` preprocessor (put it on your PATH, or your local build shows the source
instead of the picture). Four rules keep them consistent:

1. **No hardcoded colors** — no `style … fill:#…`, no `classDef`. The book has a light and a dark
   theme and mermaid picks its own palette for each; a hardcoded fill is unreadable in one of
   them. Shape, label and subgraph carry the meaning.
2. **A diagram earns its place** by compressing something the reader would otherwise hold in
   their head — an ordering, a state machine, a fan-in, a comparison. A bullet list drawn as
   boxes is not a diagram.
3. **Keep it small** (roughly a dozen nodes). If it needs more, it is two diagrams.
4. **Label the edges** where the label is the information (`park`, `after PT30S`,
   `hash(instanceId)`), and put one or two sentences under the diagram naming the takeaway.

The diagram must say what the prose says: no fact appears only in a picture, and nothing in a
picture contradicts the text.

Diagrams render at their natural size rather than being scaled down into the content column
(that scaling is what makes wide diagrams unreadable), so a wide one scrolls inside its own
block. **Clicking any diagram opens it full size in a new tab**, where the browser's own zoom,
save and print apply — which is also the escape hatch when a diagram is genuinely large.

## Where designs live

Deeper design rationale than this book covers — why a mechanism was built the way it was, staged
plans for work in flight — lives in `docs/design/` in the source (private working) repository
this public repo is curated from; a subset is rewritten into the chapters of this book as it
stabilizes, rather than published wholesale. If you're contributing a change whose "why" isn't
already captured in this book, a short design note alongside your PR is the right place for it.

## License

Sutra is dual-licensed under **MIT OR Apache-2.0**
([LICENSE-MIT](https://github.com/startr-trade/sutra/blob/main/LICENSE-MIT),
[LICENSE-APACHE](https://github.com/startr-trade/sutra/blob/main/LICENSE-APACHE)). Unless you
state otherwise, any contribution you submit is licensed under the same terms, with no additional
conditions — you retain copyright over your own contributions.

Please also read our
[Code of Conduct](https://github.com/startr-trade/sutra/blob/main/CODE_OF_CONDUCT.md) and
[Security Policy](https://github.com/startr-trade/sutra/blob/main/SECURITY.md).

## Next

- **[Debugging the engine](debugging-the-engine.md)** — tracing targets, reproducing a bug at the
  right test tier, and the development-only tools (like the DMN-TCK harness) that live alongside
  the workspace.
