# Contributing to Sutra

Thanks for your interest in Sutra! This document explains how to build the project, run the
tests, and submit changes.

By participating, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report bugs** and **request features** via GitHub Issues — please search first.
- **Improve the docs** (the [mdBook](docs/) under `docs/`).
- **Submit code** — bug fixes, new codecs / transports, examples.

For anything large (a new transport, a codec, an architectural change), please **open an issue
to discuss it first** so we can agree on the approach before you invest time.

## Development environment

Sutra is a Cargo workspace under [`rust/`](rust/). You need a stable Rust toolchain
([rustup](https://rustup.rs)); Docker is only needed for the container / integration test tiers.

```bash
# From the repository root:
make test     # tier-1: the no-Docker Rust suite
make lint     # clippy (-D warnings) + formatting + the domain-neutrality gate
```

`make help` lists every target. The test tiers (no-Docker / Docker / k8s) are documented in
[`rust/TESTING.md`](rust/TESTING.md).

## Submitting a pull request

1. **Fork** the repo and create a topic branch off `main`.
2. Keep the PR **focused** — one logical change. Small PRs get reviewed faster.
3. Before pushing, make sure it's green locally:
   ```bash
   cd rust
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test  --workspace
   ```
4. Add a **`CHANGELOG.md`** entry under `[Unreleased]` when your change is user-visible.
5. Open the PR with a clear description of **what** changed and **why**. Link the issue it
   addresses.

CI runs `fmt`, `clippy -D warnings`, the test suite, and `cargo deny` on every PR — the same
checks as step 3.

## Commit messages

Use short, imperative summaries with an area prefix, e.g.:

```
feat(channels): add AMQP 1.0 outbound reply path
fix(feel): correct DECIMAL64 rounding on division
docs(concepts): clarify wait-state correlation keys
```

## License of contributions

Sutra is dual-licensed under **MIT OR Apache-2.0**. Unless you state otherwise, any
contribution you submit is licensed under the same terms, with no additional conditions. You
retain the copyright to your contributions.
