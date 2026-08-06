# Debugging the engine

This page is for contributors working on the engine itself — its executor, model, channel
dispatch, or the FEEL/DMN evaluators. If you're building a *solution* on top of Sutra and
something in your BPMN/rules/config isn't behaving, see
[Troubleshooting BPMN solutions](operating/troubleshooting.md) instead — that page (and its
CLI-based tools) is the one written for that audience.

## Tracing targets

Every crate logs through `tracing`, and its module path is the target `RUST_LOG` filters on — so
you can raise verbosity on exactly the layer you're chasing a bug through without drowning in
noise from the rest of the workspace:

```bash
RUST_LOG=sutra_channels=debug,sutra_engine::deploy=trace cargo test -p sutra-channels
RUST_LOG=sutra_executor=trace cargo run -p sutra-dist
```

The default filter is `info`. When you're not sure which crate owns the behavior you're chasing,
start from [Engine layering](architecture/overview.md) — a message's path through the system
(channel → dispatch → executor → persistence) is also the order to add targets in.

## Reproduce at the lowest tier that can show the bug

The workspace's three test tiers (`rust/TESTING.md`; summarized in
[Contributing](contributing.md#test-tiers)) aren't just a CI cost control — they're the natural
escalation order for reproducing a bug:

1. **Tier 1 first.** Most bugs in the executor, the BPMN model, FEEL/DMN/`.srl`, or template
   rendering reproduce with a plain unit or no-docker integration test — no Postgres, no broker,
   nothing to spin up. Add it as a new module in the crate's `tests/all.rs` (a new file under
   `tests/all/` plus one `mod <name>;` line — not a new top-level `tests/*.rs`, which would
   reintroduce a separate link unit) and run it in isolation:
   ```bash
   cargo test -p sutra-executor gateways_test::your_new_case
   ```
2. **Escalate to tier 2 only if the bug needs real infrastructure** — a specific PostgreSQL
   locking behavior, a RabbitMQ redelivery edge case, a replica-convergence race. Mark the test
   `#[ignore = "docker"]` and call `sutra_testkit::reap_on_exit(container.id())` on whatever
   container handle you spin up, so the shared-fixture pattern doesn't leak it (see
   `rust/TESTING.md`'s "Reaper" section for why that call is required rather than relying on
   `Drop`).
3. **Escalate to tier 3 only if the bug is genuinely multi-replica or Kubernetes-specific** —
   leader-election handoff, ConfigMap-driven activation timing, ingress-level deploy behavior. This
   tier needs a provisioned `kind` cluster (`make -C deploy/k8s-it init`) and is the most
   expensive to iterate on — reach for it
   last, and only once you've ruled out that a tier-1 or tier-2 test could show the same thing.

A bug fixed without a new failing test at the tier that caught it isn't really pinned — the next
regression in the same area won't be caught until it reaches production again. Write the test
first, watch it fail against the unfixed code, then fix it.

## Catalog-guided navigation

```bash
make catalog          # regenerate catalog/ (git-ignored, generated on demand)
```

`sutra-catalog-gen` walks every crate and emits one markdown page per source file, plus a
bidirectional relationships table — what a file depends on, and what depends on it. When you're
about to change a shared type (an SPI trait, a diagnostic code, a config key) and need to know
every call site that will need updating, this is faster and more complete than a plain grep: it's
generated from the actual `syn`-parsed dependency graph, not a text search that can miss a
re-export or an indirect reference.

## The DMN-TCK harness — a development tool, not a user-facing command

The [DMN-TCK conformance](reference/dmn-tck.md) numbers this book cites come from a harness that
ships in the workspace but is **gated and external**: the OMG TCK corpus itself is
ASL-2.0-licensed and isn't vendored into this (unlicensed) repository, so it's never something a
person building a BPMN solution runs. If you're working on `sutra-feel` or `sutra-dmn`:

```bash
git clone https://github.com/dmn-tck/tck.git   # or wherever your checkout lives
SUTRA_DMN_TCK_DIR=/path/to/tck cargo test -p sutra-dmn --test tck -- --ignored --nocapture
```

The harness classifies every assertion as PASS (matches expected), FAIL (a real conformance gap —
the engine produced a different value), or UNSUPPORTED (the engine couldn't evaluate the
construct at all — recorded, not counted as a failure). Two env-gated side outputs help when
you're closing a gap: `SUTRA_DMN_TCK_DUMP=<file>` writes every non-pass outcome for gap analysis,
and `SUTRA_DMN_TCK_RESULTS_DIR=<dir>` writes the official vendor-submission
`tck_results.csv`/`tck_results.properties` pair. When you land an increment, re-run the harness and
update the numbers in [DMN-TCK conformance](reference/dmn-tck.md) alongside your change — that
page is a measurement, and a stale measurement is worse than none.

## Next

- **[Domain neutrality and the SPI model](architecture/neutrality-and-spi.md)** — the structural
  rule most contributions have to respect, and the gate that enforces it.
- **[Troubleshooting BPMN solutions](operating/troubleshooting.md)** — the solution-developer
  counterpart to this page.
