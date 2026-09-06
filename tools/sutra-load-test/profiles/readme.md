# Saturation-bench profiles (P1-6 Phase 0)

Five k6 profiles for `` §7 (the saturation-benchmark plan
for keyed actor sharding), plus their dedicated fixture package and driver script. This page
is the operator's guide: what each profile measures, what it needs, and — most importantly —
the honesty rule that governs citing any number they produce.

## The honesty rule (verbatim from the design doc)

> No saturation number exists today; the only prior figure is one uncommitted 100-RPS latency
> run. **The honesty rule stands: no throughput or saturation claim is made — in docs,
> marketing, or comparison collateral — until the harness below runs on committed profiles,
> and the uncommitted run is never cited.**

Concretely: this page's authoring pass (Phase 0) commits the profiles and fixtures but **does
not run them**. Nothing here is a throughput claim. The first legitimate number is the Phase 0
baseline run (below), and even that is only ever compared against later N-shard runs on the
same host — never presented as a portable or absolute figure (same caveat
`rust/bench/README.md` states for every other profile in this directory's family: compare
across runs on the same host, never across hosts).

## The five profiles

| Profile | File | What it isolates (design §7) |
|---|---|---|
| `burst-start` | `burst-start.js` | The saturation knee; pure shard parallelism, no claims |
| `steady-park-resume` | `steady-park-resume.js` | Step-commit cost, per-instance serialization overhead, handoff rate |
| `timer-storm` | `timer-storm.js` | Poller fan-out, shard convoying, claim-defer churn |
| `correlation-heavy` | `correlation-heavy.js` | Handoff path, hot-shard skew, claim-bounce behavior |
| `mixed` | `mixed.js` | Weighted blend — the number a comparison page may eventually quote |

Every profile's own file header has the full design rationale, config-block knobs, and (where
relevant) the specific caveats for reading its output — read the `.js` file itself before a
run, not just this table.

## Fixture prerequisites

All five profiles drive one dedicated, domain-neutral fixture package:
**`tools/sutra-load-test/fixtures/saturation/`** — three BPMN processes, four HTTP channels,
no schemas, no templates (every request that has no `<q:reply>` node gets a 202 Accepted once
its step commits — see `rust/crates/sutra-channels/src/http.rs`). It is a standalone package
built for this bench only, not `examples/approval-hold` or `examples/money-transfer` (those
stay as-is for `sustained-rps.sh` / `burst.js` / `sustained.js` / `smoke.js`).

| Channel | Process | Used by |
|---|---|---|
| `work-in` | `run-to-end.bpmn` (spawn, one FEEL assignment, done) | `burst-start` |
| `spawn-in` | `hold-relay.bpmn` (spawn + park, keyed by `payload.key`) | `steady-park-resume`, `correlation-heavy`, `mixed` |
| `relay-in` | `hold-relay.bpmn`'s correlated relay | `steady-park-resume`, `correlation-heavy`, `mixed` |
| `timer-in` | `timer-storm.bpmn` (spawn, park on a fixed PT2S timer, done) | `timer-storm`, `mixed` |

Every BPMN file's own header comment cites the already-proven fixture it was structurally
modeled on (`examples/approval-hold`'s `approval-hold.bpmn` / `template-showcase.bpmn`, and
the engine's own conformance package's `timer-park.bpmn`) — read those comments if you need to
extend the package.

**Packaging**: `rust/bench/saturation-matrix.sh` packages and deploys this fixture
automatically in its default (self-start) mode, exactly like `sustained-rps.sh` does for
approval-hold — see `rust/bench/lib.sh`'s `package_archives()`. To package it by hand (e.g. to
deploy it onto an already-running engine for `ENGINE_URL=` / external-mode runs):

```bash
# Full-fidelity path (needs a cargo build — do this on a host where that's not off-limits):
(cd rust && cargo build --release -p sutra-cli)
rust/target/release/sutra package tools/sutra-load-test/fixtures/saturation --out /tmp/saturation-archives

# Worktree-constrained path (never force-links the codec set — see rust/bench/README.md):
(cd rust && cargo build -p sutra-bench-packager --release)
rust/target/release/sutra-bench-packager package tools/sutra-load-test/fixtures/saturation --out /tmp/saturation-archives
```

Then point your engine's `SUTRA_DEPLOYMENTS_DIR` at `/tmp/saturation-archives` (or copy the
archive into wherever it already scans) before running any profile in `ENGINE_URL=` mode.

## Running a profile

### Via the driver script (recommended — writes structured results)

```bash
PROFILE=burst-start rust/bench/saturation-matrix.sh
PROFILE=steady-park-resume RATE=100 DURATION=5m rust/bench/saturation-matrix.sh
PROFILE=timer-storm K=2000 rust/bench/saturation-matrix.sh
PROFILE=correlation-heavy POOL_SIZE=20 RATE=200 rust/bench/saturation-matrix.sh
PROFILE=mixed TOTAL_RATE=200 rust/bench/saturation-matrix.sh
```

Each run writes `rust/bench/results/<label>/k6-summary.json` (k6's raw `--summary-export`)
and `rust/bench/results/<label>/summary.json` (a structured summary — see "Measurement
discipline" below). Read `rust/bench/saturation-matrix.sh`'s own header for the full env-var
reference (target modes, `SHARDS` labeling, `ENGINE_URL` external mode, `DRAIN_TIMEOUT`).

### Directly with k6 (proof runs, iterating on a profile)

```bash
BASE_URL=http://127.0.0.1:<port> API_KEY=saturation-bench-key \
  k6 run tools/sutra-load-test/profiles/burst-start.js
```

Direct runs skip the driver script's structured `summary.json` and (for `timer-storm`) the
post-run drain poll — use the driver script for anything you intend to record.

## The N ∈ {1, 2, 4, 8} matrix procedure

The design's target matrix (§7): **each profile run at N ∈ {1, 2, 4, 8} shards, on fixed
hardware, DB pinned.** Today (Phase 0) `shard-count` is always 1 — there is no other value to
run. The procedure below is what Phase 2 onward actually executes; Phase 0's job is only the
N=1 baseline row.

1. Pin hardware: run every N on the same load-generator host and the same engine host,
   back-to-back, without other load on either (matches `rust/bench/README.md`'s methodology
   note for every profile in this family).
2. Pin the DB: same `PG_IMAGE`, same connection-pool sizing, across every N in one matrix pass.
3. For each `N` in `1 2 4 8`: set the engine's shard count (`sutra.engine.shards` /
   `SUTRA_ENGINE_SHARDS` — lands in Phase 2, not before) to `N`, then run all five profiles:
   `SHARDS=$N PROFILE=<profile> rust/bench/saturation-matrix.sh` for each.
4. Compare `rust/bench/results/<profile>-n<N>-*/summary.json` across `N` for the SAME profile:
   achieved throughput at the knee (`burst-start`), p50/p95/p99 (all profiles), handoff/
   claim-bounce rates once Phase 2 ships those metrics (`correlation-heavy`,
   `steady-park-resume`), and drain time (`timer-storm`).
5. Never compare across profiles (they measure different things) or across hosts (see the
   honesty rule and `rust/bench/README.md`'s comparability note).

## The baseline protocol

**Phase 0 = today's engine (pre-sharding, `shard-count` fixed at 1 — the only value that
exists) at N=1, recorded before any sharding code lands.** This is the regression floor every
later phase is checked against (design §8: "Phase 1... bench regression vs Phase 0"; "Phase 2
Tests:... bench-regression gate").

Exact commands for the integrator to run at the quiet checkpoint (machine idle, nothing else
building/testing):

```bash
# One run per profile, self-start mode, default knobs (documented in each .js file's header).
# Each writes rust/bench/results/<profile>-n1-<timestamp>/{k6-summary,summary}.json.
SHARDS=1 PROFILE=burst-start          rust/bench/saturation-matrix.sh
SHARDS=1 PROFILE=steady-park-resume   rust/bench/saturation-matrix.sh
SHARDS=1 PROFILE=timer-storm          rust/bench/saturation-matrix.sh
SHARDS=1 PROFILE=correlation-heavy    rust/bench/saturation-matrix.sh
SHARDS=1 PROFILE=mixed                rust/bench/saturation-matrix.sh
```

Prerequisites: docker, curl, k6, jq, the engine image (`SUTRA_ENGINE_IMAGE`, default
`sutra-rust-engine:dev` — see `rust/bench/README.md`'s note on benching the shipped
`release` profile vs the `release-it` dev tag), and the `sutra` CLI or `sutra-bench-packager`
(see "Fixture prerequisites" above) — the driver script itself never builds anything.

After this run, `` §8's Phase 0 gate ("recorded results in
`rust/bench/results/`; zero code change") is closed, and the five `summary.json` files are the
first legitimate numbers this repository can cite for this design.

## Measurement discipline (design §7)

Every `summary.json` the driver script writes has:

- **Achieved throughput** (`achieved.rps`) and **p50/p95/p99** (`achieved.latencyMs`) — from
  k6's own `--summary-export`, always populated.
- **`timerStormDrain`** (`timer-storm` runs only) — fire-to-complete drain, measured by
  polling `GET /sutra/instances?status=WAITING` after k6's arm phase exits. Read the driver
  script's own comment above that block for the precise caveat (it measures
  arm-phase-end-to-drained, a fair proxy for due-at-to-drained because every armed instance
  shares one fixed timer duration — not identical to it).
- **`plannedMetrics`** — `perShardQueueDepth`, `handoffCount`, `claimBounceCount`: named,
  `null`, and commented. These are the design's §6.1 observability surface
  (`sutra.engine.shard.queue-depth{shard}`, a handoff counter, a claim-bounce counter split by
  relay/timer) — **they ship with Phase 2**, not before; there is nothing to scrape at
  shard-count=1. `dbPoolWaitMs` — also `null`; the design names DB pool wait a first-class
  bench metric starting at the Phase 3 pool-exhaustion soak (§8), not Phase 0. A rough,
  unautomated proxy today is sampling `pg_stat_activity` on the bench PG container mid-run.
  The field names are fixed now so the result shape does not change again when the real
  values arrive — a later run's `summary.json` is diff-compatible with a Phase 0 one.
- **`timer-storm.bpmn`'s own header** also documents the poller's batch/tick ceiling (32
  claims / 500 ms tick / deployment, `rust/crates/sutra-engine/src/timer.rs`) so a drain-rate
  plateau near there is read as the poller's ceiling, not engine saturation — the same caution
  the design's §7 table states explicitly for this profile.

## Conventions this directory follows

- Every URL is env-var-driven (`BASE_URL`, `ENGINE_URL`); nothing hardcodes port 8080 (that
  port is reserved for a local k8s server on shared dev hosts — see `rust/bench/lib.sh`'s
  `find_free_port` comment).
- Results land under `rust/bench/results/` (git-ignored), same as every other profile in the
  `rust/bench/` family — never under `tools/sutra-load-test/results/` (that directory is for
  `run.sh`'s own three profiles, a separate, older harness surface).
- All fixture vocabulary is domain-neutral (`work`, `spawn`, `hold`, `relay`, `timer` — no
  business nouns) — this package exists only to drive load, never to model a real deployment.
