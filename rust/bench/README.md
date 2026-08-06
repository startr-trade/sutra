# rust/bench — GA-readiness performance harness

Scripts (not a crate) that measure the four headline GA-readiness numbers.
Everything here is plain shell + one criterion suite; nothing links into the
workspace, so `rust/bench/` is deliberately **not** a Cargo workspace member.

| Metric | Script | Tool | Measurable in a worktree? |
|---|---|---|---|
| Cold start p50 / p95 (30 runs) | `cold-start.sh` | `hyperfine` | Needs the engine image + a PG container |
| Peak RSS at ready | `peak-rss.sh` | `docker stats` | Needs the engine image + a PG container |
| Peak RSS **per engine lane** | `lane-rss.sh` | `/proc/<pid>/status` (`VmHWM`) | **Yes** — no docker (persistence-less boot); needs a release build |
| Sustained RPS + p95 | `sustained-rps.sh` | `k6` (reuses `tools/sutra-load-test/` profiles) | Needs the engine image under load |
| FEEL parse / eval / paths | `feel-micro.sh` → `sutra-feel/benches/feel_benches.rs` | `criterion` | **Yes** — pure CPU |

## Prerequisites

```bash
# Rust bench tools (once):
cargo install hyperfine --locked          # cold-start.sh
# cargo-audit / cargo-deny are for `make audit`, not the bench harness.

# k6 (Linux) — see tools/sutra-load-test/README.md for the apt recipe.
# docker + the engine image. IMPORTANT: bench the SHIPPED profile. The Dockerfile defaults to
# CARGO_PROFILE=release (thin-LTO + one codegen unit — the smaller, faster GA binary). Do NOT
# bench a `make image-it` build (CARGO_PROFILE=release-it: no LTO, parallel codegen → a few MB
# larger + marginally slower); that image exists only to speed up k8s-IT rebuilds. Note the
# on-host `sutra-rust-engine:dev` tag is the release-it IT image, so build + point the bench at
# an explicit release tag:
docker build -f rust/Dockerfile -t sutra-rust-engine:release rust/   # default = release
export SUTRA_ENGINE_IMAGE=sutra-rust-engine:release
# the sutra CLI, to package the deployment archive:
(cd rust && cargo build --release -p sutra-cli)
```

### No `hyperfine` / can't build `sutra-cli`? (worktree-constrained hosts)

`cold-start.sh` falls back to a plain bash timing loop over the same `_cold-start-run.sh`
unit when `hyperfine` isn't on `PATH` (writes `results/cold-start-fallback.txt` +
`cold-start-raw.txt` instead of `cold-start.{json,md}`) — install hyperfine on a controlled
bench host for the canonical numbers; the fallback exists so the harness stays runnable
where `hyperfine` can't be installed (no sudo / no network).

`package_archives()` (`lib.sh`) needs the `sutra` CLI, which force-links every builtin
codec for codec-URN completeness, even though most example deployments never reference
them. Where that is unwanted — a distribution whose CLI links a large generated binding
crate, or a shared-target-dir worktree wave where rebuilding it is off-limits — build
`sutra-bench-packager` instead: it calls the exact same `sutra_loader::assemble_dir`
`sutra package` calls, byte-for-byte, but its own dependency graph never touches the
codec crates:

```bash
(cd rust && cargo build -p sutra-bench-packager --release)
export SUTRA_CLI="$PWD/rust/target/release/sutra-bench-packager"   # instead of the sutra-cli build above
```

## Running

```bash
# FEEL micro-benches (no docker; runnable now):
rust/bench/feel-micro.sh
#   → rust/target/criterion/**/estimates.json

# Per-lane memory slope (no docker; needs `cargo build --release -p sutra-dist -p sutra-cli`).
# Boots the SAME archive set at each lane count and reads the kernel's peak-RSS watermark, so the
# difference between two runs is the per-lane engine build and nothing else — the measurement
# behind execution-scale-out §7.3:
DEPLOYMENTS=24 SHARDS="1 2 4 8 16" rust/bench/lane-rss.sh   # → results/lane-rss.txt

# Container benches (need the image + docker; run on an otherwise-idle host). All three default
# to the HTTP-only approval-hold example — no broker, no second datastore, one PG for the engine
# core. sustained-rps.sh drives its stateless `showcase-request` channel (Prep → gateway →
# template → reply, 200 synchronously, no per-request state → no alias conflict under load):
RUNS=30 rust/bench/cold-start.sh      # → results/cold-start.{json,md}
rust/bench/peak-rss.sh                # → results/peak-rss.txt
PROFILE=sustained rust/bench/sustained-rps.sh   # → results/sustained-rps.json

# A short proof / smoke window (the committed default is 100 RPS × 5 min):
RATE=100 DURATION=20s rust/bench/sustained-rps.sh

# Point sustained-rps.sh at a DIFFERENT deployment's channel via the four override envs — e.g.
# the money-transfer example's read-only `balance` channel (note: money-transfer additionally
# needs a RabbitMQ broker + a second `accounts` datastore, so set BENCH_EXAMPLE_DIR and stand
# those up first — see examples/money-transfer):
BENCH_EXAMPLE_DIR="$PWD/examples/money-transfer/deployments-src/default--money-transfer--1.0.0" \
CHANNEL_URL=/channels/balance CONTENT_TYPE=application/json API_KEY=transfer-demo-key \
PAYLOAD="$PWD/tools/sutra-load-test/fixtures/money-transfer-balance-query.json" \
  rust/bench/sustained-rps.sh
```

`results/` is git-ignored (host-specific). Every container the harness starts is labelled
`sutra-bench-<pid>` and reaped by each script's `EXIT` trap; a killed run leaves at most one
labelled PostgreSQL/engine pair — `docker ps -a --filter label=sutra-bench-<pid>` then
`docker rm -f`.

## Methodology

- **Cold start** = wall time from `docker run` to `/sutra/health/ready` returning 200, for the
  default HTTP-only (approval-hold) deployment against a live PostgreSQL. "Ready" therefore
  includes datasource connect + migration apply + archive activation — the same definition the
  reference baseline used
  (process start → health-ready). PostgreSQL is started once and is **not** in the timed
  window; the engine container is started fresh and removed per run (`--conclude`) so each of
  the 30 runs is a genuine cold boot. The poll granularity (20 ms) is the measurement floor.
  hyperfine reports mean/min/max and the median (≈ p50); p95 is computed from the per-run
  times in `cold-start.json`.
- **Peak RSS** = the `MEM USAGE` column of `docker stats --no-stream` once the engine is
  ready but before it serves traffic (the idle-ready working set). For a loaded peak, sample
  again during `sustained-rps.sh`.
- **Per-lane RSS** = `VmHWM` (the kernel's peak-RSS watermark) from `/proc/<pid>/status`, read
  once the engine reports ready and before it serves anything. The engine runs **without a
  datasource** on purpose: no pool, no background loops, so the difference between two runs of
  the same archive set at different `sutra.engine.shards` is the per-lane engine build alone.
  The reported number is the SLOPE (kB per extra lane), not the absolute — absolutes are
  host- and allocator-specific, the slope is the thing the design record cares about.
- **Sustained RPS + p95** = k6's `http_reqs` rate and `http_req_duration` p95 over the chosen
  profile, driving the engine container directly (the SUT is the engine container itself). The
  profiles are the neutral k6 JS in `tools/sutra-load-test/`.
- **FEEL micro-benches** = criterion, 100 samples/benchmark with 3 s warmup, over
  payment-shaped expressions (parse, eval, eval-as-boolean = the gateway-condition path, and
  path extraction = the payload-navigation path). Run on an otherwise-idle host: concurrent
  compiles or container builds inflate the numbers and widen the confidence intervals.

## Comparability to the reference baseline

The reference baseline (69 MB image / 75 ms cold start / 53 MB RSS / 9.97 RPS smoke) was
recorded on the retired reference engine. The numbers are **directional**, not a controlled
A/B: the "ready" definition matches, but the host, the image contents, the load profile,
**and the workload** differ — the reference sustained figure drove the money-transfer flow,
whereas the Rust sustained
figure drives approval-hold's stateless `showcase-request` (an in-memory Prep → template → reply
with no per-request DB round-trip, so it measures the HTTP + codec + template path, not the
persistence path). Read the sustained comparison as an order-of-magnitude sanity check, not a
like-for-like delta.
