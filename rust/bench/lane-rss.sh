#!/usr/bin/env bash
# Peak RSS of an engine boot as a function of the LANE COUNT (`sutra.engine.shards`).
#
# The question this answers (execution scale-out §2 row 10): how much memory does one extra
# engine lane cost? Each lane used to build its OWN copy of the read-only registries
# (processes, codecs, validators, templates, decisions, redactors, outbound channels), so the
# cost per lane grew with the size of the deployed archive set. This script measures the
# slope: boot the SAME archive set at N=1 and N=8 and read the kernel's peak-RSS watermark
# (`VmHWM` from /proc/<pid>/status) once the engine reports ready.
#
# Metric: VmHWM (peak resident set since exec) after /sutra/health/ready is 200, no traffic
# served. Persistence-less on purpose — no pool, no background loop noise, so the delta
# between two runs is the per-lane engine build and nothing else.
#
# Prereqs: a release build of the engine + the CLI:
#   cargo build --release -p sutra-dist -p sutra-cli
# Env:
#   DEPLOYMENTS   how many copies of the sample package to seal in (default 24)
#   SHARDS        the lane counts to measure (default "1 8")
#   PACKAGE_DIR   the package source to replicate (default examples/money-transfer)
# Output: results/lane-rss.txt under rust/bench/.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$BENCH_DIR/../.." && pwd)"
# Honour a configured target-dir (`CARGO_TARGET_DIR`, or a `[build] target-dir` in any cargo
# config) rather than assuming the in-tree default — parallel worktrees share one.
TARGET_DIR="${CARGO_TARGET_DIR:-$(cd "$REPO_ROOT/rust" && cargo metadata --format-version 1 --no-deps 2>/dev/null \
  | tr ',' '\n' | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p' | head -1)}"
TARGET_DIR="${TARGET_DIR:-$REPO_ROOT/rust/target}"
ENGINE_BIN="${ENGINE_BIN:-$TARGET_DIR/release/sutra-engine}"
SUTRA_BIN="${SUTRA_BIN:-$TARGET_DIR/release/sutra}"
OUT="$BENCH_DIR/results"
DEPLOYMENTS="${DEPLOYMENTS:-24}"
SHARDS="${SHARDS:-1 8}"
PACKAGE_DIR="${PACKAGE_DIR:-$REPO_ROOT/examples/approval-hold/deployments-src/default--approval--1.0.0}"

mkdir -p "$OUT"
[ -x "$ENGINE_BIN" ] || { echo "missing $ENGINE_BIN — cargo build --release -p sutra-dist" >&2; exit 1; }
[ -x "$SUTRA_BIN" ] || { echo "missing $SUTRA_BIN — cargo build --release -p sutra-cli" >&2; exit 1; }

WORK="$(mktemp -d /tmp/lane-rss.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
ARCHIVES="$WORK/archives"
mkdir -p "$ARCHIVES"

# One package per synthetic tenant, so the engine activates DEPLOYMENTS distinct deployments
# (distinct module keys — the realistic multi-tenant working set, not one archive N times).
for i in $(seq 1 "$DEPLOYMENTS"); do
  src="$WORK/pkg-$i"
  cp -r "$PACKAGE_DIR" "$src"
  sed -i "s/\"tenant\": \".*\"/\"tenant\": \"t$i\"/" "$src/package.yaml"
  # Distinct HTTP bind paths per copy — two deployments may not claim one route.
  sed -i "s#POST /channels/#POST /channels/t$i-#" "$src/channels.yaml"
  "$SUTRA_BIN" package "$src" --out "$ARCHIVES" >/dev/null
done
echo "sealed $DEPLOYMENTS archives into $ARCHIVES" >&2

measure() {
  local shards="$1"
  local log="$WORK/engine-$shards.log"
  SUTRA_DEPLOYMENTS_DIR="$ARCHIVES" \
  SUTRA_HTTP_PORT=0 \
  SUTRA_ENGINE_SHARDS="$shards" \
    "$ENGINE_BIN" >"$log" 2>&1 &
  local pid=$!
  local port=""
  for _ in $(seq 1 200); do
    port="$(grep -oE '"port":[0-9]+' "$log" | head -1 | cut -d: -f2 || true)"
    [ -n "$port" ] && curl -sf -o /dev/null "http://127.0.0.1:$port/sutra/health/ready" && break
    sleep 0.2
    kill -0 "$pid" 2>/dev/null || { echo "engine died at shards=$shards; see $log" >&2; cat "$log" >&2; exit 1; }
  done
  # Settle: let the activation finish and the allocator quiesce before the watermark read.
  sleep 2
  local hwm rss
  hwm="$(awk '/VmHWM/ {print $2}' "/proc/$pid/status")"
  rss="$(awk '/VmRSS/ {print $2}' "/proc/$pid/status")"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  printf 'shards=%-3s VmHWM=%8s kB  VmRSS=%8s kB\n' "$shards" "$hwm" "$rss"
}

{
  echo "# lane-rss — deployments=$DEPLOYMENTS package=$(basename "$PACKAGE_DIR") $(date -Is)"
  for n in $SHARDS; do measure "$n"; done
} | tee "$OUT/lane-rss.txt"
