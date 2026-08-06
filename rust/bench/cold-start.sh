#!/usr/bin/env bash
# Cold-start p50/p95 over N runs (default 30) via hyperfine.
#
# Metric: wall time from `docker run` to /sutra/health/ready 200, for the default HTTP-only
# (approval-hold) deployment against a live PostgreSQL — the same "ready" definition the
# reference baseline used (process
# start → health-ready). PostgreSQL is started once (shared across runs; its start is NOT in
# the timed window); the engine container is started fresh and torn down per run so each run
# is a genuine cold boot.
#
# Prereqs: docker, hyperfine, curl, the engine image ($SUTRA_ENGINE_IMAGE), and the `sutra`
# CLI (to package the archive). See rust/bench/README.md.
#
# Output: results/cold-start.{json,md} under rust/bench/.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

RUNS="${RUNS:-30}"
OUT="${BENCH_DIR}/results"
mkdir -p "$OUT"

require docker curl
trap bench_cleanup EXIT

log "starting PostgreSQL ($PG_IMAGE)"
read -r PG_CID PG_PORT < <(start_pg)
export_pg_datasource "$PG_PORT"
log "packaging $(basename "$BENCH_EXAMPLE_DIR") archive"
export ARCHIVES; ARCHIVES="$(package_archives)"
export BENCH_TAG SUTRA_ENGINE_IMAGE
# One free port for the whole run: --network host binds it directly (no docker-port
# indirection), and each of the N runs tears its container down before the next starts, so
# reusing one port across all of them is safe — see find_free_port's doc (never hardcode 8080,
# it may be in use by something else on a shared host).
export SUTRA_HTTP_PORT; SUTRA_HTTP_PORT="$(find_free_port)"

if command -v hyperfine >/dev/null 2>&1; then
  log "hyperfine cold-start x${RUNS} (this measures docker-run -> /sutra/health/ready 200)"
  hyperfine \
    --runs "$RUNS" \
    --warmup 1 \
    --prepare "docker rm -f ${BENCH_TAG}-engine >/dev/null 2>&1 || true" \
    --conclude "docker rm -f ${BENCH_TAG}-engine >/dev/null 2>&1 || true" \
    --export-json "$OUT/cold-start.json" \
    --export-markdown "$OUT/cold-start.md" \
    --command-name "cold-start (docker run -> /sutra/health/ready 200)" \
    "bash '${BENCH_DIR}/_cold-start-run.sh'"
  log "done — see $OUT/cold-start.md (mean/min/max; p50≈median, p95 from cold-start.json)"
else
  # Fallback for a host without hyperfine (e.g. no sudo + no network to install it — see
  # rust/bench/README.md). Same unit (_cold-start-run.sh: fresh `docker run` -> poll
  # /sutra/health/ready until 200), timed with `date` instead of hyperfine. Less precise
  # (shell-loop overhead is not subtracted, unlike hyperfine's own warm-up/statistics
  # machinery) — install hyperfine on a controlled bench host for the canonical numbers.
  log "hyperfine not found (and not installable here — no sudo, no network reachable);"
  log "falling back to a bash timing loop over the same _cold-start-run.sh unit x${RUNS}"
  RAW="$OUT/cold-start-raw.txt"
  : > "$RAW"
  for i in $(seq 1 "$RUNS"); do
    docker rm -f "${BENCH_TAG}-engine" >/dev/null 2>&1 || true
    t0=$(date +%s%N)
    bash "${BENCH_DIR}/_cold-start-run.sh"
    t1=$(date +%s%N)
    docker rm -f "${BENCH_TAG}-engine" >/dev/null 2>&1 || true
    awk -v a="$t0" -v b="$t1" 'BEGIN { printf "%.3f\n", (b - a) / 1e6 }' >> "$RAW"
  done
  sort -n "$RAW" > "$OUT/cold-start-sorted.txt"
  awk '
    { a[NR] = $1; sum += $1 }
    END {
      n = NR
      mean = sum / n
      p50 = a[int((n - 1) * 0.50) + 1]
      p95 = a[int((n - 1) * 0.95) + 1]
      printf "runs=%d mean_ms=%.3f min_ms=%.3f max_ms=%.3f p50_ms=%.3f p95_ms=%.3f\n", \
        n, mean, a[1], a[n], p50, p95
    }' "$OUT/cold-start-sorted.txt" | tee "$OUT/cold-start-fallback.txt"
  log "done (bash fallback) — see $OUT/cold-start-fallback.txt (+ cold-start-raw.txt per-run ms)"
fi
