#!/usr/bin/env bash
# Saturation-bench driver (P1-6 Phase 0). Runs ONE
# k6 profile from tools/sutra-load-test/profiles/{burst-start,steady-park-resume,timer-storm,
# correlation-heavy,mixed}.js against a chosen engine endpoint and writes a structured JSON
# summary per run under rust/bench/results/<label>/ — same layering as sustained-rps.sh (this
# script owns bringing up the SUT or checking one is reachable; it never touches engine
# crates).
#
# Two target modes:
#   self-start (default) — exactly sustained-rps.sh's own shape: start_pg + package_archives +
#     start_engine + wait_ready from lib.sh. Default BENCH_EXAMPLE_DIR is the neutral
#     saturation-bench fixture package (tools/sutra-load-test/fixtures/saturation) — a
#     dedicated package for these profiles, not approval-hold/money-transfer.
#   external (ENGINE_URL=... set) — points at an ALREADY-RUNNING engine instead of starting
#     one (mirrors tools/sutra-load-test/run.sh's --target=external). This script REFUSES to
#     run if that target's /sutra/health/ready is not reachable — see require_reachable below.
#     Nothing is auto-started in this mode; package + deploy the saturation fixture onto that
#     engine yourself first (see tools/sutra-load-test/profiles/readme.md).
#
# N-shards (SHARDS): a REAL knob since P1-6 Phase 2 (`sutra.engine.shards` /
# `SUTRA_ENGINE_SHARDS`, default 1). In self-start mode this script exports
# SUTRA_ENGINE_SHARDS=$SHARDS so lib.sh's start_engine boots a genuinely-N-lane SUT; in
# external mode the operator must have deployed the target with the shard count the label
# claims (the script cannot verify it — say what you ran).
#
# Usage:
#   PROFILE=burst-start rust/bench/saturation-matrix.sh
#   PROFILE=steady-park-resume RATE=100 DURATION=5m rust/bench/saturation-matrix.sh
#   PROFILE=timer-storm K=2000 rust/bench/saturation-matrix.sh
#   PROFILE=correlation-heavy POOL_SIZE=20 RATE=200 rust/bench/saturation-matrix.sh
#   PROFILE=mixed TOTAL_RATE=200 rust/bench/saturation-matrix.sh
#   ENGINE_URL=http://127.0.0.1:8081 PROFILE=burst-start rust/bench/saturation-matrix.sh
#
# Env knobs this script reads directly (profile-specific knobs — RATE, DURATION, K, POOL_SIZE,
# STAGES, TOTAL_RATE, ... — are read by the k6 profile itself via __ENV and need no wiring
# here; export them before invoking this script and k6 inherits them):
#   PROFILE       required: burst-start | steady-park-resume | timer-storm |
#                 correlation-heavy | mixed
#   SHARDS        engine lane count for the self-started SUT AND the results label
#                 (default 1) — see the N-shards note above
#   LABEL         results directory name under rust/bench/results/ (default derived:
#                 <profile>-n<shards>-<UTC timestamp>)
#   ENGINE_URL    external-mode target base URL; unset = self-start (default)
#   API_KEY       apikey header value (default: the saturation fixture's own
#                 saturation-bench-key; override if ENGINE_URL hosts a differently-keyed
#                 deployment of the same fixture)
#   DRAIN_TIMEOUT timer-storm only: max seconds to poll for full drain (default 120)
#   BENCH_EXAMPLE_DIR / SUTRA_ENGINE_IMAGE / SUTRA_CLI / PG_IMAGE — as lib.sh (self-start mode
#     only; BENCH_EXAMPLE_DIR defaults to the saturation fixture package here, not
#     approval-hold)
#
# Output: rust/bench/results/<label>/k6-summary.json (k6 --summary-export, raw) and
#         rust/bench/results/<label>/summary.json (this script's structured summary — see the
#         "structured summary" section below for its shape and the Phase-2/3 metric hooks).
set -euo pipefail

# Default fixture is the dedicated, domain-neutral saturation package — NOT approval-hold
# (sustained-rps.sh's default). This MUST be set before lib.sh is sourced: lib.sh fills an
# unset BENCH_EXAMPLE_DIR with the approval-hold example, and a `:-` default applied after
# the source would keep that instead of ours (the exact defect the first baseline run hit —
# every profile ran against the wrong package). Override BENCH_EXAMPLE_DIR to point
# self-start mode at a different package (must expose the same channels).
_SM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_EXAMPLE_DIR="${BENCH_EXAMPLE_DIR:-$(cd "${_SM_DIR}/../.." && pwd)/tools/sutra-load-test/fixtures/saturation}"

. "${_SM_DIR}/lib.sh"

# --- args / knobs ---------------------------------------------------------------------------

PROFILE="${PROFILE:-}"
case "$PROFILE" in
    burst-start|steady-park-resume|timer-storm|correlation-heavy|mixed) ;;
    "") die "PROFILE is required: burst-start | steady-park-resume | timer-storm | correlation-heavy | mixed" ;;
    *) die "unknown PROFILE: $PROFILE (expected burst-start | steady-park-resume | timer-storm | correlation-heavy | mixed)" ;;
esac

SHARDS="${SHARDS:-1}"
# Self-start mode boots the SUT with this lane count (external mode: operator's duty).
export SUTRA_ENGINE_SHARDS="$SHARDS"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
LABEL="${LABEL:-${PROFILE}-n${SHARDS}-${TS}}"
API_KEY="${API_KEY:-saturation-bench-key}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-120}"
PROFILE_JS="${REPO_ROOT}/tools/sutra-load-test/profiles/${PROFILE}.js"
OUT="${BENCH_DIR}/results/${LABEL}"

[ -f "$PROFILE_JS" ] || die "profile not found: $PROFILE_JS"
mkdir -p "$OUT"

require curl k6 jq

# --- reachability -----------------------------------------------------------------------

# require_reachable: the hard rule from the task contract — this script refuses to run
# against an unreachable target, in EITHER mode. Self-start mode gets this for free from
# wait_ready (below); external mode checks it explicitly, up front, before spending any time
# on k6.
require_reachable() {
    local url="$1"
    if ! curl -sf -o /dev/null --max-time 5 "${url%/}/sutra/health/ready"; then
        die "target not reachable: ${url%/}/sutra/health/ready — refusing to run (no hardcoded port is ever assumed; pass a real ENGINE_URL)"
    fi
}

# --- target: external (already-running engine) or self-start -----------------------------

if [ -n "${ENGINE_URL:-}" ]; then
    log "target=external url=${ENGINE_URL}"
    require_reachable "$ENGINE_URL"
    BASE_URL="$ENGINE_URL"
    TARGET_DESC="external @ ${ENGINE_URL}"
else
    require docker
    trap bench_cleanup EXIT
    log "target=self-start (image=${SUTRA_ENGINE_IMAGE})"
    log "starting PostgreSQL ($PG_IMAGE)"
    read -r PG_CID PG_PORT < <(start_pg)
    export_pg_datasource "$PG_PORT"
    log "packaging $(basename "$BENCH_EXAMPLE_DIR") archive"
    ARCHIVES="$(package_archives)"
    log "starting engine ($SUTRA_ENGINE_IMAGE)"
    read -r ENGINE_CID ENGINE_PORT < <(start_engine "$ARCHIVES")
    log "waiting for /sutra/health/ready 200 (+ >=1 active deployment) on :${ENGINE_PORT}"
    wait_ready "$ENGINE_PORT" || die "engine never became ready"
    BASE_URL="http://127.0.0.1:${ENGINE_PORT}"
    TARGET_DESC="self-start (${SUTRA_ENGINE_IMAGE}) @ ${BASE_URL}"
fi

# --- run k6 --------------------------------------------------------------------------------

log "running k6 profile '${PROFILE}' (label=${LABEL}, shards-label=${SHARDS}) against ${BASE_URL}"
K6_SUMMARY="${OUT}/k6-summary.json"
BASE_URL="$BASE_URL" API_KEY="$API_KEY" \
    k6 run --summary-export "$K6_SUMMARY" "$PROFILE_JS"

# --- timer-storm: post-run drain poll -----------------------------------------------------
#
# k6 only measures the ARM phase (see timer-storm.js's header comment). Fire-to-complete
# drain is measured here: poll GET /sutra/instances?status=WAITING (unauthenticated
# internal-ops surface, rust/crates/sutra-engine/src/server.rs) from the moment k6's arm
# phase exits until the count reaches zero (fully drained) or DRAIN_TIMEOUT elapses.
#
# Caveat, stated plainly: this measures "time from arm-phase-END to fully drained", not
# "time from due-at to fully drained" — arming k instances takes nonzero wall time, so the
# due-at instants are spread across the arm phase (each PT2S after its own spawn), not a
# literal single point. Treat the number as an upper-bound proxy for poller-fan-out drain
# time, not an exact due-at-to-complete measurement; it is a fair proxy specifically because
# every instance shares the same PT2S duration, so the spread in due-at times equals the
# spread in arm times.
#
# ?status=WAITING with no ?deployment= scans every live deployment (server.rs default) — if
# BASE_URL serves other deployments too, scope this yourself with ?deployment=<id> (deployment
# ids are content-hash-derived at package time, `dep-<24 hex>` — not predictable from labels,
# so this script does not guess one).
DRAIN_JSON="null"
if [ "$PROFILE" = "timer-storm" ]; then
    log "timer-storm: polling ${BASE_URL}/sutra/instances?status=WAITING for full drain (timeout ${DRAIN_TIMEOUT}s)"
    poll_start_epoch="$(date +%s)"
    waiting=-1
    timed_out=false
    while :; do
        body="$(curl -s --max-time 5 "${BASE_URL}/sutra/instances?status=WAITING" 2>/dev/null || echo '{}')"
        waiting="$(printf '%s' "$body" | jq -r '(.instances | length) // -1' 2>/dev/null || echo -1)"
        elapsed=$(( $(date +%s) - poll_start_epoch ))
        if [ "$waiting" = "0" ]; then
            break
        fi
        if [ "$elapsed" -ge "$DRAIN_TIMEOUT" ]; then
            timed_out=true
            break
        fi
        sleep 0.5
    done
    elapsed=$(( $(date +%s) - poll_start_epoch ))
    log "timer-storm: drained in ${elapsed}s (waiting=${waiting}, timed_out=${timed_out})"
    DRAIN_JSON="$(jq -n \
        --arg elapsed "$elapsed" --arg waiting "$waiting" --argjson timedOut "$timed_out" \
        '{drainSeconds: ($elapsed|tonumber), finalWaitingCount: ($waiting|tonumber), timedOut: $timedOut, method: "poll GET /sutra/instances?status=WAITING from arm-phase-end"}')"
fi

# --- host metadata (load-generator host, not the SUT — see sustained-rps.sh's own note) ----

cpu_model="$(LC_ALL=C grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //' || echo unknown)"
cpu_cores="$(nproc 2>/dev/null || echo unknown)"
mem_total="$(LC_ALL=C grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2 " " $3}' || echo unknown)"
kernel="$(uname -srvmo 2>/dev/null || echo unknown)"
k6_version="$(k6 version 2>/dev/null | head -1 || echo unknown)"

# --- structured summary ---------------------------------------------------------------------
#
# The measurement-discipline hooks (design §7 + §6.1): achieved throughput and p50/p95/p99
# come straight from k6's own summary. Per-shard queue depth, the handoff counter, and the
# claim-bounce counter are named-but-null here — they ship as metrics in Phase 2
# (`sutra.engine.shard.queue-depth{shard}`, a handoff counter, and a claim-bounce counter
# split by relay/timer; design §6.1), not before; there is nothing to scrape at shard-count=1
# today. DB pool wait is likewise null — the design names it a first-class bench metric
# starting at the Phase 3 soak (§8), not Phase 0; a rough proxy today is sampling
# `pg_stat_activity` on the bench PG container during the run (not automated here).
SUMMARY="${OUT}/summary.json"
jq -n \
    --arg label "$LABEL" \
    --arg profile "$PROFILE" \
    --arg shardsLabel "$SHARDS" \
    --arg target "$TARGET_DESC" \
    --arg timestamp "$TS" \
    --arg fixture "$BENCH_EXAMPLE_DIR" \
    --argjson timerStormDrain "$DRAIN_JSON" \
    --arg cpuModel "$cpu_model" --arg cpuCores "$cpu_cores" --arg memTotal "$mem_total" \
    --arg kernel "$kernel" --arg k6Version "$k6_version" \
    --slurpfile k6 "$K6_SUMMARY" \
    '
    ($k6[0].metrics // {}) as $m |
    {
      label: $label,
      profile: $profile,
      shardsLabel: ($shardsLabel | tonumber),
      shardsNote: "SHARDS is a real engine knob since P1-6 Phase 2: self-start mode boots the SUT with SUTRA_ENGINE_SHARDS=<shardsLabel>; external mode trusts the operator'"'"'s deployment.",
      target: $target,
      timestampUtc: $timestamp,
      fixture: $fixture,
      k6SummaryFile: "k6-summary.json",
      achieved: {
        totalRequests: ($m.http_reqs.count // 0),
        rps: ($m.http_reqs.rate // 0),
        errorRate: ($m.http_req_failed.value // $m.http_req_failed.rate // 0),
        latencyMs: {
          avg: ($m.http_req_duration.avg // 0),
          p50: ($m.http_req_duration.med // 0),
          p95: ($m.http_req_duration["p(95)"] // 0),
          p99: ($m.http_req_duration["p(99)"] // 0)
        },
        checks: { passes: ($m.checks.passes // 0), fails: ($m.checks.fails // 0) }
      },
      timerStormDrain: $timerStormDrain,
      plannedMetrics: {
        note: "Land with Phase 2 — null until then, named here so the result shape does not change again when they arrive.",
        perShardQueueDepth: null,
        handoffCount: null,
        claimBounceCount: null,
        dbPoolWaitMs: null
      },
      loadGeneratorHost: {
        cpuModel: $cpuModel, cpuCores: $cpuCores, memTotal: $memTotal, kernel: $kernel, k6Version: $k6Version
      }
    }
    ' > "$SUMMARY"

log "summary written -> $SUMMARY"
cat "$SUMMARY"

# Broken-target guard: a run where EVERY request failed is not a measurement — it is a
# misconfigured target (wrong fixture/key/URL) producing absurdly-fast error latencies that
# would poison any comparison. Refuse to call it done. (Profiles with empty thresholds —
# correlation-heavy, mixed — otherwise exit 0 on exactly this: the first baseline attempt
# "passed" two profiles at errorRate=1.0 with sub-millisecond p95s.)
err_rate="$(jq -r '.achieved.errorRate' "$SUMMARY")"
total_reqs="$(jq -r '.achieved.totalRequests' "$SUMMARY")"
if [ "$total_reqs" != "0" ] && [ "$(printf '%s >= 0.999\n' "$err_rate" | bc -l)" = "1" ]; then
    die "every request failed (errorRate=${err_rate}, requests=${total_reqs}) — broken target, not a measurement; summary retained at ${SUMMARY} for diagnosis"
fi

log "done — label=${LABEL} profile=${PROFILE} rps=$(jq -r '.achieved.rps' "$SUMMARY") p95ms=$(jq -r '.achieved.latencyMs.p95' "$SUMMARY")"
