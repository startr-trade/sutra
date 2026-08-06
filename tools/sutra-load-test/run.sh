#!/usr/bin/env bash
#
# sutra load-test harness.
#
# Runs a k6 profile against an ALREADY-RUNNING Sutra deployment, captures throughput +
# latency + error rate, writes both raw JSON and a human-readable markdown summary, and
# exits with a threshold-derived code.
#
# The harness never starts, builds, or provisions anything. Point it at a base URL that
# already serves the channel under test — a locally-run `sutra-engine` container, a kind
# LoadBalancer/Ingress, or a remote deployment.
#
# Usage:
#   tools/sutra-load-test/run.sh --target=external --url=http://127.0.0.1:8081 --profile=smoke
#   tools/sutra-load-test/run.sh --target=external --url=https://staging.example/ --profile=sustained
#
# Targets:
#   external   — the only target. `--url=...` is required; k6 runs against that base URL.
#
# Profiles:
#   smoke      — 30 s @ 10 RPS, 1 VU. Cheapest "the wire path works" check.
#   sustained  — 5 min @ 100 RPS. The headline number.
#   burst      — 2 min @ 0→500 RPS ramp + 60 s hold + ramp-down.
#
# Exit codes:
#   0  success — all thresholds green.
#   1  error-rate threshold breached (default >0.5%).
#   2  p99 latency threshold breached (default >500ms).
#   3  harness error (bad arguments, k6 missing, k6 produced no output, etc.).
#
# Threshold knobs (env):
#   ERROR_RATE_THRESHOLD   default 0.005   (= 0.5%)
#   P99_MS_THRESHOLD       default 500
#
# Output (per run):
#   tools/sutra-load-test/results/<timestamp>-<profile>-<target>.json
#   tools/sutra-load-test/results/<timestamp>-<profile>-<target>.md
#
# Both are .gitignore'd locally; CI uploads them as artifacts with 90-day retention.

set -u -o pipefail

# --- arg parsing --------------------------------------------------------------

TARGET=""
PROFILE=""
EXTERNAL_URL=""
ERROR_RATE_THRESHOLD="${ERROR_RATE_THRESHOLD:-0.005}"
P99_MS_THRESHOLD="${P99_MS_THRESHOLD:-500}"

for arg in "$@"; do
    case "$arg" in
        --target=*)   TARGET="${arg#--target=}" ;;
        --profile=*)  PROFILE="${arg#--profile=}" ;;
        --url=*)      EXTERNAL_URL="${arg#--url=}" ;;
        -h|--help)
            sed -n '3,33p' "$0"
            exit 0
            ;;
        *)
            echo "[load-test] unknown arg: $arg" >&2
            echo "[load-test] usage: $0 --target=external --url=<base-url> --profile=<smoke|sustained|burst>" >&2
            exit 3
            ;;
    esac
done

if [ -z "$TARGET" ] || [ -z "$PROFILE" ]; then
    echo "[load-test] --target and --profile are required" >&2
    exit 3
fi

case "$TARGET" in
    external) ;;
    *) echo "[load-test] unknown target: $TARGET (expected external)" >&2; exit 3 ;;
esac

case "$PROFILE" in
    smoke|sustained|burst) ;;
    *) echo "[load-test] unknown profile: $PROFILE (expected smoke|sustained|burst)" >&2; exit 3 ;;
esac

if [ -z "$EXTERNAL_URL" ]; then
    echo "[load-test] --target=external requires --url=..." >&2
    exit 3
fi

# --- paths --------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
PROFILE_SCRIPT="$SCRIPT_DIR/profiles/${PROFILE}.js"
# Default request body: a small text/plain body for the sample `hello-in` channel, which
# echoes "Hello, <body>!". Override with PAYLOAD=/abs/path for a custom body.
PAYLOAD_PATH="${PAYLOAD:-$SCRIPT_DIR/fixtures/hello.txt}"

mkdir -p "$RESULTS_DIR"

if [ ! -f "$PROFILE_SCRIPT" ]; then
    echo "[load-test] profile script not found: $PROFILE_SCRIPT" >&2
    exit 3
fi
if [ ! -f "$PAYLOAD_PATH" ]; then
    echo "[load-test] payload not found: $PAYLOAD_PATH" >&2
    exit 3
fi

TS="$(date -u +%Y%m%dT%H%M%SZ)"
JSON_OUT="$RESULTS_DIR/${TS}-${PROFILE}-${TARGET}.json"
SUMMARY_OUT="$RESULTS_DIR/${TS}-${PROFILE}-${TARGET}.md"

# --- prerequisites ------------------------------------------------------------

require() {
    command -v "$1" >/dev/null 2>&1 || { echo "[load-test] missing prerequisite: $1" >&2; exit 3; }
}

require k6
require jq
require curl

# --- main ---------------------------------------------------------------------

BASE_URL="$EXTERNAL_URL"

echo "[load-test] target=$TARGET profile=$PROFILE"
echo "[load-test] base url -> $BASE_URL"
echo "[load-test] results  -> $JSON_OUT"

# The api-key sent as `X-API-Key` on every request. The engine's inbound `apikey` auth
# does a constant-time compare against the value the channel resolves, so this must match
# whatever the target deployment's channel expects.
#
# The default matches the sample `hello-in` channel used by the docs walkthrough; override
# via SUTRA_API_KEY=... when hitting a deployment that resolves a different key.
API_KEY="${SUTRA_API_KEY:-hello-demo-key}"

# Run k6 with both summary (stdout) and JSON (file) outputs. --summary-export
# emits the aggregated metrics k6 prints at end-of-run as machine-readable JSON,
# which is what we summarise below. --out json=... emits per-request samples
# (large; useful for post-hoc analysis but not parsed here).
set +e
BASE_URL="$BASE_URL" \
API_KEY="$API_KEY" \
PAYLOAD="$PAYLOAD_PATH" \
    k6 run \
        --summary-export="$JSON_OUT" \
        "$PROFILE_SCRIPT"
K6_EXIT=$?
set -e

if [ ! -f "$JSON_OUT" ]; then
    echo "[load-test] k6 produced no JSON output (exit=$K6_EXIT)" >&2
    exit 3
fi

# --- summary extraction -------------------------------------------------------

# k6's --summary-export schema:
#   .metrics.http_reqs.count       — total requests
#   .metrics.http_reqs.rate        — RPS averaged over the test
#   .metrics.http_req_duration.avg / med / p(90) / p(95) / "p(99)" — latency ms
#   .metrics.http_req_failed.value — fraction of failed requests (0..1)
#   .metrics.checks.passes / fails — k6 check() counters
#
# jq's defaults give us null on missing keys; we coalesce to 0 so the markdown
# renders cleanly even on a near-empty run.

total_reqs="$(jq -r '.metrics.http_reqs.count   // 0'           "$JSON_OUT")"
rps="$(jq        -r '.metrics.http_reqs.rate    // 0'           "$JSON_OUT")"
p50="$(jq        -r '.metrics.http_req_duration.med  // 0'      "$JSON_OUT")"
p95="$(jq        -r '.metrics.http_req_duration["p(95)"] // 0'  "$JSON_OUT")"
p99="$(jq        -r '.metrics.http_req_duration["p(99)"] // 0'  "$JSON_OUT")"
avg="$(jq        -r '.metrics.http_req_duration.avg  // 0'      "$JSON_OUT")"
err_rate="$(jq   -r '.metrics.http_req_failed.value // .metrics.http_req_failed.rate // 0' "$JSON_OUT")"
chk_pass="$(jq   -r '.metrics.checks.passes // 0'               "$JSON_OUT")"
chk_fail="$(jq   -r '.metrics.checks.fails  // 0'               "$JSON_OUT")"

# --- host metadata ------------------------------------------------------------
#
# This describes the LOAD-GENERATOR host, not the system under test — the SUT runs
# elsewhere, so its resource usage is not observable from here.

cpu_model="$(LC_ALL=C grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ //' || echo unknown)"
cpu_cores="$(nproc 2>/dev/null || echo unknown)"
mem_total="$(LC_ALL=C grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2 " " $3}' || echo unknown)"
kernel="$(uname -srvmo 2>/dev/null || echo unknown)"
k6_version="$(k6 version 2>/dev/null | head -1 || echo unknown)"

# --- markdown summary ---------------------------------------------------------

{
    echo "# Load-test run — ${PROFILE} / ${TARGET}"
    echo
    echo "- **Timestamp (UTC)**: ${TS}"
    echo "- **Profile**: \`${PROFILE}\` (script: \`tools/sutra-load-test/profiles/${PROFILE}.js\`)"
    echo "- **Target**: external @ ${EXTERNAL_URL}"
    echo "- **Payload**: \`${PAYLOAD_PATH#"$REPO_ROOT"/}\` → POST /channels/hello-in"
    echo "- **k6 exit code**: ${K6_EXIT}"
    echo
    echo "## Headline numbers"
    echo
    echo "| Metric | Value |"
    echo "|---|---|"
    printf '| Total requests | %s |\n'              "$total_reqs"
    printf '| Throughput (RPS) | %.2f |\n'          "$rps"
    printf '| Error rate (non-2xx + transport) | %.4f (= %.2f%%) |\n' "$err_rate" "$(awk -v e="$err_rate" 'BEGIN { print e*100 }')"
    printf '| Latency avg | %.2f ms |\n'            "$avg"
    printf '| Latency p50 | %.2f ms |\n'            "$p50"
    printf '| Latency p95 | %.2f ms |\n'            "$p95"
    printf '| Latency p99 | %.2f ms |\n'            "$p99"
    printf '| Checks passed | %s |\n'               "$chk_pass"
    printf '| Checks failed | %s |\n'               "$chk_fail"
    echo
    echo "## Load-generator host metadata"
    echo
    echo "- **CPU**: ${cpu_model}"
    echo "- **Cores**: ${cpu_cores}"
    echo "- **Memory total**: ${mem_total}"
    echo "- **Kernel**: ${kernel}"
    echo "- **k6**: ${k6_version}"
    echo
    echo "## How to read this"
    echo
    echo "- These numbers reflect the SUT *and* the network path between this host and it, and they are sensitive to the load-generator host (CPU model, frequency scaling, thermal). Compare across runs on the same host against the same deployment; never across hosts."
    echo "- Raw k6 \`--summary-export\` JSON for this run: \`results/${TS}-${PROFILE}-${TARGET}.json\`."
    echo "- For per-request samples, re-run with \`k6 run --out json=... \$PROFILE\` directly."
} > "$SUMMARY_OUT"

echo
echo "[load-test] summary written -> $SUMMARY_OUT"
cat "$SUMMARY_OUT"

# --- exit-code derivation -----------------------------------------------------

# Threshold gate: if either threshold is breached, we exit non-zero so CI fails
# loudly. k6's own thresholds (defined in each profile) already cause k6 to
# exit non-zero on breach; we layer our own thresholds on top so the harness
# defines the contract regardless of any k6 threshold tuning per profile.

# Use awk for floating-point comparisons (bash doesn't natively).
breached_err="$(awk -v r="$err_rate" -v t="$ERROR_RATE_THRESHOLD" 'BEGIN { print (r > t) ? "1" : "0" }')"
breached_p99="$(awk -v p="$p99"      -v t="$P99_MS_THRESHOLD"     'BEGIN { print (p > t) ? "1" : "0" }')"

if [ "$breached_err" = "1" ]; then
    echo "[load-test] FAIL — error rate $err_rate exceeds threshold $ERROR_RATE_THRESHOLD" >&2
    exit 1
fi
if [ "$breached_p99" = "1" ]; then
    echo "[load-test] FAIL — p99 ${p99}ms exceeds threshold ${P99_MS_THRESHOLD}ms" >&2
    exit 2
fi

echo "[load-test] PASS"
exit 0
