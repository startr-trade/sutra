#!/usr/bin/env bash
# Sustained RPS + p95 latency against the engine container, driven by the neutral k6
# profiles in tools/sutra-load-test/ (they are plain JS and stay outside reference/).
#
# Metric: achieved request rate and http_req_duration p95 over the chosen profile
# (default `sustained` — ~5 min, 100 offered RPS). k6 reports both in its summary export;
# this script stands up the engine container as the SUT (in place of a locally-started process)
# and points k6 at it.
#
# The profile JS reads BASE_URL / API_KEY / PAYLOAD / CHANNEL_URL / CONTENT_TYPE from env —
# supply the money-transfer channel + a valid payload for a representative end-to-end number.
# The default profile targets the scaffolded `hello-in` channel (text/plain); override
# CHANNEL_URL + CONTENT_TYPE + PAYLOAD (+ API_KEY) for the money-transfer path — see the
# `balance`-channel invocation in rust/bench/README.md.
#
# Prereqs: docker, curl, k6, the engine image, the `sutra` CLI (or `sutra-bench-packager` —
# see README's "No hyperfine / can't build sutra-cli?" section). See rust/bench/README.md.
# Output: results/sustained-rps.json (k6 summary export) under rust/bench/.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PROFILE="${PROFILE:-sustained}"
PROFILE_JS="${REPO_ROOT}/tools/sutra-load-test/profiles/${PROFILE}.js"
OUT="${BENCH_DIR}/results"
mkdir -p "$OUT"

require docker curl k6
[ -f "$PROFILE_JS" ] || die "profile not found: $PROFILE_JS"
trap bench_cleanup EXIT

log "starting PostgreSQL ($PG_IMAGE)"
read -r PG_CID PG_PORT < <(start_pg)
export_pg_datasource "$PG_PORT"
log "packaging $(basename "$BENCH_EXAMPLE_DIR") archive"
ARCHIVES="$(package_archives)"
log "starting engine ($SUTRA_ENGINE_IMAGE)"
read -r ENGINE_CID ENGINE_PORT < <(start_engine "$ARCHIVES")
log "waiting for /sutra/health/ready 200 (+ >=1 active deployment) on :${ENGINE_PORT}"
wait_ready "$ENGINE_PORT" || die "engine never became ready"

log "running k6 profile '${PROFILE}' against http://127.0.0.1:${ENGINE_PORT}"
# Defaults target the approval-hold example's `showcase-request` channel — a stateless XML flow
# (Prep -> gateway -> template -> reply) that replies 200 synchronously with no per-request state,
# so a sustained stream of identical requests never alias-conflicts. Override CHANNEL_URL /
# CONTENT_TYPE / PAYLOAD / API_KEY to bench a different deployment's channel.
BASE_URL="http://127.0.0.1:${ENGINE_PORT}" \
API_KEY="${API_KEY:-approval-demo-key}" \
PAYLOAD="${PAYLOAD:-${REPO_ROOT}/tools/sutra-load-test/fixtures/approval-showcase.xml}" \
CHANNEL_URL="${CHANNEL_URL:-/channels/showcase-request}" \
CONTENT_TYPE="${CONTENT_TYPE:-application/xml}" \
  k6 run --summary-export "$OUT/sustained-rps.json" "$PROFILE_JS"

log "done — RPS = http_reqs.rate, p95 = metrics.http_req_duration['p(95)'] in $OUT/sustained-rps.json"
