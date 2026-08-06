#!/usr/bin/env bash
# Shared helpers for the Sutra GA-readiness bench harness (rust/bench/).
#
# NOT a crate — plain shell. Sourced by cold-start.sh / peak-rss.sh / sustained-rps.sh.
# Every container these helpers start is labelled `sutra-bench` and torn down by the
# caller's EXIT trap (see each script). The engine image and the money-transfer archive are
# the same fixtures the conformance harness (sutra-conformance) uses; the shape is kept in
# step with that crate's tests/all/support/engine.rs so the two never drift.
#
# Env knobs (all optional; defaults chosen for a local single-host run):
#   SUTRA_ENGINE_IMAGE   engine image under test        (default sutra-rust-engine:dev)
#   SUTRA_CLI            path to the `sutra` binary      (default ../target/release/sutra)
#   PG_IMAGE             postgres image for the store    (default postgres:16-alpine)
#   BENCH_EXAMPLE_DIR    package-dir to deploy           (default the approval-hold archive)
#
# Default deployment is approval-hold (HTTP-only: no broker, no second datastore). Its
# `showcase-request` channel drives a STATELESS flow (Prep -> gateway -> template -> reply)
# that replies 200 synchronously and holds no per-request state, so repeated identical requests
# never alias-conflict — the ideal sustained-load target. The engine still needs its own core
# PostgreSQL (outbox / instance-state / lease); that is what start_pg wires. Override
# BENCH_EXAMPLE_DIR (+ the channel envs in sustained-rps.sh) to bench a different example.

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/../.." && pwd)"

SUTRA_ENGINE_IMAGE="${SUTRA_ENGINE_IMAGE:-sutra-rust-engine:dev}"
SUTRA_CLI="${SUTRA_CLI:-${REPO_ROOT}/rust/target/release/sutra}"
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"
BENCH_EXAMPLE_DIR="${BENCH_EXAMPLE_DIR:-${REPO_ROOT}/examples/approval-hold/deployments-src/default--approval--1.0.0}"

# A per-run tag so parallel invocations and cleanup never collide.
BENCH_TAG="sutra-bench-$$"

log() { printf '[bench] %s\n' "$*" >&2; }
die() { printf '[bench] ERROR: %s\n' "$*" >&2; exit 1; }

require() {
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || die "missing required tool: $tool (see rust/bench/README.md)"
  done
}

# Assemble the money-transfer package dir into a directory of sealed .sutra archives, using
# the CLI's deterministic packager. Echoes the archives directory.
package_archives() {
  [ -x "$SUTRA_CLI" ] || die "sutra CLI not found at $SUTRA_CLI — build it: (cd rust && cargo build --release -p sutra-cli)"
  local out; out="$(mktemp -d "/tmp/${BENCH_TAG}-archives.XXXXXX")"
  # mktemp -d defaults to 0700 (owner-only). This dir gets bind-mounted into the engine
  # container, which runs as an unprivileged, non-host UID (distroless nonroot) — it needs at
  # least traverse+read to list/open the archive, or boot fails fail-closed with
  # SUTRA.DEPLOY.SOURCE.UNREADABLE ("Permission denied listing the deployments dir").
  chmod 755 "$out"
  log "packaging $(basename "$BENCH_EXAMPLE_DIR") -> $out"
  "$SUTRA_CLI" package "$BENCH_EXAMPLE_DIR" --out "$out" >&2
  echo "$out"
}

# Start the engine-internal PostgreSQL. Echoes "<container_id> <host_port>" — mirrors
# start_engine's own return shape. The caller must set the SUTRA_DATASOURCE_* env itself, via
# export_pg_datasource (below), AFTER capturing this function's output: an `export` done
# *inside* a function only lives as long as bash keeps the subshell it runs the function's
# command/process substitution in (`"$(start_pg)"` / `< <(start_pg)`), so exports set in here
# never reach the caller's real shell — same reason start_engine only ever echoes, never
# exports.
start_pg() {
  require docker
  local cid
  cid="$(docker run -d --rm --label "$BENCH_TAG" \
    -e POSTGRES_USER=sutra -e POSTGRES_PASSWORD=sutra -e POSTGRES_DB=sutra \
    "$PG_IMAGE")"
  # This host's docker cannot route a `--network host` container to a bridge-PUBLISHED port
  # over 127.0.0.1 (userland-proxy off), so the engine reaches PG at its container IP:5432
  # instead (the host routes to docker0). We therefore echo the container IP, not a mapped port.
  local ip; ip="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$cid")"
  # Wait for the TCP listener (pg_isready over TCP inside the container). The unix socket accepts
  # before the TCP port opens, so a plain socket pg_isready returns too early for a TCP client.
  local i
  for i in $(seq 1 120); do
    if docker exec "$cid" pg_isready -U sutra -h 127.0.0.1 -q 2>/dev/null; then break; fi
    sleep 0.25
  done
  echo "$cid $ip"
}

# Export SUTRA_DATASOURCE_* for the engine, given the host port start_pg returned. Must run in
# the CALLER's shell (not inside start_pg — see its doc above).
export_pg_datasource() {
  # $1 is the PG container IP (see start_pg) — a host-networked engine reaches it via docker0.
  local host="$1"
  export SUTRA_DATASOURCE_URL="postgres://${host}:5432/sutra"
  export SUTRA_DATASOURCE_USERNAME=sutra
  export SUTRA_DATASOURCE_PASSWORD=sutra
  export SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED=false
}

# A free TCP port on 127.0.0.1, picked by asking the OS for an ephemeral one and releasing it
# immediately. Host-networked containers (below) bind host ports directly — no `docker port`
# indirection like start_pg's bridge-mode PG has — so a hardcoded port is a real collision risk
# on a shared dev host (rule: never hardcode 8080, it may be someone's local k8s server). A
# release/rebind race exists but is acceptable for a single local bench run.
find_free_port() {
  require python3
  python3 -c '
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
'
}

# Start the engine container against the given archives dir. Echoes "<container_id> <host_port>".
# The caller times this + wait_ready for cold-start, or reads its RSS / drives it with k6.
start_engine() {
  local archives="$1"
  require docker
  local cid port
  port="$(find_free_port)"
  cid="$(docker run -d --rm --label "$BENCH_TAG" \
    --network host \
    -e SUTRA_DATASOURCE_URL="$SUTRA_DATASOURCE_URL" \
    -e SUTRA_DATASOURCE_USERNAME="$SUTRA_DATASOURCE_USERNAME" \
    -e SUTRA_DATASOURCE_PASSWORD="$SUTRA_DATASOURCE_PASSWORD" \
    -e SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED="$SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED" \
    `# ACCOUNTS_DB_* is unused by the default approval-hold deployment (HTTP-only, no second` \
    `# store). It is wired to the same bench PG so a money-transfer override (which owns a second` \
    `# 'accounts'/'coverage' datastore via env:ACCOUNTS_DB_*) still resolves its store + self-` \
    `# migrating seed instead of failing with an unknown-store diagnostic. Harmless when unref'd.` \
    -e ACCOUNTS_DB_URL="$SUTRA_DATASOURCE_URL" \
    -e ACCOUNTS_DB_USER="$SUTRA_DATASOURCE_USERNAME" \
    -e ACCOUNTS_DB_PASSWORD="$SUTRA_DATASOURCE_PASSWORD" \
    -e SUTRA_DEPLOYMENTS_DIR=/etc/sutra/deployments \
    -e SUTRA_HTTP_PORT="$port" \
    `# SUTRA_ENGINE_SHARDS: a real key since P1-6 Phase 2 (default 1). Threaded from the` \
    `# caller's SHARDS knob so saturation-matrix.sh's N-matrix boots a genuinely-N-lane SUT;` \
    `# empty means the engine default.` \
    ${SUTRA_ENGINE_SHARDS:+-e SUTRA_ENGINE_SHARDS="$SUTRA_ENGINE_SHARDS"} \
    -v "${archives}:/etc/sutra/deployments:ro" \
    "$SUTRA_ENGINE_IMAGE")"
  # host networking → the engine listens on $port on the host directly.
  echo "$cid $port"
}

# Poll /sutra/health/ready until it reports HTTP 200 AND at least $2 (default 1) ACTIVE
# deployments — the same signal the conformance harness waits on (support/engine.rs
# await_ready_deployments: checks[0].data.deployments >= expected). A plain 200 already implies
# the deployment loader check is UP, but asserting the count makes the gate explicit and matches
# the tier-2 harness exactly, so k6 never fires before the channel routes are mounted (they are
# mounted straight from the boot-time assembly — there is no separate activation flip to await).
# Returns non-zero after ~30s.
wait_ready() {
  local port="$1"
  local expected="${2:-1}"
  local i body count
  for i in $(seq 1 300); do
    body="$(curl -s "http://127.0.0.1:${port}/sutra/health/ready" 2>/dev/null || true)"
    # checks[0].data.deployments — the active-deployment count the loader health check reports.
    count="$(printf '%s' "$body" | python3 -c '
import json,sys
try:
    d=json.load(sys.stdin)
    print(d["checks"][0]["data"]["deployments"])
except Exception:
    print(-1)
' 2>/dev/null || echo -1)"
    if [ "${count:-0}" -ge "$expected" ] 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

# Kill every container this run started (idempotent — safe in an EXIT trap).
bench_cleanup() {
  local ids
  ids="$(docker ps -aq --filter "label=${BENCH_TAG}" 2>/dev/null || true)"
  [ -n "$ids" ] && docker rm -f $ids >/dev/null 2>&1 || true
  rm -rf "/tmp/${BENCH_TAG}-archives."* 2>/dev/null || true
}
