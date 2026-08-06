#!/usr/bin/env bash
# Peak RSS of the engine container at ready, via `docker stats --no-stream`.
#
# Metric: MEM USAGE reported by docker stats once /sutra/health/ready is 200 (the engine has booted,
# connected the datasource, applied migrations, and activated the default (approval-hold) archive
# but has served no traffic — the idle-ready working set). For a loaded-peak figure, run this
# again immediately after sustained-rps.sh while the same engine is under load (pass
# SKIP_START=1 and an existing container id).
#
# Prereqs: docker, curl, the engine image, the `sutra` CLI. See rust/bench/README.md.
# Output: results/peak-rss.txt under rust/bench/.
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

OUT="${BENCH_DIR}/results"
mkdir -p "$OUT"

require docker curl
trap bench_cleanup EXIT

log "starting PostgreSQL ($PG_IMAGE)"
read -r PG_CID PG_PORT < <(start_pg)
export_pg_datasource "$PG_PORT"
log "packaging $(basename "$BENCH_EXAMPLE_DIR") archive"
ARCHIVES="$(package_archives)"
log "starting engine ($SUTRA_ENGINE_IMAGE)"
read -r ENGINE_CID ENGINE_PORT < <(start_engine "$ARCHIVES")
log "waiting for /sutra/health/ready 200 on :${ENGINE_PORT}"
wait_ready "$ENGINE_PORT" || die "engine never became ready"

# docker stats MEM USAGE column, e.g. "41.2MiB / 15.3GiB" -> take the used side.
STATS="$(docker stats --no-stream --format '{{.MemUsage}}' "$ENGINE_CID")"
RSS="${STATS%% /*}"
printf 'engine idle-ready RSS: %s\n' "$RSS" | tee "$OUT/peak-rss.txt" >&2
log "done — see $OUT/peak-rss.txt"
