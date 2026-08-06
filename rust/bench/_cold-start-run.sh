#!/usr/bin/env bash
# One cold-start timing unit — invoked by hyperfine (cold-start.sh), NOT run directly.
#
# Starts a fresh engine container and blocks until /sutra/health/ready returns 200. hyperfine times
# this whole script; the matching --conclude in cold-start.sh removes the container between
# runs so every run measures a genuine cold boot. The datasource env + $BENCH_TAG + $ARCHIVES
# + $SUTRA_ENGINE_IMAGE are exported by cold-start.sh and inherited here.
#
# Readiness = /sutra/health/ready 200 = datasource reachable + migrations applied + archives activated.
# The 20ms poll granularity is the measurement floor for every number this script produces.
set -euo pipefail

docker run -d --name "${BENCH_TAG}-engine" --network host \
  -e SUTRA_DATASOURCE_URL -e SUTRA_DATASOURCE_USERNAME -e SUTRA_DATASOURCE_PASSWORD \
  -e SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED \
  -e SUTRA_DEPLOYMENTS_DIR=/etc/sutra/deployments -e SUTRA_HTTP_PORT \
  -v "${ARCHIVES}:/etc/sutra/deployments:ro" \
  "${SUTRA_ENGINE_IMAGE}" >/dev/null

# Bounded readiness poll (20ms granularity = the measurement floor). Fail fast with the engine's
# logs instead of hanging forever if it refuses to serve (e.g. a datasource connect error).
ready=0
for _ in $(seq 1 3000); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${SUTRA_HTTP_PORT}/sutra/health/ready" 2>/dev/null)" = "200" ]; then
    ready=1; break
  fi
  sleep 0.02
done
[ "$ready" = 1 ] || { echo "engine did not become ready within 60s — refusing to serve?" >&2; docker logs "${BENCH_TAG}-engine" 2>&1 | tail -20 >&2; exit 1; }
