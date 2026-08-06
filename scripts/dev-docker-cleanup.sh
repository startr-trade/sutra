#!/usr/bin/env bash
# Remove leaked TEST docker resources without touching the kind cluster, the local
# registry, or the mkdocs container.
#
# Two layers:
#   1. Running test-image fixtures older than the cutoff (default 30 min) are removed
#      — so an ACTIVELY-running suite keeps its fixtures (the reaper is atexit; killed
#      or crashed runs leak). Only the known test images below are ever matched, so
#      kind / registry / docs are never candidates.
#   2. Unconditional prune of STOPPED containers, UNUSED volumes, and DANGLING (untagged)
#      images — none of which can belong to a running container, so nothing live is hit.
#      Tagged images (sutra-rust-engine:dev, sutra-engine:k8s-it, kind node images) are
#      KEPT (this never runs `image prune -a`).
#
# --deep additionally prunes the IDLE build cache (often the biggest reclaim — was ~11 GB
#   in one instance — but it makes the next image build cache-cold, so it's opt-in). Only
#   the *idle* cache is removed; an in-progress build's active cache is untouched.
#
# Usage: scripts/dev-docker-cleanup.sh [cutoff-minutes] [--deep]
set -euo pipefail

DEEP=0
POS=()
for a in "$@"; do
  if [ "$a" = "--deep" ]; then DEEP=1; else POS+=("$a"); fi
done
CUTOFF_MIN="${POS[0]:-30}"
cutoff=$(( $(date +%s) - CUTOFF_MIN*60 ))
removed=0

# ---- 1. running test-image fixtures older than the cutoff -----------------------------
while IFS='|' read -r id created image; do
  case "$image" in
    postgres:16-alpine|rabbitmq:3.13-management-alpine|apache/kafka-native:3.8.0|localstack/localstack:3|mysql:8.0|mariadb:11|mcr.microsoft.com/mssql/server:2022-latest|gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators|apache/activemq-artemis:*|hashicorp/vault:1.17|sutra-rust-engine:*)
      ts=$(date -d "$(echo "$created" | awk '{print $1, $2, $3}')" +%s 2>/dev/null) || continue
      if [ "$ts" -lt "$cutoff" ]; then
        docker rm -f "$id" >/dev/null && echo "removed $image ($id)" && removed=$((removed+1))
      fi ;;
  esac
done < <(docker ps --format '{{.ID}}|{{.CreatedAt}}|{{.Image}}')
echo "aged fixtures removed: $removed"

# ---- 2. unconditional prune of stopped/unused/dangling (never touches running) --------
echo "stopped containers: $(docker container prune -f | tail -1)"
echo "unused volumes:     $(docker volume prune -f | tail -1)"
echo "dangling images:    $(docker image prune -f | tail -1)"

# ---- 3. deep: idle build cache (opt-in; makes the next build cache-cold) --------------
if [ "$DEEP" = 1 ]; then
  echo "deep — idle build cache:"
  docker builder prune -f | tail -1
fi
