#!/usr/bin/env bash
# Reap leaked TEST docker fixtures — and nothing else on the host.
#
# Suites start their fixtures through testcontainers, whose reaper is an atexit hook: a killed
# or crashed run leaks them. This removes those leftovers without touching anything a developer
# or a CI job would miss.
#
# Two guards decide what may be removed, and both must pass:
#   * the image is one of the fixture images these suites start (the allowlist below), so a
#     local database, an image registry, a cluster node or a docs server is never a candidate;
#   * the container carries no `com.docker.compose.project` label — a container belonging to a
#     compose stack is someone's environment, even when it is stopped.
# A RUNNING fixture is removed only once it is older than the cutoff (default 30 minutes), so an
# actively-running suite keeps its own; a STOPPED fixture goes at any age. Anonymous volumes
# attached to a removed fixture go with it (`docker rm -v`), which is what keeps a machine from
# accumulating dangling volumes in the first place.
#
# Dangling (untagged) images are pruned — nothing tagged is touched, so sutra-rust-engine:dev,
# cluster node images and the like survive.
#
# --deep adds the host-wide prunes: ALL stopped containers, ALL unused volumes, and the idle
#   build cache. That reclaims the most, and it also removes stopped containers that are not
#   fixtures at all, along with their volumes — right for an ephemeral CI runner, destructive on
#   a dev machine. Opt in deliberately.
#
# Usage: scripts/dev-docker-cleanup.sh [cutoff-minutes] [--deep]
set -euo pipefail

DEEP=0
POS=()
for a in "$@"; do
    if [ "$a" = "--deep" ]; then DEEP=1; else POS+=("$a"); fi
done
CUTOFF_MIN="${POS[0]:-30}"
cutoff=$(( $(date +%s) - CUTOFF_MIN * 60 ))

# The fixture images these suites start. A container from any other image is never removed here.
is_fixture_image() {
    case "$1" in
    postgres:16-alpine | rabbitmq:3.13-management-alpine | apache/kafka-native:3.8.0 | \
        localstack/localstack:3 | mysql:8.0 | mariadb:11 | \
        mcr.microsoft.com/mssql/server:2022-latest | \
        gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators | \
        apache/activemq-artemis:* | hashicorp/vault:1.17 | sutra-rust-engine:*) return 0 ;;
    *) return 1 ;;
    esac
}

removed=0
kept_compose=0
kept_young=0
while IFS='|' read -r id name image created state compose; do
    is_fixture_image "$image" || continue
    if [ -n "$compose" ]; then
        kept_compose=$((kept_compose + 1))
        continue
    fi
    if [ "$state" = "running" ]; then
        ts=$(date -d "$created" +%s 2>/dev/null) || continue
        if [ "$ts" -ge "$cutoff" ]; then
            kept_young=$((kept_young + 1))
            continue
        fi
    fi
    docker rm -f -v "$id" >/dev/null \
        && echo "removed ${name#/} ($image, $state)" \
        && removed=$((removed + 1))
done < <(docker ps -aq | xargs -r docker inspect \
    -f '{{.Id}}|{{.Name}}|{{.Config.Image}}|{{.Created}}|{{.State.Status}}|{{if index .Config.Labels "com.docker.compose.project"}}compose{{end}}')

echo "fixtures removed: $removed (kept: $kept_compose compose-owned, $kept_young younger than ${CUTOFF_MIN}m)"
echo "dangling images:  $(docker image prune -f | tail -1)"

if [ "$DEEP" = 1 ]; then
    echo "deep — host-wide prunes (every stopped container, every unused volume, idle build cache):"
    docker container prune -f | tail -1
    docker volume prune -f | tail -1
    docker builder prune -f | tail -1
fi
