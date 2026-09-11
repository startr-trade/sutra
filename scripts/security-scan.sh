#!/usr/bin/env bash
# Local runs of the two security scanners the public repository's GitHub workflows run
# (trivy.yml and codeql.yml), so a finding surfaces before a push instead of after it.
#
#   security-scan.sh deps           Trivy over the source tree: lockfiles and committed secrets.
#                                   Seconds once its vulnerability database is cached.
#   security-scan.sh image [IMAGE]  Trivy over an engine image (default sutra-rust-engine:dev;
#                                   build it with `make image`). Run it alongside tier-2.
#   security-scan.sh codeql         CodeQL for Rust: build-mode none, the default code-scanning
#                                   suite, scripts/codeql-config.yml. Several minutes; run it
#                                   before anything goes to the public repository.
#
# Unlike the workflows, which only report, every subcommand exits non-zero on any finding. A
# Trivy finding with no reachable fix belongs in .trivyignore.yaml, time-boxed and with its
# justification written out, and nowhere else.
#
# Both tools come from pinned upstream artefacts, so nothing is installed system-wide: Trivy as
# its container image, CodeQL as the CLI bundle cached under ~/.cache/codeql. Needs docker, git,
# curl, tar and jq.
set -euo pipefail

# Keep these in step with the workflows: trivy.yml pins aquasecurity/trivy-action, whose default
# Trivy is 0.70.0; codeql.yml runs codeql-action@v4, which uses the current bundle.
TRIVY_VERSION=${TRIVY_VERSION:-0.70.0}
CODEQL_BUNDLE=${CODEQL_BUNDLE:-codeql-bundle-v2.27.0}
SEVERITY=UNKNOWN,LOW,MEDIUM,HIGH,CRITICAL

root=$(git rev-parse --show-toplevel)
cache=${XDG_CACHE_HOME:-$HOME/.cache}

scratch=()
cleanup() { [ ${#scratch[@]} -eq 0 ] || rm -rf "${scratch[@]}"; }
trap cleanup EXIT
new_scratch() {
    local dir
    dir=$(mktemp -d)
    scratch+=("$dir")
    echo "$dir"
}

# What a fresh checkout would contain (tracked files, plus untracked ones git does not ignore),
# copied to $1. The workflows scan a checkout; scanning the working directory instead would also
# read rust/target and installed node_modules, and report things GitHub never sees.
checkout_copy() {
    git -C "$root" ls-files -z --cached --others --exclude-standard \
        | tar -C "$root" --null --ignore-failed-read -T - -cf - \
        | tar -C "$1" -xf -
}

trivy() {
    mkdir -p "$cache/trivy"
    docker run --rm -v "$cache/trivy:/root/.cache/trivy" "$@"
}

scan_deps() {
    local tree
    tree=$(new_scratch)
    checkout_copy "$tree"
    trivy -v "$tree:/src:ro" -w /src "aquasec/trivy:$TRIVY_VERSION" fs \
        --severity "$SEVERITY" --ignorefile .trivyignore.yaml --exit-code 1 .
}

scan_image() {
    local image=${1:-sutra-rust-engine:dev}
    docker image inspect "$image" >/dev/null 2>&1 || {
        echo "security-scan: no image '$image'; build it first (make image)" >&2
        exit 2
    }
    trivy -v /var/run/docker.sock:/var/run/docker.sock \
        -v "$root/.trivyignore.yaml:/trivyignore.yaml:ro" "aquasec/trivy:$TRIVY_VERSION" image \
        --severity "$SEVERITY" --ignorefile /trivyignore.yaml --exit-code 1 "$image"
}

codeql_cli() {
    local home="$cache/codeql/$CODEQL_BUNDLE"
    if [ ! -x "$home/codeql/codeql" ]; then
        echo "security-scan: fetching $CODEQL_BUNDLE (once; about 1 GB unpacked)" >&2
        mkdir -p "$home"
        curl -fsSL "https://github.com/github/codeql-action/releases/download/$CODEQL_BUNDLE/codeql-bundle-linux64.tar.gz" \
            | tar -xzf - -C "$home"
    fi
    echo "$home/codeql/codeql"
}

scan_codeql() {
    local codeql work sarif count
    codeql=$(codeql_cli)
    work=$(new_scratch)
    mkdir -p "$work/src" "$root/rust/target/codeql"
    sarif="$root/rust/target/codeql/rust.sarif"
    checkout_copy "$work/src"
    "$codeql" database create "$work/db" --language=rust --build-mode=none \
        --source-root="$work/src" --codescanning-config="$root/scripts/codeql-config.yml" \
        --threads=0 --quiet
    "$codeql" database analyze "$work/db" --format=sarif-latest --output="$sarif" \
        --threads=0 --quiet
    count=$(jq '[.runs[].results[]] | length' "$sarif")
    if [ "$count" -gt 0 ]; then
        jq -r '.runs[].results[] | "\(.ruleId)  \(.locations[0].physicalLocation.artifactLocation.uri):\(.locations[0].physicalLocation.region.startLine)  \(.message.text | split("\n")[0])"' "$sarif" >&2
        echo "security-scan: CodeQL reported $count finding(s); full SARIF at $sarif" >&2
        exit 1
    fi
    echo "security-scan: CodeQL clean (SARIF at $sarif)"
}

case "${1:-}" in
    deps) scan_deps ;;
    image) scan_image "${2:-}" ;;
    codeql) scan_codeql ;;
    *)
        sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
        exit 2
        ;;
esac
