#!/usr/bin/env bash
#
# H2 (Factor 5) — emit a Sutra release manifest: the immutable record of WHAT a release shipped,
# so a rollback is "redeploy the prior manifest" rather than a guess. Emitted from CI at release
# time (see .github/workflows/release.yml) and attachable to the GitHub Release.
#
# The manifest pins:
#   - the container image by DIGEST (sha256, reproducible — the reliable rollback anchor);
#   - the git commit + release tag + reproducible build epoch;
#   - the Maven artifact version;
#   - a SHA-256 over the engine's shipped config defaults (config drift detection);
#   - optionally, a per-module DEFINITION FINGERPRINT over each tenant module version's frozen
#     assets (module.yaml + bpmn/ + rules/ + schemas/ — the same file selection VM-9's ModuleLedger
#     freezes), so a deployment can prove its modules match what the release recorded.
#
# Usage:
#   scripts/emit-release-manifest.sh \
#       --tag v0.1.0 --image ghcr.io/org/sutra --digest sha256:abc... \
#       [--platforms linux/amd64,linux/arm64] [--version 0.1.0] \
#       [--modules-root examples/money-transfer/deployments-src] \
#       [--out release-manifest.json]
#
# Deterministic: no wall-clock reads (builtAt comes from the git commit epoch), sorted iteration —
# the same inputs always yield byte-identical output, so the manifest itself is reproducible.
set -euo pipefail

TAG="" IMAGE="" DIGEST="" PLATFORMS="linux/amd64,linux/arm64" VERSION="" MODULES_ROOT="" OUT="release-manifest.json"
while [ $# -gt 0 ]; do
  case "$1" in
    --tag)          TAG="$2"; shift 2 ;;
    --image)        IMAGE="$2"; shift 2 ;;
    --digest)       DIGEST="$2"; shift 2 ;;
    --platforms)    PLATFORMS="$2"; shift 2 ;;
    --version)      VERSION="$2"; shift 2 ;;
    --modules-root) MODULES_ROOT="$2"; shift 2 ;;
    --out)          OUT="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[ -n "$TAG" ]    || { echo "--tag is required" >&2; exit 2; }
[ -n "$IMAGE" ]  || { echo "--image is required" >&2; exit 2; }
[ -n "$DIGEST" ] || { echo "--digest is required" >&2; exit 2; }
[ -n "$VERSION" ] || VERSION="${TAG#v}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

GIT_SHA="$(git rev-parse HEAD)"
GIT_SHA_SHORT="$(git rev-parse --short HEAD)"
# Reproducible: the commit's author epoch, not the wall clock (matches SOURCE_DATE_EPOCH in release.yml).
BUILT_AT="$(git log -1 --pretty=%cI)"

# Config fingerprint — SHA-256 over the engine's shipped config defaults, sorted for determinism.
config_fingerprint() {
  local files
  files="$(find engine -type f \( -name 'microprofile-config.properties' -o -name 'application.properties' \) \
            -path '*/src/main/resources/*' 2>/dev/null | LC_ALL=C sort || true)"
  if [ -z "$files" ]; then echo "sha256:"; return; fi
  echo "sha256:$(printf '%s\n' "$files" | xargs cat | sha256sum | cut -d' ' -f1)"
}

# Per-module DEFINITION FINGERPRINT over a version folder's frozen assets — module.yaml + bpmn/ +
# rules/ + schemas/ (VM-9's ModuleLedger file selection; channels/ excluded as lifecycle-mutable).
# Deterministic: files in sorted relative-path order, each contributing "relpath\0<sha256 of bytes>\0".
definition_fingerprint() {
  local verdir="$1" acc="" rel fh
  while IFS= read -r f; do
    rel="${f#"$verdir"/}"
    fh="$(sha256sum "$f" | cut -d' ' -f1)"
    acc="${acc}${rel}\x00${fh}\x00"
  done < <(
    { [ -f "$verdir/module.yaml" ] && echo "$verdir/module.yaml"
      for sub in bpmn rules schemas; do
        [ -d "$verdir/$sub" ] && find "$verdir/$sub" -type f
      done
    } | LC_ALL=C sort
  )
  echo "sha256:$(printf '%b' "$acc" | sha256sum | cut -d' ' -f1)"
}

# Emit the modules[] JSON array from --modules-root (a tree of <module>/<version>/ folders). Empty
# for a pure engine release (no tenant modules bundled); a deployment passes its own modules root.
modules_json() {
  [ -n "$MODULES_ROOT" ] && [ -d "$MODULES_ROOT" ] || { echo "[]"; return; }
  local out="[" first=1 mod ver verdir
  while IFS= read -r verdir; do
    [ -f "$verdir/module.yaml" ] || continue
    ver="$(basename "$verdir")"
    mod="$(basename "$(dirname "$verdir")")"
    [ $first -eq 1 ] || out="${out},"
    first=0
    out="${out}{\"module\":\"${mod}\",\"version\":\"${ver}\",\"definitionFingerprint\":\"$(definition_fingerprint "$verdir")\"}"
  done < <(find "$MODULES_ROOT" -mindepth 2 -maxdepth 2 -type d | LC_ALL=C sort)
  echo "${out}]"
}

CONFIG_FP="$(config_fingerprint)"
MODULES="$(modules_json)"

cat > "$OUT" <<JSON
{
  "schemaVersion": "1",
  "kind": "sutra-release-manifest",
  "release": {
    "tag": "${TAG}",
    "version": "${VERSION}",
    "gitSha": "${GIT_SHA}",
    "gitShaShort": "${GIT_SHA_SHORT}",
    "builtAt": "${BUILT_AT}",
    "builtBy": "${GITHUB_ACTOR:-ci}"
  },
  "image": {
    "ref": "${IMAGE}:${TAG}",
    "digest": "${DIGEST}",
    "platforms": [$(echo "$PLATFORMS" | awk -F, '{for(i=1;i<=NF;i++){printf "%s\"%s\"",(i>1?",":""),$i}}')]
  },
  "maven": {
    "groupId": "trade.startr.sutra",
    "version": "${VERSION}"
  },
  "config": {
    "engineConfigFingerprint": "${CONFIG_FP}"
  },
  "modules": ${MODULES}
}
JSON

echo "wrote $OUT" >&2
