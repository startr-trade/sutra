#!/bin/sh
# Sutra CLI installer.
#
#   curl -fsSL https://raw.githubusercontent.com/startr-trade/sutra/main/scripts/install.sh | sh
#
# Downloads the `sutra` binary for this platform from a GitHub release, VERIFIES its
# SHA-256 against the release's own SHA256SUMS, and installs it. Nothing else: no shell
# profile is edited, no package manager is invoked, no daemon is started.
#
# Knobs (env or flag):
#   SUTRA_VERSION=v0.2.0-rc.1   --version <tag>   pin a release (default: latest, incl. pre-release)
#   SUTRA_INSTALL_DIR=~/.local/bin  --dir <path>  install location (default: see below)
#   SUTRA_NO_VERIFY=1                             skip checksum verification (discouraged)
#
# Default install dir: $SUTRA_INSTALL_DIR, else /usr/local/bin when writable (or sudo is
# available and we are interactive), else ~/.local/bin.
#
# Exit codes: 0 ok · 1 usage/args · 2 unsupported platform · 3 download/network · 4 checksum.

set -eu

REPO="startr-trade/sutra"
API="https://api.github.com/repos/${REPO}"
DL="https://github.com/${REPO}/releases/download"

VERSION="${SUTRA_VERSION:-}"
INSTALL_DIR="${SUTRA_INSTALL_DIR:-}"
NO_VERIFY="${SUTRA_NO_VERIFY:-}"

die() { printf 'sutra-install: %s\n' "$1" >&2; exit "${2:-1}"; }
info() { printf '  %s\n' "$1" >&2; }
have() { command -v "$1" >/dev/null 2>&1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --version) VERSION="${2:?--version needs a tag}"; shift 2 ;;
        --dir)     INSTALL_DIR="${2:?--dir needs a path}"; shift 2 ;;
        --no-verify) NO_VERIFY=1; shift ;;
        -h|--help)
            sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done

# ---- platform ---------------------------------------------------------------------------
# Release assets are built for three targets (see .github/workflows/release.yml); anything
# else must build from source rather than get a silently wrong binary.
os="$(uname -s)"
arch="$(uname -m)"
case "${os}/${arch}" in
    Linux/x86_64|Linux/amd64)   TARGET="x86_64-unknown-linux-musl" ;;
    Linux/aarch64|Linux/arm64)  TARGET="aarch64-unknown-linux-musl" ;;
    Darwin/*)
        die "macOS binaries are not published yet — build from source:
    git clone https://github.com/${REPO}.git && cd sutra/rust
    cargo install --path crates/sutra-cli" 2 ;;
    *) die "unsupported platform: ${os}/${arch} (published targets: linux x86_64/aarch64 musl, windows x86_64 — see install.ps1)" 2 ;;
esac

# ---- fetching ---------------------------------------------------------------------------
# Timeouts and retries are not garnish. Without them a stalled connection hangs this script
# forever with no output at all (observed), and GitHub's hosts DO throttle — raw.githubusercontent
# answers 429 under load — which a couple of spaced retries usually rides out.
#
# A token, when the environment has one, moves api.github.com from 60 requests an hour to 5000.
# It is sent ONLY to api.github.com: the release download host needs no credential and must not
# receive one.
GH_AUTH="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
auth_header() {
    case "$1" in
        https://api.github.com/*)
            if [ -n "$GH_AUTH" ]; then
                printf 'Authorization: Bearer %s' "$GH_AUTH"
            fi
            ;;
    esac
    # ALWAYS 0. Written as `[ -n "$GH_AUTH" ] && printf …`, this returned 1 whenever no token
    # was set — and under `set -e` that aborted the CALLER at `h="$(auth_header "$1")"`, so
    # every fetch failed before curl ran while `|| true` hid it. A helper whose job is to
    # produce optional output must not report absence as failure.
    return 0
}

if have curl; then
    fetch() {
        h="$(auth_header "$1")"
        if [ -n "$h" ]; then
            curl -fsSL --connect-timeout 10 --max-time 300 --retry 3 --retry-delay 2 \
                 -H "$h" "$1"
        else
            curl -fsSL --connect-timeout 10 --max-time 300 --retry 3 --retry-delay 2 "$1"
        fi
    }
    fetch_to() {
        h="$(auth_header "$1")"
        if [ -n "$h" ]; then
            curl -fsSL --connect-timeout 10 --max-time 300 --retry 3 --retry-delay 2 \
                 -H "$h" "$1" -o "$2"
        else
            curl -fsSL --connect-timeout 10 --max-time 300 --retry 3 --retry-delay 2 "$1" -o "$2"
        fi
    }
elif have wget; then
    fetch() {
        h="$(auth_header "$1")"
        if [ -n "$h" ]; then
            wget -qO- --timeout=30 --tries=3 --header="$h" "$1"
        else
            wget -qO- --timeout=30 --tries=3 "$1"
        fi
    }
    fetch_to() {
        h="$(auth_header "$1")"
        if [ -n "$h" ]; then
            wget -qO "$2" --timeout=30 --tries=3 --header="$h" "$1"
        else
            wget -qO "$2" --timeout=30 --tries=3 "$1"
        fi
    }
else
    die "need curl or wget on PATH" 3
fi

# The first `"tag_name": "…"` of a release payload.
tag_from_json() {
    tr ',' '\n' | grep '"tag_name"' | head -n 1 | sed 's/.*"tag_name": *"//; s/".*//'
}

# `sort -V` is GNU; fall back to a plain reverse sort where it is absent.
if printf '1\n' | sort -V >/dev/null 2>&1; then SORT_DESC="sort -Vr"; else SORT_DESC="sort -r"; fi

# ---- resolve the release ----------------------------------------------------------------
# `/releases/latest` skips pre-releases, and every 0.x release so far IS a pre-release, so
# resolve from the release list and take the newest entry instead.
# ---- resolve the release ----------------------------------------------------------------
# THREE sources, tried in order, because each one has a failure mode the next covers:
#
#   1. /releases/latest — the right answer once a STABLE release exists, and a 404 while every
#      release is a pre-release (as every 0.x has been), so it cannot be the only source;
#   2. the release LIST — includes pre-releases, and was the only source this script used. It
#      has been observed returning an EMPTY ARRAY while the release itself was fetchable by tag,
#      which made a perfectly good release un-installable. A miss here is not authoritative;
#   3. git TAGS — a different endpoint, which answered when the list did not. Newest v-prefixed
#      tag by version sort, then confirmed to actually carry a release.
#
# If all three come up empty the message names the escape hatch instead of guessing.
if [ -z "$VERSION" ]; then
    info "resolving the latest release…"
    VERSION="$(fetch "${API}/releases/latest" 2>/dev/null | tag_from_json)" || true
    [ -n "$VERSION" ] || VERSION="$(fetch "${API}/releases?per_page=1" 2>/dev/null | tag_from_json)" || true
    if [ -z "$VERSION" ]; then
        info "release list empty — falling back to tags"
        for candidate in $(fetch "${API}/tags?per_page=100" 2>/dev/null \
            | tr ',' '\n' | grep '"name"' | sed 's/.*"name": *"//; s/".*//' \
            | grep '^v' | $SORT_DESC); do
            if fetch "${API}/releases/tags/${candidate}" >/dev/null 2>&1; then
                VERSION="$candidate"
                break
            fi
        done
    fi
    [ -n "$VERSION" ] || die "could not resolve a release tag from ${API}
(rate-limited, or nothing published yet). Pin one explicitly:
    ... | sh -s -- --version v0.2.0-rc.1
and set GH_TOKEN to lift the API rate limit if you are retrying." 3
fi
info "version: ${VERSION}"

ASSET="sutra-${VERSION}-${TARGET}.tar.gz"

# ---- install dir ------------------------------------------------------------------------
SUDO=""
if [ -z "$INSTALL_DIR" ]; then
    if [ -w /usr/local/bin ] 2>/dev/null; then
        INSTALL_DIR=/usr/local/bin
    elif [ -t 0 ] && have sudo && [ -d /usr/local/bin ]; then
        INSTALL_DIR=/usr/local/bin
        SUDO="sudo"
    else
        INSTALL_DIR="${HOME}/.local/bin"
    fi
fi
mkdir -p "$INSTALL_DIR" 2>/dev/null || $SUDO mkdir -p "$INSTALL_DIR"

# ---- download + verify ------------------------------------------------------------------
tmp="$(mktemp -d "${TMPDIR:-/tmp}/sutra-install.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT INT TERM

info "downloading ${ASSET}…"
fetch_to "${DL}/${VERSION}/${ASSET}" "${tmp}/${ASSET}" \
    || die "download failed: ${DL}/${VERSION}/${ASSET}
(check the tag exists and this platform has an asset)" 3

if [ -n "$NO_VERIFY" ]; then
    info "checksum verification SKIPPED (SUTRA_NO_VERIFY)"
elif have sha256sum || have shasum; then
    fetch_to "${DL}/${VERSION}/SHA256SUMS" "${tmp}/SHA256SUMS" \
        || die "could not download SHA256SUMS (set SUTRA_NO_VERIFY=1 to bypass)" 3
    want="$(grep " ${ASSET}\$" "${tmp}/SHA256SUMS" | awk '{print $1}' | head -n 1)"
    [ -n "$want" ] || die "SHA256SUMS carries no entry for ${ASSET}" 4
    if have sha256sum; then
        got="$(sha256sum "${tmp}/${ASSET}" | awk '{print $1}')"
    else
        got="$(shasum -a 256 "${tmp}/${ASSET}" | awk '{print $1}')"
    fi
    [ "$want" = "$got" ] || die "CHECKSUM MISMATCH for ${ASSET}
  expected ${want}
  got      ${got}
Do not use this download." 4
    info "checksum ok"
else
    info "no sha256sum/shasum on PATH — checksum NOT verified"
fi

# ---- install ----------------------------------------------------------------------------
tar -xzf "${tmp}/${ASSET}" -C "$tmp" || die "could not extract ${ASSET}" 3
bin="${tmp}/sutra-${VERSION}-${TARGET}/sutra"
[ -f "$bin" ] || bin="$(find "$tmp" -type f -name sutra -perm -u+x | head -n 1)"
[ -f "$bin" ] || die "the archive did not contain a 'sutra' binary" 3

chmod +x "$bin"
$SUDO mv "$bin" "${INSTALL_DIR}/sutra" || die "could not install into ${INSTALL_DIR}" 1
info "installed ${INSTALL_DIR}/sutra"

# ---- report -----------------------------------------------------------------------------
printf '\n'
"${INSTALL_DIR}/sutra" --version 2>/dev/null || true
case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) ;;
    *) printf '\n%s is not on your PATH. Add it:\n    export PATH="%s:$PATH"\n' \
           "$INSTALL_DIR" "$INSTALL_DIR" ;;
esac
printf '\nNext:  sutra create app my-first-app     # then see https://sutra.startr.trade\n'
