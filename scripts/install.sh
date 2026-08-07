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

if have curl; then
    fetch() { curl -fsSL "$1"; }
    fetch_to() { curl -fsSL "$1" -o "$2"; }
elif have wget; then
    fetch() { wget -qO- "$1"; }
    fetch_to() { wget -qO "$2" "$1"; }
else
    die "need curl or wget on PATH" 3
fi

# ---- resolve the release ----------------------------------------------------------------
# `/releases/latest` skips pre-releases, and every 0.x release so far IS a pre-release, so
# resolve from the release list and take the newest entry instead.
if [ -z "$VERSION" ]; then
    info "resolving the latest release…"
    VERSION="$(fetch "${API}/releases?per_page=1" \
        | tr ',' '\n' | grep '"tag_name"' | head -n 1 \
        | sed 's/.*"tag_name": *"//; s/".*//')" || true
    [ -n "$VERSION" ] || die "could not resolve the latest release tag (rate-limited? pass --version <tag>)" 3
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
