#!/usr/bin/env bash
#
# Install a pre-commit Git hook.
#
# Note: the Java depcat-gen catalog generator was RETIRED and its
# artifact-documentation pages are a frozen snapshot. The former pre-commit
# behaviour (auto-regenerating the Java catalog when a frozen-contract artifact
# was staged) no longer applies. The single live catalog generator is the Rust
# `sutra-catalog-gen` library (shipped as `sutra catalog`), regenerated on demand
# with `make catalog` (a release build,
# too heavy for a commit hook). This installer therefore now writes a pass-through
# hook; it is kept so a future project-specific check has a place to live.
#
# The hook lives in .git/hooks/pre-commit. Re-run this script to reinstall
# after a re-clone (.git/ is not committed).
#
# Usage:
#   scripts/install-pre-commit-hook.sh           # install
#   scripts/install-pre-commit-hook.sh --uninstall  # remove

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HOOK_PATH="$REPO_ROOT/.git/hooks/pre-commit"

if [[ "${1:-}" == "--uninstall" ]]; then
    if [[ -f "$HOOK_PATH" ]]; then
        rm "$HOOK_PATH"
        echo "Removed $HOOK_PATH"
    else
        echo "No hook installed at $HOOK_PATH (nothing to do)"
    fi
    exit 0
fi

if [[ ! -d "$REPO_ROOT/.git" ]]; then
    echo "Error: $REPO_ROOT/.git does not exist — not a git repo (or no commits yet)" >&2
    exit 1
fi

mkdir -p "$REPO_ROOT/.git/hooks"

cat > "$HOOK_PATH" <<'HOOK'
#!/usr/bin/env bash
#
# Sutra pre-commit hook.
#
# The Java depcat-gen catalog auto-regeneration was retired (the catalog is
# now Rust-only via `make catalog`, a release build too heavy for a commit hook).
# No active checks run here; the Rust workspace is guarded by `make test` / `make lint`.
# Bypass any future checks per-commit with `git commit --no-verify`.

exit 0
HOOK

chmod +x "$HOOK_PATH"

echo "Installed pre-commit hook at $HOOK_PATH (pass-through; catalog auto-regen retired)."
echo "  Uninstall:  scripts/install-pre-commit-hook.sh --uninstall"
