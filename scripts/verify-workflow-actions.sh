#!/usr/bin/env bash
#
# Check that every workflow parses and that every `uses:` reference actually EXISTS upstream.
#
# WHY THIS EXISTS: a workflow that names a version nobody published fails at the very first
# step — `Unable to resolve action sigstore/cosign-installer@v4, unable to find version v4` —
# and the only way to learn that is to spend a release discovering it. The check costs a
# second, because an action reference is just a git ref in a public repository.
#
# The subtlety it catches: MOST actions maintain a floating major tag (`checkout@v5` follows
# v5.x), but that is a convention, not a rule, and it is not uniform even within one project.
# sigstore/cosign-installer publishes a bare `v3` and then stops — v4 exists only as v4.0.0 …
# v4.1.2. `@v4` looks exactly like `@v3` and resolves to nothing. So this asks for the EXACT
# ref rather than assuming a pattern.
#
# Uses `gh` when it is authenticated (no rate limit worth worrying about) and falls back to
# unauthenticated api.github.com, which allows 60 requests an hour — enough for this file.
#
#   bash scripts/verify-workflow-actions.sh      # or: make verify-workflows
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
missing=0
checked=0

# ---- 1. every workflow is valid YAML --------------------------------------------------------
for wf in .github/workflows/*.yml .github/workflows/*.yaml; do
    [ -e "$wf" ] || continue
    python3 -c "import sys,yaml; yaml.safe_load(open(sys.argv[1]))" "$wf" \
        || { echo "INVALID YAML: $wf" >&2; missing=$((missing+1)); }
done

# ---- 2. every action reference resolves -----------------------------------------------------
# `git/ref/tags/<t>` is an EXACT lookup. `git/matching-refs/tags/<t>` is a PREFIX match and
# would happily report `v4` as present because `v4.1.2` starts with it — which is the trap this
# script exists to avoid, so do not "simplify" it back.
api() {
    if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
        gh api "$1" --silent 2>/dev/null
    else
        curl -fsS -o /dev/null "https://api.github.com${1}" 2>/dev/null
    fi
}

refs="$(grep -rhoE 'uses: +[A-Za-z0-9._/-]+@[A-Za-z0-9._-]+' .github/workflows/ \
        | sed -E 's/uses: +//' | sort -u)"

while IFS= read -r ref; do
    [ -n "$ref" ] || continue
    # owner/repo, dropping any subdirectory (github/codeql-action/init -> github/codeql-action)
    repo="$(echo "${ref%@*}" | cut -d/ -f1,2)"
    tag="${ref##*@}"
    checked=$((checked+1))

    if [ ${#tag} -eq 40 ] && [[ "$tag" =~ ^[0-9a-f]+$ ]]; then
        kind="sha"; api "/repos/${repo}/commits/${tag}" || kind="MISSING"
    elif api "/repos/${repo}/git/ref/tags/${tag}"; then
        kind="tag"
    elif api "/repos/${repo}/git/ref/heads/${tag}"; then
        kind="branch"
    else
        kind="MISSING"
    fi

    printf '  %-46s %s\n' "$ref" "$kind"
    [ "$kind" = "MISSING" ] || continue

    missing=$((missing+1))
    # Name the versions that DO exist — the fix is almost always one of the top few.
    have="$(gh api "/repos/${repo}/tags?per_page=100" --jq '[.[].name] | .[0:5] | join(" ")' 2>/dev/null || true)"
    [ -n "$have" ] && printf '  %-46s   published: %s\n' "" "$have"
done <<< "$refs"

echo
if [ "$missing" -eq 0 ]; then
    echo "${checked} action references, all resolvable."
else
    echo "${missing} unresolvable reference(s) of ${checked} — these fail a workflow at its first step." >&2
    exit 1
fi
