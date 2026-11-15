#!/usr/bin/env bash
# Drift gate for the authored-artifact pages: `sutra generate docs --check` for every
# <catalog>/authored-artifacts/<name> subtree, against its input folder examples/<name>.
# A subtree whose input folder no longer exists cannot be regenerated, so it is skipped with a
# note rather than failed. No authored-artifacts folder at all (a tree that does not carry the
# catalog) checks nothing.
#
# Usage: scripts/catalog-docs-check.sh <catalog-dir>   (run from the repository root)
set -euo pipefail

catalog="${1:?usage: $0 <catalog-dir>}"
root="$catalog/authored-artifacts"
if [ ! -d "$root" ]; then
  echo "catalog-docs-check: no $root, nothing to check"
  exit 0
fi

sutra=(cargo run -q --profile release-it --manifest-path rust/Cargo.toml -p sutra-cli --)
failed=0
checked=0
for dir in "$root"/*/; do
  name="$(basename "$dir")"
  input="examples/$name"
  if [ ! -d "$input" ]; then
    echo "catalog-docs-check: skipping $dir: its input $input no longer exists"
    continue
  fi
  checked=$((checked + 1))
  if ! "${sutra[@]}" generate docs --input "$input" --output "${dir%/}" --check; then
    failed=1
  fi
done
echo "catalog-docs-check: $checked subtree(s) checked"
exit "$failed"
