#!/usr/bin/env bash
# Uploads a directory's files to a GitHub Release without ever taking a good asset offline.
#
# `gh release upload --clobber` deletes each asset before uploading its replacement, so a re-run
# during a GitHub outage can remove files that were fine. Here a file already published with the
# same digest is left alone; a changed one goes up under a temporary name and only then replaces
# the old one. A failed upload leaves a `starter` asset that holds its name until deleted, so those
# are cleared first and between attempts.
#
#   usage: release-upload-assets.sh <tag> <dir>     (needs GH_TOKEN; run inside the repo checkout)
set -euo pipefail

tag=$1
dir=$2
repo=${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}
attempts=${UPLOAD_ATTEMPTS:-5}
backoff=${UPLOAD_BACKOFF_SECONDS:-20}
suffix=.uploading

retry() {
  local n=1
  until "$@"; do
    if [ "$n" -ge "$attempts" ]; then
      echo "giving up after $n attempts: $*" >&2
      return 1
    fi
    echo "attempt $n failed, retrying in $((backoff * n))s: $*" >&2
    sleep "$((backoff * n))"
    n=$((n + 1))
  done
}

release_id=$(retry gh release view "$tag" --json databaseId --jq .databaseId)

# One asset per line: name, state, id, size, digest. REST lists `starter` assets; `gh release view` hides them.
list_assets() {
  gh api --paginate "repos/$repo/releases/$release_id/assets" \
    --jq '.[] | [.name, .state, (.id | tostring), (.size | tostring), (.digest // "-")] | @tsv'
}

field() { # <listing> <name> <column>
  awk -F'\t' -v n="$2" -v c="$3" '$1 == n { print $c; exit }' <<< "$1"
}

delete_asset() { retry gh api -X DELETE "repos/$repo/releases/assets/$1" > /dev/null; }

clear_unfinished() { # <asset name>
  local listing id
  listing=$(retry list_assets)
  id=$(field "$listing" "$1" 3)
  if [ -n "$id" ] && [ "$(field "$listing" "$1" 2)" != uploaded ]; then
    delete_asset "$id"
  fi
}

upload_as() { # <file> <asset name>
  local tmp rc
  tmp=$(mktemp -d)
  cp -- "$1" "$tmp/$2"
  rc=0
  try_upload() { clear_unfinished "$2" && gh release upload "$tag" "$tmp/$2"; }
  retry try_upload "$1" "$2" || rc=$?
  rm -rf -- "$tmp"
  return "$rc"
}

for file in "$dir"/*; do
  name=$(basename -- "$file")
  digest="sha256:$(sha256sum -- "$file" | cut -d' ' -f1)"
  size=$(stat -c %s -- "$file")
  listing=$(retry list_assets)
  state=$(field "$listing" "$name" 2)

  # A previous run may have died part way: its temporary copy is not trusted.
  temp_id=$(field "$listing" "$name$suffix" 3)
  if [ -n "$temp_id" ]; then
    delete_asset "$temp_id"
  fi

  if [ "$state" = uploaded ] && [ "$(field "$listing" "$name" 4)" = "$size" ] \
    && [ "$(field "$listing" "$name" 5)" = "$digest" ]; then
    echo "unchanged: $name"
  elif [ "$state" = uploaded ]; then
    upload_as "$file" "$name$suffix"
    listing=$(retry list_assets)
    delete_asset "$(field "$listing" "$name" 3)"
    retry gh api -X PATCH "repos/$repo/releases/assets/$(field "$listing" "$name$suffix" 3)" \
      -f name="$name" > /dev/null
    echo "replaced: $name"
  else
    upload_as "$file" "$name"
    echo "uploaded: $name"
  fi
done

# A step that only reports success after reading the result back.
listing=$(retry list_assets)
bad=0
for file in "$dir"/*; do
  name=$(basename -- "$file")
  if [ "$(field "$listing" "$name" 2)" != uploaded ] \
    || [ "$(field "$listing" "$name" 4)" != "$(stat -c %s -- "$file")" ]; then
    echo "not published correctly: $name" >&2
    bad=1
  fi
done
exit "$bad"
