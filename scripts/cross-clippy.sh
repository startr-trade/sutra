#!/usr/bin/env bash
# `make cross`: clippy (`--all-targets`, `-D warnings`) over sutra-cli and every workspace crate it
# depends on, for each system the CLI ships to. It catches code that only compiles on one system,
# such as a unix-only API used outside `#[cfg(unix)]`. It cannot catch a test that fails only when
# it RUNS there; the hosted `cli-portability` CI job runs the tests on macOS and Windows.
#
#   scripts/cross-clippy.sh [target ...]     default: the three targets below
#
# A target is checked only where its C toolchain exists. `ring` and `aws-lc-sys` compile C for the
# target in their build scripts, and clippy runs build scripts, so:
#   - both darwin targets need a macOS host (Xcode's clang builds either architecture);
#   - x86_64-pc-windows-msvc needs a Windows host with MSVC.
# Anywhere else the target is SKIPPED with a message and left to the hosted job. To try a target
# anyway, for example with a cross C compiler configured through CC_<target> and AR_<target>, name
# it in CROSS_FORCE (space-separated).
#
# A target that should be checked but is not installed fails, naming the `rustup target add` to
# run: the toolchain is pinned in rust/rust-toolchain.toml, so the target goes on that toolchain.
set -euo pipefail

targets=("$@")
if [ ${#targets[@]} -eq 0 ]; then
  targets=(x86_64-pc-windows-msvc aarch64-apple-darwin x86_64-apple-darwin)
fi

cd "$(dirname "$0")/../rust"
host="$(rustc -vV | sed -n 's/^host: //p')"
installed="$(rustup target list --installed)"
checked=0
skipped=0

for target in "${targets[@]}"; do
  case "$target:$host" in
    *-apple-darwin:*-apple-darwin | *-windows-msvc:*-windows-msvc) runnable=1 ;;
    *) runnable=0 ;;
  esac
  case " ${CROSS_FORCE:-} " in
    *" $target "*) runnable=1 ;;
  esac
  if [ "$runnable" = 0 ]; then
    echo "cross: SKIP $target: no C toolchain for it on $host (ring and aws-lc-sys compile C" \
      "for the target); the hosted cli-portability job checks it"
    skipped=$((skipped + 1))
    continue
  fi
  if ! grep -qx "$target" <<<"$installed"; then
    echo "cross: FAIL $target is not installed on the pinned toolchain; run, inside rust/:" \
      "rustup target add $target" >&2
    exit 1
  fi
  # sutra-cli and the workspace crates it depends on: cargo tree prints a path dependency with its
  # folder in parentheses, `/…` on unix and `C:\…` on Windows.
  packages=()
  while read -r name; do
    packages+=(-p "$name")
  done < <(cargo tree -p sutra-cli --target "$target" -e normal,build --prefix none \
    --format '{p}' | awk '/\((\/|[A-Za-z]:)/ { print $1 }' | sort -u)
  echo "cross: clippy $target over $((${#packages[@]} / 2)) packages"
  cargo clippy "${packages[@]}" --all-targets --target "$target" -- -D warnings
  checked=$((checked + 1))
done

echo "cross: $checked target(s) checked, $skipped skipped"
