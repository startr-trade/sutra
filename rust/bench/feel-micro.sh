#!/usr/bin/env bash
# FEEL micro-benches — thin wrapper over the criterion suite in sutra-feel/benches/.
#
# Unlike the container benches (cold-start / peak-rss / sustained-rps), these need no engine
# and no docker — pure CPU, runnable anywhere the workspace builds. This is the one part of
# the GA bench matrix that can be populated inside a worktree.
#
# Output: criterion writes per-benchmark estimates to rust/target/criterion/.
set -euo pipefail
BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${BENCH_DIR}/.."   # rust/
exec cargo bench -p sutra-feel "$@"
