#!/usr/bin/env bash
# A/B load comparison: the candidate tree (this checkout) against a base release, built and run
# on the SAME machine in the same job, so the machine cancels out and only the code differs.
#
# Every load test prints `PERF {"test":…,"metric":…,"value":…}` lines on stdout. This script
# runs each test alternately on base and candidate, RUNS times each, takes the median of every
# metric on each side and fails when the candidate is worse than the base by more than
# PERF_AB_TOLERANCE. A metric whose name ends in `_ms` is lower-is-better; every other metric
# is higher-is-better. A metric the base never printed (a base release from before the load
# tests printed PERF lines) is reported as "no baseline" and passes.
#
# A metric outside the tolerance gets one automatic re-run of its test (RUNS more runs per side,
# compared on their own) before the script fails, so a single noisy run does not fail the job.
#
# Inputs (environment):
#   BASE               base revision (default: the previous tag, `git describe --tags --abbrev=0 HEAD^`)
#   RUNS               runs per side per test (default 3)
#   PERF_TESTS         space-separated test filters (default: "timer_fire_lag outbox_burst")
#   PERF_AB_TOLERANCE  allowed relative regression (default 0.2)
#   PERF_AB_SELFTEST=1 check the comparison itself on two fixed inputs, build and run nothing
#
# Output: a Markdown table on $GITHUB_STEP_SUMMARY when set, otherwise on stdout.
set -euo pipefail

TOL="${PERF_AB_TOLERANCE:-0.2}"
RUNS="${RUNS:-3}"
PERF_TESTS="${PERF_TESTS:-timer_fire_lag outbox_burst}"

log() { echo "perf-ab: $*" >&2; }

# Turn `PERF {...}` records on stdin into `test<TAB>metric<TAB>value` rows. Key order in the JSON
# object does not matter, and neither does text before the record on its line (with
# `--nocapture` the test harness prints `test <name> ... ` on the same line first).
perf_rows() {
  sed -n 's/.*PERF \({.*}\)[[:space:]]*$/\1/p' | while IFS= read -r json; do
    t=$(printf '%s' "$json" | sed -n 's/.*"test"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
    m=$(printf '%s' "$json" | sed -n 's/.*"metric"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
    v=$(printf '%s' "$json" | sed -n 's/.*"value"[[:space:]]*:[[:space:]]*\([-+0-9.eE]*\).*/\1/p')
    [ -n "$t" ] && [ -n "$m" ] && [ -n "$v" ] && printf '%s\t%s\t%s\n' "$t" "$m" "$v"
  done
  return 0
}

# compare BASE_ROWS CAND_ROWS TABLE_OUT FAILED_OUT — exit 1 when any metric regressed past TOL.
# TABLE_OUT gets the Markdown table rows; FAILED_OUT the tests with a regressed metric.
compare() {
  awk -F'\t' -v tol="$TOL" -v table="$3" -v failed="$4" '
    function median(list,   n, a, i, j, t) {
      n = split(list, a, " ")
      for (i = 2; i <= n; i++) { t = a[i]; for (j = i - 1; j >= 1 && a[j] > t; j--) a[j + 1] = a[j]; a[j + 1] = t }
      if (n % 2) return a[(n + 1) / 2]
      return (a[n / 2] + a[n / 2 + 1]) / 2
    }
    function spread(list,   n, a, i, lo, hi) {
      n = split(list, a, " "); lo = a[1]; hi = a[1]
      for (i = 2; i <= n; i++) { if (a[i] < lo) lo = a[i]; if (a[i] > hi) hi = a[i] }
      return sprintf("%.4g..%.4g", lo, hi)
    }
    {
      key = $1 "\t" $2
      # By file name, not by file order: an empty base file (no baseline) reads no line at all.
      if (FILENAME == ARGV[1]) base[key] = base[key] " " $3
      else { cand[key] = cand[key] " " $3; keys[key] = 1 }
    }
    END {
      bad = 0
      printf "" > table; printf "" > failed
      for (key in keys) {
        split(key, k, "\t")
        c = median(cand[key])
        if (!(key in base)) {
          printf "| %s | %s | no baseline | %.4g (%s) | — | pass |\n", k[1], k[2], c, spread(cand[key]) >> table
          continue
        }
        b = median(base[key])
        lower_better = (k[2] ~ /_ms$/)
        if (b == 0) worse = 0
        else if (lower_better) worse = (c - b) / b
        else worse = (b - c) / b
        verdict = (worse > tol) ? "FAIL" : "pass"
        if (verdict == "FAIL") { bad = 1; print k[1] >> failed }
        printf "| %s | %s | %.4g (%s) | %.4g (%s) | %+.1f%% | %s |\n", k[1], k[2], b, spread(base[key]), c, spread(cand[key]), -100 * worse, verdict >> table
      }
      exit bad
    }
  ' "$1" "$2"
}

# report TABLE_ROWS TITLE — print the table where the job summary is read.
report() {
  local out="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
  {
    echo "### $2"
    echo
    echo "Tolerance ${TOL}; median of each side, spread in brackets; change is the candidate's, positive is better."
    echo
    echo "| test | metric | base | candidate | change | verdict |"
    echo "|---|---|---|---|---|---|"
    sort "$1"
    echo
  } >>"$out"
}

if [ "${PERF_AB_SELFTEST:-0}" = "1" ]; then
  dir=$(mktemp -d)
  trap 'rm -rf "$dir"' EXIT
  printf 'test t ... PERF {"test":"t","metric":"delivered_per_s","value":%s}\nPERF {"metric":"fast_p99_ms","test":"u","value":%s}\n' \
    100 50 101 49 99 51 | perf_rows >"$dir/base"
  cp "$dir/base" "$dir/equal"
  printf 'PERF {"test":"t","metric":"delivered_per_s","value":%s}\nPERF {"test":"u","metric":"fast_p99_ms","value":%s}\n' \
    70 65 71 64 69 66 | perf_rows >"$dir/worse"
  [ "$(wc -l <"$dir/base")" -eq 6 ] || { log "self-test: PERF lines did not parse"; exit 1; }
  if ! compare "$dir/base" "$dir/equal" "$dir/t1" "$dir/f1"; then
    report "$dir/t1" "self-test: equal"
    log "self-test FAILED: equal inputs were reported as a regression"
    exit 1
  fi
  if compare "$dir/base" "$dir/worse" "$dir/t2" "$dir/f2"; then
    report "$dir/t2" "self-test: 30% worse"
    log "self-test FAILED: a 30% regression passed"
    exit 1
  fi
  [ "$(sort -u "$dir/f2" | tr '\n' ' ')" = "t u " ] || { log "self-test FAILED: wrong failing tests: $(cat "$dir/f2")"; exit 1; }
  : >"$dir/none"
  if ! compare "$dir/none" "$dir/worse" "$dir/t3" "$dir/f3" || [ "$(grep -c 'no baseline' "$dir/t3")" -ne 2 ]; then
    report "$dir/t3" "self-test: no baseline"
    log "self-test FAILED: a base without PERF lines must report 'no baseline' and pass"
    exit 1
  fi
  log "self-test passed (equal: pass, 30% worse: fail on both metrics, no baseline: pass)"
  exit 0
fi

repo=$(git rev-parse --show-toplevel)
cd "$repo"
BASE="${BASE:-$(git describe --tags --abbrev=0 HEAD^)}"
work=$(mktemp -d)
if [ -n "${RUNNER_TEMP:-}" ]; then
  base_tree="$RUNNER_TEMP/perf-ab-base"
else
  base_tree="$repo/.claude/worktrees/perf-ab-base"
fi
# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() {
  git -C "$repo" worktree remove --force "$base_tree" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

log "base $BASE vs candidate $(git rev-parse --short HEAD), $RUNS runs per side, tests: $PERF_TESTS"
git worktree remove --force "$base_tree" >/dev/null 2>&1 || true
git worktree add --detach "$base_tree" "$BASE" >/dev/null

# build TREE TARGET_DIR — build the sutra-engine `all` test binary and print its path.
build() {
  local out
  out=$(cd "$1/rust" && CARGO_TARGET_DIR="$2" cargo test -p sutra-engine --test all --no-run 2>&1) || {
    printf '%s\n' "$out" | tail -n 30 >&2
    return 1
  }
  printf '%s\n' "$out" | sed -n 's/.*Executable tests\/all.rs (\(.*\))$/\1/p' | tail -n 1
}

cand_target="${CARGO_TARGET_DIR:-$repo/rust/target}"
case "$cand_target" in /*) ;; *) cand_target="$repo/rust/$cand_target" ;; esac
log "building candidate"
cand_bin=$(build "$repo" "$cand_target")
log "building base"
base_bin=$(build "$base_tree" "$base_tree/rust/target")
case "$cand_bin" in /*) ;; *) cand_bin="$repo/rust/$cand_bin" ;; esac
case "$base_bin" in /*) ;; *) base_bin="$base_tree/rust/$base_bin" ;; esac
[ -x "$cand_bin" ] && [ -x "$base_bin" ] || { log "could not locate the test binaries"; exit 1; }

# run_side BIN TREE TEST OUT — one run of one test; PERF rows appended to OUT. A test that fails
# its own assertions still printed its figures first, and this script is the deciding check
# for exactly that case, so a failing run only warns; its figures stay in the comparison.
run_side() {
  local status=0 side=base
  [ "$2" = "$repo" ] && side=candidate
  (cd "$2/rust/crates/sutra-engine" && "$1" --ignored --test-threads=1 --nocapture "$3") \
    >"$work/run.log" 2>&1 || status=$?
  perf_rows <"$work/run.log" >>"$4"
  sed -n 's/.*\(PERF {.*}\).*/\1/p' "$work/run.log" | sed "s/^/perf-ab: $side: /" >&2
  if [ "$status" -ne 0 ]; then
    grep -A2 -m2 "panicked at" "$work/run.log" >&2 || true
    log "$side run of $3 failed its own assertions (exit $status); its figures are kept"
  fi
}

# measure TESTS SUFFIX — RUNS alternating runs per side of every test in TESTS.
measure() {
  : >"$work/base$2"
  : >"$work/cand$2"
  for i in $(seq 1 "$RUNS"); do
    for t in $1; do
      log "run $i/$RUNS: $t (base)"
      run_side "$base_bin" "$base_tree" "$t" "$work/base$2"
      log "run $i/$RUNS: $t (candidate)"
      run_side "$cand_bin" "$repo" "$t" "$work/cand$2"
    done
  done
}

measure "$PERF_TESTS" ""
if [ ! -s "$work/cand" ]; then
  log "FAIL: the candidate printed no PERF lines — the load tests did not run or lost their perf_line calls"
  exit 1
fi
if compare "$work/base" "$work/cand" "$work/table" "$work/failed"; then
  report "$work/table" "perf A/B: $BASE vs $(git rev-parse --short HEAD)"
  log "pass"
  exit 0
fi
report "$work/table" "perf A/B: $BASE vs $(git rev-parse --short HEAD) (first pass)"
retry=$(sort -u "$work/failed" | tr '\n' ' ')
log "outside tolerance: $retry— re-running once"
measure "$retry" ".retry"
if compare "$work/base.retry" "$work/cand.retry" "$work/table.retry" "$work/failed.retry"; then
  report "$work/table.retry" "perf A/B re-run: within tolerance"
  log "pass on re-run"
  exit 0
fi
report "$work/table.retry" "perf A/B re-run: still outside tolerance"
log "FAIL: the candidate is slower than $BASE by more than ${TOL} on: $(sort -u "$work/failed.retry" | tr '\n' ' ')"
exit 1
