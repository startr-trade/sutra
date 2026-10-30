#!/usr/bin/env bash
#
# relay-bench.sh — the full durable relay workload (fixtures/relay + profiles/relay.js), run
# natively against a throwaway PostgreSQL cluster, with the per-settle database cost measured.
#
# Unlike run.sh (which only points k6 at an already-running deployment), this script owns the
# whole system under test, because the numbers it reports need the database to itself:
#
#   1. a throwaway PostgreSQL cluster (initdb into a temp dir, DURABLE settings: fsync,
#      synchronous_commit and full_page_writes stay on) with pg_stat_statements loaded;
#   2. the callback sink (sink/, built on first use);
#   3. the engine binary, natively, with the relay package sealed by the `sutra` CLI;
#   4. a warm-up, then k6 at RATE for DURATION, then a drain until every order has called back.
#
# It prints one markdown block: settles/s, settle latency p50/p99 (sink-measured: callback
# received minus intake sent), intake latency, commits and WAL flushes per settle, engine CPU,
# PostgreSQL CPU, CPU milliseconds per settle (engine + PostgreSQL), and the top statements by
# calls. Everything it started is stopped on exit.
#
# Usage:
#   tools/sutra-load-test/relay-bench.sh                      # 20/s for 60 s
#   RATE=100 DURATION=60s tools/sutra-load-test/relay-bench.sh
#   ENGINE_ENV="SUTRA_OUTBOX_WORKERS=8" tools/sutra-load-test/relay-bench.sh
#
# Env:
#   RATE (20)  DURATION (60s)  WARMUP_RATE (10)  WARMUP_DURATION (10s)
#   DRAIN_IDLE_S (30)  DRAIN_MAX_S (600)
#   SUTRA_ENGINE_BIN  (rust/target/release/sutra-engine)   SUTRA_CLI (rust/target/release/sutra)
#   PG_BIN_DIR (/usr/lib/postgresql/18/bin)   PG_SHARED_BUFFERS (2GB)   PG_COMMIT_DELAY (100)
#   PG_COMMIT_SIBLINGS (4)   PG_MAX_CONNECTIONS (200)
#   ENGINE_ENV   extra "KEY=VALUE ..." for the engine (e.g. pool, outbox and cache knobs)
#   CACHE_TTL    turn on the `refdata` store's read cache with this ttl (e.g. PT0S, PT5S)
#   KEEP=1       leave the cluster and engine running after the run (prints how to stop them)
#   OUT          write the markdown block to this file as well as stdout
#   PG_EXTRA_CONF  extra postgresql.conf lines (e.g. "session_preload_libraries = 'auto_explain'")
#   PG_LOG_OUT   copy the PostgreSQL server log (lock waits over 1 s, deadlocks) to this file on exit
#   PERF_DIR     record a CPU profile of the engine (and Postgres) over the measured run into
#                this directory: perf.data, a flamegraph SVG and a top-symbol summary. Needs
#                `perf` usable by your user (kernel.perf_event_paranoid <= 1) and inferno
#                (`cargo install inferno`); build the engine with frame pointers for whole stacks.
#
# Exit codes: 0 every order settled exactly once; 1 lost or duplicated settles; 3 harness error.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RATE="${RATE:-20}"
DURATION="${DURATION:-60s}"
WARMUP_RATE="${WARMUP_RATE:-10}"
WARMUP_DURATION="${WARMUP_DURATION:-10s}"
DRAIN_IDLE_S="${DRAIN_IDLE_S:-30}"
DRAIN_MAX_S="${DRAIN_MAX_S:-600}"
SUTRA_ENGINE_BIN="${SUTRA_ENGINE_BIN:-$REPO_ROOT/rust/target/release/sutra-engine}"
SUTRA_CLI="${SUTRA_CLI:-$REPO_ROOT/rust/target/release/sutra}"
PG_BIN_DIR="${PG_BIN_DIR:-/usr/lib/postgresql/18/bin}"
PG_SHARED_BUFFERS="${PG_SHARED_BUFFERS:-2GB}"
PG_COMMIT_DELAY="${PG_COMMIT_DELAY:-100}"
PG_COMMIT_SIBLINGS="${PG_COMMIT_SIBLINGS:-4}"
PG_MAX_CONNECTIONS="${PG_MAX_CONNECTIONS:-200}"
ENGINE_ENV="${ENGINE_ENV:-}"
API_KEY="relay-bench-key"

log() { printf '[relay-bench] %s\n' "$*" >&2; }
die() { printf '[relay-bench] ERROR: %s\n' "$*" >&2; exit 3; }

for tool in k6 jq curl python3 cargo; do
    command -v "$tool" >/dev/null 2>&1 || die "missing prerequisite: $tool"
done
[ -x "$PG_BIN_DIR/initdb" ] || die "no initdb under PG_BIN_DIR=$PG_BIN_DIR"
[ -x "$SUTRA_ENGINE_BIN" ] || die "engine binary not found: $SUTRA_ENGINE_BIN (cargo build --release -p sutra-dist)"
[ -x "$SUTRA_CLI" ] || die "sutra CLI not found: $SUTRA_CLI (cargo build --release -p sutra-cli)"

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

# The cluster must live on the disk under test: on tmpfs every fsync is free and the WAL numbers
# mean nothing. results/ is gitignored and sits on the repository's disk; RELAY_BENCH_DIR moves it.
# (k6 may also be a confined snap that cannot see /tmp, another reason to stay out of it.)
BENCH_PARENT="${RELAY_BENCH_DIR:-$SCRIPT_DIR/results}"
mkdir -p "$BENCH_PARENT"
if [ "$(stat -f -c %T "$BENCH_PARENT")" = "tmpfs" ]; then
    die "RELAY_BENCH_DIR=$BENCH_PARENT is tmpfs: fsync would be free and the numbers meaningless"
fi
WORK="$(mktemp -d "$BENCH_PARENT/relay-bench.XXXXXX")"
K6DIR="$WORK/k6"
mkdir -p "$K6DIR"
PGDATA="$WORK/pg"
PG_PORT="$(free_port)"
ENGINE_PORT="$(free_port)"
ENGINE_PID=""
SINK_PID=""

cleanup() {
    if [ "${KEEP:-0}" = "1" ]; then
        log "KEEP=1: engine pid $ENGINE_PID, sink pid $SINK_PID, cluster $PGDATA (stop: $PG_BIN_DIR/pg_ctl -D $PGDATA stop)"
        return
    fi
    [ -n "$ENGINE_PID" ] && kill "$ENGINE_PID" 2>/dev/null && wait "$ENGINE_PID" 2>/dev/null
    [ -n "$SINK_PID" ] && kill "$SINK_PID" 2>/dev/null && wait "$SINK_PID" 2>/dev/null
    [ -f "$PGDATA/postmaster.pid" ] && "$PG_BIN_DIR/pg_ctl" -D "$PGDATA" -m fast stop >/dev/null 2>&1
    [ -n "${PG_LOG_OUT:-}" ] && [ -f "$WORK/pg.log" ] && cp "$WORK/pg.log" "$PG_LOG_OUT"
    rm -rf "$WORK"
}
trap cleanup EXIT

# ---- 1. PostgreSQL (durable) -------------------------------------------------------------------
log "initdb $PGDATA (port $PG_PORT)"
"$PG_BIN_DIR/initdb" -D "$PGDATA" -U sutra --auth=trust >"$WORK/initdb.log" 2>&1 || die "initdb failed ($WORK/initdb.log)"
cat >>"$PGDATA/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = $PG_PORT
unix_socket_directories = ''
max_connections = $PG_MAX_CONNECTIONS
shared_buffers = $PG_SHARED_BUFFERS
wal_buffers = 64MB
fsync = on
synchronous_commit = on
full_page_writes = on
commit_delay = $PG_COMMIT_DELAY
commit_siblings = $PG_COMMIT_SIBLINGS
max_wal_size = 8GB
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9
effective_io_concurrency = 200
random_page_cost = 1.1
work_mem = 16MB
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.max = 5000
pg_stat_statements.track = all
track_io_timing = on
track_wal_io_timing = on
log_lock_waits = on
log_line_prefix = '%m [%p] '
${PG_EXTRA_CONF:-}
EOF
"$PG_BIN_DIR/pg_ctl" -D "$PGDATA" -l "$WORK/pg.log" -w start >/dev/null || die "postgres did not start ($WORK/pg.log)"
PSQL=(psql -h 127.0.0.1 -p "$PG_PORT" -U sutra -X -q -t -A)
"${PSQL[@]}" -d postgres -c "CREATE DATABASE sutra" -c "CREATE DATABASE relaydata" || die "createdb failed"
"${PSQL[@]}" -d sutra -c "CREATE EXTENSION pg_stat_statements" || die "pg_stat_statements unavailable"

# ---- 2. the sink --------------------------------------------------------------------------------
SINK_BIN="$SCRIPT_DIR/sink/target/release/sutra-load-sink"
if [ ! -x "$SINK_BIN" ]; then
    log "building the sink"
    (cd "$SCRIPT_DIR/sink" && cargo build --release --quiet) || die "sink build failed"
fi
"$SINK_BIN" >"$WORK/sink.log" 2>&1 &
SINK_PID=$!
for _ in $(seq 1 50); do grep -q "listening on" "$WORK/sink.log" && break; sleep 0.1; done
SINK_ADDR="$(sed -n 's/^listening on //p' "$WORK/sink.log")"
[ -n "$SINK_ADDR" ] || die "sink did not start"
SINK="http://$SINK_ADDR"

# ---- 3. the engine ------------------------------------------------------------------------------
mkdir -p "$WORK/deployments" "$WORK/secrets"
PKG_SRC="$SCRIPT_DIR/fixtures/relay"
if [ -n "${CACHE_TTL:-}" ]; then
    cp -r "$PKG_SRC" "$WORK/pkg-src"
    printf '    cache:\n      enabled: true\n      maxEntries: 1000\n      ttl: %s\n' "$CACHE_TTL" \
        >>"$WORK/pkg-src/datastores.yaml"
    PKG_SRC="$WORK/pkg-src"
fi
"$SUTRA_CLI" package "$PKG_SRC" --out "$WORK/deployments" >"$WORK/package.log" 2>&1 \
    || die "packaging failed ($WORK/package.log)"
log "engine on :$ENGINE_PORT ($ENGINE_ENV)"
(
    export SUTRA_DATASOURCE_URL="postgres://127.0.0.1:$PG_PORT/sutra"
    export SUTRA_DATASOURCE_USERNAME=sutra SUTRA_DATASOURCE_PASSWORD=sutra
    export SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED=false
    export SUTRA_DEPLOYMENTS_DIR="$WORK/deployments" SUTRA_SECRETS_DIR="$WORK/secrets"
    export SUTRA_HTTP_PORT="$ENGINE_PORT"
    export SUTRA_DB_MIGRATIONS="$REPO_ROOT/rust/crates/sutra-persistence/migrations"
    export RELAY_BENCH_API_KEY="$API_KEY" RELAY_BENCH_CALLBACK_URL="$SINK/callback"
    export RELAY_DB_URL="postgres://127.0.0.1:$PG_PORT/relaydata" RELAY_DB_USER=sutra RELAY_DB_PASSWORD=sutra
    export RUST_LOG="${RUST_LOG:-warn}"
    for kv in $ENGINE_ENV; do export "${kv?}"; done
    exec "$SUTRA_ENGINE_BIN"
) >"$WORK/engine.log" 2>&1 &
ENGINE_PID=$!
ready=0
for _ in $(seq 1 600); do
    count="$(curl -s "http://127.0.0.1:$ENGINE_PORT/sutra/health/ready" 2>/dev/null \
        | jq -r '.checks[0].data.deployments // -1' 2>/dev/null || echo -1)"
    if [ "${count:--1}" -ge 1 ] 2>/dev/null; then ready=1; break; fi
    kill -0 "$ENGINE_PID" 2>/dev/null || break
    sleep 0.1
done
[ "$ready" = 1 ] || { tail -20 "$WORK/engine.log" >&2; die "engine not ready"; }

run_k6() { # rate duration run-id summary-file
    k6 run --quiet --no-color -e BASE_URL="http://127.0.0.1:$ENGINE_PORT" -e API_KEY="$API_KEY" \
        -e RATE="$1" -e DURATION="$2" -e RUN_ID="$3" --summary-export="$4" \
        "$SCRIPT_DIR/profiles/relay.js" >"$K6DIR/k6-$3.log" 2>&1
}

wait_settled() { # expected
    local expected="$1" last=-1 idle=0 waited=0 settled
    while :; do
        settled="$(curl -s "$SINK/stats" | jq -r '.settled // 0' 2>/dev/null)"
        settled="${settled:-0}"
        [ "$settled" -ge "$expected" ] && return 0
        if [ "$settled" = "$last" ]; then idle=$((idle + 1)); else idle=0; last="$settled"; fi
        [ "$idle" -ge "$DRAIN_IDLE_S" ] && return 1
        [ "$waited" -ge "$DRAIN_MAX_S" ] && return 1
        sleep 1; waited=$((waited + 1))
    done
}

# ---- 4. warm-up, measured run, drain -----------------------------------------------------------
log "warm-up ${WARMUP_RATE}/s x $WARMUP_DURATION"
run_k6 "$WARMUP_RATE" "$WARMUP_DURATION" warmup "$K6DIR/warmup.json"
wait_settled "$(jq -r '.metrics.iterations.count // 0' "$K6DIR/warmup.json")" || log "warm-up did not fully settle"
curl -s -X POST "$SINK/reset" >/dev/null

stats_sql="SELECT (SELECT sum(xact_commit) FROM pg_stat_database WHERE datname IN ('sutra','relaydata')),
  (SELECT coalesce(sum(fsyncs),0) FROM pg_stat_io WHERE object = 'wal'),
  (SELECT wal_bytes FROM pg_stat_wal),
  (SELECT coalesce(sum(fsync_time),0) FROM pg_stat_io WHERE object = 'wal')"
cpu_ticks() { awk '{print $14 + $15}' "/proc/$ENGINE_PID/stat"; }
# PostgreSQL's CPU: utime+stime of the postmaster and every backend, as "pid:ticks" pairs. A
# backend that exits mid-run takes its ticks with it, so pooled (long-lived) connections are what
# this measures well; the engine's pools keep theirs open.
pg_ticks() {
    local pm pid
    pm="$(head -1 "$PGDATA/postmaster.pid")"
    for pid in "$pm" $(pgrep -P "$pm"); do
        [ -r "/proc/$pid/stat" ] && printf '%s:%s ' "$pid" "$(awk '{print $14 + $15}' "/proc/$pid/stat" 2>/dev/null)"
    done
}
read -r C0 F0 B0 S0 < <("${PSQL[@]}" -d sutra -F ' ' -c "$stats_sql")
"${PSQL[@]}" -d sutra -c "SELECT pg_stat_statements_reset()" >/dev/null
DL0="$("${PSQL[@]}" -d sutra -c "SELECT deadlocks FROM pg_stat_database WHERE datname = 'sutra'")"
T0="$(cpu_ticks)"; P0="$(pg_ticks)"; W0="$(date +%s.%N)"

PERF_PIDS=()
if [ -n "${PERF_DIR:-}" ]; then
    mkdir -p "$PERF_DIR"
    perf record -F 499 -g -p "$ENGINE_PID" -o "$PERF_DIR/engine.perf.data" >"$PERF_DIR/engine.perf.log" 2>&1 &
    PERF_PIDS+=($!)
    PG_PIDS="$(pgrep -d, -P "$(head -1 "$PGDATA/postmaster.pid")")"
    perf record -F 499 -g -p "$PG_PIDS" -o "$PERF_DIR/postgres.perf.data" >"$PERF_DIR/postgres.perf.log" 2>&1 &
    PERF_PIDS+=($!)
fi
log "measured run ${RATE}/s x $DURATION"
run_k6 "$RATE" "$DURATION" run "$K6DIR/run.json"
SENT="$(jq -r '.metrics.iterations.count // 0' "$K6DIR/run.json")"
if wait_settled "$SENT"; then drained=yes; else drained=no; fi

for pid in "${PERF_PIDS[@]}"; do kill -INT "$pid" 2>/dev/null; done
for pid in "${PERF_PIDS[@]}"; do wait "$pid" 2>/dev/null; done
read -r C1 F1 B1 S1 < <("${PSQL[@]}" -d sutra -F ' ' -c "$stats_sql")
T1="$(cpu_ticks)"; P1="$(pg_ticks)"; W1="$(date +%s.%N)"
SINK_STATS="$(curl -s "$SINK/stats")"
SETTLED="$(jq -r .settled <<<"$SINK_STATS")"
DUPS="$(jq -r .duplicates <<<"$SINK_STATS")"
HZ="$(getconf CLK_TCK)"

report="$(python3 - "$SINK_STATS" "$K6DIR/run.json" "$SENT" "$C0" "$C1" "$F0" "$F1" "$B0" "$B1" \
    "$T0" "$T1" "$HZ" "$W0" "$W1" "$RATE" "$DURATION" "$drained" "$ENGINE_ENV" "$S0" "$S1" "$P0" "$P1" <<'PY'
import json, sys
(sink, run, sent, c0, c1, f0, f1, b0, b1, t0, t1, hz, w0, w1, rate, duration, drained, env, s0, s1, p0, p1) = sys.argv[1:]
def ticks(pairs):
    return {k: float(v) for k, v in (p.split(":") for p in pairs.split() if ":" in p and p.split(":")[1])}
pg0, pg1 = ticks(p0), ticks(p1)
pg_ticks = sum(v - pg0.get(k, 0.0) for k, v in pg1.items())
s = json.loads(sink); r = json.load(open(run))
sent = int(sent); settled = s["settled"]
# Throughput: settles over the time from the measured run's start to its last settle.
span = max((s["lastAtMs"] or 0) - float(w0) * 1000.0, 1) / 1000.0
per = lambda a, b: (float(b) - float(a)) / max(settled, 1)
cpu = (float(t1) - float(t0)) / float(hz) / max(float(w1) - float(w0), 1e-9)
d = r["metrics"].get("http_req_duration", {})
pg_cpu = pg_ticks / float(hz) / max(float(w1) - float(w0), 1e-9)
cpu_ms = (cpu + pg_cpu) * max(float(w1) - float(w0), 1e-9) * 1000.0 / max(settled, 1)
print(f"| offered | sent | settled | dup | drained | settles/s | settle p50 | settle p99 | intake p50 | intake p99 | commits/settle | WAL flushes/settle | WAL KB/settle | WAL fsync busy | engine CPU (cores) | PG CPU (cores) | CPU ms/settle (engine+PG) | engine env |")
print(f"|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
busy = (float(s1) - float(s0)) / 1000.0 / max(float(w1) - float(w0), 1e-9)
print(f"| {rate}/s x {duration} | {sent} | {settled} | {s['duplicates']} | {drained} | {settled/span:.1f} | {s['p50Ms']:.0f} ms | {s['p99Ms']:.0f} ms | {d.get('med',0):.0f} ms | {d.get('p(99)',0):.0f} ms | {per(c0,c1):.1f} | {per(f0,f1):.2f} | {per(b0,b1)/1024:.1f} | {busy*100:.0f}% | {cpu:.2f} | {pg_cpu:.2f} | {cpu_ms:.2f} | `{env or 'defaults'}` |")
PY
)"
top="$("${PSQL[@]}" -d sutra -F ' | ' -c "SELECT calls, round(total_exec_time::numeric,0), left(regexp_replace(query, '\s+', ' ', 'g'), 110)
  FROM pg_stat_statements ORDER BY calls DESC LIMIT 40")"
# The outbox's lease renewals and delivered-row deletes: how long each statement took, and how many
# deadlocks the database detected over the measured run.
outbox="$("${PSQL[@]}" -d sutra -F ' | ' -c "SELECT calls, rows, round(mean_exec_time::numeric,1),
  round(max_exec_time::numeric,1), round(total_exec_time::numeric,0), left(regexp_replace(query, '\s+', ' ', 'g'), 90)
  FROM pg_stat_statements
  WHERE query ~ 'SET next_attempt_at = \\\$3' OR query ~ '^ *(WITH doomed|DELETE FROM outbox_entry)'
  ORDER BY total_exec_time DESC")"
DL1="$("${PSQL[@]}" -d sutra -c "SELECT deadlocks FROM pg_stat_database WHERE datname = 'sutra'")"
{
    printf '%s\n\nTop statements by calls (calls | total ms | query):\n\n```\n%s\n```\n' "$report" "$top"
    printf '\nOutbox lease renewals and deletes (calls | rows | mean ms | max ms | total ms | query), deadlocks %s:\n\n```\n%s\n```\n' \
        "$((DL1 - DL0))" "$outbox"
} | tee ${OUT:+"$OUT"}

if [ -n "${PERF_DIR:-}" ]; then
    for who in engine postgres; do
        data="$PERF_DIR/$who.perf.data"
        [ -s "$data" ] || continue
        perf script -i "$data" 2>/dev/null | inferno-collapse-perf >"$PERF_DIR/$who.folded" 2>/dev/null \
            && inferno-flamegraph --title "$who, ${RATE}/s" <"$PERF_DIR/$who.folded" >"$PERF_DIR/$who.svg" 2>/dev/null
        perf report -i "$data" --no-children -g none --sort dso,symbol --stdio 2>/dev/null \
            | grep -E '^ +[0-9]' | sed 's/  */ /g' | head -25 >"$PERF_DIR/$who.top.txt"
        # Inclusive share per Rust crate (or library) anywhere on the stack, from the folded file.
        awk '{ n = $NF; $NF = ""; split($0, f, ";"); delete seen;
               for (i in f) { c = f[i]; sub(/^ +/, "", c);
                              if (c !~ /^[a-z_][a-z0-9_]*::/) continue; sub(/::.*/, "", c);
                              if (!(c in seen)) { seen[c] = 1; t[c] += n } }
               total += n }
             END { for (c in t) printf "%6.2f%% %s\n", 100 * t[c] / total, c }' \
            "$PERF_DIR/$who.folded" | sort -rn | head -30 >"$PERF_DIR/$who.crates.txt"
    done
    log "profile in $PERF_DIR"
fi

if [ "$SETTLED" -lt "$SENT" ] || [ "$DUPS" -ne 0 ]; then
    log "settled $SETTLED of $SENT, $DUPS duplicates"
    exit 1
fi
exit 0
