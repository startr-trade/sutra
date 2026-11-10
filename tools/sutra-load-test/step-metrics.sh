#!/usr/bin/env bash
#
# step-metrics.sh — per-load-step resource metrics for a cost model. Source it; do not run it.
#
#   . step-metrics.sh
#   sm_snapshot "$WORK/before"        # at the start of a step
#   ...drive load...
#   sm_snapshot "$WORK/after"         # at the end of the step
#   sm_report "$WORK/before" "$WORK/after" "smoke"          # markdown table on stdout
#   sm_report_json "$WORK/before" "$WORK/after" "smoke"     # the same as one JSON object
#
# What is measured, and where it is read from (no agent, no extra dependency):
#   engine CPU (cores)   utime+stime of the engine process, /proc/<pid>/stat
#   engine RSS (MB)      VmRSS at the end of the step, and VmHWM (the process's peak), /proc/<pid>/status
#   PostgreSQL CPU       utime+stime of the postmaster and its backends, /proc/<pid>/stat; a container
#                        (ENGINE_CONTAINER / PG_CONTAINER) is read from its cgroup cpu.stat instead,
#                        which also counts backends that exited during the step
#   PostgreSQL IOPS      block reads+writes+extends the server counted over the step, pg_stat_io (PG 16+).
#                        These are PostgreSQL's own I/O operations (buffer-cache misses and evictions
#                        included), not device IOPS: size the disk from them, do not compare them with fio.
#   WAL (bytes, bytes/s) pg_stat_wal.wal_bytes (PG 14+)
#
# Where the system under test is (set what applies; everything unset is reported as unavailable):
#   ENGINE_PID | ENGINE_CONTAINER   a host pid, or a docker container name (its init process is used)
#   PG_PID | PG_CONTAINER           the postmaster's host pid, or a docker container name
#   PG_PSQL                         a command that accepts `-c <sql>`, e.g. "psql -h db.internal -U meter -d app"
#                                   (a role that may read pg_stat_io / pg_stat_wal is enough); or, with
#                                   PG_CONTAINER set, PG_USER (postgres) and PG_DB (postgres) select the
#                                   in-container psql.
#
# A metric that cannot be read in a deployment shape — managed database with no host access, a remote
# engine, a pid that is not visible, PostgreSQL older than 16 — is printed as "unavailable", NEVER as 0.
# Only an engine and database on the load generator's host (or a container on it) expose their /proc;
# for a managed database take CPU/IOPS from the provider's metrics and still take WAL from PG_PSQL.

sm_hz() { getconf CLK_TCK 2>/dev/null || echo 100; }

# Resolve a container name to its host pid; prints nothing when it cannot.
sm_container_pid() {
    command -v docker >/dev/null 2>&1 || return 0
    local pid
    pid="$(docker inspect -f '{{.State.Pid}}' "$1" 2>/dev/null)" || return 0
    if [ -n "$pid" ] && [ "$pid" != "0" ]; then printf '%s' "$pid"; fi
}

sm_engine_pid() {
    if [ -n "${ENGINE_PID:-}" ]; then printf '%s' "$ENGINE_PID"
    elif [ -n "${ENGINE_CONTAINER:-}" ]; then sm_container_pid "$ENGINE_CONTAINER"
    fi
}

sm_pg_pid() {
    if [ -n "${PG_PID:-}" ]; then printf '%s' "$PG_PID"
    elif [ -n "${PG_CONTAINER:-}" ]; then sm_container_pid "$PG_CONTAINER"
    fi
}

# utime+stime in clock ticks of a pid plus its direct children; empty when /proc/<pid> is unreadable.
# A backend that exits mid-step takes its ticks with it, so this reads pooled (long-lived)
# connections well, which is what the engine keeps.
sm_ticks() {
    local pid="$1" p t total=0 seen=0
    [ -r "/proc/$pid/stat" ] || return 0
    for p in "$pid" $(pgrep -P "$pid" 2>/dev/null); do
        t="$(awk '{print $14 + $15}' "/proc/$p/stat" 2>/dev/null)" || continue
        if [ -n "$t" ]; then total=$((total + t)); seen=1; fi
    done
    if [ "$seen" = 1 ]; then printf '%s' "$total"; fi
}

# kB of a /proc/<pid>/status field (VmRSS, VmHWM); empty when unreadable.
sm_status_kb() { awk -v k="$2:" '$1 == k {print $2}' "/proc/$1/status" 2>/dev/null; }

# Run one SQL statement through PG_PSQL / PG_CONTAINER; prints the single row, space separated.
sm_sql() {
    if [ -n "${PG_PSQL:-}" ]; then
        # shellcheck disable=SC2086
        $PG_PSQL -X -At -F ' ' -c "$1" 2>/dev/null
    elif [ -n "${PG_CONTAINER:-}" ]; then
        docker exec "$PG_CONTAINER" psql -X -At -F ' ' -U "${PG_USER:-postgres}" -d "${PG_DB:-postgres}" -c "$1" 2>/dev/null
    else
        return 1
    fi
}

# CPU of a whole docker container in clock ticks, from its cgroup (v2) cpu.stat. Unlike summing
# /proc/<pid>, this keeps the CPU of backends that already exited, so a database that opens a
# connection per query is not undercounted. Empty when the cgroup is not readable.
sm_container_ticks() {
    local pid="$1" cg usec
    cg="$(awk -F: '$1 == "0" {print $3}' "/proc/$pid/cgroup" 2>/dev/null)"
    if [ -z "$cg" ] || [ "$cg" = "/" ]; then return 0; fi
    usec="$(awk '$1 == "usage_usec" {print $2}' "/sys/fs/cgroup$cg/cpu.stat" 2>/dev/null)"
    if [ -n "$usec" ]; then awk -v u="$usec" -v hz="$(sm_hz)" 'BEGIN { printf "%.2f", u / 1000000 * hz }'; fi
}

# Ticks for a pid: the pid and its children when the caller gave a host pid, the container's
# cgroup when it gave a container name (second argument empty).
sm_cpu_ticks() {
    if [ -n "$2" ]; then sm_ticks "$1"; else sm_container_ticks "$1"; fi
}

# Write one snapshot: key=value lines, a key absent when it could not be read.
sm_snapshot() {
    local out="$1" epid ppid v io wal
    echo "wall=$(date +%s.%N)" >"$out"
    epid="$(sm_engine_pid)"
    if [ -n "$epid" ]; then
        v="$(sm_cpu_ticks "$epid" "${ENGINE_PID:-}")"; if [ -n "$v" ]; then echo "engine_ticks=$v" >>"$out"; fi
        v="$(sm_status_kb "$epid" VmRSS)"; if [ -n "$v" ]; then echo "engine_rss_kb=$v" >>"$out"; fi
        v="$(sm_status_kb "$epid" VmHWM)"; if [ -n "$v" ]; then echo "engine_hwm_kb=$v" >>"$out"; fi
    fi
    ppid="$(sm_pg_pid)"
    if [ -n "$ppid" ]; then
        v="$(sm_cpu_ticks "$ppid" "${PG_PID:-}")"; if [ -n "$v" ]; then echo "pg_ticks=$v" >>"$out"; fi
    fi
    # pg_stat_io is absent before PostgreSQL 16 and pg_stat_wal.wal_bytes before 14: the query
    # fails, the key stays absent, and the report says "unavailable".
    io="$(sm_sql "SELECT coalesce(sum(coalesce(reads,0)+coalesce(writes,0)+coalesce(extends,0)),0)::bigint FROM pg_stat_io" || true)"
    case "$io" in ''|*[!0-9]*) ;; *) echo "pg_io_ops=$io" >>"$out" ;; esac
    wal="$(sm_sql "SELECT wal_bytes::bigint FROM pg_stat_wal" || true)"
    case "$wal" in ''|*[!0-9]*) ;; *) echo "pg_wal_bytes=$wal" >>"$out" ;; esac
    return 0
}

sm_get() { awk -F= -v k="$2" '$1 == k {print $2}' "$1"; }

# Derive the step's numbers as "name=value" lines; a value is "unavailable" when a snapshot lacks it.
sm_derive() {
    local a="$1" b="$2" hz dur k x0 x1
    hz="$(sm_hz)"
    dur="$(awk -v a="$(sm_get "$a" wall)" -v b="$(sm_get "$b" wall)" 'BEGIN { d = b - a; printf "%.3f", (d > 0 ? d : 0.001) }')"
    echo "duration_s=$dur"
    for k in engine_ticks pg_ticks; do
        x0="$(sm_get "$a" $k)"; x1="$(sm_get "$b" $k)"
        if [ -n "$x0" ] && [ -n "$x1" ]; then
            echo "${k%_ticks}_cpu_cores=$(awk -v a="$x0" -v b="$x1" -v hz="$hz" -v d="$dur" 'BEGIN { printf "%.3f", (b - a) / hz / d }')"
        else echo "${k%_ticks}_cpu_cores=unavailable"; fi
    done
    for k in engine_rss_kb engine_hwm_kb; do
        x1="$(sm_get "$b" $k)"
        if [ -n "$x1" ]; then echo "${k%_kb}_mb=$(awk -v b="$x1" 'BEGIN { printf "%.1f", b / 1024 }')"
        else echo "${k%_kb}_mb=unavailable"; fi
    done
    x0="$(sm_get "$a" pg_io_ops)"; x1="$(sm_get "$b" pg_io_ops)"
    if [ -n "$x0" ] && [ -n "$x1" ]; then
        echo "pg_io_ops=$((x1 - x0))"
        echo "pg_iops=$(awk -v a="$x0" -v b="$x1" -v d="$dur" 'BEGIN { printf "%.1f", (b - a) / d }')"
    else echo "pg_io_ops=unavailable"; echo "pg_iops=unavailable"; fi
    x0="$(sm_get "$a" pg_wal_bytes)"; x1="$(sm_get "$b" pg_wal_bytes)"
    if [ -n "$x0" ] && [ -n "$x1" ]; then
        echo "wal_bytes=$((x1 - x0))"
        echo "wal_bytes_per_s=$(awk -v a="$x0" -v b="$x1" -v d="$dur" 'BEGIN { printf "%.0f", (b - a) / d }')"
    else echo "wal_bytes=unavailable"; echo "wal_bytes_per_s=unavailable"; fi
}

sm_report() {
    local d; d="$(sm_derive "$1" "$2")"
    g() { awk -F= -v k="$1" '$1 == k {print $2}' <<<"$d"; }
    echo "### Resource metrics — step \`${3:-step}\`"
    echo
    echo "| Metric | Value |"
    echo "|---|---|"
    echo "| Step duration | $(g duration_s) s |"
    echo "| Engine CPU | $(g engine_cpu_cores) cores |"
    echo "| Engine RSS at end / peak | $(g engine_rss_mb) MB / $(g engine_hwm_mb) MB |"
    echo "| PostgreSQL CPU | $(g pg_cpu_cores) cores |"
    echo "| PostgreSQL IOPS (pg_stat_io ops) | $(g pg_iops) /s ($(g pg_io_ops) ops) |"
    echo "| WAL | $(g wal_bytes_per_s) B/s ($(g wal_bytes) B) |"
    echo
    echo "_\"unavailable\" means not readable in this deployment shape (see the header of step-metrics.sh); it is not zero._"
}

# One JSON object; unavailable metrics are null.
sm_report_json() {
    sm_derive "$1" "$2" | awk -F= -v step="${3:-step}" 'BEGIN { printf "{\"step\":\"%s\"", step }
        { v = ($2 == "unavailable") ? "null" : $2; printf ",\"%s\":%s", $1, v } END { print "}" }'
}
