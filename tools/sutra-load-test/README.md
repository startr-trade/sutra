# sutra-load-test — end-to-end load harness

Pure-shell k6 harness that exercises the full inbound-HTTP → BPMN-engine → audit-write
path under sustained concurrent load against a running Sutra deployment.

This is **not** a microbenchmark suite — those measure the FEEL parser/evaluator hot
path; this measures end-to-end throughput and latency from the operator's point of view.

**Why this is a shell module, not a workspace crate**: the harness drives a *deployed*
engine over HTTP with an external tool (k6). There is nothing to compile and no build
integration to gain, so it stays a standalone shell module.

**Tool**: [k6](https://k6.io/). Picked over wrk2 because the install story is
clean (single binary via `apt`/`brew`), JSON output is native (no Lua glue),
and the script language is plain JS. wrk2 would need C compile + Lua.

## Run locally

```bash
# 1. install k6 (Linux):
sudo gpg -k && sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install -y k6

# 2. bring up the SUT yourself — e.g. build and run the engine image
#    (docker build -t sutra-rust-engine:dev -f rust/Dockerfile rust/), or deploy it
#    to a cluster. The harness never starts anything; it only generates load.

# 3. smoke run (30 s, ~300 requests) against the running deployment:
tools/sutra-load-test/run.sh --target=external --url=http://127.0.0.1:8081 --profile=smoke
```

`--target=external` is the only target: the harness points k6 at a base URL that already
serves the channel under test. Set `SUTRA_API_KEY=...` when the target deployment's
channel resolves an api-key other than the sample `hello-demo-key`, and `PAYLOAD=/abs/path`
for a request body other than `fixtures/hello.txt`.

## The relay workload: durable throughput, measured end to end

`relay-bench.sh` owns the whole system under test, because its numbers need the database to
itself: a throwaway PostgreSQL cluster (durable: fsync, synchronous_commit, full_page_writes on)
on the repository's disk, the callback sink, and the engine binary with the `fixtures/relay`
package — an HTTP intake with a light validator and two data-store reads, a `local://` forward to
a second process, its answer relayed back over a second local channel, and an HTTP callback to
the sink. It reports settles/s, settle latency (callback received minus intake sent), commits and
WAL flushes per settle, WAL fsync busy time, engine and PostgreSQL CPU, CPU milliseconds per
settle (engine + PostgreSQL), plus the top statements.

```bash
(cd rust && cargo build --profile release-it -p sutra-dist -p sutra-cli)
export SUTRA_ENGINE_BIN=$PWD/rust/target/release-it/sutra-engine SUTRA_CLI=$PWD/rust/target/release-it/sutra
RATE=100 DURATION=60s tools/sutra-load-test/relay-bench.sh
RATE=500 DURATION=60s \
  ENGINE_ENV="SUTRA_ENGINE_SHARDS=32 SUTRA_DATASOURCE_MAX_CONNECTIONS=40" \
  tools/sutra-load-test/relay-bench.sh
```

`CACHE_TTL=PT0S` turns on the `refdata` store's read cache; `PERF_DIR=<dir>` records a CPU
profile and flamegraphs of the engine and the database (needs `perf` and `inferno`). Read the
header of `relay-bench.sh` for every knob.

## Resource metrics per load step (for a cost model)

Every load step reports the resources it cost, not only its latency: engine CPU and RSS,
PostgreSQL CPU, PostgreSQL IOPS and WAL volume. `relay-bench.sh` prints the table for its measured
step; `run.sh` appends it to the `.md` summary and writes `<run>.metrics.json` next to it. Both use
[`step-metrics.sh`](step-metrics.sh), which reads only `/proc`, `docker inspect`, the container's
cgroup and two PostgreSQL views, so there is nothing to install.

```bash
# engine and database in containers on this host
ENGINE_CONTAINER=sutra-engine PG_CONTAINER=sutra-db PG_USER=sutra PG_DB=sutra \
  tools/sutra-load-test/run.sh --target=external --url=http://127.0.0.1:8081 --profile=smoke
# engine on this host by pid, a remote or managed PostgreSQL reached over the network
ENGINE_PID=$(pgrep -f sutra-engine) PG_PSQL="psql -h db.internal -U meter -d app" \
  tools/sutra-load-test/run.sh --target=external --url=http://127.0.0.1:8081 --profile=sustained
```

| Metric | Source | Needs |
|---|---|---|
| Engine CPU (cores) | `/proc/<pid>/stat` utime+stime delta; a container uses its cgroup `cpu.stat` | `ENGINE_PID` or `ENGINE_CONTAINER`, on this host |
| Engine RSS, end and peak (MB) | `VmRSS` and `VmHWM` in `/proc/<pid>/status` (peak is the process's lifetime peak) | same |
| PostgreSQL CPU (cores) | postmaster and backends, or the container cgroup | `PG_PID` or `PG_CONTAINER`, on this host |
| PostgreSQL IOPS | reads+writes+extends from `pg_stat_io`, per second. PostgreSQL's own I/O operations, not device IOPS | PostgreSQL 16+, `PG_PSQL` or `PG_CONTAINER` |
| WAL (bytes, bytes/s) | `pg_stat_wal.wal_bytes` delta | PostgreSQL 14+, `PG_PSQL` or `PG_CONTAINER` |

A metric that cannot be read in the deployment shape (a remote engine, a managed database, an older
PostgreSQL, an unreadable pid) is reported as `unavailable` in the table and `null` in the JSON.
It is never reported as 0.

**Guidance for downstream harnesses.** The functions are small enough to copy or source: call
`sm_snapshot <file>` at the start and end of each step, then `sm_report` (markdown) or
`sm_report_json` (one object per step, with `null` for unavailable). Take one snapshot pair per
step, so a ramp or soak yields a row per stage. For a cost model use CPU cores and RSS to size
compute, `pg_iops` and `wal_bytes_per_s` to size the database volume and its provisioned IOPS, and
divide by the step's completed work (settles, requests) for a per-unit cost. Where the database is
managed, take its CPU and device IOPS from the provider's metrics for the same time window and keep
`PG_PSQL` for WAL. CPU from `/proc` on a native PostgreSQL does not include backends that exit
during the step; the engine keeps pooled connections, so this is small, and a container's cgroup
reading has no such gap. Read the numbers at steady state: discard the warm-up step.

## Layout

```
tools/sutra-load-test/
├── README.md          (this file)
├── run.sh             (harness: run k6 against a URL, capture results, exit code)
├── step-metrics.sh    (sourced: per-step engine/PostgreSQL CPU, RSS, IOPS, WAL)
├── relay-bench.sh     (the relay workload: own PG + sink + engine, per-settle DB cost)
├── sink/              (the relay workload's callback sink — standalone crate, own Cargo.lock)
├── profiles/
│   ├── smoke.js       (~30 s, 10 RPS, 1 VU — PR feedback)
│   ├── sustained.js   (~5 min, 100 RPS, 4 VUs — headline number)
│   ├── burst.js       (~2 min, 0→500 RPS ramp + hold + ramp-down — peak hour)
│   └── relay.js       (the relay workload's intake, driven by relay-bench.sh)
└── results/           (.gitignore'd; CI uploads as artifacts)
```

## Profiles at a glance

| Profile | Duration | Offered RPS | Budget | When |
|---|---|---|---|---|
| smoke     | 30 s   | 10                       | ~300 reqs    | every PR / pre-merge |
| sustained | 5 min  | 100                      | ~30,000 reqs | nightly CI / baselines |
| burst     | 2 min  | 0→500 ramp + hold + down | ~45,000 reqs | peak-hour stress, on-demand |

## Exit codes

| Code | Meaning |
|---|---|
| 0 | all thresholds green |
| 1 | error-rate threshold breached (`ERROR_RATE_THRESHOLD`, default 0.5%) |
| 2 | p99 latency threshold breached (`P99_MS_THRESHOLD`, default 500 ms) |
| 3 | harness error (bad arguments, k6 missing, no k6 output) |
