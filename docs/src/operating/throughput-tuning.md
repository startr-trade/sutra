# Tuning for throughput

*Since 0.6.5.* This page shows how to measure what one engine and one PostgreSQL complete per
second on your hardware, and which settings move it, without giving up durability: every number
here was taken with `fsync`, `synchronous_commit` and `full_page_writes` on.

## What limits a durable engine

An instance's state is safe only once PostgreSQL has flushed its write-ahead log to disk, and a
disk flushes a limited number of times per second. A consumer NVMe drive manages about 300; a
server SSD with a power-loss-protected cache manages tens of thousands. Measure yours with
PostgreSQL's own tool:

```bash
/usr/lib/postgresql/18/bin/pg_test_fsync -s 2 -f /path/on/the/database/disk/testfile
```

What decides throughput is how many commits share each flush. The engine helps in three ways:

- it commits only what must be durable before anything is acknowledged: a step's state, its
  aliases, its waits and its outbound messages, in one transaction. Bookkeeping that a crash cannot
  hurt — an ownership claim, the outbox's row locks, the delete of an already delivered row —
  does not wait for its own flush, and the inbox dedup record rides the step's own transaction;
- it delivers outbound messages on several workers at once (`sutra.outbox.workers`), deletes
  delivered rows in batches, and hands `local://` deliveries straight from the sender's commit to a
  worker;
- with several execution lanes (`sutra.engine.shards`), several steps commit at the same time,
  and PostgreSQL flushes them together.

So on a slow disk, add lanes: each lane is one more commit that can share a flush.

### What may skip its own flush, and why

The server keeps `synchronous_commit = on`. Three kinds of transaction set
`synchronous_commit = off` for themselves alone, and only because a crash that loses them can
neither lose an acknowledged state nor invent one:

| Transaction | What it writes | Why it may skip its flush |
|---|---|---|
| instance ownership claim | which engine is resuming an instance | a claim held by an engine that crashed is void anyway; the step that follows commits synchronously |
| outbox claim | row locks only | locks end with the transaction whatever happens |
| delete of delivered outbox rows | removes rows already sent | if it is lost, the row is sent again: outbound delivery has always been at-least-once, and receivers deduplicate on the idempotency key |

The inbox dedup record of a delivery is not on the list: it is written in the same transaction as
the step that processes the delivery, so it exists exactly when the step's effects do.

Everything else — every step's state, aliases, waits and outbound rows, every data-store write,
every retry or poison mark — commits synchronously, as before. The tier-2 suite kills the engine,
and separately the database, in the middle of a load and checks that every accepted request still
completes exactly once.

## The workload

The repository carries a workload that exercises the whole durable path, in
`tools/sutra-load-test`:

- `fixtures/relay/` — two processes. `relay-intake` takes an order over HTTP, validates it with a
  small `.srl` ruleset, reads two rows from a data store, forwards it over a local channel to
  `relay-worker`, waits for the worker's answer on a second local channel, and calls back an HTTP
  endpoint. Three durable hops per order.
- `profiles/relay.js` — k6 posts orders at a constant rate.
- `sink/` — a small HTTP server that receives the callbacks and measures each order's latency
  from the moment k6 sent it.
- `relay-bench.sh` — starts a throwaway PostgreSQL cluster (durable settings) on the disk you run
  it from, the sink and the engine, warms up, runs k6, waits until every order has called back,
  and prints one line of results plus the busiest SQL statements.

It needs k6, `jq`, PostgreSQL's server binaries (`PG_BIN_DIR`, default
`/usr/lib/postgresql/18/bin`) and an optimised build of the engine and the CLI. The runs below
used the faster-to-build `release-it` profile:

```bash
(cd rust && cargo build --profile release-it -p sutra-dist -p sutra-cli)
export SUTRA_ENGINE_BIN=$PWD/rust/target/release-it/sutra-engine
export SUTRA_CLI=$PWD/rust/target/release-it/sutra
```

The script refuses to put the database on `tmpfs`, where a flush costs nothing and the numbers
would mean nothing.

## Step 1: the defaults

```bash
RATE=100 DURATION=60s tools/sutra-load-test/relay-bench.sh
```

```text
[relay-bench] building the sink
[relay-bench] warm-up 10/s x 10s
[relay-bench] measured run 100/s x 60s
| offered | sent | settled | dup | drained | settles/s | settle p50 | settle p99 | intake p50 | intake p99 | commits/settle | WAL flushes/settle | WAL KB/settle | WAL fsync busy | engine CPU (cores) | engine env |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100/s x 60s | 5166 | 5167 | 0 | yes | 70.4 | 6657 ms | 13212 ms | 6638 ms | 10001 ms | 16.0 | 3.02 | 10.2 | 64% | 0.29 | `defaults` |
```

```text
Top statements by calls (calls | total ms | query):
72548 | 26 | BEGIN
72548 | 24 | COMMIT
46799 | 162 | SELECT set_config('sutra.deployment_id', $1, true)
25749 | 131 | SELECT set_config('synchronous_commit', 'off', true), set_config('sutra.deployment_id', $1, true)
15501 | 388 | INSERT INTO outbox_entry (entry_id, deployment_id, instance_id, body, content_type, destination, headers_json,
15400 | 307 | DELETE FROM outbox_entry WHERE deployment_id = $1 AND entry_id = ANY($2)
10334 | 355 | INSERT INTO alias_index (deployment_id, instance_id, alias_name, alias_value, unique_alias, live) VALUES ($1, 
10334 | 135 | SELECT store_value, rev FROM data_store WHERE store_name = $1 AND store_key = $2
```

One lane executes one step at a time, so its commits never share a flush; on this disk that caps
the engine near 70 orders per second, and the rest of the offered load queues at
intake (`intake p99`). `settled` can exceed `sent`: k6 gives up on a request after 10 seconds
and does not count it, but the intake had already accepted it, and it completes like any other.

## Step 2: lanes, and a pool to match

Each lane holds a database connection while it commits, so raise the main pool with the lanes:

```bash
RATE=100 DURATION=60s ENGINE_ENV="SUTRA_ENGINE_SHARDS=8" tools/sutra-load-test/relay-bench.sh
```

```text
[relay-bench] warm-up 10/s x 10s
[relay-bench] measured run 100/s x 60s
| offered | sent | settled | dup | drained | settles/s | settle p50 | settle p99 | intake p50 | intake p99 | commits/settle | WAL flushes/settle | WAL KB/settle | WAL fsync busy | engine CPU (cores) | engine env |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 100/s x 60s | 6001 | 6001 | 0 | yes | 99.9 | 19 ms | 46 ms | 5 ms | 14 ms | 15.9 | 2.90 | 10.2 | 87% | 0.43 | `SUTRA_ENGINE_SHARDS=8` |
```

Then the target rate:

```bash
RATE=500 DURATION=60s \
  ENGINE_ENV="SUTRA_ENGINE_SHARDS=32 SUTRA_DATASOURCE_MAX_CONNECTIONS=40" \
  tools/sutra-load-test/relay-bench.sh
```

```text
[relay-bench] warm-up 10/s x 10s
[relay-bench] measured run 500/s x 60s
| offered | sent | settled | dup | drained | settles/s | settle p50 | settle p99 | intake p50 | intake p99 | commits/settle | WAL flushes/settle | WAL KB/settle | WAL fsync busy | engine CPU (cores) | engine env |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500/s x 60s | 30001 | 30001 | 0 | yes | 499.0 | 36 ms | 250 ms | 9 ms | 51 ms | 13.8 | 0.57 | 10.1 | 90% | 2.66 | `SUTRA_ENGINE_SHARDS=32 SUTRA_DATASOURCE_MAX_CONNECTIONS=40` |
```

Read the columns this way:

| Column | Meaning |
|---|---|
| `settles/s` | orders that called back, per second, from the start of the run to the last callback |
| `settle p50` / `p99` | time from k6 sending the order to the sink receiving the callback |
| `intake p50` / `p99` | time for the intake to answer `202` |
| `commits/settle` | database transactions per order, read-only ones included |
| `WAL flushes/settle` | disk flushes per order — below 1 means commits share flushes |
| `WAL fsync busy` | share of the run the disk spent flushing |
| `engine CPU` | cores the engine used |

When `WAL fsync busy` is near 100% and adding lanes no longer raises `settles/s`, the disk is the
limit: a faster disk, or PostgreSQL's `commit_delay` (which makes a flush wait a few microseconds
for more commits to join it, still durable), are what move it next.

## The settings

| Setting | Default | Raise it when |
|---|---|---|
| `sutra.engine.shards` | `1` | the disk is busy less than the time it takes to flush, and orders queue at intake. Read [Execution lanes](../architecture/execution-lanes.md) first: lanes run different instances in parallel. |
| `sutra.datasource.max-connections` | `8` | you raise the lanes: keep it at least the lane count, plus a few |
| `sutra.datasource.dispatch-max-connections` | `2` | rarely; the dispatcher's claim and its batched delete each hold one connection |
| `sutra.outbox.workers` | the lane count, at least 8 | outbound deliveries are slow (a remote HTTP endpoint); each worker waits on one delivery at a time |
| `sutra.outbox.batch-size` | `50` | a deployment enqueues far more than 50 rows between claims |
| `sutra.outbox.tick-interval` | `PT5S` | you want retries and rows from other replicas picked up sooner; local deliveries do not wait for it |
| `cache:` on a data store | off | a store is read on every request and changed rarely — see [Caching reads](../building/data-stores.md#caching-reads) |

## Profiling

`PERF_DIR=/some/dir` records a CPU profile of the engine and of the database backends over the
measured run, and writes a flamegraph of each plus a list of the hottest functions. It needs
`perf` usable by your user (`kernel.perf_event_paranoid` of 1 or less) and
[inferno](https://github.com/jonhoo/inferno) (`cargo install inferno`). Build the engine with frame
pointers so the stacks are whole:

```bash
(cd rust && RUSTFLAGS="-C force-frame-pointers=yes" cargo build --profile release-it -p sutra-dist)
PERF_DIR=/tmp/relay-profile RATE=100 DURATION=30s tools/sutra-load-test/relay-bench.sh
```

On the run above the engine used well under one core; most of its time went to the database
protocol, and the rest of the wall clock was the disk.
