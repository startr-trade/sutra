# Upgrading to 0.6.22

**2026-11-15.** **A message now answers the instance waiting for it before it
starts anything**, with one version of a package and across versions while an older one drains.
**A retry that spans a version flip is a duplicate.** A repeated Kafka TLS or SASL failure is logged
once with a count instead of hundreds of times. Large templates compile once per process. The
engine image's CA bundle no longer trusts two roots Debian distrusted (DLA-4845-1). Two additive
migrations run at first boot: V305 and V306 (inbox). There is no archive format, deploymentId or
snapshot format change.

## Before you upgrade

**Check these against what you run:**

- **A channel with both a start event and a wait of a compatible message type.** A message that
  correlates to a parked instance now resumes it and starts nothing, also with one version.
  `sutra lint` already warns these with `SUTRA.CONFIG.CHANNEL.START_AND_WAIT`. A flow that re-sent a
  business key on such a channel to start a second instance now answers the first.
- **A message whose key names a running instance that has not reached its wait yet** is refused with
  a retryable `WAIT_PENDING` answer instead of starting a new instance.
- **A retry across an upgrade is a duplicate** (202, naming the original instance). A changed key
  header, `dedupKey` path or channel name produces another key, and that message runs again.
- **A non-owning database role** needs `SELECT, INSERT` on the new `deployment_owner` table, which
  `inbox_seen`'s row-security policy reads.
- **`inbox_seen` gets an index built concurrently at first boot** (V306). It adds I/O in proportion
  to the table and blocks no writes. Two replicas booting together build it once.

## Waits first

The walkthrough is [A reply answers the instance waiting for it](../building/waits-first.md).

Before a start event is resolved, the engine asks whether the message answers a wait an instance of
the channel's owner is parked at, in any of the owner's live versions. A hit resumes that instance
and is the whole delivery. A miss is dispatched exactly as before.

- **A resume is processed as the version it is parked on**: its own binding, codec, validators and
  reply codec, under the address and auth of the version that serves the channel. A message that
  version 2's codec refuses can still resume a parked version 1 instance under version 1's codec; it
  can never start anything.
- **A second process waiting on one channel is now tried**; before, only the first was.
- **On a `broadcast: true` channel** the process holding the correlated wait resumes and does not
  start; every other process's start on the channel still runs.
- **Cost:** nothing on a channel no live version waits on (an index answers in a few hash lookups).
  On a channel with a wait of a compatible type, one correlation lookup per live version that waits.

**Measured** (the neutral relay benchmark, `tools/sutra-load-test/relay-bench.sh`, 500/s for 60 s,
32 lanes, a pool of 40, 32 outbox workers, durable PostgreSQL; two runs each, interleaved, on one
machine; release-it builds):

| build | settles/s | settle p50 | settle p99 | WAL flushes/settle | commits/settle | CPU ms/settle (engine+PG) |
|---|---|---|---|---|---|---|
| 0.6.21 | 497.7 / 498.5 | 26 / 27 ms | 116 / 130 ms | 0.59 / 0.57 | 13.2 / 13.2 | 6.18 / 6.61 |
| 0.6.22 | 498.6 / 498.7 | 27 / 26 ms | 200 / 76 ms | 0.58 / 0.59 | 13.2 / 13.3 | 6.68 / 6.62 |

Every order settled exactly once in every run. The differences are within run-to-run noise (p99
spans 76 to 200 ms across the four runs).

## Dedup per owner

A dedup record now carries its owner, so a key version 1 recorded is a duplicate on version 2, and
two versions racing one key commit it once. Records written before the upgrade dedupe within their
own deployment only (no backfill), so a retry spanning both the engine upgrade and a version flip can
run again.

## Fixes

- **Kafka:** against a wrong CA, librdkafka reported every reconnect, each time with different text,
  and the engine logged about 740 `BROKER_TLS_FAILED` lines in 20 s. The first failure under a code is
  now logged in full, and repeats at most once per 30 s with their count, until a success resets it.
- **Templates:** compiling a Handlebars template costs time in proportion to the square of its size
  (about 0.8 s for 100 KB and 3.4 s for 200 KB). Each engine lane compiled each template again on its
  first render after every activation. A deployment's templates are now compiled when it is
  deployed (logged as `deployment templates precompiled`, with the time it took), once per process,
  so the first message pays nothing: a 210 KB template's first message answered in 16 ms. A
  template that does not compile is refused by `sutra lint` as before, and fails at render as
  before if it reaches one.
- **AMQP 1.0 sends are bounded.** Opening the connection (TLS and SASL included), beginning the
  session, attaching the sender and waiting for the broker to settle the message each get at most
  `SUTRA_SINK_AMQP_OPERATION_TIMEOUT_MS` (default 30000). A broker that stops answering fails the
  attempt with the retryable `SUTRA.OUTBOUND.AMQP.SEND_TIMEOUT` and the outbox sends again; before,
  one delivery could wait forever. A send that timed out waiting for the broker to settle it may already have
  been accepted, so the retry can deliver it twice: receivers dedupe on `sutra-outbox-key`.
- **CA bundle:** the image carries a CA bundle that no longer trusts "AAA Certificate Services" and
  "emSign ECC Root CA - G3" (DLA-4845-1).
- `rustix` 1.1.6 (RUSTSEC-2026-0486).

## Known

- **`SUTRA.INBOUND.CAPABILITY_MISMATCH` answers HTTP 500.** A body the channel's codec or format
  contract cannot take (a nested body on a flat `csv` channel, a content type the codec does not
  accept) is the caller's fault, and a later release will answer it with 415. Nothing changes in
  0.6.22.
