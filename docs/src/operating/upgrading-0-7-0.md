# Upgrading to 0.7.0

**2026-11-19.** **A data task can delete a store row** with `<q:store key="…" delete="true"/>`. It
adds an attribute to the `q:` namespace (`urn:sutra:q:1.0`), which is why this release is 0.7.0.
The addition is backward compatible: existing packages load and run unchanged. **A data-store row's
revision now starts from the database clock,** so a row deleted and created again never reuses a
revision. **A message that correlates to no live instance is recorded,** and a channel can send it
to a process instead of refusing it. **The AMQP 1.0 transport works with IBM MQ.** **Codecs can seal
what they send** and resolve secret references in their own bundle files. One migration, V103, runs
at first boot on PostgreSQL. There is no archive or snapshot format change.

**0.7.0 has no engine image.** Its CLI archives and crates were published; its image was not (see
[Upgrading to 0.8.0](upgrading-0-8-0.md)). Engine image users take these changes with 0.8.0.

## Before you upgrade

**Check these against what you run:**

- **Rows created after the upgrade start from a large revision, not 1.** An insert into a data store
  now seeds `rev` with the database clock in microseconds since the epoch (about 1.8 × 10^15 in
  2026); each write still adds 1. Rows written before the upgrade keep their revisions. Anything
  outside the engine that reads `rev` and expects a new row at 1, or expects revisions with no
  gaps, must stop expecting it. The `rev` column must be a 64-bit integer (`BIGINT`), as in every
  example in [Data stores](../building/data-stores.md); check a typed store whose table you created
  yourself.
- **V103 builds an index concurrently at first boot** (PostgreSQL: `alias_index_retired_lookup`, a
  partial index over the correlation keys of ended instances). It blocks no writes and adds I/O in
  proportion to the table. From now on a completing instance writes one more index entry per
  correlation key it retires.
- **A message that correlates to no live instance is now a WARN log line and an audit record.** A
  sender that keeps answering ended instances shows up in both. The HTTP answer is unchanged.
- **A `local://` delivery for an instance that has ended is given up at once.** The refusal no
  longer carries `sutra.waitPending=true` when every instance that carried the key has ended, so it
  is not retried for about four minutes first.
- **If you expect timer surges, run more than one shard** (`sutra.engine.shards`,
  `SUTRA_ENGINE_SHARDS`, default `1`; see [Configuration](configuration.md)). Unchanged since
  0.6.25: due timers fire as fast as they fall due, and with one shard that work shares the single
  [execution lane](../architecture/execution-lanes.md) with new work.

## Deleting a data-store row

`delete="true"` on a write removes the key's row instead of writing a value. Deleting a missing key
is a no-op. With `expect="unchanged"` the delete applies only while the row's revision is the one
the instance read; otherwise it fails with `SUTRA.RUNTIME.DATASTORE.CONFLICT`, which an error
boundary catches. A delete runs where a write runs (on a `<bpmn:transaction>` scope's store
transaction, else on its own), waits behind a `forUpdate` row lock, and invalidates the read cache
like a write. It works on key/value and typed stores on PostgreSQL, MySQL, MariaDB and SQL Server.
Combinations that make no sense for a delete are refused at load with
`SUTRA.PARSE.STORE.DELETE_INVALID`.

Reference and walkthrough: [Deleting a row](../building/data-stores.md#deleting-a-row).

## Revisions start from the database clock

Until 0.6.25 every insert started a row at revision 1. Once a process can delete rows, a key can
go from revision 1, to deleted, to created again at revision 1 with another value. Another
replica's read cache would then serve the old row, and an `expect="unchanged"` write or delete that
read the old row would apply. Now the insert reads the database's own clock inside the insert
statement, so every replica shares one clock. Revisions keep going up by one per write and never
repeat for a key, but they have gaps. This relies on the database clock moving forward: if it is
stepped backwards between a delete and the re-create, the guarantee can break.

## A message no instance waits for

A message on a channel a wait subscribes to that finds no live instance carrying its key
(`SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND`) is now recorded whatever transport carried it: a WARN
log line with `SUTRA.INBOUND.RELAY_UNCORRELATED`, and an `inbound.RELAY_UNCORRELATED` audit record.
Both name the channel, the message type, the dedup key and the keys looked for, never the body or
the headers, and sensitive key values are masked.

`uncorrelated: { start: <processId> }` on the channel in `channels.yaml` sends such a message to a
process of the package instead, with no BPMN change. Details and a run of both:
[An answer no instance waits for](../building/wait-states.md#an-answer-no-instance-waits-for).

## IBM MQ

The `amqp` transport now works with an IBM MQ queue manager's AMQP channel. Two fixes apply to every
broker: each connection has its own container id (IBM MQ closes an open connection when another
opens with the same id, so concurrent sends and replicas cut each other off), and the sink sets the
message's `to` property (IBM MQ refuses a message without it). New options, all off by default,
pick queue or topic addressing, persistence, expiry, the body form and the message properties; the
source and sink log when the broker is lost and when it is back. Also changed: the sink sends the
outbox key as `message-id`, and the source renders a binary message id as hex.

Setup and a walkthrough: [Connecting the engine to IBM MQ](ibm-mq.md).

## For codec authors

`PayloadCodec` gains two default methods, inert unless a codec overrides them: `seal`, the last
transform of an outbound message before the outbox commit (an error refuses the message with
`SUTRA.OUTBOUND.SEAL_FAILED`), and `bind_refs`, which resolves `secret:` and `env:` references named
in the codec's bundle files once, when the deployment is planned (an error refuses the deployment
with `SUTRA.CODEC.BIND_REFS_FAILED`). Existing codecs are unchanged. See
[Domain neutrality and the SPI model](../architecture/neutrality-and-spi.md).

## Fixed

- **A deployment flip could serve an older set of versions, or route a channel to a version whose
  lanes were not built yet** (database deployment source). Concurrent re-activations could
  interleave, so a request for the new version's channel answered 404
  (`RESOLVE.CHANNEL.UNKNOWN`), or a broadcast briefly had one subscriber. Re-activations are now
  serialized, and each reads the store afresh.
- **Building the engine image from source could ship stale code** when several checkouts were built
  on one host: the Dockerfile's shared cargo cache let changed crates pass as fresh. The build now
  decides by content. Published images are not affected.
