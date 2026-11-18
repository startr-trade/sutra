# Upgrading to 0.6.25

**2026-11-19.** **Templates gain an `xmlEscape` helper,** so free text relayed into an XML
message stays well-formed. **Timers no longer fall behind under sustained load:** up to 0.6.24 the
engine fired at most about 64 timers a second for the whole cluster, whatever the shard count, so
at higher rates every parked timer fired later than the one before it. **A distribution's version
line may carry its build** without the build reaching image tags. There is no migration, and no
archive format, deploymentId or snapshot format change.

## Before you upgrade

**Check this against what you run:**

- **If you expect timer surges, run more than one shard.** Set `sutra.engine.shards`
  (`SUTRA_ENGINE_SHARDS`, default `1`; see [Configuration](configuration.md)). Due timers now
  fire as fast as they fall due. With one shard, that work shares the single
  [execution lane](../architecture/execution-lanes.md) with new work, so new instances answer more
  slowly while a timer backlog drains. In the test below, at 200 new instances a second, each
  parking a 1 s timer, the median new-instance answer went from 2.5 ms to 59 ms with one shard, and
  from 1.9 ms to 2.6 ms with four.

## Timers drain instead of firing one batch per tick

Every 500 ms the leader replica's timer poller looks for due timers. Up to 0.6.24 it claimed at
most 32 per deployment, fired them and waited for the next tick. That is about 64 timer fires a
second for the whole cluster. Above that rate the backlog grew for as long as the load lasted:
instances started on time, and every timer-driven step after them ran later and later.

Now each tick keeps claiming while a claim comes back full, taking the deployments that are behind
in turn so one backlog cannot starve another. It stops when every claim comes back short, after one
second of draining (or the tick, if that is longer), or when the replica stops being the leader.
Each round's timers finish firing before the next claim. Up to `max(shards, 16)` timers fire at
once. Timer start events are unchanged. There is no new configuration key, and a timer still fires
at least once.

Measured on a debug build against PostgreSQL 16. Each instance starts over HTTP, parks a `PT1S`
timer, then sends a follow-up message; the lag is the follow-up's arrival against its due time.

| Shards | Rate, for 15 s | Follow-up lag p99, 0.6.24 | 0.6.25 |
|---|---|---|---|
| 1 | 200 a second | 31.8 s | 0.78 s |
| 4 | 400 a second | 78.2 s | 0.50 s |

The lag that remains comes from the 500 ms tick.

## `xmlEscape`

`{{xmlEscape value}}` escapes `&` `<` `>` `"` `'` in free text for XML element content and
attribute values. A null or missing value renders empty. Default `{{x}}` interpolation is
unchanged. Reference: [Template helpers](../reference/template-helpers.md#xmlescape).
Walkthrough: [Escaping free text in an XML template](../building/escaping-xml-in-templates.md).

## For distributions

`sutra_cli::product_version()` reads a distribution's version up to the first ` (`. A version
block that starts `2.0.0-rc.6 (1a2b3c4, built 2026-11-16)` reports `2.0.0-rc.6`, so the runtime
image tag, the release tag `create app` scaffolds and `codecs` no longer carry the build suffix. A
version block with the build on its own line works as before.
