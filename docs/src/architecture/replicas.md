# Replica semantics

Sutra runs as an **active-active stateless replica set** — all durable state lives in PostgreSQL,
and no replica owns any particular instance, tenant, or channel by default. Any replica can pick up
any token at any time, which is what lets the engine scale horizontally with no sticky routing.

## What every replica does

Each replica is one engine process running inbound channel listeners (the HTTP server, broker
consumers), the token executor for instances it picks up from persistence, an outbox worker
sending replies it claims, and the health/metrics endpoints. Execution inside the process runs on
one or more identical actor lanes — see [Execution lanes](execution-lanes.md); lanes are an
in-process concern and change nothing on this page. Three pieces of work don't tolerate
concurrency and are held by exactly one replica at a time instead: **timer firing**,
**stuck-instance scanning**, and **terminal-history purging**.

## Leader election: a PostgreSQL-backed lease

There is no Kubernetes-native `Lease` object and no separate coordination service — leader
election is a lease row in the engine's own PostgreSQL database (`DbLeaderElection` over
`PgLeaseStore`). One poll task per **role** tries to acquire the lease at a fixed cadence
(`ttl = 30s`, `poll = 10s`); a successful acquire flips that replica to leader for that role, a
contended one flips it to follower. There's no push notification — polling *is* the mechanism.

Roles are dynamic, not a fixed pair: the engine registers one lease per **singleton channel
role** (`channel_role(tenant, channel)`) the first time it's needed, so declaring a channel
`singleton: true` (see [Channels and transports](../building/channels.md) and the
[money-transfer example](../building/worked-example.md)) starts contending a lease without any
separate configuration. When no engine datasource is configured at all, every replica simply
leads unconditionally (`AlwaysLeading`) — there's no third posture to reason about.

## Instance ownership claims

Leases gate *roles*. What keeps two replicas from advancing the **same instance** at the same
moment is a different, finer mechanism: a per-instance ownership claim, taken by a
compare-and-set on the instance's own row.

**Every resume path claims first.** A correlated relay arriving on a channel, a timer firing, an
administrative migration — each one takes the claim before it rehydrates anything, and a claim it
cannot take is a refusal, never a wait. The refusal is retry-safe by design and carries a
structured code (`SUTRA.RUNTIME.RESUME.CLAIM_HELD`, or the admin surface's
`SUTRA.ADMIN.MIGRATE.CLAIM_HELD`): a broker relay is requeued for redelivery, a timer fire is
deferred to a later tick, an admin call answers `409` having read and written nothing. Nothing is
ever half-applied while contended, and nothing blocks waiting for a lock held on another
replica's timescale.

The claim is released inside the same transaction that commits the step's own writes, so
"committed" and "unowned again" are one atomic fact. Every exit that does *not* commit — an early
refusal, an error, a panic — releases the claim explicitly on its way out, so a claim never
outlives the work it was protecting.

**The claim is re-entrant for the same owner**, and the invariant that makes that safe is worth
stating exactly: an owner id names one *execution lane* in one process, and a lane advances
instances one at a time. Same owner therefore means same lane, which already means serialized —
so re-claiming what you already hold is a heartbeat refresh rather than contention. Owner ids
carry the lane index for exactly this reason, and the administrative migration path takes a
deliberately *distinct* owner suffix so that a migration can never re-enter past a resume this
same replica has in flight.

**The stuck-instance sweep** is the backstop for the case no protocol can prevent — a replica
that dies mid-step and never releases. The `instance-sweeper` lease role scans on
`sutra.instance.sweep-interval` (default `PT1M`) and clears any claim whose owner has been silent
longer than `sutra.instance.claim-timeout` (default `PT5M`). It sweeps by age and is
owner-blind, so it needs to know nothing about how owner ids are shaped.

## Durable `FAILED` is a state, not a disappearance {#durable-failed-is-a-state-not-a-disappearance}

An instance whose execution fails fatally is not deleted and does not silently linger as a
mystery park. Its snapshot is re-stamped **`FAILED`** in place, carrying the structured failure
code and the captured detail, and every waiting row it held is resolved in the same transaction —
so no timer refires against it and no relay finds a live wait to satisfy.

`FAILED` is deliberately **not** terminal:

- it is always retained, regardless of the history-retention window, because it needs an operator
  rather than a clock;
- it keeps blocking its deployment's retirement (below), so a dead-but-unhandled instance cannot
  be quietly swept under a hot-deploy;
- it is the one state an instance can be [migrated](../operating/instance-migration.md) *and*
  resumed out of — repair the model, move the instance onto it, then explicitly bring it back.

Restoring it is exactly the inverse of the failure commit: the failure keys are dropped, the
status goes back to suspended, and the parks the failure tore down are re-armed — after which the
instance comes back through the ordinary claim-guarded paths. There is no privileged resume entry
point.

## Inbox dedup via row locks

An inbound message's `(tenant, channel, event_id)` triple is inserted with `ON CONFLICT DO
NOTHING`; whichever replica's insert actually produced a row owns starting that instance, and any
other replica that raced it (or a genuine redelivery) sees nothing came back and treats it as a
dedup hit. This is a plain unique-index conflict — no application-level locking, and it works
identically whichever replica happens to receive the redelivered copy.

## Outbox processing: `SKIP LOCKED` fan-out

Every reply-in-waiting sits in an outbox table; each replica's outbox worker claims a batch with
`FOR UPDATE SKIP LOCKED`, so replicas never contend on the same row and never double-send under
normal operation. A replica that dies mid-send leaves its claim stale; another replica's next
scan clears the stale claim and retries — broker-side dedup (the outbox row's key riding as the
message's own idempotency token) absorbs the rare case where the original send actually went out
before the crash.

## Retiring a draining deployment: a three-legged gate {#retiring-a-draining-deployment-a-three-legged-gate}

A hot-deploy leaves the previous graph **draining** rather than deleting it, precisely so
instances pinned to it keep resuming on the definition they started under. It retires only when
it is genuinely quiescent — and "quiescent" is three independent facts, all of which must hold:

1. **No active instances** pinned to it. Retained terminal history does not count (it would
   otherwise pin a deployment open for a whole retention window); `FAILED` instances *do* count,
   because they are live work awaiting an operator.
2. **No pending outbox rows** minted by it. An emission belongs to the channel bindings that
   produced it, so it drains where it was made. A delivery that exhausted an opt-in attempt
   ceiling is marked terminally poisoned and stops counting — "we gave up" is a durable, visible
   state, and it must not hold a deployment open forever.
3. **No parked external tasks** waiting on it. A task parked for a pull worker is outstanding work
   with no in-flight anything to observe; retiring underneath it would strand the worker's
   completion. Tasks that turned terminal after exhausting their worker budget stop counting, for
   the same reason poisoned deliveries do.

All three legs are database-scoped counts, so the gate reads the same from every replica and is
unaffected by which replica happens to run the sweep.

## Recovery on pod death

Because token state lives in the database, a dead replica leaves nothing to recover from memory:

- **Mid-token-execution** — the instance's claim goes stale with its owner; the stuck-instance
  sweep clears it and the next replica to see ready work picks the instance up.
- **Mid inbox-to-instance handoff** — an inbox row without a matching instance past a recovery
  threshold gets retried; the same unique-index dedup makes the retry safe.
- **Mid outbox send** — the stale `claimed_at` is cleared and the row is retried by whichever
  replica's scan finds it next.
- **The leader itself dies** — its lease expires within its TTL and another replica's next poll
  acquires it; timer scheduling pauses for at most that window, while outbox processing continues
  uninterrupted on every other replica throughout.

## Scaling signal

CPU is not a meaningful autoscaling signal for an event-driven engine — the right one is backlog
depth (inbox rows waiting to start an instance, outbox rows waiting to send), which is what a
KEDA `ScaledObject` querying the engine's own tables drives off. See the OpenTofu module under
`deploy/modules/sutra/` for the shipped shape.

## Non-goals

No active-passive mode (every replica processes real work; the lease only gates the singleton
pieces), no sticky session routing, no tenant-to-replica affinity (isolation is a PostgreSQL RLS
concern — see [Multi-tenancy and isolation](multi-tenancy.md) — not a placement one), and no
engine-managed broker resources (queues/topics are provisioned by your own infrastructure
tooling; the engine connects to what already exists).

## Next

- **[Execution lanes](execution-lanes.md)** — the in-process half: N actor lanes under one
  replica, and why claims rather than routing keep them correct.
- **[Multi-tenancy and isolation](multi-tenancy.md)** — the other half of "many tenants, one
  engine, one database."
- **[Deployment model](deployment-model.md)** — how the same PostgreSQL-backed convergence pattern
  (`LISTEN`/`NOTIFY`, version polling) applies to deploy activation across a fleet.
- **[Ownership and claims: the design reasoning](../internals/ownership-and-claims.md)** — the
  compare-and-set, the re-entrancy invariant, and the owner-suffix conventions.
