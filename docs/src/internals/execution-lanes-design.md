# Execution lanes: the design

[Execution lanes](../architecture/execution-lanes.md) describes what lanes do. This chapter is the
reasoning: what the single-lane design was actually buying, how each of those properties was
preserved rather than re-argued, the one genuine cross-lane race that had to be fixed, and the
runtime shape that had to be falsified before the right one was found.

## What the single lane was buying

Before lanes, the engine was one actor thread draining one queue, with every store call **blocked
on** rather than awaited. That is easy to dismiss as an accident of an early implementation. It
wasn't — four properties rode on it, and each had to be accounted for.

Because a dispatch was synchronous end to end, the step's commit **happened-before** the reply to
the caller, and **happened-before** the next request was dequeued. From that:

1. **Reply implies committed.** A transport that got an answer knows the park is durable. This is
   what makes an acknowledgement mode meaningful at all.
2. **At most one store transaction in flight per process.** Connection-pool behaviour was trivially
   bounded.
3. **A park can never race its own completion.** The deferred-acknowledgement registration that
   follows a commit was serialised against every terminal event, because nothing else could run in
   between.
4. **The activation flip is atomic with respect to dispatches.** Nothing is ever observed
   half-flipped.

The honest observation — the one that set the whole sequencing — is that **blocking on a dedicated
thread is not itself the scaling defect.** The defect is having *one* serial lane, where a single
slow commit convoys every unrelated instance behind it. N lanes fix the convoy while changing
nothing else. Removing the blocking is a separate change that buys different things and should be
judged on its own.

So the work split: first N lanes with the blocking intact, then the conversion to awaiting, with
the ordering properties preserved verbatim at each step. Each half is reviewable on its own terms,
and a regression can be attributed to one of them rather than to "the concurrency change".

## The total-serialization audit

The interesting engineering was not writing a router. It was **enumerating everything that
implicitly relied on there being one thread** — because every such reliance was invisible,
uncommented, and correct right up until it wasn't.

The method was to walk each site and classify it into exactly one of four buckets:

| Verdict | Meaning |
|---|---|
| **Safe** | It relied on one *step* running wholly in one place, not on one thread existing. A step still does. |
| **Safe with a change** | Per-process scratch becomes per-lane scratch; a generated id gets the lane index mixed in so it stays unique. |
| **Accepted narrowing** | A per-process property becomes per-lane. Sound only where the production posture does not rely on it. |
| **Genuine race** | It relied on total order across *different* instances. Must be fixed. |

Most sites landed **safe**, and for one recurring reason: per-instance state has a durable home,
and a step runs wholly on one lane. In-memory audit sequence counters, for instance, are re-seeded
from the snapshot before every resume, so a stale entry on a lane that last saw an instance is
simply overwritten wherever the next resume lands.

The **accepted narrowings** were the ones worth writing down honestly, because they are real
degradations rather than non-issues. In-process correlation-alias uniqueness and in-process inbox
deduplication become per-lane instead of per-process. Both are only reachable in a persistence-less
posture (a dev or test engine with no datasource). Under any pooled production posture both are
database-backed — a unique index and a row lock — and already cross-*replica* safe, which strictly
implies cross-lane safe. Documenting the narrowing is the price of not pretending it doesn't exist.

One further site is a deliberate removal rather than a narrowing: **incidental cross-instance
serialization**. Two deliveries to two different instances of one flow never interleaved, as a side
effect of there being one lane. That was never a contract, but a deployment could have been leaning
on it. It is the reason the default lane count is one, and the reason the configuration page says
the sentence out loud rather than burying it.

## The one genuine race, and its inversion

Exactly one site was a real cross-lane race, and it is worth walking through because the fix is
smaller and stranger than the problem.

The deferred-acknowledgement registry lets a transport hold an inbound acknowledgement until the
instance actually completes. The original order was: **commit the park, then register the settle
callback.** Safe under one lane, because nothing could run between the two.

Under lanes it breaks. The park runs on the lane the delivery arrived on — call it A. The
instance's first relay routes to the instance's *owner* lane — call it B. B can claim, resume, and
**complete** the instance in the window between A's commit and A's registration. The terminal event
then fires with no registration present, the acknowledgement never fires, and the delivery dangles
until the registry's timeout sweep negatively acknowledges it. Microseconds wide, invisible under
load, catastrophic when it hits.

**The fix is to invert the order: register before committing, and deregister if the commit fails.**

The window closes cleanly, and the argument is the load-bearing part:

> Before the park commits, **no correlation alias row exists** — alias rows ride the step
> transaction. So no relay anywhere can correlate to this instance, and therefore no terminal event
> for it can fire from anywhere except this same dispatch, which is the one blocked on the commit.

A failed commit deregisters, and the caller sees the same error it always did. The registry's
documented invariant rotates from "registered ⇒ the transport was told to defer" to "the transport
was told to defer ⇒ registered ∧ committed" — which is the direction the transport actually depends
on, and arguably what it should have said all along.

At one lane the inversion is unobservable, which is why it landed unconditionally rather than as a
behind-a-flag branch. A correctness fix that only applies in one configuration is a correctness fix
you have to reason about twice.

The lesson generalizes: the ordering was safe because of a property (total order) that was never
written down as a requirement. Auditing for *implicit* reliance is the only way those surface
before production does it for you.

## Handoff rules

A relay does not carry an instance id. Resolving one needs the decode, the intake validation, and
the correlation expression — all of which need the lane-resident registries. So resolution happens
where the delivery arrived, and the resolved request is then handed to the owner lane. Two rules
constrain it, and both are structural rather than advisory.

**Lane loops never send into another lane's queue.** Only caller-side tasks do. This is what
removes the inter-lane deadlock case *by construction*: if lane loops could enqueue into each
other, lane A blocked sending into B's full queue while B is blocked sending into A's is a genuine
deadlock, and no amount of capacity tuning eliminates it — it just makes it rarer and therefore
worse. Routing the hop back out through the caller's own task means a full queue applies
backpressure to a transport, where backpressure belongs.

**At most one hop.** The lane that receives a resolved request runs it where it lands. It cannot
need a second hop — the instance id is fixed, so re-resolution would name the same lane — and more
importantly, correctness must not *depend* on the hop landing correctly. It doesn't: a wrong
landing bounces on the claim. The hop is affinity; the claim is correctness. See [Ownership and
claims](ownership-and-claims.md#routing-is-affinity-claims-are-correctness).

The receiving lane re-runs only the race-sensitive part — claim, load, guards, pin resolution,
resume. It does not repeat decode, validation, or correlation, which are deterministic over the
delivery and already done. Redoing them would be wasted work and, worse, a second place where those
semantics could drift.

## The runtime shape, including the one that was wrong

Three shapes were weighed for removing the blocking store calls.

**(a) One asynchronous loop per lane, awaited to completion per request.** Every persistence call
becomes awaited; the loop is "receive a request, await it fully, receive the next". Because the
loop awaits each request to completion before receiving the next, **every ordering property above
is preserved verbatim per lane** — commit still happens-before reply and before the next dequeue.
No completion re-entry exists, so there are no re-entry rules to get wrong.

**(b) Blocking loop plus I/O offload with completion re-entry.** Split a step at the commit, hand
the write to a worker, return to the loop, and re-enter a completion event later. This is the only
option that adds *intra-lane* pipelining — and it was rejected. It breaks reply-implies-committed
unless responders are parked on the completion; it needs per-key in-process mailbox holds, which is
a **second** serialization mechanism layered on top of claims; and its failure modes (a commit
landing after a crash-restart re-queued the work, a lost completion, a mailbox stuck held) are
precisely the subtle-ordering-bug class the design exists to avoid. Two mechanisms enforcing one
invariant is how invariants get broken.

**(c) N blocking lanes.** Zero semantic change, and N× I/O concurrency arrives from N threads.

**The order shipped was (c), then (a); (b) was rejected outright.** (c) makes the convoy fix
reviewable and bisectable on its own. (a) then removes the blocking as a mechanical conversion
whose diff is wide but whose semantics are provably unchanged. What (a) buys concretely: lanes park
on the runtime instead of holding threads when the pool is the bottleneck, an entire class of
"blocked inside an async runtime" panic disappears, and the door stays open to pipelining later
without another trait migration.

### The shape that was falsified

The first cut of (a) gave **each lane its own runtime with its own I/O reactor**. It looked like
the clean version: a lane is fully self-contained, owning its execution and its I/O.

Its own acceptance gate — the "one lane must be identical to before" bar — falsified it. When a
lane shut down, its reactor died **with** it, and pooled database connections registered on that
reactor were stranded: sockets nobody would ever poll again. The visible symptom was a restart
hang. The successor engine's lease acquisition waited out the full lease TTL before it could
proceed, roughly half the time. A restart-timing flake, not a wrong answer — the kind of thing that
gets triaged as flaky infrastructure for months.

The fix inverted the ownership: **lanes own no reactor at all.** Each lane's loop is driven on its
own dedicated thread via the *process-wide* runtime's handle, so the reactor topology is identical
to what it was before the conversion, and lane lifetime is decoupled from I/O registration
lifetime. Restart handover got materially faster as a side effect, because nothing waits out a
lease TTL any more.

Two things are worth taking from this. First, the "obviously clean" decomposition was wrong because
it decomposed the wrong resource: execution is per-lane, but **I/O registration is per-process**,
and conflating a lifetime with a locality is a recurring shape of bug. Second, the identity bar is
what caught it. An acceptance criterion of "one lane behaves exactly as before" is a much stronger
instrument than a suite of feature tests, because it fails on things nobody thought to assert.

## The activation flip

Activation rebuilds the engine's live view of processes, codecs, validators, and channels. Under
one lane that rebuild was a single request, atomic against every dispatch.

Under lanes the controller sends the rebuild to every lane and **awaits all of them** before
replacing the live deployment set and rewiring transports. Per-lane atomicity is preserved; that is
all that is needed, and the argument for why is worth being explicit about.

The flip never promised cross-*component* simultaneity in the first place. Even under one lane the
later stages — replacing the deployment set, reconciling schedules, rewiring transports — were
already non-atomic with respect to the actor swap; deliveries kept flowing between them. What was
promised, and is still promised, is **per-dispatch consistency**: nothing is ever observed
half-flipped.

Per-lane atomicity delivers exactly that, because **an instance's steps all run on its one lane**,
whose flip is a single point in its own queue. No instance can straddle two definitions. During the
fan-out window two *different* instances can be served either side of the flip — which is
indistinguishable from two deliveries ordered around a single flip point, which is what happened
before. The draining-deployment correlation tail is part of the rebuilt view and flips with it, per
lane.

## Where false confidence would have come from

The risks that mattered were not the ones the code made obvious. They were the ones where a green
test suite would have been actively misleading:

| Risk | Why a green suite would have lied | What actually catches it |
|---|---|---|
| Silent double-resume via re-entrant claim | One-lane suites cannot exercise it; cross-replica tests use *different* owners | Lane-scoped owner makes the window structurally impossible; a deliberate mis-route test proves the bounce |
| The park/completion acknowledgement race | The window is microseconds — stock tests pass almost always | The ordering inversion, plus a race test with an injected pause exactly in the window |
| Flip skew across lanes | Single-replica flip tests never produce skew | Await-all barrier, plus a flip-under-load test at several lanes |
| Ordering regressions from extracting the router | "It is just plumbing" reviews | A byte-identical outcome-sequence bar at one lane |
| Hot-instance skew pinning a lane | Averaged throughput hides it entirely | Per-lane queue-depth is a shipped meter, not a debugging afterthought |
| Pool exhaustion at N concurrent commits | Testing against a generous pool | A soak against a deliberately small pool |
| The per-lane narrowings surprising a persistence-less deployment | The tests exercise the pooled path | Documented, and unchanged under any pooled posture |

The pattern in that table is the transferable part: **for each risk, name the specific green result
that would have been misleading**, then design the test that would not have been. An
all-green-at-one-lane suite proves nothing whatsoever about cross-lane windows.

## Next

- **[Execution lanes](../architecture/execution-lanes.md)** — the operator-facing chapter.
- **[Ownership and claims](ownership-and-claims.md)** — the mechanism lanes lean on for
  correctness.
- **[Durable execution](durable-execution.md)** — what a lane commits at a quiescent point.
