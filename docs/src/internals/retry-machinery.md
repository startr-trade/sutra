# Retry machinery

[Retries, history, and schedules](../building/retries-history-schedules.md) covers what `<q:retry>`
does. This chapter is why it is built the way it is — and the channel-call half turned out to have
a genuinely hard problem in it.

## A backoff is a park, not a sleep

The first decision constrains everything else: **a retry wait is a durable timer park.**

The alternative — sleep in place and try again — is not merely inelegant here, it is unavailable. A
lane executes one request at a time, so an in-path delay does not delay one instance; it freezes
every instance queued behind it on that lane. A ten-second backoff would become a ten-second stall
for unrelated work. Even with lanes, a sleeping lane is a lane doing nothing while holding a queue.

So a failed attempt with budget remaining **re-parks** the instance: the failed task stays pending,
a timer row is armed at the backoff instant, and the ordinary timer poller re-drives it when the
instant arrives. Deliberately the *same* seam that respond-and-continue already uses — not a
parallel mechanism. A second scheduling path would be a second set of races to reason about, and
the existing one already survives restarts, hot-deploys, and replica death.

Everything good about the behaviour follows from that one choice. The backoff survives a restart. A
`PT5M` wait is `PT5M` whether or not the replica that started it is still alive. No execution
capacity is consumed while waiting. And a retry park is pinned to its deployment exactly like every
other park, so a hot-deploy cannot silently move a backing-off task onto a different definition.

The cost is honest and stated: **a `<q:retry>` task makes its process stateful.** The park needs
persistence. A process that was otherwise run-to-completion now requires a datasource, and the
structural classifier accounts for it rather than discovering it at runtime.

## Attempt state lives on the snapshot

Attempt counters are **snapshot keys**, one per node, not a column on the waiting row. Three
reasons, in increasing order of importance.

**A column would be erased by the very step that increments it.** The retry park resolves its
timer row and creates a new one each attempt. Attempt state stored there would be destroyed by the
re-park — the mechanism would delete its own bookkeeping.

**Attempt state is instance state.** It must ride the failure re-stamp untouched when the instance
eventually fails, so that an operator inspecting a dead instance can see how many attempts it
burned. Putting it on the snapshot gets that for free, because the failure re-stamp is a key patch
over the snapshot's map.

**It costs zero migration and stays byte-identical when unused.** The snapshot container is an
open-keyed map, so a new key family needs no schema change anywhere. And because the key is absent
unless a task actually has a policy, **a process with no `<q:retry>` writes byte-identical
snapshots to one that predates the feature** — which keeps a byte-for-byte golden corpus valid
across the whole feature. A feature that changes the persisted bytes of flows that don't use it is
a feature that cannot be verified cheaply. See [Durable
execution](durable-execution.md#the-typed-value-encoding) for the encoding and the patchers.

A malformed counter reads as zero rather than failing the load, for the same reason the rest of the
decode path is lenient: a snapshot must never become unloadable.

## The channel-call problem

Extending `<q:retry>` to channel-call tasks was initially skipped, on the theory that the outbox's
own retry curve plus the timeout boundary already covered them. A survey of what the code actually
did disproved it:

- the outbox curve retries **one delivery**, by default forever — it is about reaching a
  counterpart, not about the task having another go;
- a timeout without a policy simply **kills the instance**.

So there was a real gap: no way to say "if the counterpart doesn't answer, ask again". Closing it
meant answering a question the registered-task case never has to: **what, exactly, is a failed
attempt of a call?**

### The honest failure set

Derived from what the dispatcher and executor actually do, and nothing else:

1. **The route-less `<q:timeout>` boundary firing.** With a policy present the timeout is a
   retryable task failure *first*. Without one, the pre-existing catchable-error behaviour is
   preserved byte for byte.
2. **A terminally-poisoned request delivery.** The outbox exhausted its configured attempt ceiling
   while the task waited. Before this existed, the instance simply stayed parked forever — the
   delivery had given up and nothing told the task.

Both are stable structured codes, which is what makes them expressible in `nonRetryableCodes`.
("Never retry timeouts" is a reasonable policy, and it has to be sayable.)

**Not failures**, deliberately:

- **A correlated business response.** The counterpart answered. What the answer *says* — approved,
  declined, rejected — is the process's business to route on with a gateway. Retrying because a
  business answer was unwelcome would **double-submit**, and re-issuing an instruction because the
  first answer was "declined" is exactly the class of bug an engine must make structurally
  impossible.
- **BPMN errors.** They route to their boundaries, unchanged, on either task kind.

### Modelled outcomes beat policies — enforced at load

A timer boundary *with drawn outgoing flows* is a modelled outcome: the author has said what
happens. So it wins, exactly as a BPMN error routes to its boundary rather than consuming a retry
budget.

Which means a channel-call `<q:retry>` alongside a routed timer boundary is a policy that could
never fire. Rather than let that load as a silent near-no-op, the **loader refuses the
combination**. A configuration that cannot do anything should not be accepted quietly; the author
believes they have retries and does not.

## The hard part: a dead attempt and an in-flight one look identical

Here is the problem that shaped the rest of the design.

Consider a channel-call node in a backoff window — its previous request failed, it is waiting to
re-issue — and a channel-call node whose request is in flight right now. Through the durable facts
available (the wait frontier plus the attempt counter) **they look exactly the same**: a node
waiting, with attempts already burned.

They demand opposite treatment. A late response to the dead attempt must be refused; a response to
the live attempt must resume the flow. A due timer on the first is the re-drive; a due timer on the
second is stale residue.

The registered-task shortcut does not work here. For a registered task, "attempts burned and not
completed" *implies* a backoff window. For a call node it does not, because a counterpart's relay
can name the node mid-retry.

### The backoff-window marker

The resolution is one more durable key: a **marker** set while a node is in a backoff window,
carrying the classification of the failure that parked it. It is carried exactly like the attempt
counters — an open-keyed snapshot entry, no migration, absent by default so unaffected snapshots
stay byte-identical, and renamed along with the node mapping when an instance is migrated.

It keys four decisions, each made under the instance claim from durable facts alone:

| Situation | Verdict |
|---|---|
| A due timer on a **marked** call node | The backoff **re-drive**. Explicit, because the registered-task inference is unsound here. |
| A due timer on an **unmarked** call node | Stale residue. Resolved, never driven. |
| A timeout boundary firing on a **marked** host | Stale — a poisoned delivery beat it to the failure. Firing would double-count one attempt. |
| A response correlating to a **marked** node | **Refused** (`SUTRA.DISPATCH.CHANNEL_CALL.RETRY_PENDING`) — it belongs to the dead attempt. |

That last row has a deliberate sub-decision: the correlation row **stays live** rather than being
retired. A counterpart answering a superseded request gets an honest "that attempt is gone, a retry
is pending" verdict instead of a "no such instance" miss — the same posture durable `FAILED` takes,
where the truth is more useful than a 404. And when the re-drive re-emits, the same correlation
serves the new attempt's response normally, with no re-registration.

## The re-emission contract

A re-drive is not "wait for the same answer again". It re-runs the park's side effects from durable
state:

- a **fresh** request emission, built over the same persisted variables, with a **fresh idempotency
  key** on the wire;
- a **fresh** timeout window;
- the response wait **re-incarnated** — a node both resolved and re-parked in one step is a new
  incarnation, and is recorded as such rather than as an update of the dead one.

The fresh idempotency key is the subtle one and it is deliberate. The retry is a genuinely new
request, and a counterpart doing its own deduplication must see it as new. Reusing the key would
invite the counterpart to answer "already handled" for an attempt it never actually completed —
turning the retry into a silent no-op precisely when it matters.

### Withdrawing the dead attempt's deliveries

The backoff park **deletes the dead attempt's outbox rows** — pending and poisoned alike — in the
same transaction that arms the backoff.

This is a deliberate, narrow exception to an otherwise absolute rule: *the outbox never deletes an
undelivered row*. The rule exists because a deleted undelivered row is lost work with no trace, and
it is a good rule. Two concrete races justify the exception:

- **A superseded request delivered late** would race the re-drive's fresh emission into a
  **double-submit** — two live requests for one logical call.
- **A superseded request poisoning later** would fire a failure against the **live** attempt,
  consuming a budget slot that attempt never spent.

Both are worse than the loss the rule protects against — and the loss does not actually occur here,
because the durable record of the failure survives elsewhere: in the marker, in the attempt
counter, and in the incident the poison already recorded. Nothing is forgotten; only the *pending
delivery* is withdrawn.

The general shape is worth naming: an exception to a safety rule is defensible when you can point
at the specific corruption the rule would cause, and show the information the rule was protecting
is preserved by another mechanism.

## The poison wake is a prompt, never a fact

When the outbox gives up on a request, the task waiting on it needs to hear about it. The
dispatcher fires a best-effort in-process wake — a fresh serialized turn, like a timer fire.

But the wake is treated as a **prompt to go and look**, never as evidence. The engine acts only on
**durable evidence re-read under the instance claim**: a poisoned outbox row for exactly this
instance and this node. A wake that arrives spuriously finds nothing and does nothing.

And a wake that is **lost** — a crash, a shutdown, a dropped in-process message — is recovered by
the timeout boundary, which the loader guarantees every channel call has. That guarantee is what
lets the wake be best-effort at all: the fast path is a notification, the correctness path is the
boundary the model was required to declare anyway.

This is a shape worth reusing. An in-process notification that races with a crash is fine as long
as (a) the receiver re-derives the fact from durable state, and (b) something durable eventually
detects the same condition without the notification.

## The whole thing is claim-arbitrated

Every decision above — relay versus timer versus poison, marked versus unmarked, re-drive versus
resolve — is made **under the instance claim**, from durable facts. The races between a late relay,
a due timer, and a poison notification arbitrate through the claim exactly as cross-replica races
do; the marker only tells a holder *which* situation it is in, never who gets to act. See
[Ownership and claims](ownership-and-claims.md).

The determinism discipline is unchanged throughout: no lane-blocking sleep anywhere, attempt state
on the snapshot, every re-drive decision from durable facts, and byte-identical snapshots for every
process that never backs off a call.

## Exhaustion

A spent budget — or a `nonRetryableCodes` hit — lands on the same fatal path a registered task's
exhaustion does: a durable `FAILED` snapshot carrying the structured code. Identical for both task
kinds, deliberately, because "how many kinds of failure state does this engine have" should have
exactly one answer.

From there the instance is retained, inspectable, blocks its deployment's retirement, and is
repairable by [migration](migration-internals.md) — which is the loop the whole design is pointed
at: fail loudly, keep everything, let an operator fix the model and bring it back.

## Next

- **[Retries, history, and schedules](../building/retries-history-schedules.md)** — the
  author-facing chapter.
- **[Migration internals](migration-internals.md)** — how a burned budget travels with its task.
- **[The pull surface](pull-surface-design.md)** — the other retry budget, and why they never
  overlap.
