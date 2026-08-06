# Migration internals

[Instance migration](../operating/instance-migration.md) is the operator's guide. This chapter is
the reasoning: a row-security problem with a silent failure mode, why compatibility is derived from
resume behaviour rather than from BPMN taxonomy, how the re-arm set is worked out, and why batch
independence is structural rather than promised.

## The two-scope problem

Every other commit in the engine runs inside **one** deployment scope: open a transaction, set the
scope, write, commit. Row-level security policies then confine every statement to that scope, which
is how tenant and deployment isolation is enforced at the database rather than in application code.

Migration breaks the pattern by definition. It **reads rows pinned to the source** and **writes
rows pinned to the target**.

The obvious implementation — a single `UPDATE` that sets the deployment id — **cannot work** under
an enforcing policy. The shipped policies are visibility predicates with no separate write
predicate, and the database then reuses the visibility predicate as the write check for an update.
So the statement fails at *both* possible scopes, and the two failures are not equally bad:

| Scope set to | What happens | Severity |
|---|---|---|
| **Source** | The old row is visible and passes, then the re-scoped new row is rejected by the implied write check. | An outright error. Loud, obvious, safe. |
| **Target** | The old row **is not even visible**, so the statement succeeds and matches **nothing**. | **Silent.** |

The second is the dangerous one, and it is worth dwelling on why. It does not raise. It does not
warn. It returns success having moved zero rows — so an operator running a migration would see it
succeed, and the instance would still be pinned to the broken model. Every downstream signal would
agree that everything was fine.

That is the failure mode this design exists to make impossible, which is why both halves are pinned
by a test running as a genuinely non-bypassing database role. The justification for the shape is
**executable rather than asserted** — a claim about what row security does under an enforcing role
is exactly the kind of claim that rots quietly.

## The scope flip inside one commit envelope

What makes the operation possible is that the scope setting is transaction-**local**, not
transaction-**immutable**. One transaction may re-scope itself between statements.

So the commit runs two phases inside one envelope:

1. **Scoped to the source** — lock the instance row, re-assert the ownership claim and the
   non-terminal status, read every row belonging to the instance, delete them.
2. **Scoped to the target** — insert the rewritten rows.

Atomicity is unchanged: a phase-2 failure rolls phase 1 back, and no session ever observes the
instance under both pins or under neither. Isolation is not weakened either, and the distinction is
precise — the transaction is never scoped to two deployments *at once*. It finishes with one, then
scopes to the other. There is no window in which a statement could see both.

The re-assertion in phase 1 is deliberate. The claim was taken before validation; re-verifying it
**under the row lock**, inside the commit, closes the gap between "claimed" and "moved".

## The snapshot moves as a byte-level patch

The snapshot rewrite is a key patch over the raw map — the same shape marking an instance failed or
terminal uses, and for the same reasons, plus one specific to migration.

A decode-and-re-encode would need the tenant's data-encryption key, would have to re-derive an
encryption set the resume-time snapshot no longer carries in that shape, and would persist a
previously-encrypted value **in the clear** the moment either went wrong. See [Durable
execution](durable-execution.md#the-byte-level-key-patchers).

That carried-through ciphertext still decrypts under the new pin is **not luck**. The
authenticated-data binding ties a ciphertext to its key, its instance and its variable name, and
deliberately **excludes the deployment id** — a property the encryption design chose precisely so
that "a version migration changes only the pin" would stay true. A design decision made in one
subsystem paying off in another, years later, is what a well-chosen invariant looks like.

**Rewritten**: the deployment pin; the wait frontier, completed set and routed start (each entry
mapped through the node mapping); the per-node retry attempt counters — the **key** is renamed, the
counter untouched, because a burned budget follows its task; the audit sequence floor, bumped past
the migration event.

**Untouched**: variables plain and encrypted, the key anchor and sensitive-name set, the status,
the failure keys, and **coverage cursors** — those last are keyed by *declared path id*, not node
id, so applying the node mapping to them would corrupt them. The distinction is invisible unless
you look, which is why it is written down.

## Compatibility is derived from what resume *does*

The locus model is the substance of validation, and its one real idea is this: **the question is
never "is this a `userTask`". It is "can a parked message wait resume here".**

Several distinct BPMN constructs answer yes to the second question, and the set is not the same as
any single element type. So each locus is classified by the resume behaviour it will actually get,
and the target node is checked against **capability flags** rather than an element enum.

Two classifications carry the weight:

**A retry park is not merely a timer park.** It is a timer row whose node has a durable attempt
count and is absent from the completed set — which is exactly the condition the executor itself
uses to route a due timer to *re-run the task* rather than to *fire the timer*. So the compatibility
rule reuses the executor's own test rather than reinventing one. Landing a retry park on a merely
timer-capable node would silently re-run nothing.

**A continue-reply park is read from the source graph.** Whether a parked node carries a
respond-and-continue reply is a property of the definition the instance is running under, not of
the target. Which is why a source deployment whose plan is no longer registered is a **hard
refusal** rather than a fallback: validating a continue-reply park against the timer rule would pass
the wrong check and produce a confidently-wrong verdict. Refusing to answer beats answering wrong.

The capability index each plan projects is small and computed at activation, so validation never
re-verifies and re-plans two sealed archives on an administrative request path. It covers draining
deployments as well as active ones — a migration's *source* is by definition a deployment that has
been flipped away from. Sub-process bodies are flattened into their parent's index, because durable
state records a node id with no scope path.

### Every violation, not the first

The validator reports **all** violations. Fixing a mapping should take one round trip, not one per
node — a validator that stops at the first error turns a ten-node rename into ten deploy-test
cycles.

The same instinct drives the refusals that could have been silent: a mapping entry naming a node
the instance neither parks at nor has completed is **refused**, because a typo must never read as
"identity mapping, then".

## The resume re-arm set: why the frontier alone is wrong

`resume: true` brings a `FAILED` instance back. Working out *what to re-arm* is the subtlest part of
the whole operation.

The failure commit did two things: key-patched the snapshot to `FAILED` with its failure code and
detail, and **resolved every waiting row in one statement** so no timer refires and no relay finds a
live wait. The frontier itself was left untouched.

Resume is the inverse of both halves. The snapshot half is straightforward — status back to
suspended, failure keys dropped, output byte-identical to the pre-failure snapshot because the
failure keys are emit-only-when-present, and like every other re-stamp it is a raw patch, so
reviving an instance never needs the tenant key and can never downgrade its at-rest protection. It
fails closed on any status but `FAILED`.

The rows are the hard half, and **the frontier is not enough**. A `<q:timeout>` synthesizes a
boundary with a derived id, and a timer boundary event has its own id — **neither appears in the
frontier**. Re-arming by frontier alone would silently drop the timeout the park was armed with,
producing an instance that resumes and then waits forever with no deadline.

The rows are recoverable exactly, though, and the derivation is a nice piece of reasoning about
what the data already tells you:

> They were resolved by **one statement**, so they share a single resolution timestamp — and it is
> the instance's **latest**, because nothing touches a `FAILED` instance's rows afterwards (the
> poller's stale and failed branches only update rows still waiting).

So the re-arm set is **the frontier's own rows, plus every row sharing the instance's latest
resolution timestamp**. It is gated on a non-empty frontier: an instance that had nothing parked has
nothing to restore, and an older satisfied wait is spent history rather than a park.

The parks are re-armed **in the migration's own transaction**, so migrate-and-resume is one commit
and a crash cannot leave a half-revived instance.

Afterwards the instance comes back through **ordinary** paths: the row is unowned (the claim died
with the source row), the snapshot is suspended, and the parks are armed. A re-armed timer whose
instant has passed is claimed by the ordinary poller on its next tick; a message park waits for its
next correlated inbound. There is **no new resume entry point and no privileged re-drive**, which is
precisely why the claim-guarded concurrency story still holds afterwards.

### A defect this closed

The first version derived loci from waiting rows that were still marked waiting. For a `FAILED`
instance there are none — its failure commit resolved them all. So every frontier entry fell through
to the "no row behind it" branch and was validated as a **message** wait. A dead instance parked on
a timer was therefore checked against the wrong rule, and warned about a missing row that was
sitting right there.

Since a `FAILED` instance is the operation's *prime* use case, that was not a corner case. Reading
the park set as described above fixed it — and note the property that makes the fix trustworthy:
**what validation checks is now exactly the set resume re-arms.** One derivation, two consumers, no
possibility of drift.

## Batch independence is structural, not promised

The batch endpoint is not a second implementation. A request body parses **once** into a migration
plan; a single internal operation applies that plan to exactly one instance and returns a
**verdict** rather than an HTTP response; and both endpoints are thin wrappers over it.

So "each instance validates and migrates independently" is not a promise the batch endpoint makes
and must be trusted to keep. It is **structural**: the batch is a loop over the single-instance
operation, and **the attempt is the loop's return value**. There is no shared mutable state for one
instance's outcome to leak into another's, because there is nowhere to put it.

Everything else follows:

- **One claim, one transaction, one report entry per instance.** No batch-wide transaction — which
  is the point, and which makes the crash story trivial: a mid-batch crash leaves every instance
  either fully migrated or completely untouched, because that is already true of each instance's own
  commit and there is nothing larger to be half-applied.
- **The outcome is a single enum**, not a combination of booleans, so no caller has to infer a state
  from flags that might contradict each other.
- **The status describes the batch, not its instances.** A run where every instance refused still
  answers `200`: the call was accepted, executed to completion, and reported in full. Scripts key on
  the totals and the per-instance outcomes.

### Contention is reported, never retried

An instance whose claim is held bounces, and the batch moves on. Retrying inside the call is refused
on two grounds, and both are about honesty rather than convenience:

- It makes an administrative request's runtime **unbounded** — the claim is held by another
  replica's work, on *that* work's timescale.
- It turns the report into a claim about **a moment that has already passed**. A report that says
  "everything moved" after internally retrying for thirty seconds describes a world that no longer
  exists by the time it is read.

The caller re-runs the same request instead. The selector is deterministic and whatever moved is no
longer under the source pin, so **a re-run converges** — which is a much better contract than "we
tried hard".

Selection is ordered by **instance id**, then limited. Ordering by a timestamp that moves under a
live population is exactly how a caller silently skips work, and a paging bug that skips work
without saying so is the worst kind.

Two request-level contradictions are caught from the request alone rather than N times over in the
per-instance reports — resuming a selection of suspended instances (it selects exactly what resume
refuses), and re-homing a mixed population into one process (one mapping could only be right for
one of them).

## What deliberately does not move

Pending outbound rows stay under the source pin. An emission was minted by the source deployment's
channel bindings and is dispatched against them — and the dispatcher covers draining deployments, so
**it drains where it was made**. Re-targeting a message at bindings that never produced it would be
worse than leaving it: the destination, headers, and codec were all decided by a configuration the
target may not even have.

The audit journal moves scope but keeps its node ids **verbatim**. A trail names where something
happened, under the graph that was live at the time; rewriting the ids would falsify the record. The
migration event itself records source and target, so the provenance is recoverable without
corrupting the history.

Both are reported — the outbound rows as a warning — because the operator should know why, not just
what.

## Next

- **[Instance migration](../operating/instance-migration.md)** — the operator's guide.
- **[Durable execution](durable-execution.md)** — the patchers and the encryption binding this
  relies on.
- **[Ownership and claims](ownership-and-claims.md)** — why the migration claim needs its own owner
  identity.
