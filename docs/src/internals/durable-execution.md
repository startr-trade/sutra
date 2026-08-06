# Durable execution: snapshots and typed values

This chapter is the *why* behind what a parked instance actually is. If you only need the
observable behaviour, [Wait states and human tasks](../building/wait-states.md) has it; this is
the reasoning underneath.

## A snapshot at a quiescent point, not an event log

Sutra persists an instance as a **snapshot taken at a quiescent point** — the moment execution has
nothing left to do until the outside world answers. It does not persist a log of events and it does
not reconstruct state by replaying one.

The two models differ in what they make cheap and what they make hazardous.

An event-sourced engine rebuilds state by re-running history. That gives it a free audit trail and
free time-travel, and it buys them with a permanent constraint: **the code that produced the
history must keep being able to reproduce it.** Every task function, every expression, every
library the flow ever touched becomes part of the replay contract, forever, for every instance
still alive. Determinism stops being a property of one execution and becomes a property of every
version of the engine and the application, jointly, across time.

A snapshot engine stores what the instance *is* rather than how it got there. The replay contract
shrinks to a single step: from this state, given this input, do the next thing. Nothing older than
the current snapshot is ever re-executed, so an upgrade cannot retroactively change what already
happened.

The audit trail an event-sourced engine gets for free is then an explicit, separately-configured
concern here — the audit journal — and that turns out to be the honest arrangement anyway: an
audit record has different retention, different access control, and different redaction
requirements from execution state. Conflating them means one policy has to serve both.

The properties that fall out of the choice:

- **A step is one transaction.** The snapshot write, the waiting rows, the correlation aliases,
  and the outbound emissions all commit together, or none of them do. There is no window in which
  an instance has parked but not registered its correlation, or emitted but not recorded that it
  did.
- **Encoding is deterministic.** Identical logical state produces identical bytes: the container is
  sorted, and every value has exactly one canonical rendering. Re-encoding a decoded snapshot is
  byte-identical — which is what makes migration and rollback verifiable rather than hopeful.
- **The frontier is explicit.** A snapshot names the nodes it is waiting at, the nodes it has
  completed, and the start node it was routed through. Resume is "replay the completed set as
  done, satisfy this wait, continue" — a bounded operation over the recorded frontier, not a
  re-execution of the past.

## The typed value encoding {#the-typed-value-encoding}

Instance variables ride inside the snapshot container. How they are encoded is the part with the
most design in it.

### The defect it fixes

A parked instance used to flatten every variable to its **display string**. A number became
`"1250.75"`. A boolean became `"false"`. A list became `"[1, 2]"`. A null became the empty string.
Resume restored all of them as strings.

That is not cosmetic. FEEL — correctly — does not coerce a string in arithmetic or comparison, so
an exclusive gateway re-evaluating `amount > 100` after a wait compared a string to a number and
got `null`, which a gateway condition reads as `false`. A transaction that plainly exceeded its
limit took the *under-limit* branch. `not(approved)` was worse: `"false"` is a non-empty string, so a
restored boolean was never false again. **The same wait state that made the engine durable made its
decisions wrong.**

The loss happened in exactly two places — the park ran every value through the display formatter,
and resume wrapped every restored value back into a string — and both are gone.

### The encoding

A typed value is `<tag>|<payload>`: one ASCII tag byte, a separator, then the payload. Only the
**first** separator is structural, so a string whose own text is `n|42` round-trips unambiguously
as `s|n|42`.

| Tag | Type | Payload |
|---|---|---|
| `z` | null | empty |
| `b` | boolean | `true` / `false` |
| `n` | number | canonical decimal text, scale-faithful |
| `s` | string | the raw text |
| `d` | date | ISO-8601 date |
| `t` | time | FEEL time literal body |
| `i` | date and time | FEEL date-and-time literal body |
| `u` | duration | ISO-8601 duration |
| `j` | list / context | JSON |

The separator is chosen to be a character the container's own escaping never touches, so a tagged
scalar costs exactly two bytes and the row stays legible to an operator reading it:

```
sutra.var.amount=n|1250.75
sutra.var.approved=b|false
sutra.var.cancelledAt=z|
sutra.var.inboundId=s|INB-7
sutra.var.lines=j|[1,{"sku":"A-1"}]
```

Lists and contexts ride JSON. The four temporal types have no JSON counterpart, so they ride a
single-key object (`{"@d":…}` and siblings), and a user context key that would collide — any key
starting `@` — is escaped by **doubling** its leading `@`. The two are never confusable in either
direction.

### Why the generation integer says 4, not 3

The snapshot format carries a generation integer, and typing takes **4**.

Generation `3` was already spent. It is the generation at-rest encryption introduced, where the
integer means *this snapshot carries ciphertext*. Redefining `3` to mean *typed* would misread
every encrypted row already persisted — and distinguishing the two by probing values for something
that looks like a tag is exactly the kind of heuristic a persisted format must never depend on.

So the conflation stops at 3. Encryption has **always** been detected structurally, off the
ciphertext key prefix — the read path never consults the generation integer for it at all — which
means `4` means one thing only: the value encoding. An encrypted snapshot with typed values is
simply `4`. There is no generation for "typed and encrypted", and there never needs to be.

### Compatibility: lenient decode, lowest-generation encode

**Decode is version detection, not migration.** An older snapshot is not tag-decoded at all: every
value becomes a string, byte for byte what it was — *including* a legacy value that happens to look
like a tag. An old row read and re-written untouched reproduces its original bytes. There is no
silent upgrade on load.

**Encode emits the lowest generation that can carry the state**: `4` when at least one variable
actually needs the typed form, else `3` when something is encrypted, else `2`. An instance whose
variables are all strings therefore still writes the exact bytes it wrote before typing existed.

That asymmetry is doing real work. It keeps a byte-for-byte golden corpus valid across the feature,
and — more importantly — it lets a **fleet mid-upgrade** write rows an older replica can still
read. A rolling upgrade does not need every replica to reach the new version before anything is
safe.

The decode path is also deliberately lenient in one further respect: a malformed engine-internal
counter reads as zero rather than as an error. **A snapshot must never become unloadable.** An
instance that cannot be read is an instance no operator can inspect, migrate, or terminate — a
strictness that turns a small corruption into an unrecoverable one is the wrong trade.

### The byte-level key patchers

Three operations rewrite a stored snapshot without decoding it: marking an instance **failed**,
marking it **terminal**, and **re-pinning** it during migration. Each patches the raw key/value map
in place — engine-internal keys only — and never touches a variable value.

This is deliberate, and the reason is a security one rather than a performance one. Decoding and
re-encoding a variable would need the tenant's data-encryption key. That means:

- an operation as mundane as "mark this instance dead" would acquire a dependency on key
  availability, and would fail — or worse, *partially* succeed — when the key backend is down;
- it would have to re-derive which variables are sensitive, from a snapshot that at resume time no
  longer carries that set in the shape the encryptor wanted;
- and if either went wrong, it would persist a previously-encrypted value **in the clear**.

Marking an instance dead, finished, or re-pinned must never need the tenant key and must never be
able to downgrade an at-rest value to plaintext. Treating every variable as an opaque string is how
that is guaranteed structurally rather than promised procedurally.

Typing does not weaken the property, by construction: to the patchers a value is still an opaque
string. A snapshot carrying a tag this codec does not recognise rides a patch through **untouched**
rather than being "repaired" — a forward-compatibility posture the tests pin explicitly.

### Encryption: the tag rides inside the ciphertext

At the typed generation, the plaintext that gets encrypted is the **tagged** form. A sensitive
number is a number again after the resume, exactly like a non-sensitive one.

The alternative — an outer tag sitting beside the ciphertext — was rejected because it would
disclose the *type* of every encrypted value to anyone who could read the row, for no benefit at
all. "This encrypted field is a number, and that one is a list" is information, and it is
information the row does not need to carry.

Everything else about the envelope is unchanged. The authenticated-data binding still ties a
ciphertext to its key, its instance, and its variable name, and still deliberately **excludes** the
deployment id — which is precisely what keeps "a migration changes only the pin" true, and what
lets a migrated instance's encrypted values still decrypt (see [Migration
internals](migration-internals.md)). Decode still fails closed on a missing cipher or a failed
authentication.

### What is deliberately *not* typed

- **A subject blind-index input.** A subject value feeds a keyed hash over index rows that are
  already persisted. It keeps hashing the exact string it hashed before typing existed; deriving it
  from the typed value would orphan every index entry written to date — and those rows are how
  erasure and disclosure find an instance at all.
- **The operator inspect projection.** A published contract: every variable still renders through
  its display form. Typing changed what survives a wait, not what a response looks like.
- **Functions and ranges.** A FEEL function closes over an evaluation context that ceases to exist
  the moment the instance parks; a range is a comparison shape rather than instance state. Both
  persist as their canonical string — which is what they always did, so nothing regressed. They
  simply did not become typed.

### The one behaviour change a consumer can observe

Every restored value now evaluates as its real type. Where a gateway condition's answer differs
from a pre-typing release, **the previous answer was wrong** — that is the entire point of the
change. The only other visible difference is that a null variable now restores as null (and renders
as `null` rather than blank) instead of restoring as an empty string.

## The snapshot key registry

Everything a stored snapshot's properties map can carry, in one place — useful when reading a raw
row. `sutra.`-prefixed keys are engine-internal: opaque strings to the byte-level key patchers and
never tag-decoded. `sutra.var.<name>` entries are the user variables the typed encoding exists for.

| Key | Written by | Meaning |
|---|---|---|
| `sutra.snapshot` | codec | Format generation: `2` plaintext-untyped, `3` encrypted-untyped, `4` typed (encryption stays orthogonal — detected off the `sutra.enc.` prefix, never the version). |
| `sutra.status` | executor / re-stamps | Instance status; terminal and FAILED re-stamps are byte-level patches. |
| `sutra.deploymentId` | executor / migration | The content-hash pin; rewritten only by the validated migration operation. |
| `sutra.processId` | executor / migration | The owning process definition id; rewritten only by a cross-process re-home. |
| `sutra.waitingNodes` / `sutra.completedNodes` / `sutra.startNode` | executor | The replay frontier; node ids, mapped on migration. |
| `sutra.retry.<nodeId>` | retry machinery | Durable attempt counter for a retry policy; the key is renamed on migration, the counter never resets; absent unless the task has a policy. A malformed counter reads as zero — a snapshot must never become unloadable. |
| `sutra.retryWait.<nodeId>` | retry machinery | The backoff-window marker on a channel-call task: present exactly while a dead attempt's backoff timer is pending, carrying the failure's classification code. Renamed with the node on migration. |
| `sutra.auditSeq` | audit | Per-instance audit sequence floor. |
| `sutra.var.<name>` | executor | A user variable — tagged typed value at generation 4; a plain string in older rows. |
| `sutra.enc.<name>` | crypto envelope | Ciphertext for a sensitive variable; the authenticated data binds key id, instance id, and variable name — and deliberately excludes the deployment id, so migration stays decryptable. At generation 4 the encrypted plaintext is the tagged form. |
| `sutra.keyId` / `sutra.sensitive` | crypto envelope | The tenant key anchor (plaintext, self-describing) and the sensitive-name set. |
| `sutra.coverage.<pathId>` | coverage | Path-coverage cursors — keyed by declared path id, not node id; migration must not touch them. |
| `sutra.failureCode` / `sutra.failureDetail` | FAILED re-stamp | Structured code + captured message of the fatal failure, for inspection and post-repair migration. |

## Next

- **[Ownership and claims](ownership-and-claims.md)** — what guarantees only one worker is ever
  writing one instance's snapshot.
- **[Retry machinery](retry-machinery.md)** — attempt counters as snapshot keys, and why that
  choice costs zero migration.
- **[Migration internals](migration-internals.md)** — re-pinning a snapshot without decoding it.
