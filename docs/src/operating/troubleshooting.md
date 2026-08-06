# Troubleshooting BPMN solutions

This page is for the person building and running a Sutra *solution* — a deployment package that
isn't behaving the way you expect. (If instead you're debugging the *engine itself* — a crash, a
suspicious executor code path, a failing test in the workspace — see
[Debugging the engine](../debugging-the-engine.md), which is written for contributors.)

Every command below is read against `rust/crates/sutra-cli/src/commands/` — what's described is
what the code actually does, not an aspirational surface.

## Start with `sutra lint`

```bash
sutra lint packages/my-app
```

Runs the exact fail-closed validation suite `sutra package` runs before sealing an archive, and
prints nothing on success. This is always the first thing to run — most "why doesn't this deploy"
questions are answered here, before you ever touch a running engine.

## Reading a `SUTRA.*` diagnostic code

Every rejection, warning, and structured log line the engine emits carries a code shaped like
`SUTRA.<CATEGORY>.<REASON>`. The category tells you which layer to look at:

| Category | Layer | Example |
|---|---|---|
| `SUTRA.PARSE.*` | A document failed to parse or failed schema validation | `SUTRA.PARSE.XSD.SCHEMA_VIOLATION` |
| `SUTRA.CONFIG.*` | A deploy-time configuration problem (`channels.yaml`, `datastores.yaml`, a codec manifest) | `SUTRA.CONFIG.CODEC_MANIFEST.MISSING` |
| `SUTRA.INBOUND.*` | An inbound message was rejected before or during dispatch | `SUTRA.INBOUND.NO_START_EVENT_FOR_MESSAGE_TYPE`, `SUTRA.INBOUND.PAYLOAD_TOO_LARGE`, `SUTRA.INBOUND.ALIAS_CONFLICT_REJECT` |
| `SUTRA.ACK.*` | Acknowledgement / deferred-ack registry events | `SUTRA.ACK.DEFERRED_TIMEOUT` — see [Acknowledgement modes](ack-modes.md) |
| `SUTRA.VALIDATE.*` | A `q:validators` entry (complex or simple) couldn't be resolved or run | `SUTRA.VALIDATE.VALIDATOR_NOT_FOUND` |
| `SUTRA.RUNTIME.*` | A failure while executing an already-dispatched instance | `SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND`, `SUTRA.RUNTIME.DATASTORE.CONFLICT` |
| `SUTRA.OUTBOUND.*` | A reply or send failed to encode or deliver | `SUTRA.OUTBOUND.ENCODE_FAILED` |
| `SUTRA.DEPLOY.*` | The admin deploy API rejected an archive | returned as the body of a `4xx` from `POST /admin/deployments` |

`--format json` on any CLI command that reports diagnostics gives you the code, severity, and any
attributes as structured data — pipe it to `jq` rather than screen-scraping text.

Two families get their own sections below, because their codes are read in groups rather than one
at a time: [codec diagnostics](#codec-diagnostics-parse-and-schema-failures) and
[data-store diagnostics](#data-store-diagnostics-projected-stores).

## Codec diagnostics: parse and schema failures

Whichever codec a channel is bound to, a decode lands on one of three outcomes, and it is that
contract — not any codec's own vocabulary — that the rest of the engine reacts to:

- **`OK`** — a payload, no issues.
- **`SOFT_ERRORS`** — a *usable* payload plus structural issues. The instance still starts and
  still runs; the issues are data your process can branch on. This is the outcome people are most
  often surprised by, and it's deliberate: a schema violation is a business decision to route,
  not an exception to catch.
- **`FATAL`** — no payload at all. The engine rejects the inbound per the channel's configured
  posture.

Every issue carries the same five slots — `code`, `severity` (`ERROR` / `WARNING` / `INFO`),
`path` (JSON-Pointer-shaped into the payload, or the source location for a schema violation),
`message`, and `value` (the offending value, or the reason code a validator wants surfaced). They
reach your process as `<source>.validation` (the full list) plus a frozen `validation` summary:
`outcome`, `tier` (`structural` / `content`), `firstReasonCode`, `firstIssue`, and `issues`.
`<q:onValidation mode="route"/>` is what hands that to your own gateway instead of short-circuiting
— see [The q: namespace](../building/q-namespace.md#qonvalidation--the-structural-failure-policy).

### The codes this engine emits

The middle segment of a `SUTRA.PARSE.*` code names the layer that produced it, which is the fastest
way to separate a malformed-bytes problem from a schema problem:

| Code | Outcome | Means, and what to check |
|---|---|---|
| `SUTRA.PARSE.JSON.PARSE_ERROR`, `SUTRA.PARSE.XML.PARSE_ERROR`, `SUTRA.PARSE.YAML.PARSE_ERROR` | `FATAL` | The bytes aren't well-formed in the format the channel declared. Nothing schema-related has run yet — look at what the sender actually put on the wire, and at the channel's `content-type` handling. |
| `SUTRA.PARSE.XSD.SCHEMA_VIOLATION` | `SOFT_ERRORS` | A well-formed document that violates the package's own XSD. Collect-all: one issue per violation, never just the first, each carrying `line:column` in the `path` slot. |
| `SUTRA.PARSE.JSON_SCHEMA.SCHEMA_VIOLATION` | `SOFT_ERRORS` | The same, for a `schemaKind: json-schema` codec. |
| `SUTRA.RUNTIME.CODEC.DECODE_FAILED` | `FATAL` | A package codec couldn't read the bytes at all — a parse or transcode failure before validation could run. Most often a payload sent in a format the codec-manifest's `formats` list doesn't include. |
| `SUTRA.INBOUND.CODEC_NOT_FOUND` | rejected before decode | The channel names a codec this binary doesn't serve. Check what the running build actually links — see [Channels and transports](../building/channels.md). |
| `SUTRA.OUTBOUND.ENCODE_FAILED` | — | The reply direction: the payload couldn't be encoded for the outbound channel. |

Deploy time is a separate, earlier, fail-closed gate: `SUTRA.CONFIG.SCHEMA.INVALID` (a schema that
doesn't compile), `SUTRA.CONFIG.CODEC_MANIFEST.MISSING` / `.INVALID`, and
`SUTRA.CONFIG.CODEC_LAYOUT.INVALID` (a loose file, mixed kinds, or an empty codec folder under
`schemas/`) all reject the archive rather than deploy a codec that would validate less than it
claims. `sutra lint` reports every one of them before you deploy anything — which is why it's the
first command on this page.

### A code from a codec you added

An extension codec (see [Channels and transports](../building/channels.md)) claims its own middle
segment — `SUTRA.PARSE.<STANDARD>.*` and `SUTRA.VALIDATE.<STANDARD>.*` — and documents its own set;
the engine neither interprets nor rewrites them. What the engine guarantees is everything above:
the three-way outcome, the five issue slots, and the same surfaces. So an unfamiliar code reads
exactly the way the table does — the middle segment tells you which codec owns it, and that codec's
own documentation tells you what it means. Wherever it came from, it reaches you the same three
ways: the `validation` variables in the process, `--format json` diagnostic output, and the audit
trail, so [Tracing one message end to end](#tracing-one-message-end-to-end) applies unchanged.

## Data-store diagnostics: projected stores {#data-store-diagnostics-projected-stores}

A store that declares a
[`structure:` block](../building/data-stores.md#typed-columns-declaring-the-structure-a-store-holds)
is verified at package time against the store's **own** migrations — no database connection, no
credentials. A store with no `structure:` block raises none of these codes at all; it stays the
opaque key→JSON store it always was.

The posture is three-state, and reading it correctly saves a lot of time: a **definite** fault is
an error, an **unprovable** one is a warning worded as unprovable, and a projection that matches
raises nothing. Only errors fail the command — a warning here is information, not a gate.

| Code | Severity | Means, and the fix |
|---|---|---|
| `SUTRA.CONFIG.DATASTORE.STRUCTURE_NOT_FLAT` | ERROR | The declared type has a nested, repeated or open child (or no projectable child at all), so it can't become a flat row. The message names the child. Flatten the type, or remove the `structure` block and keep the opaque store — those are the only two remedies, deliberately |
| `SUTRA.CONFIG.DATASTORE.COLUMN_MISSING` | ERROR | A declared field projects to a column the effective table doesn't have, **or** the table is missing one of the three [control columns](../building/data-stores.md#control-columns) (`store_key`, `rev`, `updated_at`) every projected table needs — the message names whichever is absent. Add it in a new `V`-numbered migration, or (a declared field only) map it to a column that already exists under `columns:` — there's no `columns:` remedy for a missing control column |
| `SUTRA.CONFIG.DATASTORE.COLUMN_TYPE_MISMATCH` | ERROR | The column can't hold the declared value space — `VARCHAR(10)` for a `maxLength="35"` field, an integer column for a fractional decimal — or its nullability contradicts the declaration (an optional field against a `NOT NULL` column with no `DEFAULT`, or a column an `ALTER` adds as `NOT NULL` with no `DEFAULT`, which pre-existing rows could never satisfy). Widen or relax the column in a new migration, or narrow the declared type |
| `SUTRA.CONFIG.DATASTORE.KEY_MISMATCH` | ERROR | The table's identity isn't a key over the projected columns: it declares no `PRIMARY KEY` (nor a unique constraint) at all, or a key column the projection never writes, or one that maps to an optional field. A projected store reads, upserts and compare-and-sets exactly one row by key, so it needs a key it can always write |
| `SUTRA.CONFIG.DATASTORE.COLUMN_NAME_INVALID` | ERROR | A folded column name collides with another field's, lands on a reserved word, exceeds the 63-character identifier cap, or isn't a usable identifier. Name the column yourself under `columns:` — the mapping is checked by the same rules, so the override has to be usable too |
| `SUTRA.CONFIG.DATASTORE.DDL_UNVERIFIABLE` | **WARNING** | Nothing is wrong — something simply couldn't be *proven*. See below |
| `SUTRA.CONFIG.DATASTORE.COLUMN_UNMAPPED` | **WARNING** | The table has columns the projection never writes. Usually fine (a legacy or an operator column). One case is sharper and the message says so: an unmapped `NOT NULL` column with no `DEFAULT` would make every insert fail — give it a `DEFAULT`, declare it in the structure type, or drop it |

A `structure` block pointing at a schema or a type the package doesn't declare is reported as
`SUTRA.CONFIG.DATASTORE.INVALID` — the same code any other malformed store declaration uses — not
as one of the projection codes above.

### `DDL_UNVERIFIABLE` is not an error, and shouldn't be read as one

> *… could not be fully parsed — the statement `CREATE OR REPLACE FUNCTION f( …` is outside the DDL
> subset this lint parses — so the effective table shape was not derived and the declared structure
> was not verified; no column diagnostic is raised for this store (it may be valid; it is simply
> not provable here)*

Lint replays a package's `migrations/<store>/V*.sql` through a deliberately small SQL subset —
`CREATE TABLE`, `ALTER TABLE ADD/ALTER/DROP COLUMN`, and the key-bearing constraints, across the
three shipped dialects' spellings. Real migrations routinely contain more than that: a PL/pgSQL
trigger body, a T-SQL procedural guard, a table created by an operator outside the package. When
the parser meets something it doesn't model, it stops trying to verify that store and says so.
**It never converts that into a column error**, because a linter that cries wolf on legitimate DDL
is one authors learn to ignore.

The same wording covers the narrower cases, and they're worth telling apart:

- **The store's shape wasn't derived at all** — an out-of-subset statement, no migrations shipped
  for the store, the table created elsewhere, or several candidate tables and none named after the
  store (name the projected one with `sql: table: <name>`).
- **Individual fields couldn't be compared** — the column's type is outside the set lint compares
  (`JSONB`, `UUID`, a domain type), or the declared type has no ruled column mapping. The column
  *exists*; only the type comparison is withheld, aggregated into one line per store.
- **The declared schema isn't an enumerable XSD** — an engine-provided codec (its type set is
  open), or a JSON-Schema / schema-bundle codec folder. JSON Schema carries no length or precision
  information, so there is genuinely nothing to compare against here — but don't read the warning
  as "unverified for now, deploy anyway": a JSON-Schema-declared `structure:` isn't merely
  unverified, it is **refused outright** when the engine resolves the store at deploy time (see
  [Typed columns](../building/data-stores.md#typed-columns-declaring-the-structure-a-store-holds)).
  Declare the structure against an XSD if you want the store to exist at all, not just to have its
  column types checked.

Note that degradation is per **store**, not per statement: one out-of-subset statement anywhere in
`migrations/<store>/` withholds verification for that store's whole table. So the way to get the
warning to go away is to keep the store's own DDL inside the subset (plain `CREATE TABLE` /
`ALTER TABLE`, with procedural setup living in a different store's schema or outside the package),
or to give lint the missing pointer — `sql: table:` — when the shape *is* parseable but ambiguous.
If neither is worth it, the warning is a fair thing to live with: first-use verification against
the live table still fails the store closed if the projection genuinely doesn't hold.

### Runtime codes: `UNDECLARED_FIELD`, `VALUE_NOT_A_RECORD`, `PROJECTION_UNSATISFIABLE`

Three runtime codes ride a projected store's own operations. Where to look for them matters:
unlike `SUTRA.RUNTIME.DATASTORE.CONFLICT`, which is the failed instance's own diagnostic **code**,
these three travel *inside the message* of a `SUTRA.RUNTIME.UNEXPECTED` diagnostic. So filter on
the message text, not the code, when you go looking:

| Code | Fires when, and the fix |
|---|---|
| `SUTRA.RUNTIME.DATASTORE.UNDECLARED_FIELD` | A write carried a field the structure doesn't declare. A projected row *is* its declared scalars — there is no residue column, so an extra field has nowhere to go, and the write is refused naming the field rather than dropping it. Two causes in practice: the process is writing a value assembled from a different (wider) shape than the declared type, or the declared type has fallen behind a schema change. Declare the field and ship the column, or stop writing it |
| `SUTRA.RUNTIME.DATASTORE.VALUE_NOT_A_RECORD` | A projected store was handed a value that isn't a record at all — a scalar, an array, or `null`. A projected row *is* its declared fields, so there is nowhere for a bare value to land either. Write a record shaped like the declared type |
| `SUTRA.RUNTIME.DATASTORE.PROJECTION_UNSATISFIABLE` | The package-time drift check, caught live: on **first use** of the store — once per store instance, the same gate that runs its migrations, never on every operation — the provider reads the live table's actual columns and fails the store closed if the projection can't be satisfied, naming every offending column at once (one missing, an optional field's column `NOT NULL`, or a mandatory unmapped column with no `DEFAULT`). Usually a hand-applied `ALTER` that drifted from the package's own migrations — re-apply them (or ship the missing `ALTER`) before the store can be used |

A silent partial write is the worse outcome in every one of these, so all three are deliberately
loud rather than best-effort. `SUTRA.CONFIG.DATASTORE.COLUMN_NAME_INVALID`, in the table above, is
this family's plan-time member: a declared field folding onto a
[control column](../building/data-stores.md#control-columns) name is refused before the store is
ever served — both lint and the engine's own plan-time resolution run the same rule, sourced from
one shared constant, so the two sides cannot drift apart.

## The inspection commands

None of these execute anything — they read a BPMN file (or a running engine) and report. Use them
in roughly this order when a process isn't routing, resuming, or replying the way you expect.

### `sutra describe` — what does the engine see in this file?

```bash
sutra describe bpmn/transfer.bpmn
```

A structural summary: processes, start/end events, every service/user task (with its
`implementation`, codec, validator, and redactor refs), gateways, channel sources, and reply refs.
Read-only — it never invokes the engine's real loader, so it's safe to run against a file you're
mid-edit on. `--format json` for scripting.

### `sutra dispatch-graph` — visualize the routing

```bash
sutra dispatch-graph bpmn/transfer.bpmn --format mermaid
```

Emits a graphviz `dot` or `mermaid` diagram of the file's dispatch tree — nodes are BPMN elements,
edges are sequence flows. Paste the mermaid output straight into a markdown viewer to see the
shape of a flow you didn't author, or to sanity-check `q:dispatch`/`q:case` routing before review.

### `sutra simulate --dry-run` — will this channel actually reach my process?

```bash
sutra simulate bpmn/transfer.bpmn --channel transfer-request --dry-run
```

Resolves which process (and start event) a named channel would route to, from the file's own
`q:source` declarations — no execution, `--dry-run` is required. If the channel name doesn't
match any declared source, the error lists every channel the file *does* declare, so a typo is
obvious immediately. This is the fastest way to answer "why isn't my message reaching the process
I think it should" without deploying anything.

### `sutra explain` — evaluate a FEEL expression in isolation

```bash
sutra explain 'fromAccount.frozen or toAccount.frozen or fromAccount.balance < payload.amount'
sutra explain --context vars.txt 'payload.amount > 100'
sutra explain   # no expression: a REPL on stdin, `:quit` to exit
```

Built directly on `sutra-feel` — the same evaluator the engine runs, standalone. `--context` takes
a flat `key: value` (or `key=value`) file (values coerce to boolean/number when they parse as
such, else stay strings). Use this the moment a gateway takes the branch you didn't expect: paste
the exact condition in, supply the variables you think are in scope, and see what FEEL actually
does with them — usually a type coercion or a missing-path surprise, not a logic bug.

### `sutra coverage check` — is my compliance path actually being exercised?

```bash
sutra coverage check bpmn/transfer.bpmn                       # drift lint: declared paths still valid?
sutra coverage check --archive deployed.sutra \
    --database-url "$SUTRA_DB_URL" --threshold 100             # correlation-aware, store-backed check
```

The bare form is a drift lint: every `q:coverage` declaration must still be an ordered subsequence
of a real route through the current flow graph — it catches a path declaration that silently broke
when someone reworked the diagram. The `--archive` form reads the deployment's seeded coverage
flags out of the database its `coverage` store declares (`--database-url` names it — any of the
three shipped dialects), and fails closed below `--threshold` — the gate a CI pipeline runs after a
test campaign. See [The q: namespace](../building/q-namespace.md#qvariables-qaudit-qcoverage)
for what `q:coverage` declares and the
[money-transfer worked example](../building/worked-example.md) for it end to end.

## Tracing one message end to end

Three surfaces, used together:

1. **The audit trail** — the definitive record of what an instance did:
   `INSTANCE_STARTED` → node-by-node progress → `INSTANCE_COMPLETED` / `INSTANCE_FAILED` /
   `INSTANCE_SUSPENDED` / `INSTANCE_RESUMED`. If you have a JSONL audit sink configured (see
   [Logging and audit](logging.md)), replay one instance's history offline:
   ```bash
   sutra audit-replay <instance-id> --from-jsonl /path/to/audit --until INSTANCE_COMPLETED
   ```
2. **Traces** — if OTel is configured (see [Observability](../architecture/observability.md)),
   every span carries `bpm.instance.id`; filter your trace backend on that id to see the
   `sutra.dispatch` / `sutra.decode` / `sutra.validate` / `sutra.execute` span chain for exactly
   that instance, across every resume segment if it went through a wait state.
3. **The admin API** — against a live engine, `GET /admin/instances/{id}` (or
   `GET /admin/instances/by-alias/{key}/{value}` if you only have the business correlation key, not
   the instance id) shows the instance's current state directly. `POST
   /admin/instances/{id}/cancel` is the one instance-level control operation exposed — there is no
   generic "retry" endpoint; retrying means fixing whatever rejected the input and re-sending it
   (or, for a parked wait, sending a corrected relay message).

## Common failures, worked

**"My message got a 200/202 but nothing happened downstream."** Check `ack-mode` first (see
[Acknowledgement modes](ack-modes.md)) — `on-persist` on an HTTP channel means the `202` you got
back is only proof of durable receipt, not completion; the actual reply (if any) rides an outbound
channel, not the original connection. Then check the audit trail for the instance.

**"The message never became an instance at all."** Run `sutra simulate --channel <name>
--dry-run` against the deployed BPMN first — a `messageTypeValue`/`messageTypePattern` mismatch on
the `q:source` is the most common cause, followed by the channel's codec rejecting the payload
outright (`SUTRA.INBOUND.CODEC_NOT_FOUND` / a FATAL `DecodeResult` — check `sutra describe` for
what codec the channel is actually bound to).

**"A relay message didn't resume my parked instance."** `SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND`
in the audit trail means the relay's `q:alias` expression didn't match any parked instance's alias
— run `sutra explain` with the relay payload's variables against the exact alias expression from
the BPMN to see what key it actually produces, and compare it to what the original request set.

**"A gateway takes the wrong branch."** `sutra explain` the condition expression directly, with a
`--context` file built from the actual variable values at that point (read them off the audit
trail or a trace span if you're not sure) — this isolates a FEEL semantics surprise from a
"my BPMN is wired wrong" problem in one step.

**"My coverage report shows a path that should be covered as uncovered."** Confirm the flows
listed on the `q:coverage` declaration are still the exact ids in the current diagram
(`sutra coverage check bpmn/your-process.bpmn`, the drift-lint form) before assuming the flow
genuinely isn't being exercised.

**"Every request answers `SUTRA.RUNTIME.UNEXPECTED — engine actor is not running`."** An
execution lane died — something panicked outside the per-dispatch containment, which in practice
means the lane's own build/rebuild failed (the boot log has the panic). The process is a zombie
for that lane's share of the key space and will not heal in place: both health probes report it
(`GET /sutra/health/live` → `503` with `data.deadLanes`; `/ready` goes `DOWN` at the same
moment), so an orchestrator with a liveness probe restarts the replica automatically. If you run
without probes, restart it yourself and read the boot log for the panic that killed the lane.

**"My coverage op now fails with `SUTRA.CONFIG.COVERAGE.STORE_MISSING`."** Coverage marks are
persisted in the `coverage` data store the deployment declares in `datastores.yaml`, so the op fails
whenever that store isn't there to write to — rather than answering 0%, because a fabricated 0% is
indistinguishable from a real measurement of nothing covered. Two causes, both named in the message:
the deployment declares `<q:coverage>` paths but **no `coverage` store** (a package-shape
requirement `sutra lint` errors on with this same code — declare the store; you supply no coverage
SQL, since the engine owns that schema and applies it on first use), or it declares one whose
**connection could not be opened** (the boot log carries an error naming that deployment — fix the
URL or the environment reference it resolves from). The same code with a different message means the
store opened but the read failed; the underlying error is in the message. See
[Coverage](../building/coverage.md#where-coverage-is-stored).

## Next

- **[Reference: the `sutra` CLI](../reference/cli.md)** — every flag on every command above.
- **[Debugging the engine](../debugging-the-engine.md)** — the contributor-facing counterpart, for
  when the problem turns out to be in the engine rather than in your BPMN/rules/config.
