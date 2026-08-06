# Rules: DMN, FEEL, and `.srl`

A `businessRuleTask` binds one decision, authored either as a `.dmn` decision table or as a
`.srl` ruleset. Both compile onto the **same** FEEL evaluator (`sutra-feel`) — no JVM, no Rete
runtime, and no second expression language to learn.

## FEEL — the shared expression language

FEEL (Friendly Enough Expression Language) is what every condition, gateway expression, `q:alias`
key, `q:store` key, and rule body is written in. Sutra's implementation is a from-scratch Rust
evaluator (lexer, parser, AST, a determinism denylist that forbids non-reproducible constructs on
a replay path, and DECIMAL64 numeric semantics — 16 significant digits, HALF_EVEN rounding).

Try an expression standalone, before it's anywhere near a process:

```bash
sutra explain 'payload.amount > 100'
sutra explain --context vars.txt 'fromAccount.balance - payload.amount'
```

`sutra explain` is a one-shot evaluator or a REPL (omit the expression) — useful for debugging a
gateway condition or a `q:alias` expression without deploying anything. See
[Crates: sutra-feel and sutra-dmn](../reference/crates.md) if you want FEEL evaluation embedded in
your own Rust project, independent of the engine.

## DMN decision tables

A `.dmn` file under `rules/` is a standard DMN 1.5 decision table; the engine's evaluator supports
all seven hit policies. A `businessRuleTask` names the decision, and its named output columns merge
back into the process's instance variables:

```xml
<!-- examples/approval-hold/.../rules/approval-decide.dmn -->
<decision id="approvalDecide" name="Review decision from risk score">
  <decisionTable hitPolicy="FIRST">
    <input id="i_risk"><inputExpression typeRef="number"><text>riskScore</text></inputExpression></input>
    <output id="o_decision" name="decision" typeRef="string"/>
    <rule id="r_approve">
      <inputEntry><text>&lt; 50</text></inputEntry>
      <outputEntry><text>"approve"</text></outputEntry>
    </rule>
    <rule id="r_review">
      <inputEntry><text>-</text></inputEntry>
      <outputEntry><text>"review"</text></outputEntry>
    </rule>
  </decisionTable>
</decision>
```

Sutra's DMN conformance is measured against the real OMG DMN-TCK, not asserted — see
[DMN-TCK conformance](../reference/dmn-tck.md) for the numbers and what they cover.

## `.srl` — a Drools-inspired rule DSL over FEEL

`.srl` is Sutra's own rule language: `rule / when / then / end` framing around FEEL conditions and
a small, closed set of side-effecting actions. It targets the same use case DMN's `COLLECT` hit
policy does — several independent business-validation rules over one payload — in a syntax closer
to a Drools ruleset than a decision table.

```
rule "currency-not-usd"
when
  exists(payload.amount.currency) and payload.amount.currency != "USD"
then
  report(
    "SUTRA.VALIDATE.CURRENCY_NOT_USD",
    "payload.amount.currency",
    "Currency must be USD; got " + payload.amount.currency
  );
end
```

Grammar, in full:

```
ruleset := rule*
rule    := "rule" STRING attr* "when" <condition> "then" action* "end"
attr    := "salience" INTEGER | "activation-group" STRING
action  := verb ";"
verb    := "report" "(" <feel_expr> "," <feel_expr> "," <feel_expr> ")"
         | "set"    "(" IDENT "," <feel_expr> ")"
```

- Every condition and action argument is an embedded FEEL expression — `.srl` adds only the rule
  framing on top.
- **Two action verbs today**: `set(target, expr)` binds a value into the working context (visible
  to later rules) and the output map; `report(code, path, message)` appends a structured issue.
  `insert` / `retract` are reserved for a future stateful phase and are a clean parse error, not
  silently accepted.
- Evaluation is a **single deterministic forward pass** — a stable-sorted agenda by
  `(-salience, declaration order)`, each rule firing at most once, `activation-group` giving
  first-match-wins semantics within a group. This is sequential-agenda, not a Rete network.
- Fail-closed: a parse error, or a condition/action that errors at evaluation, is a hard error —
  never a silently-skipped rule.
- A FEEL `if / then / else` used *inside* a `when` condition must be parenthesised
  (`when (if a then b else c) …`), because the bare keyword `then` that ends the condition is
  matched at paren depth 0.

`.srl` and `.dmn` both live under a package's `rules/` folder (see
[Deployment packages](deployment-packages.md)) and are routed by file extension — no separate
configuration names one or the other.

## Composing a validator chain

A `<q:source>`'s `<q:validators>` list (see
[The q: namespace](q-namespace.md#qvalidators-and-qredactors-nested-under-qsource)) can mix
`<q:complexValidator>` entries pointing at **both** `.dmn` and `.srl` files in one chain — nothing
requires a chain to stick to one rule engine, and nothing requires a business ruleset to live in a
single file.

- **Declaration order is evaluation order.** The chain runs top to bottom exactly as written in
  the BPMN.
- **Every validator sees the same payload projection.** Whatever the channel's codec decoded is
  what each entry in the chain evaluates against — an earlier validator's issues don't change what
  a later one reads, only what accumulates alongside it.
- **Issues accumulate, they never replace.** Each entry's reported issues append to the same
  `validation.*` result; `validation.outcome` and `validation.tier` are derived from the whole
  accumulated list, so a gateway keyed on them is indifferent to which file in the chain produced
  which code.
- **`validation.firstReasonCode` follows declaration order.** When two entries both report an
  issue, `firstReasonCode` is whichever fired first in chain order — this only affects which code a
  reply template surfaces, never whether a gateway routes to reject.

This is what lets you split one business ruleset across engines by what each rule *needs*, instead
of by an arbitrary file boundary. A module with a mix of clock-dependent and stateless checks (an
extension-crate workload, not one of this repository's bundled examples) does exactly that:

```xml
<q:validators>
  <q:complexValidator source="intake-timing.dmn"/>
  <q:complexValidator source="intake-fields.srl"/>
</q:validators>
```

The DMN table keeps the rules that need a clock — a staleness window and a clock-skew/future-dated
window on some received-at timestamp — because the engine-injected evaluation clock is only
available as a DMN validator's reserved `now` input. The `.srl` file carries the stateless field
checks (a required-currency check, a positive-amount check, a required-identifier check) that have
no temporal dependency at all and read more naturally as named rules than as columns on the
decision table. Both files reason over the identical payload projection the codec decoded, report
through the same `SUTRA.VALIDATE.*` code family, and their issues land in one accumulated
`validation.issues` list — the split between engines is invisible to the BPMN gateway that routes
on the outcome.

## Which to reach for

- **DMN** if the logic is naturally tabular (a rate card, an approval matrix, one output per row)
  or you want a diagram a business analyst can review directly.
- **`.srl`** if the logic is a set of independent validation rules over a payload, where a
  Drools-style `when/then` reads more naturally than a table.
- **Both, in one chain**, when a ruleset is naturally mixed — some rules need something only a DMN
  validator gets (the injected evaluation clock), others are better named individually than
  columned into a table. See [Composing a validator chain](#composing-a-validator-chain) above for
  exactly how a `.dmn` and a `.srl` entry combine, illustrated above with a clock-dependent-vs-
  stateless split.

## Next

- **[Data stores](data-stores.md)** — durable state a rule or task reads/writes.
- **[Worked example: money-transfer](worked-example.md)** — FEEL data-assignment nodes in a real
  transaction flow.
