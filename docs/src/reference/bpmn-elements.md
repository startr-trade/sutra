# BPMN 2.0 element support

Sutra executes BPMN 2.0. This page explains the *rule* that decides what it accepts; for the
element-by-element answer, read the generated table rather than any prose:

**[`rust/crates/sutra-bpmn/bpmn-support.md`](https://github.com/startr-trade/sutra/blob/main/rust/crates/sutra-bpmn/bpmn-support.md)**

That file is generated from the engine's own `Node` enum and is compile-enforced: three exhaustive
`match`es mean a new node kind does not compile until it is described there, and a test loads a
minimal process per element to check the page against the **loader** rather than against a comment.
It therefore cannot drift from the implementation — which a hand-maintained list here would.

## The rule

Where BPMN 2.0 already spells something the engine does, the engine **accepts the standard spelling
and maps it onto existing behaviour**. The `q:` namespace carries what BPMN does not model, and
stays visibly an extension.

In practice that means four things:

1. **A conformant element the engine can execute is accepted**, even where an extension already
   expressed the same thing. `<task>` loads as the pass-through `<manualTask>` already is.
2. **An element the engine genuinely cannot execute is rejected, loudly**, with the diagnostic
   naming what to model instead. Silent inertness is worse than a refusal: a model that loads and
   quietly ignores half your diagram is harder to debug than one that will not load.
3. **Accepting something silently when you probably meant otherwise is itself a defect.** `<task>`
   loads *and* `sutra lint` warns, because an author who meant `<serviceTask>` and typed `<task>`
   has written a step that does nothing.
4. **Where BPMN and `q:` both spell something, BPMN's spelling is an alias onto the same model**,
   not a second mechanism — and declaring one name both ways is a lint warning rather than a silent
   precedence.

## What is rejected

One flow node: **`<event>`**.

That is not a gap. `tEvent` is `abstract="true"` in the BPMN 2.0 XSD — `<event>` is the
substitution-group head that concrete events substitute for, so no conformant document can use it
as a flow node and there is nothing for any engine to implement.

A handful of *forms* are also refused, each with a reason the diagnostic states:

| Form | Why |
|---|---|
| `<timeCycle>` on an intermediate catch | a token parks there exactly once, so later occurrences have nothing to resume |
| `<timeCycle>` on an **interrupting** boundary | the first firing cancels the host, so later occurrences are unreachable |
| a **bounded** `R<n>` cycle on a boundary | the remaining budget would have to survive the host's park, which needs durable per-instance state the engine does not keep. Use `R/<interval>` |
| `@instantiate="true"` on `<receiveTask>` / `<eventBasedGateway>` | process instantiation needs the intake-selection surface a `<startEvent>` has |
| `eventGatewayType="Parallel"` | meaningful only when instantiating; treating it as `Exclusive` would invert "wait for all" into "wait for one" |
| an `<eventBasedGateway>` arm that does not park | an arm that resolves instantly makes every other arm unreachable |
| a durable park inside an inline sub-process | the inline runners cannot park mid-scope — model it at the top level |

## Things that are accepted but carry no token

`<dataObject>` / `<dataObjectReference>`, `<laneSet>` / `<lane>`, artifacts and `<documentation>`
are modelling constructs, not flow nodes. A data object **does** now contribute a declared variable
when a data association names it — see
[the `q:` namespace](../building/q-namespace.md) for how that interacts with `<q:variables>`.

## The wait-state column is the one to read first

A process containing any node that parks is **stateful**, and ineligible for synchronous execution.
Most "does Sutra support X?" questions are really "does X park, and what does that imply" — which
is why the generated table has a column for it, checked against the engine's own
`Node::is_wait_state()`.
