# DMN-TCK conformance

Sutra's DMN/FEEL evaluator (`sutra-dmn` + `sutra-feel`, see
[Crates: sutra-feel and sutra-dmn](crates.md)) is checked against the real OMG
[DMN Technology Compatibility Kit](https://github.com/dmn-tck/tck) — the same corpus other DMN
implementations are measured against — rather than relying on an internal test suite alone.

## Current standing

| Level | Result |
|---|---|
| **Compliance level 2** | **126/126 (100%)** |
| **Compliance level 3** | **3349/3369 absolute (99.4%)**, and **100% of attempted assertions** (0 semantic failures among cases the engine attempts) |

The gap between "100% of attempted" and "99.4% absolute" is a small, enumerated set of
constructs the evaluator deliberately doesn't attempt yet — chiefly reflective execution of
`{java|pmml: …}` external function bodies (the grammar parses and validates their binding shape;
invoking one returns a clear, deliberate semantic rejection rather than a wrong answer). This is
the honest distinction the numbers are built to preserve: a wrong value on a supported construct
is a real conformance defect, and the harness that produces these numbers is built specifically to
never launder one into an "unsupported" bucket. Level 2 has no such gap at all.

## What this does and doesn't tell you

**What it tells you:** decision tables (all seven OMG hit policies, including COLLECT with
aggregation), decision requirement graphs, business knowledge models, decision services, and the
breadth of FEEL itself (temporal types and arithmetic, ranges/intervals, list/context
comprehensions, string and numeric builtins, filters and projections) behave per specification
across a large, independently-authored corpus — not just the cases Sutra's own contributors
thought to write.

**What it doesn't tell you:** conformance to a language specification says nothing about Sutra's
BPMN coverage, its channel/transport behavior, its persistence and replica semantics, or its
security posture — those are covered elsewhere in this book (see
[Building BPMN solutions](../building/concepts.md) and [Architecture](../architecture/overview.md)).
It also isn't a performance claim — conformance and throughput are separate properties.

## Where this comes from

The conformance measurement is produced by a development-time harness in the workspace — it is a
tool contributors run when working on the FEEL/DMN evaluator, not something a user building a
BPMN solution ever needs to invoke. See
[Debugging the engine](../debugging-the-engine.md#the-dmn-tck-harness--a-development-tool-not-a-user-facing-command) if you're contributing to
`sutra-feel` or `sutra-dmn` and need to reproduce or extend this measurement yourself.

## Next

- **[Rules: DMN, FEEL, and .srl](../building/rules.md)** — using DMN inside a Sutra process.
- **[Crates: sutra-feel and sutra-dmn](crates.md)** — using the same evaluator standalone.
