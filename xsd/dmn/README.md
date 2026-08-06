# OMG DMN XSDs

> **Historical (reference baseline, retired).** This README describes the XJC/JAXB
> code-generation pipeline that the retired reference-baseline implementation ran against the
> XSDs in this directory. The current Rust engine's DMN loader
> (`rust/crates/sutra-dmn/src/loader.rs`) parses `.dmn` files directly via `quick-xml` — no
> code generation step consumes these XSDs today. They are kept in place for OMG-attribution
> and historical continuity; the walkthrough below is preserved as a historical record of how
> the reference baseline sourced and generated from them.

This directory holds the OMG DMN XSDs that drove JAXB code generation for the reference
baseline's `sutra-validator-dmn` extension.

Per the reference baseline's load-bearing rule (2026-05-20 user directive): **if an XSD is
available for a model object, do not hand-code it. Generate classes via XJC and schedule the
generation as a build dependency.** (This directive governed the retired reference-baseline
toolchain; the current Rust engine hand-writes its DMN model types against the OMG schema
instead.)

## Why the reference baseline generated from the OMG XSD, not just delegating to a third-party DMN engine

The straightforward way to load a `.dmn` file is to let an embedded DMN engine handle it —
each ships its own internal binding tied to its private model. The reference baseline's
directive applied even there: rather than rely on a third-party engine's private model, it
generated its own DMN model types from the OMG XSD. That gave the validator extension a
stable, project-owned type surface for `.dmn` parsing **and** structural validation, while
keeping the choice of runtime DMN evaluator swappable.

## How XSDs land here

The OMG DMN spec publishes the XSDs in the spec downloads. They are royalty-free under the
OMG IPR policy with attribution.

### Option 1 — Official OMG download (preferred)

1. Go to <https://www.omg.org/spec/DMN/> and select the latest minor version (DMN 1.5).
2. From the **Specification Catalogue**, find the *Machine-readable file* link →
   downloads a zip with the XSDs.
3. Unzip into this directory. Expected files (DMN 1.5):
   - `DMN15.xsd` — top-level schema
   - `DMNDI15.xsd` — DMN Diagram Interchange
   - `DC.xsd` — Diagram Common (shared with BPMN — same schema, you may copy from `../bpmn/`)
   - `DI.xsd` — Diagram Interchange (shared with BPMN — same schema, you may copy from `../bpmn/`)

### Option 2 — Helper script (one-shot)

A helper script lives at [`../../scripts/fetch-dmn-xsds.sh`](../../scripts/fetch-dmn-xsds.sh).
Review the source URLs before running. Defaults to manual-instruction mode; pass
`SOURCE=custom FETCH_URL_PATTERN=...` to fetch from your internal mirror.

```bash
./scripts/fetch-dmn-xsds.sh
```

### Option 3 — Provide your own

Drop XSDs directly.

## What happened when XSDs were present (reference baseline, retired)

The reference-baseline DMN validator module had an XJC execution that ran **only if this
directory contained at least one `.xsd` file**. The execution emitted JAXB-bound classes into
the reference baseline's `trade.startr.sutra.validator.dmn.spec.*` packages (sub-packages
controlled by per-XSD `.xjb` files when needed).

When XSDs were present, the reference-baseline build:

1. Generated `trade.startr.sutra.validator.dmn.spec.*` classes.
2. Had its `.dmn` parser use those generated types — XSD-faithful, no hand-coded StAX, with
   schema violations surfacing as ERROR-severity diagnostics at parse time.
3. Could still delegate to a customer-chosen DMN engine for runtime decision evaluation via an
   adapter, while keeping the **model** project-owned.

The current Rust engine achieves the equivalent (project-owned, OMG-schema-faithful DMN model
types) by hand-writing the model in `rust/crates/sutra-dmn/src/model.rs` and parsing against it
directly — there is no generation step to run.

## Sharing DC/DI with BPMN

The OMG DC and DI schemas are shared between BPMN and DMN — the same files appear in both
`xsd/bpmn/` and (when populated) `xsd/dmn/`. In the reference baseline, the spec module's XJB
binding configuration put DC/DI types under its own `trade.startr.sutra.spec.di[.dc]` packages.
That module-boundary decision (separate generated DC/DI copies per consumer, no cross-module
ABI coupling) no longer applies to the current Rust engine, which has no code-generation step
at all; the note is kept here as historical record.

## Licensing reminder

OMG specifications are subject to the [OMG IPR Policy](https://www.omg.org/about/policies/index.htm).
The XSDs are royalty-free for implementation. Preserve attribution headers in the schemas.

## Historical state (reference-baseline snapshot, 2026-05-20 — pipeline now retired)

XSDs had landed and were generating cleanly under the reference baseline. Inventory at the
time:

- `DMN15.xsd` (top-level DMN 1.5 schema) → `trade.startr.sutra.validator.dmn.spec`
- `DMNDI15.xsd` (DMN Diagram Interchange) → `trade.startr.sutra.validator.dmn.spec.dmndi`
- `DI.xsd` (shared Diagram Interchange) → `trade.startr.sutra.validator.dmn.spec.di`
- `DC.xsd` (shared Diagram Common) → `trade.startr.sutra.validator.dmn.spec.di.dc`

Per-XSD `.xjb` binding files routed DC/DI/DMNDI to sub-packages (mirroring the BPMN spec
module's pattern); DMN15 kept the default `…spec` package. This inventory is preserved as a
historical record of the retired pipeline's last known-good state.
