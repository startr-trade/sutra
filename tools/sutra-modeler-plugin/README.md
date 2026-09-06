# @startr-trade/sutra-modeler-plugin

A [bpmn-js](https://github.com/bpmn-io/bpmn-js) extension for modelling **`<q:*>` BPMN
elements** — the `sutra` extension namespace (`urn:sutra:q:1.0`).

The plugin ships:

- a **moddle descriptor** (`qModdle`) that teaches `bpmn-moddle` how to read & write
  `<q:source>`, `<q:reply>`, `<q:validator>`, `<q:alias>`, `<q:dispatch>`, `<q:case>`,
  `<q:onValidation>`, `<q:audit>` and supporting types;
- a **properties-panel provider** (`qPropertiesProvider`) for **all 7 panels**:
  - `<q:source>`    on `bpmn:StartEvent`     (channel, ack-mode, dataClass)
  - `<q:reply>`     on `bpmn:EndEvent`       (reply-mode, idempotency-key, authRef)
  - `<q:validator>` on `bpmn:ServiceTask`    (ordered validator list)
  - `<q:alias>`     on `bpmn:StartEvent`     (expression, onConflict, multiValue) **+ live FEEL preview**
  - `<q:dispatch>`  on `bpmn:CallActivity`   (key, defaultCase, onNoMatch) **+ live FEEL preview**
  - `<q:case>`      on `bpmn:CallActivity`   (ordered when/call list)
  - `<q:audit>`     on `bpmn:Process`        (dataClass, redactors)
- a **FEEL evaluator** (`src/feel/FeelEvaluator.js`) thin-wrapping
  `@bpmn-io/feelin` — the canonical JS FEEL interpreter used across the bpmn-io
  ecosystem.

## Install

```bash
npm install @startr-trade/sutra-modeler-plugin bpmn-js bpmn-js-properties-panel
```

## Usage

```js
import Modeler from 'bpmn-js/lib/Modeler';
import { qModdle, qPropertiesProvider } from '@startr-trade/sutra-modeler-plugin';

new Modeler({
  moddleExtensions: { q: qModdle },
  additionalModules: [ qPropertiesProvider ]
});
```

The provider plugs into `bpmn-js-properties-panel` via the standard
`propertiesPanel.registerProvider()` contract; no extra wiring is required beyond
including the module in `additionalModules`.

## Live FEEL preview

Both the `<q:alias>` `expression` entry and the `<q:dispatch>` `key` entry expose
a live-preview pair directly beneath the expression input:

1. **Sample input (JSON)** — a textarea where the author pastes a representative
   payload object (e.g. `{"payload":{"orderId":"ORD-42","messageType":"order.created"}}`).
   The sample is held in an in-memory cache scoped to the BPMN element id; it is
   **not** persisted into the BPMN XML.
2. **Preview result** — a read-only line that evaluates the expression against the
   parsed sample on every keystroke. The result is rendered as either:
   - a JSON-stringified value (e.g. `"ORD-42"`, `true`, `null`),
   - `Error: <message>` when the expression fails to parse / evaluate,
   - `Sample JSON invalid: <message>` when the sample textarea is not parseable JSON.

This lets operators paste a sample payload and immediately see which case route
fires (for `q:dispatch`) or what business key the engine will derive (for `q:alias`)
without round-tripping through a running engine.

### Programmatic API

The evaluator can also be imported directly for tests or for wiring into custom
hosts:

```js
import { evaluate, evaluateWithSample, formatResult } from '@startr-trade/sutra-modeler-plugin/src/feel/FeelEvaluator.js';

evaluate('payload.orderId', { payload: { orderId: 'ORD-7' } });
// → { ok: true, value: 'ORD-7', warnings: [] }

evaluateWithSample('payload.orderId', '{"payload":{"orderId":"ORD-7"}}');
// → { ok: true, value: 'ORD-7', warnings: [] }

formatResult(evaluate('1 + 2 * 3'));
// → '7'
```

### Supported expression subset

The evaluator is backed by `@bpmn-io/feelin@^6` and covers the full DMN 1.5 FEEL
surface that the engine's `trade.startr.sutra.runtime.feel.FeelEvaluator`
relies on for inbound dispatch. Practically that means:

| Feature                         | Example                                            |
|---------------------------------|----------------------------------------------------|
| Literals (number/string/bool)   | `42`, `"hi"`, `true`, `null`                       |
| Arithmetic                      | `1 + 2 * 3`, `(a - b) / 2`                         |
| Comparison                      | `=`, `!=`, `<`, `<=`, `>`, `>=`                    |
| Boolean operators               | `and`, `or`, `not(...)`                            |
| Property access                 | `payload.orderId`, `tenant.id`                     |
| Nested context / list iteration | `for x in items return x * 2`                      |
| 1-based list indexing           | `items[1]` (first), `items[-1]` (last)             |
| String builtins                 | `string length(s)`, `contains(s, sub)`, `upper case(s)` |
| List builtins                   | `count(items)`, `sum(items)`, `min(items)`         |
| Temporal types                  | `date("2026-01-01")`, `duration("PT1H")`           |

Undefined property paths evaluate to `null` with a `NO_CONTEXT_ENTRY_FOUND`
warning — same semantics as the engine. The preview entry surfaces the warning
count inline (e.g. `null  (1 warning)`).

### Note on "WASM evaluator" naming

This work item was originally listed as **"FEEL WASM evaluator"**. We
shipped the same user-visible capability (live in-browser FEEL evaluation) by
depending on the canonical JS implementation rather than building a separate
WebAssembly module — the bundle weight, behavior parity with Camunda Modeler /
bpmn-io tooling, and maintenance ergonomics all point the same direction.

## Schema source of truth

The moddle descriptor mirrors `xsd/q.xsd` at the repo root. That XSD is the authoritative
shape definition — both this plugin and the engine's BPMN parser validate against it.
Changes to `q.xsd` are semver-major and batched at wave boundaries.

## Develop

```bash
npm install
npm test
```

Tests use `vitest` under `jsdom` and exercise both moddle round-tripping and headless
property-panel descriptor assertions with the provider attached.

## Publish

The package is **not** auto-published from CI. To cut a release manually after
landing a version bump on `main`:

```bash
cd tools/sutra-modeler-plugin
npm install
npm test
npm publish --access public
```

The `files` field in `package.json` restricts the published tarball to `src/` —
verify with `npm pack --dry-run` before publishing.

## License

Apache-2.0
