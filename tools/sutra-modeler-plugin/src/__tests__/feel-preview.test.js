/**
 * feel-preview.test.js — exercises the FEEL live-preview module shipped in 0.2.0:
 *
 *   - src/feel/FeelEvaluator.js          : evaluate() / evaluateWithSample() / formatResult()
 *   - src/properties/util/feelPreview.js : sampleEntry() / previewEntry() descriptors
 *   - integration with QAliasGroup       : preview wired beneath the alias expression entry
 *
 * The original naming said "FEEL WASM evaluator" — we settled on Option A from the
 * implementation note (thin wrapper over `@bpmn-io/feelin`, the canonical JS FEEL
 * interpreter used across the bpmn-io ecosystem) since shipping a custom WASM module
 * would duplicate a real, well-tested npm package without any behavior change visible
 * to property-panel users.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import BpmnModdle from 'bpmn-moddle';

import { evaluate, evaluateWithSample, formatResult } from '../feel/FeelEvaluator.js';
import { sampleEntry, previewEntry, __resetSampleCache, __testing } from '../properties/util/feelPreview.js';
import { QAliasGroup } from '../properties/groups/QAliasGroup.js';
import { QDispatchGroup } from '../properties/groups/QDispatchGroup.js';
import { qModdle } from '../index.js';

const Q_NS = 'urn:sutra:q:1.0';

const SAMPLE_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="${Q_NS}">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="start1">
      <bpmn:extensionElements>
        <q:alias expression="payload.orderId" on-conflict="reject" multi-value="false"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:callActivity id="call1">
      <bpmn:extensionElements>
        <q:dispatch key="payload.messageType" default-case="process://x/v1" on-no-match="skip"/>
      </bpmn:extensionElements>
    </bpmn:callActivity>
  </bpmn:process>
</bpmn:definitions>`;

let startBo;
let callBo;

beforeEach(async () => {
  __resetSampleCache();
  const moddle = new BpmnModdle({ q: qModdle });
  const { rootElement } = await moddle.fromXML(SAMPLE_BPMN);
  const proc = rootElement.rootElements[0];
  startBo = proc.flowElements.find((e) => e.id === 'start1');
  callBo = proc.flowElements.find((e) => e.id === 'call1');
});

describe('FeelEvaluator.evaluate — core surface', () => {

  it('1. evaluates a literal (number) and returns ok=true', () => {
    const r = evaluate('42');
    expect(r.ok).toBe(true);
    expect(r.value).toBe(42);
    expect(r.warnings).toEqual([]);
  });

  it('2. evaluates property access against a context', () => {
    const r = evaluate('payload.orderId', { payload: { orderId: 'ORD-7' } });
    expect(r.ok).toBe(true);
    expect(r.value).toBe('ORD-7');
  });

  it('3. evaluates arithmetic with operator precedence', () => {
    const r = evaluate('1 + 2 * 3');
    expect(r.ok).toBe(true);
    expect(r.value).toBe(7);
  });

  it('4. evaluates boolean and / or / comparison operators', () => {
    const r = evaluate('a > 1 and b < 5', { a: 2, b: 3 });
    expect(r.ok).toBe(true);
    expect(r.value).toBe(true);

    const r2 = evaluate('payload.type = "order.created"', { payload: { type: 'order.created' } });
    expect(r2.ok).toBe(true);
    expect(r2.value).toBe(true);
  });

  it('5. reports parse-failure as ok=false with an error message', () => {
    const r = evaluate('1 + + +');
    expect(r.ok).toBe(false);
    expect(r.error).toBeTypeOf('string');
    expect(r.error.length).toBeGreaterThan(0);
  });

  it('6. reports an undefined property as ok=true with a warning + null value (FEEL spec)', () => {
    // FEEL treats missing context entries as null — this matches the engine's
    // FeelEvaluator behavior on `payload.X` paths where the field is absent.
    const r = evaluate('payload.missing', { payload: {} });
    expect(r.ok).toBe(true);
    expect(r.value).toBeNull();
    expect(r.warnings.length).toBeGreaterThan(0);
    expect(r.warnings[0].type).toBe('NO_CONTEXT_ENTRY_FOUND');
  });

  it('7. evaluates 1-based array indexing per FEEL spec', () => {
    const r = evaluate('items[1]', { items: ['first', 'second', 'third'] });
    expect(r.ok).toBe(true);
    expect(r.value).toBe('first');
  });

  it('8. evaluates the string-length builtin', () => {
    const r = evaluate('string length(s)', { s: 'hello' });
    expect(r.ok).toBe(true);
    expect(r.value).toBe(5);
  });
});

describe('FeelEvaluator.evaluateWithSample — sample-input plumbing', () => {

  it('9. surfaces JSON parse errors via sampleError (does NOT throw)', () => {
    const r = evaluateWithSample('payload.x', '{ this is: not JSON ');
    expect(r.ok).toBe(false);
    expect(r.sampleError).toBeTypeOf('string');
    expect(r.sampleError).toMatch(/Sample JSON invalid/);
    expect(r.error).toBeUndefined();
  });

  it('10. rejects non-object JSON (array / scalar) with a clear message', () => {
    const r = evaluateWithSample('payload.x', '[1,2,3]');
    expect(r.ok).toBe(false);
    expect(r.sampleError).toMatch(/must be a JSON object/);
  });

  it('11. evaluates against parsed sample JSON object', () => {
    const r = evaluateWithSample('payload.orderId', '{"payload":{"orderId":"ORD-99"}}');
    expect(r.ok).toBe(true);
    expect(r.value).toBe('ORD-99');
  });

  it('12. treats empty sample input as empty context (and still evaluates literals)', () => {
    const r = evaluateWithSample('1 + 1', '');
    expect(r.ok).toBe(true);
    expect(r.value).toBe(2);
  });
});

describe('FeelEvaluator.formatResult — UI rendering', () => {

  it('13. renders ok results compactly', () => {
    expect(formatResult({ ok: true, value: 7, warnings: [] })).toBe('7');
    expect(formatResult({ ok: true, value: 'hi', warnings: [] })).toBe('"hi"');
    expect(formatResult({ ok: true, value: null, warnings: [] })).toBe('null');
    expect(formatResult({ ok: true, value: { a: 1 }, warnings: [] })).toBe('{"a":1}');
  });

  it('14. prefixes error messages and surfaces warning counts', () => {
    expect(formatResult({ ok: false, error: 'boom' })).toBe('Error: boom');
    expect(formatResult({ ok: true, value: null, warnings: [{ type: 'X' }] }))
      .toBe('null  (1 warning)');
  });
});

describe('feelPreview entry descriptors', () => {

  it('15. sampleEntry round-trips through the in-memory cache', () => {
    const el = { businessObject: startBo };
    const entry = sampleEntry(el, 'q-alias');
    expect(entry.id).toBe('q-alias-sample');
    expect(entry.label).toBe('Sample input (JSON)');
    expect(entry.get().value).toBe('');

    entry.set(null, '{"payload":{"orderId":"X"}}');
    expect(entry.get().value).toBe('{"payload":{"orderId":"X"}}');
    expect(__testing.getSample(el)).toBe('{"payload":{"orderId":"X"}}');
  });

  it('16. previewEntry evaluates against the cached sample on each get()', () => {
    const el = { businessObject: startBo };
    sampleEntry(el, 'q-alias').set(null, '{"payload":{"orderId":"ABC"}}');

    let currentExpr = 'payload.orderId';
    const preview = previewEntry(el, 'q-alias', () => currentExpr);
    expect(preview.id).toBe('q-alias-preview');
    expect(preview.readOnly).toBe(true);
    expect(preview.get().value).toBe('"ABC"');

    // simulate user editing the expression — preview stays live
    currentExpr = 'payload.orderId + "-suffix"';
    expect(preview.get().value).toBe('"ABC-suffix"');
  });
});

describe('QAliasGroup integration — live preview wired into the panel', () => {

  it('17. QAliasGroup exposes a working sample + preview pair against the alias expression', () => {
    const el = { businessObject: startBo };
    const group = QAliasGroup(el, (s) => s);

    const sample = group.entries.find((e) => e.id === 'q-alias-sample');
    const preview = group.entries.find((e) => e.id === 'q-alias-preview');
    expect(sample).toBeTruthy();
    expect(preview).toBeTruthy();
    expect(sample.__preview).toBe(true);
    expect(preview.__preview).toBe(true);

    // alias expression on the imported BPMN is `payload.orderId`
    sample.set(null, '{"payload":{"orderId":"INT-1"}}');
    const out = preview.get();
    expect(out.value).toBe('"INT-1"');
    expect(out.result.ok).toBe(true);
    expect(out.result.value).toBe('INT-1');
  });

  it('18. QDispatchGroup exposes a working sample + preview pair against the dispatch key', () => {
    const el = { businessObject: callBo };
    const group = QDispatchGroup(el, (s) => s);

    const sample = group.entries.find((e) => e.id === 'q-dispatch-sample');
    const preview = group.entries.find((e) => e.id === 'q-dispatch-preview');
    expect(sample).toBeTruthy();
    expect(preview).toBeTruthy();

    sample.set(null, '{"payload":{"messageType":"order.created"}}');
    const out = preview.get();
    // dispatch key is `payload.messageType` per the imported BPMN
    expect(out.value).toBe('"order.created"');
    expect(out.result.ok).toBe(true);

    // bad sample is surfaced — does not crash
    sample.set(null, '{ broken');
    const bad = preview.get();
    expect(bad.result.ok).toBe(false);
    expect(bad.result.sampleError).toMatch(/Sample JSON invalid/);
    expect(bad.value).toMatch(/Sample JSON invalid/);
  });
});
