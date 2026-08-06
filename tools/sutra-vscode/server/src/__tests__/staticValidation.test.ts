import { describe, it, expect } from 'vitest';
import { parseBpmn } from '../parser.js';
import { computeStaticValidation } from '../staticValidation.js';
import { coverageDoc, variableSourceDoc } from './fixtures.js';

function validate(source: string) {
  return computeStaticValidation(parseBpmn(source));
}
function codes(source: string): string[] {
  return validate(source)
    .map((d) => d.code)
    .sort();
}

describe('staticValidation — coverage path extraction (parity with validate_coverage_paths)', () => {
  it('is clean for a contiguous, well-formed coverage path', () => {
    const src = coverageDoc(`      <q:coverage path="happy" flows="f1 f2"/>`);
    expect(validate(src)).toEqual([]);
  });

  it('ERRORs SUTRA.CONFIG.COVERAGE.UNKNOWN_FLOW for a flow that is not a sequenceFlow', () => {
    const src = coverageDoc(`      <q:coverage path="happy" flows="f1 fX"/>`);
    const d = validate(src).find((x) => x.code === 'SUTRA.CONFIG.COVERAGE.UNKNOWN_FLOW');
    expect(d).toBeDefined();
    expect(d!.severity).toBe(1);
    expect(d!.message).toContain("references flow 'fX'");

    // Squiggle underlines exactly the offending `fX` token inside the flows list.
    const lineNo = src.split('\n').findIndex((l) => l.includes('flows="f1 fX"'));
    const startChar = src.split('\n')[lineNo].indexOf('fX');
    expect(d!.range.start.line).toBe(lineNo);
    expect(d!.range.start.character).toBe(startChar);
    expect(d!.range.end.character).toBe(startChar + 'fX'.length);
  });

  it('ERRORs SUTRA.CONFIG.COVERAGE.INVALID_ROUTE for a non-contiguous route', () => {
    const src = coverageDoc(`      <q:coverage path="reversed" flows="f2 f1"/>`);
    const d = validate(src).find((x) => x.code === 'SUTRA.CONFIG.COVERAGE.INVALID_ROUTE');
    expect(d).toBeDefined();
    expect(d!.severity).toBe(1);
    expect(d!.message).toContain('is not a contiguous route');
  });

  it('ERRORs SUTRA.CONFIG.COVERAGE.INVALID_ROUTE when a path lists no flows', () => {
    const src = coverageDoc(`      <q:coverage path="empty" flows=""/>`);
    const d = validate(src).find((x) => x.code === 'SUTRA.CONFIG.COVERAGE.INVALID_ROUTE');
    expect(d).toBeDefined();
    expect(d!.message).toContain('lists no flows');
  });

  it('ERRORs SUTRA.CONFIG.COVERAGE.DUPLICATE_PATH when a path id repeats', () => {
    const src = coverageDoc(
      `      <q:coverage path="happy" flows="f1 f2"/>\n      <q:coverage path="happy" flows="f1 f2"/>`
    );
    const dups = validate(src).filter((x) => x.code === 'SUTRA.CONFIG.COVERAGE.DUPLICATE_PATH');
    expect(dups).toHaveLength(1);
    expect(dups[0].message).toContain('declared more than once');
  });

  it('collects every issue rather than stopping at the first (editor posture)', () => {
    const src = coverageDoc(
      `      <q:coverage path="a" flows="f1 fX"/>\n      <q:coverage path="a" flows="f2 f1"/>`
    );
    // path "a": unknown flow fX; then the duplicate "a".
    expect(codes(src)).toEqual([
      'SUTRA.CONFIG.COVERAGE.DUPLICATE_PATH',
      'SUTRA.CONFIG.COVERAGE.UNKNOWN_FLOW',
    ]);
  });
});

describe('staticValidation — variable source intake (parity with validate_variable_sources)', () => {
  it('is clean when the variable feeds off a subscribed channel', () => {
    const src = variableSourceDoc(`      <q:variable name="acct" source="ordersIn"/>`);
    expect(validate(src)).toEqual([]);
  });

  it('ERRORs SUTRA.CONFIG.BPMN.VARIABLE_SOURCE_UNKNOWN for an unsubscribed channel', () => {
    const src = variableSourceDoc(`      <q:variable name="acct" source="shipmentsIn"/>`);
    const d = validate(src).find((x) => x.code === 'SUTRA.CONFIG.BPMN.VARIABLE_SOURCE_UNKNOWN');
    expect(d).toBeDefined();
    expect(d!.severity).toBe(1);
    expect(d!.message).toContain("source=\"shipmentsIn\"");
    expect(d!.message).toContain('no intake node in the process subscribes to it');

    // Range covers exactly the `shipmentsIn` source value (no quotes).
    const lineNo = src.split('\n').findIndex((l) => l.includes('source="shipmentsIn"'));
    const startChar = src.split('\n')[lineNo].indexOf('shipmentsIn');
    expect(d!.range.start.line).toBe(lineNo);
    expect(d!.range.start.character).toBe(startChar);
    expect(d!.range.end.character).toBe(startChar + 'shipmentsIn'.length);
  });

  it('does not flag an in-instance variable (no @source)', () => {
    const src = variableSourceDoc(`      <q:variable name="counter" type="number"/>`);
    expect(validate(src)).toEqual([]);
  });
});
