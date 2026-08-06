import { describe, it, expect } from 'vitest';
import { parseBpmn } from '../parser.js';
import { buildSymbolTable } from '../symbols.js';
import { computeDiagnostics } from '../diagnostics.js';
import { defaultRegistry } from '../registry.js';
import { ORDER_PROCESS, BAD_VALIDATOR, DISPATCH_PROCESS } from './fixtures.js';

function diag(code: string, source: string) {
  const parsed = parseBpmn(source);
  const table = buildSymbolTable(source, parsed);
  return computeDiagnostics(parsed, table, defaultRegistry()).find((d) => d.code === code);
}

describe('diagnostics — schema conformance to xsd/q.xsd', () => {
  it('emits no diagnostics for a well-formed document', () => {
    const parsed = parseBpmn(ORDER_PROCESS);
    const table = buildSymbolTable(ORDER_PROCESS, parsed);
    const diags = computeDiagnostics(parsed, table, defaultRegistry());
    expect(diags).toEqual([]);
  });

  it('emits SUTRA.RESOLVE.VALIDATOR.UNKNOWN at the correct range for an unknown validator id', () => {
    const parsed = parseBpmn(BAD_VALIDATOR);
    const table = buildSymbolTable(BAD_VALIDATOR, parsed);
    const diags = computeDiagnostics(parsed, table, defaultRegistry());

    const validatorDiag = diags.find((d) => d.code === 'SUTRA.RESOLVE.VALIDATOR.UNKNOWN');
    expect(validatorDiag).toBeDefined();
    expect(validatorDiag!.severity).toBe(1);
    expect(validatorDiag!.message).toContain('xyzzy');

    // Range covers exactly `xyzzy` (no quotes)
    const lineNo = BAD_VALIDATOR.split('\n').findIndex((l) => l.includes('xyzzy'));
    const line = BAD_VALIDATOR.split('\n')[lineNo];
    const startChar = line.indexOf('xyzzy');
    expect(validatorDiag!.range.start.line).toBe(lineNo);
    expect(validatorDiag!.range.start.character).toBe(startChar);
    expect(validatorDiag!.range.end.character).toBe(startChar + 'xyzzy'.length);
  });

  it('emits SUTRA.PARSE.Q_INPUT_MISSING_CODEC when <q:input> has no codec', () => {
    const src = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="s">
      <bpmn:extensionElements>
        <q:input accept="*"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>`;
    expect(diag('SUTRA.PARSE.Q_INPUT_MISSING_CODEC', src)).toBeDefined();
  });

  it('emits SUTRA.PARSE.Q_CASE_MISSING_WHEN when <q:case> has no when', () => {
    const src = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:callActivity id="c">
      <bpmn:extensionElements>
        <q:dispatch>
          <q:case calledElement="foo"/>
        </q:dispatch>
      </bpmn:extensionElements>
    </bpmn:callActivity>
  </bpmn:process>
</bpmn:definitions>`;
    expect(diag('SUTRA.PARSE.Q_CASE_MISSING_WHEN', src)).toBeDefined();
  });

  it('emits SUTRA.PARSE.Q_CASE_MISSING_CALLED_ELEMENT when <q:case> has no calledElement', () => {
    const src = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:callActivity id="c">
      <bpmn:extensionElements>
        <q:dispatch>
          <q:case when="true"/>
        </q:dispatch>
      </bpmn:extensionElements>
    </bpmn:callActivity>
  </bpmn:process>
</bpmn:definitions>`;
    expect(diag('SUTRA.PARSE.Q_CASE_MISSING_CALLED_ELEMENT', src)).toBeDefined();
  });

  it('emits SUTRA.PARSE.Q_ALIAS_MISSING_NAME and SUTRA.PARSE.Q_ALIAS_MISSING_EXPRESSION', () => {
    const noName = `<bpmn:extensionElements xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
      <q:alias expression="payload.x"/>
    </bpmn:extensionElements>`;
    expect(diag('SUTRA.PARSE.Q_ALIAS_MISSING_NAME', noName)).toBeDefined();

    const noExpr = `<bpmn:extensionElements xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
      <q:alias name="orderKey"/>
    </bpmn:extensionElements>`;
    expect(diag('SUTRA.PARSE.Q_ALIAS_MISSING_EXPRESSION', noExpr)).toBeDefined();
  });

  it('emits SUTRA.PARSE.Q_REPLY_INVALID_MODE for a bad reply mode', () => {
    const src = `<bpmn:extensionElements xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
      <q:reply mode="bogus"/>
    </bpmn:extensionElements>`;
    const d = diag('SUTRA.PARSE.Q_REPLY_INVALID_MODE', src);
    expect(d).toBeDefined();
    expect(d!.message).toContain('native');
  });

  it('emits SUTRA.PARSE.Q_ON_VALIDATION_INVALID_MODE for a bad onValidation mode', () => {
    const src = `<bpmn:extensionElements xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
      <q:onValidation mode="ignore"/>
    </bpmn:extensionElements>`;
    expect(diag('SUTRA.PARSE.Q_ON_VALIDATION_INVALID_MODE', src)).toBeDefined();
  });

  it('emits invalid-enum for q:audit capture (catch-all schema-XSD code path)', () => {
    const src = `<bpmn:extensionElements xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
      <q:audit capture="all"/>
    </bpmn:extensionElements>`;
    const d = diag('SUTRA.PARSE.QXSD.INVALID_CAPTURE', src);
    expect(d).toBeDefined();
    expect(d!.message).toContain('payload');
  });

  it('emits an unknown-attribute warning for typoed attributes on a known element', () => {
    const src = `<bpmn:extensionElements xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
      <q:alias name="orderKey" expression="payload.x" oncon="reject"/>
    </bpmn:extensionElements>`;
    const d = diag('SUTRA.PARSE.QXSD.UNKNOWN_ATTRIBUTE', src);
    expect(d).toBeDefined();
    expect(d!.severity).toBe(2);
    expect(d!.message).toContain('oncon');
    expect(d!.message).toContain('onConflict');
  });

  it('reports no diagnostics for a fully-valid q:dispatch document', () => {
    const parsed = parseBpmn(DISPATCH_PROCESS);
    const table = buildSymbolTable(DISPATCH_PROCESS, parsed);
    const diags = computeDiagnostics(parsed, table, defaultRegistry());
    // calledElement references won't be flagged because the registry doesn't
    // track called-element ids — those are resolved against process ids,
    // which live in the same document.
    expect(diags).toEqual([]);
  });
});
