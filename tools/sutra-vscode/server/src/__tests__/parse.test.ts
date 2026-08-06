import { describe, it, expect } from 'vitest';
import { parseBpmn } from '../parser.js';
import { buildSymbolTable } from '../symbols.js';
import { ORDER_PROCESS, DISPATCH_PROCESS, AUDIT_PROCESS } from './fixtures.js';

describe('symbol-table extraction', () => {
  it('extracts schema-valid q:* elements from a start-event payload binding', () => {
    const parsed = parseBpmn(ORDER_PROCESS);
    expect(parsed.errors).toHaveLength(0);

    const table = buildSymbolTable(ORDER_PROCESS, parsed);
    const kinds = table.symbols.map((s) => s.kind).sort();
    expect(kinds).toEqual([
      'alias',
      'input',
      'onValidation',
      'reply',
      'source',
      'validators',
    ]);

    const alias = table.symbols.find((s) => s.kind === 'alias');
    expect(alias?.attributes.name).toBe('orderKey');
    expect(alias?.attributes.expression).toBe('payload.orderId');
    expect(alias?.attributes.unique).toBe('true');
    expect(alias?.attributes.onConflict).toBe('correlate');

    const input = table.symbols.find((s) => s.kind === 'input');
    expect(input?.attributes.codec).toBe('xml');
    expect(input?.attributes.accept).toBe('application/xml');
  });

  it('collects cross-references on channel, codec, validator source, destination', () => {
    const table = buildSymbolTable(ORDER_PROCESS);
    const refs = table.references.map((r) => `${r.attribute}=${r.value}`).sort();
    expect(refs).toContain('channel=xml');
    expect(refs).toContain('codec=xml');
    expect(refs).toContain('validator=dmn');
    // q:reply destination= surfaces as a channel reference
    expect(refs.filter((r) => r === 'channel=xml')).toHaveLength(2);
  });

  it('records FEEL expressions on FEEL-bearing attributes (alias/@expression, case/@when)', () => {
    const order = buildSymbolTable(ORDER_PROCESS);
    expect(order.feel).toHaveLength(1);
    expect(order.feel[0].tag).toBe('q:alias');
    expect(order.feel[0].attribute).toBe('expression');
    expect(order.feel[0].text).toBe('payload.orderId');

    const dispatch = buildSymbolTable(DISPATCH_PROCESS);
    const whens = dispatch.feel
      .filter((f) => f.tag === 'q:case' && f.attribute === 'when')
      .map((f) => f.text);
    expect(whens).toEqual([
      "payload.kind = 'invoice'",
      "payload.kind = 'order'",
    ]);
  });

  it('parses q:dispatch + q:case + q:audit elements per xsd/q.xsd', () => {
    const dispatch = buildSymbolTable(DISPATCH_PROCESS);
    const dispKinds = dispatch.symbols.map((s) => s.kind).sort();
    expect(dispKinds).toEqual(['case', 'case', 'dispatch']);
    const audit = buildSymbolTable(AUDIT_PROCESS);
    expect(audit.symbols.map((s) => s.kind)).toEqual(['audit']);
    expect(audit.symbols[0].attributes.sink).toBe('jsonl');
    expect(audit.symbols[0].attributes.capture).toBe('metadata');
  });

  it('does NOT recognise the legacy orphan elements (q:expression / q:condition / q:guard / q:field / q:schema)', () => {
    // Regression guard — these were never in xsd/q.xsd and must
    // not resurface in the symbol table.
    const orphanSource = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="legacy">
    <q:expression>1+1</q:expression>
    <q:condition>true</q:condition>
    <q:guard>false</q:guard>
    <q:schema><q:field name="foo"/></q:schema>
  </bpmn:process>
</bpmn:definitions>`;
    const table = buildSymbolTable(orphanSource);
    expect(table.symbols).toHaveLength(0);
    expect(table.feel).toHaveLength(0);
  });
});
