import { describe, it, expect } from 'vitest';
import { buildSymbolTable } from '../symbols.js';
import { resolveDefinition } from '../definition.js';
import { defaultRegistry } from '../registry.js';
import { ORDER_PROCESS, DISPATCH_PROCESS } from './fixtures.js';

function positionOfSubstring(source: string, search: string, offsetInMatch: number): { line: number; character: number } {
  const idx = source.indexOf(search);
  if (idx < 0) throw new Error(`could not find ${search}`);
  const absolute = idx + offsetInMatch;
  let line = 0;
  let lineStart = 0;
  for (let i = 0; i < absolute; i++) {
    if (source[i] === '\n') {
      line++;
      lineStart = i + 1;
    }
  }
  return { line, character: absolute - lineStart };
}

describe('go-to-definition', () => {
  it('jumps from <q:case calledElement="invoiceFlow"> to <bpmn:process id="invoiceFlow"> when present in the same file', () => {
    const source = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="router">
    <bpmn:callActivity id="route">
      <bpmn:extensionElements>
        <q:dispatch>
          <q:case when="true" calledElement="invoiceFlow"/>
        </q:dispatch>
      </bpmn:extensionElements>
    </bpmn:callActivity>
  </bpmn:process>
  <bpmn:process id="invoiceFlow"/>
</bpmn:definitions>`;
    const table = buildSymbolTable(source);
    const pos = positionOfSubstring(source, 'calledElement="invoiceFlow"', 'calledElement="invo'.length);
    const links = resolveDefinition(source, pos, table);
    expect(links).toHaveLength(1);
    const targetIdLine = source.split('\n').findIndex((l) => l.includes('id="invoiceFlow"'));
    expect(links[0].targetSelectionRange.start.line).toBe(targetIdLine);
  });

  it('returns no link when calledElement does not resolve to a process in this document', () => {
    const source = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="router">
    <bpmn:callActivity id="route">
      <bpmn:extensionElements>
        <q:dispatch>
          <q:case when="true" calledElement="missing"/>
        </q:dispatch>
      </bpmn:extensionElements>
    </bpmn:callActivity>
  </bpmn:process>
</bpmn:definitions>`;
    const table = buildSymbolTable(source);
    const pos = positionOfSubstring(source, 'calledElement="missing"', 'calledElement="miss'.length);
    expect(resolveDefinition(source, pos, table)).toEqual([]);
  });

  it('handles <q:source channel="xml"> as a known-channel jump (peek-in-place)', () => {
    const table = buildSymbolTable(ORDER_PROCESS);
    const pos = positionOfSubstring(ORDER_PROCESS, 'channel="xml"', 'channel="x'.length);
    const links = resolveDefinition(ORDER_PROCESS, pos, table, defaultRegistry());
    // 'xml' is in the fallback registry's channels, so we expect a link.
    expect(links).toHaveLength(1);
  });

  it('returns no link for an unknown channel id when registry is provided', () => {
    const source = ORDER_PROCESS.replace('channel="xml"', 'channel="unknown"');
    const table = buildSymbolTable(source);
    const pos = positionOfSubstring(source, 'channel="unknown"', 'channel="unk'.length);
    expect(resolveDefinition(source, pos, table, defaultRegistry())).toEqual([]);
  });

  it('does not crash on a fully-valid dispatch fixture (smoke)', () => {
    const table = buildSymbolTable(DISPATCH_PROCESS);
    // Cursor in the middle of nothing relevant — first line of XML decl.
    expect(resolveDefinition(DISPATCH_PROCESS, { line: 0, character: 0 }, table)).toEqual([]);
  });
});
