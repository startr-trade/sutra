import { describe, it, expect } from 'vitest';
import { buildSymbolTable } from '../symbols.js';
import { computeHover } from '../hover.js';
import { defaultRegistry } from '../registry.js';
import { ORDER_PROCESS } from './fixtures.js';

function positionOf(source: string, search: string, offsetInMatch = 0): { line: number; character: number } {
  const idx = source.indexOf(search);
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

describe('hover', () => {
  it('returns XSD documentation when hovering an attribute name', () => {
    const table = buildSymbolTable(ORDER_PROCESS);
    const pos = positionOf(ORDER_PROCESS, 'unique="true"', 2);
    const hover = computeHover(ORDER_PROCESS, pos, table, defaultRegistry());
    expect(hover).toBeDefined();
    expect(hover!.contents).toContain('q:alias/@unique');
    expect(hover!.contents).toContain('unique');
  });

  it('returns XSD documentation when hovering an element name', () => {
    const table = buildSymbolTable(ORDER_PROCESS);
    const pos = positionOf(ORDER_PROCESS, '<q:input ', 2); // points at "q"
    const hover = computeHover(ORDER_PROCESS, pos, table, defaultRegistry());
    expect(hover).toBeDefined();
    expect(hover!.contents).toContain('q:input');
    expect(hover!.contents.toLowerCase()).toContain('codec');
  });

  it('reports known-id status when hovering a channel reference', () => {
    const table = buildSymbolTable(ORDER_PROCESS);
    const pos = positionOf(ORDER_PROCESS, 'channel="xml"', 'channel="x'.length);
    const hover = computeHover(ORDER_PROCESS, pos, table, defaultRegistry());
    expect(hover).toBeDefined();
    expect(hover!.contents).toMatch(/known|UNKNOWN/);
    expect(hover!.contents).toContain('xml');
  });
});
