import { describe, it, expect } from 'vitest';
import { computeCompletions } from '../completion.js';
import { defaultRegistry } from '../registry.js';

function positionFor(source: string, marker: string, offsetInMarker?: number): { line: number; character: number } {
  // Convert a flat offset into {line, character}
  const idx = source.indexOf(marker);
  const offset = idx + (offsetInMarker ?? marker.length);
  let line = 0;
  let lineStart = 0;
  for (let i = 0; i < offset; i++) {
    if (source[i] === '\n') {
      line++;
      lineStart = i + 1;
    }
  }
  return { line, character: offset - lineStart };
}

describe('completion', () => {
  it('suggests known channel ids when cursor is inside <q:source channel="">', () => {
    const source = `<q:source channel=""/>`;
    const cursorOffset = source.indexOf('channel="') + 'channel="'.length;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    const labels = items.map((i) => i.label).sort();
    expect(labels).toEqual(['dmn', 'schema', 'srl', 'xml']);
    expect(items.every((i) => i.detail === 'channel id')).toBe(true);
  });

  it('returns codec ids when cursor is inside <q:input codec="">', () => {
    const source = `<q:input codec=""/>`;
    const cursorOffset = source.indexOf('codec="') + 'codec="'.length;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    expect(items.map((i) => i.label).sort()).toEqual(['dmn', 'schema', 'srl', 'xml']);
    expect(items[0].detail).toBe('codec id');
  });

  it('returns validator ids when cursor is inside <q:validators source="">', () => {
    const source = `<q:validators source=""/>`;
    const cursorOffset = source.indexOf('source="') + 'source="'.length;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    expect(items.map((i) => i.label).sort()).toEqual(['dmn', 'schema', 'srl', 'xml']);
    expect(items[0].detail).toBe('validator id');
  });

  it('returns enum values when cursor is inside an enum attribute (<q:reply mode="">)', () => {
    const source = `<q:reply mode=""/>`;
    const cursorOffset = source.indexOf('mode="') + 'mode="'.length;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    expect(items.map((i) => i.label).sort()).toEqual([
      'cloudevent-binary',
      'cloudevent-structured',
      'match-inbound',
      'native',
    ]);
  });

  it('returns onValidation mode enums', () => {
    const source = `<q:onValidation mode=""/>`;
    const cursorOffset = source.indexOf('mode="') + 'mode="'.length;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    expect(items.map((i) => i.label).sort()).toEqual(['error', 'reject', 'route']);
  });

  it('returns no completions outside any referencing attribute (id="")', () => {
    const source = `<q:source id="s" channel="orders-in"/>`;
    const cursorOffset = source.indexOf('id="') + 'id="'.length;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    expect(items).toEqual([]);
  });

  it('suggests all 9 schema-valid q:* elements inside <bpmn:extensionElements>', () => {
    const source = `<bpmn:extensionElements>
PLACEHOLDER
</bpmn:extensionElements>`;
    // Cursor at start of the PLACEHOLDER line
    const idx = source.indexOf('PLACEHOLDER');
    let line = 0;
    let lineStart = 0;
    for (let i = 0; i < idx; i++) {
      if (source[i] === '\n') {
        line++;
        lineStart = i + 1;
      }
    }
    const position = { line, character: idx - lineStart };
    // Replace placeholder with empty cursor — pass the source as-is, but the
    // completion provider works on the raw source plus cursor position.
    const items = computeCompletions(source.replace('PLACEHOLDER', ''), position, defaultRegistry());
    const labels = items.map((i) => i.label).sort();
    expect(labels).toEqual([
      'q:alias',
      'q:audit',
      'q:case',
      'q:dispatch',
      'q:input',
      'q:onValidation',
      'q:reply',
      'q:source',
      'q:validators',
    ]);
    // Required attributes embedded in snippet body
    const inputSnippet = items.find((i) => i.label === 'q:input');
    expect(inputSnippet?.insertText).toContain('codec=');
    const caseSnippet = items.find((i) => i.label === 'q:case');
    expect(caseSnippet?.insertText).toContain('when=');
    expect(caseSnippet?.insertText).toContain('calledElement=');
  });

  it('suggests attribute names inside an open tag at <q:case |', () => {
    const source = `<q:case >`;
    // cursor placed between the space and the `>`
    const cursorOffset = source.indexOf('<q:case ') + '<q:case '.length;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    const labels = items.map((i) => i.label).sort();
    expect(labels).toEqual(['calledElement', 'scope', 'when']);
    const required = items.filter((i) => i.detail?.includes('required')).map((i) => i.label).sort();
    expect(required).toEqual(['calledElement', 'when']);
  });

  it('omits already-present attributes from <q:case | completions', () => {
    const source = `<q:case when="x" >`;
    const cursorOffset = source.indexOf('" ') + 2;
    const items = computeCompletions(source, { line: 0, character: cursorOffset }, defaultRegistry());
    expect(items.map((i) => i.label).sort()).toEqual(['calledElement', 'scope']);
  });
});
