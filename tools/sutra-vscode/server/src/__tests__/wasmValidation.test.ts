import { describe, it, expect } from 'vitest';
import { createRequire } from 'node:module';
import { parseBpmn } from '../parser.js';
import {
  buildLintRequest,
  parseWasmDiagnostics,
  mapWasmDiagnostics,
  resolveAnchorRange,
  WasmDiagnostic,
} from '../wasmValidation.js';

// A two-process document with a DUPLICATED node id (`task1` in both processes),
// so the process-scoped range resolution is actually exercised.
const TWO_PROCESS = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="order">
    <bpmn:startEvent id="start1"/>
    <bpmn:task id="task1"/>
  </bpmn:process>
  <bpmn:process id="other">
    <bpmn:task id="task1"/>
  </bpmn:process>
</bpmn:definitions>`;

const LINES = TWO_PROCESS.split('\n');
const lineOf = (needle: string) => LINES.findIndex((l) => l.includes(needle));

describe('buildLintRequest', () => {
  it('serialises files only when no labels are given', () => {
    const json = buildLintRequest({ 'bpmn/order.bpmn': '<xml/>' });
    expect(JSON.parse(json)).toEqual({ files: { 'bpmn/order.bpmn': '<xml/>' } });
  });

  it('includes labels when provided', () => {
    const json = buildLintRequest({ 'channels.yaml': 'channels: []' }, { tenant: 'acme' });
    expect(JSON.parse(json)).toEqual({
      files: { 'channels.yaml': 'channels: []' },
      labels: { tenant: 'acme' },
    });
  });
});

describe('parseWasmDiagnostics', () => {
  it('parses a well-formed diagnostics array', () => {
    const out = parseWasmDiagnostics(
      '[{"severity":"error","code":"SUTRA.X","message":"boom"}]'
    );
    expect(out).toEqual([{ severity: 'error', code: 'SUTRA.X', message: 'boom' }]);
  });

  it('returns [] for non-JSON, non-array, and drops malformed entries', () => {
    expect(parseWasmDiagnostics('not json')).toEqual([]);
    expect(parseWasmDiagnostics('{"not":"array"}')).toEqual([]);
    expect(
      parseWasmDiagnostics('[{"severity":"nope","code":"C","message":"m"},{"code":"D"}]')
    ).toEqual([]);
  });
});

describe('resolveAnchorRange / mapWasmDiagnostics', () => {
  const parsed = parseBpmn(TWO_PROCESS);
  const OPEN = 'bpmn/order.bpmn';

  it('ranges a bpmnNode anchor at the node inside the matching process', () => {
    const orderTask = resolveAnchorRange(
      { kind: 'bpmnNode', process: 'order', node: 'task1' },
      parsed
    );
    const otherTask = resolveAnchorRange(
      { kind: 'bpmnNode', process: 'other', node: 'task1' },
      parsed
    );
    // Both processes declare a `task1`; the process scope disambiguates them.
    expect(orderTask?.start.line).toBe(lineOf('<bpmn:task id="task1"')); // first occurrence (order)
    expect(otherTask?.start.line).toBe(LINES.length - 3); // second occurrence (other)
    expect(orderTask?.start.line).not.toBe(otherTask?.start.line);
  });

  it('ranges a bpmnProcess anchor at the process header', () => {
    const range = resolveAnchorRange({ kind: 'bpmnProcess', process: 'other' }, parsed);
    expect(range?.start.line).toBe(lineOf('<bpmn:process id="other"'));
  });

  it('returns null for an unresolvable node and for namedEntry anchors', () => {
    expect(resolveAnchorRange({ kind: 'bpmnNode', process: 'order', node: 'ghost' }, parsed)).toBeNull();
    expect(resolveAnchorRange({ kind: 'namedEntry', name: 'in' }, parsed)).toBeNull();
  });

  it('surfaces open-doc diagnostics ranged, deployment-level at 0:0, and skips other files', () => {
    const diags: WasmDiagnostic[] = [
      // open-doc, node-anchored → ranged
      {
        severity: 'error',
        code: 'SUTRA.SCHEMA.FIELD_UNKNOWN',
        message: 'field x not in schema',
        site: { file: OPEN, anchor: { kind: 'bpmnNode', process: 'order', node: 'task1' } },
      },
      // deployment-level (no file) → 0:0 on the open doc
      { severity: 'error', code: 'SUTRA.LSP.REQUEST_INVALID', message: 'bad' },
      // another file → skipped while the bpmn is the active doc
      {
        severity: 'warning',
        code: 'SUTRA.INBOUND.CODEC_NOT_FOUND',
        message: 'codec missing',
        site: { file: 'channels.yaml', anchor: { kind: 'namedEntry', name: 'in' } },
      },
    ];

    const mapped = mapWasmDiagnostics(diags, OPEN, parsed);
    expect(mapped.map((d) => d.code)).toEqual([
      'SUTRA.SCHEMA.FIELD_UNKNOWN',
      'SUTRA.LSP.REQUEST_INVALID',
    ]);
    // severity: error → 1, and it landed on the order/task1 line (not 0:0)
    expect(mapped[0].severity).toBe(1);
    expect(mapped[0].range.start.line).toBe(lineOf('<bpmn:task id="task1"'));
    // deployment-level diagnostic pinned to the top of the document
    expect(mapped[1].range).toEqual({ start: { line: 0, character: 0 }, end: { line: 0, character: 0 } });
  });

  it('maps warning severity to 2 and falls back to 0:0 when the anchor is unresolvable', () => {
    const diags: WasmDiagnostic[] = [
      {
        severity: 'warning',
        code: 'SUTRA.CONFIG.CHANNEL.INERT',
        message: 'inert',
        site: { file: OPEN, anchor: { kind: 'bpmnNode', process: 'order', node: 'ghost' } },
      },
    ];
    const mapped = mapWasmDiagnostics(diags, OPEN, parsed);
    expect(mapped[0].severity).toBe(2);
    expect(mapped[0].range.start).toEqual({ line: 0, character: 0 });
  });
});

// Genuine end-to-end exercise of the generated wasm-bindgen glue (loaded through a
// native `require` so vite does not transform the CJS + `__dirname` wasm read),
// proving the bindings work — the same contract the Rust crate asserts natively.
describe('generated WASM glue (sutra-lint-core)', () => {
  const require = createRequire(import.meta.url);
  let lint: ((req: string) => string) | null = null;
  try {
    lint = require('../generated/sutra_lint_core.js').lint;
  } catch {
    lint = null;
  }

  it.skipIf(!lint)('returns [] for an empty deployment', () => {
    expect(JSON.parse(lint!(buildLintRequest({})))).toEqual([]);
  });

  it.skipIf(!lint)('returns SUTRA.LSP.REQUEST_INVALID for a malformed request', () => {
    const out = parseWasmDiagnostics(lint!('{ not json'));
    expect(out).toHaveLength(1);
    expect(out[0].code).toBe('SUTRA.LSP.REQUEST_INVALID');
  });

  it.skipIf(!lint)('flags an unresolvable channel codec, anchored at its channels.yaml entry', () => {
    const req = buildLintRequest({
      'channels.yaml':
        'channels:\n  - name: in\n    transport: http\n    bind: "POST /channels/in"\n    codec: urn:doesnotexist\n',
    });
    const out = parseWasmDiagnostics(lint!(req));
    const codec = out.find((d) => d.code === 'SUTRA.INBOUND.CODEC_NOT_FOUND');
    expect(codec).toBeDefined();
    expect(codec!.severity).toBe('error');
    expect(codec!.site?.file).toBe('channels.yaml');
    expect(codec!.site?.anchor).toMatchObject({ kind: 'namedEntry', name: 'in' });
  });
});
