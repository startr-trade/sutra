/**
 * WASM lint bridge — the in-editor half of the schema/codec/FEEL-heavy
 * deploy-time checks that need cross-file context (channels.yaml, schemas/*.xsd,
 * referenced templates). These are the checks that were too expensive to
 * re-implement in TypeScript (`staticValidation.ts` covers only the cheap
 * single-document ones); instead we compile the Rust `sutra lint` core to WASM
 * (`sutra-lint-core`) and call it here, so in-editor == deploy-time BY
 * CONSTRUCTION (same code, no drift).
 *
 * Flow (orchestrated in `server.ts`):
 *   1. `workspaceConfig.gatherDeploymentFiles` builds the `{ archivePath: content }`
 *      map around the open `.bpmn` doc.
 *   2. {@link buildLintRequest} serialises it to the request JSON.
 *   3. {@link runWasmLint} calls the WASM `lint(requestJson)` and parses the
 *      diagnostics array.
 *   4. {@link mapWasmDiagnostics} maps each diagnostic that concerns the OPEN doc
 *      (or the whole deployment) to an editor-ranged `BpmDiagnostic`.
 *
 * The WASM module is loaded lazily + guarded: if it is missing or fails to load,
 * every entry point degrades to "no cross-file diagnostics" so the intra-document
 * TS checks (`diagnostics.ts` / `staticValidation.ts`) keep working unaffected.
 */

import type { ParseResult, Range } from './parser.js';
import type { BpmDiagnostic } from './diagnostics.js';

/** Anchor kinds emitted by the Rust `DiagnosticAnchor` (camelCase, internally tagged). */
export type WasmAnchor =
  | { kind: 'bpmnNode'; process: string; node: string }
  | { kind: 'bpmnProcess'; process: string }
  | { kind: 'namedEntry'; name: string };

/** One diagnostic as emitted by the WASM lint boundary (`sutra-lint-core::lint`). */
export interface WasmDiagnostic {
  severity: 'error' | 'warning';
  code: string;
  message: string;
  site?: {
    /** archive-local path the diagnostic applies to, e.g. `bpmn/order.bpmn`; absent = deployment-level. */
    file?: string;
    anchor?: WasmAnchor;
  };
}

/** A zero-width range at the top of the document — the fallback when an anchor
 *  cannot be resolved to a concrete element (file-level squiggle). */
const ZERO_RANGE: Range = {
  start: { line: 0, character: 0 },
  end: { line: 0, character: 0 },
};

/** The minimal shape of the generated wasm-bindgen glue we call. */
interface LintModule {
  lint(requestJson: string): string;
}

// Lazy, guarded module handle: `undefined` = not yet attempted, `null` = load failed.
let wasmModule: LintModule | null | undefined;

/**
 * Load the generated wasm-bindgen glue (`generated/sutra_lint_core.js`), once.
 * The glue is CommonJS and reads its sibling `.wasm` synchronously at require
 * time, so a plain `require` yields a ready `{ lint }`. Any failure (module or
 * wasm absent) is swallowed and cached as `null` — the caller then simply adds
 * no cross-file diagnostics.
 */
function loadWasm(): LintModule | null {
  if (wasmModule !== undefined) return wasmModule;
  try {
    // The built server runs as CommonJS (see tsconfig `module: NodeNext`, no
    // `"type": "module"` in package.json), so `require` is the native loader and
    // the glue's `__dirname`-relative `.wasm` read resolves correctly.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    wasmModule = require('./generated/sutra_lint_core.js') as LintModule;
  } catch {
    wasmModule = null;
  }
  return wasmModule;
}

/** Serialise the gathered deployment files (+ optional labels) into the WASM request JSON. */
export function buildLintRequest(
  files: Record<string, string>,
  labels?: Record<string, string>
): string {
  return JSON.stringify(labels ? { files, labels } : { files });
}

/**
 * Call the WASM lint with `requestJson` and parse its diagnostics array. Returns
 * `[]` when the WASM is unavailable or returns something unparseable — the lint
 * is advisory and must never break the editor. (The WASM itself never throws: a
 * malformed request comes back as a single `SUTRA.LSP.REQUEST_INVALID`.)
 */
export function runWasmLint(requestJson: string): WasmDiagnostic[] {
  const mod = loadWasm();
  if (!mod) return [];
  let out: string;
  try {
    out = mod.lint(requestJson);
  } catch {
    return [];
  }
  return parseWasmDiagnostics(out);
}

/** Parse the WASM output string into `WasmDiagnostic[]`, tolerating malformed output. */
export function parseWasmDiagnostics(out: string): WasmDiagnostic[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(out);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  const diags: WasmDiagnostic[] = [];
  for (const d of parsed) {
    if (
      d &&
      typeof d === 'object' &&
      typeof (d as WasmDiagnostic).code === 'string' &&
      typeof (d as WasmDiagnostic).message === 'string' &&
      ((d as WasmDiagnostic).severity === 'error' || (d as WasmDiagnostic).severity === 'warning')
    ) {
      diags.push(d as WasmDiagnostic);
    }
  }
  return diags;
}

/**
 * Map the WASM diagnostics to editor-ranged `BpmDiagnostic`s for the OPEN document.
 *
 * Routing (documented in the P2d plan):
 *   - `site.file === openDocArchivePath` → SURFACED, ranged to the anchored BPMN
 *     node / process (via the parser event stream), or `ZERO_RANGE` when the
 *     anchor cannot be resolved to a concrete element.
 *   - no `site.file` (deployment-level, e.g. an unbuildable-deployment error) →
 *     SURFACED at `ZERO_RANGE` (it concerns the whole deployment being edited).
 *   - `site.file` naming a DIFFERENT artifact (another bpmn, `channels.yaml`) →
 *     SKIPPED here; it surfaces when that artifact is the active document. The
 *     headline schema/codec `@schema`/`@source` field-checks anchor to BPMN nodes
 *     in the open doc, so they do surface.
 */
export function mapWasmDiagnostics(
  diags: WasmDiagnostic[],
  openDocArchivePath: string,
  parsed: ParseResult
): BpmDiagnostic[] {
  const out: BpmDiagnostic[] = [];
  for (const d of diags) {
    const file = d.site?.file;
    const isOpenDoc = !!file && file === openDocArchivePath;
    const isDeploymentLevel = !file;
    if (!isOpenDoc && !isDeploymentLevel) continue; // other-file → skip

    const range =
      isOpenDoc && d.site?.anchor ? resolveAnchorRange(d.site.anchor, parsed) ?? ZERO_RANGE : ZERO_RANGE;

    out.push({
      range,
      severity: d.severity === 'error' ? 1 : 2,
      code: d.code,
      source: 'sutra',
      message: d.message,
    });
  }
  return out;
}

/**
 * Resolve a WASM anchor to a text `Range` in the open document by locating the
 * named element in the parser event stream. `bpmnNode` matches the element whose
 * `id` attribute equals `node` (preferring one inside `<*:process id="process">`);
 * `bpmnProcess` matches the process header. Returns `null` when unresolvable.
 */
export function resolveAnchorRange(anchor: WasmAnchor, parsed: ParseResult): Range | null {
  if (anchor.kind === 'bpmnProcess') {
    return findProcessHeaderRange(anchor.process, parsed);
  }
  if (anchor.kind === 'bpmnNode') {
    return findNodeRange(anchor.process, anchor.node, parsed);
  }
  return null; // namedEntry anchors belong to config YAML, not the open bpmn
}

/** Local element name, dropping any namespace prefix (`bpmn:process` → `process`). */
function localOf(name: string): string {
  const idx = name.indexOf(':');
  return idx >= 0 ? name.slice(idx + 1) : name;
}

/** Range of `<*:process id="{process}">`'s tag name, or `null`. */
function findProcessHeaderRange(process: string, parsed: ParseResult): Range | null {
  for (const ev of parsed.events) {
    if (ev.kind !== 'open' && ev.kind !== 'self-closing') continue;
    if (localOf(ev.name) !== 'process') continue;
    const id = ev.attributes.find((a) => a.name === 'id')?.value;
    if (id === process) return ev.nameRange;
  }
  return null;
}

/**
 * Range of the element whose `id` is `node`. Two passes: first restricted to the
 * matching `<*:process id="{process}">` scope (disambiguates a multi-process
 * file), then a whole-document fallback so a node still ranges when the process
 * id does not line up. Returns the tag-name range, or `null` when not found.
 */
function findNodeRange(process: string, node: string, parsed: ParseResult): Range | null {
  let inProcess = false;
  let fallback: Range | null = null;

  for (const ev of parsed.events) {
    if (ev.kind === 'close') {
      if (localOf(ev.name) === 'process' && inProcess) inProcess = false;
      continue;
    }
    if (ev.kind !== 'open' && ev.kind !== 'self-closing') continue;

    if (localOf(ev.name) === 'process') {
      const pid = ev.attributes.find((a) => a.name === 'id')?.value;
      inProcess = pid === process && ev.kind === 'open';
      continue;
    }

    const id = ev.attributes.find((a) => a.name === 'id')?.value;
    if (id !== node) continue;
    if (inProcess) return ev.nameRange; // best match: right node in the right process
    if (fallback === null) fallback = ev.nameRange; // remember first any-scope match
  }

  // No scoped match → the first document-wide match (still a better squiggle than
  // a file-level 0:0), or null when the node id is nowhere in the document.
  return fallback;
}
