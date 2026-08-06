/**
 * Static structural validation — the in-editor mirror of the deploy-time
 * `sutra lint` checks that are provable from a SINGLE open `.bpmn` document
 * (no engine process, no archive, no codec/XSD introspection required).
 *
 * Parity target: the Rust loader path that `sutra lint` / `sutra package` run.
 *   - `<q:coverage>` path/flow extraction — `sutra-bpmn::loader::validate_coverage_paths`
 *     (`SUTRA.CONFIG.COVERAGE.{DUPLICATE_PATH,INVALID_ROUTE,UNKNOWN_FLOW}`).
 *   - `<q:variable source="…">` intake-subscription — `validate_variable_sources`
 *     (`SUTRA.CONFIG.BPMN.VARIABLE_SOURCE_UNKNOWN`).
 *
 * Codes, severities and message text mirror the Rust source so that in-editor
 * lint lines up 1:1 with deploy-time lint. Deliberate divergence from the Rust
 * pass: the loader is fail-CLOSED and returns the FIRST error; the editor is
 * advisory and collects EVERY diagnostic so the author sees all issues at once.
 * Codes/severity/message per issue are identical.
 *
 * These checks are all deploy-blocking ERRORs in the Rust source (the WARN-level
 * lint diagnostics — schema-less channels, unverifiable fields, etc. — all need
 * cross-file context: channels.yaml, datastores.yaml, schemas/*.xsd, or the
 * referenced templates, and are out of scope for single-document static lint).
 */

import type { ParseResult, ElementOpen, AttributeNode, Range } from './parser.js';
import type { BpmDiagnostic } from './diagnostics.js';

/** `SUTRA.*` codes, mirroring `sutra-bpmn::codes` / `sutra-loader::error::codes`. */
const COVERAGE_DUPLICATE_PATH = 'SUTRA.CONFIG.COVERAGE.DUPLICATE_PATH';
const COVERAGE_INVALID_ROUTE = 'SUTRA.CONFIG.COVERAGE.INVALID_ROUTE';
const COVERAGE_UNKNOWN_FLOW = 'SUTRA.CONFIG.COVERAGE.UNKNOWN_FLOW';
const VARIABLE_SOURCE_UNKNOWN = 'SUTRA.CONFIG.BPMN.VARIABLE_SOURCE_UNKNOWN';

interface FlowDef {
  id: string;
  sourceRef: string;
  targetRef: string;
}

interface CoverageDecl {
  pathId: string;
  flows: string[];
  el: ElementOpen;
  pathAttr?: AttributeNode;
  flowsAttr?: AttributeNode;
}

interface VariableDecl {
  name: string;
  source: string;
  sourceAttr: AttributeNode;
}

/** Everything collected within one `<bpmn:process>` element. */
interface ProcessScope {
  id: string;
  headerRange: Range;
  flows: FlowDef[];
  coverage: CoverageDecl[];
  variables: VariableDecl[];
  /** `<q:source channel="…">` intake channels the process subscribes to. */
  sourceChannels: Set<string>;
}

/** Local name for an element, dropping any namespace prefix (`bpmn:process` → `process`). */
function localName(name: string): string {
  const idx = name.indexOf(':');
  return idx >= 0 ? name.slice(idx + 1) : name;
}

/** Local name of a `q:`-prefixed element, or `null` when it is not in the q namespace. */
function qLocal(name: string): string | null {
  return name.startsWith('q:') ? name.slice(2) : null;
}

function attrOf(el: ElementOpen, name: string): AttributeNode | undefined {
  return el.attributes.find((a) => a.name === name);
}

function err(code: string, range: Range, message: string): BpmDiagnostic {
  return { range, severity: 1, code, source: 'sutra', message };
}

/**
 * Compute the sub-range covering one whitespace-delimited token inside an
 * attribute value (so an unknown-flow squiggle underlines just that flow id,
 * not the whole `flows` list). Falls back to `undefined` for multi-line values
 * or when the token is not present as a whole word.
 */
function flowTokenRange(attr: AttributeNode | undefined, token: string): Range | undefined {
  if (!attr) return undefined;
  const { start, end } = attr.innerValueRange;
  if (start.line !== end.line) return undefined;
  const value = attr.value;
  let searchFrom = 0;
  while (searchFrom <= value.length) {
    const idx = value.indexOf(token, searchFrom);
    if (idx < 0) return undefined;
    const before = idx === 0 || /\s/.test(value[idx - 1]);
    const afterIdx = idx + token.length;
    const after = afterIdx === value.length || /\s/.test(value[afterIdx]);
    if (before && after) {
      return {
        start: { line: start.line, character: start.character + idx },
        end: { line: start.line, character: start.character + afterIdx },
      };
    }
    searchFrom = idx + 1;
  }
  return undefined;
}

/**
 * Walk the parser event stream, group flows / coverage / variables / intake
 * channels per `<bpmn:process>`, then run the per-process validators. Top-level
 * processes do not nest in BPMN, so a single "current scope" tracker suffices;
 * nested sub-process flows are folded into the enclosing process's flow set (a
 * conservative superset that only ever suppresses a false UNKNOWN_FLOW).
 */
export function computeStaticValidation(parsed: ParseResult): BpmDiagnostic[] {
  const diags: BpmDiagnostic[] = [];
  let current: ProcessScope | null = null;

  for (const ev of parsed.events) {
    if (ev.kind === 'close') {
      if (localName(ev.name) === 'process' && current) {
        validateScope(current, diags);
        current = null;
      }
      continue;
    }
    if (ev.kind !== 'open' && ev.kind !== 'self-closing') continue;

    const ln = localName(ev.name);
    if (ln === 'process' && ev.kind === 'open') {
      if (current) validateScope(current, diags); // defensive: unclosed prior process
      current = {
        id: attrOf(ev, 'id')?.value ?? '',
        headerRange: ev.nameRange,
        flows: [],
        coverage: [],
        variables: [],
        sourceChannels: new Set(),
      };
      continue;
    }
    if (!current) continue;

    if (ln === 'sequenceFlow') {
      current.flows.push({
        id: attrOf(ev, 'id')?.value ?? '',
        sourceRef: attrOf(ev, 'sourceRef')?.value ?? '',
        targetRef: attrOf(ev, 'targetRef')?.value ?? '',
      });
      continue;
    }

    const q = qLocal(ev.name);
    if (q === 'coverage') {
      const pathAttr = attrOf(ev, 'path');
      const pathId = (pathAttr?.value ?? '').trim();
      if (pathId.length === 0) continue; // loader skips a blank `path`
      const flowsAttr = attrOf(ev, 'flows');
      const flows = (flowsAttr?.value ?? '').split(/\s+/).filter((s) => s.length > 0);
      current.coverage.push({ pathId, flows, el: ev, pathAttr, flowsAttr });
    } else if (q === 'variable') {
      const source = (attrOf(ev, 'source')?.value ?? '').trim();
      const sourceAttr = attrOf(ev, 'source');
      if (source.length > 0 && sourceAttr) {
        current.variables.push({
          name: (attrOf(ev, 'name')?.value ?? '').trim(),
          source,
          sourceAttr,
        });
      }
    } else if (q === 'source') {
      const channel = (attrOf(ev, 'channel')?.value ?? '').trim();
      if (channel.length > 0) current.sourceChannels.add(channel);
    }
  }
  if (current) validateScope(current, diags);
  return diags;
}

function validateScope(scope: ProcessScope, diags: BpmDiagnostic[]): void {
  validateCoveragePaths(scope, diags);
  validateVariableSources(scope, diags);
}

/**
 * Mirror of `sutra-bpmn::loader::validate_coverage_paths`: every `<q:coverage>`
 * path is a unique id whose flows are declared sequence flows forming one
 * contiguous route (each flow's `targetRef` is the next flow's `sourceRef`).
 */
function validateCoveragePaths(scope: ProcessScope, diags: BpmDiagnostic[]): void {
  if (scope.coverage.length === 0) return;
  const byId = new Map<string, FlowDef>();
  for (const f of scope.flows) if (f.id.length > 0) byId.set(f.id, f);

  const seen = new Set<string>();
  for (const cov of scope.coverage) {
    const pathAnchor = cov.pathAttr?.innerValueRange ?? cov.el.nameRange;
    const flowsAnchor = cov.flowsAttr?.innerValueRange ?? cov.el.nameRange;

    if (seen.has(cov.pathId)) {
      diags.push(
        err(
          COVERAGE_DUPLICATE_PATH,
          pathAnchor,
          `<q:coverage path="${cov.pathId}"> is declared more than once in process '${scope.id}'.`
        )
      );
      continue;
    }
    seen.add(cov.pathId);

    if (cov.flows.length === 0) {
      diags.push(
        err(
          COVERAGE_INVALID_ROUTE,
          flowsAnchor,
          `<q:coverage path="${cov.pathId}"> in process '${scope.id}' lists no flows.`
        )
      );
      continue;
    }

    let anyUnknown = false;
    for (const fid of cov.flows) {
      if (!byId.has(fid)) {
        anyUnknown = true;
        diags.push(
          err(
            COVERAGE_UNKNOWN_FLOW,
            flowTokenRange(cov.flowsAttr, fid) ?? flowsAnchor,
            `<q:coverage path="${cov.pathId}"> in process '${scope.id}' references flow '${fid}', which is not a <bpmn:sequenceFlow> in the process.`
          )
        );
      }
    }
    if (anyUnknown) continue; // contiguity needs all flows resolvable

    for (let i = 0; i + 1 < cov.flows.length; i++) {
      const a = byId.get(cov.flows[i])!;
      const b = byId.get(cov.flows[i + 1])!;
      if (a.targetRef !== b.sourceRef) {
        diags.push(
          err(
            COVERAGE_INVALID_ROUTE,
            flowsAnchor,
            `<q:coverage path="${cov.pathId}"> in process '${scope.id}' is not a contiguous route: flow '${a.id}' ends at '${a.targetRef}' but the next flow '${b.id}' starts at '${b.sourceRef}'.`
          )
        );
        break; // one contiguity error per path (the loader stops at the first break)
      }
    }
  }
}

/**
 * Mirror of `sutra-bpmn::loader::validate_variable_sources`: a
 * `<q:variable source="channel">` must feed off a channel the process actually
 * subscribes to via some `<q:source channel="channel">` — otherwise the variable
 * could never be initialized.
 */
function validateVariableSources(scope: ProcessScope, diags: BpmDiagnostic[]): void {
  for (const v of scope.variables) {
    if (scope.sourceChannels.has(v.source)) continue;
    diags.push(
      err(
        VARIABLE_SOURCE_UNKNOWN,
        v.sourceAttr.innerValueRange,
        `<q:variable name="${v.name}" source="${v.source}"> in process '${scope.id}' feeds off channel '${v.source}', but no intake node in the process subscribes to it (no <q:source channel="${v.source}">) — the variable could never be initialized. Bind it to a channel a start event / message catch / userTask consumes, or drop @source if it is in-instance state.`
      )
    );
  }
}
