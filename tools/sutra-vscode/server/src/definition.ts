/**
 * Go-to-definition provider.
 *
 * Three jump targets — all driven by attribute references on schema-valid
 * `<q:*>` elements (per `xsd/q.xsd` / `qSchema.ts`):
 *
 *   1. `<q:source channel="…">` — jump to the channel-binding declaration in
 *      `tenant-configuration.yaml`. We do not actually resolve the YAML path
 *      from the BPMN file alone, so the jump returns the `<q:source>` element
 *      itself as a placeholder when the channel is known to the workspace
 *      registry (which DOES scan module.yml). Real cross-file jump is a
 *      follow-up.
 *
 *   2. `<q:case calledElement="…">` and `<q:dispatch default="…">` — jump to
 *      the `<bpmn:process id="…">` element referenced by `calledElement`,
 *      either in this document or (future) across imports.
 *
 *   3. `<q:validators source="…">` — jump to the validator-extension or DRL
 *      file under `<resources>/rules/`. Cross-file resolution requires
 *      workspace scanning; we surface the current-document target only.
 *
 * For each kind the implementation returns the location of the *defining*
 * `<bpmn:process>` (case 2) when we can find one in the same file; otherwise
 * returns an empty result list and the LSP host treats it as "no definition".
 */

import { Position, Range, parseBpmn } from './parser.js';
import { SymbolTable } from './symbols.js';
import { WorkspaceRegistry, knownIdsFor } from './registry.js';

export interface DefinitionLink {
  /** Range covering the matched attribute value in the source. */
  originSelectionRange: Range;
  /** Range covering the element being targeted. */
  targetRange: Range;
  /** Range covering just the target id attribute value. */
  targetSelectionRange: Range;
}

interface ProcessLocation {
  id: string;
  range: Range;
  idRange: Range;
}

export function resolveDefinition(
  source: string,
  position: Position,
  symbols: SymbolTable,
  registry?: WorkspaceRegistry
): DefinitionLink[] {
  // 1) cursor on an attribute value carrying a reference?
  for (const ref of symbols.references) {
    if (!positionInRange(position, ref.range)) continue;

    if (ref.attribute === 'calledElement') {
      const processes = findProcessIds(source);
      const target = processes.find((p) => p.id === ref.value);
      if (target) {
        return [
          {
            originSelectionRange: ref.range,
            targetRange: target.range,
            targetSelectionRange: target.idRange,
          },
        ];
      }
      return [];
    }

    if (ref.attribute === 'channel' || ref.attribute === 'validator' || ref.attribute === 'sink') {
      // Known to the workspace registry? We can't open the external file from
      // here without a workspace-resolved path, but we acknowledge the jump by
      // returning the originating `<q:*>` element so the client renders
      // "Peek Definition" of the binding in-document. Cross-file jump is a
      // follow-up.
      const known = registry ? knownIdsFor(registry, ref.attribute) : new Set<string>();
      if (registry && !known.has(ref.value)) return [];
      // Find the q:* symbol owning this reference range so we can use its
      // element range as the target.
      for (const sym of symbols.symbols) {
        if (positionInRange(position, sym.range)) {
          return [
            {
              originSelectionRange: ref.range,
              targetRange: sym.range,
              targetSelectionRange: ref.range,
            },
          ];
        }
      }
    }
  }

  return [];
}

function findProcessIds(source: string): ProcessLocation[] {
  const out: ProcessLocation[] = [];
  const parsed = parseBpmn(source);
  for (const ev of parsed.events) {
    if (ev.kind !== 'open' && ev.kind !== 'self-closing') continue;
    if (!isLocalName(ev.name, 'process')) continue;
    const idAttr = ev.attributes.find((a) => a.name === 'id');
    if (!idAttr) continue;
    out.push({ id: idAttr.value, range: ev.range, idRange: idAttr.innerValueRange });
  }
  return out;
}

function isLocalName(tag: string, localName: string): boolean {
  const idx = tag.indexOf(':');
  const local = idx < 0 ? tag : tag.slice(idx + 1);
  return local === localName;
}

function positionInRange(pos: Position, range: Range): boolean {
  if (pos.line < range.start.line || pos.line > range.end.line) return false;
  if (pos.line === range.start.line && pos.character < range.start.character) return false;
  if (pos.line === range.end.line && pos.character > range.end.character) return false;
  return true;
}
