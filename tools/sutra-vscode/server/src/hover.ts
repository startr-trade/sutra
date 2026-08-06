/**
 * Hover provider.
 *
 * Driven by `qSchema.ts`. Three hover targets:
 *
 *   1. Hovering an attribute value that names a registry entity
 *      (channel / codec / validator / sink) reports known-or-UNKNOWN status
 *      plus the attribute's XSD documentation.
 *   2. Hovering an attribute *name* shows the attribute's XSD documentation
 *      and required-flag.
 *   3. Hovering the element tag name shows the element's XSD documentation.
 */

import { Position, Range, parseBpmn } from './parser.js';
import { SymbolTable } from './symbols.js';
import { WorkspaceRegistry, knownIdsFor } from './registry.js';
import { Q_ELEMENT_BY_NAME } from './qSchema.js';

export interface HoverResult {
  contents: string;
  range?: Range;
}

export function computeHover(
  source: string,
  position: Position,
  symbols: SymbolTable,
  registry: WorkspaceRegistry
): HoverResult | null {
  // 1. Attribute-value reference?
  for (const ref of symbols.references) {
    if (!positionInRange(position, ref.range)) continue;
    const known = knownIdsFor(registry, ref.attribute);
    const status = known.has(ref.value) ? 'known' : 'UNKNOWN';
    return {
      contents: `**${ref.attribute}** \`${ref.value}\` (${status})`,
      range: ref.range,
    };
  }

  // 2. Look up the element under the cursor via the raw parser stream so we
  //    can distinguish "on the tag name" from "on an attribute name" from
  //    "on an attribute value not on our reference list".
  const parsed = parseBpmn(source);
  for (const ev of parsed.events) {
    if (ev.kind !== 'open' && ev.kind !== 'self-closing') continue;
    const spec = Q_ELEMENT_BY_NAME.get(ev.name);
    if (!spec) continue;

    // 2a. Attribute name hover.
    for (const attr of ev.attributes) {
      if (positionInRange(position, attr.nameRange)) {
        const attrSpec = spec.attributes.find((a) => a.name === attr.name);
        const required = attrSpec?.required ? ' _(required)_' : '';
        const doc = attrSpec?.doc ?? 'Unknown attribute for this element.';
        return {
          contents: `**${ev.name}/@${attr.name}**${required}\n\n${doc}`,
          range: attr.nameRange,
        };
      }
    }

    // 2b. Tag-name hover.
    if (positionInRange(position, ev.nameRange)) {
      return {
        contents: `**${ev.name}**\n\n${spec.doc}`,
        range: ev.nameRange,
      };
    }
  }

  // 3. Fall back to a generic "symbol at position" hover for symbols that
  //    don't fit the schema map (preserved for back-compat with callers).
  for (const sym of symbols.symbols) {
    if (positionInRange(position, sym.range)) {
      return {
        contents: `**q:${sym.kind}** \`${sym.id || '(no id)'}\``,
        range: sym.range,
      };
    }
  }

  return null;
}

function positionInRange(pos: Position, range: Range): boolean {
  if (pos.line < range.start.line || pos.line > range.end.line) return false;
  if (pos.line === range.start.line && pos.character < range.start.character) return false;
  if (pos.line === range.end.line && pos.character > range.end.character) return false;
  return true;
}
