/**
 * Diagnostics computation.
 *
 * Produces LSP-compatible Diagnostic objects from a parsed BPMN document.
 * Severity numbers match `DiagnosticSeverity`:
 *   1 = Error, 2 = Warning, 3 = Information, 4 = Hint
 *
 * Codes mirror `engine/spi/src/main/resources/diagnostics.yaml` so that IDE
 * warnings line up 1:1 with engine parse-time errors. The mapping table
 * lives in `qSchema.ts` (MISSING_REQUIRED_CODES, INVALID_ENUM_CODES).
 */

import { ParseResult, Range, parseBpmn } from './parser.js';
import { SymbolTable } from './symbols.js';
import { WorkspaceRegistry, knownIdsFor } from './registry.js';
import {
  Q_ELEMENT_BY_NAME,
  Q_ELEMENT_NAMES,
  MISSING_REQUIRED_CODES,
  INVALID_ENUM_CODES,
} from './qSchema.js';
import { computeStaticValidation } from './staticValidation.js';

export interface BpmDiagnostic {
  range: Range;
  severity: 1 | 2 | 3 | 4;
  code: string;
  source: string;
  message: string;
}

/** Maps a reference kind to the SUTRA.* code emitted on unknown id. */
const UNKNOWN_REF_CODES: Readonly<Record<string, string>> = {
  codec: 'SUTRA.RESOLVE.CODEC.UNKNOWN',
  validator: 'SUTRA.RESOLVE.VALIDATOR.UNKNOWN',
  channel: 'SUTRA.CHANNEL.TENANT.UNKNOWN',
  sink: 'SUTRA.AUDIT.SINK_NOT_FOUND',
};

export function computeDiagnostics(
  parsed: ParseResult,
  symbols: SymbolTable,
  registry: WorkspaceRegistry
): BpmDiagnostic[] {
  const diags: BpmDiagnostic[] = [];

  // 1. Parse errors → Errors
  for (const err of parsed.errors) {
    diags.push({
      range: err.range,
      severity: 1,
      code: 'SUTRA.PARSE.XSD.UNCLOSED_TAG',
      source: 'sutra',
      message: err.message,
    });
  }

  // 2. Unknown reference ids → Errors (sinks downgraded to Warning to match
  //    engine's SUTRA.AUDIT.SINK_NOT_FOUND severity)
  for (const ref of symbols.references) {
    if (ref.value.length === 0) continue;
    const valid = knownIdsFor(registry, ref.attribute);
    if (valid.size === 0) continue; // Skip when registry doesn't track this kind
    if (!valid.has(ref.value)) {
      const code = UNKNOWN_REF_CODES[ref.attribute] ?? `SUTRA.RESOLVE.${ref.attribute.toUpperCase()}.UNKNOWN`;
      const sev: 1 | 2 = ref.attribute === 'sink' ? 2 : 1;
      diags.push({
        range: ref.range,
        severity: sev,
        code,
        source: 'sutra',
        message: `Unknown ${ref.attribute} '${ref.value}'. Expected one of: ${[...valid].sort().join(', ')}.`,
      });
    }
  }

  // 3. Schema-driven per-element checks: missing required attrs, invalid
  //    enums, unknown attribute names. Walks the raw parser stream so we
  //    have ranges for every attribute slot — including the element name
  //    itself (used when reporting missing-required).
  for (const ev of parsed.events) {
    if (ev.kind !== 'open' && ev.kind !== 'self-closing') continue;
    if (!Q_ELEMENT_NAMES.has(ev.name)) continue;
    const spec = Q_ELEMENT_BY_NAME.get(ev.name)!;

    const seen = new Set<string>();
    for (const attr of ev.attributes) {
      seen.add(attr.name);
      const attrSpec = spec.attributes.find((a) => a.name === attr.name);
      if (!attrSpec) {
        // Unknown attribute name → Warning with typo hint
        const valid = spec.attributes.map((a) => a.name).sort();
        diags.push({
          range: attr.nameRange,
          severity: 2,
          code: `SUTRA.PARSE.QXSD.UNKNOWN_ATTRIBUTE`,
          source: 'sutra',
          message: `Unknown attribute '${attr.name}' on ${ev.name}. Valid attributes: ${valid.join(', ')}.`,
        });
        continue;
      }
      if (attrSpec.enumValues && attr.value.length > 0 && !attrSpec.enumValues.includes(attr.value)) {
        const code =
          INVALID_ENUM_CODES[ev.name]?.[attr.name] ??
          `SUTRA.PARSE.QXSD.INVALID_${attr.name.toUpperCase()}`;
        diags.push({
          range: attr.innerValueRange,
          severity: 1,
          code,
          source: 'sutra',
          message: `Invalid value '${attr.value}' for ${ev.name}/@${attr.name}. Expected one of: ${attrSpec.enumValues.join(', ')}.`,
        });
      }
    }

    // Required-attribute checks
    for (const attrSpec of spec.attributes) {
      if (!attrSpec.required) continue;
      if (seen.has(attrSpec.name)) continue;
      const code =
        MISSING_REQUIRED_CODES[ev.name]?.[attrSpec.name] ??
        `SUTRA.PARSE.QXSD.${ev.name.replace('q:', '').toUpperCase()}_MISSING_${attrSpec.name.toUpperCase()}`;
      diags.push({
        range: ev.nameRange,
        severity: 1,
        code,
        source: 'sutra',
        message: `${ev.name} is missing required attribute '${attrSpec.name}'.`,
      });
    }
  }

  // 4. Static structural validation reproduced from the deploy-time `sutra lint`
  //    pass (coverage path-extraction + variable-source intake), so in-editor
  //    lint matches deploy-time lint for the checks provable from one document.
  diags.push(...computeStaticValidation(parsed));

  return diags;
}
