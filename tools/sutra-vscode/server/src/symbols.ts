/**
 * Symbol table extraction for a BPMN document.
 *
 * Walks the parser event stream and gathers:
 *   - Every `<q:*>` element from xsd/q.xsd — `source`, `input`, `validators`,
 *     `reply`, `alias`, `dispatch`, `case`, `onValidation`, `audit`.
 *   - FEEL expression text + location for every FEEL-bearing attribute the
 *     schema declares (`<q:alias expression>`, `<q:case when>`,
 *     `<q:validators when>` — see qSchema.ts for the canonical list).
 *   - Cross-references on attributes that name an external id —
 *     `<q:source channel>`, `<q:input codec>`, `<q:validators source>`,
 *     `<q:case calledElement>`, `<q:dispatch default>`, `<q:audit sink>`.
 *
 * The five "orphan" tags (`q:expression`, `q:condition`, `q:guard`,
 * `q:field`, `q:schema`) that an earlier exploratory pass tracked are
 * intentionally NOT recognised here — they are not part of `xsd/q.xsd`.
 * Regression-guard tests in `__tests__/parse.test.ts` keep it that way.
 */

import {
  ParseResult,
  ParserEvent,
  Range,
  AttributeNode,
  ElementOpen,
  parseBpmn,
} from './parser.js';
import { Q_ELEMENT_BY_NAME, Q_ELEMENT_NAMES } from './qSchema.js';

export interface QSymbol {
  /** Element id — either the `id` attribute or, for elements without one,
   *  a synthesised key derived from the primary attribute. */
  id: string;
  /** Tag name without prefix. e.g. `source`, `input`, `case`. */
  kind: string;
  /** Range of the element id attribute value (inner, no quotes). */
  idRange: Range;
  /** Range of the entire opening tag. */
  range: Range;
  /** Raw attribute map for the element. */
  attributes: Record<string, string>;
  /** Inner value ranges for attributes, keyed by attribute name. */
  attributeRanges: Record<string, Range>;
}

export interface FeelExpression {
  /** Surrounding tag name, e.g. `q:alias`. */
  tag: string;
  /** Attribute name carrying the FEEL expression, e.g. `expression` / `when`. */
  attribute: string;
  /** Range of the inner attribute value (no quotes). */
  range: Range;
  /** Raw FEEL text. */
  text: string;
  /** Absolute offset where the FEEL text begins in the source. */
  textOffset: number;
}

export interface AttrReference {
  /** Element kind referencing another id, e.g. `q:source`. */
  fromTag: string;
  /** Attribute name (`codec`, `channel`, `source`, `calledElement`, `sink`). */
  attribute: string;
  /** Referenced id. */
  value: string;
  /** Range of the inner attribute value (no quotes). */
  range: Range;
}

export interface SymbolTable {
  symbols: QSymbol[];
  byId: Map<string, QSymbol>;
  feel: FeelExpression[];
  references: AttrReference[];
}

/**
 * Maps each `q:*` element + its referencing attribute to a logical reference
 * kind. The `kind` string is what hover / diagnostics / completion look up
 * against the workspace registry — so e.g. `<q:input codec>` and the (legacy)
 * `<q:source codec>` both surface as kind `codec`.
 */
const REFERENCE_KINDS: ReadonlyArray<{ tag: string; attr: string; kind: string }> = [
  { tag: 'q:source', attr: 'channel', kind: 'channel' },
  { tag: 'q:input', attr: 'codec', kind: 'codec' },
  { tag: 'q:validators', attr: 'source', kind: 'validator' },
  { tag: 'q:case', attr: 'calledElement', kind: 'calledElement' },
  { tag: 'q:dispatch', attr: 'default', kind: 'calledElement' },
  { tag: 'q:audit', attr: 'sink', kind: 'sink' },
  { tag: 'q:reply', attr: 'destination', kind: 'channel' },
];

const REFERENCE_KEY_INDEX: ReadonlyMap<string, string> = new Map(
  REFERENCE_KINDS.map((r) => [`${r.tag}::${r.attr}`, r.kind])
);

export function buildSymbolTable(source: string, parsed?: ParseResult): SymbolTable {
  const result = parsed ?? parseBpmn(source);
  const events = result.events;

  const symbols: QSymbol[] = [];
  const byId = new Map<string, QSymbol>();
  const feel: FeelExpression[] = [];
  const references: AttrReference[] = [];

  for (let i = 0; i < events.length; i++) {
    const ev = events[i];
    if (ev.kind !== 'open' && ev.kind !== 'self-closing') continue;
    if (!Q_ELEMENT_NAMES.has(ev.name)) continue;

    const sym = toSymbol(ev);
    symbols.push(sym);
    if (sym.id) byId.set(sym.id, sym);

    // Cross-reference collection
    for (const attr of ev.attributes) {
      const refKind = REFERENCE_KEY_INDEX.get(`${ev.name}::${attr.name}`);
      if (refKind && attr.value.length > 0) {
        references.push({
          fromTag: ev.name,
          attribute: refKind,
          value: attr.value,
          range: attr.innerValueRange,
        });
      }
    }

    // FEEL collection — driven entirely by the schema's `feel: true` flags
    const spec = Q_ELEMENT_BY_NAME.get(ev.name);
    if (spec) {
      for (const attrSpec of spec.attributes) {
        if (!attrSpec.feel) continue;
        const attr = ev.attributes.find((a) => a.name === attrSpec.name);
        if (!attr || attr.value.length === 0) continue;
        feel.push({
          tag: ev.name,
          attribute: attr.name,
          range: attr.innerValueRange,
          text: attr.value,
          textOffset: offsetForPosition(source, attr.innerValueRange.start),
        });
      }
    }
  }

  return { symbols, byId, feel, references };
}

function toSymbol(ev: ElementOpen): QSymbol {
  const attrs: Record<string, string> = {};
  const attrRanges: Record<string, Range> = {};
  let idAttr: AttributeNode | undefined;
  let primaryAttr: AttributeNode | undefined;
  for (const a of ev.attributes) {
    attrs[a.name] = a.value;
    attrRanges[a.name] = a.innerValueRange;
    if (a.name === 'id') idAttr = a;
  }
  // For schema elements that don't carry an `id`, prefer the most-identifying
  // attribute so that go-to-definition still has a stable target range.
  if (!idAttr) {
    const localName = ev.name.replace(/^q:/, '');
    const preferred: Record<string, string[]> = {
      source: ['channel'],
      input: ['codec', 'name'],
      validators: ['source'],
      reply: ['destination'],
      alias: ['name'],
      dispatch: ['default'],
      case: ['calledElement'],
      onValidation: ['mode'],
      audit: ['sink'],
    };
    for (const attrName of preferred[localName] ?? []) {
      const a = ev.attributes.find((x) => x.name === attrName);
      if (a) {
        primaryAttr = a;
        break;
      }
    }
  }
  const idSource = idAttr ?? primaryAttr;
  return {
    id: idSource?.value ?? '',
    kind: ev.name.replace(/^q:/, ''),
    idRange: idSource?.innerValueRange ?? ev.nameRange,
    range: ev.range,
    attributes: attrs,
    attributeRanges: attrRanges,
  };
}

function offsetForPosition(source: string, position: { line: number; character: number }): number {
  let offset = 0;
  let line = 0;
  while (line < position.line && offset < source.length) {
    if (source[offset] === '\n') line++;
    offset++;
  }
  return offset + position.character;
}
