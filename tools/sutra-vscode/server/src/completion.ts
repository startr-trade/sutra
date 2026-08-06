/**
 * Completion provider.
 *
 * Three modes:
 *   1. Inside `<bpmn:extensionElements>` — suggest the 9 schema-valid
 *      `<q:*>` elements with snippet bodies containing required attributes.
 *   2. Inside an open `<q:*` tag but before the first attribute — suggest
 *      the element's allowed attribute names (per `qSchema.ts`).
 *   3. Inside the value of a referencing attribute (`channel`, `codec`,
 *      `source` on `<q:validators>`, `sink`) — suggest known ids from the
 *      workspace registry. Also serves attribute-value completion for
 *      schema enums (e.g. `<q:reply mode=`).
 */

import { Position } from './parser.js';
import { WorkspaceRegistry, knownIdsFor } from './registry.js';
import { Q_ELEMENTS, Q_ELEMENT_BY_NAME, ElementSpec } from './qSchema.js';

export interface CompletionItem {
  label: string;
  kind: number; // matches LSP CompletionItemKind
  detail?: string;
  /** Optional snippet body for `InsertTextFormat = Snippet (2)`. */
  insertText?: string;
  insertTextFormat?: 1 | 2;
}

const KIND_VALUE = 12;
const KIND_ENUM = 13;
const KIND_SNIPPET = 15;
const KIND_PROPERTY = 10;

/** Maps a referencing attribute name to the registry pool name. */
const REGISTRY_ATTR_TO_KIND: Readonly<Record<string, string>> = {
  channel: 'channel',
  codec: 'codec',
  source: 'validator', // only fires on q:validators source=
  destination: 'channel',
  sink: 'sink',
};

export function computeCompletions(
  source: string,
  position: Position,
  registry: WorkspaceRegistry
): CompletionItem[] {
  const offset = positionToOffset(source, position);
  if (offset < 0) return [];

  // Look backwards for the nearest `<` so we know if we're inside a tag.
  const tagStart = lastUnquotedIndexOf(source, '<', offset);
  if (tagStart < 0) {
    return suggestElementsInExtensionElements(source, offset);
  }
  const tagEnd = nextTagEnd(source, tagStart);
  if (tagEnd >= 0 && tagEnd < offset) {
    // Cursor is past the most recent `>`; we're in element-content territory.
    return suggestElementsInExtensionElements(source, offset);
  }

  // Determine whether we're inside an attribute *value* (between quotes).
  const ctx = attributeContextAt(source, tagStart, offset);
  if (ctx) {
    return attributeValueCompletions(source, tagStart, ctx.attribute, registry);
  }

  // Otherwise we might be inside the tag but not in an attribute value —
  // suggest the element's allowed attribute names.
  return attributeNameCompletions(source, tagStart, offset);
}

function attributeValueCompletions(
  source: string,
  tagStart: number,
  attrName: string,
  registry: WorkspaceRegistry
): CompletionItem[] {
  const tagName = readTagName(source, tagStart);
  const spec = Q_ELEMENT_BY_NAME.get(tagName);

  // Enum values from the schema take priority over registry ids.
  if (spec) {
    const attrSpec = spec.attributes.find((a) => a.name === attrName);
    if (attrSpec?.enumValues) {
      return attrSpec.enumValues.map((v) => ({
        label: v,
        kind: KIND_ENUM,
        detail: `q:${spec.localName}/@${attrName}`,
      }));
    }
  }

  const kindName = REGISTRY_ATTR_TO_KIND[attrName];
  if (!kindName) return [];
  // For q:validators source=, we want validator ids — but `source` is a
  // generic attribute name. Only apply the validator pool when the tag is
  // q:validators.
  if (attrName === 'source' && tagName !== 'q:validators') return [];

  const pool = knownIdsFor(registry, kindName);
  return [...pool].sort().map((id) => ({
    label: id,
    kind: KIND_VALUE,
    detail: `${kindName} id`,
  }));
}

function attributeNameCompletions(
  source: string,
  tagStart: number,
  offset: number
): CompletionItem[] {
  const tagName = readTagName(source, tagStart);
  const spec = Q_ELEMENT_BY_NAME.get(tagName);
  if (!spec) return [];

  // Avoid suggesting attributes that are already present in the tag.
  const inner = source.slice(tagStart + 1 + tagName.length, offset);
  const present = new Set<string>();
  const re = /([a-zA-Z][\w-]*)\s*=/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(inner)) !== null) present.add(m[1]);

  return spec.attributes
    .filter((a) => !present.has(a.name))
    .map((a) => ({
      label: a.name,
      kind: KIND_PROPERTY,
      detail: `q:${spec.localName}/@${a.name}${a.required ? ' (required)' : ''}`,
      insertText: `${a.name}="$1"`,
      insertTextFormat: 2,
    }));
}

function suggestElementsInExtensionElements(source: string, offset: number): CompletionItem[] {
  // Cheap heuristic: look backwards for the nearest `<bpmn:extensionElements`
  // opener without a matching closer. If we find one, we're inside it.
  const opener = source.lastIndexOf('<bpmn:extensionElements', offset);
  if (opener < 0) return [];
  const closer = source.lastIndexOf('</bpmn:extensionElements', offset);
  if (closer > opener) return [];

  return Q_ELEMENTS.map((e) => elementSnippet(e));
}

function elementSnippet(spec: ElementSpec): CompletionItem {
  const required = spec.attributes.filter((a) => a.required);
  let tabIndex = 1;
  const attrSnippets = required
    .map((a) => `${a.name}="\${${tabIndex++}}"`)
    .join(' ');
  const body = required.length === 0
    ? `<q:${spec.localName}/>`
    : `<q:${spec.localName} ${attrSnippets}/>`;
  return {
    label: `q:${spec.localName}`,
    kind: KIND_SNIPPET,
    detail: spec.doc.split('. ')[0],
    insertText: body,
    insertTextFormat: 2,
  };
}

function readTagName(source: string, tagStart: number): string {
  let i = tagStart + 1;
  while (i < source.length && !/[\s/>]/.test(source[i])) i++;
  return source.slice(tagStart + 1, i);
}

function positionToOffset(source: string, position: Position): number {
  let line = 0;
  let i = 0;
  while (i < source.length && line < position.line) {
    if (source[i] === '\n') line++;
    i++;
  }
  return i + position.character;
}

function lastUnquotedIndexOf(source: string, target: string, before: number): number {
  let inSingle = false;
  let inDouble = false;
  let last = -1;
  for (let i = 0; i < before; i++) {
    const c = source[i];
    if (c === '"' && !inSingle) inDouble = !inDouble;
    else if (c === "'" && !inDouble) inSingle = !inSingle;
    else if (c === target && !inSingle && !inDouble) last = i;
  }
  return last;
}

function nextTagEnd(source: string, from: number): number {
  let inSingle = false;
  let inDouble = false;
  for (let i = from + 1; i < source.length; i++) {
    const c = source[i];
    if (c === '"' && !inSingle) inDouble = !inDouble;
    else if (c === "'" && !inDouble) inSingle = !inSingle;
    else if (c === '>' && !inSingle && !inDouble) return i;
  }
  return -1;
}

interface AttributeContext {
  attribute: string;
}

function attributeContextAt(
  source: string,
  tagStart: number,
  offset: number
): AttributeContext | null {
  let i = tagStart + 1;
  while (i < offset && !/\s/.test(source[i]) && source[i] !== '>' && source[i] !== '/') i++;

  while (i < offset) {
    while (i < offset && /\s/.test(source[i])) i++;
    if (i >= offset) return null;

    const nameStart = i;
    while (i < offset && !/[\s=]/.test(source[i]) && source[i] !== '>' && source[i] !== '/') i++;
    const attrName = source.slice(nameStart, i);

    while (i < offset && /\s/.test(source[i])) i++;
    if (source[i] !== '=') continue;
    i++;
    while (i < offset && /\s/.test(source[i])) i++;

    const quote = source[i];
    if (quote !== '"' && quote !== "'") return null;
    const valueStart = i + 1;
    let endQuote = -1;
    for (let j = valueStart; j < source.length; j++) {
      if (source[j] === quote) {
        endQuote = j;
        break;
      }
    }
    if (endQuote < 0) {
      if (offset >= valueStart) return { attribute: attrName };
      return null;
    }
    if (offset >= valueStart && offset <= endQuote) return { attribute: attrName };
    i = endQuote + 1;
  }
  return null;
}
