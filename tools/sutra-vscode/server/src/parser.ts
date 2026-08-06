/**
 * Lightweight StAX-style walker for BPMN documents with q:* extensions.
 *
 * We do not aim to fully parse XML — only to locate elements/attributes
 * whose positions we need for symbol-table construction, diagnostics,
 * go-to-definition and completion.
 *
 * Every reported range uses zero-based line/character offsets so that
 * results can be lifted directly into LSP `Range` values.
 */

export interface Position {
  line: number;
  character: number;
}

export interface Range {
  start: Position;
  end: Position;
}

export interface AttributeNode {
  name: string;
  value: string;
  /** Range covering the attribute name. */
  nameRange: Range;
  /** Range covering the attribute value, including quotes. */
  valueRange: Range;
  /** Range covering just the inner attribute value, excluding quotes. */
  innerValueRange: Range;
}

export interface ElementOpen {
  kind: 'open' | 'self-closing';
  name: string;
  attributes: AttributeNode[];
  /** Range over the start tag (`<…>` or `<…/>`). */
  range: Range;
  /** Range over just the tag name. */
  nameRange: Range;
}

export interface ElementClose {
  kind: 'close';
  name: string;
  range: Range;
}

export interface TextNode {
  kind: 'text';
  text: string;
  range: Range;
}

export type ParserEvent = ElementOpen | ElementClose | TextNode;

/** Result of `parseBpmn`. */
export interface ParseResult {
  events: ParserEvent[];
  errors: ParseError[];
}

export interface ParseError {
  message: string;
  range: Range;
}

/**
 * Walks the source string and emits StAX-style events. Tolerant of
 * malformed XML — bad tags are reported as errors but parsing continues.
 */
export function parseBpmn(source: string): ParseResult {
  const events: ParserEvent[] = [];
  const errors: ParseError[] = [];
  const lineStarts = computeLineStarts(source);

  let i = 0;
  while (i < source.length) {
    const ch = source[i];

    if (ch === '<') {
      // Comments
      if (source.startsWith('<!--', i)) {
        const end = source.indexOf('-->', i + 4);
        if (end < 0) {
          errors.push({
            message: 'Unterminated comment',
            range: rangeOf(i, source.length, lineStarts),
          });
          i = source.length;
        } else {
          i = end + 3;
        }
        continue;
      }

      // CDATA — treat as opaque text
      if (source.startsWith('<![CDATA[', i)) {
        const end = source.indexOf(']]>', i + 9);
        const closeAt = end < 0 ? source.length : end + 3;
        const text = source.slice(i + 9, end < 0 ? source.length : end);
        events.push({
          kind: 'text',
          text,
          range: rangeOf(i, closeAt, lineStarts),
        });
        if (end < 0) {
          errors.push({
            message: 'Unterminated CDATA section',
            range: rangeOf(i, source.length, lineStarts),
          });
        }
        i = closeAt;
        continue;
      }

      // Processing instruction / XML declaration / doctype — skip
      if (source.startsWith('<?', i) || source.startsWith('<!', i)) {
        const end = source.indexOf('>', i);
        i = end < 0 ? source.length : end + 1;
        continue;
      }

      // Closing tag
      if (source[i + 1] === '/') {
        const end = source.indexOf('>', i);
        if (end < 0) {
          errors.push({
            message: 'Unterminated closing tag',
            range: rangeOf(i, source.length, lineStarts),
          });
          i = source.length;
          continue;
        }
        const name = source.slice(i + 2, end).trim();
        events.push({
          kind: 'close',
          name,
          range: rangeOf(i, end + 1, lineStarts),
        });
        i = end + 1;
        continue;
      }

      // Opening (or self-closing) tag
      const tagEnd = findTagEnd(source, i);
      if (tagEnd < 0) {
        errors.push({
          message: 'Unterminated tag',
          range: rangeOf(i, source.length, lineStarts),
        });
        i = source.length;
        continue;
      }
      const selfClosing = source[tagEnd - 1] === '/';
      const open = parseOpenTag(source, i, tagEnd, selfClosing, lineStarts, errors);
      events.push(open);
      i = tagEnd + 1;
      continue;
    }

    // Text node
    const next = source.indexOf('<', i);
    const end = next < 0 ? source.length : next;
    const text = source.slice(i, end);
    if (text.length > 0) {
      events.push({
        kind: 'text',
        text,
        range: rangeOf(i, end, lineStarts),
      });
    }
    i = end;
  }

  return { events, errors };
}

function findTagEnd(source: string, start: number): number {
  let i = start + 1;
  let inSingle = false;
  let inDouble = false;
  while (i < source.length) {
    const ch = source[i];
    if (ch === '"' && !inSingle) {
      inDouble = !inDouble;
    } else if (ch === "'" && !inDouble) {
      inSingle = !inSingle;
    } else if (ch === '>' && !inSingle && !inDouble) {
      return i;
    }
    i++;
  }
  return -1;
}

function parseOpenTag(
  source: string,
  start: number,
  end: number,
  selfClosing: boolean,
  lineStarts: number[],
  errors: ParseError[]
): ElementOpen {
  const inner = source.slice(start + 1, selfClosing ? end - 1 : end);
  // Tag name: read until whitespace or end
  let p = 0;
  while (p < inner.length && !/\s/.test(inner[p])) p++;
  const name = inner.slice(0, p);
  const nameAbsStart = start + 1;
  const nameAbsEnd = nameAbsStart + name.length;

  const attributes: AttributeNode[] = [];

  while (p < inner.length) {
    // Skip whitespace
    while (p < inner.length && /\s/.test(inner[p])) p++;
    if (p >= inner.length) break;

    const attrNameStart = p;
    while (p < inner.length && !/[\s=]/.test(inner[p])) p++;
    const attrName = inner.slice(attrNameStart, p);
    if (attrName.length === 0) break;
    const attrNameAbsStart = start + 1 + attrNameStart;
    const attrNameAbsEnd = start + 1 + p;

    // Skip whitespace and `=`
    while (p < inner.length && /\s/.test(inner[p])) p++;
    if (inner[p] !== '=') {
      errors.push({
        message: `Attribute '${attrName}' missing value`,
        range: rangeOf(attrNameAbsStart, attrNameAbsEnd, lineStarts),
      });
      continue;
    }
    p++;
    while (p < inner.length && /\s/.test(inner[p])) p++;

    const quote = inner[p];
    if (quote !== '"' && quote !== "'") {
      errors.push({
        message: `Attribute '${attrName}' value must be quoted`,
        range: rangeOf(attrNameAbsStart, attrNameAbsEnd, lineStarts),
      });
      continue;
    }
    const valueStart = p; // includes quote
    const innerValueStart = p + 1;
    p++;
    while (p < inner.length && inner[p] !== quote) p++;
    const innerValueEnd = p;
    const valueEnd = p + 1; // include closing quote
    if (p >= inner.length) {
      errors.push({
        message: `Attribute '${attrName}' value is not terminated`,
        range: rangeOf(start + 1 + valueStart, start + 1 + inner.length, lineStarts),
      });
      p = inner.length;
    } else {
      p++; // step past closing quote
    }

    const value = inner.slice(innerValueStart, innerValueEnd);
    attributes.push({
      name: attrName,
      value,
      nameRange: rangeOf(attrNameAbsStart, attrNameAbsEnd, lineStarts),
      valueRange: rangeOf(start + 1 + valueStart, start + 1 + valueEnd, lineStarts),
      innerValueRange: rangeOf(
        start + 1 + innerValueStart,
        start + 1 + innerValueEnd,
        lineStarts
      ),
    });
  }

  return {
    kind: selfClosing ? 'self-closing' : 'open',
    name,
    attributes,
    range: rangeOf(start, end + 1, lineStarts),
    nameRange: rangeOf(nameAbsStart, nameAbsEnd, lineStarts),
  };
}

function computeLineStarts(source: string): number[] {
  const starts: number[] = [0];
  for (let i = 0; i < source.length; i++) {
    if (source[i] === '\n') starts.push(i + 1);
  }
  return starts;
}

export function offsetToPosition(offset: number, lineStarts: number[]): Position {
  // Binary search for line
  let lo = 0;
  let hi = lineStarts.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >>> 1;
    if (lineStarts[mid] <= offset) lo = mid;
    else hi = mid - 1;
  }
  return { line: lo, character: offset - lineStarts[lo] };
}

function rangeOf(startOffset: number, endOffset: number, lineStarts: number[]): Range {
  return {
    start: offsetToPosition(startOffset, lineStarts),
    end: offsetToPosition(endOffset, lineStarts),
  };
}

/**
 * Convenience helper: re-compute the line starts for a source string.
 * Exposed for callers that need to translate offsets independently.
 */
export function lineStartsOf(source: string): number[] {
  return computeLineStarts(source);
}
