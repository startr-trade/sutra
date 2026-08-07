/**
 * Alias-index extractor.
 *
 * Walks a BPMN document and collects every `<q:alias>` declaration nested
 * inside `<bpmn:startEvent>` elements, grouped by enclosing `<bpmn:process>`.
 *
 * The returned structure backs both the LSP `sutra/aliasIndex` request and the
 * VS Code tree-view (`AliasTreeProvider`) — keep the shape stable so the
 * client side can render entries without re-parsing.
 *
 * BPMN shape (per `xsd/q.xsd#AliasType`):
 *
 *   <bpmn:process id="orderFlow">
 *     <bpmn:startEvent id="start">
 *       <bpmn:extensionElements>
 *         <q:alias name="orderKey" expression="payload.orderId"
 *                  unique="true" onConflict="reject" multi="false"/>
 *       </bpmn:extensionElements>
 *     </bpmn:startEvent>
 *   </bpmn:process>
 *
 * The extractor is tolerant of malformed XML — entries are skipped rather
 * than failing the whole file (the LSP would still surface diagnostics for
 * the underlying parse errors). The legacy kebab-case forms
 * `on-conflict` / `multi-value` are still recognised so that fixtures
 * written before the M0 freeze keep working in IDE outline.
 */

import { parseBpmn, ParseResult, Range } from './parser.js';

export interface AliasEntry {
  /** Optional logical name (`name=`). Empty string when not declared. */
  label: string;
  /** FEEL expression. Empty when not declared. */
  expression: string;
  /** `reject` (default) | `correlate`. */
  onConflict: string;
  /** True when the alias evaluates to a list. */
  multiValue: boolean;
  /** Range of the `<q:alias>` opening tag in the source document. */
  range: Range;
}

export interface StartEventAliases {
  startEventId: string;
  startEventName: string;
  aliases: AliasEntry[];
}

export interface ProcessAliases {
  processId: string;
  processName: string;
  startEvents: StartEventAliases[];
}

/**
 * Build the alias index for a BPMN source string. Returns an empty array
 * for an empty or all-comments document.
 */
export function buildAliasIndex(source: string, parsed?: ParseResult): ProcessAliases[] {
  const events = (parsed ?? parseBpmn(source)).events;

  const processes: ProcessAliases[] = [];
  let currentProcess: ProcessAliases | null = null;
  let currentStart: StartEventAliases | null = null;
  // Stack tracks "are we inside <bpmn:extensionElements> of the current start event?"

  for (const ev of events) {
    if (ev.kind === 'open' || ev.kind === 'self-closing') {
      if (isLocalName(ev.name, 'process')) {
        const id = attr(ev.attributes, 'id') ?? '';
        const name = attr(ev.attributes, 'name') ?? id;
        currentProcess = { processId: id, processName: name, startEvents: [] };
        processes.push(currentProcess);
        continue;
      }

      if (isLocalName(ev.name, 'startEvent')) {
        if (currentProcess) {
          const id = attr(ev.attributes, 'id') ?? '';
          const name = attr(ev.attributes, 'name') ?? id;
          currentStart = { startEventId: id, startEventName: name, aliases: [] };
          currentProcess.startEvents.push(currentStart);
        }
        continue;
      }

      if (isLocalName(ev.name, 'extensionElements')) {
        // (extension-element depth is not consulted here)
        continue;
      }

      // q:alias is recognised either inside extensionElements (canonical) or
      // anywhere inside a startEvent (looser tolerance for hand-written docs).
      if (ev.name === 'q:alias' && currentStart) {
        const attrs = ev.attributes;
        const expression = attr(attrs, 'expression') ?? '';
        const labelName = attr(attrs, 'name') ?? '';
        const onConflict = attr(attrs, 'on-conflict') ?? attr(attrs, 'onConflict') ?? 'reject';
        const multiRaw = attr(attrs, 'multi-value') ?? attr(attrs, 'multi') ?? 'false';
        const multiValue = multiRaw === 'true' || multiRaw === '1';
        currentStart.aliases.push({
          label: labelName,
          expression,
          onConflict,
          multiValue,
          range: ev.range,
        });
        continue;
      }
    }

    if (ev.kind === 'close') {
      if (isLocalName(ev.name, 'process')) {
        currentProcess = null;
        currentStart = null;
      } else if (isLocalName(ev.name, 'startEvent')) {
        currentStart = null;
      }
    }
  }

  return processes;
}

function attr(
  list: { name: string; value: string }[],
  name: string
): string | undefined {
  for (const a of list) if (a.name === name) return a.value;
  return undefined;
}

/**
 * Match by local name, ignoring any namespace prefix. So `bpmn:process` and
 * `bpmn2:process` and `process` all match localName `process`.
 */
function isLocalName(tag: string, localName: string): boolean {
  const idx = tag.indexOf(':');
  const local = idx < 0 ? tag : tag.slice(idx + 1);
  return local === localName;
}
