/**
 * <q:audit> group — bound to bpmn:Process plus any flow node.
 *
 * Process-level <q:audit> provides defaults for the whole process. Per-node <q:audit>
 * overlays override those defaults on the specific node — and `target` is used as the
 * emitted AuditEvent.eventType override (e.g. `ORDER_ACCEPTED`).
 *
 * Per xsd/q.xsd AuditType:
 *   - sink     (select)  : sql | jsonl | custom...
 *   - target   (text)    : event-type override (per-node)
 *   - capture  (select)  : none | metadata | payload  (default "payload")
 *
 * Note: "Data class" and "Redactors" historically lived here but per xsd/q.xsd they are
 * SourceType attributes; they are now exclusively edited from QSourceGroup.
 */

import { getExtensionElement } from '../util/extensionElements.js';

const Q_AUDIT_TYPE = 'q:Audit';

const KNOWN_SINKS = [ 'sql', 'jsonl' ];

function sinkEntry(element) {
  return {
    id: 'q-audit-sink',
    component: SelectEntryStub,
    label: 'Sink',
    description: 'Where audit events land. Built-ins: sql, jsonl. Custom sink ids are accepted as free text.',
    options: [
      { value: 'sql',    label: 'SQL (default)' },
      { value: 'jsonl',  label: 'JSON-lines' },
      { value: 'custom', label: 'Custom…' }
    ],
    get: () => {
      const a = getExtensionElement(element, Q_AUDIT_TYPE);
      const raw = a ? (a.get('sink') || 'sql') : 'sql';
      // If the stored value isn't one of the built-ins, surface it as "custom" so the
      // accompanying free-text entry below can edit it.
      return { value: KNOWN_SINKS.includes(raw) ? raw : 'custom' };
    },
    set: (state, value) => ({ sink: value }),
    element
  };
}

function sinkCustomEntry(element) {
  return {
    id: 'q-audit-sink-custom',
    component: TextEntryStub,
    label: 'Custom sink id',
    description: 'Free-text sink identifier resolved against registered AuditSink beans (used when Sink = Custom…)',
    get: () => {
      const a = getExtensionElement(element, Q_AUDIT_TYPE);
      const raw = a ? (a.get('sink') || '') : '';
      return { value: KNOWN_SINKS.includes(raw) ? '' : raw };
    },
    set: (state, value) => ({ sink: value }),
    element
  };
}

function targetEntry(element) {
  return {
    id: 'q-audit-target',
    component: TextEntryStub,
    label: 'Event-type override',
    description:
      'When set on a flow-node overlay, overrides the emitted AuditEvent.eventType ' +
      '(e.g. "ORDER_ACCEPTED"). At process scope this is the default sink target.',
    get: () => {
      const a = getExtensionElement(element, Q_AUDIT_TYPE);
      return { value: a ? (a.get('target') || '') : '' };
    },
    set: (state, value) => ({ target: value }),
    element
  };
}

function captureEntry(element) {
  return {
    id: 'q-audit-capture',
    component: SelectEntryStub,
    label: 'Capture',
    description: 'How much of the payload is recorded on each AuditEvent',
    options: [
      { value: 'none',     label: 'None (event metadata only — no payload, no headers)' },
      { value: 'metadata', label: 'Metadata (event + headers, redacted payload)' },
      { value: 'payload',  label: 'Payload (full, after redactor chain)' }
    ],
    get: () => {
      const a = getExtensionElement(element, Q_AUDIT_TYPE);
      return { value: a ? (a.get('capture') || 'payload') : 'payload' };
    },
    set: (state, value) => ({ capture: value }),
    element
  };
}

function TextEntryStub() { return null; }
function SelectEntryStub() { return null; }

export function QAuditGroup(element, translate) {
  return {
    id: 'q-audit',
    label: translate ? translate('q:audit — audit policy') : 'q:audit — audit policy',
    entries: [
      sinkEntry(element),
      sinkCustomEntry(element),
      targetEntry(element),
      captureEntry(element)
    ]
  };
}
