/**
 * <q:audit> group — bound to bpmn:Process plus any flow node.
 *
 * A process-level <q:audit/> turns audit on; a node-level <q:audit capture="none"/> only
 * opts that node out. Storage is chosen in the package manifest (audit.sink / audit.sinks),
 * not in the BPMN: `sink` and `target` are removed and rejected at load.
 *
 * Per xsd/q.xsd AuditType:
 *   - events   (select)  : points | all | instance  (default "points"; process scope)
 *   - capture  (select)  : none | metadata | payload  (default "metadata")
 */

import { getExtensionElement } from '../util/extensionElements.js';

const Q_AUDIT_TYPE = 'q:Audit';

function eventsEntry(element) {
  return {
    id: 'q-audit-events',
    component: SelectEntryStub,
    label: 'Events',
    description: 'Which events are audited (process scope): points = declared audit points only, all = every engine event, instance = instance lifecycle only',
    options: [
      { value: 'points',   label: 'Points (default)' },
      { value: 'all',      label: 'All events' },
      { value: 'instance', label: 'Instance lifecycle only' }
    ],
    get: () => {
      const a = getExtensionElement(element, Q_AUDIT_TYPE);
      return { value: a ? (a.get('events') || 'points') : 'points' };
    },
    set: (state, value) => ({ events: value }),
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
      return { value: a ? (a.get('capture') || 'metadata') : 'metadata' };
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
      eventsEntry(element),
      captureEntry(element)
    ]
  };
}
