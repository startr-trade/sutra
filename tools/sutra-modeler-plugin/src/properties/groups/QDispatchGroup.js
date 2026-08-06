/**
 * <q:dispatch> group — bound to bpmn:CallActivity.
 *
 * Entries:
 *   - key            (text)    : FEEL expression selecting the case (e.g. "payload.messageType")
 *   - defaultCase    (text)    : process URI used when no <q:case when="..."> matches
 *   - onNoMatch      (select)  : skip | error
 *
 * The repeated <q:case when="..."> children live in {@link QCaseGroup}.
 */

import { getExtensionElement } from '../util/extensionElements.js';
import { sampleEntry, previewEntry } from '../util/feelPreview.js';

const Q_DISPATCH_TYPE = 'q:Dispatch';

function readKey(element) {
  const d = getExtensionElement(element, Q_DISPATCH_TYPE);
  return d ? (d.get('key') || '') : '';
}

function keyEntry(element) {
  return {
    id: 'q-dispatch-key',
    component: TextEntryStub,
    label: 'Key (FEEL)',
    description: 'FEEL expression evaluated against the inbound payload to pick the case',
    get: () => {
      const d = getExtensionElement(element, Q_DISPATCH_TYPE);
      return { value: d ? d.get('key') : '' };
    },
    set: (state, value) => ({ key: value }),
    element
  };
}

function defaultCaseEntry(element) {
  return {
    id: 'q-dispatch-default',
    component: TextEntryStub,
    label: 'Default case (process://)',
    description: 'Process URI used when no <q:case when="..."> matches',
    get: () => {
      const d = getExtensionElement(element, Q_DISPATCH_TYPE);
      return { value: d ? d.get('default-case') : '' };
    },
    set: (state, value) => ({ 'default-case': value }),
    element
  };
}

function onNoMatchEntry(element) {
  return {
    id: 'q-dispatch-onNoMatch',
    component: SelectEntryStub,
    label: 'On no match',
    options: [
      { value: 'skip', label: 'Skip silently (default)' },
      { value: 'error', label: 'Raise BPMN error' }
    ],
    get: () => {
      const d = getExtensionElement(element, Q_DISPATCH_TYPE);
      return { value: d ? (d.get('on-no-match') || 'skip') : 'skip' };
    },
    element
  };
}

function TextEntryStub() { return null; }
function SelectEntryStub() { return null; }

export function QDispatchGroup(element, translate) {
  return {
    id: 'q-dispatch',
    label: translate ? translate('q:dispatch — route by FEEL key') : 'q:dispatch — route by FEEL key',
    entries: [
      keyEntry(element),
      sampleEntry(element, 'q-dispatch'),
      previewEntry(element, 'q-dispatch', () => readKey(element)),
      defaultCaseEntry(element),
      onNoMatchEntry(element)
    ]
  };
}
