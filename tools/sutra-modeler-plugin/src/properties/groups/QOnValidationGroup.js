/**
 * <q:onValidation> group — bound to bpmn:StartEvent.
 *
 * Per xsd/q.xsd OnValidationType — declares the engine's behaviour when payload-level
 * validation (q:input + q:validators chain) fails structurally.
 *
 * Entries:
 *   - mode       (select)  : route | reject | error  (required)
 *   - errorCode  (text)    : optional BPMN error code raised when mode="error"
 */

import { getExtensionElement } from '../util/extensionElements.js';

const Q_ON_VALIDATION_TYPE = 'q:OnValidation';

function modeEntry(element) {
  return {
    id: 'q-onValidation-mode',
    component: SelectEntryStub,
    label: 'On validation failure',
    description:
      'route → forward to error subprocess; reject → ack-and-drop with diagnostic; error → raise a BPMN error event',
    options: [
      { value: 'route', label: 'Route to error subprocess' },
      { value: 'reject', label: 'Reject (ack + drop)' },
      { value: 'error', label: 'Raise BPMN error' }
    ],
    get: () => {
      const v = getExtensionElement(element, Q_ON_VALIDATION_TYPE);
      return { value: v ? (v.get('mode') || 'route') : 'route' };
    },
    set: (state, value) => ({ mode: value }),
    element
  };
}

function errorCodeEntry(element) {
  return {
    id: 'q-onValidation-errorCode',
    component: TextEntryStub,
    label: 'Error code',
    description: 'BPMN error code (used when mode="error"). E.g. "PAYLOAD_VALIDATION_FAILED"',
    get: () => {
      const v = getExtensionElement(element, Q_ON_VALIDATION_TYPE);
      return { value: v ? (v.get('errorCode') || '') : '' };
    },
    set: (state, value) => ({ errorCode: value }),
    element
  };
}

function TextEntryStub() { return null; }
function SelectEntryStub() { return null; }

export function QOnValidationGroup(element, translate) {
  return {
    id: 'q-onValidation',
    label: translate
      ? translate('q:onValidation — payload validation failure policy')
      : 'q:onValidation — payload validation failure policy',
    entries: [
      modeEntry(element),
      errorCodeEntry(element)
    ]
  };
}
