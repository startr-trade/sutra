/**
 * <q:alias> group — bound to bpmn:StartEvent.
 *
 * Lets BPMN authors declare an alias expression for the process instance — typically a
 * derived business-key FEEL expression used for instance lookup and idempotency.
 *
 * Entries:
 *   - expression  (text)    : FEEL expression (e.g. "tenant.id + ':' + payload.orderId")
 *   - onConflict  (select)  : reject | correlate
 *   - multiValue  (checkbox): true → alias evaluates to a list
 */

import { getExtensionElement } from '../util/extensionElements.js';
import { sampleEntry, previewEntry } from '../util/feelPreview.js';

const Q_ALIAS_TYPE = 'q:Alias';

function readExpression(element) {
  const a = getExtensionElement(element, Q_ALIAS_TYPE);
  return a ? (a.get('expression') || '') : '';
}

function expressionEntry(element) {
  return {
    id: 'q-alias-expression',
    component: TextEntryStub,
    label: 'Expression (FEEL)',
    description: 'Derives the alias from the inbound payload + tenant context',
    get: () => {
      const a = getExtensionElement(element, Q_ALIAS_TYPE);
      return { value: a ? a.get('expression') : '' };
    },
    set: (state, value) => ({ expression: value }),
    element
  };
}

function onConflictEntry(element) {
  return {
    id: 'q-alias-onConflict',
    component: SelectEntryStub,
    label: 'On conflict',
    options: [
      { value: 'reject', label: 'Reject (default)' },
      { value: 'correlate', label: 'Correlate to existing instance' }
    ],
    get: () => {
      const a = getExtensionElement(element, Q_ALIAS_TYPE);
      return { value: a ? (a.get('on-conflict') || 'reject') : 'reject' };
    },
    element
  };
}

function multiValueEntry(element) {
  return {
    id: 'q-alias-multiValue',
    component: CheckboxEntryStub,
    label: 'Multi-value',
    description: 'When true, the expression must evaluate to a list — every member is registered as an alias',
    get: () => {
      const a = getExtensionElement(element, Q_ALIAS_TYPE);
      return { value: a ? (a.get('multi-value') === 'true' || a.get('multi-value') === true) : false };
    },
    element
  };
}

function TextEntryStub() { return null; }
function SelectEntryStub() { return null; }
function CheckboxEntryStub() { return null; }

export function QAliasGroup(element, translate) {
  return {
    id: 'q-alias',
    label: translate ? translate('q:alias — derived business key') : 'q:alias — derived business key',
    entries: [
      expressionEntry(element),
      sampleEntry(element, 'q-alias'),
      previewEntry(element, 'q-alias', () => readExpression(element)),
      onConflictEntry(element),
      multiValueEntry(element)
    ]
  };
}
