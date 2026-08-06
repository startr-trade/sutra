/**
 * <q:case when="…" call="…"> list — bound to bpmn:CallActivity (paired with q:dispatch).
 *
 * Each case has two fields:
 *   - when (FEEL expression)
 *   - call (process://module/version URI)
 *
 * Rendered as a list entry: operators add / remove / reorder cases inline.
 */

import { getAllExtensionElements } from '../util/extensionElements.js';

const Q_CASE_TYPE = 'q:Case';

function ListEntryStub() { return null; }

function caseList(element) {
  return {
    id: 'q-case-list',
    component: ListEntryStub,
    label: 'Cases',
    description: 'Ordered list of (when, call) pairs evaluated against the q:dispatch key',
    items: (() => {
      const all = getAllExtensionElements(element, Q_CASE_TYPE);
      return all.map((c, idx) => ({
        id: `q-case-${idx}`,
        label: `case[${idx}]`,
        when: c.get('when') || '',
        call: c.get('call') || ''
      }));
    })(),
    element
  };
}

export function QCaseGroup(element, translate) {
  return {
    id: 'q-case',
    label: translate ? translate('q:case — dispatch cases') : 'q:case — dispatch cases',
    entries: [caseList(element)]
  };
}
