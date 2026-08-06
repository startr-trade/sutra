/**
 * <q:reply> group — bound to bpmn:EndEvent.
 *
 * Entries:
 *   - mode            (select) : NATIVE / CLOUDEVENT_BINARY / CLOUDEVENT_STRUCTURED
 *                                (the XSD also accepts "match-inbound"; surfaced in tooltip)
 *   - idempotencyKey  (text)   : note — the XSD models idempotency on q:source, but the
 *                                modeler exposes a reply-side "idempotencyKey" field here as
 *                                requested by the canary spec. Engine-side it is captured on
 *                                the reply attempt for outbound dedup.
 *   - authRef         (text)   : maps to q:reply/@authSecretRef
 */

import { getExtensionElement } from '../util/extensionElements.js';

const Q_REPLY_TYPE = 'q:Reply';

function modeEntry(element) {
  return {
    id: 'q-reply-mode',
    component: SelectEntryStub,
    label: 'Reply mode',
    options: [
      { value: 'native', label: 'NATIVE' },
      { value: 'cloudevent-binary', label: 'CLOUDEVENT_BINARY' },
      { value: 'cloudevent-structured', label: 'CLOUDEVENT_STRUCTURED' },
      { value: 'match-inbound', label: 'MATCH_INBOUND' }
    ],
    get: () => {
      const r = getExtensionElement(element, Q_REPLY_TYPE);
      return { value: r ? (r.get('mode') || 'native') : 'native' };
    },
    element
  };
}

function idempotencyKeyEntry(element) {
  return {
    id: 'q-reply-idempotencyKey',
    component: TextEntryStub,
    label: 'Idempotency key (FEEL)',
    description: 'Captured on the outbound reply attempt. Engine-side dedup uses this key.',
    get: () => {
      const r = getExtensionElement(element, Q_REPLY_TYPE);
      // Stored on a moddle-extension attribute for forward-compat; canary slot only.
      return { value: r ? (r.get('idempotencyKey') || '') : '' };
    },
    element
  };
}

function authRefEntry(element) {
  return {
    id: 'q-reply-authRef',
    component: TextEntryStub,
    label: 'Auth secret ref',
    description: 'Maps to q:reply/@authSecretRef',
    get: () => {
      const r = getExtensionElement(element, Q_REPLY_TYPE);
      return { value: r ? (r.get('authSecretRef') || '') : '' };
    },
    element
  };
}

function TextEntryStub() { return null; }
function SelectEntryStub() { return null; }

export function QReplyGroup(element, translate) {
  return {
    id: 'q-reply',
    label: translate ? translate('q:reply — outbound reply') : 'q:reply — outbound reply',
    entries: [
      modeEntry(element),
      idempotencyKeyEntry(element),
      authRefEntry(element)
    ]
  };
}
