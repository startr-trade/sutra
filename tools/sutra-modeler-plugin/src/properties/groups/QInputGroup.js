/**
 * <q:input> group — bound to bpmn:StartEvent.
 *
 * Per xsd/q.xsd InputType — declares the codec that decodes the inbound payload into a typed
 * envelope visible to FEEL as `payload.*` plus an ordered <q:validators source=…> chain.
 *
 * Entries:
 *   - name      (text)    : binding name (default "payload")
 *   - codec     (text)    : codec id (free-text; modeler has no live access to the registered
 *                           codec beans, so we ship a placeholder enumerating known codecs).
 *   - accept    (text)    : content-type matcher (default "*")
 *   - validators (list)   : ordered <q:validators source="…"> children
 */

import { getExtensionElement } from '../util/extensionElements.js';

const Q_INPUT_TYPE = 'q:Input';

const CODEC_PLACEHOLDER =
  'json | yaml | xml | csv | raw-text | raw-bytes';

function nameEntry(element) {
  return {
    id: 'q-input-name',
    component: TextEntryStub,
    label: 'Binding name',
    description: 'Variable name the decoded payload binds to (default "payload" → payload.*)',
    get: () => {
      const i = getExtensionElement(element, Q_INPUT_TYPE);
      return { value: i ? (i.get('name') || 'payload') : 'payload' };
    },
    set: (state, value) => ({ name: value }),
    element
  };
}

function codecEntry(element) {
  return {
    id: 'q-input-codec',
    component: TextEntryStub,
    label: 'Codec',
    description:
      'Codec id resolved against registered PayloadCodec beans. Examples: ' + CODEC_PLACEHOLDER,
    placeholder: CODEC_PLACEHOLDER,
    get: () => {
      const i = getExtensionElement(element, Q_INPUT_TYPE);
      return { value: i ? (i.get('codec') || '') : '' };
    },
    set: (state, value) => ({ codec: value }),
    element
  };
}

function acceptEntry(element) {
  return {
    id: 'q-input-accept',
    component: TextEntryStub,
    label: 'Accept',
    description: 'Content-type matcher; default "*" accepts anything the codec can decode',
    get: () => {
      const i = getExtensionElement(element, Q_INPUT_TYPE);
      return { value: i ? (i.get('accept') || '*') : '*' };
    },
    set: (state, value) => ({ accept: value }),
    element
  };
}

function validatorsListEntry(element) {
  return {
    id: 'q-input-validators-list',
    component: ListEntryStub,
    label: 'Validators (ordered)',
    description: 'Ordered list of <q:validators source="…"> chains evaluated after decode',
    items: (() => {
      const i = getExtensionElement(element, Q_INPUT_TYPE);
      if (!i) return [];
      const validators = i.get('validators') || [];
      return validators.map((v, idx) => ({
        id: `q-input-validators-${idx}`,
        label: `validators[${idx}]`,
        source: v.get('source') || '',
        scope: v.get('scope') || '',
        when: v.get('when') || '',
        consolidate: v.get('consolidate') !== false
      }));
    })(),
    element
  };
}

function TextEntryStub() { return null; }
function ListEntryStub() { return null; }

export function QInputGroup(element, translate) {
  return {
    id: 'q-input',
    label: translate ? translate('q:input — codec + validators') : 'q:input — codec + validators',
    entries: [
      nameEntry(element),
      codecEntry(element),
      acceptEntry(element),
      validatorsListEntry(element)
    ]
  };
}
