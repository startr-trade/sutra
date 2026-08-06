/**
 * <q:source> group — bound to bpmn:StartEvent.
 *
 * Per xsd/q.xsd SourceType — binds an inbound channel to a Message Start Event.
 *
 * Entries (all five attributes from SourceType):
 *   - channel         (text)    : channel-id resolved against tenant-configuration.yaml
 *   - ack             (select)  : on-persist | on-complete
 *   - dedupKey        (text)    : FEEL expression extracting a duplicate-detection value (renamed
 *                                 from the misnamed idempotencyKey — a dedup key is not an idempotency
 *                                 assertion; that is process-level <q:process idempotent>)
 *   - type            (text)    : optional CloudEvents type (enables ce.* in FEEL contexts)
 *   - dataClass       (select)  : none | pii | pci | phi | financial
 */

import { getExtensionElement } from '../util/extensionElements.js';

const Q_SOURCE_TYPE = 'q:Source';

function channelEntry(element) {
  return {
    id: 'q-source-channel',
    component: TextEntryStub,
    isEdited: (node) => !!node && !!node.value,
    label: 'Channel',
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? src.get('channel') : '' };
    },
    set: (state, value) => ({ channel: value }),
    element
  };
}

function ackEntry(element) {
  return {
    id: 'q-source-ack',
    component: SelectEntryStub,
    label: 'Ack mode',
    options: [
      { value: 'on-persist', label: 'On persist (default)' },
      { value: 'on-complete', label: 'On complete' }
    ],
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? (src.get('ack') || 'on-persist') : 'on-persist' };
    },
    element
  };
}

function dedupKeyEntry(element) {
  return {
    id: 'q-source-dedupKey',
    component: TextEntryStub,
    label: 'Dedup key',
    description:
      'Expression extracting a duplicate-detection value from native headers / payload (e.g. ' +
      'header.X-Request-Id, amqp.message-id, body.GrpHdr.MsgId). A body.<path> form drives inbox ' +
      'dedup. Defaults to ce.id when CloudEvents wrap mode is active and type="" is set. NOTE: a ' +
      'dedup key does not assert idempotency — that is the process-level <q:process idempotent>.',
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? (src.get('dedupKey') || '') : '' };
    },
    set: (state, value) => ({ dedupKey: value }),
    element
  };
}

function typeEntry(element) {
  return {
    id: 'q-source-type',
    component: TextEntryStub,
    label: 'CloudEvents type',
    description:
      'Optional CloudEvents type. When set, enables CE detection on the channel and exposes event.* in FEEL.',
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? (src.get('type') || '') : '' };
    },
    set: (state, value) => ({ type: value }),
    element
  };
}

function dataClassEntry(element) {
  return {
    id: 'q-source-dataClass',
    component: SelectEntryStub,
    label: 'Data class',
    description: 'Drives PayloadRedactor chain selection + audit retention policy (per docs/15-factor.md GDPR design)',
    options: [
      { value: 'none', label: 'None' },
      { value: 'pii', label: 'PII' },
      { value: 'pci', label: 'PCI' },
      { value: 'phi', label: 'PHI' },
      { value: 'financial', label: 'Financial' }
    ],
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? (src.get('dataClass') || 'none') : 'none' };
    },
    element
  };
}

// Placeholder Inferno component stubs. The actual renderers are supplied by
// @bpmn-io/properties-panel; we keep references symbolic so the provider can be unit-tested
// without booting a DOM. The wiring is exercised end-to-end in headless bpmn-js integration
// tests that boot a full Modeler.
function TextEntryStub() { return null; }
function SelectEntryStub() { return null; }

export function QSourceGroup(element, translate) {
  return {
    id: 'q-source',
    label: translate ? translate('q:source — inbound channel') : 'q:source — inbound channel',
    entries: [
      channelEntry(element),
      ackEntry(element),
      dedupKeyEntry(element),
      typeEntry(element),
      dataClassEntry(element)
    ]
  };
}
