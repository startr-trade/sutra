/**
 * <q:source> group — bound to bpmn:StartEvent.
 *
 * Per xsd/q.xsd SourceType — binds an inbound channel to a Message Start Event.
 *
 * Entries — every attribute SourceType declares:
 *   - channel            (text)   : channel-id resolved against tenant-configuration.yaml
 *   - ack                (select) : on-persist | on-complete
 *   - dedupKey           (text)   : FEEL expression extracting a duplicate-detection value (renamed
 *                                    from the misnamed idempotencyKey — a dedup key is not an idempotency
 *                                    assertion; that is process-level <q:process idempotent>)
 *   - type               (text)   : optional CloudEvents type (enables ce.* in FEEL contexts)
 *   - dataClass          (select) : none | pii | pci | phi | financial
 *   - messageTypeValue   (text)   : exact codec message type this start event subscribes to
 *   - messageTypePattern (text)   : anchored regex over the message type (exclusive with the value)
 *   - filterExpression   (text)   : FEEL predicate over the codec's reported message attributes
 *
 * The last three were missing while SourceType declared them, so the panel could not express the
 * message-type filter at all — the attribute an author reaches for first.
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
    description: 'Drives PayloadRedactor chain selection + audit retention policy (per the 15-factor GDPR design)',
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

function messageTypeValueEntry(element) {
  return {
    id: 'q-source-messageTypeValue',
    component: TextEntryStub,
    label: 'Message type',
    description:
      'Exact codec message type this start event handles — the value the codec reports. Mutually ' +
      'exclusive with the pattern below; leave both empty to accept any type the channel yields.',
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? (src.get('messageTypeValue') || '') : '' };
    },
    set: (state, value) => ({ messageTypeValue: value }),
    element
  };
}

function messageTypePatternEntry(element) {
  return {
    id: 'q-source-messageTypePattern',
    component: TextEntryStub,
    label: 'Message type pattern',
    description:
      'Anchored regular expression over the message type — it must match the whole type, not a ' +
      'substring. Mutually exclusive with the exact message type above.',
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? (src.get('messageTypePattern') || '') : '' };
    },
    set: (state, value) => ({ messageTypePattern: value }),
    element
  };
}

function filterExpressionEntry(element) {
  return {
    id: 'q-source-filterExpression',
    component: TextEntryStub,
    label: 'Filter expression',
    description:
      'FEEL predicate over the message attributes the channel\'s codec reported, read under the ' +
      'attr root (e.g. attr.direction = "incoming"). Narrows the message-type filter rather than ' +
      'replacing it, so two start events can share one message type and be told apart by an ' +
      'attribute. Sees attributes only — not headers, not the payload. Must be deterministic, and ' +
      'must be built from and/or/not over attr comparisons when the channel has a second ' +
      'subscriber, because packaging has to prove the two cannot both match.',
    get: () => {
      const src = getExtensionElement(element, Q_SOURCE_TYPE);
      return { value: src ? (src.get('filterExpression') || '') : '' };
    },
    set: (state, value) => ({ filterExpression: value }),
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
      dataClassEntry(element),
      messageTypeValueEntry(element),
      messageTypePatternEntry(element),
      filterExpressionEntry(element)
    ]
  };
}
