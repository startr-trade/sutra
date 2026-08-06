/**
 * q-properties-provider.test.js — exercise QPropertiesProvider headlessly.
 *
 * Strategy: instead of booting a full bpmn-js Modeler (which under jsdom requires deep
 * SVG/CSS polyfills — getBBox, getCTM, SVGAnimatedTransformList, etc.), we drive the
 * provider directly using:
 *   - a FakePropertiesPanel that implements only the registerProvider() contract
 *     (the exact same contract `bpmn-js-properties-panel` calls);
 *   - real bpmn-moddle-parsed business objects (the same objects the real Modeler hands
 *     to provider.getGroups()).
 *
 * This pins the provider's "panel registration for StartEvent / EndEvent / ServiceTask"
 * contract — which is the explicit assertion required by the canary spec. Full bpmn-js
 * Modeler integration is covered by separate e2e tests against the bpmn.io demo host.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import BpmnModdle from 'bpmn-moddle';

import QPropertiesProvider from '../properties/QPropertiesProvider.js';
import qPropertiesModule from '../properties/index.js';
import { qModdle } from '../index.js';

const SAMPLE_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
    xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
    xmlns:q="urn:sutra:q:1.0"
    targetNamespace="urn:sutra:test">
  <bpmn:process id="P1" isExecutable="true">
    <bpmn:startEvent id="Start_1">
      <bpmn:extensionElements>
        <q:source channel="orders.in" ack="on-persist" dedupKey="header.X-Request-Id" type="orders.placed.v1" dataClass="pii"/>
        <q:input name="payload" codec="xml" accept="*">
          <q:validators source="schema-v1" scope="tenant" consolidate="true"/>
          <q:validators source="schema-v2" scope="common"/>
        </q:input>
        <q:onValidation mode="route" errorCode="VALIDATION_FAILED"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="End_1">
      <bpmn:extensionElements>
        <q:reply mode="cloudevent-binary" authSecretRef="secret/foo"/>
      </bpmn:extensionElements>
    </bpmn:endEvent>
    <bpmn:task id="Plain_Task_1"/>
  </bpmn:process>
</bpmn:definitions>`;

class FakePropertiesPanel {
  constructor() {
    this.providers = [];
  }
  registerProvider(priority, provider) {
    this.providers.push({ priority, provider });
  }
}

// bpmn-js wraps business objects in shape/connection containers exposing `.businessObject`.
// We mirror that wrapping so the provider's `getBusinessObject(element)` call resolves
// correctly when handed our synthetic "elements".
function asElement(bo) {
  return { id: bo.id, type: bo.$type, businessObject: bo };
}

function flowOf(proc, id) {
  return proc.flowElements.find((e) => e.id === id);
}

describe('QPropertiesProvider — registration', () => {

  let panel;
  let provider;

  beforeAll(() => {
    panel = new FakePropertiesPanel();
    const translate = (s) => s;
    provider = new QPropertiesProvider(panel, translate);
  });

  it('registers itself with the propertiesPanel on construction', () => {
    expect(panel.providers).toHaveLength(1);
    expect(panel.providers[0].provider).toBe(provider);
    expect(typeof panel.providers[0].priority).toBe('number');
  });

  it('exposes the bpmn-js DI module shape', () => {
    expect(qPropertiesModule.__init__).toEqual([ 'qPropertiesProvider' ]);
    expect(qPropertiesModule.qPropertiesProvider).toEqual([ 'type', QPropertiesProvider ]);
  });

  it('declares the canonical $inject contract', () => {
    expect(QPropertiesProvider.$inject).toEqual([ 'propertiesPanel', 'translate' ]);
  });
});

describe('QPropertiesProvider — getGroups()', () => {

  let provider;
  let proc;

  beforeAll(async () => {
    const moddle = new BpmnModdle({ q: qModdle });
    const { rootElement } = await moddle.fromXML(SAMPLE_BPMN);
    proc = rootElement.rootElements.find((e) => e.$type === 'bpmn:Process');

    provider = new QPropertiesProvider(new FakePropertiesPanel(), (s) => s);
  });

  it('returns a q-source group for a StartEvent', () => {
    const start = asElement(flowOf(proc, 'Start_1'));
    const groups = provider.getGroups(start)([]);

    const ids = groups.map((g) => g.id);
    expect(ids).toContain('q-source');

    const sourceGroup = groups.find((g) => g.id === 'q-source');
    const entryIds = sourceGroup.entries.map((e) => e.id);
    expect(entryIds).toEqual(
      expect.arrayContaining([
        'q-source-channel',
        'q-source-ack',
        'q-source-dedupKey',
        'q-source-type',
        'q-source-dataClass'
      ])
    );
  });

  it('returns a q-reply group for an EndEvent', () => {
    const end = asElement(flowOf(proc, 'End_1'));
    const groups = provider.getGroups(end)([]);

    const ids = groups.map((g) => g.id);
    expect(ids).toContain('q-reply');

    const replyGroup = groups.find((g) => g.id === 'q-reply');
    const entryIds = replyGroup.entries.map((e) => e.id);
    expect(entryIds).toEqual(
      expect.arrayContaining([ 'q-reply-mode', 'q-reply-idempotencyKey', 'q-reply-authRef' ])
    );
  });

  it('does NOT return q-source / q-reply on a plain bpmn:Task (but q-audit overlay is allowed)', () => {
    const plain = asElement(flowOf(proc, 'Plain_Task_1'));
    const groups = provider.getGroups(plain)([]);

    const ids = groups.map((g) => g.id);
    expect(ids).not.toContain('q-source');
    expect(ids).not.toContain('q-reply');
    expect(ids).not.toContain('q-input');
    expect(ids).not.toContain('q-onValidation');
    // bpmn:Task is in AUDIT_FLOW_NODE_TYPES per the per-node overlay model.
    expect(ids).toContain('q-audit');
  });

  it('reads existing q:source values off the imported StartEvent', () => {
    const start = asElement(flowOf(proc, 'Start_1'));
    const sourceGroup = provider.getGroups(start)([])
      .find((g) => g.id === 'q-source');

    const channelEntry = sourceGroup.entries.find((e) => e.id === 'q-source-channel');
    expect(channelEntry.get().value).toBe('orders.in');

    const dataClassEntry = sourceGroup.entries.find((e) => e.id === 'q-source-dataClass');
    expect(dataClassEntry.get().value).toBe('pii');

    const ackEntry = sourceGroup.entries.find((e) => e.id === 'q-source-ack');
    expect(ackEntry.get().value).toBe('on-persist');
  });

  it('reads existing q:reply values off the imported EndEvent', () => {
    const end = asElement(flowOf(proc, 'End_1'));
    const replyGroup = provider.getGroups(end)([])
      .find((g) => g.id === 'q-reply');

    const modeEntry = replyGroup.entries.find((e) => e.id === 'q-reply-mode');
    expect(modeEntry.get().value).toBe('cloudevent-binary');

    const authEntry = replyGroup.entries.find((e) => e.id === 'q-reply-authRef');
    expect(authEntry.get().value).toBe('secret/foo');
  });

  it('reads an ordered q:validators chain off the imported StartEvent (nested in q:input)', () => {
    const start = asElement(flowOf(proc, 'Start_1'));
    const inputGroup = provider.getGroups(start)([])
      .find((g) => g.id === 'q-input');

    const listEntry = inputGroup.entries.find((e) => e.id === 'q-input-validators-list');
    const items = listEntry.items;

    expect(items).toHaveLength(2);
    expect(items[0]).toMatchObject({ source: 'schema-v1', scope: 'tenant', consolidate: true });
    expect(items[1]).toMatchObject({ source: 'schema-v2', scope: 'common' });
  });
});
