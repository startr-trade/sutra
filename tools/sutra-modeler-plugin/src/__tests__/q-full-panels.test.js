/**
 * Tests for the 4 panels added in the canary→full upgrade:
 *   q:dispatch / q:case  on bpmn:CallActivity
 *   q:alias              on bpmn:StartEvent
 *   q:audit              on bpmn:Process
 *
 * Strategy mirrors q-properties-provider.test.js — feed real bpmn-moddle-parsed
 * BusinessObjects through the provider's getGroups() and assert the right group
 * shows up with the right entries + default values.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import BpmnModdle from 'bpmn-moddle';
import qModdle from '../moddle/q-moddle.json';
import QPropertiesProvider from '../properties/QPropertiesProvider.js';
import { QDispatchGroup } from '../properties/groups/QDispatchGroup.js';
import { QCaseGroup } from '../properties/groups/QCaseGroup.js';
import { QAliasGroup } from '../properties/groups/QAliasGroup.js';
import { QAuditGroup } from '../properties/groups/QAuditGroup.js';

const Q_NS = 'urn:sutra:q:1.0';

const BPMN_WITH_ALL_PANELS = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="${Q_NS}"
                  targetNamespace="http://example.com/test">
  <bpmn:process id="orders" isExecutable="true">
    <bpmn:extensionElements>
      <q:audit sink="sql" target="ORDERS_AUDIT" capture="metadata"/>
    </bpmn:extensionElements>
    <bpmn:startEvent id="start1">
      <bpmn:extensionElements>
        <q:source channel="orders-rabbit-acme" ack="on-persist" dataClass="financial"/>
        <q:alias expression="payload.orderId" on-conflict="reject" multi-value="false"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:callActivity id="dispatchCall">
      <bpmn:extensionElements>
        <q:dispatch key="payload.messageType" default-case="process://orders/default/v1" on-no-match="error"/>
        <q:case when="payload.messageType = 'order.created'" call="process://orders/order-created/v1"/>
        <q:case when="payload.messageType = 'order.cancelled'" call="process://orders/order-cancelled/v1"/>
      </bpmn:extensionElements>
    </bpmn:callActivity>
  </bpmn:process>
</bpmn:definitions>
`;

class FakePropertiesPanel {
  constructor() { this.priority = null; this.provider = null; }
  registerProvider(priority, provider) {
    this.priority = priority;
    this.provider = provider;
  }
}

const translate = (s) => s;

let definitions;
beforeAll(async () => {
  const moddle = new BpmnModdle({ q: qModdle });
  const { rootElement } = await moddle.fromXML(BPMN_WITH_ALL_PANELS);
  definitions = rootElement;
});

function findById(id) {
  const process = definitions.rootElements[0];
  if (process.id === id) return process;
  for (const flow of process.flowElements || []) {
    if (flow.id === id) return flow;
  }
  return null;
}

describe('QDispatchGroup', () => {
  it('exposes key + live-preview pair + default-case + onNoMatch entries with current values', () => {
    const call = findById('dispatchCall');
    const group = QDispatchGroup({ businessObject: call }, translate);
    expect(group.id).toBe('q-dispatch');
    const ids = group.entries.map((e) => e.id);
    expect(ids).toEqual([
      'q-dispatch-key',
      'q-dispatch-sample',
      'q-dispatch-preview',
      'q-dispatch-default',
      'q-dispatch-onNoMatch'
    ]);
    expect(group.entries[0].get().value).toBe('payload.messageType');
    // q-dispatch-sample (idx 1) and q-dispatch-preview (idx 2) tested in feel-preview.test.js
    expect(group.entries[3].get().value).toBe('process://orders/default/v1');
    expect(group.entries[4].get().value).toBe('error');
  });
});

describe('QCaseGroup', () => {
  it('lists every q:case child in declaration order', () => {
    const call = findById('dispatchCall');
    const group = QCaseGroup({ businessObject: call }, translate);
    expect(group.id).toBe('q-case');
    const items = group.entries[0].items;
    expect(items).toHaveLength(2);
    expect(items[0].when).toBe("payload.messageType = 'order.created'");
    expect(items[0].call).toBe('process://orders/order-created/v1');
    expect(items[1].when).toBe("payload.messageType = 'order.cancelled'");
  });
});

describe('QAliasGroup', () => {
  it('exposes expression + live-preview pair + on-conflict + multi-value entries', () => {
    const start = findById('start1');
    const group = QAliasGroup({ businessObject: start }, translate);
    expect(group.id).toBe('q-alias');
    const ids = group.entries.map((e) => e.id);
    expect(ids).toEqual([
      'q-alias-expression',
      'q-alias-sample',
      'q-alias-preview',
      'q-alias-onConflict',
      'q-alias-multiValue'
    ]);
    expect(group.entries[0].get().value).toBe('payload.orderId');
    // q-alias-sample (idx 1) and q-alias-preview (idx 2) tested in feel-preview.test.js
    expect(group.entries[3].get().value).toBe('reject');
    expect(group.entries[4].get().value).toBe(false);
  });
});

describe('QAuditGroup', () => {
  it('reads sink + target + capture from q:audit on the process (xsd/q.xsd AuditType)', () => {
    const process = definitions.rootElements[0];
    const group = QAuditGroup({ businessObject: process }, translate);
    expect(group.id).toBe('q-audit');
    const ids = group.entries.map((e) => e.id);
    expect(ids).toEqual([
      'q-audit-sink',
      'q-audit-sink-custom',
      'q-audit-target',
      'q-audit-capture'
    ]);
    // sink=sql -> built-in, surfaced as "sql", custom field empty
    expect(group.entries[0].get().value).toBe('sql');
    expect(group.entries[1].get().value).toBe('');
    // target = event-type override
    expect(group.entries[2].get().value).toBe('ORDERS_AUDIT');
    expect(group.entries[3].get().value).toBe('metadata');
  });
});

describe('QPropertiesProvider with all 7 panels', () => {
  it('registers q-source + q-alias on bpmn:StartEvent', () => {
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);
    const start = findById('start1');
    const groups = provider.getGroups({ businessObject: start })([]);
    const ids = groups.map((g) => g.id);
    expect(ids).toContain('q-source');
    expect(ids).toContain('q-alias');
  });

  it('registers q-dispatch + q-case on bpmn:CallActivity', () => {
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);
    const call = findById('dispatchCall');
    const groups = provider.getGroups({ businessObject: call })([]);
    const ids = groups.map((g) => g.id);
    expect(ids).toEqual(expect.arrayContaining(['q-dispatch', 'q-case']));
  });

  it('registers q-audit on bpmn:Process', () => {
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);
    const process = definitions.rootElements[0];
    const groups = provider.getGroups({ businessObject: process })([]);
    const ids = groups.map((g) => g.id);
    expect(ids).toContain('q-audit');
  });

  it('adds only the q-audit overlay group to a plain bpmn:Task (per-node overlays)', async () => {
    const moddle = new BpmnModdle({ q: qModdle });
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
        <bpmn:process id="p"><bpmn:task id="plainTask"/></bpmn:process>
      </bpmn:definitions>`;
    const { rootElement } = await moddle.fromXML(xml);
    const task = rootElement.rootElements[0].flowElements[0];
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);
    const groups = provider.getGroups({ businessObject: task })([]);
    const ids = groups.map((g) => g.id);
    expect(ids).toEqual(['q-audit']);
  });

  it('adds NO q-* groups to a SequenceFlow (not a flow-node, not a process)', async () => {
    const moddle = new BpmnModdle({ q: qModdle });
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
        <bpmn:process id="p">
          <bpmn:startEvent id="s1"/>
          <bpmn:endEvent id="e1"/>
          <bpmn:sequenceFlow id="f1" sourceRef="s1" targetRef="e1"/>
        </bpmn:process>
      </bpmn:definitions>`;
    const { rootElement } = await moddle.fromXML(xml);
    const flow = rootElement.rootElements[0].flowElements.find((f) => f.id === 'f1');
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);
    const groups = provider.getGroups({ businessObject: flow })([]);
    expect(groups).toEqual([]);
  });
});
