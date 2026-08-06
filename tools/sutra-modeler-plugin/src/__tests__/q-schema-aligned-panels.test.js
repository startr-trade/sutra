/**
 * q-schema-aligned-panels.test.js — schema-alignment tests.
 *
 * Verifies the panels brought into alignment with the frozen xsd/q.xsd surface:
 *   - QInputGroup        (new)   — q:input on bpmn:StartEvent
 *   - QOnValidationGroup (new)   — q:onValidation on bpmn:StartEvent
 *   - QAuditGroup        (rewrite) — q:audit overlay on flow nodes + process
 *   - QSourceGroup       (gap-fill) — all 5 SourceType attributes
 *   - QPropertiesProvider          — registers the new groups on the right element types
 *   - Moddle removal               — Validator (singular) is gone
 */

import { describe, it, expect, beforeAll } from 'vitest';
import BpmnModdle from 'bpmn-moddle';

import { qModdle } from '../index.js';
import QPropertiesProvider from '../properties/QPropertiesProvider.js';
import { QInputGroup } from '../properties/groups/QInputGroup.js';
import { QOnValidationGroup } from '../properties/groups/QOnValidationGroup.js';
import { QAuditGroup } from '../properties/groups/QAuditGroup.js';
import { QSourceGroup } from '../properties/groups/QSourceGroup.js';

const Q_NS = 'urn:sutra:q:1.0';

const BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="${Q_NS}"
                  targetNamespace="http://example.com/test">
  <bpmn:process id="proc1" isExecutable="true">
    <bpmn:extensionElements>
      <q:audit sink="sql" target="ROOT_AUDIT" capture="payload"/>
    </bpmn:extensionElements>
    <bpmn:startEvent id="start1">
      <bpmn:extensionElements>
        <q:source channel="orders.in"
                  ack="on-complete"
                  dedupKey="header.X-Request-Id"
                  type="order.created.v1"
                  dataClass="financial"/>
        <q:input name="payload" codec="xml" accept="application/xml">
          <q:validators source="orders/order-created" scope="common" consolidate="true"/>
          <q:validators source="acme/order-created-overlay" scope="tenant"/>
        </q:input>
        <q:onValidation mode="route" errorCode="VALIDATION_FAILED"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="svc1">
      <bpmn:extensionElements>
        <q:audit sink="custom-kafka" target="ORDER_ACCEPTED" capture="metadata"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="end1"/>
  </bpmn:process>
</bpmn:definitions>
`;

class FakePropertiesPanel {
  constructor() { this.providers = []; }
  registerProvider(priority, provider) {
    this.providers.push({ priority, provider });
  }
}

const translate = (s) => s;

let definitions;

beforeAll(async () => {
  const moddle = new BpmnModdle({ q: qModdle });
  const { rootElement } = await moddle.fromXML(BPMN);
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

// ────────────────────────── Moddle / schema alignment ──────────────────────────

describe('q-moddle — schema alignment', () => {
  it('does NOT declare a singular Validator type (unreachable per xsd/q.xsd)', () => {
    const names = qModdle.types.map((t) => t.name);
    expect(names).not.toContain('Validator');
    // Plural Validators (the schema-correct form) is still present.
    expect(names).toContain('Validators');
  });

  it('declares the Input type as a separate moddle entry with the validators chain', () => {
    const input = qModdle.types.find((t) => t.name === 'Input');
    expect(input).toBeTruthy();
    // Property names may be prefix-normalized (e.g. `q:codec`) depending on how the
    // descriptor has been processed. Strip any prefix before comparing.
    const props = input.properties.map((p) => p.name.replace(/^[^:]+:/, ''));
    expect(props).toEqual(expect.arrayContaining(['name', 'codec', 'accept', 'validators']));
    const validatorsProp = input.properties.find(
      (p) => p.name === 'validators' || p.name.endsWith(':validators')
    );
    expect(validatorsProp.isMany).toBe(true);
    // Type may be prefix-normalized too (e.g. `q:Validators` vs `Validators`).
    expect(validatorsProp.type.replace(/^[^:]+:/, '')).toBe('Validators');
  });

  it('declares Input.allowedIn covering bpmn:StartEvent', () => {
    const input = qModdle.types.find((t) => t.name === 'Input');
    expect(input.meta.allowedIn).toContain('bpmn:StartEvent');
  });

  it('declares Audit.allowedIn covering bpmn:Process AND flow nodes (per-node overlays)', () => {
    const audit = qModdle.types.find((t) => t.name === 'Audit');
    expect(audit.meta.allowedIn).toContain('bpmn:Process');
    expect(audit.meta.allowedIn).toEqual(
      expect.arrayContaining([
        'bpmn:Process',
        'bpmn:ServiceTask',
        'bpmn:Task',
        'bpmn:UserTask',
        'bpmn:EndEvent',
        'bpmn:CallActivity',
        'bpmn:ExclusiveGateway'
      ])
    );
  });
});

// ────────────────────────── QInputGroup ──────────────────────────

describe('QInputGroup', () => {
  it('registers id=q-input with all four InputType attribute entries', () => {
    const start = findById('start1');
    const group = QInputGroup({ businessObject: start }, translate);
    expect(group.id).toBe('q-input');
    const ids = group.entries.map((e) => e.id);
    expect(ids).toEqual([
      'q-input-name',
      'q-input-codec',
      'q-input-accept',
      'q-input-validators-list'
    ]);
  });

  it('reads existing q:input attributes off the imported StartEvent', () => {
    const start = findById('start1');
    const group = QInputGroup({ businessObject: start }, translate);
    expect(group.entries[0].get().value).toBe('payload');
    expect(group.entries[1].get().value).toBe('xml');
    expect(group.entries[2].get().value).toBe('application/xml');
  });

  it('exposes the nested q:validators chain as an ordered list', () => {
    const start = findById('start1');
    const group = QInputGroup({ businessObject: start }, translate);
    const list = group.entries.find((e) => e.id === 'q-input-validators-list');
    expect(list.items).toHaveLength(2);
    expect(list.items[0]).toMatchObject({
      source: 'orders/order-created',
      scope: 'common',
      consolidate: true
    });
    expect(list.items[1]).toMatchObject({
      source: 'acme/order-created-overlay',
      scope: 'tenant'
    });
  });

  it('defaults name=payload and accept=* when no q:input is present', () => {
    const bareStart = { businessObject: { $type: 'bpmn:StartEvent', id: 'bare', extensionElements: null } };
    const group = QInputGroup(bareStart, translate);
    expect(group.entries[0].get().value).toBe('payload');
    expect(group.entries[2].get().value).toBe('*');
    expect(group.entries[3].items).toEqual([]);
  });
});

// ────────────────────────── QOnValidationGroup ──────────────────────────

describe('QOnValidationGroup', () => {
  it('registers id=q-onValidation with mode + errorCode entries', () => {
    const start = findById('start1');
    const group = QOnValidationGroup({ businessObject: start }, translate);
    expect(group.id).toBe('q-onValidation');
    const ids = group.entries.map((e) => e.id);
    expect(ids).toEqual(['q-onValidation-mode', 'q-onValidation-errorCode']);
  });

  it('mode entry exposes exactly the 3 OnValidationMode enum values from xsd/q.xsd', () => {
    const start = findById('start1');
    const group = QOnValidationGroup({ businessObject: start }, translate);
    const mode = group.entries[0];
    const values = mode.options.map((o) => o.value).sort();
    expect(values).toEqual(['error', 'reject', 'route']);
  });

  it('reads existing q:onValidation attributes', () => {
    const start = findById('start1');
    const group = QOnValidationGroup({ businessObject: start }, translate);
    expect(group.entries[0].get().value).toBe('route');
    expect(group.entries[1].get().value).toBe('VALIDATION_FAILED');
  });

  it('defaults mode=route + errorCode="" when no q:onValidation is present', () => {
    const bare = { businessObject: { $type: 'bpmn:StartEvent', id: 'bare2', extensionElements: null } };
    const group = QOnValidationGroup(bare, translate);
    expect(group.entries[0].get().value).toBe('route');
    expect(group.entries[1].get().value).toBe('');
  });
});

// ────────────────────────── QAuditGroup (rewrite) ──────────────────────────

describe('QAuditGroup — xsd/q.xsd AuditType alignment', () => {
  it('exposes sink + sink-custom + target + capture (no Data class, no Redactors)', () => {
    const proc = findById('proc1');
    const group = QAuditGroup({ businessObject: proc }, translate);
    const ids = group.entries.map((e) => e.id);
    expect(ids).toEqual([
      'q-audit-sink',
      'q-audit-sink-custom',
      'q-audit-target',
      'q-audit-capture'
    ]);
    // Confirm the historical "Data class" + "Redactors" entries are gone.
    expect(ids).not.toContain('q-audit-dataClass');
    expect(ids).not.toContain('q-audit-redactors');
  });

  it('capture entry exposes exactly the 3 AuditCapture enum values from xsd/q.xsd', () => {
    const proc = findById('proc1');
    const group = QAuditGroup({ businessObject: proc }, translate);
    const capture = group.entries.find((e) => e.id === 'q-audit-capture');
    const values = capture.options.map((o) => o.value).sort();
    expect(values).toEqual(['metadata', 'none', 'payload']);
  });

  it('target entry is labelled as the event-type override', () => {
    const proc = findById('proc1');
    const group = QAuditGroup({ businessObject: proc }, translate);
    const target = group.entries.find((e) => e.id === 'q-audit-target');
    expect(target.label).toMatch(/event-type/i);
  });

  it('reads a built-in sink (sql) cleanly - surfaces "sql" with empty custom field', () => {
    const proc = findById('proc1');
    const group = QAuditGroup({ businessObject: proc }, translate);
    expect(group.entries[0].get().value).toBe('sql');
    expect(group.entries[1].get().value).toBe('');
    expect(group.entries[2].get().value).toBe('ROOT_AUDIT');
    expect(group.entries[3].get().value).toBe('payload');
  });

  it('reads a custom sink — surfaces as "custom" + populates the custom field with the raw id', () => {
    const svc = findById('svc1');
    const group = QAuditGroup({ businessObject: svc }, translate);
    expect(group.entries[0].get().value).toBe('custom');
    expect(group.entries[1].get().value).toBe('custom-kafka');
    expect(group.entries[2].get().value).toBe('ORDER_ACCEPTED');
    expect(group.entries[3].get().value).toBe('metadata');
  });
});

// ────────────────────────── QSourceGroup (gap-fill) ──────────────────────────

describe('QSourceGroup — covers all 5 SourceType attributes', () => {
  it('exposes channel + ack + dedupKey + type + dataClass entries', () => {
    const start = findById('start1');
    const group = QSourceGroup({ businessObject: start }, translate);
    const ids = group.entries.map((e) => e.id);
    expect(ids).toEqual([
      'q-source-channel',
      'q-source-ack',
      'q-source-dedupKey',
      'q-source-type',
      'q-source-dataClass'
    ]);
  });

  it('reads all 5 attributes off the imported StartEvent', () => {
    const start = findById('start1');
    const group = QSourceGroup({ businessObject: start }, translate);
    const get = (id) => group.entries.find((e) => e.id === id).get().value;
    expect(get('q-source-channel')).toBe('orders.in');
    expect(get('q-source-ack')).toBe('on-complete');
    expect(get('q-source-dedupKey')).toBe('header.X-Request-Id');
    expect(get('q-source-type')).toBe('order.created.v1');
    expect(get('q-source-dataClass')).toBe('financial');
  });
});

// ────────────────────────── QPropertiesProvider registration ──────────────────────────

describe('QPropertiesProvider — new groups registered on the right element types', () => {
  it('registers q-source + q-input + q-onValidation + q-alias on bpmn:StartEvent', () => {
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);
    const start = findById('start1');
    const groups = provider.getGroups({ businessObject: start })([]);
    const ids = groups.map((g) => g.id);
    expect(ids).toEqual(expect.arrayContaining([
      'q-source',
      'q-input',
      'q-onValidation',
      'q-alias'
    ]));
  });

  it('registers q-audit on bpmn:Process AND on bpmn:ServiceTask (per-node overlay)', () => {
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);

    const procGroups = provider.getGroups({ businessObject: findById('proc1') })([]);
    expect(procGroups.map((g) => g.id)).toContain('q-audit');

    const svcGroups = provider.getGroups({ businessObject: findById('svc1') })([]);
    expect(svcGroups.map((g) => g.id)).toContain('q-audit');
  });

  it('does NOT register a q-validators group on any element (singular removed)', async () => {
    const moddle = new BpmnModdle({ q: qModdle });
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
      <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
        <bpmn:process id="p"><bpmn:serviceTask id="st"/></bpmn:process>
      </bpmn:definitions>`;
    const { rootElement } = await moddle.fromXML(xml);
    const st = rootElement.rootElements[0].flowElements[0];
    const panel = new FakePropertiesPanel();
    const provider = new QPropertiesProvider(panel, translate);
    const groups = provider.getGroups({ businessObject: st })([]);
    expect(groups.map((g) => g.id)).not.toContain('q-validators');
  });
});
