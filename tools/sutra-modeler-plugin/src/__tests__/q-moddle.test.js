/**
 * q-moddle.test.js — parse a sample BPMN with q:* elements via bpmn-moddle
 * and assert the moddle extension resolves them to typed instances.
 */

import { describe, it, expect } from 'vitest';
import BpmnModdle from 'bpmn-moddle';

import { qModdle } from '../index.js';

const SAMPLE_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions
    xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
    xmlns:q="urn:sutra:q:1.0"
    targetNamespace="urn:sutra:test">
  <bpmn:process id="P1" isExecutable="true">
    <bpmn:extensionElements>
      <q:audit sink="sql" target="audit_events" capture="metadata"/>
    </bpmn:extensionElements>
    <bpmn:startEvent id="Start_1">
      <bpmn:extensionElements>
        <q:source channel="orders.in" ack="on-complete" dataClass="pii"/>
        <q:input name="payload" codec="xml" accept="*">
          <q:validators source="schema-v1" scope="tenant" consolidate="true"/>
          <q:validators source="schema-v2" scope="common"/>
        </q:input>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="End_1">
      <bpmn:extensionElements>
        <q:reply mode="cloudevent-binary" type="trade.startr.orders.placed.v1" authSecretRef="secret/foo"/>
      </bpmn:extensionElements>
    </bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>`;

function makeModdle() {
  return new BpmnModdle({ q: qModdle });
}

async function readDefinitions() {
  const moddle = makeModdle();
  const { rootElement } = await moddle.fromXML(SAMPLE_BPMN);
  return rootElement;
}

function processOf(defs) {
  return defs.rootElements.find((e) => e.$type === 'bpmn:Process');
}

function flowOf(proc, id) {
  return proc.flowElements.find((e) => e.id === id);
}

describe('q-moddle descriptor', () => {

  it('registers the q: prefix and URI', () => {
    expect(qModdle.prefix).toBe('q');
    expect(qModdle.uri).toBe('urn:sutra:q:1.0');
  });

  it('declares the canonical types', () => {
    const names = qModdle.types.map((t) => t.name);
    for (const t of [ 'Source', 'Reply', 'Validators', 'Alias', 'Dispatch', 'Case', 'OnValidation', 'Audit', 'Input' ]) {
      expect(names).toContain(t);
    }
  });

  it('does NOT declare a singular Validator type (unreachable per xsd/q.xsd)', () => {
    const names = qModdle.types.map((t) => t.name);
    expect(names).not.toContain('Validator');
  });

  it('resolves q:source on a StartEvent into a typed instance', async () => {
    const defs = await readDefinitions();
    const proc = processOf(defs);
    const start = flowOf(proc, 'Start_1');
    const ext = start.extensionElements.values;

    const src = ext.find((v) => v.$type === 'q:Source');
    expect(src).toBeTruthy();
    expect(src.get('channel')).toBe('orders.in');
    expect(src.get('ack')).toBe('on-complete');
    expect(src.get('dataClass')).toBe('pii');
  });

  it('resolves q:reply on an EndEvent into a typed instance', async () => {
    const defs = await readDefinitions();
    const proc = processOf(defs);
    const end = flowOf(proc, 'End_1');
    const ext = end.extensionElements.values;

    expect(ext).toHaveLength(1);
    expect(ext[0].$type).toBe('q:Reply');
    expect(ext[0].get('mode')).toBe('cloudevent-binary');
    expect(ext[0].get('type')).toBe('trade.startr.orders.placed.v1');
    expect(ext[0].get('authSecretRef')).toBe('secret/foo');
  });

  it('resolves a q:input with a nested ordered q:validators chain on a StartEvent', async () => {
    const defs = await readDefinitions();
    const proc = processOf(defs);
    const start = flowOf(proc, 'Start_1');
    const ext = start.extensionElements.values;

    const input = ext.find((v) => v.$type === 'q:Input');
    expect(input).toBeTruthy();
    expect(input.get('codec')).toBe('xml');
    expect(input.get('name')).toBe('payload');
    expect(input.get('accept')).toBe('*');

    const validators = input.get('validators');
    expect(validators).toHaveLength(2);
    expect(validators.every((v) => v.$type === 'q:Validators')).toBe(true);
    expect(validators[0].get('source')).toBe('schema-v1');
    expect(validators[0].get('scope')).toBe('tenant');
    expect(validators[0].get('consolidate')).toBe(true);
    expect(validators[1].get('source')).toBe('schema-v2');
    expect(validators[1].get('scope')).toBe('common');
  });

  it('resolves q:audit on a Process', async () => {
    const defs = await readDefinitions();
    const proc = processOf(defs);
    const ext = proc.extensionElements.values;

    expect(ext).toHaveLength(1);
    expect(ext[0].$type).toBe('q:Audit');
    expect(ext[0].get('sink')).toBe('sql');
    expect(ext[0].get('target')).toBe('audit_events');
    expect(ext[0].get('capture')).toBe('metadata');
  });

  it('round-trips q:* elements back to XML', async () => {
    const moddle = makeModdle();
    const { rootElement } = await moddle.fromXML(SAMPLE_BPMN);
    const { xml } = await moddle.toXML(rootElement);

    expect(xml).toContain('q:source');
    expect(xml).toContain('channel="orders.in"');
    expect(xml).toContain('q:reply');
    expect(xml).toContain('mode="cloudevent-binary"');
    expect(xml).toContain('q:input');
    expect(xml).toContain('q:validators');
    expect(xml).toContain('q:audit');
  });
});
