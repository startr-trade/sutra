import { describe, it, expect } from 'vitest';
import { buildAliasIndex } from '../aliasIndex.js';

const EMPTY = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="empty"/>
</bpmn:definitions>
`;

const SINGLE_ALIAS = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="orderFlow" name="Order Flow">
    <bpmn:startEvent id="start" name="Inbound">
      <bpmn:extensionElements>
        <q:alias expression="payload.orderId" on-conflict="reject" multi-value="false"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>
`;

const MULTI_START_EVENT = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="dualEntry">
    <bpmn:startEvent id="http-start">
      <bpmn:extensionElements>
        <q:alias name="orderKey" expression="payload.orderId"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:startEvent id="amqp-start">
      <bpmn:extensionElements>
        <q:alias expression="payload.refId" on-conflict="correlate" multi-value="true"/>
        <q:alias expression="payload.shipmentId"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>
`;

const MULTI_PROCESS = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="processA">
    <bpmn:startEvent id="aStart">
      <bpmn:extensionElements>
        <q:alias expression="payload.a" on-conflict="reject"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
  <bpmn:process id="processB">
    <bpmn:startEvent id="bStart">
      <bpmn:extensionElements>
        <q:alias expression="payload.b" on-conflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>
`;

const MALFORMED = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="brokenFlow">
    <bpmn:startEvent id="start">
      <bpmn:extensionElements>
        <q:alias expression="payload.id" on-conflict="reject"/>
        <q:alias expression="payload.broken
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>
`;

const ON_CONFLICT_VARIANTS = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="variants">
    <bpmn:startEvent id="start">
      <bpmn:extensionElements>
        <q:alias expression="payload.a" on-conflict="reject"/>
        <q:alias expression="payload.b" on-conflict="correlate"/>
        <q:alias expression="payload.c"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>
`;

describe('alias-index extraction', () => {
  it('returns a process with no start events for an empty BPMN', () => {
    const index = buildAliasIndex(EMPTY);
    expect(index).toHaveLength(1);
    expect(index[0].processId).toBe('empty');
    expect(index[0].startEvents).toEqual([]);
  });

  it('extracts a single q:alias entry from a start event', () => {
    const index = buildAliasIndex(SINGLE_ALIAS);
    expect(index).toHaveLength(1);
    const proc = index[0];
    expect(proc.processId).toBe('orderFlow');
    expect(proc.processName).toBe('Order Flow');
    expect(proc.startEvents).toHaveLength(1);
    const start = proc.startEvents[0];
    expect(start.startEventId).toBe('start');
    expect(start.aliases).toHaveLength(1);
    const a = start.aliases[0];
    expect(a.expression).toBe('payload.orderId');
    expect(a.onConflict).toBe('reject');
    expect(a.multiValue).toBe(false);
  });

  it('extracts multiple aliases across different start events in the same process', () => {
    const index = buildAliasIndex(MULTI_START_EVENT);
    expect(index).toHaveLength(1);
    const starts = index[0].startEvents;
    expect(starts.map((s) => s.startEventId)).toEqual(['http-start', 'amqp-start']);
    expect(starts[0].aliases).toHaveLength(1);
    expect(starts[0].aliases[0].label).toBe('orderKey');
    expect(starts[0].aliases[0].onConflict).toBe('reject');
    expect(starts[1].aliases).toHaveLength(2);
    expect(starts[1].aliases[0].multiValue).toBe(true);
    expect(starts[1].aliases[0].onConflict).toBe('correlate');
    expect(starts[1].aliases[1].expression).toBe('payload.shipmentId');
  });

  it('extracts aliases across multiple <bpmn:process> definitions', () => {
    const index = buildAliasIndex(MULTI_PROCESS);
    expect(index).toHaveLength(2);
    expect(index.map((p) => p.processId)).toEqual(['processA', 'processB']);
    expect(index[0].startEvents[0].aliases[0].expression).toBe('payload.a');
    expect(index[1].startEvents[0].aliases[0].expression).toBe('payload.b');
    expect(index[0].startEvents[0].aliases[0].onConflict).toBe('reject');
    expect(index[1].startEvents[0].aliases[0].onConflict).toBe('correlate');
  });

  it('tolerates malformed BPMN — surfaces what it could parse', () => {
    const index = buildAliasIndex(MALFORMED);
    // We expect at least the first well-formed alias to be discovered.
    expect(index).toHaveLength(1);
    const start = index[0].startEvents[0];
    expect(start.aliases.length).toBeGreaterThanOrEqual(1);
    expect(start.aliases[0].expression).toBe('payload.id');
  });

  it('reports on-conflict variants verbatim (reject / correlate / default reject)', () => {
    const index = buildAliasIndex(ON_CONFLICT_VARIANTS);
    const aliases = index[0].startEvents[0].aliases;
    expect(aliases).toHaveLength(3);
    expect(aliases[0].onConflict).toBe('reject');
    expect(aliases[1].onConflict).toBe('correlate');
    // Third alias has no on-conflict declared — default is 'reject'.
    expect(aliases[2].onConflict).toBe('reject');
  });
});
