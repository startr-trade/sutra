/** Shared BPMN fixtures for canary tests.
 *
 * Every fixture below uses elements/attributes that exist in `xsd/q.xsd`
 * (the M0-frozen schema). See `tools/sutra-vscode/server/src/qSchema.ts`
 * for the IDE-side mirror.
 */

export const ORDER_PROCESS = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="orderFlow">
    <bpmn:startEvent id="start" name="Inbound">
      <bpmn:extensionElements>
        <q:source channel="xml" ack="on-persist"/>
        <q:input codec="xml" accept="application/xml">
          <q:validators source="dmn"/>
        </q:input>
        <q:alias name="orderKey" expression="payload.orderId" unique="true" onConflict="correlate"/>
        <q:onValidation mode="reject"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="end">
      <bpmn:extensionElements>
        <q:reply mode="native" destination="xml" required="true"/>
      </bpmn:extensionElements>
    </bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
`;

export const BAD_VALIDATOR = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="orderFlow">
    <bpmn:startEvent id="start">
      <bpmn:extensionElements>
        <q:input codec="xml">
          <q:validators source="xyzzy"/>
        </q:input>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>
`;

/** Fixture exercising q:dispatch + q:case on a call activity. */
export const DISPATCH_PROCESS = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="router">
    <bpmn:callActivity id="route">
      <bpmn:extensionElements>
        <q:dispatch default="catchAll" onNoMatch="error">
          <q:case when="payload.kind = 'invoice'" calledElement="invoiceFlow"/>
          <q:case when="payload.kind = 'order'" calledElement="orderFlow" scope="tenant"/>
        </q:dispatch>
      </bpmn:extensionElements>
    </bpmn:callActivity>
  </bpmn:process>
</bpmn:definitions>
`;

/** Fixture covering q:audit at process level. */
export const AUDIT_PROCESS = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="audited">
    <bpmn:extensionElements>
      <q:audit sink="jsonl" target="/var/log/sutra/audit.jsonl" capture="metadata"/>
    </bpmn:extensionElements>
  </bpmn:process>
</bpmn:definitions>
`;

/**
 * Build a `<q:coverage>` fixture over a fixed 3-node / 2-flow process
 * (`start -f1-> t1 -f2-> end`) with the given coverage declarations spliced in.
 */
export function coverageDoc(coverage: string): string {
  return `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="orderFlow">
    <bpmn:extensionElements>
${coverage}
    </bpmn:extensionElements>
    <bpmn:startEvent id="start"/>
    <bpmn:task id="t1"/>
    <bpmn:endEvent id="end"/>
    <bpmn:sequenceFlow id="f1" sourceRef="start" targetRef="t1"/>
    <bpmn:sequenceFlow id="f2" sourceRef="t1" targetRef="end"/>
  </bpmn:process>
</bpmn:definitions>
`;
}

/**
 * Build a `<q:variable source="…">` fixture whose process subscribes to the
 * intake channel `ordersIn` via a `<q:source>` on its start event.
 */
export function variableSourceDoc(variable: string): string {
  return `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/2.0" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="orderFlow">
    <bpmn:extensionElements>
${variable}
    </bpmn:extensionElements>
    <bpmn:startEvent id="start">
      <bpmn:extensionElements>
        <q:source channel="ordersIn"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
  </bpmn:process>
</bpmn:definitions>
`;
}
