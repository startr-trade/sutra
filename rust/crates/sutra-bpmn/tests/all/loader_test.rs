//! BPMN model loading: the supported element set, the fail-closed rejection of
//! unsupported elements, `<bpmn:import>` extraction, and message-catch parse pins.

use sutra_bpmn::{BpmnModelLoader, Node};

fn loader() -> BpmnModelLoader {
    BpmnModelLoader::new()
}

// ---- supported-element parsing ------------------------------------------------

#[test]
fn loads_minimal_linear_process() {
    let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          targetNamespace="https://example.com/orders/v1">
          <bpmn:process id="processOrder" isExecutable="true">
            <bpmn:startEvent id="Start"/>
            <bpmn:serviceTask id="Validate" name="Validate Order" implementation="${validateOrder}"/>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Validate"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Validate" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;

    let module = loader().load(bpmn.as_bytes()).unwrap();
    assert_eq!(module.target_namespace, "https://example.com/orders/v1");
    assert_eq!(module.process_ids(), vec!["processOrder"]);

    let process = module.process("processOrder").unwrap();
    assert!(process.is_executable);
    let mut ids: Vec<&str> = process.nodes().iter().map(|n| n.id()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["End", "Start", "Validate"]);
    assert_eq!(process.flows().len(), 2);

    match process.node("Validate").unwrap() {
        Node::ServiceTask {
            implementation,
            name,
            ..
        } => {
            assert_eq!(implementation, "validateOrder"); // ${...} stripped
            assert_eq!(name.as_deref(), Some("Validate Order"));
        }
        other => panic!("expected ServiceTask, got {other:?}"),
    }
}

#[test]
fn loads_exclusive_gateway_with_default() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:exclusiveGateway id="G" default="fDefault"/>
            <bpmn:endEvent id="EA"/>
            <bpmn:endEvent id="EB"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="G"/>
            <bpmn:sequenceFlow id="fA" sourceRef="G" targetRef="EA">
              <bpmn:conditionExpression>payload.amount &gt; 100</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
            <bpmn:sequenceFlow id="fDefault" sourceRef="G" targetRef="EB"/>
          </bpmn:process>
        </bpmn:definitions>"#;

    let module = loader().load(bpmn.as_bytes()).unwrap();
    let process = module.process("p1").unwrap();

    match process.node("G").unwrap() {
        Node::ExclusiveGateway {
            default_flow_id, ..
        } => {
            assert_eq!(default_flow_id.as_deref(), Some("fDefault"));
        }
        other => panic!("expected ExclusiveGateway, got {other:?}"),
    }
    let flow_a = process.flows().iter().find(|f| f.id == "fA").unwrap();
    assert_eq!(flow_a.condition.as_deref(), Some("payload.amount > 100"));
}

#[test]
fn missing_document_element_rejected() {
    let e = loader().load(b"not xml").unwrap_err();
    assert!(e.message.contains("BPMN parse failed"), "{e}");
}

#[test]
fn definitions_without_process_rejected() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"/>"#;
    let e = loader().load(bpmn.as_bytes()).unwrap_err();
    assert!(e.message.contains("no <bpmn:process>"), "{e}");
}

#[test]
fn service_task_without_implementation_rejected() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T"/>
            <bpmn:endEvent id="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = loader().load(bpmn.as_bytes()).unwrap_err();
    assert!(e.message.contains("no implementation"), "{e}");
}

#[test]
fn parallel_gateway_loads() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:parallelGateway id="Fork"/>
            <bpmn:parallelGateway id="Join"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Fork" targetRef="Join"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Fork" targetRef="Join"/>
            <bpmn:sequenceFlow id="f4" sourceRef="Join" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = loader().load(bpmn.as_bytes()).unwrap();
    let process = module.process("p1").unwrap();
    assert!(matches!(
        process.node("Fork").unwrap(),
        Node::ParallelGateway { .. }
    ));
    assert_eq!(process.outgoing("Fork").len(), 2);
    assert_eq!(process.incoming("Join").len(), 2);
}

#[test]
fn call_activity_loads_ignoring_the_dead_q_scope_attr() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="wrapper">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="C1" calledElement="orders" q:scope="common"/>
            <bpmn:callActivity id="C2" calledElement="local"/>
            <bpmn:endEvent id="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = loader().load(bpmn.as_bytes()).unwrap();
    let process = module.process("wrapper").unwrap();

    match process.node("C1").unwrap() {
        Node::CallActivity { called_element, .. } => {
            assert_eq!(called_element, "orders");
        }
        other => panic!("expected CallActivity, got {other:?}"),
    }
    match process.node("C2").unwrap() {
        Node::CallActivity { called_element, .. } => {
            assert_eq!(called_element, "local");
        }
        other => panic!("expected CallActivity, got {other:?}"),
    }
}

// ---- unsupported elements fail closed -----------------------------------------

fn process_body(body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p">
            {body}
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

#[test]
fn send_task_without_send_is_rejected() {
    let xml = process_body(
        r#"<bpmn:startEvent id="S"/>
           <bpmn:sendTask id="Send"/>
           <bpmn:endEvent id="E"/>
           <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Send"/>
           <bpmn:sequenceFlow id="f2" sourceRef="Send" targetRef="E"/>"#,
    );
    let e = loader().load(xml.as_bytes()).unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::PARSE_THROW_SEND_REQUIRED);
    assert!(e.message.contains("sendTask"), "{e}");
    assert!(e.message.contains("Send"), "{e}");
}

#[test]
/// Repurposed 2026-10-23: the gateway itself now loads, but this shape stays rejected for a
/// different and better reason — its single arm goes straight to an end event, which does not
/// park. An arm that does not wait resolves instantly and makes every other arm unreachable, so
/// a one-armed race is a modelling mistake rather than a race.
fn event_based_gateway_with_a_non_parking_arm_is_rejected() {
    let xml = process_body(
        r#"<bpmn:startEvent id="S"/>
           <bpmn:eventBasedGateway id="EBG"/>
           <bpmn:endEvent id="E"/>
           <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="EBG"/>
           <bpmn:sequenceFlow id="f2" sourceRef="EBG" targetRef="E"/>"#,
    );
    let e = loader().load(xml.as_bytes()).unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::CONFIG_BPMN_UNSUPPORTED_ELEMENT);
}

#[test]
fn an_inert_data_object_is_ignored_not_rejected() {
    let xml = process_body(
        r#"<bpmn:startEvent id="S"/>
           <bpmn:dataObject id="Payload"/>
           <bpmn:endEvent id="E"/>
           <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>"#,
    );
    let module = loader().load(xml.as_bytes()).unwrap();
    let process = module.process("p").unwrap();
    let mut ids: Vec<&str> = process.nodes().iter().map(|n| n.id()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["E", "S"]); // the dataObject is not an engine node
}

// ---- `<bpmn:import>` extraction -----------------------------------------------

#[test]
fn single_import_is_extracted() {
    let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          targetNamespace="https://example.com/orders/v1">
          <bpmn:import importType="http://www.omg.org/spec/BPMN/20100524/MODEL"
                       location="common/payments-shared.bpmn"
                       namespace="urn:sutra:common:payments"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;

    let module = loader().load(bpmn.as_bytes()).unwrap();
    assert_eq!(module.imports.len(), 1);
    let imp = &module.imports[0];
    assert_eq!(
        imp.import_type,
        "http://www.omg.org/spec/BPMN/20100524/MODEL"
    );
    assert_eq!(imp.namespace, "urn:sutra:common:payments");
    assert_eq!(imp.location, "common/payments-shared.bpmn");
}

#[test]
fn multiple_imports_are_preserved_in_declaration_order() {
    let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:import importType="http://www.omg.org/spec/BPMN/20100524/MODEL"
                       location="common/a.bpmn" namespace="urn:a"/>
          <bpmn:import importType="http://www.omg.org/spec/BPMN/20100524/MODEL"
                       location="common/b.bpmn" namespace="urn:b"/>
          <bpmn:import importType="http://www.omg.org/spec/DMN/20191111/MODEL/"
                       location="rules/c.dmn"  namespace="urn:c"/>
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;

    let module = loader().load(bpmn.as_bytes()).unwrap();
    let namespaces: Vec<&str> = module
        .imports
        .iter()
        .map(|i| i.namespace.as_str())
        .collect();
    assert_eq!(namespaces, vec!["urn:a", "urn:b", "urn:c"]);
    let locations: Vec<&str> = module.imports.iter().map(|i| i.location.as_str()).collect();
    assert_eq!(
        locations,
        vec!["common/a.bpmn", "common/b.bpmn", "rules/c.dmn"]
    );
}

#[test]
fn no_imports_yields_empty_list() {
    let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = loader().load(bpmn.as_bytes()).unwrap();
    assert!(module.imports.is_empty());
}

#[test]
fn call_activity_namespace_is_captured_from_qname_prefix() {
    let bpmn = r#"<?xml version="1.0" encoding="UTF-8"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:payments="urn:sutra:common:payments">
          <bpmn:import importType="http://www.omg.org/spec/BPMN/20100524/MODEL"
                       location="common/payments-shared.bpmn"
                       namespace="urn:sutra:common:payments"/>
          <bpmn:process id="parent">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="C" calledElement="payments:processBatchPayment"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
            <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;

    let module = loader().load(bpmn.as_bytes()).unwrap();
    match module.process("parent").unwrap().node("C").unwrap() {
        Node::CallActivity {
            called_element,
            called_namespace,
            ..
        } => {
            assert_eq!(called_element, "processBatchPayment");
            assert_eq!(
                called_namespace.as_deref(),
                Some("urn:sutra:common:payments")
            );
        }
        other => panic!("expected CallActivity, got {other:?}"),
    }
}

// ---- message-catch parse pins -------------------------------------------------

/// An `<intermediateCatchEvent>` with a `<q:source channel>` and a `<messageEventDefinition
/// messageRef>` parses to a [`Node::MessageCatchEvent`] carrying both — a wait state.
#[test]
fn message_catch_event_parses_with_channels_and_message_ref() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateCatchEvent id="C">
              <bpmn:extensionElements>
                <q:source channel="relay-in"/>
              </bpmn:extensionElements>
              <bpmn:messageEventDefinition messageRef="M"/>
            </bpmn:intermediateCatchEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
            <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = loader().load(bpmn.as_bytes()).unwrap();
    let process = module.process("p1").unwrap();
    let node = process.node("C").unwrap();
    match node {
        Node::MessageCatchEvent {
            channels,
            message_ref,
            ..
        } => {
            assert_eq!(channels, &vec!["relay-in".to_string()]);
            assert_eq!(message_ref.as_deref(), Some("M"));
        }
        other => panic!("expected MessageCatchEvent, got {other:?}"),
    }
    assert!(
        node.is_wait_state(),
        "a message catch event is a wait state"
    );
}

/// An intermediate catch that is neither a link nor timer catch and carries NO
/// `<messageEventDefinition>` fails closed (`SUTRA.PARSE.BPMN.UNSUPPORTED_CATCH_EVENT`).
/// NOTE: a `<timerEventDefinition>` catch IS a valid node here (it is the timer wait
/// state), so the fail-closed input uses a `<signalEventDefinition>` instead.
#[test]
fn a_non_message_intermediate_catch_is_rejected_fail_closed() {
    let xml = process_body(
        r#"<bpmn:startEvent id="S"/>
           <bpmn:intermediateCatchEvent id="C">
             <bpmn:signalEventDefinition/>
           </bpmn:intermediateCatchEvent>
           <bpmn:endEvent id="E"/>
           <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
           <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="E"/>"#,
    );
    let e = loader().load(xml.as_bytes()).unwrap_err();
    assert_eq!(
        e.code,
        sutra_bpmn::codes::PARSE_BPMN_UNSUPPORTED_CATCH_EVENT
    );
    assert!(e.message.contains("messageEventDefinition"), "{e}");
}

/// A `<bpmn:dataObject>` declared on the PROCESS is visible to a store write inside a nested
/// `<bpmn:subProcess>` — BPMN scopes a data object to its container *and everything nested in
/// it*, and the loader indexes each container separately, so the inherited half has to be
/// threaded down explicitly.
///
/// Regression: it was not. The nested association's `<sourceRef>doEntry</sourceRef>` resolved
/// against an index that held only the sub-process's own (empty) declarations, missed, and fell
/// back to the raw ELEMENT ID — so the write named a variable `doEntry` that nothing ever sets.
/// The key expression is FEEL over the live variables and kept resolving, which made the failure
/// maximally confusing at runtime: the store rejected the write for the *right* key with a null
/// value (`SUTRA.RUNTIME.DATASTORE.VALUE_NOT_A_RECORD`), pointing at the transform rather than at
/// the association that dropped it.
#[test]
fn a_sub_process_sees_the_parent_process_data_objects() {
    let xml = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:dataStore id="st" name="entries"/>
      <bpmn:process id="p">
        <bpmn:dataObject id="doEntry" name="entry"/>
        <bpmn:dataStoreReference id="dsr" name="entries" dataStoreRef="st">
          <bpmn:extensionElements><q:store key="entry.id"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:startEvent id="S"/>
        <bpmn:subProcess id="Loop">
          <bpmn:startEvent id="RS"/>
          <bpmn:serviceTask id="Persist">
            <bpmn:dataOutputAssociation>
              <bpmn:sourceRef>doEntry</bpmn:sourceRef>
              <bpmn:targetRef>dsr</bpmn:targetRef>
            </bpmn:dataOutputAssociation>
          </bpmn:serviceTask>
          <bpmn:endEvent id="RE"/>
          <bpmn:sequenceFlow id="r1" sourceRef="RS" targetRef="Persist"/>
          <bpmn:sequenceFlow id="r2" sourceRef="Persist" targetRef="RE"/>
        </bpmn:subProcess>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Loop"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Loop" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

    let module = loader().load(xml.as_bytes()).unwrap();
    let inner = module
        .process("p")
        .unwrap()
        .nodes()
        .iter()
        .find_map(|n| match n {
            Node::SubProcess { inner, .. } => Some(inner),
            _ => None,
        })
        .expect("sub-process");
    let write = inner
        .nodes()
        .iter()
        .find_map(|n| match n {
            Node::DataTask { data_mapping, .. } => data_mapping.store_writes.first(),
            _ => None,
        })
        .expect("the nested store write");
    assert_eq!(
        write.value_var, "entry",
        "resolved through the parent's <dataObject>"
    );
    assert_eq!(write.key_expression, "entry.id");
}

// ---- BPMN's untyped <task> and <implicitThrowEvent> -----------------------------------------
//
// Both are CONCRETE types in the BPMN 2.0 XSD (`tTask` and `tImplicitThrowEvent` are not
// abstract, unlike `tEvent`), so refusing them rejected conformant models and broke round trips
// from modelling tools. They now load onto the behaviour they already had: an untyped task has
// no behaviour to run, which is what `<manualTask>` is, and an implicit throw does what an
// intermediate throw does — the spec distinguishes how it is REACHED, not what it does.

const UNTYPED_TASK_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="c"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:task id="T"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:task>
    <bpmn:manualTask id="M"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing></bpmn:manualTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="M"/>
    <bpmn:sequenceFlow id="f3" sourceRef="M" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[test]
fn an_untyped_task_loads_as_a_pass_through_and_records_that_it_was_untyped() {
    let module = BpmnModelLoader::new()
        .load(UNTYPED_TASK_BPMN.as_bytes())
        .expect("an untyped <task> is conformant BPMN and must load");
    let process = &module.processes()[0];

    let spellings: Vec<(&str, bool)> = process
        .nodes()
        .iter()
        .filter_map(|n| match n {
            Node::ManualTask { id, untyped, .. } => Some((id.as_str(), *untyped)),
            _ => None,
        })
        .collect();

    // Both load onto the same variant — identical behaviour — but the authored spelling is kept,
    // because that is the only channel from the parse to lint's advisory (the loader has no
    // warning channel of its own).
    assert_eq!(
        spellings,
        vec![("T", true), ("M", false)],
        "both spellings load as a pass-through, and which one the author wrote is recorded"
    );
}

#[test]
fn an_implicit_throw_event_loads_as_an_intermediate_throw() {
    const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="c"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:implicitThrowEvent id="X"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:implicitThrowEvent>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="X"/>
    <bpmn:sequenceFlow id="f2" sourceRef="X" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

    let module = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect("<implicitThrowEvent> is conformant BPMN and must load");
    let process = &module.processes()[0];
    assert!(
        process
            .nodes()
            .iter()
            .any(|n| matches!(n, Node::IntermediateThrowEvent { id, .. } if id == "X")),
        "an implicit throw is an intermediate throw: {:?}",
        process.nodes().iter().map(|n| n.id()).collect::<Vec<_>>()
    );
}

/// `tEvent` IS `abstract="true"` in the BPMN 2.0 XSD — `<event>` is only the substitution-group
/// head, so no conformant document can use it as a flow node and there is nothing to implement.
/// Pinned so a later sweep over "unsupported elements" does not helpfully accept it.
#[test]
fn the_abstract_event_element_stays_rejected() {
    const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="c"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:event id="V"><bpmn:incoming>f1</bpmn:incoming></bpmn:event>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="V"/>
  </bpmn:process>
</bpmn:definitions>"#;

    let err = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect_err("<event> is the abstract base element, not a flow node");
    assert_eq!(err.code, sutra_bpmn::codes::CONFIG_BPMN_UNSUPPORTED_ELEMENT);
}

// ---- <receiveTask> --------------------------------------------------------------------------
//
// It parks on a message exactly as a message catch event does. The reason to support it anyway
// is that it is an ACTIVITY: a boundary event can attach, which is how BPMN says "wait for this
// message, or time out". An event cannot host a boundary, so the catch event can never express
// that shape — today the only route to it is a channel-call service task, which needs a request
// leg the model may not have.

#[test]
fn a_receive_task_parks_on_its_source_channel() {
    const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:receiveTask id="R" name="Await settlement" messageRef="Settled">
      <bpmn:extensionElements><q:source channel="responses"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="R"/>
    <bpmn:sequenceFlow id="f2" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

    let module = BpmnModelLoader::new().load(BPMN.as_bytes()).expect("loads");
    let process = &module.processes()[0];
    let node = process
        .nodes()
        .iter()
        .find(|n| n.id() == "R")
        .expect("the receive task is a flow node");

    match node {
        Node::ReceiveTask {
            name,
            channels,
            message_ref,
            ..
        } => {
            assert_eq!(name.as_deref(), Some("Await settlement"));
            assert_eq!(channels, &vec!["responses".to_string()]);
            assert_eq!(message_ref.as_deref(), Some("Settled"));
        }
        other => panic!("expected a ReceiveTask, got {other:?}"),
    }
    assert!(
        node.is_wait_state(),
        "a receive task parks — which also makes the process ineligible for execute_sync"
    );
}

/// The point of the variant: a timer boundary may attach, which a message catch event could
/// never host. Before this, the only way to model "wait or time out" was a channel-call service
/// task, which requires a request leg.
#[test]
fn a_timer_boundary_may_attach_to_a_receive_task() {
    const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:receiveTask id="R">
      <bpmn:extensionElements><q:source channel="responses"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:boundaryEvent id="B" attachedToRef="R">
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT30M</bpmn:timeDuration></bpmn:timerEventDefinition>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="T"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="R"/>
    <bpmn:sequenceFlow id="f2" sourceRef="R" targetRef="E"/>
    <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="T"/>
  </bpmn:process>
</bpmn:definitions>"#;

    BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect("a receive task is a wait-capable boundary host");
}

#[test]
fn a_receive_task_without_a_source_channel_is_rejected() {
    const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:receiveTask id="R"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:receiveTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="R"/>
    <bpmn:sequenceFlow id="f2" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

    // A park with no channel could never be satisfied — fail closed rather than load a process
    // that stops forever at R.
    let err = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect_err("a receive task must name the channel it waits on");
    assert_eq!(err.code, sutra_bpmn::codes::CONFIG_BPMN_UNSUPPORTED_ELEMENT);
    assert!(
        err.message.contains("q:source"),
        "the diagnostic must name what is missing: {}",
        err.message
    );
}

/// `@instantiate="true"` makes a receive task a process-INSTANTIATION point, an alternative to a
/// start event. That is a different contract — it needs the intake-selection surface a start
/// event has — so it is refused explicitly. Silently ignoring the attribute would load a model
/// that can never start.
#[test]
fn an_instantiating_receive_task_is_refused_explicitly() {
    const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:receiveTask id="R" instantiate="true">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:endEvent id="E"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="R" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

    let err = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect_err("an instantiating receive task is a start event's job");
    assert!(
        err.message.contains("instantiate") && err.message.contains("startEvent"),
        "the diagnostic must say what to model instead: {}",
        err.message
    );
}

// ---- non-interrupting timer boundaries, and recurring ones --------------------------------
//
// BPMN's escalate-while-waiting shape: the host keeps waiting and each firing sends a token down
// the boundary's path. Previously the loader refused `cancelActivity="false"` on a timer boundary
// outright, so the shape could not be modelled at all.

fn with_boundary(attrs: &str, timer_child: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:userTask id="H"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:userTask>
    <bpmn:boundaryEvent id="B" attachedToRef="H" {attrs}>
      <bpmn:timerEventDefinition>{timer_child}</bpmn:timerEventDefinition>
      <bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:boundaryEvent>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:endEvent id="T"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="H"/>
    <bpmn:sequenceFlow id="f2" sourceRef="H" targetRef="E"/>
    <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="T"/>
  </bpmn:process>
</bpmn:definitions>"#
    )
}

#[test]
fn a_non_interrupting_timer_boundary_loads() {
    let module = BpmnModelLoader::new()
        .load(
            with_boundary(
                r#"cancelActivity="false""#,
                "<bpmn:timeDuration>PT30M</bpmn:timeDuration>",
            )
            .as_bytes(),
        )
        .expect("cancelActivity=\"false\" on a timer boundary is now a supported form");
    let process = &module.processes()[0];
    let b = process
        .nodes()
        .iter()
        .find(|n| n.id() == "B")
        .expect("boundary");
    assert!(
        matches!(
            b,
            Node::BoundaryEvent {
                interrupting: false,
                ..
            }
        ),
        "the flag must reach the model — the executor branches on it: {b:?}"
    );
}

#[test]
fn an_unbounded_cycle_is_allowed_on_a_non_interrupting_boundary() {
    BpmnModelLoader::new()
        .load(
            with_boundary(
                r#"cancelActivity="false""#,
                "<bpmn:timeCycle>R/PT2H</bpmn:timeCycle>",
            )
            .as_bytes(),
        )
        .expect("the escalate-every-N-hours shape");
}

/// A bounded `R<n>` would need its remaining budget to survive the host's park, which means
/// durable per-instance state (the shape `retry_attempts` has) and a snapshot format change.
/// Refused loudly rather than silently firing forever on a spec that asked for three.
#[test]
fn a_bounded_cycle_is_refused_on_a_boundary() {
    let err = BpmnModelLoader::new()
        .load(
            with_boundary(
                r#"cancelActivity="false""#,
                "<bpmn:timeCycle>R3/PT2H</bpmn:timeCycle>",
            )
            .as_bytes(),
        )
        .expect_err("a repeat budget cannot survive the park yet");
    assert_eq!(err.code, sutra_bpmn::codes::DISPATCH_TIMER_UNSUPPORTED);
    assert!(
        err.message.contains("R/<interval>"),
        "the diagnostic must name the supported form: {}",
        err.message
    );
}

/// An INTERRUPTING cycle stays rejected, and for a reason the diagnostic now states: the first
/// firing cancels the host, so later occurrences could never be reached.
#[test]
fn a_cycle_on_an_interrupting_boundary_is_still_rejected() {
    let err = BpmnModelLoader::new()
        .load(with_boundary("", "<bpmn:timeCycle>R/PT2H</bpmn:timeCycle>").as_bytes())
        .expect_err("an interrupting boundary fires once by construction");
    assert_eq!(err.code, sutra_bpmn::codes::DISPATCH_TIMER_UNSUPPORTED);
    assert!(
        err.message.contains("INTERRUPTING"),
        "the diagnostic must distinguish the two boundary kinds: {}",
        err.message
    );
}

/// And on an intermediate catch, where the token parks exactly once.
#[test]
fn a_cycle_on_an_intermediate_catch_is_still_rejected() {
    const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="C">
      <bpmn:timerEventDefinition><bpmn:timeCycle>R/PT2H</bpmn:timeCycle></bpmn:timerEventDefinition>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
    <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
    let err = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect_err("a token parks at a catch exactly once");
    assert_eq!(err.code, sutra_bpmn::codes::DISPATCH_TIMER_UNSUPPORTED);
}

// ---- <eventBasedGateway> — the race -------------------------------------------------------
//
// The gateway does not park: it arms its TARGETS, which are already wait states, so one token
// enters and one leaves. First-wins is enforced at the resume boundary by retiring the losing
// arms in the same transaction that advances the winner — see `resolved_waits_for` in
// sutra-channels. The grouping is static model structure (the gateway's outgoing targets), so
// nothing has to survive the park.

fn race_process(arms: &str, flows: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f0</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:eventBasedGateway id="G"><bpmn:incoming>f0</bpmn:incoming></bpmn:eventBasedGateway>
    {arms}
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="G"/>
    {flows}
  </bpmn:process>
</bpmn:definitions>"#
    )
}

/// The canonical race: a message arm against a timer arm — "respond, or time out".
const MESSAGE_VS_TIMER: (&str, &str) = (
    r#"<bpmn:intermediateCatchEvent id="A">
         <bpmn:extensionElements><q:source channel="responses"/></bpmn:extensionElements>
         <bpmn:messageEventDefinition/>
       </bpmn:intermediateCatchEvent>
       <bpmn:intermediateCatchEvent id="B">
         <bpmn:timerEventDefinition><bpmn:timeDuration>PT30M</bpmn:timeDuration></bpmn:timerEventDefinition>
       </bpmn:intermediateCatchEvent>"#,
    r#"<bpmn:sequenceFlow id="f1" sourceRef="G" targetRef="A"/>
       <bpmn:sequenceFlow id="f2" sourceRef="G" targetRef="B"/>
       <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="E"/>
       <bpmn:sequenceFlow id="f4" sourceRef="B" targetRef="E"/>"#,
);

#[test]
fn an_event_based_gateway_races_a_message_against_a_timer() {
    let module = BpmnModelLoader::new()
        .load(race_process(MESSAGE_VS_TIMER.0, MESSAGE_VS_TIMER.1).as_bytes())
        .expect("message-vs-timer is the canonical race");
    let process = &module.processes()[0];
    let g = process
        .nodes()
        .iter()
        .find(|n| n.id() == "G")
        .expect("gateway");
    assert!(matches!(g, Node::EventBasedGateway { .. }));
    assert!(
        !g.is_wait_state(),
        "the gateway arms its targets; the TARGETS park, so it must not claim to"
    );
    // Statefulness has to come from the arms, or the process would be run synchronously.
    assert!(
        !process.is_sync_eligible(),
        "a process containing a race is stateful via its arms"
    );
}

#[test]
fn a_receive_task_is_a_valid_race_arm() {
    let arms = r#"<bpmn:receiveTask id="A">
         <bpmn:extensionElements><q:source channel="responses"/></bpmn:extensionElements>
       </bpmn:receiveTask>
       <bpmn:intermediateCatchEvent id="B">
         <bpmn:timerEventDefinition><bpmn:timeDuration>PT5M</bpmn:timeDuration></bpmn:timerEventDefinition>
       </bpmn:intermediateCatchEvent>"#;
    BpmnModelLoader::new()
        .load(race_process(arms, MESSAGE_VS_TIMER.1).as_bytes())
        .expect("BPMN allows a receiveTask as a race arm, and it parks");
}

/// A one-armed race is not a race. Caught early because the diagnostic can say something
/// useful — park on the catch event directly.
#[test]
fn a_single_armed_race_is_rejected() {
    let arms = r#"<bpmn:intermediateCatchEvent id="A">
         <bpmn:extensionElements><q:source channel="responses"/></bpmn:extensionElements>
         <bpmn:messageEventDefinition/>
       </bpmn:intermediateCatchEvent>"#;
    let flows = r#"<bpmn:sequenceFlow id="f1" sourceRef="G" targetRef="A"/>
       <bpmn:sequenceFlow id="f3" sourceRef="A" targetRef="E"/>"#;
    let err = BpmnModelLoader::new()
        .load(race_process(arms, flows).as_bytes())
        .expect_err("one arm is not a race");
    assert!(
        err.message.contains("at least two"),
        "the diagnostic must say what is wrong: {}",
        err.message
    );
}

/// `Parallel` is only meaningful with `instantiate="true"`, so it is refused explicitly rather
/// than silently treated as `Exclusive` — which would turn "wait for all" into "wait for one".
#[test]
fn a_parallel_event_gateway_is_refused() {
    let p = race_process(MESSAGE_VS_TIMER.0, MESSAGE_VS_TIMER.1).replace(
        r#"<bpmn:eventBasedGateway id="G">"#,
        r#"<bpmn:eventBasedGateway id="G" eventGatewayType="Parallel">"#,
    );
    let err = BpmnModelLoader::new()
        .load(p.as_bytes())
        .expect_err("Parallel is a different contract");
    assert!(
        err.message.contains("Exclusive"),
        "the diagnostic must name the supported flavour: {}",
        err.message
    );
}

#[test]
fn an_instantiating_event_gateway_is_refused() {
    let p = race_process(MESSAGE_VS_TIMER.0, MESSAGE_VS_TIMER.1).replace(
        r#"<bpmn:eventBasedGateway id="G">"#,
        r#"<bpmn:eventBasedGateway id="G" instantiate="true">"#,
    );
    let err = BpmnModelLoader::new()
        .load(p.as_bytes())
        .expect_err("an instantiating race is a start event's job");
    assert!(err.message.contains("startEvent"), "{}", err.message);
}
