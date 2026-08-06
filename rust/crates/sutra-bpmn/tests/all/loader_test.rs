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
fn event_based_gateway_is_also_rejected() {
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
