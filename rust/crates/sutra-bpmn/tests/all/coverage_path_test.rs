//! Path-coverage P1 (static): the loader parses
//! `<q:coverage path flows>` into `ProcessDefinition::coverage_paths` and fail-closes on a
//! path referencing an unknown flow, a non-contiguous route, or a duplicate path id.

use sutra_bpmn::{codes, BpmnModelLoader};

fn bpmn(coverage: &str) -> Vec<u8> {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p">
            <bpmn:extensionElements>
        {coverage}
            </bpmn:extensionElements>
            <bpmn:startEvent id="S"/>
            <bpmn:exclusiveGateway id="G" default="f2"/>
            <bpmn:endEvent id="EOk"/>
            <bpmn:endEvent id="ERej"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="G"/>
            <bpmn:sequenceFlow id="f2" sourceRef="G" targetRef="EOk"/>
            <bpmn:sequenceFlow id="f3" sourceRef="G" targetRef="ERej">
              <bpmn:conditionExpression>reject</bpmn:conditionExpression>
            </bpmn:sequenceFlow>
          </bpmn:process>
        </bpmn:definitions>"#
    )
    .into_bytes()
}

#[test]
fn parses_declared_coverage_paths() {
    let module = BpmnModelLoader::new()
        .load(&bpmn(
            r#"<q:coverage path="accept" flows="f1 f2"/>
               <q:coverage path="reject" flows="f1 f3"/>"#,
        ))
        .unwrap();
    let process = module.process("p").unwrap();

    let ids: Vec<&str> = process
        .coverage_paths
        .iter()
        .map(|p| p.id.as_str())
        .collect();
    assert_eq!(ids, vec!["accept", "reject"]);
    assert_eq!(process.coverage_paths[0].flows, vec!["f1", "f2"]);
    assert_eq!(process.coverage_paths[1].flows, vec!["f1", "f3"]);
}

#[test]
fn no_coverage_means_empty() {
    let module = BpmnModelLoader::new().load(&bpmn("")).unwrap();
    assert!(module.process("p").unwrap().coverage_paths.is_empty());
}

#[test]
fn unknown_flow_is_rejected() {
    let e = BpmnModelLoader::new()
        .load(&bpmn(r#"<q:coverage path="x" flows="f1 f9"/>"#))
        .unwrap_err();
    assert_eq!(e.code, codes::CONFIG_COVERAGE_UNKNOWN_FLOW);
}

#[test]
fn non_contiguous_route_is_rejected() {
    // f2 ends at EOk, f3 starts at G — not contiguous.
    let e = BpmnModelLoader::new()
        .load(&bpmn(r#"<q:coverage path="x" flows="f2 f3"/>"#))
        .unwrap_err();
    assert_eq!(e.code, codes::CONFIG_COVERAGE_INVALID_ROUTE);
}

#[test]
fn duplicate_path_id_is_rejected() {
    let e = BpmnModelLoader::new()
        .load(&bpmn(
            r#"<q:coverage path="dup" flows="f1 f2"/>
               <q:coverage path="dup" flows="f1 f3"/>"#,
        ))
        .unwrap_err();
    assert_eq!(e.code, codes::CONFIG_COVERAGE_DUPLICATE_PATH);
}
