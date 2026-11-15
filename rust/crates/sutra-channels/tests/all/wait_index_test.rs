//! The per-deployment wait index the dispatcher consults before any correlation: which channels a
//! wait subscribes to, and which message types those waits accept. It must give the relay's own
//! answer (`resolve_relay_targets` non-empty, predicates aside) in every type tier.

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::ProcessModuleRegistry;
use sutra_executor::DeploymentId;
use sutra_feel::FeelContext;

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000a1").expect("valid deployment id")
}

fn registry(bpmn: &str) -> ProcessModuleRegistry {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let mut processes = ProcessModuleRegistry::new();
    processes.register(&deployment(), &module);
    processes
}

const WAITS: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="d" targetNamespace="urn:t">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="start-in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Fork"/>
    <bpmn:parallelGateway id="Fork">
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>a</bpmn:outgoing><bpmn:outgoing>b</bpmn:outgoing><bpmn:outgoing>c</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:sequenceFlow id="a" sourceRef="Fork" targetRef="Exact"/>
    <bpmn:receiveTask id="Exact">
      <bpmn:extensionElements><q:source channel="typed-in" messageTypeValue="Answer"/></bpmn:extensionElements>
      <bpmn:incoming>a</bpmn:incoming><bpmn:outgoing>a2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="b" sourceRef="Fork" targetRef="Pattern"/>
    <bpmn:receiveTask id="Pattern">
      <bpmn:extensionElements><q:source channel="typed-in" messageTypePattern="Status\..*"/></bpmn:extensionElements>
      <bpmn:incoming>b</bpmn:incoming><bpmn:outgoing>b2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="c" sourceRef="Fork" targetRef="Any"/>
    <bpmn:receiveTask id="Any">
      <bpmn:extensionElements><q:source channel="any-in"/></bpmn:extensionElements>
      <bpmn:incoming>c</bpmn:incoming><bpmn:outgoing>c2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="a2" sourceRef="Exact" targetRef="Join"/>
    <bpmn:sequenceFlow id="b2" sourceRef="Pattern" targetRef="Join"/>
    <bpmn:sequenceFlow id="c2" sourceRef="Any" targetRef="Join"/>
    <bpmn:parallelGateway id="Join">
      <bpmn:incoming>a2</bpmn:incoming><bpmn:incoming>b2</bpmn:incoming><bpmn:incoming>c2</bpmn:incoming>
      <bpmn:outgoing>e</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:sequenceFlow id="e" sourceRef="Join" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>e</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

#[test]
fn wait_index_matches_the_three_type_tiers() {
    let processes = registry(WAITS);
    let d = deployment();
    let relay = |channel: &str, mt: Option<&str>| {
        !processes
            .resolve_relay_targets(&d, channel, mt, &FeelContext::new())
            .is_empty()
    };
    // A start-only channel has no wait: the fast path.
    assert!(!processes.waits_on_channel(&d, "start-in"));
    assert!(processes.waits_on_channel(&d, "typed-in"));
    assert!(processes.waits_on_channel(&d, "any-in"));
    assert!(!processes.waits_on_channel(&d, "nowhere"));
    assert!(!processes.waits_on_channel(
        &DeploymentId::of("dep-0000000000000000000000b2").unwrap(),
        "typed-in"
    ));
    for (channel, mt) in [
        ("typed-in", Some("Answer")),       // exact
        ("typed-in", Some("Status.Done")),  // pattern
        ("typed-in", Some("Other")),        // neither
        ("typed-in", None),                 // a codec that reports no type
        ("typed-in", Some("XStatus.Done")), // the pattern is anchored
        ("any-in", None),                   // untyped wait
        ("any-in", Some("Whatever")),       // untyped wait
        ("start-in", Some("Answer")),       // no wait at all
    ] {
        assert_eq!(
            processes.wait_may_match(&d, channel, mt),
            relay(channel, mt),
            "{channel} {mt:?}"
        );
    }
    assert!(processes.wait_may_match(&d, "typed-in", Some("Answer")));
    assert!(processes.wait_may_match(&d, "typed-in", Some("Status.Done")));
    assert!(!processes.wait_may_match(&d, "typed-in", Some("Other")));
    assert!(!processes.wait_may_match(&d, "typed-in", None));
}
