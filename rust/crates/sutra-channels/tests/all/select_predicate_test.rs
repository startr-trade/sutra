//! `<q:source filterExpression>` — selecting a start event by the message ATTRIBUTES a codec
//! reported, not just by its message type.
//!
//! The case these exist for: one message type that arrives in more than one flavour, where the
//! flavour decides which process handles it. A single `messageTypeValue` string cannot express
//! that, so two subscribers would be an ambiguous handler.
//!
//! Registry-level on purpose. Selection happens in `resolve_handlers`, so driving a whole engine
//! would test the dispatcher's plumbing rather than the predicate semantics these pin.

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::ProcessModuleRegistry;
use sutra_executor::DeploymentId;
use sutra_feel::{FeelContext, FeelValue};

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000f9").expect("valid deployment id")
}

fn registry(bpmn: &str) -> ProcessModuleRegistry {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let mut processes = ProcessModuleRegistry::new();
    processes.register(&deployment(), &module);
    processes
}

/// The selector context the dispatcher builds: codec attributes under `attr`, plus `messageType`.
fn context(attributes: &[(&str, &str)], message_type: Option<&str>) -> FeelContext {
    let mut attr = std::collections::BTreeMap::new();
    for (k, v) in attributes {
        attr.insert(k.to_string(), FeelValue::String(v.to_string()));
    }
    let mut ctx = FeelContext::new();
    ctx.insert("attr".to_string(), FeelValue::Map(attr));
    ctx.insert(
        "messageType".to_string(),
        message_type
            .map(|m| FeelValue::String(m.to_string()))
            .unwrap_or(FeelValue::Null),
    );
    ctx
}

fn start_events(processes: &ProcessModuleRegistry, ctx: &FeelContext) -> Vec<String> {
    processes
        .resolve_handlers(&deployment(), "rail-in", Some("Transfer"), ctx)
        .handlers
        .iter()
        .map(|h| format!("{}/{}", h.process.id, h.start_event_id))
        .collect()
}

/// Two PROCESSES subscribing to the same channel and the same message type, told apart only by
/// the direction attribute — plus a third with no predicate, to prove a false predicate falls
/// through rather than swallowing the message.
const TWO_PROCESSES: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="inbound-handler">
    <bpmn:startEvent id="S_in">
      <bpmn:extensionElements>
        <q:source channel="rail-in" messageTypeValue="Transfer"
                  filterExpression="attr.direction = &quot;incoming&quot;"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:endEvent id="E1"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S_in" targetRef="E1"/>
  </bpmn:process>
  <bpmn:process id="outbound-handler">
    <bpmn:startEvent id="S_out">
      <bpmn:extensionElements>
        <q:source channel="rail-in" messageTypeValue="Transfer"
                  filterExpression="attr.direction = &quot;outgoing&quot;"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="f2" sourceRef="S_out" targetRef="E2"/>
  </bpmn:process>
</bpmn:definitions>"#;

#[test]
fn a_predicate_routes_one_message_type_to_the_right_process() {
    let processes = registry(TWO_PROCESSES);

    assert_eq!(
        start_events(
            &processes,
            &context(&[("direction", "incoming")], Some("Transfer"))
        ),
        vec!["inbound-handler/S_in"]
    );
    assert_eq!(
        start_events(
            &processes,
            &context(&[("direction", "outgoing")], Some("Transfer"))
        ),
        vec!["outbound-handler/S_out"]
    );
}

#[test]
fn no_predicate_matching_yields_no_handler() {
    // Fail closed: an attribute value nobody subscribes to must not fall back to "any handler".
    let processes = registry(TWO_PROCESSES);
    assert!(start_events(
        &processes,
        &context(&[("direction", "sideways")], Some("Transfer"))
    )
    .is_empty());
}

#[test]
fn an_absent_attribute_reads_as_null_and_matches_nothing() {
    // The dispatcher passes an EMPTY context for a FATAL decode, so this is also the posture a
    // message whose codec could not classify it gets: comparisons against null are false.
    let processes = registry(TWO_PROCESSES);
    assert!(start_events(&processes, &FeelContext::new()).is_empty());
    assert!(start_events(&processes, &context(&[], Some("Transfer"))).is_empty());
}

#[test]
fn the_predicate_can_read_the_message_type_too() {
    const ON_TYPE: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="rail-in"
                  filterExpression="starts with(messageType, &quot;Trans&quot;)"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
    let processes = registry(ON_TYPE);
    assert_eq!(
        start_events(&processes, &context(&[], Some("Transfer"))),
        vec!["p/S"]
    );
    let other = processes.resolve_handlers(
        &deployment(),
        "rail-in",
        Some("Receipt"),
        &context(&[], Some("Receipt")),
    );
    assert!(other.handlers.is_empty());
}

#[test]
fn two_matching_starts_in_one_process_are_both_resolved() {
    // Previously a start event was resolved per PROCESS, so two matching starts inside one
    // process resolved silently by document order and the dispatcher's ambiguity check never
    // saw them. Predicates make this configuration ordinary, so both must surface.
    const SAME_PROCESS: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S_a">
      <bpmn:extensionElements>
        <q:source channel="rail-in" messageTypeValue="Transfer"
                  filterExpression="attr.direction = &quot;incoming&quot;"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>fa</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:startEvent id="S_b">
      <bpmn:extensionElements>
        <q:source channel="rail-in" messageTypeValue="Transfer"
                  filterExpression="attr.urgency = &quot;high&quot;"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>fb</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="fa" sourceRef="S_a" targetRef="E"/>
    <bpmn:sequenceFlow id="fb" sourceRef="S_b" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
    let processes = registry(SAME_PROCESS);

    // Overlapping predicates: a message that is both incoming AND high-urgency matches both.
    let both = start_events(
        &processes,
        &context(
            &[("direction", "incoming"), ("urgency", "high")],
            Some("Transfer"),
        ),
    );
    assert_eq!(both, vec!["p/S_a", "p/S_b"]);

    // Disjoint in practice: only one holds.
    let one = start_events(
        &processes,
        &context(
            &[("direction", "incoming"), ("urgency", "low")],
            Some("Transfer"),
        ),
    );
    assert_eq!(one, vec!["p/S_a"]);
}

/// Builds a one-start-event module whose `<q:source>` carries the given attributes verbatim.
fn single(source_attrs: &str) -> ProcessModuleRegistry {
    registry(&format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="rail-in" {source_attrs}/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
    ))
}

#[test]
fn an_evaluation_error_fails_closed_and_records_a_fault() {
    // A relational operator against a text attribute is a FEEL type error. The start event must
    // not be selected, and the operator must be told why — a dropped message with no trace is
    // the failure this records.
    let processes = single(r#"filterExpression="attr.direction > 5""#);
    let resolution = processes.resolve_handlers(
        &deployment(),
        "rail-in",
        Some("Transfer"),
        &context(&[("direction", "incoming")], Some("Transfer")),
    );
    assert!(resolution.handlers.is_empty());
    assert_eq!(resolution.predicate_faults.len(), 1);
    let (process_id, node_id, code, _detail) = &resolution.predicate_faults[0];
    assert_eq!(process_id, "p");
    assert_eq!(node_id, "S");
    assert_eq!(code, "SUTRA.INBOUND.SELECT_PREDICATE_FAILED");
}

#[test]
fn an_unparseable_predicate_is_a_selector_fault_and_matches_nothing() {
    // Caught once at activation, not per message. Critically it must match NOTHING: treating a
    // broken filter as "no filter" would make the start event match everything.
    let processes = single(r#"filterExpression="attr.direction = ""#);
    let faults = processes.selector_faults();
    assert_eq!(faults.len(), 1, "{faults:?}");
    assert_eq!((faults[0].0.as_str(), faults[0].1.as_str()), ("p", "S"));

    let resolution = processes.resolve_handlers(
        &deployment(),
        "rail-in",
        Some("Transfer"),
        &context(&[("direction", "incoming")], Some("Transfer")),
    );
    assert!(resolution.handlers.is_empty());
    assert!(!resolution.predicate_faults.is_empty());
}

#[test]
fn a_non_deterministic_predicate_is_a_selector_fault() {
    // Routing that consults the clock cannot be replayed, so it is refused at compile time
    // rather than producing a deployment whose routing drifts.
    let processes = single(r#"filterExpression="attr.direction = string(today())""#);
    let faults = processes.selector_faults();
    assert_eq!(faults.len(), 1, "{faults:?}");
    assert!(
        faults[0].2.contains("DETERMINISM"),
        "expected a determinism code, got {:?}",
        faults[0].2
    );
}

#[test]
fn a_malformed_message_type_pattern_is_a_selector_fault() {
    // It used to be skipped in silence at dispatch, matching nothing — indistinguishable from a
    // channel with no traffic.
    let processes = single(r#"messageTypePattern="Trans[""#);
    let faults = processes.selector_faults();
    assert_eq!(faults.len(), 1, "{faults:?}");
    assert_eq!(faults[0].2, "SUTRA.RESOLVE.SELECTOR_COMPILE_FAILED");
}

#[test]
fn a_well_formed_selector_reports_no_fault() {
    let processes = single(
        r#"messageTypePattern="Trans.*" filterExpression="attr.direction = &quot;incoming&quot;""#,
    );
    assert!(processes.selector_faults().is_empty());
    assert_eq!(
        start_events(
            &processes,
            &context(&[("direction", "incoming")], Some("Transfer"))
        ),
        vec!["p/S"]
    );
}

#[test]
fn an_unpredicated_start_event_is_unaffected() {
    // The predicate is purely additive: a source without one keeps matching on message type
    // alone, whatever the attribute context says.
    let processes = single(r#"messageTypeValue="Transfer""#);
    assert!(processes.selector_faults().is_empty());
    assert_eq!(
        start_events(&processes, &FeelContext::new()),
        vec!["p/S"],
        "an empty context must not disturb a source with no predicate"
    );
}

// ---- the relay path honours a wait node's predicate ------------------------------------

/// `<q:source>` is legal on a wait node too, where it names the channel a correlated response
/// arrives on. A predicate there must be honoured, not silently ignored on half its legal hosts.
const WAIT_NODE: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="kickoff"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements>
        <q:source channel="rail-in"
                  filterExpression="attr.direction = &quot;incoming&quot;"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="W"/>
    <bpmn:sequenceFlow id="f2" sourceRef="W" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn relay_nodes(processes: &ProcessModuleRegistry, ctx: &FeelContext) -> Vec<String> {
    processes
        .resolve_relay_targets(&deployment(), "rail-in", None, ctx)
        .iter()
        .map(|t| t.wait_node_id.clone())
        .collect()
}

#[test]
fn the_relay_path_honours_a_wait_node_predicate() {
    let processes = registry(WAIT_NODE);
    assert!(processes.selector_faults().is_empty());

    assert_eq!(
        relay_nodes(&processes, &context(&[("direction", "incoming")], None)),
        vec!["W"]
    );
}

#[test]
fn the_relay_path_skips_a_wait_node_whose_predicate_is_false() {
    let processes = registry(WAIT_NODE);
    assert!(relay_nodes(&processes, &context(&[("direction", "outgoing")], None)).is_empty());
    // And the fatal-decode posture: an empty context selects on channel alone, so a predicated
    // wait node is skipped rather than resuming on a message its codec could not classify.
    assert!(relay_nodes(&processes, &FeelContext::new()).is_empty());
}

#[test]
fn a_wait_node_without_a_predicate_still_relays() {
    const PLAIN_WAIT: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="kickoff"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="W">
      <bpmn:extensionElements>
        <q:source channel="rail-in"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="W"/>
    <bpmn:sequenceFlow id="f2" sourceRef="W" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;
    let processes = registry(PLAIN_WAIT);
    assert_eq!(relay_nodes(&processes, &FeelContext::new()), vec!["W"]);
}

// ---- the relay path honours a wait node's declared message type (0.6.10) ------------------

/// Three waits on ONE channel, forked in parallel: one pinned to `Answer`, one pinned by
/// pattern to `Cancel.*`, one with no message type. A relay must reach only the waits whose
/// declared type admits it, most specific first.
const TYPED_WAITS: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="kickoff"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:parallelGateway id="Fork"/>
    <bpmn:intermediateCatchEvent id="WAnswer">
      <bpmn:extensionElements>
        <q:source channel="shared-in" messageTypeValue="Answer"/>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="WCancel">
      <bpmn:extensionElements>
        <q:source channel="shared-in" messageTypePattern="Cancel.*"/>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:intermediateCatchEvent id="WAny">
      <bpmn:extensionElements>
        <q:source channel="shared-in"/>
      </bpmn:extensionElements>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E1"/>
    <bpmn:endEvent id="E2"/>
    <bpmn:endEvent id="E3"/>
    <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="Fork"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Fork" targetRef="WAnswer"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Fork" targetRef="WCancel"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Fork" targetRef="WAny"/>
    <bpmn:sequenceFlow id="f4" sourceRef="WAnswer" targetRef="E1"/>
    <bpmn:sequenceFlow id="f5" sourceRef="WCancel" targetRef="E2"/>
    <bpmn:sequenceFlow id="f6" sourceRef="WAny" targetRef="E3"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn typed_relay_nodes(message_type: Option<&str>) -> Vec<String> {
    registry(TYPED_WAITS)
        .resolve_relay_targets(
            &deployment(),
            "shared-in",
            message_type,
            &context(&[], message_type),
        )
        .iter()
        .map(|t| t.wait_node_id.clone())
        .collect()
}

#[test]
fn a_relay_reaches_only_the_waits_its_message_type_admits_most_specific_first() {
    assert_eq!(typed_relay_nodes(Some("Answer")), vec!["WAnswer", "WAny"]);
    assert_eq!(
        typed_relay_nodes(Some("CancelRequest")),
        vec!["WCancel", "WAny"]
    );
    assert_eq!(typed_relay_nodes(Some("Other")), vec!["WAny"]);
}

#[test]
fn a_typeless_relay_never_reaches_a_wait_pinned_to_a_type() {
    // A codec that reports no message type cannot satisfy a declared type — the start-event
    // rule — so only the untyped wait remains.
    assert_eq!(typed_relay_nodes(None), vec!["WAny"]);
}
