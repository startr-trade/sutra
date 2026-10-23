//! Process-level intake-contract inheritance (P3), `<q:variable>` source validation
//! (T4-2), and start-event selection.

use sutra_bpmn::qbindings::OnValidationMode;
use sutra_bpmn::{codes, BpmnModelLoader, Node};

// ---- process-level contract inheritance -----------------------------------------

#[test]
fn two_start_events_inherit_the_process_level_contract() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:extensionElements>
              <q:validators><q:complexValidator source="business.srl"/></q:validators>
              <q:onValidation mode="route" errorCode="T505"/>
              <q:alias name="endToEndId" expression="payload.body.PmtId.EndToEndId" onConflict="correlate"/>
              <q:alias name="creditorAccount" expression="payload.body.CdtrAcct.Id"/>
            </bpmn:extensionElements>
            <bpmn:startEvent id="StartHttp">
              <bpmn:extensionElements>
                <q:source channel="orders-http-in" messageTypeValue="order.created.001.14"/>
              </bpmn:extensionElements>
              <bpmn:outgoing>f1</bpmn:outgoing>
            </bpmn:startEvent>
            <bpmn:startEvent id="StartQueue">
              <bpmn:extensionElements>
                <q:source channel="orders-queue-in" messageTypeValue="order.created.001.14"/>
              </bpmn:extensionElements>
              <bpmn:outgoing>f2</bpmn:outgoing>
            </bpmn:startEvent>
            <bpmn:endEvent id="E1"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
            <bpmn:endEvent id="E2"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="StartHttp" targetRef="E1"/>
            <bpmn:sequenceFlow id="f2" sourceRef="StartQueue" targetRef="E2"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = BpmnModelLoader::new().load(bpmn.as_bytes()).unwrap();
    let process = module.process("p1").unwrap();

    for start_id in ["StartHttp", "StartQueue"] {
        let b = process.bindings_for(start_id);
        // validators inherited
        assert_eq!(
            b.sources[0].complex_validators,
            vec!["business.srl"],
            "{start_id} inherits the process validators"
        );
        // onValidation inherited
        let ov = b.on_validation.as_ref().expect("onValidation inherited");
        assert_eq!(ov.error_code.as_deref(), Some("T505"));
        // aliases inherited
        let mut names: Vec<&str> = b.aliases.iter().map(|a| a.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["creditorAccount", "endToEndId"]);
        // transport binding stays per-source
        assert_eq!(
            b.sources[0].message_type_value.as_deref(),
            Some("order.created.001.14")
        );
    }
    assert_eq!(
        process.bindings_for("StartHttp").sources[0].channel,
        "orders-http-in"
    );
    assert_eq!(
        process.bindings_for("StartQueue").sources[0].channel,
        "orders-queue-in"
    );
}

#[test]
fn start_event_own_declarations_override_and_aliases_union_with_start_winning() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:extensionElements>
              <q:validators><q:complexValidator source="shared.srl"/></q:validators>
              <q:onValidation mode="route" errorCode="SHARED"/>
              <q:alias name="endToEndId" expression="payload.body.shared"/>
              <q:alias name="msgId" expression="payload.body.MsgId"/>
            </bpmn:extensionElements>
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="c">
                  <q:validators><q:complexValidator source="own.srl"/></q:validators>
                </q:source>
                <q:onValidation mode="reject" errorCode="OWN"/>
                <q:alias name="endToEndId" expression="payload.body.own"/>
              </bpmn:extensionElements>
              <bpmn:outgoing>f1</bpmn:outgoing>
            </bpmn:startEvent>
            <bpmn:endEvent id="E"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = BpmnModelLoader::new().load(bpmn.as_bytes()).unwrap();
    let process = module.process("p1").unwrap();

    let b = process.bindings_for("Start");
    // validators: the source declared its own block → NOT inherited (block is a unit)
    assert_eq!(b.sources[0].complex_validators, vec!["own.srl"]);
    // onValidation: the node declared its own → override
    let ov = b.on_validation.as_ref().unwrap();
    assert_eq!(ov.mode, OnValidationMode::Reject);
    assert_eq!(ov.error_code.as_deref(), Some("OWN"));
    // aliases: union, node wins on the endToEndId collision, msgId inherited from process
    let mut names: Vec<&str> = b.aliases.iter().map(|a| a.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["endToEndId", "msgId"]);
    let e2e = b.aliases.iter().find(|a| a.name == "endToEndId").unwrap();
    assert_eq!(
        e2e.expression, "payload.body.own",
        "start-event alias wins on collision"
    );
}

#[test]
fn no_process_contract_leaves_start_event_unchanged() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements><q:source channel="c"/></bpmn:extensionElements>
              <bpmn:outgoing>f1</bpmn:outgoing>
            </bpmn:startEvent>
            <bpmn:endEvent id="E"><bpmn:incoming>f1</bpmn:incoming></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = BpmnModelLoader::new().load(bpmn.as_bytes()).unwrap();
    let process = module.process("p1").unwrap();
    let b = process.bindings_for("Start");
    assert!(b.sources[0].complex_validators.is_empty());
    assert!(b.on_validation.is_none());
    assert!(b.aliases.is_empty());
}

// ---- `<q:variable>` source validation (T4-2) -----------------------------------

fn variable_bpmn(variable_decls: &str, start_source_channel: &str) -> Vec<u8> {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p">
            <bpmn:extensionElements>
              <q:variables>
        {variable_decls}
              </q:variables>
            </bpmn:extensionElements>
            <bpmn:startEvent id="S">
              <bpmn:extensionElements><q:source channel="{start_source_channel}"/></bpmn:extensionElements>
              <bpmn:outgoing>f</bpmn:outgoing>
            </bpmn:startEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
    .into_bytes()
}

#[test]
fn a_variable_sourced_from_a_real_intake_channel_loads() {
    let xml = variable_bpmn(
        r#"<q:variable name="amount" type="number" source="pay-in"/>"#,
        "pay-in",
    );
    BpmnModelLoader::new().load(&xml).unwrap();
}

#[test]
fn a_variable_sourced_from_a_non_intake_channel_is_rejected() {
    // The process's only intake is "pay-in"; the variable claims to feed off "settlement-in".
    let xml = variable_bpmn(
        r#"<q:variable name="amount" type="number" source="settlement-in"/>"#,
        "pay-in",
    );
    let e = BpmnModelLoader::new().load(&xml).unwrap_err();
    assert_eq!(e.code, codes::CONFIG_BPMN_VARIABLE_SOURCE_UNKNOWN);
    assert!(e.message.contains("settlement-in"), "{e}");
    assert!(e.message.contains("amount"), "{e}");
}

#[test]
fn an_in_instance_variable_without_source_is_unaffected() {
    let xml = variable_bpmn(r#"<q:variable name="tally" type="number"/>"#, "pay-in");
    BpmnModelLoader::new().load(&xml).unwrap();
}

// ---- start-event selection -------------------------------------------------------

const SELECTION_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p">
        <bpmn:startEvent id="S_exact">
          <bpmn:extensionElements>
            <q:source channel="orders-in" messageTypeValue="order.created.001.14"/>
          </bpmn:extensionElements>
          <bpmn:outgoing>f1</bpmn:outgoing>
        </bpmn:startEvent>
        <bpmn:startEvent id="S_family">
          <bpmn:extensionElements>
            <q:source channel="orders-in" messageTypePattern="invoice\..*"/>
          </bpmn:extensionElements>
          <bpmn:outgoing>f2</bpmn:outgoing>
        </bpmn:startEvent>
        <bpmn:startEvent id="S_any">
          <bpmn:extensionElements>
            <q:source channel="orders-in"/>
          </bpmn:extensionElements>
          <bpmn:outgoing>f3</bpmn:outgoing>
        </bpmn:startEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S_exact" targetRef="E"/>
        <bpmn:sequenceFlow id="f2" sourceRef="S_family" targetRef="E"/>
        <bpmn:sequenceFlow id="f3" sourceRef="S_any" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

fn selected(channel: &str, message_type: Option<&str>) -> Option<String> {
    // No `filterExpression` anywhere in this fixture, so the predicate is never consulted; the
    // constant stand-in keeps these cases about message-type precedence alone.
    selected_with(channel, message_type, &sutra_bpmn::PredicateAlways(true))
}

fn selected_with(
    channel: &str,
    message_type: Option<&str>,
    predicate: &dyn sutra_bpmn::SourcePredicate,
) -> Option<String> {
    let module = BpmnModelLoader::new()
        .load(SELECTION_BPMN.as_bytes())
        .unwrap();
    let process = module.process("p").unwrap();
    process
        .select_start_event(channel, message_type, predicate)
        .map(|n| match n {
            Node::StartEvent { id, .. } => id.clone(),
            other => panic!("expected StartEvent, got {other:?}"),
        })
}

#[test]
fn exact_message_type_value_wins() {
    assert_eq!(
        selected("orders-in", Some("order.created.001.14")).as_deref(),
        Some("S_exact")
    );
}

#[test]
fn pattern_matches_its_family() {
    assert_eq!(
        selected("orders-in", Some("invoice.settled.001.08")).as_deref(),
        Some("S_family")
    );
}

#[test]
fn unmatched_type_falls_to_the_catch_all() {
    assert_eq!(
        selected("orders-in", Some("shipment.dispatched.001.09")).as_deref(),
        Some("S_any")
    );
}

#[test]
fn absent_message_type_matches_only_the_catch_all() {
    assert_eq!(selected("orders-in", None).as_deref(), Some("S_any"));
}

#[test]
fn unknown_channel_selects_nothing() {
    assert_eq!(selected("nope", Some("order.created.001.14")), None);
}

// ---- predicate-narrowed selection ------------------------------------------------

/// Two start events pinning the SAME message type, told apart only by their predicate — the
/// configuration this feature exists for — plus an unpredicated catch-all to prove a false
/// predicate falls THROUGH its tier rather than swallowing the message.
const PREDICATE_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p">
        <bpmn:startEvent id="S_in">
          <bpmn:extensionElements>
            <q:source channel="rail-in" messageTypeValue="Transfer"
                      filterExpression="attr.direction = &quot;incoming&quot;"/>
          </bpmn:extensionElements>
          <bpmn:outgoing>f1</bpmn:outgoing>
        </bpmn:startEvent>
        <bpmn:startEvent id="S_out">
          <bpmn:extensionElements>
            <q:source channel="rail-in" messageTypeValue="Transfer"
                      filterExpression="attr.direction = &quot;outgoing&quot;"/>
          </bpmn:extensionElements>
          <bpmn:outgoing>f2</bpmn:outgoing>
        </bpmn:startEvent>
        <bpmn:startEvent id="S_any">
          <bpmn:extensionElements>
            <q:source channel="rail-in"/>
          </bpmn:extensionElements>
          <bpmn:outgoing>f3</bpmn:outgoing>
        </bpmn:startEvent>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S_in" targetRef="E"/>
        <bpmn:sequenceFlow id="f2" sourceRef="S_out" targetRef="E"/>
        <bpmn:sequenceFlow id="f3" sourceRef="S_any" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

/// Answers true for exactly one node id — stands in for the compiled predicate table that
/// `sutra-channels` builds, without pulling an expression evaluator into this crate.
struct OnlyNode(&'static str);

impl sutra_bpmn::SourcePredicate for OnlyNode {
    fn holds(&self, node_id: &str) -> bool {
        node_id == self.0
    }
}

fn predicate_selected(predicate: &dyn sutra_bpmn::SourcePredicate) -> Vec<String> {
    let module = BpmnModelLoader::new()
        .load(PREDICATE_BPMN.as_bytes())
        .unwrap();
    let process = module.process("p").unwrap();
    process
        .select_start_events("rail-in", Some("Transfer"), predicate)
        .into_iter()
        .map(|n| n.id().to_string())
        .collect()
}

#[test]
fn the_predicate_picks_between_two_starts_on_one_message_type() {
    assert_eq!(predicate_selected(&OnlyNode("S_in")), vec!["S_in"]);
    assert_eq!(predicate_selected(&OnlyNode("S_out")), vec!["S_out"]);
}

#[test]
fn a_false_predicate_falls_through_to_the_catch_all() {
    // Neither pinned start event holds, so the exact tier is EMPTY rather than terminal, and
    // selection drops to the unpredicated catch-all. A false predicate must not swallow the
    // message.
    assert_eq!(
        predicate_selected(&sutra_bpmn::PredicateAlways(false)),
        vec!["S_any"]
    );
}

#[test]
fn predicate_unavailable_fails_closed() {
    // A caller with no evaluator must not route a predicated start event unchecked.
    assert_eq!(
        predicate_selected(&sutra_bpmn::PredicateUnavailable),
        vec!["S_any"]
    );
}

#[test]
fn two_matching_starts_in_one_process_are_both_returned() {
    // Ambiguity is the caller's to police; the model must not hide it behind document order.
    assert_eq!(
        predicate_selected(&sutra_bpmn::PredicateAlways(true)),
        vec!["S_in", "S_out"]
    );
}

#[test]
fn an_unpredicated_start_event_never_consults_the_predicate() {
    // S_any declares no filterExpression, so even a predicate that answers false for everything
    // leaves it eligible.
    let module = BpmnModelLoader::new()
        .load(PREDICATE_BPMN.as_bytes())
        .unwrap();
    let process = module.process("p").unwrap();
    let selected = process.select_start_event(
        "rail-in",
        Some("Unpinned"),
        &sutra_bpmn::PredicateUnavailable,
    );
    assert_eq!(
        selected.map(|n| n.id().to_string()).as_deref(),
        Some("S_any")
    );
}
