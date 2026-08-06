//! Task-kind coverage: `manualTask` pass-through, `businessRuleTask` decision evaluation (both
//! decision-table and rule-language implementations), `scriptTask` rendering, terminate end
//! events, and `sendTask` emission (explicit destination and outbound channel).

use std::cell::RefCell;
use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::Node;
use sutra_executor::{
    CollectingSink, DecisionEngine, DecisionEngineRegistry, DecisionRegistry, DeploymentId,
    EmissionSink, OutboundChannelRegistry, ResolvedOutboundChannel, ScriptRegistry, SrlEngine,
    TaskRegistry, TemplateEngine, TemplateEngineRegistry, TemplateRegistry, TokenExecutor,
    Variables,
};
use sutra_feel::FeelValue;

// ---- ManualTaskTest -----------------------------------------------------------

#[test]
fn manual_task_parses_to_manual_task_node() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:manualTask id="Sign" name="Wet-ink signature"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sign"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sign" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    assert!(matches!(
        process.node("Sign").unwrap(),
        Node::ManualTask { .. }
    ));
}

#[tokio::test]
async fn manual_task_passes_the_token_straight_through() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:manualTask id="Sign"/>
            <bpmn:serviceTask id="After" implementation="${after}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sign"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sign" targetRef="After"/>
            <bpmn:sequenceFlow id="f3" sourceRef="After" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry =
        TaskRegistry::new().register("after", |_, _| ok_map(&[("ranAfter", boolean(true))]));
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    for node in ["Sign", "After", "E"] {
        assert!(result.visited_nodes.contains(node), "visited {node}");
    }
    assert_eq!(result.output("ranAfter"), Some(&boolean(true)));
}

// ---- BusinessRuleTaskTest -----------------------------------------------------

/// Stub decision engine for `.stubdec`: decides a `tier` from `amount`, echoes the file.
struct StubDecision;

impl DecisionEngine for StubDecision {
    fn name(&self) -> &str {
        "stub"
    }
    fn extensions(&self) -> Vec<String> {
        vec![".stubdec".to_string()]
    }
    fn evaluate(&self, _id: &str, decision: &[u8], input: &Variables) -> Result<Variables, String> {
        let threshold = bigdecimal::BigDecimal::from(100);
        let gold = matches!(
            input.get("amount"),
            Some(FeelValue::Number(n)) if *n >= threshold
        );
        let mut out = Variables::new();
        out.insert("tier", string(if gold { "GOLD" } else { "STANDARD" }));
        out.insert("ruleFile", string(&String::from_utf8_lossy(decision)));
        Ok(out)
    }
}

fn business_rule_process(implementation_attr: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:businessRuleTask id="Decide" name="Decide tier"{implementation_attr}/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Decide"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Decide" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn decision_executor(files: &[(&str, &str)]) -> TokenExecutor {
    let engines = DecisionEngineRegistry::new().register(StubDecision);
    let mut decisions = DecisionRegistry::new();
    for (name, body) in files {
        decisions.register(name, body.as_bytes().to_vec());
    }
    TokenExecutor::builder(TaskRegistry::new())
        .with_decisions(engines, decisions)
        .build()
}

#[tokio::test]
async fn business_rule_task_evaluates_and_merges_result() {
    let process = proc(
        &business_rule_process(" implementation=\"tier.stubdec\""),
        "p1",
    );
    let executor = decision_executor(&[("tier.stubdec", "RULES")]);

    let result = executor
        .execute_sync(&process, vars(&[("amount", num(250))]))
        .await
        .unwrap();

    assert_eq!(result.output("tier"), Some(&string("GOLD")));
    assert_eq!(result.output("ruleFile"), Some(&string("RULES")));
    assert!(result.visited_nodes.contains("Decide"));
    assert!(result.visited_nodes.contains("E"));
}

#[test]
fn business_rule_task_without_implementation_fails_at_load() {
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(business_rule_process("").as_bytes())
        .unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::RESOLVE_TASK_UNKNOWN);
}

#[tokio::test]
async fn unknown_decision_file_fails_closed_at_runtime() {
    let process = proc(
        &business_rule_process(" implementation=\"missing.stubdec\""),
        "p1",
    );
    let executor = decision_executor(&[]); // nothing registered
    let e = executor
        .execute_sync(&process, vars(&[("amount", num(10))]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "SUTRA.RESOLVE.TEMPLATE.UNKNOWN");
}

// ---- businessRuleTask binds a `.srl` ruleset via the real SrlEngine -------

/// A `.srl` ruleset exercising salience, an activation-group (first-match-wins), `set`, and
/// `report`. `gold tier` outranks `standard tier` by salience; both share the `tier`
/// activation-group so only the first matching one fires. The ungrouped `negative amount`
/// rule reports an issue.
const TIER_RULESET: &str = r#"
rule "gold tier" salience 10 activation-group "tier"
  when amount >= 100
  then
    set(tier, "GOLD");
end
rule "standard tier" activation-group "tier"
  when amount < 100
  then
    set(tier, "STANDARD");
end
rule "negative amount"
  when amount < 0
  then
    report("AMT_NEGATIVE", "amount", "amount must not be negative");
end
"#;

fn srl_executor(ruleset: &str) -> TokenExecutor {
    let engines = DecisionEngineRegistry::new().register(SrlEngine::new());
    let mut decisions = DecisionRegistry::new();
    decisions.register("tier.srl", ruleset.as_bytes().to_vec());
    TokenExecutor::builder(TaskRegistry::new())
        .with_decisions(engines, decisions)
        .build()
}

#[tokio::test]
async fn srl_business_rule_task_sets_tier_via_salience_and_activation_group() {
    let process = proc(&business_rule_process(" implementation=\"tier.srl\""), "p1");
    let executor = srl_executor(TIER_RULESET);

    let result = executor
        .execute_sync(&process, vars(&[("amount", num(250))]))
        .await
        .unwrap();

    // salience 10 fires `gold tier` first; the shared activation-group blocks `standard tier`.
    assert_eq!(result.output("tier"), Some(&string("GOLD")));
    // no `report` fired → the engine emits no `issues` key (never clobbers a prior list).
    assert_eq!(result.output("issues"), None);
    assert!(result.visited_nodes.contains("Decide"));
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn srl_business_rule_task_reports_issue_for_negative_amount() {
    let process = proc(&business_rule_process(" implementation=\"tier.srl\""), "p1");
    let executor = srl_executor(TIER_RULESET);

    let result = executor
        .execute_sync(&process, vars(&[("amount", num(-5))]))
        .await
        .unwrap();

    // activation-group `tier`: `gold tier` skips (amount < 100), `standard tier` fires.
    assert_eq!(result.output("tier"), Some(&string("STANDARD")));
    // the ungrouped `negative amount` rule reports one issue in the frozen issue shape.
    match result.output("issues") {
        Some(FeelValue::List(items)) => {
            assert_eq!(items.len(), 1, "exactly one issue reported");
            let FeelValue::Map(m) = &items[0] else {
                panic!("issue entry is not a map: {:?}", items[0]);
            };
            assert_eq!(m.get("code"), Some(&string("AMT_NEGATIVE")));
            assert_eq!(m.get("severity"), Some(&string("ERROR")));
            assert_eq!(m.get("path"), Some(&string("amount")));
        }
        other => panic!("expected an `issues` list, got {other:?}"),
    }
}

// ---- ScriptTaskTest -----------------------------------------------------------

/// Trivial engine for `.testjson` files: substitutes `${key}` from the model
/// (stringifying each substituted value).
pub struct SubstitutionEngine;

impl TemplateEngine for SubstitutionEngine {
    fn name(&self) -> &str {
        "testjson"
    }
    fn extensions(&self) -> Vec<String> {
        vec![".testjson".to_string()]
    }
    fn render(
        &self,
        _id: &str,
        template: &[u8],
        model: &serde_json::Value,
    ) -> Result<String, String> {
        let mut s = String::from_utf8_lossy(template).into_owned();
        if let serde_json::Value::Object(map) = model {
            for (k, v) in map {
                let rendered = match v {
                    serde_json::Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                s = s.replace(&format!("${{{k}}}"), &rendered);
            }
        }
        Ok(s)
    }
}

fn script_executor(files: &[(&str, &str)]) -> TokenExecutor {
    let engines = TemplateEngineRegistry::new().register(SubstitutionEngine);
    let mut scripts = ScriptRegistry::new();
    for (name, body) in files {
        scripts.register(name, body.as_bytes().to_vec());
    }
    TokenExecutor::builder(TaskRegistry::new())
        .with_templates(engines, TemplateRegistry::new())
        .with_scripts(scripts)
        .build()
}

fn script_process(script_file_ref: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:scriptTask id="calc"><bpmn:script>{script_file_ref}</bpmn:script></bpmn:scriptTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="calc"/>
            <bpmn:sequenceFlow id="f2" sourceRef="calc" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

#[test]
fn script_tag_names_the_script_file() {
    let process = proc(&script_process("derive-fee.testjson"), "p1");
    match process.node("calc").unwrap() {
        Node::ScriptTask { script_file, .. } => assert_eq!(script_file, "derive-fee.testjson"),
        other => panic!("expected ScriptTask, got {other:?}"),
    }
}

#[tokio::test]
async fn script_file_renders_to_json_object_and_merges_typed_variables() {
    let process = proc(&script_process("derive-fee.testjson"), "p1");
    let executor = script_executor(&[(
        "derive-fee.testjson",
        r#"{"fee": ${amount}, "flagged": ${big}}"#,
    )]);

    let result = executor
        .execute_sync(
            &process,
            vars(&[("amount", num(250)), ("big", boolean(true))]),
        )
        .await
        .unwrap();

    assert_eq!(result.output("fee"), Some(&num(250)));
    assert_eq!(result.output("flagged"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("E"));
}

#[tokio::test]
async fn missing_script_file_fails_closed() {
    let process = proc(&script_process("absent.testjson"), "p1");
    let executor = script_executor(&[]); // nothing registered
    let e = executor
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert!(e.message().contains("no such file is registered"), "{e}");
}

#[tokio::test]
async fn script_file_rendering_to_non_object_fails_closed() {
    let process = proc(&script_process("scalar.testjson"), "p1");
    let executor = script_executor(&[("scalar.testjson", "42")]); // valid JSON, not an object
    let e = executor
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert!(e.message().contains("must render a JSON object"), "{e}");
}

#[test]
fn script_task_without_a_script_file_is_rejected_at_load() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:scriptTask id="calc"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="calc"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert!(e.message.contains("requires a <bpmn:script>"), "{e}");
}

// ---- TerminateEndEventTest ------------------------------------------------------

#[test]
fn terminate_event_definition_parses_to_terminate_end_event_node() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="Plain"/>
            <bpmn:endEvent id="Term"><bpmn:terminateEventDefinition/></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Term"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    assert!(matches!(
        process.node("Term").unwrap(),
        Node::TerminateEndEvent { .. }
    ));
    assert!(matches!(
        process.node("Plain").unwrap(),
        Node::EndEvent { .. }
    ));
}

#[tokio::test]
async fn linear_process_ending_in_terminate_completes() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T1" implementation="${t1}"/>
            <bpmn:endEvent id="Term"><bpmn:terminateEventDefinition/></bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T1"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T1" targetRef="Term"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = TaskRegistry::new().register("t1", |_, _| ok_map(&[("ranT1", boolean(true))]));
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(result.output("ranT1"), Some(&boolean(true)));
    assert!(result.visited_nodes.contains("Term"));
}

#[tokio::test]
async fn terminate_cancels_sibling_parallel_branch() {
    // The terminate branch's flow is declared first, so in the deterministic FIFO executor
    // its token is polled before the sibling's — terminate then drops the sibling token.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:parallelGateway id="Fork"/>
            <bpmn:endEvent id="Term"><bpmn:terminateEventDefinition/></bpmn:endEvent>
            <bpmn:serviceTask id="TB" implementation="${tb}"/>
            <bpmn:endEvent id="EB"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Fork"/>
            <bpmn:sequenceFlow id="fTerm" sourceRef="Fork" targetRef="Term"/>
            <bpmn:sequenceFlow id="fB" sourceRef="Fork" targetRef="TB"/>
            <bpmn:sequenceFlow id="fBE" sourceRef="TB" targetRef="EB"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let tb_runs = Rc::new(RefCell::new(0));
    let tbr = Rc::clone(&tb_runs);
    let registry = TaskRegistry::new().register("tb", move |_, _| {
        *tbr.borrow_mut() += 1;
        ok_map(&[("ranTB", boolean(true))])
    });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(*tb_runs.borrow(), 0);
    assert_eq!(result.output("ranTB"), None);
    assert!(result.visited_nodes.contains("Term"));
    assert!(!result.visited_nodes.contains("TB"));
    assert!(!result.visited_nodes.contains("EB"));
}

// ---- SendTaskTest -------------------------------------------------------------

fn send_task_process(send_element: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:sendTask id="Notify" name="Notify ops">
              <bpmn:extensionElements>
                {send_element}
              </bpmn:extensionElements>
            </bpmn:sendTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Notify"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

#[tokio::test]
async fn send_task_emits_its_destination_then_continues() {
    let process = proc(
        &send_task_process(r#"<q:send destination="https://ops.example/notify"/>"#),
        "p1",
    );
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();

    let result = executor
        .execute_sync(&process, vars(&[("payload.body", string("flagged"))]))
        .await
        .unwrap();

    // Emit-and-continue: one emission at the destination AND the token reached the end.
    assert!(result.visited_nodes.contains("Notify"));
    assert!(result.visited_nodes.contains("E"));
    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(emissions[0].destination, "https://ops.example/notify");
    assert_eq!(emissions[0].body_utf8(), "flagged");
}

#[tokio::test]
async fn send_task_via_outbound_channel_resolves_and_enqueues() {
    let process = proc(
        &send_task_process(r#"<q:send channel="responses-out"/>"#),
        "p1",
    );
    let mut registry = OutboundChannelRegistry::new();
    registry.register(
        &DeploymentId::unresolved(),
        ResolvedOutboundChannel::resolve(
            "responses-out",
            "http",
            "https://callbacks.example/out",
            None,
            None,
            None,
            "none",
        ),
    );
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_outbound_channels(registry)
        .build();

    executor
        .execute_sync(&process, vars(&[("payload.body", string("<msg/>"))]))
        .await
        .unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(emissions[0].destination, "https://callbacks.example/out");
}

#[test]
fn send_task_without_send_fails_closed_at_load() {
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(send_task_process("").as_bytes())
        .unwrap_err();
    assert_eq!(e.code, sutra_bpmn::codes::PARSE_THROW_SEND_REQUIRED);
}
