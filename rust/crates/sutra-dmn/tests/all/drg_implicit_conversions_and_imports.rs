//! Focused tests for two DRG mechanisms the DMN-TCK run exercises only as whole models:
//!
//! - DMN 1.5's implicit "date to date and time" conversion at every typeRef-bearing slot (a
//!   decision result, a context entry, a BKM formal parameter, a decision service's declared
//!   output type), plus direct BKM invocation via `Drg::evaluate_bkm`, whose result conforms to
//!   the `functionItem` output type the BKM's `<variable typeRef>` declares (DMN-TCK
//!   1157-implicit-conversions);
//! - imported models' input data, reachable through a named import's prefix and through an
//!   unnamed (DMN 1.5 empty-name) import, including a diamond where the importing model reaches
//!   the same imported input both directly and through another import (DMN-TCK
//!   1160-multiple-imports).

use std::collections::HashMap;
use std::sync::Arc;

use sutra_dmn::{load_drg, load_drg_with_imports, Drg};
use sutra_feel::{expressions, FeelContext, FeelValue};

fn feel(text: &str) -> FeelValue {
    expressions::eval(text, &FeelContext::new()).expect("expression should evaluate")
}

fn ok(
    out: &std::collections::BTreeMap<String, Result<FeelValue, String>>,
    name: &str,
) -> FeelValue {
    out.get(name)
        .unwrap_or_else(|| panic!("no result for '{name}' — got {out:?}"))
        .clone()
        .unwrap_or_else(|e| panic!("'{name}' errored: {e}"))
}

const CONVERSIONS_MODEL: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:conversions" name="conversions">
    <itemDefinition name="numberList" isCollection="true"><typeRef>number</typeRef></itemDefinition>
    <itemDefinition name="returnsNumberList"><functionItem outputTypeRef="numberList"/></itemDefinition>
    <itemDefinition name="returnsNumber"><functionItem outputTypeRef="number"/></itemDefinition>
    <itemDefinition name="returnsDateTime"><functionItem outputTypeRef="date and time"/></itemDefinition>

    <decision name="Typed Result" id="_typed_result">
        <variable name="Typed Result" typeRef="date and time"/>
        <literalExpression><text>date(2000, 1, 2)</text></literalExpression>
    </decision>

    <decision name="Typed Entry" id="_typed_entry">
        <variable name="Typed Entry"/>
        <context>
            <contextEntry>
                <variable name="stamp" typeRef="date and time"/>
                <literalExpression typeRef="date"><text>date("2000-12-01")</text></literalExpression>
            </contextEntry>
            <contextEntry>
                <literalExpression><text>stamp</text></literalExpression>
            </contextEntry>
        </context>
    </decision>

    <businessKnowledgeModel name="Echo" id="_echo">
        <variable name="Echo"/>
        <encapsulatedLogic>
            <formalParameter name="when" typeRef="date and time"/>
            <literalExpression><text>when</text></literalExpression>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <decision name="Typed Parameter" id="_typed_param">
        <variable name="Typed Parameter"/>
        <knowledgeRequirement><requiredKnowledge href="#_echo"/></knowledgeRequirement>
        <invocation>
            <literalExpression><text>Echo</text></literalExpression>
            <binding>
                <parameter name="when"/>
                <literalExpression><text>date("2000-02-01")</text></literalExpression>
            </binding>
        </invocation>
    </decision>

    <decision name="Service Body" id="_service_body">
        <variable name="Service Body" typeRef="date"/>
        <literalExpression><text>date(2000, 1, 2)</text></literalExpression>
    </decision>
    <decisionService name="Service" id="_service">
        <variable name="Service" typeRef="returnsDateTime"/>
        <outputDecision href="#_service_body"/>
    </decisionService>

    <businessKnowledgeModel name="Wraps" id="_wraps">
        <variable name="Wraps" typeRef="returnsNumberList"/>
        <encapsulatedLogic>
            <literalExpression typeRef="number"><text>1</text></literalExpression>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <businessKnowledgeModel name="Unwraps" id="_unwraps">
        <variable name="Unwraps" typeRef="returnsNumber"/>
        <encapsulatedLogic>
            <literalExpression><text>[1]</text></literalExpression>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <businessKnowledgeModel name="Increment" id="_increment">
        <variable name="Increment"/>
        <encapsulatedLogic>
            <formalParameter name="n" typeRef="number"/>
            <literalExpression><text>n + 1</text></literalExpression>
        </encapsulatedLogic>
    </businessKnowledgeModel>
    <decision name="Calls Wraps" id="_calls_wraps">
        <variable name="Calls Wraps"/>
        <knowledgeRequirement><requiredKnowledge href="#_wraps"/></knowledgeRequirement>
        <literalExpression><text>Wraps()</text></literalExpression>
    </decision>
</definitions>"##;

fn conversions() -> Arc<Drg> {
    load_drg(CONVERSIONS_MODEL.as_bytes()).expect("model should load")
}

#[test]
fn a_date_meets_a_date_and_time_decision_context_entry_and_parameter() {
    let out = conversions().evaluate(&FeelContext::new());
    assert_eq!(
        ok(&out, "Typed Result"),
        feel(r#"date and time("2000-01-02T00:00:00Z")"#)
    );
    assert_eq!(
        ok(&out, "Typed Entry"),
        feel(r#"date and time("2000-12-01T00:00:00Z")"#)
    );
    assert_eq!(
        ok(&out, "Typed Parameter"),
        feel(r#"date and time("2000-02-01T00:00:00Z")"#)
    );
}

#[test]
fn a_decision_service_output_type_converts_its_date_output() {
    let out = conversions().evaluate_decision_service("Service", &FeelContext::new());
    assert_eq!(
        ok(&out, "Service Body"),
        feel(r#"date and time("2000-01-02T00:00:00Z")"#)
    );
}

#[test]
fn a_bkm_result_conforms_to_its_function_item_output_type() {
    let drg = conversions();
    let empty = FeelContext::new();
    assert_eq!(ok(&drg.evaluate_bkm("Wraps", &empty), "Wraps"), feel("[1]"));
    assert_eq!(
        ok(&drg.evaluate_bkm("Unwraps", &empty), "Unwraps"),
        feel("1")
    );
    // The same return type applies when the BKM is called from a decision.
    assert_eq!(ok(&drg.evaluate(&empty), "Calls Wraps"), feel("[1]"));
}

#[test]
fn direct_bkm_invocation_binds_and_gates_parameters_by_name() {
    let drg = conversions();
    let mut args = FeelContext::new();
    args.insert("n".to_string(), feel("41"));
    assert_eq!(
        ok(&drg.evaluate_bkm("Increment", &args), "Increment"),
        feel("42")
    );
    // A singleton list conforms to `number` (from singleton list).
    args.insert("n".to_string(), feel("[41]"));
    assert_eq!(
        ok(&drg.evaluate_bkm("Increment", &args), "Increment"),
        feel("42")
    );
    // A non-conforming argument: the BKM is never invoked.
    args.insert("n".to_string(), feel(r#""forty-one""#));
    assert_eq!(
        ok(&drg.evaluate_bkm("Increment", &args), "Increment"),
        FeelValue::Null
    );
    assert!(drg.evaluate_bkm("No Such BKM", &args).is_empty());
}

// ---- imports --------------------------------------------------------------------------------

const MODEL_A: &str = r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:a" name="A">
    <inputData name="person" id="_person">
        <variable name="person"/>
    </inputData>
</definitions>"##;

/// Model B, importing A under `prefix` (empty for an unnamed import).
fn model_b(prefix: &str) -> String {
    let dot = if prefix.is_empty() { "" } else { "." };
    format!(
        r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:b-{prefix}" name="B">
    <import name="{prefix}" namespace="urn:test:a" importType="https://www.omg.org/spec/DMN/20230324/MODEL/"/>
    <decision name="DecisionB" id="_b">
        <variable name="DecisionB" typeRef="string"/>
        <informationRequirement><requiredInput href="urn:test:a#_person"/></informationRequirement>
        <literalExpression><text>{prefix}{dot}person.name</text></literalExpression>
    </decision>
</definitions>"##
    )
}

/// Model C, importing A under `a_prefix` and B under `b_prefix` — a diamond, since B imports A.
fn model_c(a_prefix: &str, b_prefix: &str, b_namespace: &str) -> String {
    let qualify = |prefix: &str, name: &str| {
        if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}.{name}")
        }
    };
    format!(
        r##"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:c" name="C">
    <import name="{a_prefix}" namespace="urn:test:a" importType="https://www.omg.org/spec/DMN/20230324/MODEL/"/>
    <import name="{b_prefix}" namespace="{b_namespace}" importType="https://www.omg.org/spec/DMN/20230324/MODEL/"/>
    <decision name="DecisionC" id="_c">
        <variable name="DecisionC" typeRef="string"/>
        <informationRequirement><requiredInput href="urn:test:a#_person"/></informationRequirement>
        <informationRequirement><requiredDecision href="{b_namespace}#_b"/></informationRequirement>
        <literalExpression><text>{} + {}</text></literalExpression>
    </decision>
</definitions>"##,
        qualify(a_prefix, "person.name"),
        qualify(b_prefix, "DecisionB"),
    )
}

fn person_input() -> FeelContext {
    let mut ctx = FeelContext::new();
    ctx.insert("person".to_string(), feel(r#"{name: "Peter", age: 25}"#));
    ctx
}

fn evaluate_with(model: &str, siblings: &[(&str, String)], decision: &str) -> FeelValue {
    let by_namespace: HashMap<String, Vec<u8>> = siblings
        .iter()
        .map(|(ns, bytes)| (ns.to_string(), bytes.clone().into_bytes()))
        .collect();
    let resolve = |ns: &str| by_namespace.get(ns).cloned();
    let drg = load_drg_with_imports(model.as_bytes(), &resolve).expect("model should load");
    ok(&drg.evaluate(&person_input()), decision)
}

#[test]
fn an_imported_input_is_reachable_through_the_import_prefix() {
    let siblings = [("urn:test:a", MODEL_A.to_string())];
    assert_eq!(
        evaluate_with(&model_b("a"), &siblings, "DecisionB"),
        feel(r#""Peter""#)
    );
}

#[test]
fn an_unnamed_import_is_referenced_unqualified() {
    let siblings = [("urn:test:a", MODEL_A.to_string())];
    assert_eq!(
        evaluate_with(&model_b(""), &siblings, "DecisionB"),
        feel(r#""Peter""#)
    );
}

#[test]
fn a_diamond_of_named_imports_sees_one_input_value() {
    let siblings = [
        ("urn:test:a", MODEL_A.to_string()),
        ("urn:test:b-a", model_b("a")),
    ];
    assert_eq!(
        evaluate_with(&model_c("a", "b", "urn:test:b-a"), &siblings, "DecisionC"),
        feel(r#""PeterPeter""#)
    );
}

#[test]
fn a_diamond_of_unnamed_imports_exposes_the_imported_decision_unqualified() {
    let siblings = [
        ("urn:test:a", MODEL_A.to_string()),
        ("urn:test:b-", model_b("")),
    ];
    assert_eq!(
        evaluate_with(&model_c("", "", "urn:test:b-"), &siblings, "DecisionC"),
        feel(r#""PeterPeter""#)
    );
}
