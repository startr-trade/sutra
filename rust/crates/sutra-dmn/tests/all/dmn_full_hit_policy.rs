//! 1:1 port of `DmnFullHitPolicyTest` — behavioural coverage for ANY / PRIORITY /
//! OUTPUT_ORDER / RULE_ORDER, including the fallback + WARNING behaviour when the
//! `<outputValues>` priority list is missing.

use sutra_dmn::codes;
use sutra_dmn::model::HitPolicy;
use sutra_dmn::{DmnFileLoader, DmnRulesetValidator, Severity};
use sutra_feel::{FeelContext, FeelValue};

fn amount_ctx(amount: i64) -> FeelContext {
    let mut c = FeelContext::new();
    c.insert("amount".to_string(), FeelValue::from(amount));
    c
}

fn validator(dmn: &str, decision_id: &str) -> DmnRulesetValidator {
    let defs = DmnFileLoader::new()
        .load(dmn.as_bytes())
        .expect("valid DMN");
    DmnRulesetValidator::new(
        defs.decision(decision_id)
            .unwrap_or_else(|| panic!("decision {decision_id}"))
            .clone(),
    )
}

fn messages(issues: &[sutra_dmn::ValidationIssue]) -> Vec<&str> {
    issues.iter().map(|i| i.message.as_str()).collect()
}

// ------------------------------------------------------------------ ANY (3 rules agree)

const ANY_THREE_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:any-3rule">
  <decision id="anyThree">
    <decisionTable hitPolicy="ANY">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1"/>
      <rule id="r1"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"flag"</text></outputEntry></rule>
      <rule id="r2"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"flag"</text></outputEntry></rule>
      <rule id="r3"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"flag"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// three rules fire with identical output → single issue, no ambiguity
#[test]
fn three_rules_agree_produces_one_issue() {
    let v = validator(ANY_THREE_DMN, "anyThree");
    let issues = v.validate_map(&amount_ctx(50)).unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].message, "flag");
}

// ------------------------------------------------------------------ PRIORITY

// outputValues priority order: high > medium > low. Rules fire in source order r_low,
// r_med, r_high — but the priority sort picks r_high.
const PRIORITY_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:priority">
  <decision id="severityCheck">
    <decisionTable hitPolicy="PRIORITY">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" name="severity" typeRef="string">
        <outputValues>
          <text>"high","medium","low"</text>
        </outputValues>
      </output>
      <rule id="r_low">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"low"</text></outputEntry>
      </rule>
      <rule id="r_med">
        <inputEntry><text>&gt; 100</text></inputEntry>
        <outputEntry><text>"medium"</text></outputEntry>
      </rule>
      <rule id="r_high">
        <inputEntry><text>&gt; 1000</text></inputEntry>
        <outputEntry><text>"high"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

const PRIORITY_NO_OUTPUT_VALUES_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:priority-no-list">
  <decision id="severityNoList">
    <decisionTable hitPolicy="PRIORITY">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" name="severity"/>
      <rule id="r_a">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"a"</text></outputEntry>
      </rule>
      <rule id="r_b">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"b"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// 3 rules fire; highest-priority output value wins → single 'high' issue
#[test]
fn priority_picks_highest_ranked() {
    let v = validator(PRIORITY_DMN, "severityCheck");
    // amount=5000 → r_low (>0), r_med (>100), r_high (>1000) all fire. Priority order is
    // high > medium > low, so r_high's output wins.
    let issues = v.validate_map(&amount_ctx(5000)).unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].message, "high");
    assert_eq!(issues[0].code, codes::DMN_RULESET_FAILED);
}

/// only one rule fires → its output emits regardless of priority position
#[test]
fn priority_with_single_firing_emits_that_output() {
    let v = validator(PRIORITY_DMN, "severityCheck");
    // amount=50 → only r_low (>0) fires.
    let issues = v.validate_map(&amount_ctx(50)).unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].message, "low");
}

/// missing <outputValues> → UNIQUE fallback + WARNING
#[test]
fn priority_without_output_values_falls_back_to_unique() {
    let v = validator(PRIORITY_NO_OUTPUT_VALUES_DMN, "severityNoList");
    // Both rules fire → UNIQUE fallback yields a violation. Plus the WARNING.
    let issues = v.validate_map(&amount_ctx(50)).unwrap();
    assert_eq!(issues.len(), 2);
    assert_eq!(issues[0].code, codes::DMN_PRIORITY_MISSING_OUTPUT_VALUES);
    assert_eq!(issues[0].severity, Severity::Warning);
    assert!(issues[0].message.contains("hitPolicy=PRIORITY"));
    assert!(issues[0].message.contains("falling back to UNIQUE"));
    assert_eq!(issues[1].code, codes::DMN_UNIQUE_VIOLATION);
}

// ------------------------------------------------------------------ OUTPUT ORDER

// outputValues order: red, amber, green. Rules fire in source order green, amber, red —
// the result list should be re-ordered to red, amber, green.
const OUTPUT_ORDER_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:output-order">
  <decision id="trafficLight">
    <decisionTable hitPolicy="OUTPUT ORDER">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" name="signal" typeRef="string">
        <outputValues>
          <text>"red","amber","green"</text>
        </outputValues>
      </output>
      <rule id="r_green">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"green"</text></outputEntry>
      </rule>
      <rule id="r_amber">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"amber"</text></outputEntry>
      </rule>
      <rule id="r_red">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"red"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

const OUTPUT_ORDER_NO_LIST_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:output-order-no-list">
  <decision id="trafficNoList">
    <decisionTable hitPolicy="OUTPUT ORDER">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" name="signal"/>
      <rule id="r1">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"x"</text></outputEntry>
      </rule>
      <rule id="r2">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"y"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// 3 rules fire; result list sorted by outputValues position, not rule position
#[test]
fn output_order_sorts_by_priority_list_position() {
    let v = validator(OUTPUT_ORDER_DMN, "trafficLight");
    let issues = v.validate_map(&amount_ctx(50)).unwrap();
    assert_eq!(issues.len(), 3);
    // Lexical rule order is r_green, r_amber, r_red. Output-order sort flips to red,
    // amber, green per the priority list.
    assert_eq!(messages(&issues), vec!["red", "amber", "green"]);
}

/// missing <outputValues> → COLLECT fallback (rule-defined order) + WARNING
#[test]
fn output_order_without_list_falls_back_to_collect() {
    let v = validator(OUTPUT_ORDER_NO_LIST_DMN, "trafficNoList");
    let issues = v.validate_map(&amount_ctx(50)).unwrap();
    assert_eq!(issues.len(), 3); // 1 WARNING + 2 rule outputs
    assert_eq!(
        issues[0].code,
        codes::DMN_OUTPUT_ORDER_MISSING_OUTPUT_VALUES
    );
    assert_eq!(issues[0].severity, Severity::Warning);
    assert!(issues[0].message.contains("hitPolicy=OUTPUT_ORDER"));
    assert!(issues[0].message.contains("falling back to COLLECT"));
    // Rule-defined order preserved.
    assert_eq!(messages(&issues[1..]), vec!["x", "y"]);
}

// ------------------------------------------------------------------ RULE ORDER

const RULE_ORDER_THREE_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:rule-order-3">
  <decision id="ruleOrderThree">
    <decisionTable hitPolicy="RULE ORDER">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1"/>
      <rule id="r1"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"alpha"</text></outputEntry></rule>
      <rule id="r2"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"bravo"</text></outputEntry></rule>
      <rule id="r3"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"charlie"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// 3 rules fire under RULE_ORDER → result list is in lexical (source) order
#[test]
fn rule_order_preserves_lexical_order() {
    let v = validator(RULE_ORDER_THREE_DMN, "ruleOrderThree");
    let issues = v.validate_map(&amount_ctx(50)).unwrap();
    assert_eq!(issues.len(), 3);
    assert_eq!(messages(&issues), vec!["alpha", "bravo", "charlie"]);
}

// ------------------------------------------------------------------ Hit-policy discriminator
// Same payload + same rules, swap hit policy, observe shape diff.

fn combo_dmn(policy_attr: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:combo">
  <decision id="combo">
    <decisionTable hitPolicy="{policy_attr}">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" name="severity" typeRef="string">
        <outputValues>
          <text>"high","medium","low"</text>
        </outputValues>
      </output>
      <rule id="r_low">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"low"</text></outputEntry>
      </rule>
      <rule id="r_med">
        <inputEntry><text>&gt; 100</text></inputEntry>
        <outputEntry><text>"medium"</text></outputEntry>
      </rule>
      <rule id="r_high">
        <inputEntry><text>&gt; 1000</text></inputEntry>
        <outputEntry><text>"high"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#
    )
}

fn run_combo(policy: &str, payload: &FeelContext) -> Vec<sutra_dmn::ValidationIssue> {
    validator(&combo_dmn(policy), "combo")
        .validate_map(payload)
        .unwrap()
}

/// PRIORITY → 1 issue ('high'); RULE_ORDER → [low,medium,high];
/// OUTPUT_ORDER → [high,medium,low]; COLLECT → [low,medium,high]
#[test]
fn different_policies_produce_different_shapes() {
    let payload = amount_ctx(5000);

    let priority = run_combo("PRIORITY", &payload);
    assert_eq!(priority.len(), 1);
    assert_eq!(priority[0].message, "high");

    let rule_order = run_combo("RULE ORDER", &payload);
    assert_eq!(messages(&rule_order), vec!["low", "medium", "high"]);

    let output_order = run_combo("OUTPUT ORDER", &payload);
    assert_eq!(messages(&output_order), vec!["high", "medium", "low"]);

    let collect = run_combo("COLLECT", &payload);
    assert_eq!(messages(&collect), vec!["low", "medium", "high"]);
}

// ------------------------------------------------------------------ <outputValues> parsing

const OUTPUT_VALUES_PARSE_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:output-values-parse">
  <decision id="parsedOutput">
    <decisionTable hitPolicy="PRIORITY">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" name="severity" typeRef="string">
        <outputValues>
          <text>"high","medium","low"</text>
        </outputValues>
      </output>
      <rule id="r1"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"low"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// string priority list parsed in order, quotes stripped
#[test]
fn output_values_string_list() {
    let defs = DmnFileLoader::new()
        .load(OUTPUT_VALUES_PARSE_DMN.as_bytes())
        .expect("valid DMN");
    let output = &defs
        .decision("parsedOutput")
        .expect("decision")
        .table
        .outputs[0];
    assert_eq!(output.output_values, vec!["high", "medium", "low"]);
}

/// missing <outputValues> → empty list on the output clause
#[test]
fn missing_output_values_empty_list() {
    let dmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/">
  <decision id="noList">
    <decisionTable hitPolicy="COLLECT">
      <input id="i1"><inputExpression><text>x</text></inputExpression></input>
      <output id="o1"/>
      <rule id="r1"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"a"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>
"#;
    let defs = DmnFileLoader::new()
        .load(dmn.as_bytes())
        .expect("valid DMN");
    let output = &defs.decision("noList").expect("decision").table.outputs[0];
    assert!(output.output_values.is_empty());
}

// ------------------------------------------------------------------ returns_list discriminator

/// list-returning policies: COLLECT, OUTPUT_ORDER, RULE_ORDER
#[test]
fn list_policies() {
    assert!(DmnRulesetValidator::returns_list(HitPolicy::Collect));
    assert!(DmnRulesetValidator::returns_list(HitPolicy::OutputOrder));
    assert!(DmnRulesetValidator::returns_list(HitPolicy::RuleOrder));
}

/// single-output policies: UNIQUE, FIRST, ANY, PRIORITY
#[test]
fn single_policies() {
    assert!(!DmnRulesetValidator::returns_list(HitPolicy::Unique));
    assert!(!DmnRulesetValidator::returns_list(HitPolicy::First));
    assert!(!DmnRulesetValidator::returns_list(HitPolicy::Any));
    assert!(!DmnRulesetValidator::returns_list(HitPolicy::Priority));
}
