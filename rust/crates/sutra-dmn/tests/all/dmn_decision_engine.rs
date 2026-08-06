//! 1:1 port of `DmnDecisionEngineTest` — the businessRuleTask decision-evaluation
//! path: evaluate a `.dmn` decision table against the process variables and return its
//! outputs as a result map (output-clause name → value).

use sutra_dmn::DmnDecisionEngine;
use sutra_feel::{FeelContext, FeelValue};

fn amount_ctx(amount: i64) -> FeelContext {
    let mut c = FeelContext::new();
    c.insert("amount".to_string(), FeelValue::from(amount));
    c
}

// A two-output tier decision: amount >= 100 -> GOLD/20, else STANDARD/0. FIRST hit policy.
const TIER_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:tier">
  <decision id="tier">
    <decisionTable hitPolicy="FIRST">
      <input id="i1"><inputExpression typeRef="number"><text>amount</text></inputExpression></input>
      <output id="o1" name="tier" typeRef="string"/>
      <output id="o2" name="discount" typeRef="number"/>
      <rule id="r1">
        <inputEntry><text>&gt;= 100</text></inputEntry>
        <outputEntry><text>"GOLD"</text></outputEntry>
        <outputEntry><text>20</text></outputEntry>
      </rule>
      <rule id="r2">
        <inputEntry><text>&lt; 100</text></inputEntry>
        <outputEntry><text>"STANDARD"</text></outputEntry>
        <outputEntry><text>0</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

#[test]
fn evaluates_matching_rule_into_named_outputs() {
    let engine = DmnDecisionEngine::new();
    let result = engine
        .evaluate("tier.dmn", TIER_DMN.as_bytes(), &amount_ctx(250))
        .unwrap();
    assert_eq!(result.get("tier"), Some(&FeelValue::from("GOLD")));
    assert!(result.contains_key("discount"));
}

#[test]
fn picks_the_standard_branch_for_a_lower_input() {
    let engine = DmnDecisionEngine::new();
    let result = engine
        .evaluate("tier.dmn", TIER_DMN.as_bytes(), &amount_ctx(50))
        .unwrap();
    assert_eq!(result.get("tier"), Some(&FeelValue::from("STANDARD")));
}

#[test]
fn name_and_extensions() {
    let engine = DmnDecisionEngine::new();
    assert_eq!(engine.name(), "dmn");
    assert_eq!(engine.extensions(), vec![".dmn"]);
}

// Temporal decision through the businessRuleTask path (default system clock): a 2020
// timestamp is stale for any real evaluation time, proving the reserved `now` variable is
// injected on this entry point too — not only on the validator path.
const STALENESS_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:stale">
  <decision id="staleness">
    <decisionTable hitPolicy="FIRST">
      <input id="i1"><inputExpression typeRef="number"><text>secondsBetween(createdAt, now)</text></inputExpression></input>
      <output id="o1" name="verdict" typeRef="string"/>
      <rule id="stale">
        <inputEntry><text>&gt; 300</text></inputEntry>
        <outputEntry><text>"STALE"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

#[test]
fn injects_evaluation_clock_as_now_variable() {
    let engine = DmnDecisionEngine::new();
    let mut input = FeelContext::new();
    input.insert(
        "createdAt".to_string(),
        FeelValue::from("2020-01-01T00:00:00Z"),
    );
    let result = engine
        .evaluate("staleness.dmn", STALENESS_DMN.as_bytes(), &input)
        .unwrap();
    assert_eq!(result.get("verdict"), Some(&FeelValue::from("STALE")));
}
