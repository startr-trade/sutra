//! Behavioural tests for the DMN decision-table core.
//!
//! Each test parses an inline DMN string via `DmnFileLoader`, wraps the resulting decision
//! in a `DmnRulesetValidator`, and asserts on the `ValidationIssue` list produced for a
//! payload. The registry section covers definitions-level decision-id lookup only — the
//! file-watching registry itself is out of scope here.

use sutra_dmn::codes;
use sutra_dmn::model::HitPolicy;
use sutra_dmn::unary_test;
use sutra_dmn::{DmnFileLoader, DmnPayload, DmnRulesetValidator, FixedClock, Severity};
use sutra_feel::{FeelContext, FeelValue};
use time::macros::datetime;
use time::OffsetDateTime;

fn ctx(pairs: Vec<(&str, FeelValue)>) -> FeelContext {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn map(pairs: Vec<(&str, FeelValue)>) -> FeelValue {
    FeelValue::Map(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
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

// ------------------------------------------------------------------ COLLECT policy

const COLLECT_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             xmlns:bpm="urn:sutra:dmn:annotations"
             namespace="urn:test:credit">
  <decision id="creditCheck" name="Credit Check">
    <decisionTable hitPolicy="COLLECT">
      <input id="i1">
        <inputExpression typeRef="number">
          <text>amount</text>
        </inputExpression>
      </input>
      <output id="o1" name="issue" typeRef="string"/>
      <rule id="r_high">
        <inputEntry><text>&gt; 10000</text></inputEntry>
        <outputEntry><text>"high-value"</text></outputEntry>
      </rule>
      <rule id="r_neg">
        <inputEntry><text>&lt; 0</text></inputEntry>
        <outputEntry><text>"invalid-amount"</text></outputEntry>
      </rule>
      <rule id="r_huge">
        <inputEntry><text>&gt; 14000</text></inputEntry>
        <outputEntry><text>"manual-review"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// amount=15000 fires two rules (high-value + manual-review) → 2 issues
#[test]
fn collect_two_rules_fire_on_large_amount() {
    let v = validator(COLLECT_DMN, "creditCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(15000))]))
        .unwrap();
    assert_eq!(issues.len(), 2);
    let mut messages: Vec<&str> = issues.iter().map(|i| i.message.as_str()).collect();
    messages.sort_unstable();
    assert_eq!(messages, vec!["high-value", "manual-review"]);
    assert!(issues.iter().all(|i| i.code == codes::DMN_RULESET_FAILED));
}

/// amount=-100 fires only the 'invalid-amount' rule → 1 issue
#[test]
fn collect_negative_amount_fires_invalid_rule() {
    let v = validator(COLLECT_DMN, "creditCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(-100))]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].message, "invalid-amount");
    assert_eq!(issues[0].code, codes::DMN_RULESET_FAILED);
}

/// amount=5000 fires no rules → empty issue list
#[test]
fn collect_normal_amount_produces_no_issues() {
    let v = validator(COLLECT_DMN, "creditCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(5000))]))
        .unwrap();
    assert!(issues.is_empty());
}

/// name() returns the wrapped decision's id
#[test]
fn collect_name_is_decision_id() {
    assert_eq!(validator(COLLECT_DMN, "creditCheck").name(), "creditCheck");
}

// ------------------------------------------------------------------ UNIQUE policy

const UNIQUE_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:unique">
  <decision id="uniqueCheck" name="Unique Check">
    <decisionTable hitPolicy="UNIQUE">
      <input id="i1">
        <inputExpression typeRef="number">
          <text>amount</text>
        </inputExpression>
      </input>
      <output id="o1" name="issue" typeRef="string"/>
      <rule id="r_a">
        <inputEntry><text>&gt; 100</text></inputEntry>
        <outputEntry><text>"branch-a"</text></outputEntry>
      </rule>
      <rule id="r_b">
        <inputEntry><text>&gt; 200</text></inputEntry>
        <outputEntry><text>"branch-b"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// two rules fire under UNIQUE → single DMN_UNIQUE_VIOLATION issue (not the outputs)
#[test]
fn unique_violation_overrides_outputs() {
    let v = validator(UNIQUE_DMN, "uniqueCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(500))]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].code, codes::DMN_UNIQUE_VIOLATION);
    assert!(issues[0].message.contains("hitPolicy=UNIQUE"));
    assert!(issues[0].message.contains("r_a"));
    assert!(issues[0].message.contains("r_b"));
}

/// only one rule fires under UNIQUE → its output emits a single issue
#[test]
fn unique_with_single_firing_emits_output() {
    let v = validator(UNIQUE_DMN, "uniqueCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(150))]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].code, codes::DMN_RULESET_FAILED);
    assert_eq!(issues[0].message, "branch-a");
}

// ------------------------------------------------------------------ FIRST policy

const FIRST_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:first">
  <decision id="firstCheck">
    <decisionTable hitPolicy="FIRST">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1"/>
      <rule id="r1">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"first"</text></outputEntry>
      </rule>
      <rule id="r2">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"second"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// multiple rules match under FIRST → only the first rule's output emits an issue
#[test]
fn first_policy_picks_only_first_firing() {
    let v = validator(FIRST_DMN, "firstCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(50))]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].message, "first");
}

// ------------------------------------------------------------------ ANY policy

const ANY_DMN_AGREE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:any-agree">
  <decision id="anyAgreement">
    <decisionTable hitPolicy="ANY">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1"/>
      <rule id="r1">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"flagged"</text></outputEntry>
      </rule>
      <rule id="r2">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"flagged"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

const ANY_DMN_DISAGREE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:any-disagree">
  <decision id="anyDisagreement">
    <decisionTable hitPolicy="ANY">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1"/>
      <rule id="r1">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"flagA"</text></outputEntry>
      </rule>
      <rule id="r2">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"flagB"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// multiple rules fire with identical output → exactly one issue (canonical ANY)
#[test]
fn any_policy_agreement_collapses_to_single_issue() {
    let v = validator(ANY_DMN_AGREE, "anyAgreement");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(50))]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].message, "flagged");
}

/// rules fire with disagreeing outputs → DMN_ANY_HIT_POLICY_AMBIGUOUS
#[test]
fn any_policy_disagreement_surfaces_violation() {
    let v = validator(ANY_DMN_DISAGREE, "anyDisagreement");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(50))]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].code, codes::DMN_ANY_HIT_POLICY_AMBIGUOUS);
    assert!(issues[0].message.contains("hitPolicy=ANY"));
    assert!(issues[0].message.contains("r1, r2"));
}

/// single rule fires under ANY → single issue with original message (here: none fire)
#[test]
fn any_policy_with_single_firing_passes() {
    let v = validator(ANY_DMN_AGREE, "anyAgreement");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(0))]))
        .unwrap();
    assert!(issues.is_empty());
}

// ------------------------------------------------------------------ RULE ORDER policy

const RULE_ORDER_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:rule-order">
  <decision id="ruleOrderCheck">
    <decisionTable hitPolicy="RULE ORDER">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1"/>
      <rule id="r1">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"a"</text></outputEntry>
      </rule>
      <rule id="r2">
        <inputEntry><text>&gt; 0</text></inputEntry>
        <outputEntry><text>"b"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// RULE_ORDER preserves rule definition order in issues list
#[test]
fn rule_order_preserves_definition_order() {
    let v = validator(RULE_ORDER_DMN, "ruleOrderCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(50))]))
        .unwrap();
    assert_eq!(issues.len(), 2);
    assert_eq!(issues[0].message, "a");
    assert_eq!(issues[1].message, "b");
}

// ------------------------------------------------------------------ Custom bpm:code

const CUSTOM_CODE_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             xmlns:bpm="urn:sutra:dmn:annotations"
             namespace="urn:test:custom-code">
  <decision id="codedCheck">
    <decisionTable hitPolicy="COLLECT">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" bpm:code="SUTRA.VALIDATE.DOMAIN.AMOUNT_TOO_HIGH"/>
      <rule id="r1">
        <inputEntry><text>&gt; 1000</text></inputEntry>
        <outputEntry><text>"too high"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

/// bpm:code on the output overrides the default DMN_RULESET_FAILED code
#[test]
fn custom_code_overrides_default() {
    let v = validator(CUSTOM_CODE_DMN, "codedCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(5000))]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert_eq!(issues[0].code, "SUTRA.VALIDATE.DOMAIN.AMOUNT_TOO_HIGH");
    assert_eq!(issues[0].message, "too high");
}

// ------------------------------------------------------------------ Loader error handling

/// malformed DMN missing <decisionTable> → DmnError with DMN_FILE_PARSE_ERROR
#[test]
fn missing_decision_table_errors() {
    let bad = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/">
  <decision id="broken"/>
</definitions>
"#;
    let err = DmnFileLoader::new().load(bad.as_bytes()).unwrap_err();
    assert_eq!(err.code, codes::DMN_FILE_PARSE_ERROR);
    assert!(err.message.contains("decisionTable"), "{}", err.message);
}

/// malformed XML → DmnError with DMN_FILE_PARSE_ERROR
#[test]
fn malformed_xml_errors() {
    let bad = "<definitions xmlns=\"https://www.omg.org/spec/DMN/20230324/MODEL/\">not closed";
    let err = DmnFileLoader::new().load(bad.as_bytes()).unwrap_err();
    assert_eq!(err.code, codes::DMN_FILE_PARSE_ERROR);
}

/// wrong document element → DmnError with DMN_FILE_PARSE_ERROR
#[test]
fn wrong_root_element_errors() {
    let bad = r#"<?xml version="1.0" encoding="UTF-8"?>
<notDefinitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"/>
"#;
    assert!(DmnFileLoader::new().load(bad.as_bytes()).is_err());
}

/// empty input → DmnError with DMN_FILE_PARSE_ERROR
#[test]
fn empty_input_errors() {
    let err = DmnFileLoader::new().load(&[]).unwrap_err();
    assert_eq!(err.code, codes::DMN_FILE_PARSE_ERROR);
}

// ------------------------------------------------------------------ UnaryTest translator

/// > 100 → input > 100
#[test]
fn unary_greater_than() {
    assert_eq!(unary_test::to_full_expression("> 100"), "input > 100");
}

/// >= 100 → input >= 100 (two-char op handled before single-char)
#[test]
fn unary_greater_eq() {
    assert_eq!(unary_test::to_full_expression(">= 100"), "input >= 100");
}

/// = "USD" → input = "USD"
#[test]
fn unary_string_equality() {
    assert_eq!(
        unary_test::to_full_expression("= \"USD\""),
        "input = \"USD\""
    );
}

/// - (wildcard) → true
#[test]
fn unary_wildcard() {
    assert_eq!(unary_test::to_full_expression("-"), "true");
}

/// bare literal 42 → input = 42
#[test]
fn unary_bare_literal() {
    assert_eq!(unary_test::to_full_expression("42"), "input = 42");
}

/// "Medium","Low" (a disjunction list of string literals) → OR'd equality (DMN-TCK
/// 0004/0005/0006/0007-simpletable-*).
#[test]
fn unary_disjunction_list_of_string_literals() {
    assert_eq!(
        unary_test::to_full_expression("\"Medium\",\"Low\""),
        "input = \"Medium\" or input = \"Low\""
    );
}

/// <18,>=60 (a disjunction list of comparisons) → OR'd comparisons (DMN-TCK
/// 0020-vacation-days's decision tables).
#[test]
fn unary_disjunction_list_of_comparisons() {
    assert_eq!(
        unary_test::to_full_expression("<18,>=60"),
        "input < 18 or input >= 60"
    );
}

/// A comma inside a quoted literal does not split the disjunction.
#[test]
fn unary_disjunction_ignores_comma_inside_string() {
    assert_eq!(
        unary_test::to_full_expression("\"a,b\",\"c\""),
        "input = \"a,b\" or input = \"c\""
    );
}

/// [15..30) (a half-open interval literal) → membership via `in` (DMN-TCK
/// 0020-vacation-days's "Extra days case 3" decision table).
#[test]
fn unary_range_literal_becomes_membership() {
    assert_eq!(
        unary_test::to_full_expression("[15..30)"),
        "input in [15..30)"
    );
}

/// A bare `true`/`false` entry is a unary-test LITERAL ("input = true"), not a standalone
/// constant expression — DMN-TCK 0004-lending/0087-chapter-11-example's `RoutingRules`/
/// `Pre-bureau risk category table` BKMs: before this, a rule's bare `true`/`false` entry on a
/// boolean column fell to the pass-through branch and evaluated as the CONSTANT `true`/`false`,
/// firing (or never firing) unconditionally, completely ignoring the real input value.
#[test]
fn unary_bare_boolean_is_a_literal_not_a_standalone_expression() {
    assert_eq!(unary_test::to_full_expression("true"), "input = true");
    assert_eq!(unary_test::to_full_expression("false"), "input = false");
}

/// A bare navigable name/path endpoint (`Complex.aBoolean`, `Flu Symtoms`) is DMN Table 8.2's
/// "simple value" grammar for a unary test, not a standalone boolean expression — DMN-TCK
/// 0036-dt-variable-input's `Compare Boolean`/`Compare String`/… columns and
/// 0039-dt-list-semantics' list-typed `Flu Symtoms` endpoint. Translated to `input in <name>` so
/// FEEL's own `in` operator dispatches correctly on the referenced value's runtime type (scalar ->
/// equality, list -> membership) with no new evaluator work.
#[test]
fn unary_bare_reference_becomes_membership() {
    assert_eq!(
        unary_test::to_full_expression("Complex.aBoolean"),
        "input in Complex.aBoolean"
    );
    assert_eq!(
        unary_test::to_full_expression("Flu Symtoms"),
        "input in Flu Symtoms"
    );
}

/// `not(<bare reference>)` (DMN-TCK 0036's `not(Complex.aBoolean)`) translates to
/// `not(input in <inner>)` — the same bare-reference rule, negated.
#[test]
fn unary_negated_bare_reference_becomes_negated_membership() {
    assert_eq!(
        unary_test::to_full_expression("not(Complex.aBoolean)"),
        "not(input in Complex.aBoolean)"
    );
}

/// A genuine full-FEEL-expression pass-through (an operator/call present) is never mistaken for a
/// bare reference — `not(x > 3)`'s inner text has a comparison operator, so it keeps falling
/// through to the unchanged pass-through branch exactly as before this cycle's fix.
#[test]
fn unary_genuine_boolean_passthrough_is_unaffected_by_the_bare_reference_rule() {
    assert_eq!(unary_test::to_full_expression("not(x > 3)"), "not(x > 3)");
    assert_eq!(unary_test::to_full_expression("foo and bar"), "foo and bar");
}

// ------------------------------------------------------------------ Temporal rules
// Engine-injected `now` + secondsBetween(): FEEL is deterministic, so evaluation time enters
// as an injected input rather than a wall-clock builtin.

// The DMN re-expression of a "creation-timestamp-too-old" rule: the input
// expression reads the reserved `now` variable injected by the validator's clock.
const STALENESS_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/" namespace="urn:test:staleness">
  <decision id="stalenessCheck">
    <decisionTable hitPolicy="FIRST">
      <input id="i1"><inputExpression typeRef="number"><text>secondsBetween(createdAt, now)</text></inputExpression></input>
      <output id="o1" name="verdict" typeRef="string"/>
      <rule id="stale">
        <inputEntry><text>&gt; 300</text></inputEntry>
        <outputEntry><text>"Creation timestamp is older than 5 minutes [reasonCode=E990]"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>
"#;

fn staleness_validator_at(fixed_now: OffsetDateTime) -> DmnRulesetValidator {
    let defs = DmnFileLoader::new()
        .load(STALENESS_DMN.as_bytes())
        .expect("valid DMN");
    DmnRulesetValidator::with_clock(
        defs.decision("stalenessCheck").expect("decision").clone(),
        Box::new(FixedClock(fixed_now)),
    )
}

/// 361s-old message → stale rule fires under a fixed clock
#[test]
fn temporal_stale_message_fires() {
    let issues = staleness_validator_at(datetime!(2026-07-11 10:06:01 UTC))
        .validate_map(&ctx(vec![(
            "createdAt",
            FeelValue::from("2026-07-11T10:00:00Z"),
        )]))
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert!(issues[0].message.contains("reasonCode=E990"));
}

/// 240s-old message → no firing
#[test]
fn temporal_fresh_message_does_not_fire() {
    let issues = staleness_validator_at(datetime!(2026-07-11 10:04:00 UTC))
        .validate_map(&ctx(vec![(
            "createdAt",
            FeelValue::from("2026-07-11T10:00:00Z"),
        )]))
        .unwrap();
    assert!(issues.is_empty());
}

/// TypedPayloadEnvelope payloads project to {body: bodyAsMap()} — the envelope-codec shape.
///
/// Regression: the inbound chain feeds the codec's typed payload; for envelope codecs that
/// is NOT a map. The original map-typed signature raised a class-cast failure here and every
/// message was business-rejected as SUTRA.RUNTIME.VALIDATOR.UNCAUGHT.
#[test]
fn temporal_envelope_payload_projects_to_body_context() {
    let body = ctx(vec![
        (
            "header",
            map(vec![("createdAt", FeelValue::from("2026-07-11T10:00:00Z"))]),
        ),
        (
            "orderLine",
            map(vec![(
                "refs",
                map(vec![("correlationId", FeelValue::from("COR-1"))]),
            )]),
        ),
    ]);
    // Body-pathed table — the projection namespaces the envelope under `body.`, exactly
    // like a shipped envelope-pathed DMN (body.header.createdAt), unlike the flat-path table above.
    let body_pathed_dmn = STALENESS_DMN.replace(
        "secondsBetween(createdAt, now)",
        "secondsBetween(body.header.createdAt, now)",
    );
    let defs = DmnFileLoader::new()
        .load(body_pathed_dmn.as_bytes())
        .expect("valid DMN");
    let v = DmnRulesetValidator::with_clock(
        defs.decision("stalenessCheck").expect("decision").clone(),
        Box::new(FixedClock(datetime!(2026-07-11 10:06:01 UTC))),
    );
    let issues = v
        .validate(&DmnPayload::Envelope { body: Some(body) })
        .unwrap();
    assert_eq!(issues.len(), 1);
    assert!(issues[0].message.contains("reasonCode=E990"));
}

/// `now` is reserved — a same-named payload key cannot shadow the clock
#[test]
fn temporal_injected_now_wins_over_payload_key() {
    // Payload claims now = createdAt + 1s (would not fire); the engine clock says 361s.
    let issues = staleness_validator_at(datetime!(2026-07-11 10:06:01 UTC))
        .validate_map(&ctx(vec![
            ("createdAt", FeelValue::from("2026-07-11T10:00:00Z")),
            ("now", FeelValue::from("2026-07-11T10:00:01Z")),
        ]))
        .unwrap();
    assert_eq!(issues.len(), 1);
}

// ------------------------------------------------------------------ Registry (definitions-level)

/// Loading a multi-decision file exposes every decision by id — the definitions-level
/// register / lookup / decision-ids semantics.
#[test]
fn definitions_expose_every_decision_by_id() {
    let dmn = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/">
  <decision id="d1">
    <decisionTable hitPolicy="COLLECT">
      <input id="i1"><inputExpression><text>x</text></inputExpression></input>
      <output id="o1"/>
      <rule id="r1"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"ok"</text></outputEntry></rule>
    </decisionTable>
  </decision>
  <decision id="d2">
    <decisionTable hitPolicy="COLLECT">
      <input id="i1"><inputExpression><text>y</text></inputExpression></input>
      <output id="o1"/>
      <rule id="r1"><inputEntry><text>&gt; 0</text></inputEntry><outputEntry><text>"ok"</text></outputEntry></rule>
    </decisionTable>
  </decision>
</definitions>
"#;
    let defs = DmnFileLoader::new()
        .load(dmn.as_bytes())
        .expect("valid DMN");
    assert_eq!(defs.decision_ids(), vec!["d1", "d2"]);
    assert!(defs.decision("d1").is_some());
    assert!(defs.decision("missing").is_none());
}

// ------------------------------------------------------------------ severity sanity

/// All rule-output issues surface at ERROR severity (validation.outcome routing input).
#[test]
fn rule_output_issues_are_error_severity() {
    let v = validator(COLLECT_DMN, "creditCheck");
    let issues = v
        .validate_map(&ctx(vec![("amount", FeelValue::from(15000))]))
        .unwrap();
    assert!(issues.iter().all(|i| i.severity == Severity::Error));
    assert!(DmnRulesetValidator::returns_list(HitPolicy::Collect));
}
