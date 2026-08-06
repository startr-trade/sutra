//! Sequential-agenda evaluation semantics for the `.srl` engine.

use std::collections::BTreeMap;

use sutra_srl::{FeelContext, FeelValue, SrlRuleEngine};

fn eval(src: &str, input: &FeelContext) -> BTreeMap<String, FeelValue> {
    SrlRuleEngine::new()
        .evaluate("test.srl", src.as_bytes(), input)
        .expect("evaluate ok")
}

fn empty() -> FeelContext {
    FeelContext::new()
}

/// Extract the single issue map's field, or panic.
fn issue_field<'a>(issue: &'a FeelValue, key: &str) -> &'a FeelValue {
    match issue {
        FeelValue::Map(m) => m
            .get(key)
            .unwrap_or_else(|| panic!("issue missing key {key}")),
        other => panic!("issue is not a map: {other:?}"),
    }
}

#[test]
fn engine_identity() {
    let e = SrlRuleEngine::new();
    assert_eq!(e.name(), "srl");
    assert_eq!(e.extensions(), vec![".srl"]);
}

#[test]
fn salience_fires_highest_first_and_set_forward_updates() {
    // `seed` (salience 100) sets counter=1; `bump` (salience 1) sets counter=counter+1. Correct
    // order → 2; reversed order would leave 1 (bump would read a null counter first).
    let src = r#"
        rule "bump" salience 1 when true then set(counter, counter + 1); end
        rule "seed" salience 100 when true then set(counter, 1); end
    "#;
    let out = eval(src, &empty());
    assert_eq!(out.get("counter"), Some(&FeelValue::from(2i64)));
}

#[test]
fn equal_salience_keeps_declaration_order() {
    // Both salience 0. `a` fires first (declared first): seq = "a"; then `b`: seq = seq + "-b".
    let src = r#"
        rule "a" when true then set(seq, "a"); end
        rule "b" when true then set(seq, seq + "-b"); end
    "#;
    let out = eval(src, &empty());
    assert_eq!(out.get("seq"), Some(&FeelValue::String("a-b".to_string())));
}

#[test]
fn activation_group_first_match_wins() {
    // Both rules share group "g" and both conditions are true, but only the first-in-agenda
    // (higher salience) fires; the later same-group rule is skipped even though it would match.
    let src = r#"
        rule "winner"
          salience 10
          activation-group "g"
        when true
        then set(who, "winner");
        end

        rule "loser"
          salience 1
          activation-group "g"
        when true
        then set(who, "loser"); set(loser_ran, true);
        end
    "#;
    let out = eval(src, &empty());
    assert_eq!(
        out.get("who"),
        Some(&FeelValue::String("winner".to_string()))
    );
    assert_eq!(out.get("loser_ran"), None, "loser must not fire");
}

#[test]
fn set_forward_visibility_across_rules() {
    // First rule sets x=5; second rule's condition reads x and fires only because x > 3.
    let src = r#"
        rule "set-x" salience 10 when true then set(x, 5); end
        rule "read-x" salience 1 when x > 3 then set(y, true); end
    "#;
    let out = eval(src, &empty());
    assert_eq!(out.get("x"), Some(&FeelValue::from(5i64)));
    assert_eq!(out.get("y"), Some(&FeelValue::Boolean(true)));
}

#[test]
fn false_condition_does_not_fire() {
    let src = r#"rule "r" when false then set(x, 1); end"#;
    let out = eval(src, &empty());
    assert!(out.is_empty(), "nothing should be produced: {out:?}");
}

#[test]
fn report_produces_issue_with_exact_shape() {
    let src = r#"
        rule "r"
        when true
        then report("CODE.X", "payload.field", "something is wrong");
        end
    "#;
    let out = eval(src, &empty());
    let issues = match out.get("issues") {
        Some(FeelValue::List(xs)) => xs,
        other => panic!("expected issues list, got {other:?}"),
    };
    assert_eq!(issues.len(), 1);
    let issue = &issues[0];
    // Exactly the five frozen keys.
    if let FeelValue::Map(m) = issue {
        let keys: Vec<&str> = m.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["code", "message", "path", "severity", "value"]);
    } else {
        panic!("issue not a map");
    }
    assert_eq!(
        issue_field(issue, "code"),
        &FeelValue::String("CODE.X".to_string())
    );
    assert_eq!(
        issue_field(issue, "severity"),
        &FeelValue::String("ERROR".to_string())
    );
    assert_eq!(
        issue_field(issue, "path"),
        &FeelValue::String("payload.field".to_string())
    );
    assert_eq!(
        issue_field(issue, "message"),
        &FeelValue::String("something is wrong".to_string())
    );
    assert_eq!(issue_field(issue, "value"), &FeelValue::Null);
}

#[test]
fn no_issues_key_when_nothing_reported() {
    let src = r#"rule "r" when true then set(x, 1); end"#;
    let out = eval(src, &empty());
    assert!(!out.contains_key("issues"), "no issues expected: {out:?}");
    assert_eq!(out.get("x"), Some(&FeelValue::from(1i64)));
}

#[test]
fn report_coerces_non_string_arguments() {
    // code=1 (number) → "1", message=true (boolean) → "true". path stays a string.
    let src = r#"rule "r" when true then report(1, "p", true); end"#;
    let out = eval(src, &empty());
    let issues = match out.get("issues") {
        Some(FeelValue::List(xs)) => xs,
        other => panic!("expected issues list, got {other:?}"),
    };
    assert_eq!(
        issue_field(&issues[0], "code"),
        &FeelValue::String("1".to_string())
    );
    assert_eq!(
        issue_field(&issues[0], "message"),
        &FeelValue::String("true".to_string())
    );
}

#[test]
fn feel_eval_error_is_surfaced_as_srl_error() {
    // Comparing a String against a Number is a FEEL type-mismatch at eval time — fail-closed.
    let mut input = empty();
    input.insert("name".to_string(), FeelValue::String("alice".to_string()));
    let src = r#"rule "r" when name > 5 then set(x, 1); end"#;
    let err = SrlRuleEngine::new()
        .evaluate("test.srl", src.as_bytes(), &input)
        .expect_err("condition eval must fail");
    assert_eq!(err.code, "SUTRA.SRL.EVAL.FEEL_ERROR");
    assert!(
        err.message.contains("FEEL evaluation error"),
        "msg: {}",
        err.message
    );
}

#[test]
fn parse_error_aborts_evaluate_fail_closed() {
    let err = SrlRuleEngine::new()
        .evaluate("test.srl", b"rule \"r\" when true then", &empty())
        .expect_err("parse error aborts evaluate");
    assert!(err.code.starts_with("SUTRA.SRL.PARSE."));
}

#[test]
fn multiple_reports_accumulate_in_order() {
    let src = r#"
        rule "a" salience 10 when true then report("A", "p", "m1"); end
        rule "b" salience 1 when true then report("B", "p", "m2"); end
    "#;
    let out = eval(src, &empty());
    let issues = match out.get("issues") {
        Some(FeelValue::List(xs)) => xs,
        other => panic!("expected issues list, got {other:?}"),
    };
    assert_eq!(issues.len(), 2);
    assert_eq!(
        issue_field(&issues[0], "code"),
        &FeelValue::String("A".to_string())
    );
    assert_eq!(
        issue_field(&issues[1], "code"),
        &FeelValue::String("B".to_string())
    );
}
