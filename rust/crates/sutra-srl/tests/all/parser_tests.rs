//! Parsing + parse-error diagnostics for the `.srl` DSL.

use sutra_srl::ast::Action;
use sutra_srl::{parse, SrlError};

fn parse_err(src: &str) -> SrlError {
    parse(src).expect_err("expected a parse error")
}

#[test]
fn multi_rule_ruleset_with_salience_and_activation_group() {
    let src = r#"
        // a two-rule ruleset with attributes and multi-action then-block
        rule "high"
          salience 10
          activation-group "amount"
        when
          payload.amount > 100
        then
          set(flag, true);
          report("CODE.A", "payload.amount", "amount too high");
        end

        rule "low"
          salience -5
        when
          payload.amount <= 100
        then
          set(flag, false);
        end
    "#;
    let rs = parse(src).expect("parses");
    assert_eq!(rs.rules.len(), 2);

    let high = &rs.rules[0];
    assert_eq!(high.name, "high");
    assert_eq!(high.salience, 10);
    assert_eq!(high.activation_group.as_deref(), Some("amount"));
    assert_eq!(high.decl_index, 0);
    assert_eq!(high.actions.len(), 2);
    assert!(matches!(&high.actions[0], Action::Set { target, .. } if target == "flag"));
    assert!(matches!(&high.actions[1], Action::Report { .. }));

    let low = &rs.rules[1];
    assert_eq!(low.name, "low");
    assert_eq!(low.salience, -5); // optional-sign integer
    assert_eq!(low.activation_group, None);
    assert_eq!(low.decl_index, 1);
    assert_eq!(low.actions.len(), 1);
}

#[test]
fn line_comments_are_ignored() {
    let src = r#"
        rule "r" // trailing comment after the name
        // full-line comment
        when payload.x > 0 // comment after the condition token stream
        then
          set(y, 1); // comment after an action
        end
    "#;
    let rs = parse(src).expect("parses with comments");
    assert_eq!(rs.rules.len(), 1);
    assert_eq!(rs.rules[0].actions.len(), 1);
}

#[test]
fn condition_with_parenthesised_if_then_else() {
    // The FEEL `then` sits at depth >= 1 (inside the parens), so the section `then` is the
    // depth-0 one after the parenthesised expression.
    let src = r#"
        rule "r"
        when (if payload.x > 0 then payload.x else 0) > 5
        then
          set(ok, true);
        end
    "#;
    let rs = parse(src).expect("parenthesised if/then/else parses");
    assert_eq!(rs.rules.len(), 1);
    assert_eq!(rs.rules[0].actions.len(), 1);
}

#[test]
fn empty_then_block_is_allowed() {
    let rs = parse(r#"rule "r" when payload.x > 0 then end"#).expect("zero actions is valid");
    assert_eq!(rs.rules[0].actions.len(), 0);
}

#[test]
fn escaped_quotes_in_rule_name() {
    let rs = parse(r#"rule "say \"hi\"" when true then end"#).expect("escaped quotes parse");
    assert_eq!(rs.rules[0].name, r#"say "hi""#);
}

#[test]
fn err_missing_end_has_line_col() {
    // Missing `end` — the error points at end-of-input.
    let err = parse_err("rule \"r\"\nwhen payload.x > 0\nthen\n  set(y, 1);");
    assert_eq!(err.code, "SUTRA.SRL.PARSE.SYNTAX_ERROR");
    assert!(
        err.message.contains("missing 'end'"),
        "msg: {}",
        err.message
    );
    // `line:col: message` display shape.
    assert!(format!("{err}").starts_with(&format!("{}:{}: ", err.line, err.column)));
}

#[test]
fn err_missing_then() {
    let err = parse_err("rule \"r\" when payload.x > 0 end");
    assert_eq!(err.code, "SUTRA.SRL.PARSE.SYNTAX_ERROR");
    assert!(
        err.message.contains("missing 'then'"),
        "msg: {}",
        err.message
    );
}

#[test]
fn err_missing_semicolon() {
    let err = parse_err(r#"rule "r" when true then set(y, 1) end"#);
    assert_eq!(err.code, "SUTRA.SRL.PARSE.SYNTAX_ERROR");
    assert!(err.message.contains("expected ;"), "msg: {}", err.message);
}

#[test]
fn err_report_wrong_arity() {
    let err = parse_err(r#"rule "r" when true then report("a", "b"); end"#);
    assert_eq!(err.code, "SUTRA.SRL.PARSE.BAD_ARITY");
    assert!(err.message.contains("exactly 3"), "msg: {}", err.message);
}

#[test]
fn err_unknown_verb() {
    let err = parse_err(r#"rule "r" when true then log("x"); end"#);
    assert_eq!(err.code, "SUTRA.SRL.PARSE.UNKNOWN_VERB");
    assert!(err.message.contains("log"), "msg: {}", err.message);
}

#[test]
fn err_insert_reserved_for_stateful_engine() {
    let err = parse_err(r#"rule "r" when true then insert(fact); end"#);
    assert_eq!(err.code, "SUTRA.SRL.PARSE.RESERVED_VERB");
    assert!(err.message.contains("stateful"), "msg: {}", err.message);
    assert!(
        err.message.contains("insert/retract"),
        "msg: {}",
        err.message
    );
}

#[test]
fn err_retract_reserved_for_stateful_engine() {
    let err = parse_err(r#"rule "r" when true then retract(fact); end"#);
    assert_eq!(err.code, "SUTRA.SRL.PARSE.RESERVED_VERB");
}

#[test]
fn err_embedded_feel_parse_error_carries_srl_position() {
    // The FEEL expression `payload. > 1` is malformed; the error must fold FEEL's offset onto
    // the `.srl` line/column.
    let err = parse_err("rule \"r\"\nwhen payload. > 1\nthen end");
    assert_eq!(err.code, "SUTRA.SRL.PARSE.FEEL_ERROR");
    assert_eq!(err.line, 2, "should point at the condition's line");
    assert!(
        err.message.contains("invalid FEEL expression"),
        "msg: {}",
        err.message
    );
}

#[test]
fn err_set_target_must_be_identifier() {
    let err = parse_err(r#"rule "r" when true then set(a.b, 1); end"#);
    assert_eq!(err.code, "SUTRA.SRL.PARSE.SYNTAX_ERROR");
    assert!(
        err.message.contains("bare identifier"),
        "msg: {}",
        err.message
    );
}

#[test]
fn err_single_quoted_rule_name_rejected() {
    // The DSL's STRING is double-quoted; a single-quoted rule name is a syntax error.
    let err = parse_err("rule 'r' when true then end");
    assert_eq!(err.code, "SUTRA.SRL.PARSE.SYNTAX_ERROR");
}

#[test]
fn err_unclosed_string() {
    let err = parse_err("rule \"r");
    assert_eq!(err.code, "SUTRA.SRL.PARSE.UNCLOSED_STRING");
}
