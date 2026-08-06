//! Handlebars rendering: interpolation without HTML escaping, conditionals and
//! subexpressions, nested/bracketed path access, the `coalesce`/`let`/`substring`/`replace`
//! helpers, standalone-block whitespace handling, fail-closed missing values and invalid
//! templates, and compiled-template reuse across renders.

use serde_json::{json, Value};
use sutra_templates::HandlebarsTemplateEngine;

fn engine() -> HandlebarsTemplateEngine {
    HandlebarsTemplateEngine::new()
}

fn render(e: &HandlebarsTemplateEngine, id: &str, tpl: &str, model: Value) -> String {
    e.render(id, tpl.as_bytes(), &model)
        .expect("render succeeds")
}

fn render_err(e: &HandlebarsTemplateEngine, id: &str, tpl: &str, model: Value) -> String {
    e.render(id, tpl.as_bytes(), &model)
        .expect_err("render fails")
        .message
}

#[test]
fn name_and_extensions() {
    let e = engine();
    assert_eq!(e.name(), "h");
    assert_eq!(e.extensions(), vec![".hbs"]);
}

#[test]
fn interpolates_from_map() {
    let e = engine();
    let out = render(
        &e,
        "t1",
        "<Id>{{endToEndId}}</Id>",
        json!({"endToEndId": "E2E-1"}),
    );
    assert_eq!(out, "<Id>E2E-1</Id>");
}

#[test]
fn renders_verbatim_not_html_escaped() {
    // EscapingStrategy.NOOP — {{ }} is raw, so XML metacharacters pass through unescaped.
    let e = engine();
    let out = render(&e, "t2", "<A>{{v}}</A>", json!({"v": "a<b>&c"}));
    assert_eq!(out, "<A>a<b>&c</A>");
}

#[test]
fn single_braces_are_literal_text() {
    // Some wire formats use single braces structurally — only {{ }} is template syntax.
    let e = engine();
    let out = render(
        &e,
        "t2b",
        "{1:F01PROXYGB2LAXXX0000000000}{2:I103X}{4:\n:20:{{ref}}\n-}",
        json!({"ref": "REF-1"}),
    );
    assert_eq!(
        out,
        "{1:F01PROXYGB2LAXXX0000000000}{2:I103X}{4:\n:20:REF-1\n-}"
    );
}

#[test]
fn conditional_section() {
    let e = engine();
    let tpl = "<S>{{#if reject}}RJCT{{else}}ACSC{{/if}}</S>";
    assert_eq!(
        render(&e, "t3", tpl, json!({"reject": true})),
        "<S>RJCT</S>"
    );
    assert_eq!(
        render(&e, "t3", tpl, json!({"reject": false})),
        "<S>ACSC</S>"
    );
}

#[test]
fn eq_subexpression_and_else_if_chain() {
    let e = engine();
    let tpl = r#"{{#if (eq c "DEBT")}}OUR{{else if (eq c "CRED")}}BEN{{else}}SHA{{/if}}"#;
    assert_eq!(render(&e, "t3b", tpl, json!({"c": "DEBT"})), "OUR");
    assert_eq!(render(&e, "t3b", tpl, json!({"c": "CRED"})), "BEN");
    assert_eq!(render(&e, "t3b", tpl, json!({"c": "other"})), "SHA");
    // an ABSENT condition value branches (params are null-tolerant), it does not strict-fail
    assert_eq!(render(&e, "t3b", tpl, json!({})), "SHA");
}

#[test]
fn nested_map_access() {
    let e = engine();
    let model = json!({"vars": {"firstReasonCode": "NARR"}});
    assert_eq!(
        render(&e, "t4", "<Cd>{{vars.firstReasonCode}}</Cd>", model),
        "<Cd>NARR</Cd>"
    );
}

#[test]
fn bracket_segment_reaches_dotted_keys_and_list_indexes() {
    let e = engine();
    let model = json!({
        "vars": {"validation.firstReasonCode": "AC03"},
        "bals": [{"id": "b0"}, {"id": "b1"}]
    });
    assert_eq!(
        render(
            &e,
            "t4b",
            "{{vars.[validation.firstReasonCode]}}|{{bals.[0].id}}|{{bals.[1].id}}",
            model
        ),
        "AC03|b0|b1"
    );
}

#[test]
fn missing_value_fails_render() {
    // Strict mode (R6, engine-enforced): a missing key, a typo'd nested path, or a null
    // mid-path aborts the render — it never silently renders empty.
    let e = engine();
    let msg = render_err(&e, "t5", "<A>{{nope}}</A>", json!({}));
    assert!(msg.contains("unresolved template reference"), "{msg}");
    let msg = render_err(&e, "t5b", "<A>{{a.nope}}</A>", json!({"a": {"x": 1}}));
    assert!(msg.contains("unresolved template reference"), "{msg}");
}

#[test]
fn coalesce_supplies_the_explicit_default() {
    // The strict-mode escape hatch for genuinely optional values.
    let e = engine();
    let tpl = r#"<Cd>{{coalesce vars.[validation.firstReasonCode] "E990"}}</Cd>"#;
    assert_eq!(render(&e, "t7", tpl, json!({"vars": {}})), "<Cd>E990</Cd>");
    assert_eq!(
        render(
            &e,
            "t7",
            tpl,
            json!({"vars": {"validation.firstReasonCode": "AC03"}})
        ),
        "<Cd>AC03</Cd>"
    );
    // path fallback: first non-null argument wins
    let f = json!({"f": {"20": {"value": "REF20"}}});
    assert_eq!(
        render(&e, "t7b", "{{coalesce f.[21].value f.[20].value}}", f),
        "REF20"
    );
}

#[test]
fn let_binds_block_param_and_always_renders_body() {
    let e = engine();
    let model = json!({"payload": {"body": {"Amt": {"Ccy": "USD"}}}});
    assert_eq!(
        render(
            &e,
            "t8",
            "{{#let payload.body.Amt as |amt|}}<C>{{amt.Ccy}}</C>{{/let}}",
            model
        ),
        "<C>USD</C>"
    );
    // unlike {{#with}}, a null/missing binding still renders the body (strict `{{#let}}` semantics)…
    assert_eq!(
        render(&e, "t8b", "{{#let nope as |x|}}ok{{/let}}", json!({})),
        "ok"
    );
    // …and USING the null binding then fails via strict mode instead of silently skipping
    let msg = render_err(&e, "t8c", "{{#let nope as |x|}}{{x.f}}{{/let}}", json!({}));
    assert!(msg.contains("unresolved template reference"), "{msg}");
}

#[test]
fn substring_and_replace_reshape_values() {
    let e = engine();
    let model = json!({"dt": "2026-05-22", "amt": {"value": "100.00"}});
    assert_eq!(
        render(
            &e,
            "t9",
            r#"{{substring dt 2 4}}{{substring dt 5 7}}{{substring dt 8 10}}|{{replace amt.value "." ","}}"#,
            model
        ),
        "260522|100,00"
    );
}

#[test]
fn standalone_block_lines_are_removed_variable_lines_kept() {
    // Mustache-spec standalone-line removal: block-only
    // lines vanish, variable-only lines stay.
    let e = engine();
    let tpl = "head\n{{#if (eq c \"X\")}}\nX-LINE\n{{else}}\nY-LINE\n{{/if}}\n{{v}}\ntail\n";
    assert_eq!(
        render(&e, "t10", tpl, json!({"c": "X", "v": "V"})),
        "head\nX-LINE\nV\ntail\n"
    );
}

#[test]
fn invalid_template_fails_clearly() {
    let e = engine();
    let msg = render_err(&e, "t6", "<A>{{#if x}}unterminated", json!({"x": true}));
    assert!(msg.contains("Invalid Handlebars template"), "{msg}");
}

#[test]
fn reuses_compiled_template_across_renders() {
    let e = engine();
    let tpl = "<V>{{v}}</V>";
    assert_eq!(render(&e, "same", tpl, json!({"v": "1"})), "<V>1</V>");
    assert_eq!(render(&e, "same", tpl, json!({"v": "2"})), "<V>2</V>");
}
