//! Static template analysis: which roots and `payload.*` paths a Handlebars template
//! actually reads. Block params, `@data` vars, `this` and helper/block names are never
//! roots (helper ARGUMENTS are); bracket segments resolve to their outer root; and a
//! construct whose key is only known at runtime (`{{lookup item @index}}`) is reported as
//! unresolvable while a literal key is not.

use sutra_templates::{HandlebarsTemplateEngine, TemplateAnalysis};

fn analyze(template: &str) -> TemplateAnalysis {
    HandlebarsTemplateEngine::new().analyze(template.as_bytes())
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

#[test]
fn reports_roots_and_payload_paths() {
    let a = analyze(
        r#"<X e2e="{{payload.E2EId}}" amt="{{payload.CdtTrfTxInf.IntrBkSttlmAmt}}" id="{{uuid}}" e="{{endToEndId}}"/>"#,
    );
    for root in ["payload", "uuid", "endToEndId"] {
        assert!(a.roots.iter().any(|r| r == root), "roots {:?}", a.roots);
    }
    assert_eq!(
        sorted(a.payload_paths),
        vec!["CdtTrfTxInf.IntrBkSttlmAmt", "E2EId"]
    );
}

#[test]
fn block_params_are_excluded_from_roots() {
    let a = analyze("{{#each payload.Items as |item|}}{{item.n}}{{/each}}");
    assert!(a.roots.iter().any(|r| r == "payload"));
    assert!(!a.roots.iter().any(|r| r == "item")); // block param excluded
    assert!(a.payload_paths.iter().any(|p| p == "Items"));
}

#[test]
fn data_vars_and_this_are_excluded() {
    let a = analyze("{{#each payload.Txs}}{{@index}}{{this}}{{/each}}");
    assert!(!a.roots.iter().any(|r| r == "@index" || r == "this"));
    assert!(a.roots.iter().any(|r| r == "payload"));
}

#[test]
fn showcase_template_has_no_spurious_roots() {
    let a = analyze(r#"<Hbs e2e="{{payload.E2EId}}" amount="{{payload.Amount}}"/>"#);
    assert_eq!(a.roots, vec!["payload"]);
    assert_eq!(sorted(a.payload_paths), vec!["Amount", "E2EId"]);
}

#[test]
fn helper_names_are_never_roots_but_their_arguments_are() {
    let a = analyze(
        r#"{{substring payload.Dt 2 4}}|{{replace amt.value "." ","}}|{{coalesce vars.[validation.firstReasonCode] "E990"}}"#,
    );
    assert_eq!(sorted(a.roots.clone()), vec!["amt", "payload", "vars"]);
    assert!(!a
        .roots
        .iter()
        .any(|r| r == "substring" || r == "replace" || r == "coalesce"));
    assert_eq!(a.payload_paths, vec!["Dt"]);
}

#[test]
fn let_block_reports_its_value_not_its_name() {
    let a = analyze("{{#let payload.body.Stmt as |stmt|}}<Id>{{stmt.Id}}</Id>{{/let}}");
    assert_eq!(a.roots, vec!["payload"]);
    assert!(!a.roots.iter().any(|r| r == "let" || r == "stmt"));
    assert_eq!(a.payload_paths, vec!["body.Stmt"]);
}

#[test]
fn subexpression_condition_arguments_are_roots() {
    let a = analyze(
        r#"{{#if (eq creditorAccount "111")}}A{{else if (eq creditorAccount "222")}}B{{/if}}"#,
    );
    assert_eq!(a.roots, vec!["creditorAccount"]);
    assert!(!a
        .roots
        .iter()
        .any(|r| r == "if" || r == "eq" || r == "else"));
}

#[test]
fn bracket_segments_resolve_to_the_outer_root() {
    let a = analyze("{{f.[71A].code}}{{vars.[validation.firstReasonCode]}}{{payload.[a.b].c}}");
    assert_eq!(sorted(a.roots), vec!["f", "payload", "vars"]);
    assert_eq!(a.payload_paths, vec!["a.b.c"]);
    // literal bracket keys are statically resolvable — never surfaced as unresolvable
    assert!(a.unresolvable.is_empty(), "got {:?}", a.unresolvable);
}

#[test]
fn dynamic_lookup_key_is_unresolvable() {
    // {{lookup item @index}} — the key is a runtime data var, so the referenced field cannot be
    // tied to a concrete schema path → surfaced as unresolvable (raw construct text).
    let a = analyze("{{#each payload.Items as |item|}}{{lookup item @index}}{{/each}}");
    assert_eq!(a.unresolvable, vec!["{{lookup item @index}}"]);
    // roots / payload_paths behavior is unchanged by the unresolvable pass
    assert!(a.roots.iter().any(|r| r == "payload"));
    assert!(a.payload_paths.iter().any(|p| p == "Items"));
}

#[test]
fn literal_lookup_key_is_resolvable() {
    // A quoted-string key and a numeric key are static → never surfaced as unresolvable.
    let a = analyze(r#"{{lookup payload.Items "Fixed"}}{{lookup payload.Rows 0}}"#);
    assert!(a.unresolvable.is_empty(), "got {:?}", a.unresolvable);
}
