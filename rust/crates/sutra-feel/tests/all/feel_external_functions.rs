//! FEEL `external` function definitions (grammar rule 55, DMN 1.4 §10.3.1.2 / §10.3.2.13.3):
//! `function(params) external {java: {…}}` / `{pmml: {…}}` must PARSE and evaluate to a function
//! VALUE (external functions are an optional DMN feature — defining one never errors), while any
//! INVOCATION is rejected with the deliberate semantic code
//! `SUTRA.FEEL.EVAL.EXTERNAL_UNSUPPORTED` (never a `SYNTAX.*` code — DMN-TCK
//! 0076-feel-external-java's errorResult cases are credited only on a non-syntax error).

use sutra_feel::expressions;
use sutra_feel::{codes, ExternalFunctionBinding, FeelContext, FeelValue};

fn empty() -> FeelContext {
    FeelContext::new()
}

const JAVA_COS_DEF: &str =
    r#"function(n1) external {java: {class: "java.lang.Math", method signature: "cos(double)"}}"#;

#[test]
fn external_java_definition_evaluates_to_a_function_value() {
    let v = expressions::eval(JAVA_COS_DEF, &empty()).expect("defining must not error");
    let FeelValue::Function(f) = v else {
        panic!("expected a function value, got {v:?}");
    };
    assert_eq!(f.params, vec!["n1"]);
    assert_eq!(
        f.external.as_deref(),
        Some(&ExternalFunctionBinding::Java {
            class: "java.lang.Math".to_string(),
            method_signature: "cos(double)".to_string(),
        })
    );
}

#[test]
fn external_pmml_definition_records_the_document_and_model() {
    let v = expressions::eval(
        r#"function(a, b) external {pmml: {document: "model.pmml", model: "iris"}}"#,
        &empty(),
    )
    .expect("defining must not error");
    let FeelValue::Function(f) = v else {
        panic!("expected a function value, got {v:?}");
    };
    assert_eq!(
        f.external.as_deref(),
        Some(&ExternalFunctionBinding::Pmml {
            document: "model.pmml".to_string(),
            model: Some("iris".to_string()),
        })
    );
}

/// `external` is NOT a reserved word — only the position immediately after a function
/// definition's `)` reads it as the keyword; a variable or context key named `external` keeps
/// working.
#[test]
fn external_stays_an_ordinary_name_outside_a_function_definition() {
    assert_eq!(
        expressions::eval("{external: 41}.external + 1", &empty()).unwrap(),
        FeelValue::num("42")
    );
    let ctx: FeelContext = [("external".to_string(), FeelValue::num("7"))]
        .into_iter()
        .collect();
    assert_eq!(
        expressions::eval("external * 2", &ctx).unwrap(),
        FeelValue::num("14")
    );
}

#[test]
fn invoking_an_external_function_is_a_semantic_error_not_a_syntax_error() {
    let err = expressions::eval(&format!("{{f: {JAVA_COS_DEF}, r: f(123)}}"), &empty())
        .expect_err("invocation must error");
    assert_eq!(err.code, codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED);
    assert!(
        err.message
            .contains("external function execution is not supported")
            && err.message.contains("java.lang.Math")
            && err.message.contains("cos(double)"),
        "message should name the binding: {}",
        err.message
    );
    // The TCK harness credits errorResult cases only on non-SYNTAX errors — the full display
    // form (code included) must never contain that substring.
    assert!(
        !err.to_string().contains("SYNTAX"),
        "must not be syntax-classed: {err}"
    );
}

#[test]
fn immediate_invocation_of_an_external_literal_errors_too() {
    // The postfix-Invoke path (callee is an expression, not a bare name) rejects the same way.
    let err = expressions::eval(&format!("({JAVA_COS_DEF})(1)"), &empty())
        .expect_err("invocation must error");
    assert_eq!(err.code, codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED);
}

#[test]
fn external_rejection_fires_regardless_of_arity() {
    // Wrong argument count would make an ordinary call `null` — the external rejection comes
    // FIRST, unconditionally.
    let err = expressions::eval(&format!("{{f: {JAVA_COS_DEF}, r: f(1, 2, 3)}}"), &empty())
        .expect_err("invocation must error");
    assert_eq!(err.code, codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED);
}

#[test]
fn malformed_external_body_defines_fine_and_reports_at_invocation() {
    // Body is a context but neither {java: …} nor {pmml: …} — defining still yields a function
    // value; invoking reports the shape problem (still the semantic code).
    let defined = expressions::eval("function() external {smalltalk: 1}", &empty())
        .expect("defining must not error");
    assert!(matches!(defined, FeelValue::Function(_)));
    let err = expressions::eval("{f: function() external {smalltalk: 1}, r: f()}", &empty())
        .expect_err("invocation must error");
    assert_eq!(err.code, codes::FEEL_EVAL_EXTERNAL_UNSUPPORTED);
    assert!(
        err.message.contains("not a valid java/pmml binding"),
        "message should report the malformed body: {}",
        err.message
    );
}
