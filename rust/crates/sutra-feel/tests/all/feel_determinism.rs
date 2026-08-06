//! 1:1 port of `FeelDeterminismTest`.

use sutra_feel::codes;
use sutra_feel::determinism;
use sutra_feel::expressions;

#[test]
fn pure_expression_accepted() {
    let ast = expressions::parse("payload.uetr").unwrap();
    assert!(determinism::is_pure(&ast));

    // require_pure does not error
    determinism::require_pure(&ast, "alias 'trace-id'").unwrap();
}

#[test]
fn now_is_rejected() {
    let ast = expressions::parse("now()").unwrap();
    assert!(!determinism::is_pure(&ast));
    let denied = determinism::find_denied_calls(&ast);
    assert_eq!(denied.len(), 1);
    assert_eq!(denied[0].builtin, "now");
}

#[test]
fn today_in_complex_expression_rejected() {
    let ast = expressions::parse("payload.day + today()").unwrap();
    let err = determinism::require_pure(&ast, "alias 'batch-stamp'").unwrap_err();
    assert_eq!(err.code, codes::FEEL_DETERMINISM_UNSAFE_BUILTIN);
    assert!(err.message.contains("today"), "{}", err.message);
    assert!(
        err.message.contains("alias 'batch-stamp'"),
        "{}",
        err.message
    );
}

#[test]
fn uuid_and_random_are_rejected() {
    assert!(!determinism::is_pure(
        &expressions::parse("uuid()").unwrap()
    ));
    assert!(!determinism::is_pure(
        &expressions::parse("random()").unwrap()
    ));
}

#[test]
fn temporal_builtins_are_pure_with_injected_now_variable() {
    // secondsBetween()/isBlank() are pure functions of their arguments; `now` here is a
    // bare PATH (the engine-injected context variable at DMN evaluation sites), not the
    // banned now() call — the sanctioned temporal-rule shape.
    let ast = expressions::parse(
        "secondsBetween(payload.creDtTm, now) > 300 or isBlank(payload.endToEndId)",
    )
    .unwrap();
    assert!(determinism::is_pure(&ast));
    determinism::require_pure(&ast, "decision 'staleness'").unwrap();
}

#[test]
fn all_banned_builtins_listed() {
    // Sanity: every banned name produces a denied call when used.
    for banned in determinism::NON_DETERMINISTIC_BUILTINS {
        let ast = expressions::parse(&format!("{banned}()")).unwrap();
        assert!(
            !determinism::is_pure(&ast),
            "'{banned}' should be in the denylist and detected"
        );
    }
}

#[test]
fn nested_denied_call_inside_conditional_is_caught() {
    let ast = expressions::parse("if payload.x > 0 then now() else 'safe'").unwrap();
    assert!(!determinism::is_pure(&ast));
}

#[test]
fn denied_call_inside_argument_list_is_caught() {
    let ast = expressions::parse("matches(uuid(), \"foo\")").unwrap();
    assert!(!determinism::is_pure(&ast));
}

#[test]
fn denied_call_inside_invoke_callee_or_args_is_caught() {
    // `now()` as the callee of a postfix invocation, and as one of its arguments.
    assert!(!determinism::is_pure(
        &expressions::parse("now()(1)").unwrap()
    ));
    assert!(!determinism::is_pure(
        &expressions::parse("(function(a) a)(now())").unwrap()
    ));
}

#[test]
fn denied_call_inside_open_range_bound_is_caught() {
    assert!(!determinism::is_pure(
        &expressions::parse("(< now())").unwrap()
    ));
}

#[test]
fn hint_message_guides_remediation() {
    let err = expressions::require_pure("now()", "alias 'x'").unwrap_err();
    assert!(err.message.contains("now"), "{}", err.message);
    // The structured hint carries the remediation guidance; verifying its presence here
    // ensures we route to the right resolution.
    assert!(err.hint.is_some());
}
