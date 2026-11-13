//! `password hash` / `password verify` through the FEEL evaluator: the null rules, arity, and
//! the round trip a sign-in flow relies on.

use sutra_feel::expressions::eval;
use sutra_feel::{FeelContext, FeelValue};

fn ctx_with(pairs: &[(&str, FeelValue)]) -> FeelContext {
    let mut ctx = FeelContext::new();
    for (k, v) in pairs {
        ctx.insert((*k).to_string(), v.clone());
    }
    ctx
}

#[test]
fn password_hash_then_verify_through_feel_is_true() {
    let ctx = ctx_with(&[("pw", FeelValue::from("s3cret-pass"))]);
    let FeelValue::String(phc) = eval("password hash(pw)", &ctx).unwrap() else {
        panic!("password hash returns a string");
    };
    assert!(phc.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"), "{phc}");
    let ctx = ctx_with(&[
        ("pw", FeelValue::from("s3cret-pass")),
        ("stored", FeelValue::from(phc.as_str())),
    ]);
    assert_eq!(
        eval("password verify(pw, stored)", &ctx).unwrap(),
        FeelValue::Boolean(true)
    );
    assert_eq!(
        eval(r#"password verify("other", stored)"#, &ctx).unwrap(),
        FeelValue::Boolean(false)
    );
}

#[test]
fn password_hash_rejects_empty_and_non_string() {
    let ctx = FeelContext::new();
    for text in [
        r#"password hash("")"#,
        "password hash(null)",
        "password hash(42)",
        "password hash([1, 2])",
    ] {
        assert_eq!(eval(text, &ctx).unwrap(), FeelValue::Null, "{text}");
    }
}

#[test]
fn password_verify_non_string_arguments_are_null() {
    let ctx = FeelContext::new();
    for text in [
        r#"password verify(null, "x")"#,
        r#"password verify("x", null)"#,
        r#"password verify(1, "x")"#,
        r#"password verify("x", "not a hash")"#,
    ] {
        assert_eq!(eval(text, &ctx).unwrap(), FeelValue::Null, "{text}");
    }
}

#[test]
fn password_builtins_check_their_arity_without_naming_arguments() {
    let ctx = FeelContext::new();
    let err = eval(r#"password hash("a-secret", "b")"#, &ctx).unwrap_err();
    assert!(err.message.contains("password hash"), "{}", err.message);
    assert!(!err.message.contains("a-secret"), "{}", err.message);
    let err = eval(r#"password verify("a-secret")"#, &ctx).unwrap_err();
    assert!(err.message.contains("password verify"), "{}", err.message);
    assert!(!err.message.contains("a-secret"), "{}", err.message);
}
