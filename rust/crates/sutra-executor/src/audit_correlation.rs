//! The correlation keys an audit event carries: the instance's `<q:alias>` names and their
//! current values, redacted exactly as every other observability surface redacts.
//!
//! # Where the values come from
//!
//! No store is read. A start event's aliases are bound into the instance's variables under the
//! alias name when the instance spawns, and the variables travel in the instance snapshot, so the
//! value is a map lookup on every pass, including after a resume on another replica. An alias
//! declared on a wait node is evaluated over the current variables (a plain dotted path is walked
//! directly; anything else goes through the executor's FEEL evaluator). An alias whose value is
//! null, absent, or fails to evaluate is left out: the event carries the keys known at that point
//! in the instance.
//!
//! # Redaction
//!
//! An alias value is shown masked ([`sutra_bpmn::REDACTED_PLACEHOLDER`]) when the alias name, or
//! any variable its expression reads, is declared `sensitive`. When a variable the expression
//! reads has a redactor-produced masked projection (`<name>.redacted`), the value is taken from
//! that projection instead of the raw variable, or masked outright when the expression is not a
//! plain path. The raw value of a redacted input therefore never reaches an audit sink.

use std::collections::{BTreeMap, HashSet};

use sutra_bpmn::model::ProcessDefinition;
use sutra_bpmn::qbindings::AliasBinding;
use sutra_feel::value::FeelValue;

use crate::variables::Variables;

/// Evaluates one FEEL expression over the instance variables (the executor's value evaluator).
pub type AliasEvaluator<'a> = &'a dyn Fn(&str, &Variables) -> Option<FeelValue>;

/// Build the `alias name -> value` map for one audit event. Empty when the process declares no
/// `<q:alias>`. When two nodes declare the same alias name, the first declaration in document
/// order wins.
pub fn correlation_snapshot(
    process: &ProcessDefinition,
    variables: &Variables,
    evaluate: AliasEvaluator<'_>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut aliases: Vec<&AliasBinding> = Vec::new();
    for node in process.nodes() {
        for alias in &process.bindings_for(node.id()).aliases {
            if !aliases.iter().any(|a| a.name == alias.name) {
                aliases.push(alias);
            }
        }
    }
    if aliases.is_empty() {
        return out;
    }
    let sensitive: HashSet<&str> = process
        .declared_variables
        .iter()
        .filter(|d| d.sensitive)
        .map(|d| d.name.as_str())
        .collect();
    for alias in aliases {
        if let Some(value) = alias_value(alias, variables, &sensitive, evaluate) {
            out.insert(alias.name.clone(), value);
        }
    }
    out
}

/// One alias's rendered value, or `None` when it has none yet.
fn alias_value(
    alias: &AliasBinding,
    variables: &Variables,
    sensitive: &HashSet<&str>,
    evaluate: AliasEvaluator<'_>,
) -> Option<String> {
    let roots = expression_roots(&alias.expression);
    let path = plain_path(&alias.expression);
    let value = variables
        .get(&alias.name)
        .cloned()
        .or_else(|| match &path {
            Some(segments) => walk(variables.get(segments[0])?, &segments[1..]).cloned(),
            None => evaluate(&alias.expression, variables),
        })
        .filter(|v| !v.is_null())?;
    if sensitive.contains(alias.name.as_str()) {
        return Some(sutra_bpmn::REDACTED_PLACEHOLDER.to_string());
    }
    if let Some(masked) = masked_by_input(&roots, path.as_deref(), variables, sensitive) {
        return Some(masked);
    }
    if variables
        .get(&format!(
            "{}{}",
            alias.name,
            sutra_bpmn::REDACTION_COMPANION_SUFFIX
        ))
        .is_some()
    {
        return Some(sutra_bpmn::REDACTED_PLACEHOLDER.to_string());
    }
    Some(render(&value))
}

/// The masked rendering of an expression whose inputs are protected, or `None` when none is: a
/// `sensitive` input masks the value outright, and a redactor-controlled input (one with a
/// `<name>.redacted` projection) reads the same path from the projection. A non-path expression
/// over a redacted input cannot be re-run safely on the projection, so it is masked whole (fail
/// closed).
fn masked_by_input(
    roots: &[&str],
    path: Option<&[&str]>,
    variables: &Variables,
    sensitive: &HashSet<&str>,
) -> Option<String> {
    if roots.iter().any(|r| sensitive.contains(r)) {
        return Some(sutra_bpmn::REDACTED_PLACEHOLDER.to_string());
    }
    let root = roots.iter().find(|r| {
        variables
            .get(&format!("{r}{}", sutra_bpmn::REDACTION_COMPANION_SUFFIX))
            .is_some()
    })?;
    let projected = path.filter(|p| p[0] == *root).and_then(|p| {
        let companion =
            variables.get(&format!("{root}{}", sutra_bpmn::REDACTION_COMPANION_SUFFIX))?;
        walk(companion, &p[1..]).cloned()
    });
    Some(match projected {
        Some(v) if !v.is_null() => render(&v),
        _ => sutra_bpmn::REDACTED_PLACEHOLDER.to_string(),
    })
}

/// The fields of the intake's `event` map that carry no payload content. Everything else under
/// `event` (the raw body, the headers, a CloudEvent's data) repeats what a redactor exists to
/// hide, under a name that has no `.redacted` projection of its own.
const EVENT_METADATA_FIELDS: [&str; 5] = [
    "channel",
    "contentType",
    "dedupKey",
    "idempotencyKey",
    "receivedAt",
];

/// The `<q:receipt>` attributes of `process` over `variables`, redacted as every observability
/// surface redacts: this value leaves the engine. A null or a failed evaluation is left out; the
/// evaluator is expected to report a failure, since the work the receipt describes is already
/// accepted and must not fail over it.
pub fn receipt_attribute_snapshot(
    process: &ProcessDefinition,
    variables: &Variables,
    evaluate: AliasEvaluator<'_>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if process.receipt_attributes.is_empty() {
        return out;
    }
    let sensitive: HashSet<&str> = process
        .declared_variables
        .iter()
        .filter(|d| d.sensitive)
        .map(|d| d.name.as_str())
        .collect();
    let any_redactor = variables
        .as_feel_context()
        .keys()
        .any(|k| k.ends_with(sutra_bpmn::REDACTION_COMPANION_SUFFIX));
    for attribute in &process.receipt_attributes {
        let expression = attribute.expression.as_str();
        let Some(value) = evaluate(expression, variables).filter(|v| !v.is_null()) else {
            continue;
        };
        let roots = expression_roots(expression);
        let path = plain_path(expression);
        let shown = masked_by_input(&roots, path.as_deref(), variables, &sensitive).or_else(|| {
            let metadata = path
                .as_ref()
                .is_some_and(|p| p.len() == 2 && EVENT_METADATA_FIELDS.contains(&p[1]));
            (any_redactor && roots.contains(&"event") && !metadata)
                .then(|| sutra_bpmn::REDACTED_PLACEHOLDER.to_string())
        });
        out.insert(
            attribute.name.clone(),
            shown.unwrap_or_else(|| render(&value)),
        );
    }
    out
}

/// Whether `expression` reads the variable `name`, by the rule redaction itself uses: over-
/// inclusive, because a false positive only means a warning about a value that is not masked.
pub fn expression_reads(expression: &str, name: &str) -> bool {
    expression_roots(expression).contains(&name)
}

fn render(value: &FeelValue) -> String {
    sutra_feel::value::canonical_string_of(value)
}

/// Follow `segments` through nested maps.
fn walk<'v>(value: &'v FeelValue, segments: &[&str]) -> Option<&'v FeelValue> {
    let mut current = value;
    for segment in segments {
        match current {
            FeelValue::Map(m) => current = m.get(*segment)?,
            _ => return None,
        }
    }
    Some(current)
}

/// `Some(["a", "b", "c"])` when the expression is exactly `a.b.c` with identifier segments.
fn plain_path(expression: &str) -> Option<Vec<&str>> {
    let segments: Vec<&str> = expression.trim().split('.').collect();
    segments
        .iter()
        .all(|s| is_identifier(s))
        .then_some(segments)
}

fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Every identifier the expression could read as a variable: identifiers outside string
/// literals that do not follow a `.`. Over-inclusive on purpose (function names and keywords
/// are listed too): the result only decides what to mask, so listing too much is safe.
fn expression_roots(expression: &str) -> Vec<&str> {
    let bytes = expression.as_bytes();
    let mut roots = Vec::new();
    let mut i = 0;
    let mut after_dot = false;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'"' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            i += 1;
            after_dot = false;
        } else if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            if !after_dot {
                roots.push(&expression[start..i]);
            }
            after_dot = false;
        } else {
            if !c.is_ascii_whitespace() {
                after_dot = c == b'.';
            }
            i += 1;
        }
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process(xml_vars: &str, start_aliases: &str, wait_aliases: &str) -> ProcessDefinition {
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="D" targetNamespace="urn:t">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:extensionElements><q:variables>{xml_vars}</q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>{start_aliases}</bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Wait"/>
    <bpmn:intermediateCatchEvent id="Wait">
      <bpmn:extensionElements>{wait_aliases}</bpmn:extensionElements>
      <bpmn:incoming>F1</bpmn:incoming><bpmn:outgoing>F2</bpmn:outgoing>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="F2" sourceRef="Wait" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#
        );
        sutra_bpmn::BpmnModelLoader::new()
            .load(xml.as_bytes())
            .expect("fixture loads")
            .process("p")
            .expect("one process")
            .clone()
    }

    fn map(pairs: &[(&str, FeelValue)]) -> FeelValue {
        FeelValue::Map(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    fn s(v: &str) -> FeelValue {
        FeelValue::String(v.to_string())
    }

    fn no_eval(_: &str, _: &Variables) -> Option<FeelValue> {
        None
    }

    /// A process exposing `attributes` (`(name, expression)`) over `sensitive`-declared variables.
    fn receipt_process(sensitive: &[&str], attributes: &[(&str, &str)]) -> ProcessDefinition {
        let vars: String = sensitive
            .iter()
            .map(|n| format!(r#"<q:variable name="{n}" sensitive="true"/>"#))
            .collect();
        let mut p = process(&vars, "", "");
        p.receipt_attributes = attributes
            .iter()
            .map(
                |(name, expression)| sutra_bpmn::qbindings::ReceiptAttribute {
                    name: name.to_string(),
                    expression: expression.to_string(),
                },
            )
            .collect();
        p
    }

    /// The evaluator for plain variable names and `a.b` paths, as FEEL answers them.
    fn path_eval(expression: &str, vars: &Variables) -> Option<FeelValue> {
        let mut parts = expression.split('.');
        let mut current = vars.get(parts.next()?)?.clone();
        for part in parts {
            match current {
                FeelValue::Map(m) => current = m.get(part)?.clone(),
                _ => return None,
            }
        }
        Some(current)
    }

    #[test]
    fn a_process_without_a_receipt_exposes_nothing() {
        let p = receipt_process(&[], &[]);
        let vars: Variables = [("x".to_string(), s("1"))].into_iter().collect();
        assert!(receipt_attribute_snapshot(&p, &vars, &path_eval).is_empty());
    }

    #[test]
    fn a_receipt_attribute_renders_or_is_left_out() {
        let p = receipt_process(&[], &[("caseId", "req.id"), ("gone", "req.nope")]);
        let vars: Variables = [("req".to_string(), map(&[("id", s("C-1"))]))]
            .into_iter()
            .collect();
        let got = receipt_attribute_snapshot(&p, &vars, &path_eval);
        assert_eq!(got.get("caseId").map(String::as_str), Some("C-1"));
        assert!(!got.contains_key("gone"), "a null is left out");
    }

    #[test]
    fn a_receipt_attribute_over_a_sensitive_variable_is_masked() {
        let p = receipt_process(&["ssn"], &[("holder", "ssn")]);
        let vars: Variables = [("ssn".to_string(), s("000"))].into_iter().collect();
        let got = receipt_attribute_snapshot(&p, &vars, &path_eval);
        assert_eq!(
            got.get("holder").map(String::as_str),
            Some(sutra_bpmn::REDACTED_PLACEHOLDER)
        );
    }

    #[test]
    fn a_receipt_attribute_over_a_redacted_input_reads_the_projection() {
        let p = receipt_process(&[], &[("card", "req.card"), ("ref", "req.ref")]);
        let vars: Variables = [
            (
                "req".to_string(),
                map(&[("card", s("4111")), ("ref", s("R-1"))]),
            ),
            (
                format!("req{}", sutra_bpmn::REDACTION_COMPANION_SUFFIX),
                map(&[("card", s("****")), ("ref", s("R-1"))]),
            ),
        ]
        .into_iter()
        .collect();
        let got = receipt_attribute_snapshot(&p, &vars, &path_eval);
        assert_eq!(got.get("card").map(String::as_str), Some("****"));
        assert_eq!(got.get("ref").map(String::as_str), Some("R-1"));
    }

    #[test]
    fn a_receipt_attribute_cannot_read_a_redacted_payload_through_event() {
        let p = receipt_process(&[], &[("raw", "event.body"), ("channel", "event.channel")]);
        let vars: Variables = [
            (
                "event".to_string(),
                map(&[("body", s("{\"card\":\"4111\"}")), ("channel", s("c"))]),
            ),
            ("req".to_string(), map(&[("card", s("4111"))])),
            (
                format!("req{}", sutra_bpmn::REDACTION_COMPANION_SUFFIX),
                map(&[("card", s("****"))]),
            ),
        ]
        .into_iter()
        .collect();
        let got = receipt_attribute_snapshot(&p, &vars, &path_eval);
        assert_eq!(
            got.get("raw").map(String::as_str),
            Some(sutra_bpmn::REDACTED_PLACEHOLDER)
        );
        assert_eq!(got.get("channel").map(String::as_str), Some("c"));
    }

    #[test]
    fn a_process_without_aliases_has_no_correlation_keys() {
        let p = process("", "", "");
        let vars: Variables = [("x".to_string(), s("1"))].into_iter().collect();
        assert!(correlation_snapshot(&p, &vars, &no_eval).is_empty());
    }

    #[test]
    fn a_start_alias_reads_its_bound_variable() {
        let p = process(
            "",
            r#"<q:alias name="orderRef" expression="payload.ref" unique="true"/>"#,
            "",
        );
        let vars: Variables = [
            ("orderRef".to_string(), s("R-1")),
            ("payload".to_string(), map(&[("ref", s("R-1"))])),
        ]
        .into_iter()
        .collect();
        let got = correlation_snapshot(&p, &vars, &no_eval);
        assert_eq!(got.get("orderRef").map(String::as_str), Some("R-1"));
    }

    #[test]
    fn a_wait_alias_walks_a_plain_path_and_skips_an_absent_value() {
        let p = process(
            "",
            "",
            r#"<q:alias name="replyRef" expression="reply.header.ref"/>
               <q:alias name="later" expression="notYet.ref"/>"#,
        );
        let vars: Variables = [(
            "reply".to_string(),
            map(&[("header", map(&[("ref", s("H-9"))]))]),
        )]
        .into_iter()
        .collect();
        let got = correlation_snapshot(&p, &vars, &no_eval);
        assert_eq!(got.get("replyRef").map(String::as_str), Some("H-9"));
        assert!(!got.contains_key("later"), "absent value is left out");
    }

    #[test]
    fn a_non_path_wait_alias_goes_through_the_evaluator() {
        let p = process("", "", r#"<q:alias name="k" expression="upper case(a)"/>"#);
        let vars: Variables = [("a".to_string(), s("x"))].into_iter().collect();
        let eval = |expr: &str, _: &Variables| -> Option<FeelValue> {
            assert_eq!(expr, "upper case(a)");
            Some(s("X"))
        };
        let got = correlation_snapshot(&p, &vars, &eval);
        assert_eq!(got.get("k").map(String::as_str), Some("X"));
    }

    #[test]
    fn a_sensitive_alias_name_or_input_is_masked() {
        let p = process(
            r#"<q:variable name="holder" type="string" sensitive="true"/>
               <q:variable name="secretInput" sensitive="true"/>"#,
            r#"<q:alias name="holder" expression="payload.holder"/>
               <q:alias name="derived" expression="secretInput.id"/>"#,
            "",
        );
        let vars: Variables = [
            ("holder".to_string(), s("Jane Doe")),
            ("secretInput".to_string(), map(&[("id", s("S-1"))])),
        ]
        .into_iter()
        .collect();
        let got = correlation_snapshot(&p, &vars, &no_eval);
        assert_eq!(
            got.get("holder").map(String::as_str),
            Some(sutra_bpmn::REDACTED_PLACEHOLDER)
        );
        assert_eq!(
            got.get("derived").map(String::as_str),
            Some(sutra_bpmn::REDACTED_PLACEHOLDER)
        );
        assert!(!format!("{got:?}").contains("Jane"), "{got:?}");
    }

    #[test]
    fn a_redactor_projection_replaces_the_raw_input() {
        let p = process(
            "",
            r#"<q:alias name="acct" expression="payload.account"/>
               <q:alias name="ref" expression="payload.ref"/>
               <q:alias name="mixed" expression="string(payload.account)"/>"#,
            "",
        );
        let vars: Variables = [
            // The start path bound the RAW value under the alias name.
            ("acct".to_string(), s("12345678")),
            ("ref".to_string(), s("R-2")),
            (
                "payload".to_string(),
                map(&[("account", s("12345678")), ("ref", s("R-2"))]),
            ),
            (
                format!("payload{}", sutra_bpmn::REDACTION_COMPANION_SUFFIX),
                map(&[("account", s("****5678")), ("ref", s("R-2"))]),
            ),
        ]
        .into_iter()
        .collect();
        let eval = |_: &str, _: &Variables| Some(s("12345678"));
        let got = correlation_snapshot(&p, &vars, &eval);
        assert_eq!(got.get("acct").map(String::as_str), Some("****5678"));
        assert_eq!(got.get("ref").map(String::as_str), Some("R-2"));
        assert_eq!(
            got.get("mixed").map(String::as_str),
            Some(sutra_bpmn::REDACTED_PLACEHOLDER),
            "a non-path expression over a redacted input is masked whole"
        );
        assert!(!format!("{got:?}").contains("12345678"), "{got:?}");
    }

    #[test]
    fn roots_skip_members_and_string_literals() {
        assert_eq!(
            expression_roots(r#"a.b + c["x"] and "lit.d" = e . f"#),
            vec!["a", "c", "and", "e"]
        );
    }
}
