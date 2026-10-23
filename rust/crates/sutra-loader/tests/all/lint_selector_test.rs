//! `<q:source filterExpression>` package-time gate — the intake-selector analysis end to end
//! through `lint_dir`.
//!
//! The case this unlocks is the one the uniqueness check used to refuse outright: two start
//! events on one non-broadcast channel claiming ONE message type, told apart by a
//! codec-reported attribute. The example attribute is a direction, deliberately — the engine
//! never names a message standard or a rail, and `rust/` is mirrored into the public repo.
//!
//! The closed-vocabulary half of the gate (unknown key, out-of-domain value, kind mismatch,
//! `NoneOf → OneOf`) is exercised in `selector.rs`'s own unit tests: no codec in this workspace
//! declares attributes, so a package fixture can only reach the OPEN-vocabulary paths.

use sutra_loader::{lint_dir, LintReport};

const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

/// One process whose start event subscribes to `channel`, optionally pinning a message type and
/// optionally carrying a `filterExpression`.
fn bpmn(
    process_id: &str,
    channel: &str,
    message_type: Option<&str>,
    filter: Option<&str>,
) -> String {
    let pin = message_type
        .map(|t| format!(r#" messageTypeValue="{t}""#))
        .unwrap_or_default();
    let predicate = filter
        .map(|f| format!(r#" filterExpression="{}""#, f.replace('"', "&quot;")))
        .unwrap_or_default();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_{process_id}"
                  targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="{process_id}" name="Demo" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="{channel}"{pin}{predicate} dedupKey="payload.id"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F1</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    )
}

fn channels_yaml(broadcast: bool) -> String {
    format!(
        "channels:\n  - name: demo-in\n    transport: http\n    bind: \"POST /channels/demo-in\"\n    broadcast: {broadcast}\n"
    )
}

/// A package directory with one subscriber per `(process, filter)` pair on `demo-in`.
fn package(
    subscribers: &[(&str, Option<&str>, Option<&str>)],
    broadcast: bool,
) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("package.yaml"), PACKAGE_YAML).unwrap();
    std::fs::write(dir.path().join("channels.yaml"), channels_yaml(broadcast)).unwrap();
    std::fs::create_dir_all(dir.path().join("bpmn")).unwrap();
    for (process_id, message_type, filter) in subscribers {
        std::fs::write(
            dir.path().join(format!("bpmn/{process_id}.bpmn")),
            bpmn(process_id, "demo-in", *message_type, *filter),
        )
        .unwrap();
    }
    dir
}

fn error_codes(report: &LintReport) -> Vec<String> {
    report.errors().map(|d| d.code.clone()).collect()
}

fn message_for(report: &LintReport, code: &str) -> String {
    report
        .diagnostics
        .iter()
        .find(|d| d.code == code)
        .map(|d| d.message.clone())
        .unwrap_or_else(|| panic!("no {code} in {:?}", report.diagnostics))
}

// ---- the case the gate exists to unlock -------------------------------------------------

#[test]
fn two_subscribers_on_one_message_type_are_accepted_when_their_predicates_are_disjoint() {
    let dir = package(
        &[
            (
                "inbound-flow",
                Some("Demo"),
                Some(r#"attr.direction = "incoming""#),
            ),
            (
                "outbound-flow",
                Some("Demo"),
                Some(r#"attr.direction = "outgoing""#),
            ),
        ],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        !report.has_errors(),
        "disjoint predicates must package, got: {:?}",
        error_codes(&report)
    );
}

#[test]
fn predicates_that_do_not_separate_two_subscribers_are_deploy_blocking() {
    let dir = package(
        &[
            (
                "first-flow",
                Some("Demo"),
                Some(r#"attr.direction = "incoming""#),
            ),
            (
                "second-flow",
                Some("Demo"),
                Some(r#"attr.direction != "outgoing""#),
            ),
        ],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.CHANNEL.SELECTOR_OVERLAP".to_string()),
        "got: {:?}",
        report.diagnostics
    );
    let message = message_for(&report, "SUTRA.CHANNEL.SELECTOR_OVERLAP");
    assert!(
        message.contains("attr.direction = \"incoming\""),
        "the witnessing assignment must be printed: {message}"
    );
    for escape in [
        "rewrite the predicate",
        "split the channel",
        "message-type clauses distinct",
        "broadcast: true",
    ] {
        assert!(
            message.contains(escape),
            "missing escape '{escape}': {message}"
        );
    }
}

#[test]
fn an_unanalysable_predicate_is_rejected_where_a_proof_is_required() {
    let dir = package(
        &[
            (
                "first-flow",
                Some("Demo"),
                Some(r#"attr.direction = "incoming""#),
            ),
            (
                "second-flow",
                Some("Demo"),
                Some(r#"if attr.direction = "outgoing" then true else false"#),
            ),
        ],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.CHANNEL.SELECTOR_NOT_ANALYSABLE".to_string()),
        "got: {:?}",
        report.diagnostics
    );
    let message = message_for(&report, "SUTRA.CHANNEL.SELECTOR_NOT_ANALYSABLE");
    assert!(
        message.contains("`if` expression") && message.contains("at offset"),
        "the construct and its FEEL offset must be named: {message}"
    );
    assert!(
        message.contains("second-flow"),
        "the offending process must be named: {message}"
    );
    for escape in [
        "rewrite the predicate",
        "split the channel",
        "message-type clauses distinct",
        "broadcast: true",
    ] {
        assert!(
            message.contains(escape),
            "missing escape '{escape}': {message}"
        );
    }
}

// ---- where no proof is required ---------------------------------------------------------

#[test]
fn a_lone_subscriber_accepts_any_predicate_including_an_unanalysable_one() {
    let dir = package(
        &[(
            "only-flow",
            Some("Demo"),
            Some(r#"if attr.direction = "incoming" then true else false"#),
        )],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        !report.has_errors(),
        "a lone subscriber needs no disjointness proof, got: {:?}",
        error_codes(&report)
    );
}

#[test]
fn a_broadcast_channel_accepts_predicates_it_cannot_separate() {
    let dir = package(
        &[
            (
                "first-flow",
                Some("Demo"),
                Some(r#"if attr.direction = "incoming" then true else false"#),
            ),
            (
                "second-flow",
                Some("Demo"),
                Some(r#"attr.direction = "incoming""#),
            ),
        ],
        true,
    );
    let report = lint_dir(dir.path());
    assert!(
        !report.has_errors(),
        "pub/sub is exempt from uniqueness, got: {:?}",
        error_codes(&report)
    );
}

// ---- per-subscriber faults --------------------------------------------------------------

#[test]
fn a_contradictory_predicate_is_a_dead_start_event() {
    let dir = package(
        &[(
            "only-flow",
            Some("Demo"),
            Some(r#"attr.direction = "incoming" and attr.direction = "outgoing""#),
        )],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.CHANNEL.SELECTOR_UNSATISFIABLE".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn an_open_attribute_vocabulary_warns_and_does_not_block() {
    let dir = package(
        &[(
            "only-flow",
            Some("Demo"),
            Some(r#"attr.direction = "incoming""#),
        )],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(!report.has_errors(), "got: {:?}", error_codes(&report));
    let warnings: Vec<&str> = report.diagnostics.iter().map(|d| d.code.as_str()).collect();
    assert!(
        warnings.contains(&"SUTRA.CONFIG.BPMN.SELECT_UNVERIFIABLE"),
        "an undeclared vocabulary is unverifiable, not wrong: {warnings:?}"
    );
    let message = message_for(&report, "SUTRA.CONFIG.BPMN.SELECT_UNVERIFIABLE");
    assert!(
        message.contains("direction"),
        "the unverifiable keys must be named: {message}"
    );
}

#[test]
fn a_non_deterministic_predicate_is_rejected() {
    let dir = package(
        &[(
            "only-flow",
            Some("Demo"),
            Some(r#"attr.direction = string(now())"#),
        )],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report)
            .iter()
            .any(|c| c.starts_with("SUTRA.FEEL.DETERMINISM")),
        "a routing decision may not read the clock, got: {:?}",
        report.diagnostics
    );
}

#[test]
fn an_unparseable_predicate_is_rejected_once() {
    let dir = package(
        &[("only-flow", Some("Demo"), Some(r#"attr.direction = "#))],
        false,
    );
    let report = lint_dir(dir.path());
    let codes = error_codes(&report);
    assert!(
        codes.iter().any(|c| c.starts_with("SUTRA.FEEL.")),
        "got: {:?}",
        report.diagnostics
    );
    assert!(
        !codes
            .iter()
            .any(|c| c.starts_with("SUTRA.CONFIG.BPMN.SELECT_")),
        "the parse failure is reported once, not again as a vocabulary fault: {codes:?}"
    );
}

// ---- the subsumed branch still behaves ---------------------------------------------------

#[test]
fn a_message_type_overlap_without_predicates_keeps_its_original_code() {
    let dir = package(
        &[
            ("first-flow", Some("Demo"), None),
            ("second-flow", Some("Demo"), None),
        ],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.CHANNEL.AMBIGUOUS_HANDLER".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn a_catch_all_overlap_without_predicates_keeps_its_original_code() {
    let dir = package(
        &[("first-flow", None, None), ("second-flow", None, None)],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        error_codes(&report).contains(&"SUTRA.CHANNEL.AMBIGUOUS_HANDLER".to_string()),
        "got: {:?}",
        report.diagnostics
    );
}

#[test]
fn two_catch_all_subscribers_separated_by_predicates_are_accepted() {
    let dir = package(
        &[
            ("inbound-flow", None, Some(r#"attr.direction = "incoming""#)),
            (
                "outbound-flow",
                None,
                Some(r#"attr.direction = "outgoing""#),
            ),
        ],
        false,
    );
    let report = lint_dir(dir.path());
    assert!(
        !report.has_errors(),
        "a predicate separates catch-alls too, got: {:?}",
        error_codes(&report)
    );
}
