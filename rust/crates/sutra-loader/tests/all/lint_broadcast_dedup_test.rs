//! A subscriber that receives its own copy of each event learns of a redelivery the hard way: one
//! failed subscriber brings the event back to all of them. Without an explicit dedup key the others
//! run it again, and the lint says so.

use sutra_loader::{lint_dir, LintReport};

const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const NO_IDEMPOTENCY_KEY: &str = "SUTRA.CHANNEL.NO_IDEMPOTENCY_KEY";

fn lint(source_attributes: &str, channel: &str) -> LintReport {
    let root = std::env::temp_dir().join(format!(
        "lint-broadcast-dedup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_flow"
                  targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="flow" name="Demo" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="demo-in"{source_attributes}/>
      </bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F1</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    std::fs::create_dir_all(root.join("bpmn")).unwrap();
    std::fs::write(root.join("package.yaml"), PACKAGE_YAML).unwrap();
    std::fs::write(root.join("bpmn/flow.bpmn"), bpmn).unwrap();
    std::fs::write(
        root.join("channels.yaml"),
        format!("channels:\n  - name: demo-in\n{channel}"),
    )
    .unwrap();
    let report = lint_dir(&root);
    let _ = std::fs::remove_dir_all(&root);
    report
}

fn warning_of(report: &LintReport) -> Option<String> {
    let found: Vec<&str> = report
        .diagnostics
        .iter()
        .filter(|d| d.code == NO_IDEMPOTENCY_KEY)
        .map(|d| d.message.as_str())
        .collect();
    assert!(found.len() <= 1, "warned more than once: {found:?}");
    found.first().map(|m| m.to_string())
}

const FLAGGED_BROADCAST: &str =
    "    transport: http\n    bind: \"POST /demo\"\n    broadcast: true\n";
const TOPIC: &str = "    transport: dapr\n    topic: t\n";
const SUBSCRIPTION: &str = "    transport: knative\n    subscription: s\n";
const AMQP_TOPIC: &str = "    transport: amqp\n    host: b\n    port: 5672\n    topic: t\n";

#[test]
fn a_broadcast_subscriber_with_no_dedup_key_is_told_a_redelivery_runs_twice() {
    for channel in [FLAGGED_BROADCAST, TOPIC, SUBSCRIPTION, AMQP_TOPIC] {
        let message = warning_of(&lint("", channel))
            .unwrap_or_else(|| panic!("no warning for a subscriber on:\n{channel}"));
        assert!(message.contains("can run twice"), "{message}");
        assert!(message.contains("flow"), "names the process: {message}");
    }
}

#[test]
fn a_typed_subscriber_on_a_topic_is_warned_once() {
    let message = warning_of(&lint(r#" messageTypeValue="t""#, TOPIC)).expect("warned");
    assert!(message.contains("can run twice"), "{message}");
}

/// A declared key is read at dispatch, whatever the channel says, and a key header is an explicit
/// signal of its own.
#[test]
fn a_declared_key_or_a_key_header_silences_it() {
    for (source, channel) in [
        (r#" dedupKey="body.id""#, TOPIC),
        (r#" dedupKey="ce.id""#, TOPIC),
        (r#" dedupKey="ce.id""#, SUBSCRIPTION),
        (
            r#" dedupKey="ce.id""#,
            "    transport: knative\n    subscription: s\n    cloudevents-mode: none\n",
        ),
        (r#" dedupKey="header.X-Event""#, TOPIC),
        (
            "",
            "    transport: dapr\n    topic: t\n    idempotency-key-header: X-Event\n",
        ),
    ] {
        let found = warning_of(&lint(source, channel));
        assert_eq!(found, None, "{source} on\n{channel}");
    }
}

#[test]
fn a_point_to_point_catch_all_is_not_a_broadcast_subscriber() {
    let channel = "    transport: http\n    bind: \"POST /demo\"\n";
    assert_eq!(warning_of(&lint("", channel)), None);
}

fn parse_error_of(report: &LintReport) -> String {
    let found: Vec<&str> = report
        .errors()
        .filter(|d| d.code == "SUTRA.PARSE.Q_SOURCE.DEDUP_KEY_UNRECOGNISED")
        .map(|d| d.message.as_str())
        .collect();
    assert_eq!(found.len(), 1, "{:?}", report.diagnostics);
    found[0].to_string()
}

/// Nothing would read these, so they would dedupe nothing and say so to nobody.
#[test]
fn an_unrecognised_dedup_key_form_is_an_error_naming_it_and_the_recognised_forms() {
    for form in [
        "payload.id",
        "message.id",
        "amqp.correlation-id",
        "CE.ID",
        "header.",
        "body.",
        "id",
    ] {
        let source = format!(r#" dedupKey="{form}""#);
        let message = parse_error_of(&lint(&source, TOPIC));
        assert!(message.contains(&format!("'{form}'")), "{message}");
        for recognised in ["header.<name>", "ce.id", "body.<path>", "amqp.message-id"] {
            assert!(message.contains(recognised), "{message}");
        }
    }
}

#[test]
fn every_recognised_dedup_key_form_lints_without_a_parse_error() {
    for form in ["header.X-Event", "ce.id", "body.id", "amqp.message-id"] {
        let source = format!(r#" dedupKey="{form}""#);
        let report = lint(&source, TOPIC);
        assert!(
            report
                .diagnostics
                .iter()
                .all(|d| d.code != "SUTRA.PARSE.Q_SOURCE.DEDUP_KEY_UNRECOGNISED"),
            "{form}: {:?}",
            report.diagnostics
        );
    }
}

/// `sutra package` refuses to seal one, so it never reaches a target.
#[test]
fn an_unrecognised_dedup_key_form_does_not_seal() {
    let root = std::env::temp_dir().join(format!("seal-unrecognised-{}", std::process::id()));
    let out = root.join("out");
    let package = root.join("pkg");
    std::fs::create_dir_all(package.join("bpmn")).unwrap();
    std::fs::write(package.join("package.yaml"), PACKAGE_YAML).unwrap();
    std::fs::write(
        package.join("bpmn/flow.bpmn"),
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="D" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="flow" name="Demo" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="demo-in" dedupKey="payload.id"/></bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F1</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#,
    )
    .unwrap();
    std::fs::write(
        package.join("channels.yaml"),
        "channels:\n  - name: demo-in\n    transport: http\n    bind: \"POST /demo\"\n",
    )
    .unwrap();
    let refused = sutra_loader::assemble_dir(&package, &out, &Default::default());
    let _ = std::fs::remove_dir_all(&root);
    let Err(error) = refused else {
        panic!("it sealed");
    };
    let message = format!("{error:?}");
    assert!(message.contains("DEDUP_KEY_UNRECOGNISED"), "{message}");
}

/// What a running package's log line has to say, all of it.
#[test]
fn a_running_package_is_told_what_its_form_does_and_what_would_work() {
    let found = sutra_loader::UnrecognisedDedupKey {
        process: "flow".to_string(),
        node: "Start".to_string(),
        form: "payload.id".to_string(),
    };
    let message = found.describe_running("acme--orders--1.0.0");
    for part in [
        "acme--orders--1.0.0",
        "payload.id",
        "dedupes nothing",
        "header.<name>",
        "ce.id",
        "body.<path>",
        "amqp.message-id",
    ] {
        assert!(message.contains(part), "{part}: {message}");
    }
}
