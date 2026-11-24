//! A queue-style broker intake hands each message to one consumer, but at least once: a sender's
//! outbox sends again when it cannot tell a send arrived, and a broker redelivers what a lost
//! consumer never settled. A catch-all start there with no key the engine treats as explicit runs
//! the same message twice, and the lint says so.

use sutra_loader::{lint_dir, LintReport};

const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const NO_IDEMPOTENCY_KEY: &str = "SUTRA.CHANNEL.NO_IDEMPOTENCY_KEY";

fn lint(process_extensions: &str, source_attributes: &str, channel: &str) -> LintReport {
    let root = std::env::temp_dir().join(format!(
        "lint-queue-dedup-{}-{}",
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
                  id="Definitions_relay"
                  targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="relay" name="Relay" isExecutable="true">
    <bpmn:extensionElements>{process_extensions}</bpmn:extensionElements>
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
    std::fs::write(root.join("bpmn/relay.bpmn"), bpmn).unwrap();
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

const AMQP_QUEUE: &str =
    "    transport: amqp\n    host: b\n    port: 5672\n    queue: q\n    address-type: queue\n";
const RABBITMQ_QUEUE: &str = "    transport: rabbitmq\n    host: b\n    port: 5672\n    queue: q\n";
const KAFKA_TOPIC: &str = "    transport: kafka\n    topic: t\n    group: g\n";

/// The shape that ran payments twice under overload: a relay taking everything on a queue whose
/// sender puts no application properties, so the message carries no outbox key of its own.
#[test]
fn a_catch_all_start_on_a_broker_queue_with_no_dedup_key_is_told_a_resend_runs_twice() {
    for channel in [AMQP_QUEUE, RABBITMQ_QUEUE, KAFKA_TOPIC] {
        let message = warning_of(&lint("", "", channel))
            .unwrap_or_else(|| panic!("no warning for a catch-all on:\n{channel}"));
        assert!(message.contains("runs twice"), "{message}");
        assert!(message.contains("'relay'"), "names the process: {message}");
        assert!(
            message.contains("amqp.message-id"),
            "names the fix: {message}"
        );
    }
}

#[test]
fn a_declared_key_a_key_header_or_an_idempotent_process_silences_the_queue_warning() {
    for (process, source, channel) in [
        ("", r#" dedupKey="amqp.message-id""#, AMQP_QUEUE),
        ("", r#" dedupKey="header.X-Event""#, RABBITMQ_QUEUE),
        ("", r#" dedupKey="body.id""#, KAFKA_TOPIC),
        (
            "",
            "",
            "    transport: amqp\n    host: b\n    port: 5672\n    queue: q\n    idempotency-key-header: X-Event\n",
        ),
        (r#"<q:process idempotent="true"/>"#, "", AMQP_QUEUE),
    ] {
        let found = warning_of(&lint(process, source, channel));
        assert_eq!(found, None, "{process} {source} on\n{channel}");
    }
}

/// An in-process hop always carries the sender's outbox key, and an HTTP caller learns of its own
/// failure: neither is a broker redelivery.
#[test]
fn a_catch_all_on_a_local_or_http_channel_is_not_warned() {
    for channel in [
        "    transport: local\n",
        "    transport: http\n    bind: \"POST /demo\"\n",
    ] {
        assert_eq!(warning_of(&lint("", "", channel)), None, "{channel}");
    }
}
