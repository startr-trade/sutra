//! A channel whose messages only RESUME a waiting instance (a message catch, a user task, a
//! channel-call's reply) has no start-event subscriber and is not inert; a channel nothing
//! subscribes to at all still is.

use sutra_loader::lint_dir;

const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const CHANNELS: &str = "channels:\n  - name: orders\n    transport: http\n    bind: \"POST /channels/orders\"\n  - name: answers\n    transport: http\n    bind: \"POST /channels/answers\"\n  - name: unused\n    transport: http\n    bind: \"POST /channels/unused\"\n";

const BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="D" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="order" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="orders"/>
        <q:alias name="order" expression="payload.id" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="Answered">
      <bpmn:extensionElements><q:source channel="answers" name="answer"/></bpmn:extensionElements>
      <bpmn:incoming>F1</bpmn:incoming><bpmn:outgoing>F2</bpmn:outgoing>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="End"><bpmn:incoming>F2</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Answered"/>
    <bpmn:sequenceFlow id="F2" sourceRef="Answered" targetRef="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

#[test]
fn a_relay_only_channel_is_not_inert_an_unsubscribed_one_is() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("package.yaml"), PACKAGE_YAML).unwrap();
    std::fs::write(dir.path().join("channels.yaml"), CHANNELS).unwrap();
    std::fs::create_dir_all(dir.path().join("bpmn")).unwrap();
    std::fs::write(dir.path().join("bpmn/order.bpmn"), BPMN).unwrap();
    let report = lint_dir(dir.path());
    let inert: Vec<&str> = report
        .diagnostics
        .iter()
        .filter(|d| d.code == "SUTRA.CONFIG.CHANNEL.INERT")
        .map(|d| d.message.as_str())
        .collect();
    assert_eq!(inert.len(), 1, "{inert:?}");
    assert!(inert[0].contains("'unused'"), "{inert:?}");
}

/// A channel named only by another channel's `session-end: on-message` is not inert: its
/// messages correlate to an instance and end its session, with no wait in the flow taking them.
#[test]
fn a_session_end_channel_is_not_inert() {
    const SESSION_CHANNELS: &str = "channels:\n  - name: orders\n    transport: http\n    bind: \"POST /channels/orders\"\n    session-end:\n      on-message: closed\n  - name: answers\n    transport: http\n    bind: \"POST /channels/answers\"\n  - name: closed\n    transport: http\n    bind: \"POST /channels/closed\"\n";
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("package.yaml"), PACKAGE_YAML).unwrap();
    std::fs::write(dir.path().join("channels.yaml"), SESSION_CHANNELS).unwrap();
    std::fs::create_dir_all(dir.path().join("bpmn")).unwrap();
    std::fs::write(dir.path().join("bpmn/order.bpmn"), BPMN).unwrap();
    let report = lint_dir(dir.path());
    let inert: Vec<&str> = report
        .diagnostics
        .iter()
        .filter(|d| d.code == "SUTRA.CONFIG.CHANNEL.INERT")
        .map(|d| d.message.as_str())
        .collect();
    assert!(inert.is_empty(), "{inert:?}");
}
