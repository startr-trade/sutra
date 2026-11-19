//! `uncorrelated: { start: <processId> }` on an inbound channel: the named process must be one of
//! the deployment's and have a start event on the channel. Its reserved start does not compete with
//! the channel's ordinary starts in the non-broadcast uniqueness check.

use sutra_loader::lint_dir;

const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

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
  <bpmn:process id="late" isExecutable="true">
    <bpmn:startEvent id="LateStart">
      <bpmn:extensionElements><q:source channel="answers"/></bpmn:extensionElements>
      <bpmn:outgoing>L1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:endEvent id="LateEnd"><bpmn:incoming>L1</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="L1" sourceRef="LateStart" targetRef="LateEnd"/>
  </bpmn:process>
</bpmn:definitions>
"#;

fn channels(uncorrelated: &str) -> String {
    format!(
        "channels:\n  - name: orders\n    transport: http\n    bind: \"POST /channels/orders\"\n  - name: answers\n    transport: http\n    bind: \"POST /channels/answers\"\n{uncorrelated}"
    )
}

fn codes_for(channels_yaml: &str) -> Vec<(String, String)> {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("package.yaml"), PACKAGE_YAML).unwrap();
    std::fs::write(dir.path().join("channels.yaml"), channels_yaml).unwrap();
    std::fs::create_dir_all(dir.path().join("bpmn")).unwrap();
    std::fs::write(dir.path().join("bpmn/order.bpmn"), BPMN).unwrap();
    lint_dir(dir.path())
        .diagnostics
        .iter()
        .map(|d| (d.code.clone(), d.message.clone()))
        .collect()
}

fn uncorrelated_errors(channels_yaml: &str) -> Vec<String> {
    codes_for(channels_yaml)
        .into_iter()
        .filter(|(code, _)| code == "SUTRA.CONFIG.CHANNEL.UNCORRELATED_INVALID")
        .map(|(_, message)| message)
        .collect()
}

#[test]
fn an_uncorrelated_start_naming_a_process_with_a_start_on_the_channel_lints_clean() {
    let yaml = channels("    uncorrelated:\n      start: late\n");
    assert!(uncorrelated_errors(&yaml).is_empty());
    let found = codes_for(&yaml);
    assert!(
        !found
            .iter()
            .any(|(code, _)| code == "SUTRA.CONFIG.CHANNEL.START_AND_WAIT"
                || code.contains("AMBIGUOUS")),
        "a reserved start neither overlaps the wait nor competes with starts: {found:?}"
    );
}

#[test]
fn without_the_key_the_same_start_warns_that_it_shares_the_wait_channel() {
    let found = codes_for(&channels(""));
    assert!(
        found.iter().any(
            |(code, message)| code == "SUTRA.CONFIG.CHANNEL.START_AND_WAIT"
                && message.contains("'late'")
        ),
        "{found:?}"
    );
}

#[test]
fn an_uncorrelated_start_naming_an_unknown_process_is_a_lint_error() {
    let errors = uncorrelated_errors(&channels("    uncorrelated:\n      start: nobody\n"));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("'nobody'"), "{errors:?}");
    assert!(errors[0].contains("not a process"), "{errors:?}");
}

#[test]
fn an_uncorrelated_start_naming_a_process_without_a_start_on_the_channel_is_a_lint_error() {
    // `order` starts on `orders`, not on `answers`.
    let errors = uncorrelated_errors(&channels("    uncorrelated:\n      start: order\n"));
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].contains("no start event on channel 'answers'"),
        "{errors:?}"
    );
}

#[test]
fn a_malformed_uncorrelated_block_is_a_lint_error() {
    let found = codes_for(&channels("    uncorrelated:\n      begin: late\n"));
    assert!(
        found.iter().any(
            |(code, message)| code == "SUTRA.CONFIG.CHANNEL.UNCORRELATED_INVALID"
                && message.contains("unknown key 'begin'")
        ),
        "{found:?}"
    );
}
