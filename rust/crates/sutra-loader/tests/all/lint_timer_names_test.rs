//! Timer expressions may read only names something in scope provides (0.6.5): `config`, declared
//! variables / data objects, the payload, or a variable the process sets. Anything else is a lint
//! error; a mistyped literal (already refused as a malformed literal) now names the literal that
//! was meant ("did you mean PT10S?").

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const UNKNOWN: &str = "SUTRA.CONFIG.BPMN.TIMER_EXPRESSION_UNKNOWN_NAME";

const LABELS: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"timers\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\nproperties:\n  window: \"PT1M\"\n";

/// A flow whose catch timer reads `BODY` and whose start runs a FEEL assignment of `delay`.
fn flow(body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:timers:1.0.0">
  <bpmn:process id="waits" isExecutable="true">
    <bpmn:extensionElements>
      <q:variables><q:variable name="declared" type="string"/></q:variables>
    </bpmn:extensionElements>
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Set">
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>"PT2S"</bpmn:from><bpmn:to>delay</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:intermediateCatchEvent id="Wait">
      <bpmn:timerEventDefinition><bpmn:timeDuration>{body}</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="Set"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Set" targetRef="Wait"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
    )
}

fn lint(body: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let write = |rel: &str, content: &str| {
        let path = dir.path().join(rel);
        std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write("package.yaml", LABELS);
    write("bpmn/waits.bpmn", &flow(body));
    lint_dir(dir.path())
}

fn unknown(report: &LintReport) -> Vec<String> {
    report
        .errors()
        .filter(|d| d.code == UNKNOWN)
        .map(|d| d.message.clone())
        .collect()
}

#[test]
fn names_in_scope_lint_clean() {
    for body in [
        "PT10S",
        "config.window",
        "delay",
        "declared",
        "if payload.fast then \"PT1S\" else config.window",
    ] {
        let report = lint(body);
        assert!(
            unknown(&report).is_empty(),
            "{body}: {:?}",
            unknown(&report)
        );
    }
}

#[test]
fn a_mistyped_literal_is_refused_and_the_literal_is_named() {
    // Literal-shaped: refused as a malformed literal (as since 0.6.4), now naming the literal.
    let report = lint("PT10SEC");
    let messages: Vec<String> = report.errors().map(|d| d.message.clone()).collect();
    assert!(
        messages.iter().any(|m| m.contains("did you mean PT10S?")),
        "{messages:?}"
    );
}

#[test]
fn an_unknown_variable_is_refused_without_a_literal_hint() {
    let messages = unknown(&lint("retryWindow"));
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert!(messages[0].contains("'retryWindow'"), "{}", messages[0]);
    assert!(!messages[0].contains("did you mean"), "{}", messages[0]);
}
