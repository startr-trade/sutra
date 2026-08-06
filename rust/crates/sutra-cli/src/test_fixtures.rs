//! Shared test fixtures — BPMN files carried over from the reference baseline's test
//! suite, plus scratch-file helpers.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

/// Multi-task process with a gateway, channel source and extension attributes.
pub(crate) const BRANCHING_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:test:branching">
  <bpmn:process id="branching" name="BranchingProcess" isExecutable="true">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="branch-in"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Validate"/>
    <bpmn:serviceTask id="Validate" name="Validate" implementation="validate"
                      q:codec="json" q:validator="schema-v1" q:redactor="pii"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Validate" targetRef="GW"/>
    <bpmn:exclusiveGateway id="GW" name="OK?"/>
    <bpmn:sequenceFlow id="f3" sourceRef="GW" targetRef="Store"/>
    <bpmn:sequenceFlow id="f4" sourceRef="GW" targetRef="End"/>
    <bpmn:serviceTask id="Store" name="Persist" implementation="store"/>
    <bpmn:sequenceFlow id="f5" sourceRef="Store" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

/// Minimal linear process with one channel source.
pub(crate) const HELLO_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:test:hello">
  <bpmn:process id="hello" name="HelloProcess" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="hello-in"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Greet"/>
    <bpmn:serviceTask id="Greet" name="Say Hello"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Greet" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

static SCRATCH_SEQ: AtomicU32 = AtomicU32::new(0);

/// Creates a fresh scratch directory for a test.
pub(crate) fn scratch_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sutra-cli-test-{label}-{}-{}",
        std::process::id(),
        SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// Writes `content` under a fresh scratch directory and returns the file path.
pub(crate) fn scratch_file(label: &str, name: &str, content: &str) -> PathBuf {
    let dir = scratch_dir(label);
    let path = dir.join(name);
    std::fs::write(&path, content).expect("scratch file");
    path
}
