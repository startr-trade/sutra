//! `q:stream` and `reply-stream: sse` are checked at deploy time: a streamed task must call a
//! synchronous `llm` channel (only its answer, made inline, can stream to the caller), and only
//! an inbound `http` channel answering `on-complete` can serve a streamed reply.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const TARGET: &str = "SUTRA.CONFIG.STREAM.TARGET_NOT_SYNC_LLM";
const REPLY_STREAM: &str = "SUTRA.CONFIG.STREAM.REPLY_STREAM_UNSUPPORTED";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// `inbound` is the start channel's extra YAML; `target` the called channel's whole entry.
fn report(inbound: &str, target: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "package.yaml", PACKAGE_YAML);
    write(
        root,
        "channels.yaml",
        &format!(
            "channels:\n\
             \x20 - name: in-ch\n    transport: http\n    bind: \"POST /channels/in-ch\"\n    auth:\n      scheme: apikey\n      apikey:\n        value: k\n{inbound}\
             \x20 - name: model\n{target}"
        ),
    );
    write(
        root,
        "bpmn/flow.bpmn",
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in-ch"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Ask" implementation="channel:model" q:stream="true">
      <bpmn:extensionElements>
        <q:timeout duration="PT30S"/>
        <q:alias name="k" expression="'x'"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Ask"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Ask" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>
"#,
    );
    lint_dir(root)
}

fn has(report: &LintReport, code: &str) -> bool {
    report.diagnostics.iter().any(|d| d.code == code)
}

const SYNC_LLM: &str =
    "    direction: outbound\n    transport: llm\n    ack-mode: on-complete\n    bind: llm://model\n";

#[test]
fn a_streamed_call_to_a_sync_llm_channel_passes() {
    let r = report("", SYNC_LLM);
    assert!(!has(&r, TARGET), "{:#?}", r.diagnostics);
}

#[test]
fn a_streamed_call_to_an_async_llm_channel_is_an_error() {
    let r = report(
        "",
        "    direction: outbound\n    transport: llm\n    bind: llm://model\n",
    );
    assert!(has(&r, TARGET), "{:#?}", r.diagnostics);
}

#[test]
fn a_streamed_call_to_a_sync_non_llm_channel_is_an_error() {
    let r = report(
        "",
        "    direction: outbound\n    transport: http\n    ack-mode: on-complete\n    bind: https://svc.example/op\n",
    );
    assert!(has(&r, TARGET), "{:#?}", r.diagnostics);
}

#[test]
fn reply_stream_sse_is_accepted_on_an_on_complete_http_channel() {
    let r = report("    reply-stream: sse\n", SYNC_LLM);
    assert!(!has(&r, REPLY_STREAM), "{:#?}", r.diagnostics);
}

#[test]
fn reply_stream_sse_on_an_on_persist_http_channel_is_an_error() {
    let r = report(
        "    ack-mode: on-persist\n    reply-stream: sse\n",
        SYNC_LLM,
    );
    assert!(has(&r, REPLY_STREAM), "{:#?}", r.diagnostics);
}

#[test]
fn reply_stream_sse_on_a_non_http_channel_is_an_error() {
    let r = report("", &format!("{SYNC_LLM}    reply-stream: sse\n"));
    assert!(has(&r, REPLY_STREAM), "{:#?}", r.diagnostics);
}
