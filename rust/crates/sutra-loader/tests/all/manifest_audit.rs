//! The manifest's `audit:` block says only WHERE audit goes (`sink`, `sinks`), never whether or
//! what: `capture` is ignored with a warning, `sinks` must name processes the package defines,
//! there is no `events` key, and a destination never turns audit on for a process that declares
//! no `<q:audit>`.

use sutra_bpmn::BpmnModelLoader;
use sutra_loader::package::PackageConfig;
use sutra_loader::{lint_dir, LintReport};

const PACKAGE_LABELS: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\n";

const BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
      <bpmn:process id="undeclared" isExecutable="true">
        <bpmn:startEvent id="S"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
      </bpmn:process>
      <bpmn:process id="own" isExecutable="true">
        <bpmn:extensionElements><q:audit capture="metadata"/></bpmn:extensionElements>
        <bpmn:startEvent id="S2"/>
        <bpmn:endEvent id="E2"/>
        <bpmn:sequenceFlow id="g1" sourceRef="S2" targetRef="E2"/>
      </bpmn:process>
      <bpmn:process id="other" isExecutable="true">
        <bpmn:extensionElements><q:audit/></bpmn:extensionElements>
        <bpmn:startEvent id="S3"/>
        <bpmn:endEvent id="E3"/>
        <bpmn:sequenceFlow id="h1" sourceRef="S3" targetRef="E3"/>
      </bpmn:process>
    </bpmn:definitions>"#;

fn lint_package(audit_block: &str) -> LintReport {
    let root = std::env::temp_dir().join(format!(
        "manifest-audit-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(root.join("bpmn")).unwrap();
    std::fs::write(
        root.join("package.yaml"),
        format!("{PACKAGE_LABELS}{audit_block}"),
    )
    .unwrap();
    std::fs::write(root.join("bpmn/flow.bpmn"), BPMN).unwrap();
    let report = lint_dir(&root);
    let _ = std::fs::remove_dir_all(&root);
    report
}

fn codes(report: &LintReport) -> Vec<String> {
    report.diagnostics.iter().map(|d| d.code.clone()).collect()
}

/// The manifest's `audit.capture` is ignored, and the lint says so as a warning, not an error.
#[test]
fn manifest_capture_ignored_warns() {
    let report = lint_package("audit:\n  sink: sql\n  capture: payload\n");
    assert!(!report.has_errors(), "{:?}", report.diagnostics);
    assert!(
        report
            .warnings()
            .any(|d| d.code == "SUTRA.MANIFEST.AUDIT.CAPTURE_IGNORED"),
        "{:?}",
        codes(&report)
    );
    let quiet = lint_package("audit:\n  sink: sql\n");
    assert!(
        !codes(&quiet).contains(&"SUTRA.MANIFEST.AUDIT.CAPTURE_IGNORED".to_string()),
        "{:?}",
        codes(&quiet)
    );
}

/// `audit.sinks` routes per process over `audit.sink`, and only audited processes get one.
#[test]
fn manifest_sinks_per_process() {
    let config = PackageConfig::parse(
        "audit:\n  sink: sql\n  sinks:\n    own: jsonl\n    undeclared: otel\n",
    )
    .expect("parses");
    let audit = config.audit.expect("audit block");
    let destinations = audit.destinations();
    assert_eq!(destinations.default.as_deref(), Some("sql"));
    assert_eq!(
        destinations.per_process.get("own").map(String::as_str),
        Some("jsonl")
    );

    let module = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect("loads")
        .with_audit_destinations(&destinations);
    let sink_of = |id: &str| {
        module
            .process(id)
            .unwrap()
            .audit
            .as_ref()
            .map(|a| a.sink.clone())
    };
    assert_eq!(sink_of("own"), Some(Some("jsonl".to_string())));
    assert_eq!(sink_of("other"), Some(Some("sql".to_string())));
    // A destination never turns audit on.
    assert_eq!(sink_of("undeclared"), None);
    assert!(!lint_package("audit:\n  sinks:\n    own: jsonl\n").has_errors());
}

/// `audit.sinks` naming a process no BPMN defines is a manifest error.
#[test]
fn manifest_sinks_unknown_process_rejected() {
    let report = lint_package("audit:\n  sinks:\n    nosuch: jsonl\n");
    assert!(report.has_errors());
    let first = report.errors().next().unwrap();
    assert_eq!(first.code, "SUTRA.MANIFEST.AUDIT.UNKNOWN_PROCESS");
    assert!(first.message.contains("nosuch"), "{}", first.message);
}

/// The event scope lives only on the BPMN `<q:audit events>`; the manifest has no such key.
#[test]
fn manifest_has_no_events_key() {
    let err = PackageConfig::parse("audit:\n  sink: sql\n  events: all\n")
        .expect_err("events is not a manifest key");
    assert!(err.to_string().contains("events"), "{err}");
}
