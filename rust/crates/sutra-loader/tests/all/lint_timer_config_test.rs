//! Expression-valued timers at package time: `package.yaml` `properties:` declare the
//! `config.<key>` names a timer expression may read; an undeclared key is a lint error, a secret
//! reference is refused, and the properties are sealed into the manifest (only when present).

use std::path::Path;

use sutra_loader::{assemble_dir, lint_dir, read_archive_file, LintReport, PackageOptions};

const UNDECLARED: &str = "SUTRA.CONFIG.BPMN.TIMER_CONFIG_UNDECLARED";

const LABELS: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"timers\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const FLOW: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:timers:1.0.0">
  <bpmn:process id="nightly" isExecutable="true">
    <bpmn:startEvent id="Tick">
      <bpmn:timerEventDefinition><bpmn:timeDuration>config.startAfter</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:intermediateCatchEvent id="Wait">
      <bpmn:timerEventDefinition><bpmn:timeDuration>config.window</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Tick" targetRef="Wait"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Wait" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn package(properties: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "package.yaml", &format!("{LABELS}{properties}"));
    write(dir.path(), "bpmn/nightly.bpmn", FLOW);
    dir
}

fn codes(report: &LintReport) -> Vec<String> {
    report.errors().map(|d| d.code.clone()).collect()
}

#[test]
fn declared_properties_lint_clean() {
    let dir = package("properties:\n  startAfter: \"PT1H\"\n  window: \"${WINDOW:PT10M}\"\n");
    let report = lint_dir(dir.path());
    assert!(!report.has_errors(), "{:?}", codes(&report));
}

#[test]
fn an_undeclared_config_key_is_a_lint_error_naming_it() {
    let dir = package("properties:\n  startAfter: \"PT1H\"\n");
    let report = lint_dir(dir.path());
    let errors: Vec<_> = report.errors().collect();
    assert_eq!(errors.len(), 1, "{:?}", codes(&report));
    assert_eq!(errors[0].code, UNDECLARED);
    assert!(
        errors[0].message.contains("config.window"),
        "{}",
        errors[0].message
    );

    // No properties at all: both keys are reported.
    let dir = package("");
    let report = lint_dir(dir.path());
    assert_eq!(codes(&report), vec![UNDECLARED, UNDECLARED]);
}

#[test]
fn a_secret_reference_in_properties_is_refused() {
    let dir = package("properties:\n  startAfter: \"secret:START\"\n  window: \"PT1M\"\n");
    let report = lint_dir(dir.path());
    assert_eq!(codes(&report), vec!["SUTRA.DEPLOY.PACKAGE.CONFIG_INVALID"]);
}

#[test]
fn properties_are_sealed_unresolved_and_read_back() {
    let dir = package("properties:\n  startAfter: \"PT1H\"\n  window: \"${WINDOW:PT10M}\"\n");
    let out = tempfile::tempdir().unwrap();
    let outcome = assemble_dir(dir.path(), out.path(), &PackageOptions::default())
        .expect("the package seals");
    let archive = &outcome.archives[0];
    let manifest = archive.manifest.to_yaml();
    assert!(
        manifest.contains(
            "properties:\n  \"startAfter\": \"PT1H\"\n  \"window\": \"${WINDOW:PT10M}\"\n"
        ),
        "{manifest}"
    );
    let loaded = read_archive_file(&archive.file_path).expect("the archive reads back");
    assert_eq!(loaded.deployment.properties["window"], "${WINDOW:PT10M}");
}

#[test]
fn a_package_without_properties_seals_no_properties_key() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "package.yaml", LABELS);
    write(
        dir.path(),
        "bpmn/nightly.bpmn",
        &FLOW
            .replace("config.startAfter", "PT1H")
            .replace("config.window", "PT5M"),
    );
    let out = tempfile::tempdir().unwrap();
    let outcome = assemble_dir(dir.path(), out.path(), &PackageOptions::default())
        .expect("the package seals");
    assert!(!outcome.archives[0]
        .manifest
        .to_yaml()
        .contains("properties"));
}
