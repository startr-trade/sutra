//! B2 slice 1 — the deploy-time static "read-but-never-initialised variable" lint
//! (`check_never_initialized`, `SUTRA.CONFIG.VARIABLE.NEVER_INITIALIZED`). A `<q:variables>`
//! variable that is READ but has NO writer of any statically-visible kind (no `@source`, no
//! data-task output, no `<q:output variable>`) can never be initialised — every read yields null.
//! It is an advisory WARNING, and the whole check is suppressed for a process carrying an opaque
//! writer (`scriptTask`/`businessRuleTask`/non-template serviceTask), so it fires only on the
//! clearest provable case.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const CODE: &str = "SUTRA.CONFIG.VARIABLE.NEVER_INITIALIZED";

const PACKAGE_YAML: &str =
    "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const CHANNELS_YAML: &str =
    "channels:\n  - name: ch\n    transport: http\n    bind: \"POST /channels/ch\"\n    codec: \"urn:sutra:codec:json\"\n";

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Write a package whose single process `p1` has a `<q:source channel="ch">` start event (default
/// `payload` var), the given `<q:variables>` inner XML, the given task nodes + flows (which must
/// end at `<endEvent id="E">`), and the given `(templates/<name>, body)` / `(scripts/<name>, body)`
/// files.
fn build(
    root: &Path,
    variables: &str,
    nodes: &str,
    flows: &str,
    templates: &[(&str, &str)],
    scripts: &[(&str, &str)],
) {
    write(root, "package.yaml", PACKAGE_YAML);
    write(root, "channels.yaml", CHANNELS_YAML);
    for (name, body) in templates {
        write(root, &format!("templates/{name}"), body);
    }
    for (name, body) in scripts {
        write(root, &format!("scripts/{name}"), body);
    }
    write(
        root,
        "bpmn/flow.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:demo:1.0.0">
  <bpmn:process id="p1">
    <bpmn:extensionElements><q:variables>{variables}</q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="ch"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    {nodes}
    <bpmn:endEvent id="E"><bpmn:incoming>fend</bpmn:incoming></bpmn:endEvent>
    {flows}
  </bpmn:process>
</bpmn:definitions>
"#
        ),
    );
}

fn never_init_for(report: &LintReport, var: &str) -> bool {
    report
        .diagnostics
        .iter()
        .any(|d| d.code == CODE && d.message.contains(&format!("variable '{var}'")))
}

fn any_never_init(report: &LintReport) -> bool {
    report.diagnostics.iter().any(|d| d.code == CODE)
}

fn assert_no_errors(report: &LintReport) {
    let errors: Vec<String> = report.errors().map(|d| d.to_string()).collect();
    assert!(errors.is_empty(), "unexpected lint ERRORs: {errors:#?}");
}

/// `S → T(reads {{orphan}}) → E`, `orphan` declared with no `@source` and no writer anywhere → WARN.
#[test]
fn read_variable_with_no_writer_warns() {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        r#"<q:variable name="orphan"/>"#,
        r#"<bpmn:serviceTask id="T" implementation="t.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>fend</bpmn:outgoing></bpmn:serviceTask>"#,
        r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="fend" sourceRef="T" targetRef="E"/>"#,
        &[("t.hbs", "<v>{{orphan}}</v>")],
        &[],
    );
    let report = lint_dir(dir.path());
    assert_no_errors(&report);
    assert!(
        never_init_for(&report, "orphan"),
        "a read variable with no @source/output/assignment writer must WARN; diagnostics: {:#?}",
        report.diagnostics
    );
    // Severity is advisory — never a deploy-blocking error.
    assert!(
        report
            .diagnostics
            .iter()
            .filter(|d| d.code == CODE)
            .all(|d| d.severity == sutra_loader::LintSeverity::Warning),
        "NEVER_INITIALIZED must be a WARNING, never an ERROR"
    );
}

/// The same read, but the variable carries `@source="ch"` (an intake initialiser) → clean.
#[test]
fn variable_with_source_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        r#"<q:variable name="seed" source="ch"/>"#,
        r#"<bpmn:serviceTask id="T" implementation="t.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>fend</bpmn:outgoing></bpmn:serviceTask>"#,
        r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="fend" sourceRef="T" targetRef="E"/>"#,
        &[("t.hbs", "<v>{{seed}}</v>")],
        &[],
    );
    let report = lint_dir(dir.path());
    assert_no_errors(&report);
    assert!(
        !never_init_for(&report, "seed"),
        "a variable with @source has an initialiser and must not WARN; diagnostics: {:#?}",
        report.diagnostics
    );
}

/// `computed` is written by a task OUTPUT mapping (`<dataOutputAssociation><targetRef>`) on T1 and
/// read by T2's template → clean.
#[test]
fn variable_written_by_task_output_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        r#"<q:variable name="computed"/>"#,
        r#"<bpmn:serviceTask id="T1" implementation="t1.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing><bpmn:dataOutputAssociation><bpmn:targetRef>computed</bpmn:targetRef></bpmn:dataOutputAssociation></bpmn:serviceTask>
    <bpmn:serviceTask id="T2" implementation="t2.hbs"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>fend</bpmn:outgoing></bpmn:serviceTask>"#,
        r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T1"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T1" targetRef="T2"/>
    <bpmn:sequenceFlow id="fend" sourceRef="T2" targetRef="E"/>"#,
        &[("t1.hbs", "<a/>"), ("t2.hbs", "<v>{{computed}}</v>")],
        &[],
    );
    let report = lint_dir(dir.path());
    assert_no_errors(&report);
    assert!(
        !never_init_for(&report, "computed"),
        "a variable written by a data-task output must not WARN; diagnostics: {:#?}",
        report.diagnostics
    );
}

/// `rendered` is written by `<q:output variable="rendered">` on T1 and read by T2 → clean.
#[test]
fn variable_written_by_q_output_is_clean() {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        r#"<q:variable name="rendered"/>"#,
        r#"<bpmn:serviceTask id="T1" implementation="t1.hbs"><bpmn:extensionElements><q:output variable="rendered"/></bpmn:extensionElements><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:serviceTask id="T2" implementation="t2.hbs"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>fend</bpmn:outgoing></bpmn:serviceTask>"#,
        r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T1"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T1" targetRef="T2"/>
    <bpmn:sequenceFlow id="fend" sourceRef="T2" targetRef="E"/>"#,
        &[("t1.hbs", "<a/>"), ("t2.hbs", "<v>{{rendered}}</v>")],
        &[],
    );
    let report = lint_dir(dir.path());
    assert_no_errors(&report);
    assert!(
        !never_init_for(&report, "rendered"),
        "a variable written by <q:output> must not WARN; diagnostics: {:#?}",
        report.diagnostics
    );
}

/// Bail condition: a process carrying an opaque writer (`scriptTask`, which merges arbitrary
/// parsed output) can initialise any variable statically-invisibly, so the check is SUPPRESSED
/// for the whole process — even the otherwise-orphan `orphan` read produces no WARN.
#[test]
fn opaque_writer_suppresses_the_check() {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        r#"<q:variable name="orphan"/>"#,
        r#"<bpmn:serviceTask id="T" implementation="t.hbs"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:scriptTask id="SC"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>fend</bpmn:outgoing><bpmn:script>calc.hbs</bpmn:script></bpmn:scriptTask>"#,
        r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="SC"/>
    <bpmn:sequenceFlow id="fend" sourceRef="SC" targetRef="E"/>"#,
        &[("t.hbs", "<v>{{orphan}}</v>")],
        &[("calc.hbs", "{{now}}")],
    );
    let report = lint_dir(dir.path());
    assert_no_errors(&report);
    assert!(
        !any_never_init(&report),
        "a scriptTask (opaque writer) must suppress the never-init check for the process; \
         diagnostics: {:#?}",
        report.diagnostics
    );
}
