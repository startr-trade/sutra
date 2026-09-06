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

// ---- multi-instance / standard-loop item variables (engine-supplied writers) ----------------

/// A collection-driven `<bpmn:multiInstanceLoopCharacteristics>` binds its
/// `<bpmn:inputDataItem name>` — and `loopCounter` — on every iteration, so a `<q:variables>`
/// declaration of either IS initialised: by the engine, with no data association to see.
/// Declaring them is also mandatory (an undeclared root fails the template-input check), so
/// warning here left an author with no clean way to write the flow at all.
#[test]
fn multi_instance_item_variable_is_initialised_by_the_loop() {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        r#"<q:variable name="rows"/><q:variable name="row"/><q:variable name="loopCounter" type="number"/>"#,
        r#"<bpmn:serviceTask id="Prep"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>payload</bpmn:from><bpmn:to>rows</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:subProcess id="Loop"><bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>fend</bpmn:outgoing>
      <bpmn:multiInstanceLoopCharacteristics isSequential="true">
        <bpmn:loopDataInputRef>rows</bpmn:loopDataInputRef>
        <bpmn:inputDataItem name="row"/>
      </bpmn:multiInstanceLoopCharacteristics>
      <bpmn:startEvent id="LS"><bpmn:outgoing>l1</bpmn:outgoing></bpmn:startEvent>
      <bpmn:sequenceFlow id="l1" sourceRef="LS" targetRef="LT"/>
      <bpmn:serviceTask id="LT" implementation="t.hbs"><bpmn:incoming>l1</bpmn:incoming><bpmn:outgoing>l2</bpmn:outgoing></bpmn:serviceTask>
      <bpmn:sequenceFlow id="l2" sourceRef="LT" targetRef="LE"/>
      <bpmn:endEvent id="LE"><bpmn:incoming>l2</bpmn:incoming></bpmn:endEvent>
    </bpmn:subProcess>"#,
        r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Prep"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Prep" targetRef="Loop"/>
    <bpmn:sequenceFlow id="fend" sourceRef="Loop" targetRef="E"/>"#,
        &[("t.hbs", "<v>{{row.id}}<c>{{loopCounter}}</c></v>")],
        &[],
    );
    let report = lint_dir(dir.path());
    assert_no_errors(&report);
    assert!(
        !any_never_init(&report),
        "the multi-instance item variable and loopCounter are engine-supplied writers; \
         diagnostics: {:#?}",
        report.diagnostics
    );
}

/// The counterpart that must still fire: a CARDINALITY-only multi-instance iterates no
/// collection, so `run_multi_instance` binds no item variable — a declaration of one is
/// genuinely never written and the warning is correct.
#[test]
fn cardinality_only_multi_instance_still_warns_for_its_item_variable() {
    let dir = tempfile::tempdir().unwrap();
    build(
        dir.path(),
        r#"<q:variable name="row"/>"#,
        r#"<bpmn:subProcess id="Loop"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>fend</bpmn:outgoing>
      <bpmn:multiInstanceLoopCharacteristics isSequential="true">
        <bpmn:loopCardinality>3</bpmn:loopCardinality>
        <bpmn:inputDataItem name="row"/>
      </bpmn:multiInstanceLoopCharacteristics>
      <bpmn:startEvent id="LS"><bpmn:outgoing>l1</bpmn:outgoing></bpmn:startEvent>
      <bpmn:sequenceFlow id="l1" sourceRef="LS" targetRef="LT"/>
      <bpmn:serviceTask id="LT" implementation="t.hbs"><bpmn:incoming>l1</bpmn:incoming><bpmn:outgoing>l2</bpmn:outgoing></bpmn:serviceTask>
      <bpmn:sequenceFlow id="l2" sourceRef="LT" targetRef="LE"/>
      <bpmn:endEvent id="LE"><bpmn:incoming>l2</bpmn:incoming></bpmn:endEvent>
    </bpmn:subProcess>"#,
        r#"<bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Loop"/>
    <bpmn:sequenceFlow id="fend" sourceRef="Loop" targetRef="E"/>"#,
        &[("t.hbs", "<v>{{row.id}}</v>")],
        &[],
    );
    let report = lint_dir(dir.path());
    assert_no_errors(&report);
    assert!(
        never_init_for(&report, "row"),
        "a cardinality-only loop binds no item variable, so the warning must still fire; \
         diagnostics: {:#?}",
        report.diagnostics
    );
}
