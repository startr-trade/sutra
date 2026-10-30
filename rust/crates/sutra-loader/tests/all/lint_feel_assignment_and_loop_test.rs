//! Two lint gaps closed in 0.6.10:
//! - a multi-instance activity's `<inputDataItem>` (and `loopCounter`) is an input to every
//!   template the activity renders, so reading it is not `INPUT_UNSATISFIED`;
//! - assignment-side FEEL (data-task `<assignment><from>`, store keys, `<q:param>`) is
//!   parse-checked at package time, as flow conditions always were.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport};

const UNSATISFIED: &str = "SUTRA.CONFIG.TEMPLATE.INPUT_UNSATISFIED";

const LABELS: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"loops\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

fn lint(bpmn: &str, template: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let write = |rel: &str, content: &str| {
        let path = dir.path().join(rel);
        std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write("package.yaml", LABELS);
    write("bpmn/loops.bpmn", bpmn);
    write("templates/line.hbs", template);
    lint_dir(dir.path())
}

fn process(body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:loops:1.0.0">
  <bpmn:process id="loops" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Set">
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>["a", "b"]</bpmn:from><bpmn:to>entries</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    {body}
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="Set"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Set" targetRef="Each"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Each" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
    )
}

fn codes_of<'a>(report: &'a LintReport, code: &'a str) -> Vec<String> {
    report
        .errors()
        .filter(|d| d.code == code)
        .map(|d| d.message.clone())
        .collect()
}

const MI_TASK: &str = r#"<bpmn:serviceTask id="Each" implementation="line.hbs">
      <bpmn:multiInstanceLoopCharacteristics isSequential="true">
        <bpmn:loopDataInputRef>entries</bpmn:loopDataInputRef>
        <bpmn:inputDataItem id="entry" name="entry"/>
      </bpmn:multiInstanceLoopCharacteristics>
    </bpmn:serviceTask>"#;

#[test]
fn a_multi_instance_item_is_a_template_input() {
    let report = lint(&process(MI_TASK), "{{entry}} #{{loopCounter}}");
    assert!(
        codes_of(&report, UNSATISFIED).is_empty(),
        "{:?}",
        codes_of(&report, UNSATISFIED)
    );
}

#[test]
fn the_default_item_name_is_an_input_when_no_input_data_item_is_declared() {
    let task = r#"<bpmn:serviceTask id="Each" implementation="line.hbs">
      <bpmn:multiInstanceLoopCharacteristics isSequential="true">
        <bpmn:loopDataInputRef>entries</bpmn:loopDataInputRef>
      </bpmn:multiInstanceLoopCharacteristics>
    </bpmn:serviceTask>"#;
    let report = lint(&process(task), "{{item}}");
    assert!(codes_of(&report, UNSATISFIED).is_empty(), "{report:?}");
}

#[test]
fn the_loop_item_is_not_an_input_outside_its_activity() {
    // The same template rendered by a plain task after the loop still cannot read `entry`
    // unless something else sets it: the loop item is scoped to the multi-instance activity.
    let body = r#"<bpmn:serviceTask id="Each" implementation="line.hbs"/>"#;
    let report = lint(&process(body), "{{entry}}");
    let unsatisfied = codes_of(&report, UNSATISFIED);
    assert!(
        unsatisfied.iter().any(|m| m.contains("'entry'")),
        "{unsatisfied:?}"
    );
}

#[test]
fn an_assignment_that_does_not_parse_is_a_lint_error() {
    let body = r#"<bpmn:serviceTask id="Each">
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>{ a: 1 </bpmn:from><bpmn:to>broken</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>"#;
    let report = lint(&process(body), "");
    let messages: Vec<String> = report.errors().map(|d| d.message.clone()).collect();
    assert!(
        messages
            .iter()
            .any(|m| m.contains("<assignment> to 'broken'") && m.contains("does not parse")),
        "{messages:?}"
    );
}

#[test]
fn spaced_builtins_in_assignments_and_conditions_lint_clean() {
    let body = r#"<bpmn:serviceTask id="Each">
      <bpmn:dataInputAssociation>
        <bpmn:assignment><bpmn:from>context put({}, "k", string length("abc"))</bpmn:from><bpmn:to>ctx</bpmn:to></bpmn:assignment>
        <bpmn:assignment><bpmn:from>entries != null and not(list contains(entries, "z"))</bpmn:from><bpmn:to>ok</bpmn:to></bpmn:assignment>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>"#;
    let report = lint(&process(body), "");
    let parse_errors: Vec<String> = report
        .errors()
        .filter(|d| d.message.contains("does not parse"))
        .map(|d| d.message.clone())
        .collect();
    assert!(parse_errors.is_empty(), "{parse_errors:?}");
}

// ---- key queries (`<q:store query=…>`) ------------------------------------------------------

fn query_package(store: &str) -> LintReport {
    let dir = tempfile::tempdir().unwrap();
    let write = |rel: &str, content: &str| {
        let path = dir.path().join(rel);
        std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write("package.yaml", LABELS);
    write(
        "datastores.yaml",
        "datastores:\n  - name: events\n    type: sql\n    sql:\n      url-ref: env:LEDGER_DB_URL\n",
    );
    write(
        "bpmn/report.bpmn",
        &format!(
            r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:loops:1.0.0">
  <bpmn:dataStore id="eventsStore" name="events"/>
  <bpmn:process id="report" isExecutable="true">
    <bpmn:dataStoreReference id="refDay" dataStoreRef="eventsStore">
      <bpmn:extensionElements>{store}</bpmn:extensionElements>
    </bpmn:dataStoreReference>
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Query">
      <bpmn:dataInputAssociation>
        <bpmn:sourceRef>refDay</bpmn:sourceRef><bpmn:targetRef>report</bpmn:targetRef>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f0" sourceRef="S" targetRef="Query"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Query" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
        ),
    );
    lint_dir(dir.path())
}

#[test]
fn a_well_formed_key_query_lints_clean() {
    let report = query_package(
        r#"<q:store key="'src|'" query="range" to="'src}'" after="null" limit="50">
             <q:aggregate fn="sum" field="amount" groupBy="dir"/>
           </q:store>"#,
    );
    let errors: Vec<String> = report.errors().map(|d| d.message.clone()).collect();
    assert!(errors.is_empty(), "{errors:?}");
}

#[test]
fn key_query_bounds_are_parse_checked() {
    let report = query_package(r#"<q:store key="'src|'" query="range" to="'src}" after="{ x"/>"#);
    let messages: Vec<String> = report.errors().map(|d| d.message.clone()).collect();
    assert!(
        messages
            .iter()
            .any(|m| m.contains("query @to") && m.contains("does not parse")),
        "{messages:?}"
    );
    assert!(
        messages
            .iter()
            .any(|m| m.contains("query @after") && m.contains("does not parse")),
        "{messages:?}"
    );
}

#[test]
fn a_malformed_key_query_fails_the_package() {
    let report = query_package(r#"<q:store key="'src|'" query="prefix" limit="20000"/>"#);
    // The loader refuses the model; lint reports it as the package's load failure.
    let messages: Vec<String> = report.errors().map(|d| d.message.clone()).collect();
    assert!(
        messages.iter().any(
            |m| m.contains("SUTRA.PARSE.STORE.QUERY_INVALID") || m.contains("@limit=\"20000\"")
        ),
        "{messages:?}"
    );
}
