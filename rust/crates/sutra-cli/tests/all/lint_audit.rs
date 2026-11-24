//! `sutra lint --audit` prints each process's audit plan, and `sutra lint --fix audit` brings a
//! 0.8 package to the 0.9 audit model without changing the trail it records.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};

use sutra_bpmn::qbindings::{AuditCapture, AuditEventScope};
use sutra_bpmn::{BpmnModelLoader, ProcessAudit, ProcessDefinition};
use sutra_cli::commands::lint::{self, LintArgs};
use sutra_cli::exit;
use sutra_cli::output::Io;
use sutra_cli::GlobalArgs;
use sutra_executor::listener::{ExecutionListener, InstanceEvent, TokenEvent};
use sutra_executor::{TaskRegistry, TokenExecutor, Variables};
use sutra_feel::FeelValue;

static SEQ: AtomicU32 = AtomicU32::new(0);

const PACKAGE_YAML: &str = "# a package\nlabels:\n  module: \"audit\"\n  version: \"1.0.0\"\n";

fn scratch(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sutra-cli-lint-audit-{label}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("bpmn")).expect("scratch dir");
    dir
}

fn write(dir: &Path, relative: &str, content: &str) {
    std::fs::write(dir.join(relative), content).expect("write");
}

fn read(dir: &Path, relative: &str) -> String {
    std::fs::read_to_string(dir.join(relative)).expect("read")
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../sutra-bpmn/tests/fixtures/audit")
            .join(name),
    )
    .expect("fixture")
}

fn run(args: LintArgs, format: Option<&str>) -> (i32, String, String) {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut input = std::io::Cursor::new(Vec::new());
    let global = GlobalArgs {
        format: format.map(str::to_string),
        ..GlobalArgs::default()
    };
    let code = {
        let mut io = Io {
            out: &mut out,
            err: &mut err,
            input: &mut input,
        };
        lint::execute(args, &global, &mut io)
    };
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

fn audit_args(dir: &Path) -> LintArgs {
    LintArgs {
        package_dir: dir.to_path_buf(),
        audit: true,
        ..LintArgs::default()
    }
}

fn fix_args(dir: &Path, dry_run: bool) -> LintArgs {
    LintArgs {
        package_dir: dir.to_path_buf(),
        fix: Some("audit".to_string()),
        dry_run,
        ..LintArgs::default()
    }
}

#[test]
fn lint_audit_prints_plan() {
    let dir = scratch("plan");
    write(&dir, "package.yaml", PACKAGE_YAML);
    write(&dir, "bpmn/flow.bpmn", &fixture("bare.bpmn"));
    let (code, out, err) = run(audit_args(&dir), None);
    assert_eq!(code, exit::OK, "{err}");
    assert!(out.contains("process flow: points · metadata"), "{out}");
    assert!(out.contains("start event (left)"), "{out}");
    assert!(out.contains("exclusive gateway (not a point)"), "{out}");

    write(&dir, "bpmn/flow.bpmn", &fixture("no-audit.bpmn"));
    let (_, out, _) = run(audit_args(&dir), None);
    assert!(out.contains("not audited"), "{out}");
    assert!(out.contains("SUTRA.LINT.AUDIT.NOT_DECLARED"), "{out}");
    let _ = std::fs::remove_dir_all(dir);
}

/// The JSON form of each process is exactly the plan the executor runs (the audit fixtures).
#[test]
fn lint_audit_json_matches_fixture() {
    for name in [
        "no-audit",
        "bare",
        "all",
        "points",
        "instance",
        "opt-out",
        "boundary-fired",
    ] {
        let dir = scratch("json");
        write(&dir, "package.yaml", PACKAGE_YAML);
        write(&dir, "bpmn/flow.bpmn", &fixture(&format!("{name}.bpmn")));
        let (code, out, err) = run(audit_args(&dir), Some("json"));
        assert_eq!(code, exit::OK, "{name}: {err}");
        let report: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
        let expected: serde_json::Value =
            serde_json::from_str(&fixture(&format!("{name}.audit.json"))).expect("fixture json");
        assert_eq!(report["processes"][0], expected, "{name}");
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// A 0.8 package: a process with no `<q:audit>` (audited by the manifest default), one with its
/// own `<q:audit sink capture>`, and a version-only pin.
const OLD_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_old" targetNamespace="urn:sutra:module:audit:1.0.0">
  <bpmn:process id="inherits" isExecutable="true">
    <bpmn:startEvent id="S1"/>
    <bpmn:exclusiveGateway id="G1"/>
    <bpmn:serviceTask id="T1" implementation="${noop}">
      <bpmn:extensionElements><q:audit sink="sql" capture="none"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E1"/>
    <bpmn:sequenceFlow id="a1" sourceRef="S1" targetRef="G1"/>
    <bpmn:sequenceFlow id="a2" sourceRef="G1" targetRef="T1"/>
    <bpmn:sequenceFlow id="a3" sourceRef="T1" targetRef="E1"/>
  </bpmn:process>
  <bpmn:process id="own" isExecutable="true">
    <bpmn:extensionElements>
      <q:audit sink="jsonl" capture="metadata"/>
    </bpmn:extensionElements>
    <bpmn:startEvent id="S2"/>
    <bpmn:serviceTask id="T2" implementation="${noop}"/>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="b1" sourceRef="S2" targetRef="T2"/>
    <bpmn:sequenceFlow id="b2" sourceRef="T2" targetRef="E2"/>
  </bpmn:process>
  <bpmn:process id="pinned" isExecutable="true">
    <bpmn:extensionElements><q:audit version="2.0"/></bpmn:extensionElements>
    <bpmn:startEvent id="S3"/>
    <bpmn:endEvent id="E3"/>
    <bpmn:sequenceFlow id="c1" sourceRef="S3" targetRef="E3"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const OLD_PACKAGE_YAML: &str = "# a package\nlabels:\n  module: \"audit\"\n  version: \"1.0.0\"\naudit:\n  sink: sql\n  capture: metadata\nproperties:\n  a: \"b\"\n";

fn old_package(label: &str) -> PathBuf {
    let dir = scratch(label);
    write(&dir, "package.yaml", OLD_PACKAGE_YAML);
    write(&dir, "bpmn/flow.bpmn", OLD_BPMN);
    dir
}

#[derive(Default)]
struct Trail(RefCell<Vec<(String, String)>>);

impl Trail {
    fn push(&self, event: &str, node: &str, sink: &Option<String>) {
        if sink.is_some() {
            self.0
                .borrow_mut()
                .push((event.to_string(), node.to_string()));
        }
    }
}

impl ExecutionListener for Trail {
    fn on_instance_started(&self, e: &InstanceEvent) {
        self.push("INSTANCE_STARTED", "", &e.audit_sink);
    }
    fn on_instance_completed(&self, e: &InstanceEvent) {
        self.push("INSTANCE_COMPLETED", "", &e.audit_sink);
    }
    fn on_token_entered(&self, e: &TokenEvent) {
        self.push("NODE_ENTERED", &e.node_id, &e.audit_sink);
    }
    fn on_token_left(&self, e: &TokenEvent) {
        self.push("NODE_LEFT", &e.node_id, &e.audit_sink);
    }
}

async fn trail_of(process: &ProcessDefinition) -> Vec<(String, String)> {
    let registry =
        TaskRegistry::new().register("noop", |_, _| Ok(FeelValue::Map(Default::default())));
    let trail = Rc::new(Trail::default());
    TokenExecutor::builder(registry)
        .with_listener(Rc::clone(&trail) as Rc<dyn ExecutionListener>)
        .build()
        .execute_sync(process, Variables::default())
        .await
        .expect("runs");
    let events = trail.0.borrow().clone();
    events
}

/// What 0.8 recorded: every event of every audited process. A process with its own
/// `<q:audit sink capture>` used it; any other was audited by the manifest default. Loaded here
/// with the removed attributes stripped (0.9 refuses them) and the 0.8 meaning applied by hand.
async fn trail_under_08(bpmn: &str, manifest_capture: AuditCapture) -> Vec<(String, String)> {
    let stripped = bpmn
        .replace(r#" sink="sql""#, "")
        .replace(r#" sink="jsonl""#, "");
    let module = BpmnModelLoader::new()
        .load(stripped.as_bytes())
        .expect("loads");
    let mut trail = Vec::new();
    for process in module.processes() {
        let own = !matches!(process.id.as_str(), "inherits" | "pinned");
        let capture = if own {
            process.audit.as_ref().expect("declared").capture
        } else {
            manifest_capture
        };
        let as_08 = process.clone().with_audit(Some(ProcessAudit {
            events: AuditEventScope::All,
            capture,
            sink: Some("sql".to_string()),
        }));
        trail.extend(trail_of(&as_08).await);
    }
    trail
}

async fn trail_under_09(dir: &Path) -> Vec<(String, String)> {
    let module = BpmnModelLoader::new()
        .load(read(dir, "bpmn/flow.bpmn").as_bytes())
        .expect("the fixed BPMN loads under 0.9");
    let mut trail = Vec::new();
    for process in module.processes() {
        let mut process = process.clone();
        process.set_audit_sink(Some("sql".to_string()));
        trail.extend(trail_of(&process).await);
    }
    trail
}

#[tokio::test]
async fn fix_audit_preserves_trail() {
    let dir = old_package("trail");
    let before = trail_under_08(OLD_BPMN, AuditCapture::Metadata).await;
    let (code, out, err) = run(fix_args(&dir, false), None);
    assert_eq!(code, exit::OK, "{err}");
    assert!(out.contains("+++ b/bpmn/flow.bpmn"), "{out}");
    let after = trail_under_09(&dir).await;
    assert!(!before.is_empty());
    assert_eq!(before, after);
    // Every process now says what it records.
    let fixed = read(&dir, "bpmn/flow.bpmn");
    assert!(!fixed.contains("sink="), "{fixed}");
    assert_eq!(fixed.matches(r#"events="all""#).count(), 3, "{fixed}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn fix_audit_idempotent() {
    let dir = old_package("idem");
    let (code, _, err) = run(fix_args(&dir, false), None);
    assert_eq!(code, exit::OK, "{err}");
    let bpmn = read(&dir, "bpmn/flow.bpmn");
    let yaml = read(&dir, "package.yaml");
    let (code, out, _) = run(fix_args(&dir, false), None);
    assert_eq!(code, exit::OK);
    assert!(out.contains("changed 0 file(s)"), "{out}");
    assert_eq!(read(&dir, "bpmn/flow.bpmn"), bpmn);
    assert_eq!(read(&dir, "package.yaml"), yaml);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn fix_audit_moves_sink_to_manifest() {
    let dir = old_package("sinks");
    let (code, _, err) = run(fix_args(&dir, false), None);
    assert_eq!(code, exit::OK, "{err}");
    let yaml = read(&dir, "package.yaml");
    // `own` named jsonl over the manifest's sql: it keeps it, per process.
    assert!(
        yaml.contains("audit:\n  sink: \"sql\"\n  sinks:\n    \"own\": \"jsonl\"\n"),
        "{yaml}"
    );
    // The ignored capture is gone; every other line is as it was.
    assert!(!yaml.contains("capture"), "{yaml}");
    assert!(yaml.starts_with("# a package\nlabels:\n"), "{yaml}");
    assert!(yaml.ends_with("properties:\n  a: \"b\"\n"), "{yaml}");
    let config = sutra_loader::package::PackageConfig::parse(&yaml).expect("parses");
    let audit = config.audit.expect("audit block");
    assert_eq!(audit.sinks.get("own").map(String::as_str), Some("jsonl"));
    // The fixed package lints clean of errors.
    let (code, out, _) = run(
        LintArgs {
            package_dir: dir.clone(),
            ..LintArgs::default()
        },
        None,
    );
    assert_eq!(code, exit::OK, "{out}");

    // With no manifest sink and every process naming the same one, that one becomes audit.sink.
    let lone = scratch("lone");
    write(&lone, "package.yaml", PACKAGE_YAML);
    write(
        &lone,
        "bpmn/flow.bpmn",
        &OLD_BPMN.replace(r#"sink="jsonl""#, r#"sink="otel""#),
    );
    let (code, _, err) = run(fix_args(&lone, false), None);
    assert_eq!(code, exit::OK, "{err}");
    let yaml = read(&lone, "package.yaml");
    assert!(yaml.ends_with("audit:\n  sink: \"otel\"\n"), "{yaml}");
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(lone);
}

#[test]
fn fix_audit_dry_run_writes_nothing() {
    let dir = old_package("dry");
    let (code, out, err) = run(fix_args(&dir, true), None);
    assert_eq!(code, exit::OK, "{err}");
    assert!(out.contains("--- a/bpmn/flow.bpmn"), "{out}");
    assert!(out.contains("--- a/package.yaml"), "{out}");
    assert!(out.contains("would change 2 file(s)"), "{out}");
    assert_eq!(read(&dir, "bpmn/flow.bpmn"), OLD_BPMN);
    assert_eq!(read(&dir, "package.yaml"), OLD_PACKAGE_YAML);
    let _ = std::fs::remove_dir_all(dir);
}
