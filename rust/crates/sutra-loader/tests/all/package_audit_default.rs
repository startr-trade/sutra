//! The manifest's deployment-wide `audit:` default reaches every process that declares no
//! `<q:audit>` of its own, and `capture: none` there switches those processes' execution audit
//! off: no `INSTANCE_*` or `NODE_*` event is routed to a sink.

use std::cell::RefCell;
use std::rc::Rc;

use sutra_bpmn::qbindings::AuditCapture;
use sutra_bpmn::BpmnModelLoader;
use sutra_executor::listener::{ExecutionListener, InstanceEvent, TokenEvent};
use sutra_executor::{TaskRegistry, TokenExecutor, Variables};
use sutra_feel::FeelValue;
use sutra_loader::package::PackageConfig;

const BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="inherits">
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="${noop}"/>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
      </bpmn:process>
      <bpmn:process id="own">
        <bpmn:extensionElements><q:audit sink="jsonl" capture="metadata"/></bpmn:extensionElements>
        <bpmn:startEvent id="S2"/>
        <bpmn:endEvent id="E2"/>
        <bpmn:sequenceFlow id="g1" sourceRef="S2" targetRef="E2"/>
      </bpmn:process>
    </bpmn:definitions>"#;

#[derive(Default)]
struct Sinks(RefCell<Vec<(String, Option<String>)>>);

impl ExecutionListener for Sinks {
    fn on_instance_started(&self, e: &InstanceEvent) {
        self.0
            .borrow_mut()
            .push(("INSTANCE_STARTED".into(), e.audit_sink.clone()));
    }
    fn on_instance_completed(&self, e: &InstanceEvent) {
        self.0
            .borrow_mut()
            .push(("INSTANCE_COMPLETED".into(), e.audit_sink.clone()));
    }
    fn on_token_entered(&self, e: &TokenEvent) {
        self.0
            .borrow_mut()
            .push((format!("NODE_ENTERED {}", e.node_id), e.audit_sink.clone()));
    }
}

#[tokio::test]
async fn manifest_capture_none_suppresses() {
    let manifest = PackageConfig::parse("audit:\n  sink: sql\n  capture: none\n").expect("parses");
    assert_eq!(manifest.audit.as_ref().unwrap().capture, AuditCapture::None);
    let module = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect("BPMN loads")
        .with_audit_default(&manifest.audit);

    let run = |id: &'static str| {
        let process = module.process(id).expect("present").clone();
        async move {
            let registry =
                TaskRegistry::new().register("noop", |_, _| Ok(FeelValue::Map(Default::default())));
            let sinks = Rc::new(Sinks::default());
            let executor = TokenExecutor::builder(registry)
                .with_listener(Rc::clone(&sinks) as Rc<dyn ExecutionListener>)
                .with_default_audit_sink(Some("sql".to_string()))
                .build();
            executor
                .execute_sync(&process, Variables::default())
                .await
                .expect("runs");
            let events = sinks.0.borrow().clone();
            events
        }
    };

    let inherited = run("inherits").await;
    assert!(!inherited.is_empty(), "the pass still reports");
    assert!(
        inherited.iter().all(|(_, sink)| sink.is_none()),
        "manifest capture none routes nothing: {inherited:?}"
    );

    // A process with its own <q:audit> keeps it; the manifest is only a default.
    let own = run("own").await;
    assert!(
        own.iter().all(|(_, sink)| sink.as_deref() == Some("jsonl")),
        "{own:?}"
    );
}
