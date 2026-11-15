//! The dispatcher's fast path for a message nothing waits for: the owner-scope lookup and one
//! wait-index probe per live version. Budget: under 200 ns with 100 deployments served.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{ChannelBinding, Namespace, OwnerScopes, ProcessModuleRegistry};
use sutra_executor::DeploymentId;

const START_ONLY: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="d" targetNamespace="urn:t">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f" sourceRef="Start" targetRef="Wait"/>
    <bpmn:receiveTask id="Wait">
      <bpmn:extensionElements><q:source channel="reply-in"/></bpmn:extensionElements>
      <bpmn:incoming>f</bpmn:incoming><bpmn:outgoing>g</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="g" sourceRef="Wait" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>g</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>"#;

fn served(n: usize) -> (ProcessModuleRegistry, Arc<OwnerScopes>, DeploymentId) {
    let module = BpmnModelLoader::new().load(START_ONLY.as_bytes()).unwrap();
    let mut processes = ProcessModuleRegistry::new();
    let mut versions = Vec::new();
    let mut last = None;
    for i in 0..n {
        let d = DeploymentId::of(&format!("dep-{i:024x}")).unwrap();
        processes.register(&d, &module);
        let ns = Namespace::new("t", &format!("m{i}"), "1.0.0");
        versions.push((
            format!("owner-{i}"),
            d.clone(),
            vec![ChannelBinding::new("in", ns, d.clone(), "json")],
        ));
        last = Some(d);
    }
    (
        processes,
        Arc::new(OwnerScopes::build(versions)),
        last.unwrap(),
    )
}

fn fast_path(c: &mut Criterion) {
    for n in [1usize, 10, 100] {
        let (processes, scopes, d) = served(n);
        c.bench_function(
            &format!("live_for + waits_on_channel, {n} deployments"),
            |b| {
                b.iter(|| {
                    let live = scopes.live_for(black_box(&d));
                    live.iter()
                        .any(|s| processes.waits_on_channel(&s.deployment, black_box("in")))
                })
            },
        );
    }
}

criterion_group!(benches, fast_path);
criterion_main!(benches);
