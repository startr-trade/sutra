//! A deployment's templates are compiled when it is deployed, not when its first message renders
//! one. Compiling a large Handlebars template takes seconds (the cost grows with the square of its
//! size), so a first message that paid for it would answer seconds late.

use std::time::{Duration, Instant};

use crate::archive_activation_conformance::{
    boot_db_engine, fresh_db, package, post, temp_root, write,
};

const AUTH: &str = "    auth:\n      scheme: apikey\n      apikey:\n        value: flip-key\n        header: X-Api-Key\n";

/// About 210 KB: roughly 3 s to compile in an optimised build.
fn large_template() -> String {
    let mut s = String::from("<Rows>\n");
    for i in 0..2000 {
        s.push_str(&format!(
            "<Row n=\"{i}\"><K>{{{{payload.k}}}}</K>{{{{#if payload.flag}}}}<F>{{{{payload.k}}}}</F>{{{{/if}}}}</Row>\n"
        ));
    }
    s.push_str("</Rows>\n");
    s
}

fn large_template_package() -> std::path::PathBuf {
    let root = temp_root("precompile-pkg");
    write(
        &root,
        "bpmn/render.bpmn",
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" id="Definitions_render"
                  targetNamespace="urn:sutra:module:precompile:1.0.0">
  <bpmn:process id="render" name="Render" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="render-in"/></bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Reply"/>
    <bpmn:serviceTask id="Reply" name="Render" implementation="large.hbs">
      <bpmn:extensionElements><q:reply mode="native" contentType="text/plain"/></bpmn:extensionElements>
      <bpmn:incoming>F1</bpmn:incoming><bpmn:outgoing>F2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="F2" sourceRef="Reply" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#,
    );
    write(&root, "templates/large.hbs", &large_template());
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: render-in\n    transport: http\n    bind: \"POST /precompile/render\"\n    codec: json\n{AUTH}"
        ),
    );
    write(
        &root,
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"precompile\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    root
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_deploy_with_a_large_template_answers_its_first_message_fast() {
    let (_pool, url) = fresh_db().await;
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let archive = package(&large_template_package());
    let deployed = Instant::now();
    let (status, body) = post(addr, "/admin/deployments", &archive).await;
    assert_eq!(status, 200, "deploy: {body}");
    let deploy_took = deployed.elapsed();

    let started = Instant::now();
    let (status, body) = post(addr, "/precompile/render", br#"{"k":"v","flag":true}"#).await;
    let first = started.elapsed();
    assert_eq!(status, 200, "{}", &body[..body.len().min(300)]);
    assert!(body.contains("<K>v</K>"), "rendered");
    println!("deploy {deploy_took:?}, first message {first:?}");
    assert!(
        first < Duration::from_millis(1500),
        "the first message rendered from the precompiled template: {first:?} (deploy {deploy_took:?})"
    );
    engine.shutdown().await;
}
