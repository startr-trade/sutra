//! `sutra lint` checks a template compiles and then analyses it for every rule that reads its
//! inputs. Compiling is the expensive part (its cost grows with the square of the template's size),
//! so the checks share one compile, through the template engine's process-wide cache.

use std::path::Path;

use sutra_loader::lint_dir;

#[test]
fn lint_of_a_package_compiles_each_template_once() {
    // Unique text: the compile count is per text, so other tests in this process cannot add to it.
    let template = "<CompileOnceProbe>{{payload.alpha}}{{#if payload.beta}}{{payload.gamma}}{{/if}}</CompileOnceProbe>\n";
    let dir = tempfile::tempdir().unwrap();
    let write = |rel: &str, content: &str| {
        let path = dir.path().join(rel);
        std::fs::create_dir_all(Path::new(&path).parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write(
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"once\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    write(
        "bpmn/once.bpmn",
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0" targetNamespace="urn:sutra:module:once:1.0.0">
  <bpmn:process id="once" isExecutable="true">
    <bpmn:startEvent id="S"/>
    <bpmn:serviceTask id="Render" implementation="probe.hbs"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Render"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Render" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#,
    );
    write("templates/probe.hbs", template);
    let _ = lint_dir(dir.path());
    assert_eq!(sutra_templates::compile_count(template.as_bytes()), 1);
    // A second lint of the same package compiles nothing more.
    let _ = lint_dir(dir.path());
    assert_eq!(sutra_templates::compile_count(template.as_bytes()), 1);
}
