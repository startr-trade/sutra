//! Call-activity dispatch (`<q:dispatch>` + `<q:case>` routing, including version-scoped call
//! resolution) and scoped `<q:param>` service-task inputs.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::model::ParamBinding;
use sutra_bpmn::{Node, ProcessDefinition};
use sutra_executor::executor::feel_value_evaluator;
use sutra_executor::listener::{DispatchEvent, ExecutionListener};
use sutra_executor::{
    DeploymentId, ProcessRegistry, TaskRegistry, TemplateEngine, TemplateEngineRegistry,
    TemplateRegistry, TokenExecutor,
};

// ---- CallActivityDispatchTest ------------------------------------------------------

fn parent_bpmn(dispatch_body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="parent">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="Route" calledElement="ignored">
              <bpmn:extensionElements>
                {dispatch_body}
              </bpmn:extensionElements>
            </bpmn:callActivity>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Route"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Route" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn sub_bpmn(process_id: &str, task_name: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="{process_id}">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Stamp" implementation="${{{task_name}}}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Stamp"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Stamp" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn sub_bpmn_ns(namespace: &str, process_id: &str, task_name: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          targetNamespace="{namespace}">
          <bpmn:process id="{process_id}">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Stamp" implementation="${{{task_name}}}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Stamp"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Stamp" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn parent_static_call(called_element: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="parent">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="C" calledElement="{called_element}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
            <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn resolver_of(
    defs: Vec<ProcessDefinition>,
) -> impl Fn(&str) -> Result<Option<std::sync::Arc<ProcessDefinition>>, sutra_executor::ExecError> {
    let map: HashMap<String, std::sync::Arc<ProcessDefinition>> = defs
        .into_iter()
        .map(|d| (d.id.clone(), std::sync::Arc::new(d)))
        .collect();
    move |id: &str| Ok(map.get(id).cloned())
}

#[tokio::test]
async fn single_case_truthy_routes_to_named_sub_process() {
    let parent = proc(
        &parent_bpmn(
            r#"<q:dispatch>
                 <q:case when="kind == 'a'" calledElement="procA"/>
               </q:dispatch>"#,
        ),
        "parent",
    );
    let sub = proc(&sub_bpmn("procA", "a"), "procA");

    let registry = TaskRegistry::new().register("a", |_, _| ok_map(&[("ranA", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_condition_evaluator(|expr, vars| {
            Ok(expr == "kind == 'a'" && vars.get("kind") == Some(&string("a")))
        })
        .with_process_resolver(resolver_of(vec![sub]))
        .build();

    let result = executor
        .execute_sync(&parent, vars(&[("kind", string("a"))]))
        .await
        .unwrap();
    assert_eq!(result.output("ranA"), Some(&boolean(true)));
}

#[tokio::test]
async fn first_matching_case_wins_and_second_case_is_ignored_even_if_truthy() {
    let parent = proc(
        &parent_bpmn(
            r#"<q:dispatch>
                 <q:case when="kind == 'a'" calledElement="procA"/>
                 <q:case when="kind == 'b'" calledElement="procB"/>
               </q:dispatch>"#,
        ),
        "parent",
    );
    let sub_a = proc(&sub_bpmn("procA", "a"), "procA");
    let sub_b = proc(&sub_bpmn("procB", "b"), "procB");

    let registry = TaskRegistry::new()
        .register("a", |_, _| ok_map(&[("ranA", boolean(true))]))
        .register("b", |_, _| ok_map(&[("ranB", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_condition_evaluator(|expr, vars| {
            Ok(match expr {
                "kind == 'a'" => vars.get("kind") == Some(&string("a")),
                "kind == 'b'" => vars.get("kind") == Some(&string("b")),
                _ => false,
            })
        })
        .with_process_resolver(resolver_of(vec![sub_a, sub_b]))
        .build();

    let result_a = executor
        .execute_sync(&parent, vars(&[("kind", string("a"))]))
        .await
        .unwrap();
    assert_eq!(result_a.output("ranA"), Some(&boolean(true)));
    assert_eq!(result_a.output("ranB"), None);

    let result_b = executor
        .execute_sync(&parent, vars(&[("kind", string("b"))]))
        .await
        .unwrap();
    assert_eq!(result_b.output("ranB"), Some(&boolean(true)));
    assert_eq!(result_b.output("ranA"), None);
}

#[tokio::test]
async fn no_case_matches_default_used_when_set() {
    let parent = proc(
        &parent_bpmn(
            r#"<q:dispatch default="procFallback">
                 <q:case when="kind == 'a'" calledElement="procA"/>
               </q:dispatch>"#,
        ),
        "parent",
    );
    let fallback = proc(&sub_bpmn("procFallback", "fb"), "procFallback");
    let sub_a = proc(&sub_bpmn("procA", "a"), "procA");

    let registry = TaskRegistry::new()
        .register("fb", |_, _| ok_map(&[("ranFb", boolean(true))]))
        .register("a", |_, _| ok_map(&[("ranA", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_condition_evaluator(|_, _| Ok(false))
        .with_process_resolver(resolver_of(vec![fallback, sub_a]))
        .build();

    let result = executor
        .execute_sync(&parent, vars(&[("kind", string("x"))]))
        .await
        .unwrap();
    assert_eq!(result.output("ranFb"), Some(&boolean(true)));
    assert_eq!(result.output("ranA"), None);
}

#[tokio::test]
async fn no_match_and_no_default_error_raises_dispatch_no_match() {
    let parent = proc(
        &parent_bpmn(
            r#"<q:dispatch>
                 <q:case when="never" calledElement="procA"/>
               </q:dispatch>"#,
        ),
        "parent",
    );
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(|_, _| Ok(false))
        .build();
    let e = executor.execute_sync(&parent, vars(&[])).await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.DISPATCH.NO_MATCH");
}

struct SkipRecorder {
    skipped: RefCell<Vec<DispatchEvent>>,
}

impl ExecutionListener for SkipRecorder {
    fn on_dispatch_skipped(&self, event: &DispatchEvent) {
        self.skipped.borrow_mut().push(event.clone());
    }
}

#[tokio::test]
async fn no_match_skip_marker_completes_instance_with_dispatch_skipped_audit() {
    let parent = proc(
        &parent_bpmn(
            r#"<q:dispatch onNoMatch="skip">
                 <q:case when="never" calledElement="procA"/>
               </q:dispatch>"#,
        ),
        "parent",
    );
    let recorder = Rc::new(SkipRecorder {
        skipped: RefCell::new(Vec::new()),
    });
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(|_, _| Ok(false))
        .with_listener(Rc::clone(&recorder) as Rc<dyn ExecutionListener>)
        .build();

    let result = executor.execute_sync(&parent, vars(&[])).await.unwrap();
    assert!(result.visited_nodes.contains("E"));
    assert_eq!(recorder.skipped.borrow().len(), 1);
    assert_eq!(recorder.skipped.borrow()[0].node_id, "Route");
}

#[tokio::test]
async fn feel_expression_that_fails_is_wrapped_as_feel_eval_failed() {
    let parent = proc(
        &parent_bpmn(
            r#"<q:dispatch>
                 <q:case when="bad-expr" calledElement="procA"/>
               </q:dispatch>"#,
        ),
        "parent",
    );
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(|expr, _| {
            Err(sutra_executor::ExecError::diag(
                "SUTRA.RUNTIME.UNEXPECTED",
                format!("blew up on {expr}"),
            ))
        })
        .build();

    let e = executor.execute_sync(&parent, vars(&[])).await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.DISPATCH.FEEL_EVAL_FAILED");
    assert!(e.message().contains("bad-expr"), "{e}");
}

#[tokio::test]
async fn unknown_called_element_raises_sub_process_not_found() {
    let parent = proc(
        &parent_bpmn(
            r#"<q:dispatch>
                 <q:case when="true" calledElement="procDoesNotExist"/>
               </q:dispatch>"#,
        ),
        "parent",
    );
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_condition_evaluator(|_, _| Ok(true))
        .with_process_resolver(|_| Ok(None))
        .build();

    let e = executor.execute_sync(&parent, vars(&[])).await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.DISPATCH.SUB_PROCESS_NOT_FOUND");
    assert!(e.message().contains("procDoesNotExist"), "{e}");
}

#[tokio::test]
async fn call_activity_without_dispatch_binding_falls_back_to_static_called_element() {
    let parent = proc(&parent_static_call("procStatic"), "parent");
    let sub = proc(&sub_bpmn("procStatic", "s"), "procStatic");

    let registry =
        TaskRegistry::new().register("s", |_, _| ok_map(&[("ranStatic", boolean(true))]));
    let executor = TokenExecutor::builder(registry)
        .with_process_resolver(resolver_of(vec![sub]))
        .build();
    let result = executor.execute_sync(&parent, vars(&[])).await.unwrap();
    assert_eq!(result.output("ranStatic"), Some(&boolean(true)));
    assert_ne!(result.instance_id, "00000000-0000-0000-0000-000000000000");
}

// ---- VM-7b: strict version-scoped call routing ------------------------------------------

fn key_v1() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000081").expect("valid deployment id")
}

fn key_v2() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000082").expect("valid deployment id")
}

fn two_version_registry() -> ProcessRegistry {
    let mut registry = ProcessRegistry::new();
    registry.register(
        key_v1(),
        load(&sub_bpmn_ns(
            "urn:sutra:module:billing:1.0.0",
            "procX",
            "v1",
        )),
    );
    registry.register(
        key_v2(),
        load(&sub_bpmn_ns(
            "urn:sutra:module:billing:2.0.0",
            "procX",
            "v2",
        )),
    );
    registry
}

fn version_scoped_executor(registry: ProcessRegistry) -> TokenExecutor {
    let tasks = TaskRegistry::new()
        .register("v1", |_, _| ok_map(&[("ran", string("v1"))]))
        .register("v2", |_, _| ok_map(&[("ran", string("v2"))]));
    let registry = Rc::new(registry);
    let anywhere = Rc::clone(&registry);
    let in_module = Rc::clone(&registry);
    TokenExecutor::builder(tasks)
        .with_process_resolver(move |id| anywhere.find_process_anywhere(id))
        .with_module_resolver(move |deployment, id| in_module.find_in_module(deployment, id))
        .build()
}

#[tokio::test]
async fn bare_called_element_resolves_within_callers_own_module_version() {
    let executor = version_scoped_executor(two_version_registry());
    let parent = proc(&parent_static_call("procX"), "parent");

    let r_v1 = executor
        .execute_sync_with(&parent, vars(&[]), key_v1(), Default::default())
        .await
        .unwrap();
    assert_eq!(r_v1.output("ran"), Some(&string("v1")));

    let r_v2 = executor
        .execute_sync_with(&parent, vars(&[]), key_v2(), Default::default())
        .await
        .unwrap();
    assert_eq!(r_v2.output("ran"), Some(&string("v2")));
}

#[tokio::test]
async fn bare_called_element_without_caller_module_stays_ambiguous_across_live_versions() {
    let executor = version_scoped_executor(two_version_registry());
    let parent = proc(&parent_static_call("procX"), "parent");

    let e = executor.execute_sync(&parent, vars(&[])).await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.RESOLVE.BARE_ID.AMBIGUOUS");
}

#[tokio::test]
async fn bare_called_element_never_crosses_into_another_version_even_when_unique_there() {
    // Strict isolation (VM-7b): procX exists ONLY in v2; a v1 caller must NOT cross into v2.
    let mut registry = ProcessRegistry::new();
    registry.register(
        key_v1(),
        load(&sub_bpmn_ns(
            "urn:sutra:module:billing:1.0.0",
            "other-flow",
            "v1",
        )),
    );
    registry.register(
        key_v2(),
        load(&sub_bpmn_ns(
            "urn:sutra:module:billing:2.0.0",
            "procX",
            "v2",
        )),
    );
    let executor = version_scoped_executor(registry);
    let parent = proc(&parent_static_call("procX"), "parent");

    let e = executor
        .execute_sync_with(&parent, vars(&[]), key_v1(), Default::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), "SUTRA.DISPATCH.SUB_PROCESS_NOT_FOUND");
    assert!(e.message().contains(key_v1().value()), "{e}");
}

// ---- ServiceTaskParamTest -----------------------------------------------------------------

fn param_process(service_task_xml: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p">
            <bpmn:startEvent id="S"/>
            {service_task_xml}
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

const ECHO_TASK: &str = r#"<bpmn:serviceTask id="T" implementation="echo">
      <bpmn:extensionElements>
        <q:param name="salutation" expression="greeting"/>
        <q:param name="shout" expression="'HELLO'"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>"#;

#[test]
fn params_parse_onto_service_task_in_order() {
    let process = proc(&param_process(ECHO_TASK), "p");
    match process.node("T").unwrap() {
        Node::ServiceTask { params, .. } => assert_eq!(
            params,
            &vec![
                ParamBinding {
                    name: "salutation".to_string(),
                    expression: "greeting".to_string(),
                },
                ParamBinding {
                    name: "shout".to_string(),
                    expression: "'HELLO'".to_string(),
                },
            ]
        ),
        other => panic!("expected ServiceTask, got {other:?}"),
    }
}

#[tokio::test]
async fn task_reads_scoped_params_but_they_do_not_persist() {
    let tasks = TaskRegistry::new().register("echo", |_, ctx| {
        ok_map(&[
            (
                "sawSalutation",
                ctx.variable("salutation")
                    .cloned()
                    .unwrap_or(sutra_feel::FeelValue::Null),
            ),
            (
                "sawShout",
                ctx.variable("shout")
                    .cloned()
                    .unwrap_or(sutra_feel::FeelValue::Null),
            ),
        ])
    });
    let process = proc(&param_process(ECHO_TASK), "p");

    let result = TokenExecutor::builder(tasks)
        .with_value_evaluator(feel_value_evaluator())
        .build()
        .execute_sync(&process, vars(&[("greeting", string("Hi Alice"))]))
        .await
        .unwrap();

    assert_eq!(result.output("sawSalutation"), Some(&string("Hi Alice")));
    assert_eq!(result.output("sawShout"), Some(&string("HELLO")));
    // ...but the params themselves never became process variables.
    assert_eq!(result.output("salutation"), None);
    assert_eq!(result.output("shout"), None);
    assert_eq!(result.output("greeting"), Some(&string("Hi Alice")));
}

#[tokio::test]
async fn param_shadows_a_same_named_variable_for_the_call_only() {
    let tasks = TaskRegistry::new().register("echo", |_, ctx| {
        ok_map(&[(
            "saw",
            ctx.variable("x").cloned().unwrap_or(string("MISSING")),
        )])
    });
    let st = r#"<bpmn:serviceTask id="T" implementation="echo">
          <bpmn:extensionElements>
            <q:param name="x" expression="greeting"/>
          </bpmn:extensionElements>
        </bpmn:serviceTask>"#;
    let process = proc(&param_process(st), "p");

    let result = TokenExecutor::builder(tasks)
        .with_value_evaluator(feel_value_evaluator())
        .build()
        .execute_sync(
            &process,
            vars(&[("x", string("original")), ("greeting", string("shadowed"))]),
        )
        .await
        .unwrap();

    // During the call the param 'x' shadowed the same-named process variable...
    assert_eq!(result.output("saw"), Some(&string("shadowed")));
    // ...but the underlying variable x is unchanged afterwards.
    assert_eq!(result.output("x"), Some(&string("original")));
}

/// Trivial engine for `.testjson` files: substitutes `${key}` from the render model.
struct SubstitutionEngine;

impl TemplateEngine for SubstitutionEngine {
    fn name(&self) -> &str {
        "testjson"
    }
    fn extensions(&self) -> Vec<String> {
        vec![".testjson".to_string()]
    }
    fn render(
        &self,
        _id: &str,
        template: &[u8],
        model: &serde_json::Value,
    ) -> Result<String, String> {
        let mut s = String::from_utf8_lossy(template).into_owned();
        if let serde_json::Value::Object(map) = model {
            for (k, v) in map {
                let rendered = match v {
                    serde_json::Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                s = s.replace(&format!("${{{k}}}"), &rendered);
            }
        }
        Ok(s)
    }
}

#[tokio::test]
async fn template_task_renders_against_scoped_params() {
    let engines = TemplateEngineRegistry::new().register(SubstitutionEngine);
    let mut templates = TemplateRegistry::new();
    templates.register("greet.testjson", b"${salutation}".to_vec());
    let st = r#"<bpmn:serviceTask id="T" implementation="greet.testjson">
          <bpmn:extensionElements>
            <q:param name="salutation" expression="greeting"/>
          </bpmn:extensionElements>
        </bpmn:serviceTask>"#;
    let process = proc(&param_process(st), "p");

    let result = TokenExecutor::builder(TaskRegistry::new())
        .with_value_evaluator(feel_value_evaluator())
        .with_templates(engines, templates)
        .build()
        .execute_sync(&process, vars(&[("greeting", string("Hi Bob"))]))
        .await
        .unwrap();

    // The template read {salutation} straight from the scoped param.
    assert_eq!(result.output("responseBody"), Some(&string("Hi Bob")));
    assert_eq!(result.output("salutation"), None);
}
