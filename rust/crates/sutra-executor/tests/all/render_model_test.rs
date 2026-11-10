//! The render model an engine that drops null entries receives: built without its nulls in one
//! pass, it must equal the model with nulls, run through the recursive null strip the strict
//! Handlebars engine applies (null object entries dropped at every depth, array elements kept),
//! key order included.

use std::cell::RefCell;
use std::rc::Rc;

use serde_json::Value as Json;

use crate::common::*;
use sutra_executor::executor::feel_value_evaluator;
use sutra_executor::variables::feel_to_json;
use sutra_executor::{
    ScriptRegistry, TaskRegistry, TemplateEngine, TemplateEngineRegistry, TemplateRegistry,
    TokenExecutor, Variables,
};
use sutra_feel::FeelValue;

/// The reference: the strict Handlebars engine's strip, applied to a converted model.
fn strip_nulls(v: &Json) -> Json {
    match v {
        Json::Object(map) => Json::Object(
            map.iter()
                .filter(|(_, val)| !val.is_null())
                .map(|(k, val)| (k.clone(), strip_nulls(val)))
                .collect(),
        ),
        Json::Array(items) => Json::Array(items.iter().map(strip_nulls).collect()),
        other => other.clone(),
    }
}

/// Records every model it renders; answers `{}` so a script task merges nothing.
struct CapturingEngine {
    drops_nulls: bool,
    seen: Rc<RefCell<Vec<Json>>>,
}

impl TemplateEngine for CapturingEngine {
    fn name(&self) -> &str {
        "capture"
    }
    fn extensions(&self) -> Vec<String> {
        vec![".capture".to_string()]
    }
    fn render(&self, _id: &str, _template: &[u8], model: &Json) -> Result<String, String> {
        self.seen.borrow_mut().push(model.clone());
        Ok("{}".to_string())
    }
    fn drops_null_model_entries(&self) -> bool {
        self.drops_nulls
    }
}

const PROCESS: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="p">
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="T" implementation="render.capture">
          <bpmn:extensionElements>
            <q:param name="present" expression="greeting"/>
            <q:param name="absentParam" expression="null"/>
            <q:param name="shadowedByNull" expression="null"/>
            <q:param name="nullVarFilled" expression="'filled'"/>
            <q:param name="nested" expression="{a: null, b: [null, {c: null, d: 1}]}"/>
          </bpmn:extensionElements>
        </bpmn:serviceTask>
        <bpmn:scriptTask id="X"><bpmn:script>calc.capture</bpmn:script></bpmn:scriptTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
        <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="X"/>
        <bpmn:sequenceFlow id="f3" sourceRef="X" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

/// Variables with nulls at the top level, nested in maps, in lists, in maps inside lists, and
/// under names the model also sets itself (`uuid`, `vars`).
fn inputs() -> Variables {
    vars(&[
        ("greeting", string("hi")),
        ("nullVarFilled", FeelValue::Null),
        ("top", FeelValue::Null),
        (
            "deep",
            fmap(&[
                ("x", FeelValue::Null),
                ("y", fmap(&[("z", FeelValue::Null), ("w", num(2))])),
                (
                    "list",
                    FeelValue::List(vec![
                        FeelValue::Null,
                        fmap(&[("k", FeelValue::Null), ("v", string("s"))]),
                        FeelValue::List(vec![fmap(&[("n", FeelValue::Null)])]),
                    ]),
                ),
            ]),
        ),
        ("shadowedByNull", num(7)),
        ("uuid", FeelValue::Null),
        ("vars", FeelValue::Null),
        (
            "event",
            fmap(&[("body", string("<a/>")), ("headers", FeelValue::Null)]),
        ),
        ("emptyMap", fmap(&[("only", FeelValue::Null)])),
    ])
}

async fn models(drops_nulls: bool) -> Vec<Json> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let engines = TemplateEngineRegistry::new().register(CapturingEngine {
        drops_nulls,
        seen: Rc::clone(&seen),
    });
    let mut templates = TemplateRegistry::new();
    templates.register("render.capture", b"t".to_vec());
    let mut scripts = ScriptRegistry::new();
    scripts.register("calc.capture", b"s".to_vec());
    TokenExecutor::builder(TaskRegistry::new())
        .with_value_evaluator(feel_value_evaluator())
        .with_templates(engines, templates)
        .with_scripts(scripts)
        .with_uuid_supplier(|| "fixed-uuid".to_string())
        .with_now_supplier(|| "fixed-now".to_string())
        .build()
        .execute_sync(&proc(PROCESS, "p"), inputs())
        .await
        .unwrap();
    let out = seen.borrow().clone();
    out
}

#[tokio::test]
async fn null_free_model_equals_the_converted_model_with_its_nulls_stripped() {
    let with_nulls = models(false).await;
    let without_nulls = models(true).await;
    assert_eq!(
        with_nulls.len(),
        2,
        "the service task and the script task each render once"
    );
    assert_eq!(without_nulls.len(), 2);
    for (full, built) in with_nulls.iter().zip(&without_nulls) {
        let reference = strip_nulls(full);
        assert_eq!(built, &reference);
        // Same key order too: the engine iterates the model in this order.
        let keys = |v: &Json| v.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
        assert_eq!(keys(built), keys(&reference));
        assert_eq!(
            serde_json::to_string(built).unwrap(),
            serde_json::to_string(&reference).unwrap()
        );
    }
    // The cases the test exists for are really in the models.
    let full = &with_nulls[0];
    assert!(full["top"].is_null() && full["absentParam"].is_null());
    assert_eq!(
        without_nulls[0]["nullVarFilled"],
        Json::String("filled".into())
    );
    assert_eq!(without_nulls[0]["uuid"], Json::String("fixed-uuid".into()));
    assert!(without_nulls[0].get("shadowedByNull").is_none());
    assert_eq!(without_nulls[0]["deep"]["list"][0], Json::Null);
}

#[test]
fn variables_to_json_without_nulls_equals_converting_then_stripping() {
    let v = inputs();
    assert_eq!(v.to_json_without_nulls(), strip_nulls(&v.to_json()));
    for (_, value) in v.iter() {
        assert_eq!(
            sutra_executor::variables::feel_to_json_without_nulls(value),
            strip_nulls(&feel_to_json(value))
        );
    }
}
