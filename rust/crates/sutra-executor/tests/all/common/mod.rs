//! Shared executor-test helpers (each test binary compiles its own copy).
#![allow(dead_code)]

use std::collections::BTreeMap;

use sutra_bpmn::{BpmnModelLoader, ProcessDefinition, ProcessModule};
use sutra_executor::{TaskError, Variables};
use sutra_feel::FeelValue;

pub fn load(bpmn: &str) -> ProcessModule {
    BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads")
}

pub fn proc(bpmn: &str, id: &str) -> ProcessDefinition {
    load(bpmn).process(id).expect("process present").clone()
}

pub fn vars(pairs: &[(&str, FeelValue)]) -> Variables {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

pub fn fmap(pairs: &[(&str, FeelValue)]) -> FeelValue {
    FeelValue::Map(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect::<BTreeMap<String, FeelValue>>(),
    )
}

/// Task output convenience — `Ok(Map.of(...))`.
pub fn ok_map(pairs: &[(&str, FeelValue)]) -> Result<FeelValue, TaskError> {
    Ok(fmap(pairs))
}

pub fn num(n: i64) -> FeelValue {
    FeelValue::from(n)
}

pub fn string(s: &str) -> FeelValue {
    FeelValue::from(s)
}

pub fn boolean(b: bool) -> FeelValue {
    FeelValue::Boolean(b)
}

/// A boolean-variable condition evaluator: `expr` names a variable; true when it is
/// `Boolean(true)`.
pub fn var_truthy_evaluator() -> impl Fn(&str, &Variables) -> Result<bool, sutra_executor::ExecError>
{
    |expr: &str, vars: &Variables| {
        Ok(matches!(
            vars.get(expr.trim()),
            Some(FeelValue::Boolean(true))
        ))
    }
}
