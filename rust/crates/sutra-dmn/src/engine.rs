//! `DecisionEngine` for `.dmn` decision files (the `<bpmn:businessRuleTask>` path). Parses the
//! DMN with the shared [`DmnFileLoader`],
//! evaluates each decision's table against the process variables via
//! [`DmnRulesetValidator::evaluate`] (the very same FEEL decision-table core as the
//! validator), and merges the winning outputs into one result map. A one-decision file
//! (the common case) yields that decision's outputs; a multi-decision file merges each
//! decision's outputs — a later decision wins on a name collision (deterministic in the
//! port: decisions merge in document order).

use std::collections::BTreeMap;

use sutra_feel::{FeelContext, FeelValue};

use crate::error::DmnError;
use crate::loader::DmnFileLoader;
use crate::validator::DmnRulesetValidator;

#[derive(Debug, Clone, Copy, Default)]
pub struct DmnDecisionEngine {
    loader: DmnFileLoader,
}

impl DmnDecisionEngine {
    pub fn new() -> Self {
        DmnDecisionEngine {
            loader: DmnFileLoader::new(),
        }
    }

    /// Engine handle (registry key).
    pub fn name(&self) -> &'static str {
        "dmn"
    }

    /// File extensions this engine claims.
    pub fn extensions(&self) -> Vec<&'static str> {
        vec![".dmn"]
    }

    /// Evaluate every decision in the file against `input`, merging the winning outputs
    /// (output-clause name → value). The `decision_id` parameter is the caller-side file
    /// handle (unused beyond parity with the SPI signature).
    pub fn evaluate(
        &self,
        _decision_id: &str,
        decision: &[u8],
        input: &FeelContext,
    ) -> Result<BTreeMap<String, FeelValue>, DmnError> {
        let defs = self.loader.load(decision)?;
        let mut result = BTreeMap::new();
        for d in defs.decisions() {
            result.extend(DmnRulesetValidator::new(d.clone()).evaluate(input));
        }
        Ok(result)
    }
}
