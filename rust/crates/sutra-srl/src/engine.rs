//! The `.srl` decision engine — a stateless, deterministic **sequential agenda**. This is
//! *not* a rete network: there is no working-memory, no re-activation, no truth maintenance. A
//! single forward pass fires each rule at most once, in agenda order, mirroring the shape of
//! `sutra_dmn::engine::DmnDecisionEngine`.
//!
//! Determinism guarantees:
//! - the agenda is a **stable sort** by `(-salience, decl_index)` — higher salience first, ties
//!   keep declaration order;
//! - the fire path never iterates a `HashMap` (outputs are a `BTreeMap`, fired groups a
//!   `BTreeSet`);
//! - the working context is a clone of the input, so conditions see input variables, and a
//!   `set` forward-updates the working context within the single pass.

use std::collections::{BTreeMap, BTreeSet};

use sutra_feel::evaluator::{eval, eval_boolean};
use sutra_feel::positions::FeelSourcePositions;
use sutra_feel::value::canonical_string_of;
use sutra_feel::{FeelContext, FeelError, FeelValue};

use crate::ast::{Action, Rule};
use crate::codes;
use crate::error::SrlError;
use crate::parser;

/// Stateless `.srl` rule engine. Holds no mutable state between calls; every [`evaluate`] parses
/// the ruleset fresh and runs an independent agenda.
///
/// [`evaluate`]: SrlRuleEngine::evaluate
#[derive(Debug, Clone, Copy, Default)]
pub struct SrlRuleEngine;

impl SrlRuleEngine {
    pub fn new() -> Self {
        SrlRuleEngine
    }

    /// Engine handle (registry key).
    pub fn name(&self) -> &'static str {
        "srl"
    }

    /// File extensions this engine claims.
    pub fn extensions(&self) -> Vec<&'static str> {
        vec![".srl"]
    }

    /// Parse `ruleset` bytes, run the sequential agenda against `input`, and return the merged
    /// outputs: each `set` target → its value, plus an `"issues"` list (present only when at least
    /// one `report` fired). `decision_id` is the caller-side file handle (unused beyond SPI
    /// parity, matching the DMN engine).
    ///
    /// Fail-closed: a parse error, a condition that errors, or an action expression that errors is
    /// a hard `SrlError` — never a silent skip.
    pub fn evaluate(
        &self,
        _decision_id: &str,
        ruleset: &[u8],
        input: &FeelContext,
    ) -> Result<BTreeMap<String, FeelValue>, SrlError> {
        let src = std::str::from_utf8(ruleset).map_err(|_| SrlError {
            code: codes::SRL_INVALID_UTF8.to_string(),
            message: "ruleset is not valid UTF-8".to_string(),
            line: 1,
            column: 1,
            construct: None,
        })?;
        let ruleset = parser::parse(src)?;

        // Positions over the same source so FEEL eval-error offsets compose onto `.srl` line/col.
        let positions = FeelSourcePositions::new(src, "srl:inline");

        // Agenda: stable sort by (-salience, decl_index). No HashMap in the fire path.
        let mut agenda: Vec<&Rule> = ruleset.rules.iter().collect();
        agenda.sort_by(|a, b| {
            b.salience
                .cmp(&a.salience)
                .then(a.decl_index.cmp(&b.decl_index))
        });

        let mut working: FeelContext = input.clone();
        let mut output: BTreeMap<String, FeelValue> = BTreeMap::new();
        let mut issues: Vec<FeelValue> = Vec::new();
        let mut fired_groups: BTreeSet<String> = BTreeSet::new();

        for rule in agenda {
            if let Some(group) = &rule.activation_group {
                if fired_groups.contains(group) {
                    continue;
                }
            }

            let fires = eval_boolean(&rule.condition, &working)
                .map_err(|e| wrap_eval_error(&e, rule.condition_span.0, &positions))?;
            if !fires {
                continue;
            }

            for action in &rule.actions {
                match action {
                    Action::Set {
                        target,
                        expr,
                        expr_span,
                    } => {
                        let value = eval(expr, &working)
                            .map_err(|e| wrap_eval_error(&e, expr_span.0, &positions))?;
                        working.insert(target.clone(), value.clone());
                        output.insert(target.clone(), value);
                    }
                    Action::Report {
                        code,
                        path,
                        message,
                        arg_spans,
                    } => {
                        let code_s = coerce_string(
                            eval(code, &working)
                                .map_err(|e| wrap_eval_error(&e, arg_spans[0].0, &positions))?,
                        );
                        let path_s = coerce_string(
                            eval(path, &working)
                                .map_err(|e| wrap_eval_error(&e, arg_spans[1].0, &positions))?,
                        );
                        let message_s = coerce_string(
                            eval(message, &working)
                                .map_err(|e| wrap_eval_error(&e, arg_spans[2].0, &positions))?,
                        );
                        issues.push(issue_map(code_s, path_s, message_s));
                    }
                }
            }

            if let Some(group) = &rule.activation_group {
                fired_groups.insert(group.clone());
            }
        }

        // Only emit `issues` when non-empty — never clobber a prior list with an empty one.
        if !issues.is_empty() {
            output.insert("issues".to_string(), FeelValue::List(issues));
        }
        Ok(output)
    }
}

/// Coerce a FEEL value to its report-string form: a `String` passes through unchanged, everything
/// else uses FEEL's canonical string rendering (`null` → `"null"`), so the output is
/// deterministic.
fn coerce_string(v: FeelValue) -> String {
    match v {
        FeelValue::String(s) => s,
        other => canonical_string_of(&other),
    }
}

/// Build one issue map with EXACTLY the engine's frozen issue shape
/// (`code` / `severity` / `path` / `message` / `value`). `severity` is always `"ERROR"`
/// and `value` is always `Null`.
fn issue_map(code: String, path: String, message: String) -> FeelValue {
    let mut m: BTreeMap<String, FeelValue> = BTreeMap::new();
    m.insert("code".to_string(), FeelValue::String(code));
    m.insert(
        "severity".to_string(),
        FeelValue::String("ERROR".to_string()),
    );
    m.insert("path".to_string(), FeelValue::String(path));
    m.insert("message".to_string(), FeelValue::String(message));
    m.insert("value".to_string(), FeelValue::Null);
    FeelValue::Map(m)
}

/// Wrap a FEEL evaluation error onto its `.srl` position: the FEEL error's character offset (into
/// its own sub-expression) is composed with the embedded expression's `.srl` `origin`.
fn wrap_eval_error(e: &FeelError, origin: usize, positions: &FeelSourcePositions) -> SrlError {
    let abs = origin + e.offset.unwrap_or(0);
    SrlError::at(
        codes::SRL_FEEL_EVAL_ERROR,
        format!("FEEL evaluation error: [{}] {}", e.code, e.message),
        abs,
        positions,
    )
}
