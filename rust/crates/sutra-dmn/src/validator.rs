//! Content validator that evaluates a single [`DmnDecision`] against an inbound payload,
//! including full OMG DMN 1.5 § 8.2.10 hit-policy coverage.
//!
//! # Hit-policy behaviour matrix
//!
//! | Policy | Result shape | Spec note |
//! |---|---|---|
//! | `UNIQUE` | Single output (or `DMN_UNIQUE_VIOLATION`) | Exactly one rule must fire. |
//! | `FIRST` | Single output | First firing rule in lexical order wins. |
//! | `ANY` | Single output (or `DMN_ANY_HIT_POLICY_AMBIGUOUS`) | All firing rules MUST agree. |
//! | `PRIORITY` | Single output | Highest-ranked `<outputValues>` entry wins; missing list → `UNIQUE` fallback + WARNING. |
//! | `OUTPUT_ORDER` | List of outputs | Sorted by `<outputValues>` position; missing list → `COLLECT` fallback + WARNING. |
//! | `RULE_ORDER` | List of outputs | Firing rules in lexical (source-order) sequence. |
//! | `COLLECT` | List of outputs | Same as `RULE_ORDER` for validator use. |
//!
//! # Evaluation model
//!
//! For each rule: each input clause's FEEL expression is resolved against the payload; the
//! rule's matching `<inputEntry>` is translated by [`crate::unary_test::to_full_expression`]
//! and evaluated against `{"input": value}` (plus the payload keys for pass-through
//! expressions). If ALL input entries hold, the rule fires; each output entry is evaluated
//! as full FEEL and surfaces as a [`ValidationIssue`] whose code is the output clause's
//! `bpm:code` (default `SUTRA.VALIDATE.DMN.RULESET_FAILED`).

use std::collections::BTreeMap;

use sutra_feel::expressions;
use sutra_feel::value::canonical_string_of;
use sutra_feel::{FeelContext, FeelValue, TimeQualifier};

use crate::clock::{Clock, SystemClock};
use crate::codes;
use crate::error::DmnError;
use crate::issue::{Severity, ValidationIssue};
use crate::model::{DmnDecision, DmnDecisionTable, DmnOutputClause, DmnRule, HitPolicy};
use crate::unary_test;

/// Reserved context variable carrying the engine's evaluation clock (an instant) — the
/// FEEL-side view of the injected evaluation-clock input. Injected only at the two DMN
/// evaluation entry points ([`DmnRulesetValidator::validate`] /
/// [`DmnRulesetValidator::evaluate`]); replay-bound FEEL sites never see a clock. The engine
/// value wins over a same-named payload key: `now` is reserved. Rules pair it with
/// `secondsBetween()`, e.g. `secondsBetween(createdAt, now) > 300`.
pub const NOW_VARIABLE: &str = "now";

/// A codec output handed to [`DmnRulesetValidator::validate`] — the inbound chain feeds the
/// codec's TYPED payload, which for envelope codecs is not a map. Models the untyped payload
/// signature + typed-envelope projection.
#[derive(Debug, Clone)]
pub enum DmnPayload {
    /// Map payload (map-producing codecs, tests) — used as the FEEL context directly.
    Map(FeelContext),
    /// Typed payload envelope — projected to `{body: bodyAsMap()}`, mirroring the engine's
    /// alias-resolution payload view (`InboundChain#payloadView`).
    Envelope { body: Option<FeelContext> },
    /// Any other scalar — `{value: …}`; `Null` projects to the empty context.
    Value(FeelValue),
}

pub struct DmnRulesetValidator {
    decision: DmnDecision,
    clock: Box<dyn Clock>,
}

impl DmnRulesetValidator {
    /// System-UTC clock (production path).
    pub fn new(decision: DmnDecision) -> Self {
        Self::with_clock(decision, Box::new(SystemClock))
    }

    /// Clock-injecting constructor — tests pass a fixed clock for deterministic temporal
    /// rules.
    pub fn with_clock(decision: DmnDecision, clock: Box<dyn Clock>) -> Self {
        DmnRulesetValidator { decision, clock }
    }

    /// The validator's name is the wrapped decision's *id* (the content-validator name
    /// contract) — intentionally not the decision's display name.
    #[allow(clippy::misnamed_getters)]
    pub fn name(&self) -> &str {
        &self.decision.id
    }

    /// The wrapped decision — exposed for diagnostic / introspection use.
    pub fn decision(&self) -> &DmnDecision {
        &self.decision
    }

    /// True when the given hit policy aggregates multiple outputs into a list rather than
    /// collapsing to a single output (0..N issues vs 0..1 issues per output clause).
    pub fn returns_list(policy: HitPolicy) -> bool {
        matches!(
            policy,
            HitPolicy::Collect | HitPolicy::OutputOrder | HitPolicy::RuleOrder
        )
    }

    /// Validate a codec output — projects it to the FEEL context (see [`feel_context`]),
    /// injects the reserved `now` variable, and applies the decision table.
    ///
    /// Errors only on an invalid `bpm:code` annotation (the coded error from `to_code`).
    pub fn validate(&self, payload: &DmnPayload) -> Result<Vec<ValidationIssue>, DmnError> {
        let safe_payload = self.with_now(feel_context(payload));
        self.validate_against(&safe_payload, &self.decision.table)
    }

    /// Map-payload convenience entry (the common test shape).
    pub fn validate_map(&self, payload: &FeelContext) -> Result<Vec<ValidationIssue>, DmnError> {
        self.validate(&DmnPayload::Map(payload.clone()))
    }

    fn validate_against(
        &self,
        safe_payload: &FeelContext,
        table: &DmnDecisionTable,
    ) -> Result<Vec<ValidationIssue>, DmnError> {
        let mut firings = Vec::new();
        for rule in &table.rules {
            if rule.input_entries.len() != table.inputs.len() {
                // Malformed rule: skip, don't abort the whole validation.
                continue;
            }
            if rule_fires(rule, table, safe_payload) {
                firings.push(RuleFiring {
                    rule: rule.clone(),
                    outputs: evaluate_outputs(rule, table, safe_payload),
                });
            }
        }
        self.apply_hit_policy(table.hit_policy, firings, table)
    }

    /// Evaluate this decision against `input`, returning the winning rule's outputs as a
    /// result map (output-clause name → value) — the BusinessRuleTask decision-evaluation
    /// path, distinct from the [`Self::validate`] verdict path. First-match semantics
    /// (covering UNIQUE / FIRST — the common single-hit table); an empty map when no rule
    /// fires. Reuses the very same rule-firing / output-evaluation FEEL core as validation.
    pub fn evaluate(&self, input: &FeelContext) -> BTreeMap<String, FeelValue> {
        let safe = self.with_now(input.clone());
        let table = &self.decision.table;
        for rule in &table.rules {
            if rule.input_entries.len() != table.inputs.len() {
                continue;
            }
            if rule_fires(rule, table, &safe) {
                let mut out = BTreeMap::new();
                for eo in evaluate_outputs(rule, table, &safe) {
                    if let Some(name) = &eo.clause.name {
                        out.insert(name.clone(), eo.value);
                    }
                }
                return out;
            }
        }
        BTreeMap::new()
    }

    /// Copies the evaluation context and injects the reserved [`NOW_VARIABLE`] from this
    /// validator's clock — the engine value wins over a same-named payload key.
    fn with_now(&self, mut context: FeelContext) -> FeelContext {
        context.insert(
            NOW_VARIABLE.to_string(),
            FeelValue::Instant(self.clock.now(), Some(TimeQualifier::Zulu)),
        );
        context
    }

    fn apply_hit_policy(
        &self,
        policy: HitPolicy,
        firings: Vec<RuleFiring>,
        table: &DmnDecisionTable,
    ) -> Result<Vec<ValidationIssue>, DmnError> {
        if firings.is_empty() {
            return Ok(Vec::new());
        }
        match policy {
            HitPolicy::Unique => self.apply_unique(&firings),
            HitPolicy::First => self.issues_for(&firings[0]),
            HitPolicy::Any => self.apply_any(&firings),
            HitPolicy::RuleOrder | HitPolicy::Collect => self.flatten(&firings),
            HitPolicy::Priority => self.apply_priority(firings, table),
            HitPolicy::OutputOrder => self.apply_output_order(firings, table),
        }
    }

    /// ANY: multiple rules MAY fire but MUST produce the same output; disagreement surfaces
    /// `DMN_ANY_HIT_POLICY_AMBIGUOUS`.
    fn apply_any(&self, firings: &[RuleFiring]) -> Result<Vec<ValidationIssue>, DmnError> {
        if firings.len() == 1 {
            return self.issues_for(&firings[0]);
        }
        let first = &firings[0].outputs;
        for f in &firings[1..] {
            if !outputs_agree(first, &f.outputs) {
                return Ok(vec![ValidationIssue {
                    code: codes::DMN_ANY_HIT_POLICY_AMBIGUOUS.to_string(),
                    severity: Severity::Error,
                    path: String::new(),
                    message: format!(
                        "Decision '{}' has hitPolicy=ANY but rules disagree on output: [{}]",
                        self.decision.id,
                        join_rule_ids(firings)
                    ),
                    value: None,
                }]);
            }
        }
        self.issues_for(&firings[0])
    }

    /// PRIORITY: of the firing rules, the output ranked highest in `<outputValues>` wins.
    /// Missing priority list → UNIQUE fallback + `DMN_PRIORITY_MISSING_OUTPUT_VALUES`
    /// WARNING. When the table has multiple output clauses the FIRST priority-bearing output
    /// column drives the sort (Camunda / Trisotech behaviour).
    fn apply_priority(
        &self,
        firings: Vec<RuleFiring>,
        table: &DmnDecisionTable,
    ) -> Result<Vec<ValidationIssue>, DmnError> {
        let Some(priority_clause) = first_output_with_priority_list(table) else {
            let warning = self.missing_output_values_warning(
                codes::DMN_PRIORITY_MISSING_OUTPUT_VALUES,
                "PRIORITY",
                first_output_id(table),
                "UNIQUE",
            );
            let mut combined = vec![warning];
            combined.extend(self.apply_unique(&firings)?);
            return Ok(combined);
        };
        let mut winner = &firings[0];
        let mut winner_rank = rank_of(winner, priority_clause);
        for candidate in &firings[1..] {
            let candidate_rank = rank_of(candidate, priority_clause);
            // Lower index in outputValues = higher priority. Unranked outputs lose to
            // ranked ones — same posture as Trisotech "value not in list" handling.
            if candidate_rank < winner_rank {
                winner = candidate;
                winner_rank = candidate_rank;
            }
        }
        self.issues_for(winner)
    }

    /// OUTPUT_ORDER: all firing rules' outputs, sorted by their output value's position in
    /// `<outputValues>`. Missing priority list → COLLECT fallback +
    /// `DMN_OUTPUT_ORDER_MISSING_OUTPUT_VALUES` WARNING.
    fn apply_output_order(
        &self,
        mut firings: Vec<RuleFiring>,
        table: &DmnDecisionTable,
    ) -> Result<Vec<ValidationIssue>, DmnError> {
        let Some(priority_clause) = first_output_with_priority_list(table) else {
            let warning = self.missing_output_values_warning(
                codes::DMN_OUTPUT_ORDER_MISSING_OUTPUT_VALUES,
                "OUTPUT_ORDER",
                first_output_id(table),
                "COLLECT",
            );
            let mut combined = vec![warning];
            combined.extend(self.flatten(&firings)?);
            return Ok(combined);
        };
        // Stable sort preserves rule order among equal ranks (mirror of List.sort).
        firings.sort_by_key(|f| rank_of(f, priority_clause));
        self.flatten(&firings)
    }

    fn apply_unique(&self, firings: &[RuleFiring]) -> Result<Vec<ValidationIssue>, DmnError> {
        if firings.len() == 1 {
            return self.issues_for(&firings[0]);
        }
        Ok(vec![ValidationIssue {
            code: codes::DMN_UNIQUE_VIOLATION.to_string(),
            severity: Severity::Error,
            path: String::new(),
            message: format!(
                "Decision '{}' has hitPolicy=UNIQUE but {} rules fired: [{}]",
                self.decision.id,
                firings.len(),
                join_rule_ids(firings)
            ),
            value: None,
        }])
    }

    fn missing_output_values_warning(
        &self,
        code: &str,
        policy_name: &str,
        output_id: &str,
        fallback_policy: &str,
    ) -> ValidationIssue {
        ValidationIssue {
            code: code.to_string(),
            severity: Severity::Warning,
            path: String::new(),
            message: format!(
                "Decision '{}' output '{output_id}' has hitPolicy={policy_name} but no \
                 <outputValues> priority list; falling back to {fallback_policy}",
                self.decision.id
            ),
            value: None,
        }
    }

    fn flatten(&self, firings: &[RuleFiring]) -> Result<Vec<ValidationIssue>, DmnError> {
        let mut out = Vec::new();
        for f in firings {
            out.extend(self.issues_for(f)?);
        }
        Ok(out)
    }

    fn issues_for(&self, firing: &RuleFiring) -> Result<Vec<ValidationIssue>, DmnError> {
        if firing.outputs.is_empty() {
            // Rule fired with no output entries — still emit a default issue.
            return Ok(vec![ValidationIssue {
                code: codes::DMN_RULESET_FAILED.to_string(),
                severity: Severity::Error,
                path: String::new(),
                message: format!(
                    "Decision '{}' rule '{}' fired",
                    self.decision.id, firing.rule.id
                ),
                value: None,
            }]);
        }
        let mut out = Vec::new();
        for ev in &firing.outputs {
            let code = match &ev.clause.diagnostic_code {
                Some(dotted) => to_code(dotted)?,
                None => codes::DMN_RULESET_FAILED.to_string(),
            };
            out.push(ValidationIssue {
                code,
                severity: Severity::Error,
                path: String::new(),
                message: stringify(&ev.value),
                value: if ev.value.is_null() {
                    None
                } else {
                    Some(ev.value.clone())
                },
            });
        }
        Ok(out)
    }
}

/// Projects a codec output to the FEEL-walkable map context (see
/// [`DmnRulesetValidator::validate`]): envelopes namespace their decoded body under `body`
/// (the envelope-codec shape), maps pass through unchanged, null projects to an empty context,
/// anything else wraps as `{value: …}`.
pub fn feel_context(payload: &DmnPayload) -> FeelContext {
    match payload {
        DmnPayload::Envelope { body } => {
            let mut view = FeelContext::new();
            if let Some(b) = body {
                view.insert("body".to_string(), FeelValue::Map(b.clone()));
            }
            view
        }
        DmnPayload::Map(m) => m.clone(),
        DmnPayload::Value(FeelValue::Map(m)) => m.clone(),
        DmnPayload::Value(FeelValue::Null) => FeelContext::new(),
        DmnPayload::Value(other) => {
            let mut view = FeelContext::new();
            view.insert("value".to_string(), other.clone());
            view
        }
    }
}

pub(crate) fn rule_fires(rule: &DmnRule, table: &DmnDecisionTable, payload: &FeelContext) -> bool {
    for (i, input_clause) in table.inputs.iter().enumerate() {
        let entry = &rule.input_entries[i];
        if entry.trim().is_empty() || entry.trim() == "-" {
            // DMN wildcard / empty — always matches this input.
            continue;
        }
        // The input expression failed to resolve (e.g. missing variable): treat as no
        // match — the rule does not fire for this payload, but we don't abort.
        let Ok(value) = expressions::eval(&input_clause.expression, payload) else {
            return false;
        };
        let full_expr = unary_test::to_full_expression(entry);
        let mut input_ctx = FeelContext::new();
        input_ctx.insert("input".to_string(), value);
        // Also expose payload keys so pass-through full expressions can reference other vars
        // (the synthesised `input` binding wins on collision).
        for (k, v) in payload {
            input_ctx.entry(k.clone()).or_insert_with(|| v.clone());
        }
        match expressions::eval_boolean(&full_expr, &input_ctx) {
            Ok(true) => {}
            Ok(false) | Err(_) => return false,
        }
    }
    true
}

pub(crate) fn evaluate_outputs(
    rule: &DmnRule,
    table: &DmnDecisionTable,
    payload: &FeelContext,
) -> Vec<EvaluatedOutput> {
    let mut results = Vec::new();
    for (i, output_clause) in table.outputs.iter().enumerate() {
        let entry = rule.output_entries.get(i).map(String::as_str).unwrap_or("");
        if entry.trim().is_empty() {
            continue;
        }
        let value = expressions::eval(entry, payload)
            // Fall back to the raw text — better than dropping the diagnostic.
            .unwrap_or_else(|_| FeelValue::String(entry.to_string()));
        results.push(EvaluatedOutput {
            clause: output_clause.clone(),
            value,
        });
    }
    results
}

/// Resolve the firing rule's value for the priority-bearing output column into its rank.
fn rank_of(firing: &RuleFiring, priority_clause: &DmnOutputClause) -> usize {
    rank_of_output(&firing.outputs, priority_clause)
}

/// Resolve a firing's evaluated outputs against the priority-bearing output column into its
/// rank (lower index in `<outputValues>` = higher priority; an unranked/absent output loses to
/// every ranked one). Shared by [`DmnRulesetValidator`]'s own PRIORITY/OUTPUT_ORDER handling and
/// `drg.rs`'s decision-result PRIORITY hit-policy evaluation — one ranking rule, two consumers.
pub(crate) fn rank_of_output(
    outputs: &[EvaluatedOutput],
    priority_clause: &DmnOutputClause,
) -> usize {
    for ev in outputs {
        if ev.clause.id == priority_clause.id {
            let value = stringify(&ev.value);
            return priority_clause
                .output_values
                .iter()
                .position(|v| *v == value)
                .unwrap_or(usize::MAX);
        }
    }
    usize::MAX
}

/// First output clause that declares a non-empty `<outputValues>` priority list.
pub(crate) fn first_output_with_priority_list(
    table: &DmnDecisionTable,
) -> Option<&DmnOutputClause> {
    table.outputs.iter().find(|o| !o.output_values.is_empty())
}

fn first_output_id(table: &DmnDecisionTable) -> &str {
    table.outputs.first().map(|o| o.id.as_str()).unwrap_or("")
}

/// ANY-policy agreement check. Value+scale equality for `BigDecimal` is scale-sensitive
/// (`1.0` and `1.00` DISAGREE) — matched here by comparing the numeric representation
/// (unscaled value + scale), not the numeric value.
fn outputs_agree(a: &[EvaluatedOutput], b: &[EvaluatedOutput]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| strict_equals(&x.value, &y.value))
}

fn strict_equals(a: &FeelValue, b: &FeelValue) -> bool {
    match (a, b) {
        (FeelValue::Number(x), FeelValue::Number(y)) => {
            x.as_bigint_and_exponent() == y.as_bigint_and_exponent()
        }
        _ => a == b,
    }
}

fn join_rule_ids(firings: &[RuleFiring]) -> String {
    firings
        .iter()
        .map(|f| f.rule.id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Diagnostic-code validation: `SUTRA.` prefix, 3–5 dotted segments. A malformed `bpm:code`
/// annotation surfaces as a structural parse error.
fn to_code(dotted: &str) -> Result<String, DmnError> {
    let reason = if !dotted.starts_with("SUTRA.") {
        Some(format!("DiagnosticCode must start with 'SUTRA.': {dotted}"))
    } else {
        let segments = dotted.split('.').count();
        if !(3..=5).contains(&segments) {
            Some(format!(
                "DiagnosticCode must have 3-5 dotted segments (got {segments}): {dotted}"
            ))
        } else {
            None
        }
    };
    match reason {
        None => Ok(dotted.to_string()),
        Some(r) => Err(DmnError::parse(format!(
            "Invalid <bpm:code> '{dotted}': {r}"
        ))),
    }
}

fn stringify(v: &FeelValue) -> String {
    match v {
        FeelValue::Null => "(null)".to_string(),
        FeelValue::String(s) => s.clone(),
        other => canonical_string_of(other),
    }
}

struct RuleFiring {
    rule: DmnRule,
    outputs: Vec<EvaluatedOutput>,
}

pub(crate) struct EvaluatedOutput {
    pub(crate) clause: DmnOutputClause,
    pub(crate) value: FeelValue,
}
