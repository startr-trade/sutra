//! Internal, engine-owned DMN model. Decoupled from the XML parse shape — the same separation
//! between the schema/spec types and the runtime model.

/// Hit policies per OMG DMN 1.5 § 8.2.10.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HitPolicy {
    Unique,
    First,
    Collect,
    Any,
    Priority,
    OutputOrder,
    RuleOrder,
}

/// Top-level `<definitions>` container — one or more decisions.
///
/// Divergence note: the reference implementation stores decisions in a copied map (unspecified
/// iteration order); this preserves document order, which makes the multi-decision
/// merge in [`crate::engine::DmnDecisionEngine::evaluate`] deterministic.
#[derive(Debug, Clone, PartialEq)]
pub struct DmnDefinitions {
    pub namespace: String,
    decisions: Vec<DmnDecision>,
}

impl DmnDefinitions {
    pub fn new(namespace: String, decisions: Vec<DmnDecision>) -> Self {
        DmnDefinitions {
            namespace,
            decisions,
        }
    }

    /// All decisions in document order.
    pub fn decisions(&self) -> &[DmnDecision] {
        &self.decisions
    }

    /// Lookup by decision id.
    pub fn decision(&self, id: &str) -> Option<&DmnDecision> {
        self.decisions.iter().find(|d| d.id == id)
    }

    /// All decision ids in document order.
    pub fn decision_ids(&self) -> Vec<&str> {
        self.decisions.iter().map(|d| d.id.as_str()).collect()
    }
}

/// A single `<decision>` containing one decision table.
#[derive(Debug, Clone, PartialEq)]
pub struct DmnDecision {
    pub id: String,
    pub name: String,
    pub table: DmnDecisionTable,
}

/// `<decisionTable>` — inputs, outputs, rules, and the hit policy that selects firings.
#[derive(Debug, Clone, PartialEq)]
pub struct DmnDecisionTable {
    pub hit_policy: HitPolicy,
    pub inputs: Vec<DmnInputClause>,
    pub outputs: Vec<DmnOutputClause>,
    pub rules: Vec<DmnRule>,
}

/// `<input>` clause. `expression` is the FEEL expression text from the inner
/// `<inputExpression><text>…</text></inputExpression>` — evaluated against the payload map
/// to produce a single value compared against each rule's `<inputEntry>` unary test.
#[derive(Debug, Clone, PartialEq)]
pub struct DmnInputClause {
    pub id: String,
    pub expression: String,
    pub type_ref: Option<String>,
}

/// `<output>` clause. `diagnostic_code` carries the dotted code from a `bpm:code="…"`
/// attribute or a `<bpm:diagnosticCode>` child — when present, firing rules emit issues with
/// this code instead of the default `SUTRA.VALIDATE.DMN.RULESET_FAILED`.
///
/// `output_values` is the ordered priority list parsed from the optional
/// `<outputValues><text>"a","b","c"</text></outputValues>` child. It is load-bearing for the
/// `PRIORITY` and `OUTPUT_ORDER` hit policies — earlier entries win. Empty when
/// `<outputValues>` is absent, in which case those policies degrade with a documented
/// WARNING.
#[derive(Debug, Clone, PartialEq)]
pub struct DmnOutputClause {
    pub id: String,
    pub name: Option<String>,
    pub type_ref: Option<String>,
    pub diagnostic_code: Option<String>,
    pub output_values: Vec<String>,
    /// `<defaultOutputEntry><text>…</text></defaultOutputEntry>` — the FEEL text to fall back to
    /// when NO rule fires (DMN § 8.2.4). Consulted by `drg.rs`'s decision-result evaluation
    /// (the validator SPI's own verdict path has no analogous "no rule fired ⇒ a value" concept
    /// to plug it into, so it's unused there).
    pub default_output: Option<String>,
}

/// `<rule>` — one row of the decision table. `input_entries.len()` must match the table's
/// `inputs.len()`; `output_entries.len()` must match `outputs.len()`.
#[derive(Debug, Clone, PartialEq)]
pub struct DmnRule {
    pub id: String,
    pub input_entries: Vec<String>,
    pub output_entries: Vec<String>,
}
