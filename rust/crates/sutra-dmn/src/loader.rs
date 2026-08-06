//! Loader for OMG DMN 1.5 `.dmn` files.
//!
//! Two-phase load: the input is parsed to a hardened mini-DOM
//! (quick-xml — DTDs rejected, no external entities), the `bpm:code` extension attributes
//! (which the OMG XSD doesn't declare) are harvested by a targeted DOM walk keyed by output
//! id, and the tree is then mapped into the engine-internal [`crate::model`] types.
//!
//! Element matching is by local name (a documented fallback posture — the DOM walk falls back
//! to a local-name scan when the DMN namespace isn't bound as expected);
//! the root element must be `<definitions>`.

use std::path::Path;

use crate::codes;
use crate::error::DmnError;
use crate::model::{
    DmnDecision, DmnDecisionTable, DmnDefinitions, DmnInputClause, DmnOutputClause, DmnRule,
    HitPolicy,
};
use crate::xml::{self, XmlElement};

/// OMG DMN 1.5 model namespace.
pub const DMN_NS_15: &str = "https://www.omg.org/spec/DMN/20230324/MODEL/";

/// Custom BPM annotation namespace — carries the `bpm:code` attribute on `<output>`
/// elements that maps a firing rule to a specific diagnostic code.
pub const BPM_ANNOT_NS: &str = "urn:sutra:dmn:annotations";

#[derive(Debug, Clone, Copy, Default)]
pub struct DmnFileLoader;

impl DmnFileLoader {
    pub fn new() -> Self {
        DmnFileLoader
    }

    pub fn load(&self, dmn_bytes: &[u8]) -> Result<DmnDefinitions, DmnError> {
        if dmn_bytes.is_empty() {
            return Err(DmnError::parse("DMN body is empty"));
        }
        let root =
            xml::parse(dmn_bytes).map_err(|e| DmnError::parse(format!("DMN parse failed: {e}")))?;
        if root.local != "definitions" {
            return Err(DmnError::parse(format!(
                "Expected <definitions> as document element, got <{}>",
                root.local
            )));
        }

        // Phase 1: collect bpm:code annotations by output id (the XSD drops these).
        let bpm_codes = collect_bpm_codes(&root);

        // Phase 2: walk the tree, building the engine-internal model.
        build_model(&root, &bpm_codes)
    }

    pub fn load_path(&self, path: &Path) -> Result<DmnDefinitions, DmnError> {
        let bytes = std::fs::read(path).map_err(|e| {
            DmnError::parse(format!("Failed to read DMN file {}: {e}", path.display()))
        })?;
        self.load(&bytes)
    }
}

// ---- DOM → bpm:code annotation extraction ----------------------------------

/// Walks the `<definitions>` tree for `<output>` elements and harvests their `bpm:code="…"`
/// attribute or `<bpm:diagnosticCode>…</bpm:diagnosticCode>` child, keyed by the output's
/// `id` attribute.
fn collect_bpm_codes(root: &XmlElement) -> Vec<(String, String)> {
    let mut outputs = Vec::new();
    root.descendants_named("output", &mut outputs);
    let mut out = Vec::new();
    for el in outputs {
        let Some(output_id) = el.attr(None, "id").filter(|s| !s.trim().is_empty()) else {
            continue;
        };
        if let Some(code) = el
            .attr(Some(BPM_ANNOT_NS), "code")
            .filter(|s| !s.trim().is_empty())
        {
            out.push((output_id.to_string(), code.trim().to_string()));
            continue;
        }
        if let Some(dc) = el.child_ns(BPM_ANNOT_NS, "diagnosticCode") {
            let t = dc.trimmed_text();
            if !t.is_empty() {
                out.push((output_id.to_string(), t.to_string()));
            }
        }
    }
    out
}

fn lookup_code<'a>(codes: &'a [(String, String)], output_id: &str) -> Option<&'a str> {
    codes
        .iter()
        .find(|(id, _)| id == output_id)
        .map(|(_, c)| c.as_str())
}

// ---- tree → DmnModel mapping ------------------------------------------------

fn build_model(
    root: &XmlElement,
    bpm_codes: &[(String, String)],
) -> Result<DmnDefinitions, DmnError> {
    let namespace = root.attr(None, "namespace").unwrap_or("").to_string();
    let mut decisions = Vec::new();
    for d in root.children_named("decision") {
        decisions.push(build_decision(d, bpm_codes)?);
        // Non-decision DRG elements (inputData, businessKnowledgeModel, knowledgeSource)
        // are out of scope — the validator only routes decisions today.
    }
    Ok(DmnDefinitions::new(namespace, decisions))
}

fn build_decision(d: &XmlElement, bpm_codes: &[(String, String)]) -> Result<DmnDecision, DmnError> {
    let id = required(d.attr(None, "id"), "decision", "id")?;
    let name = d
        .attr(None, "name")
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(&id)
        .to_string();

    let Some(table) = d.child("decisionTable") else {
        return Err(DmnError::parse(format!(
            "<decision id=\"{id}\"> has no <decisionTable>"
        )));
    };
    let table = build_table(table, &id, bpm_codes)?;
    Ok(DmnDecision { id, name, table })
}

pub(crate) fn build_table(
    table: &XmlElement,
    decision_id: &str,
    bpm_codes: &[(String, String)],
) -> Result<DmnDecisionTable, DmnError> {
    let hit_policy = map_hit_policy(table.attr(None, "hitPolicy"))?;

    let mut inputs = Vec::new();
    for el in table.children_named("input") {
        inputs.push(build_input(el, decision_id)?);
    }
    let mut outputs = Vec::new();
    for el in table.children_named("output") {
        outputs.push(build_output(el, bpm_codes)?);
    }
    let mut rules = Vec::new();
    for el in table.children_named("rule") {
        rules.push(build_rule(el)?);
    }
    Ok(DmnDecisionTable {
        hit_policy,
        inputs,
        outputs,
        rules,
    })
}

fn build_input(input: &XmlElement, decision_id: &str) -> Result<DmnInputClause, DmnError> {
    let id = required(input.attr(None, "id"), "input", "id")?;
    let Some(expr) = input.child("inputExpression") else {
        return Err(DmnError::parse(format!(
            "<input id=\"{id}\"> in decision {decision_id} has no <inputExpression>"
        )));
    };
    let text = expr
        .child("text")
        .map(XmlElement::trimmed_text)
        .unwrap_or("");
    if text.is_empty() {
        return Err(DmnError::parse(format!(
            "<inputExpression> for input {id} has no <text>"
        )));
    }
    let type_ref = expr
        .attr(None, "typeRef")
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string);
    Ok(DmnInputClause {
        id,
        expression: text.to_string(),
        type_ref,
    })
}

fn build_output(
    output: &XmlElement,
    bpm_codes: &[(String, String)],
) -> Result<DmnOutputClause, DmnError> {
    let id = required(output.attr(None, "id"), "output", "id")?;
    let name = output
        .attr(None, "name")
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string);
    let type_ref = output
        .attr(None, "typeRef")
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string);
    let diagnostic_code = lookup_code(bpm_codes, &id).map(str::to_string);
    let output_values = output
        .child("outputValues")
        .and_then(|ov| ov.child("text"))
        .map(|t| parse_output_values(t.trimmed_text()))
        .unwrap_or_default();
    let default_output = output
        .child("defaultOutputEntry")
        .and_then(|d| d.child("text"))
        .map(|t| t.trimmed_text().to_string());
    Ok(DmnOutputClause {
        id,
        name,
        type_ref,
        diagnostic_code,
        output_values,
        default_output,
    })
}

/// Parse the `<outputValues><text>"a","b","c"</text></outputValues>` priority list into an
/// ordered list of literal values — quoted strings have their surrounding quotes stripped so
/// the priority key matches FEEL's already-unquoted evaluated value. Ranges/negations are
/// out of scope, exactly as in the canary.
fn parse_output_values(text: &str) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    split_top_level_commas(text)
        .into_iter()
        .filter_map(|raw| {
            let t = raw.trim();
            if t.is_empty() {
                return None;
            }
            let stripped = if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
                &t[1..t.len() - 1]
            } else {
                t
            };
            Some(stripped.to_string())
        })
        .collect()
}

/// Splits a comma-separated FEEL list, respecting double-quoted strings so embedded commas
/// inside string literals aren't treated as separators.
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_quotes = false;
    let mut current = String::new();
    for c in s.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                current.push(c);
            }
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    out.push(current);
    out
}

fn build_rule(rule: &XmlElement) -> Result<DmnRule, DmnError> {
    let id = required(rule.attr(None, "id"), "rule", "id")?;
    let input_entries = rule.children_named("inputEntry").map(entry_text).collect();
    let output_entries = rule.children_named("outputEntry").map(entry_text).collect();
    Ok(DmnRule {
        id,
        input_entries,
        output_entries,
    })
}

fn entry_text(entry: &XmlElement) -> String {
    entry
        .child("text")
        .map(XmlElement::trimmed_text)
        .unwrap_or("")
        .to_string()
}

// ---- helpers -----------------------------------------------------------

/// Hit-policy attribute values follow the OMG XSD enumeration (spaces, not underscores);
/// a missing attribute defaults to UNIQUE per the spec.
fn map_hit_policy(raw: Option<&str>) -> Result<HitPolicy, DmnError> {
    let Some(raw) = raw else {
        return Ok(HitPolicy::Unique);
    };
    match raw {
        "UNIQUE" => Ok(HitPolicy::Unique),
        "FIRST" => Ok(HitPolicy::First),
        "COLLECT" => Ok(HitPolicy::Collect),
        "ANY" => Ok(HitPolicy::Any),
        "PRIORITY" => Ok(HitPolicy::Priority),
        "OUTPUT ORDER" => Ok(HitPolicy::OutputOrder),
        "RULE ORDER" => Ok(HitPolicy::RuleOrder),
        other => Err(DmnError::parse(format!("Unknown hitPolicy '{other}'"))),
    }
}

fn required(value: Option<&str>, element_name: &str, attr_name: &str) -> Result<String, DmnError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.to_string()),
        _ => Err(DmnError::new(
            codes::DMN_FILE_PARSE_ERROR,
            format!("<{element_name}> missing required @{attr_name}"),
        )),
    }
}
