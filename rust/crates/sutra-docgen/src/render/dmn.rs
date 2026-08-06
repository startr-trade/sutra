//! DMN decision-table pages — inputs, outputs, rules and hit policy, built from the engine's
//! own [`sutra_dmn::DmnFileLoader`].

use std::fmt::Write as _;

use anyhow::{Context, Result};
use sutra_dmn::model::HitPolicy;
use sutra_dmn::DmnFileLoader;

use crate::manifest::RulesManifest;
use crate::util::cell;

/// Render one `.dmn` file's page body. `manifest` is the parsed `rules-manifest.yaml` of the
/// same package, if any — used to show the declared message-type applicability.
pub fn render_dmn_page(
    bytes: &[u8],
    rel: &str,
    basename: &str,
    manifest: Option<&RulesManifest>,
) -> Result<String> {
    let defs = DmnFileLoader::new()
        .load(bytes)
        .map_err(|e| anyhow::anyhow!("{}", e))
        .with_context(|| format!("parsing DMN {rel}"))?;

    let mut out = String::new();
    let _ = writeln!(out, "**Namespace:** `{}`\n", defs.namespace);

    if let Some(entry) = manifest.and_then(|m| m.entry_for(basename)) {
        if entry.message_types.is_empty() {
            out.push_str(
                "**Applicable message types:** _open-typed (no single messageType pinned)_\n\n",
            );
        } else {
            let _ = writeln!(
                out,
                "**Applicable message types:** {}\n",
                entry.message_types.join(", ")
            );
        }
    } else {
        out.push_str("**Applicable message types:** _no rules-manifest.yaml entry found_\n\n");
    }

    for decision in defs.decisions() {
        let _ = writeln!(out, "## Decision `{}`\n", decision.id);
        if !decision.name.is_empty() {
            let _ = writeln!(out, "**Name:** {}\n", decision.name);
        }
        let table = &decision.table;
        let _ = writeln!(
            out,
            "**Hit policy:** {}\n",
            hit_policy_str(table.hit_policy)
        );

        out.push_str("### Inputs\n\n");
        if table.inputs.is_empty() {
            out.push_str("_No inputs._\n\n");
        } else {
            out.push_str("| Id | Expression | Type |\n|---|---|---|\n");
            for i in &table.inputs {
                let _ = writeln!(
                    out,
                    "| `{}` | `{}` | {} |",
                    i.id,
                    cell(&i.expression),
                    i.type_ref.as_deref().unwrap_or("—")
                );
            }
            out.push('\n');
        }

        out.push_str("### Outputs\n\n");
        if table.outputs.is_empty() {
            out.push_str("_No outputs._\n\n");
        } else {
            out.push_str(
                "| Id | Name | Type | Diagnostic code | Priority values |\n|---|---|---|---|---|\n",
            );
            for o in &table.outputs {
                let _ = writeln!(
                    out,
                    "| `{}` | {} | {} | {} | {} |",
                    o.id,
                    o.name.as_deref().unwrap_or("—"),
                    o.type_ref.as_deref().unwrap_or("—"),
                    o.diagnostic_code.as_deref().unwrap_or("—"),
                    if o.output_values.is_empty() {
                        "—".to_string()
                    } else {
                        cell(&o.output_values.join(", "))
                    }
                );
            }
            out.push('\n');
        }

        out.push_str("### Rules\n\n");
        if table.rules.is_empty() {
            out.push_str("_No rules._\n\n");
        } else {
            out.push('|');
            out.push_str(" Id |");
            for i in &table.inputs {
                let _ = write!(out, " in: `{}` |", i.id);
            }
            for o in &table.outputs {
                let _ = write!(out, " out: `{}` |", o.name.as_deref().unwrap_or(&o.id));
            }
            out.push_str("\n|---|");
            for _ in &table.inputs {
                out.push_str("---|");
            }
            for _ in &table.outputs {
                out.push_str("---|");
            }
            out.push('\n');
            for r in &table.rules {
                out.push('|');
                let _ = write!(out, " `{}` |", r.id);
                for e in &r.input_entries {
                    let _ = write!(out, " {} |", cell(blank_dash(e)));
                }
                for e in &r.output_entries {
                    let _ = write!(out, " {} |", cell(blank_dash(e)));
                }
                out.push('\n');
            }
            out.push('\n');
        }
    }

    Ok(out)
}

fn blank_dash(s: &str) -> &str {
    if s.trim().is_empty() {
        "—"
    } else {
        s
    }
}

fn hit_policy_str(h: HitPolicy) -> &'static str {
    match h {
        HitPolicy::Unique => "UNIQUE",
        HitPolicy::First => "FIRST",
        HitPolicy::Collect => "COLLECT",
        HitPolicy::Any => "ANY",
        HitPolicy::Priority => "PRIORITY",
        HitPolicy::OutputOrder => "OUTPUT ORDER",
        HitPolicy::RuleOrder => "RULE ORDER",
    }
}
