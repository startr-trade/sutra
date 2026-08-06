//! `.srl` rules-DSL pages — rule agenda, conditions and action verbs, built from the
//! engine's own [`sutra_srl::parse`] (mirrors [`crate::render::dmn`]'s use of
//! `sutra_dmn::DmnFileLoader`: docs must reflect exactly what the engine loads).

use std::fmt::Write as _;

use anyhow::{Context, Result};
use sutra_srl::ast::{Action, Rule};
use sutra_srl::parse;

use crate::manifest::RulesManifest;
use crate::util::cell;

/// Render one `.srl` file's page body. `manifest` is the parsed `rules-manifest.yaml` of the
/// same package, if any — used to show the declared message-type applicability (the same
/// contract `.dmn` pages show).
pub fn render_srl_page(
    text: &str,
    rel: &str,
    basename: &str,
    manifest: Option<&RulesManifest>,
) -> Result<String> {
    let ruleset = parse(text)
        .map_err(|e| anyhow::anyhow!("{}", e))
        .with_context(|| format!("parsing SRL {rel}"))?;

    // Spans in the AST are raw character offsets into this same source (comments are
    // blanked-in-place by the lexer, never shortened) — slice straight from `text`.
    let chars: Vec<char> = text.chars().collect();
    let slice = |span: (usize, usize)| -> String {
        let start = span.0.min(chars.len());
        let end = span.1.min(chars.len()).max(start);
        chars[start..end]
            .iter()
            .collect::<String>()
            .trim()
            .to_string()
    };

    let mut out = String::new();
    let _ = writeln!(out, "**Rules:** {}\n", ruleset.rules.len());

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

    out.push_str("### Agenda (fire order)\n\n");
    if ruleset.rules.is_empty() {
        out.push_str("_No rules._\n\n");
    } else {
        // The same stable sort the engine's sequential agenda uses (see `sutra_srl::engine`):
        // `(-salience, decl_index)` — higher salience first, ties keep declaration order.
        let mut agenda: Vec<&Rule> = ruleset.rules.iter().collect();
        agenda.sort_by(|a, b| {
            b.salience
                .cmp(&a.salience)
                .then(a.decl_index.cmp(&b.decl_index))
        });

        out.push_str("| Order | Rule | Salience | Activation group |\n|---|---|---|---|\n");
        for (i, r) in agenda.iter().enumerate() {
            let _ = writeln!(
                out,
                "| {} | `{}` | {} | {} |",
                i + 1,
                cell(&r.name),
                r.salience,
                activation_group_cell(r)
            );
        }
        out.push('\n');
    }

    for rule in &ruleset.rules {
        let _ = writeln!(out, "## Rule `{}`\n", rule.name);
        let _ = writeln!(out, "**Salience:** {}\n", rule.salience);
        let _ = writeln!(
            out,
            "**Activation group:** {}\n",
            activation_group_cell(rule)
        );

        out.push_str("**Condition (FEEL, `when`):**\n\n");
        let _ = writeln!(out, "```\n{}\n```\n", slice(rule.condition_span));

        out.push_str("### Actions\n\n");
        if rule.actions.is_empty() {
            out.push_str("_No actions._\n\n");
        } else {
            out.push_str("| # | Verb | Detail |\n|---|---|---|\n");
            for (i, action) in rule.actions.iter().enumerate() {
                let (verb, detail) = match action {
                    Action::Set {
                        target, expr_span, ..
                    } => (
                        "set",
                        format!("`{}` = `{}`", target, cell(&slice(*expr_span))),
                    ),
                    Action::Report { arg_spans, .. } => (
                        "report",
                        format!(
                            "code=`{}`, path=`{}`, message=`{}`",
                            cell(&slice(arg_spans[0])),
                            cell(&slice(arg_spans[1])),
                            cell(&slice(arg_spans[2]))
                        ),
                    ),
                };
                let _ = writeln!(out, "| {} | `{}` | {} |", i + 1, verb, detail);
            }
            out.push('\n');
        }
    }

    Ok(out)
}

/// `` `group` `` when the rule declares an `activation-group`, else the placeholder dash.
fn activation_group_cell(rule: &Rule) -> String {
    match rule.activation_group.as_deref() {
        Some(g) => format!("`{}`", cell(g)),
        None => "—".to_string(),
    }
}
