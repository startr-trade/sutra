//! Cross-process coverage pages (`coverage/**.yaml`). A coverage file declares one or more
//! `correlations` (a business key + per-hop `links`) and, under each, `coverages` — the named
//! cross-process coverage routes (`path` + per-process `segments`). The file format is described
//! in the book's *Coverage: declared routes as the compliance signal* chapter.
//!
//! Rather than dump the raw YAML, this surfaces the artifact's identity: the declared correlation
//! ids and the coverage-route (path) ids — the mnemonics that become the fully-qualified store
//! keys `urn:sutra:coverage:<folder…>:<file>:<path>`. Any file whose shape is not the expected
//! `correlations:` list falls back to the generic YAML table so nothing is ever lost.

use std::fmt::Write as _;

use serde_yaml::Value;

use crate::util::cell;

use super::yaml_table::render_yaml_body;

/// Render one coverage file's body (everything below the page title — no header/footer).
pub fn render_coverage_body(value: &Value) -> String {
    let correlations = value
        .as_mapping()
        .and_then(|m| m.get(Value::String("correlations".to_string())))
        .and_then(Value::as_sequence);

    let Some(correlations) = correlations else {
        // Unexpected shape — never lose the content; fall back to the generic table renderer.
        return render_yaml_body(value);
    };

    let mut out = String::new();
    out.push_str(
        "Cross-process coverage artifact (C6) — declares correlations and the coverage routes \
         over them. Each route's mnemonic id is made globally unique by this file's URN \
         (`urn:sutra:coverage:<folder…>:<file>:<path>`).\n\n",
    );

    // ---- Correlations -----------------------------------------------------------------------
    out.push_str("## Correlations\n\n");
    if correlations.is_empty() {
        out.push_str("_None declared._\n\n");
    } else {
        out.push_str(
            "| Correlation | Default key | Links | Coverage routes |\n|---|---|---|---|\n",
        );
        for c in correlations {
            let id = str_field(c, "id");
            let key = str_field(c, "key");
            let links = seq_len(c, "links");
            let routes = coverage_path_ids(c);
            let routes_cell = if routes.is_empty() {
                "—".to_string()
            } else {
                routes
                    .iter()
                    .map(|r| format!("`{r}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let _ = writeln!(
                out,
                "| {} | {} | {} | {} |",
                code_or_dash(&id),
                code_or_dash(&key),
                links,
                cell(&routes_cell),
            );
        }
        out.push('\n');
    }

    // ---- Coverage routes --------------------------------------------------------------------
    out.push_str("## Coverage routes\n\n");
    let mut any_route = false;
    out.push_str("| Route | Correlation | Processes |\n|---|---|---|\n");
    for c in correlations {
        let corr_id = str_field(c, "id");
        let coverages = c
            .as_mapping()
            .and_then(|m| m.get(Value::String("coverages".to_string())))
            .and_then(Value::as_sequence);
        let Some(coverages) = coverages else { continue };
        for route in coverages {
            let path = str_field(route, "path");
            let procs = segment_process_ids(route);
            let procs_cell = if procs.is_empty() {
                "—".to_string()
            } else {
                procs
                    .iter()
                    .map(|p| format!("`{p}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let _ = writeln!(
                out,
                "| {} | {} | {} |",
                code_or_dash(&path),
                code_or_dash(&corr_id),
                cell(&procs_cell),
            );
            any_route = true;
        }
    }
    if !any_route {
        // Replace the empty table header with a placeholder for readability.
        out.truncate(out.rfind("## Coverage routes").unwrap());
        out.push_str("## Coverage routes\n\n_None declared._\n\n");
    } else {
        out.push('\n');
    }

    out
}

/// The `path` ids declared under a correlation's `coverages`.
fn coverage_path_ids(correlation: &Value) -> Vec<String> {
    correlation
        .as_mapping()
        .and_then(|m| m.get(Value::String("coverages".to_string())))
        .and_then(Value::as_sequence)
        .map(|seq| {
            seq.iter()
                .map(|route| str_field(route, "path"))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// The process ids (keys of the route's `segments` mapping), in document order.
fn segment_process_ids(route: &Value) -> Vec<String> {
    route
        .as_mapping()
        .and_then(|m| m.get(Value::String("segments".to_string())))
        .and_then(Value::as_mapping)
        .map(|segs| {
            segs.iter()
                .filter_map(|(k, _)| k.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn str_field(v: &Value, key: &str) -> String {
    v.as_mapping()
        .and_then(|m| m.get(Value::String(key.to_string())))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn seq_len(v: &Value, key: &str) -> usize {
    v.as_mapping()
        .and_then(|m| m.get(Value::String(key.to_string())))
        .and_then(Value::as_sequence)
        .map(Vec::len)
        .unwrap_or(0)
}

fn code_or_dash(s: &str) -> String {
    if s.is_empty() {
        "—".to_string()
    } else {
        format!("`{}`", cell(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
correlations:
  - id: transfer
    key: txnId
    links:
      - { from: p1:sendMessage, to: p2:startEvent }
      - { from: p2:sendReplyMessage, to: p1:imec1 }
    coverages:
      - path: reply1
        segments: { p1: [startSeq, endSeq], p2: [startSeq, endSeq] }
      - path: reply2
        segments: { p1: [startSeq, endSeq], p3: [startSeq, endSeq2] }
"#;

    #[test]
    fn renders_correlations_and_routes() {
        let v: Value = serde_yaml::from_str(SAMPLE).unwrap();
        let body = render_coverage_body(&v);
        assert!(body.contains("## Correlations"));
        assert!(body.contains("| `transfer` | `txnId` | 2 | `reply1`, `reply2` |"));
        assert!(body.contains("## Coverage routes"));
        assert!(body.contains("| `reply1` | `transfer` | `p1`, `p2` |"));
        assert!(body.contains("| `reply2` | `transfer` | `p1`, `p3` |"));
    }

    #[test]
    fn falls_back_to_generic_table_for_unexpected_shape() {
        let v: Value = serde_yaml::from_str("name: not-a-coverage-file\n").unwrap();
        let body = render_coverage_body(&v);
        // Generic renderer emits a Properties key/value table, not the coverage sections.
        assert!(body.contains("| `name` | not-a-coverage-file |"));
        assert!(!body.contains("## Correlations"));
    }
}
