//! Generic YAML → Markdown table renderer, used for `channels.yaml`, `package.yaml`, and any
//! other `*.yaml`/`*.yml` file discovered under a package that isn't one of the four named
//! manifests. This is the generator's "generic catalog" half — arbitrary authoring YAML gets a
//! readable table without a hand-written schema for every file shape.
//!
//! Rendering rules, applied per top-level key of the document's root mapping (in the document's
//! own key order — stable for a given input, and more readable than an alphabetical resort):
//! - a scalar (or a list of scalars) becomes one row of a leading `Properties` table;
//! - a non-empty list of mappings (e.g. `channels: [...]`) becomes its own `### key` table, one
//!   row per item, columns = the union of keys seen across items (first-seen order);
//! - a non-empty mapping (e.g. `labels: {...}`) becomes its own `### key` key/value table.

use std::fmt::Write as _;

use serde_yaml::Value;

use crate::util::cell;

/// Render one YAML document's body (everything below the page title — no header/footer).
pub fn render_yaml_body(value: &Value) -> String {
    let mut out = String::new();
    let Some(map) = value.as_mapping() else {
        if value.is_null() {
            out.push_str("_Empty document._\n\n");
        } else {
            let _ = writeln!(out, "```yaml\n{}\n```\n", compact(value));
        }
        return out;
    };
    if map.is_empty() {
        out.push_str("_Empty document._\n\n");
        return out;
    }

    let mut props: Vec<(String, String)> = Vec::new();
    let mut sections: Vec<(String, String)> = Vec::new();

    for (k, v) in map {
        let key = scalar_str(k);
        match v {
            Value::Sequence(seq) if !seq.is_empty() && seq.iter().any(Value::is_mapping) => {
                sections.push((key, list_of_maps_table(seq)));
            }
            Value::Mapping(inner) if !inner.is_empty() => {
                sections.push((key, mapping_table(inner)));
            }
            _ => props.push((key, compact(v))),
        }
    }

    if !props.is_empty() {
        out.push_str("| Key | Value |\n|---|---|\n");
        for (k, v) in &props {
            let _ = writeln!(out, "| `{}` | {} |", k, cell(v));
        }
        out.push('\n');
    }

    for (key, body) in sections {
        let _ = writeln!(out, "### `{key}`\n");
        out.push_str(&body);
        out.push('\n');
    }

    out
}

fn list_of_maps_table(seq: &[Value]) -> String {
    let mut columns: Vec<String> = Vec::new();
    for item in seq {
        if let Some(m) = item.as_mapping() {
            for (k, _) in m {
                let ks = scalar_str(k);
                if !columns.contains(&ks) {
                    columns.push(ks);
                }
            }
        }
    }

    let mut out = String::new();
    out.push('|');
    for c in &columns {
        let _ = write!(out, " `{c}` |");
    }
    out.push_str("\n|");
    for _ in &columns {
        out.push_str("---|");
    }
    out.push('\n');
    for item in seq {
        out.push('|');
        for c in &columns {
            let cellval = item
                .as_mapping()
                .and_then(|m| m.get(Value::String(c.clone())))
                .map(compact)
                .unwrap_or_default();
            let _ = write!(out, " {} |", cell(&cellval));
        }
        out.push('\n');
    }
    out.push('\n');
    out
}

fn mapping_table(map: &serde_yaml::Mapping) -> String {
    let mut out = String::new();
    out.push_str("| Key | Value |\n|---|---|\n");
    for (k, v) in map {
        let _ = writeln!(out, "| `{}` | {} |", scalar_str(k), cell(&compact(v)));
    }
    out.push('\n');
    out
}

/// A bare scalar's display string (keys are always scalar in these authoring YAML files).
fn scalar_str(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => compact(other),
    }
}

/// Compact single-cell rendering of any YAML value — scalars as-is, sequences comma-joined,
/// mappings as a `{k: v, ...}` flow form.
fn compact(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Sequence(seq) => seq.iter().map(compact).collect::<Vec<_>>().join(", "),
        Value::Mapping(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", scalar_str(k), compact(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        Value::Tagged(t) => compact(&t.value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_scalars_as_properties_table() {
        let v: Value = serde_yaml::from_str("engine:\n  minContract: 1\nname: demo\n").unwrap();
        let body = render_yaml_body(&v);
        assert!(body.contains("| `name` | demo |"));
        assert!(body.contains("### `engine`"));
        assert!(body.contains("| `minContract` | 1 |"));
    }

    #[test]
    fn renders_list_of_maps_as_table_with_union_columns() {
        let v: Value = serde_yaml::from_str(
            "channels:\n  - name: a\n    transport: http\n  - name: b\n    transport: rabbitmq\n    queue: q1\n",
        )
        .unwrap();
        let body = render_yaml_body(&v);
        assert!(body.contains("### `channels`"));
        assert!(body.contains("| `name` | `transport` | `queue` |"));
        assert!(body.contains("| a | http |  |"));
        assert!(body.contains("| b | rabbitmq | q1 |"));
    }
}
