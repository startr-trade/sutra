//! `channels.yaml` renderer: the generic table ([`super::yaml_table`]) over a *sanitised* copy of
//! the document, plus per-channel documentation of the `llm:` block and the outbound `ack-mode`.
//!
//! Secrets are references (`env:…`, `secret:…`, `vault:…`, `${…}`). The generic table would print
//! the `llm` mapping verbatim, so the sanitised copy drops it and masks every secret-bearing key;
//! the dedicated tables show only the reference *scheme*, never the reference text or a literal.

use std::fmt::Write as _;

use serde_yaml::Value;

use super::yaml_table::render_yaml_body;
use crate::util::cell;

const SECRET_KEYS: [&str; 2] = ["api-key", "callback-secret"];

/// Render a `channels.yaml` body.
pub fn render_channels_body(value: &Value) -> String {
    let mut clean = value.clone();
    mask_secrets(&mut clean);
    if let Some(items) = clean.get_mut("channels").and_then(Value::as_sequence_mut) {
        for item in items {
            if let Some(m) = item.as_mapping_mut() {
                m.remove("llm");
            }
        }
    }
    let mut out = render_yaml_body(&clean);

    let Some(items) = value.get("channels").and_then(Value::as_sequence) else {
        return out;
    };

    let mut acks = String::new();
    let mut llms = String::new();
    for item in items {
        let Some(ch) = item.as_mapping() else {
            continue;
        };
        let name = text(ch.get("name"));
        if text(ch.get("direction")) == "outbound" {
            let label = match text(ch.get("ack-mode")).as_str() {
                "on-complete" => Some("sync (on-complete)"),
                "on-persist" => Some("async (on-persist)"),
                _ => None,
            };
            if let Some(l) = label {
                let _ = writeln!(acks, "| `{}` | {l} |", cell(&name));
            }
        }
        if let Some(llm) = ch.get("llm").and_then(Value::as_mapping) {
            llms.push_str(&render_llm(&name, llm));
        }
        // Callback channel markers may sit at channel level.
        if ch.contains_key("callback-for") || ch.contains_key("callback-secret") {
            let _ = writeln!(
                llms,
                "### Callback channel — `{}`\n\n| Key | Value |\n|---|---|\n| `callback-for` | {} |\n| `callback-secret` | {} |\n",
                cell(&name),
                cell(&text(ch.get("callback-for"))),
                secret_ref(ch.get("callback-secret")),
            );
        }
    }
    if !acks.is_empty() {
        out.push_str("### Outbound ack mode\n\n| Channel | Ack mode |\n|---|---|\n");
        out.push_str(&acks);
        out.push('\n');
    }
    out.push_str(&llms);
    out
}

fn render_llm(name: &str, llm: &serde_yaml::Mapping) -> String {
    let mut out = format!("### LLM — `{}`\n\n", cell(name));
    out.push_str("| Key | Value |\n|---|---|\n");
    for key in [
        "mode",
        "reply-channel",
        "system",
        "max-output-tokens",
        "temperature",
        "timeout-seconds",
        "concurrency",
        "fallback-on",
        "require",
        "callback-for",
    ] {
        if let Some(v) = llm.get(key) {
            let _ = writeln!(out, "| `{key}` | {} |", cell(&text(Some(v))));
        }
    }
    if llm.contains_key("callback-secret") {
        let _ = writeln!(
            out,
            "| `callback-secret` | {} |",
            secret_ref(llm.get("callback-secret"))
        );
    }
    out.push('\n');

    let cands: Vec<&serde_yaml::Mapping> = llm
        .get("candidates")
        .and_then(Value::as_sequence)
        .map(|s| s.iter().filter_map(Value::as_mapping).collect())
        .unwrap_or_default();
    if cands.is_empty() {
        return out;
    }
    let has = |k: &str| cands.iter().any(|c| c.contains_key(k));
    let job = [
        "callback-channel",
        "poll-interval-seconds",
        "job-timeout-seconds",
        "callback-secret",
    ]
    .into_iter()
    .filter(|k| has(k))
    .collect::<Vec<_>>();
    out.push_str("| # | Provider | Model | Endpoint override | Completion | API key |");
    for k in &job {
        let _ = write!(out, " `{k}` |");
    }
    out.push_str("\n|---|---|---|---|---|---|");
    for _ in &job {
        out.push_str("---|");
    }
    out.push('\n');
    for (i, c) in cands.iter().enumerate() {
        let get = |k: &str| cell(&text(c.get(k)));
        let _ = write!(
            out,
            "| {} | {} | {} | {} | {} | {} |",
            i + 1,
            get("provider"),
            get("model"),
            get("base-url"),
            get("completion"),
            secret_ref(c.get("api-key")),
        );
        for k in &job {
            if *k == "callback-secret" {
                let _ = write!(out, " {} |", secret_ref(c.get(k)));
            } else {
                let _ = write!(out, " {} |", get(k));
            }
        }
        out.push('\n');
    }
    out.push('\n');
    out
}

/// Only the scheme of a secret reference; a literal (no known scheme) is never echoed.
fn secret_ref(v: Option<&Value>) -> String {
    match v.and_then(Value::as_str) {
        None => String::new(),
        Some(s) => mask(s),
    }
}

fn mask(s: &str) -> String {
    let t = s.trim();
    if t.starts_with("${") {
        return "`${…}`".to_string();
    }
    for scheme in ["env", "secret", "vault"] {
        if t.strip_prefix(scheme).is_some_and(|r| r.starts_with(':')) {
            return format!("`{scheme}:…`");
        }
    }
    "`(literal — hidden)`".to_string()
}

fn mask_secrets(v: &mut Value) {
    match v {
        Value::Mapping(m) => {
            for (k, val) in m.iter_mut() {
                if k.as_str().is_some_and(|k| SECRET_KEYS.contains(&k)) {
                    if let Some(s) = val.as_str() {
                        *val = Value::String(mask(s).replace('`', ""));
                    }
                } else {
                    mask_secrets(val);
                }
            }
        }
        Value::Sequence(s) => s.iter_mut().for_each(mask_secrets),
        _ => {}
    }
}

fn text(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::Null) => String::new(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Sequence(s)) => s
            .iter()
            .map(|x| text(Some(x)))
            .collect::<Vec<_>>()
            .join(", "),
        Some(other) => serde_yaml::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_string(),
    }
}
