//! The deployment configuration — `package.yaml` `properties:`, sealed into `manifest.yaml`.
//!
//! A flat map of keys to TEXT. A value is a literal, or an environment reference in the forms
//! the channel and datastore YAMLs already use: a whole value `env:NAME`, or `${NAME}` /
//! `${NAME:default}` placeholders inside a value. What is sealed is the UNRESOLVED text, so the
//! archive (and its deploymentId) is the same in every environment; the engine resolves the
//! references when it plans the deployment.
//!
//! Timer expressions read the map as `config.<key>`, which is why two things are refused here:
//! a key that is not a plain FEEL name, and a secret reference (`secret:KEY`,
//! `${secret:KEY}`). The configuration is visible to every timer expression and to the audit
//! trail, so a secret never belongs in it.

use std::collections::BTreeMap;

/// Is `key` usable as `config.<key>` in FEEL: `[A-Za-z_][A-Za-z0-9_]*`.
pub fn is_property_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Why a property value is refused at package time, if it is: a secret reference.
pub fn property_value_refusal(value: &str) -> Option<String> {
    let trimmed = value.trim_start();
    if trimmed.starts_with("secret:") || value.contains("${secret:") {
        return Some(
            "a secret reference is not allowed in properties: the deployment configuration is \
             readable by every timer expression and the audit trail; keep secrets in the \
             channel or datastore configuration that needs them"
                .to_string(),
        );
    }
    None
}

/// Parse a `properties:` YAML value (null = empty). `invalid` builds the caller's error.
pub fn parse_properties<E>(
    value: &serde_yaml::Value,
    invalid: impl Fn(String) -> E,
) -> Result<BTreeMap<String, String>, E> {
    let mut out = BTreeMap::new();
    if value.is_null() {
        return Ok(out);
    }
    let map = value
        .as_mapping()
        .ok_or_else(|| invalid("properties must be a mapping of key: value".to_string()))?;
    for (k, v) in map {
        let key = k
            .as_str()
            .ok_or_else(|| invalid("properties keys must be strings".to_string()))?;
        if !is_property_key(key) {
            return Err(invalid(format!(
                "property key '{key}' is not usable as config.{key} in FEEL; use letters, \
                 digits and '_' only, starting with a letter or '_'"
            )));
        }
        let text = match v {
            serde_yaml::Value::String(s) => s.clone(),
            serde_yaml::Value::Number(n) => n.to_string(),
            serde_yaml::Value::Bool(b) => b.to_string(),
            _ => {
                return Err(invalid(format!(
                    "property '{key}' must be a scalar (a string, number or boolean)"
                )))
            }
        };
        if let Some(reason) = property_value_refusal(&text) {
            return Err(invalid(format!("property '{key}': {reason}")));
        }
        out.insert(key.to_string(), text);
    }
    Ok(out)
}

/// Append the canonical `properties:` block (nothing when empty, so a manifest without
/// properties keeps its exact pre-existing bytes and deploymentId).
pub fn push_properties(out: &mut String, properties: &BTreeMap<String, String>) {
    if properties.is_empty() {
        return;
    }
    out.push_str("properties:\n");
    for (key, value) in properties {
        out.push_str(&format!("  {}: {}\n", quote(key), quote(value)));
    }
}

/// YAML double-quoted scalar (the manifest's uniform quoting).
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<BTreeMap<String, String>, String> {
        parse_properties(&serde_yaml::from_str(yaml).unwrap(), |m| m)
    }

    #[test]
    fn scalars_are_kept_as_text_and_round_trip() {
        let props =
            parse("listenWindow: PT10M\nretries: 3\nenabled: true\nwait: \"${W:PT1S}\"\n").unwrap();
        assert_eq!(props["listenWindow"], "PT10M");
        assert_eq!(props["retries"], "3");
        assert_eq!(props["enabled"], "true");
        assert_eq!(props["wait"], "${W:PT1S}");
        let mut out = String::new();
        push_properties(&mut out, &props);
        let back: serde_yaml::Value = serde_yaml::from_str(&out).unwrap();
        let back = parse_properties(&back["properties"], |m| m).unwrap();
        assert_eq!(back, props);
    }

    #[test]
    fn bad_keys_secrets_and_non_scalars_are_refused() {
        assert!(parse("listen-window: PT1S\n").is_err());
        assert!(parse("1st: PT1S\n").is_err());
        assert!(parse("token: secret:API_TOKEN\n")
            .unwrap_err()
            .contains("secret"));
        assert!(parse("dsn: \"x${secret:PASS}\"\n")
            .unwrap_err()
            .contains("secret"));
        assert!(parse("nested: { a: 1 }\n").is_err());
        assert!(parse("[a, b]\n").is_err());
    }

    #[test]
    fn empty_emits_nothing() {
        let mut out = String::new();
        push_properties(&mut out, &BTreeMap::new());
        assert!(out.is_empty());
        assert!(parse("~\n").unwrap().is_empty());
    }
}
