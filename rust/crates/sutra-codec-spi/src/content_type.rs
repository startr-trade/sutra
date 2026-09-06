//! Media-type matching for the inbound capability gate — the content-type
//! matcher contract.
//!
//! Fail-open by construction: an empty accepted list ("declares none") and a blank
//! inbound content-type ("unknown") both ADMIT — the only rejections are genuine
//! mismatches (the VoIP call-drop case). Patterns: exact, `*/*` / `*`, `type/*`, and the
//! RFC 6839 structured-syntax-suffix wildcard `type/*+suffix`. Case-insensitive;
//! `;`-delimited parameters stripped.

/// True when `content_type` is admitted by `accepted_patterns`.
pub fn accepts(accepted_patterns: &[String], content_type: Option<&str>) -> bool {
    if accepted_patterns.is_empty() {
        return true;
    }
    let ct = normalize(content_type.unwrap_or(""));
    if ct.is_empty() {
        return true;
    }
    accepted_patterns
        .iter()
        .any(|p| matches(&normalize(p), &ct))
}

/// Lower-case, trim, and drop any `;`-delimited parameters (charset, boundary, …).
fn normalize(s: &str) -> String {
    let s = match s.find(';') {
        Some(i) => &s[..i],
        None => s,
    };
    s.trim().to_lowercase()
}

fn matches(pattern: &str, ct: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }
    if pattern == "*/*" || pattern == "*" || pattern == ct {
        return true;
    }
    let (Some((p_type, p_sub)), Some((c_type, c_sub))) =
        (pattern.split_once('/'), ct.split_once('/'))
    else {
        return false;
    };
    if p_type != c_type {
        return false;
    }
    if p_sub == "*" {
        return true;
    }
    // Structured-syntax-suffix wildcard: application/*+xml matches application/foo+xml.
    if let Some(suffix) = p_sub.strip_prefix("*+") {
        return c_sub.ends_with(&format!("+{suffix}"));
    }
    false
}
