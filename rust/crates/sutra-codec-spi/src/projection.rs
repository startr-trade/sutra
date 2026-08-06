//! Crate-internal helpers shared by the schema-modelled codecs.
//!
//! Every codec with an XSD-declared canonical model (`schemas/<codec>.xsd` in the codec's
//! extension folder) projects its decoded model to a FEEL-walkable map under the same
//! conventions, so `payload.body.…` navigation is uniform across codecs:
//!
//! - Keys are the schema's PascalCase element names.
//! - Absent optional values are omitted (no `null` keys).
//! - An empty collection is omitted; a **singleton collapses to its element** (the common
//!   one-record case navigates without indexing); a multi-element collection stays a list.
//!
//! [`split_lines`] is the record/segment line view shared by the line-oriented wire formats:
//! CRLF counts as ONE terminator, and empty entries (including a trailing one) are preserved
//! so parsers can spot trailing blank lines.

/// The collection projection rule: `None` for empty (key omitted), the single element for a
/// singleton, a JSON array otherwise.
pub fn collapse(mut values: Vec<serde_json::Value>) -> Option<serde_json::Value> {
    match values.len() {
        0 => None,
        1 => Some(values.remove(0)),
        _ => Some(serde_json::Value::Array(values)),
    }
}

/// Insert `key` only when the collapsed collection is non-empty.
pub fn insert_collapsed(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    values: Vec<serde_json::Value>,
) {
    if let Some(v) = collapse(values) {
        map.insert(key.to_string(), v);
    }
}

/// Insert `key` only when the optional scalar is present (absent values are omitted).
pub fn insert_opt_str(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: &Option<String>,
) {
    if let Some(s) = value {
        map.insert(key.to_string(), serde_json::Value::String(s.clone()));
    }
}

/// Split on CRLF / LF / CR — a CRLF pair is a single terminator — preserving empty entries
/// including a trailing one.
pub fn split_lines(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\r' => {
                out.push(&text[start..i]);
                i += if bytes.get(i + 1) == Some(&b'\n') {
                    2
                } else {
                    1
                };
                start = i;
            }
            b'\n' => {
                out.push(&text[start..i]);
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    out.push(&text[start..]);
    out
}

/// Substring of a char-indexed buffer — fixed-width wire formats address fields by
/// character position, not byte offset.
pub fn sub(chars: &[char], start: usize, end: usize) -> String {
    chars[start..end].iter().collect()
}
