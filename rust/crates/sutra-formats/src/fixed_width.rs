//! Builtin `fixed-width` format: fixed-column records → a JSON tree — an array of one object
//! per line, each keyed by the configured field layout — so a fixed-width message reuses the
//! schema layer for typing. As with [`crate::csv`] the *physical layout* (the field widths) is
//! parser config; the *logical type contract* is a schema over the parsed tree.
//!
//! Each line yields one object; a value is the trimmed slice `[offset, offset+width)` of the
//! line, offsets being the running sum of prior widths. A short line is tolerated — a field past
//! the line's end is the empty string. Values are emitted as **strings**, faithful to the untyped
//! wire form; typing is the schema's job (an `xs:int` column arrives as a number because the
//! bound XSD says so, not because this parser guessed).
//!
//! ## Why this one carries no zero-config default
//!
//! Every other built-in format is self-describing enough to parse with no configuration: JSON and
//! XML carry their own structure, and a header-bearing CSV names its own columns. A fixed-width
//! record carries **nothing** — without the widths, a line is an undifferentiated string. So
//! there is no meaningful default layout, and consequently this format does **not**
//! `inventory::submit!` a [`sutra_codec_spi::BuiltinFormat`]: a channel cannot bind it bare. It is
//! reached only through a schema codec whose `codec-manifest.yaml` declares the layout, which is
//! exactly the gap that got the original codec deleted ("no xsd/json way to express its column
//! layout") and what the manifest layout block now fills.
//!
//! It needs no hand-written [`sutra_codec_spi::MessageFormat`] impl either — the generic
//! `PayloadCodecFormat` adapter lifts any [`PayloadCodec`] into one.

use sutra_codec_spi::codec::PayloadCodec;
use sutra_codec_spi::codes::PARSE_CSV_PARSE_ERROR;
use sutra_codec_spi::issue::ValidationIssue;
use sutra_codec_spi::result::{CodecValue, DecodeResult};

/// One fixed-width column: a field `name` occupying `width` characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedWidthField {
    name: String,
    width: usize,
}

impl FixedWidthField {
    /// A named column of `width > 0` characters; a blank name or a zero width is a configuration
    /// error, not a silently-skipped column.
    pub fn new(name: impl Into<String>, width: usize) -> Result<FixedWidthField, String> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err("fixed-width field name is required".to_string());
        }
        if width == 0 {
            return Err(format!("fixed-width field '{name}' must have width > 0"));
        }
        Ok(FixedWidthField { name, width })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn width(&self) -> usize {
        self.width
    }
}

/// The `fixed-width` format, configured with its column layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedWidthCodec {
    layout: Vec<FixedWidthField>,
}

impl FixedWidthCodec {
    pub const NAME: &'static str = "fixed-width";

    /// A layout must declare at least one field, and no two fields may share a name (a duplicate
    /// would make one column silently unreachable in the parsed object).
    pub fn new(layout: Vec<FixedWidthField>) -> Result<FixedWidthCodec, String> {
        if layout.is_empty() {
            return Err("fixed-width layout must declare at least one field".to_string());
        }
        for (i, f) in layout.iter().enumerate() {
            if layout[..i].iter().any(|prior| prior.name == f.name) {
                return Err(format!(
                    "fixed-width layout declares field '{}' twice — one would be unreachable",
                    f.name
                ));
            }
        }
        Ok(FixedWidthCodec { layout })
    }

    /// The declared columns, in wire order.
    pub fn layout(&self) -> &[FixedWidthField] {
        &self.layout
    }

    /// The total record width — the sum of every column.
    pub fn record_width(&self) -> usize {
        self.layout.iter().map(|f| f.width).sum()
    }
}

impl PayloadCodec for FixedWidthCodec {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec![
            "text/plain".to_string(),
            "application/x-fixed-width".to_string(),
        ]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let ct = content_type.unwrap_or("text/plain");
        if body.is_empty() {
            return DecodeResult::fatal(
                vec![ValidationIssue::error(
                    PARSE_CSV_PARSE_ERROR,
                    "",
                    "fixed-width parse failed: empty document",
                )],
                ct,
            );
        }
        let text = String::from_utf8_lossy(body);
        let mut out: Vec<serde_json::Value> = Vec::new();
        for line in split_lines(&text) {
            if line.trim().is_empty() {
                continue; // blank line, including a trailing newline's empty tail
            }
            let chars: Vec<char> = line.chars().collect();
            let mut obj = serde_json::Map::new();
            let mut offset = 0usize;
            for f in &self.layout {
                let start = offset.min(chars.len());
                let end = (offset + f.width).min(chars.len());
                let value: String = chars[start..end].iter().collect();
                obj.insert(
                    f.name.clone(),
                    serde_json::Value::String(value.trim().to_string()),
                );
                offset += f.width;
            }
            out.push(serde_json::Value::Object(obj));
        }
        if out.is_empty() {
            return DecodeResult::fatal(
                vec![ValidationIssue::error(
                    PARSE_CSV_PARSE_ERROR,
                    "",
                    "fixed-width parse failed: no records",
                )],
                ct,
            );
        }
        DecodeResult::ok(CodecValue::Json(serde_json::Value::Array(out)), ct)
    }

    /// Records back to fixed-column bytes — the inverse of [`Self::decode`], so a fixed-width
    /// channel can answer in its own format and a problem document can come back as records.
    ///
    /// Accepts the same three shapes csv's encode does: a bare array of row objects, the
    /// `{"value": [rows]}` wrapper an array root projects under, and a single row object. Each
    /// value is left-aligned and space-padded to its column width; a value LONGER than its column
    /// is an error rather than a silent truncation, because truncating a fixed-width field
    /// corrupts every column after it on that line. An absent field is an empty (all-space)
    /// column, which round-trips to the empty string `decode` would read back.
    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let CodecValue::Json(tree) = payload else {
            return Err(
                "format 'fixed-width' encodes a JSON tree of records, not raw text/bytes"
                    .to_string(),
            );
        };
        let rows = match tree {
            serde_json::Value::Array(rows) => rows.clone(),
            serde_json::Value::Object(map) => match map.get("value") {
                Some(serde_json::Value::Array(rows)) => rows.clone(),
                _ => vec![tree.clone()],
            },
            _ => {
                return Err(
                    "format 'fixed-width' encodes records (an array, or one record object), \
                     not a scalar"
                        .to_string(),
                )
            }
        };
        let mut out = String::new();
        for row in &rows {
            let serde_json::Value::Object(map) = row else {
                return Err(
                    "format 'fixed-width' encodes records as name→scalar objects".to_string(),
                );
            };
            for f in &self.layout {
                let text = match map.get(&f.name) {
                    None | Some(serde_json::Value::Null) => String::new(),
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(serde_json::Value::Number(n)) => n.to_string(),
                    Some(serde_json::Value::Bool(b)) => b.to_string(),
                    Some(_) => {
                        return Err(format!(
                            "format 'fixed-width' cannot encode a nested value in field '{}' — \
                             records are flat",
                            f.name
                        ))
                    }
                };
                let len = text.chars().count();
                if len > f.width {
                    return Err(format!(
                        "format 'fixed-width' field '{}' holds {len} characters but its column is \
                         {} wide — truncating would shift every column after it",
                        f.name, f.width
                    ));
                }
                out.push_str(&text);
                for _ in len..f.width {
                    out.push(' ');
                }
            }
            out.push('\n');
        }
        Ok(out.into_bytes())
    }
}

/// Split on CRLF / LF / CR — a CRLF pair is a single terminator.
fn split_lines(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let (mut start, mut i) = (0usize, 0usize);
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
