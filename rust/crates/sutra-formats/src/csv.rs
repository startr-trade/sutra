//! Builtin `csv` format: delimited bytes → a JSON tree — an array of row objects (with a
//! header) or an array of row arrays (without) — so a tabular message reuses the JSON
//! schema layer for typing. The *physical layout* (delimiter, header) is parser config
//! here; the *logical type contract* is a schema over this tree.
//!
//! RFC 4180 quoting is honoured (a `"`-quoted field may contain the delimiter, CR/LF, and
//! a doubled `""` escape). Values are emitted as **strings** — faithful to CSV's untyped
//! wire form; a csv schema therefore types fields as `string`. Total: only empty /
//! row-less input is FATAL.
//!
//! Part of the engine's builtin **format** family (a pure parser, distinct from the
//! schema-bound codecs), but NOT one of [`sutra_codec_spi::builtin_codecs`]'s five schema-less
//! entries — csv is config-bearing (delimiter, header flag), so a channel binds a
//! configured instance instead of a bare registry name.

use sutra_codec_spi::codec::PayloadCodec;
use sutra_codec_spi::codes::PARSE_CSV_PARSE_ERROR;
use sutra_codec_spi::issue::ValidationIssue;
use sutra_codec_spi::result::{CodecValue, DecodeResult};

// Self-registers as a zero-config global built-in (inventory pull model); the Default
// instance (comma delimiter, header row) is the zero-config form.
// csv is a FLAT-map format — text/csv (a flat json/xml/yaml cross-accept is a tracked follow-on).
// It is a FORMAT, not a codec (flat rows carry no composite structure to schema-bind).
inventory::submit! {
    sutra_codec_spi::BuiltinFormat {
        name: "csv",
        shape_class: sutra_codec_spi::ShapeClass::FlatMap,
        make: || std::sync::Arc::new(CsvCodec::default()),
    }
}

/// The `csv` format. `Default` is comma-delimited with a header row.
pub struct CsvCodec {
    delimiter: char,
    has_header: bool,
}

impl Default for CsvCodec {
    fn default() -> CsvCodec {
        CsvCodec::new(',', true)
    }
}

impl CsvCodec {
    pub const NAME: &'static str = "csv";

    pub fn new(delimiter: char, has_header: bool) -> CsvCodec {
        CsvCodec {
            delimiter,
            has_header,
        }
    }
}

impl PayloadCodec for CsvCodec {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/csv".to_string(), "application/csv".to_string()]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let ct = content_type.unwrap_or("text/csv");
        if body.is_empty() {
            return DecodeResult::fatal(
                vec![ValidationIssue::error(
                    PARSE_CSV_PARSE_ERROR,
                    "",
                    "CSV parse failed: empty document",
                )],
                ct,
            );
        }
        let rows = parse_rows(&String::from_utf8_lossy(body), self.delimiter);
        if rows.is_empty() {
            return DecodeResult::fatal(
                vec![ValidationIssue::error(
                    PARSE_CSV_PARSE_ERROR,
                    "",
                    "CSV parse failed: no rows",
                )],
                ct,
            );
        }

        let mut out: Vec<serde_json::Value> = Vec::new();
        if self.has_header {
            let header = &rows[0];
            for cells in rows.iter().skip(1) {
                let mut obj = serde_json::Map::new();
                for (c, name) in header.iter().enumerate() {
                    let value = cells.get(c).cloned().unwrap_or_default();
                    obj.insert(name.clone(), serde_json::Value::String(value));
                }
                out.push(serde_json::Value::Object(obj));
            }
        } else {
            for cells in rows {
                out.push(serde_json::Value::Array(
                    cells.into_iter().map(serde_json::Value::String).collect(),
                ));
            }
        }
        DecodeResult::ok(CodecValue::Json(serde_json::Value::Array(out)), ct)
    }

    /// The csv format is decode-only: replies are template renders, not row re-serialization.
    fn encode(
        &self,
        _payload: &CodecValue,
        _content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        Err("format 'csv' does not support encode() yet".to_string())
    }
}

impl sutra_codec_spi::schema::MessageFormat for CsvCodec {
    fn name(&self) -> &str {
        <Self as PayloadCodec>::name(self)
    }

    fn accepted_content_types(&self) -> Vec<String> {
        <Self as PayloadCodec>::accepted_content_types(self)
    }

    fn parse(
        &self,
        body: &[u8],
        content_type: Option<&str>,
    ) -> sutra_codec_spi::schema::FormatParse {
        sutra_codec_spi::schema::FormatParse::from_decode(self.decode(body, content_type))
    }
}

/// RFC 4180 row/field split: quoted fields may carry the delimiter, CR/LF, and a doubled
/// `""` escape. End-of-row on LF or CRLF (the LF of a CRLF pair is swallowed).
fn parse_rows(text: &str, delimiter: char) -> Vec<Vec<String>> {
    let chars: Vec<char> = text.chars().collect();
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if in_quotes {
            if ch == '"' {
                if chars.get(i + 1) == Some(&'"') {
                    field.push('"');
                    i += 1; // consume the escaped quote
                } else {
                    in_quotes = false; // closing quote
                }
            } else {
                field.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == delimiter {
            row.push(std::mem::take(&mut field));
        } else if ch == '\n' || ch == '\r' {
            if ch == '\r' && chars.get(i + 1) == Some(&'\n') {
                i += 1;
            }
            row.push(std::mem::take(&mut field));
            rows.push(std::mem::take(&mut row));
        } else {
            field.push(ch);
        }
        i += 1;
    }
    // Trailing field/row (no terminating newline).
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    rows
}
