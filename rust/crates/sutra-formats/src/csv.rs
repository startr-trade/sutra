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

    /// Rows back to delimited bytes — the inverse of [`Self::decode`], so a csv channel can
    /// answer in csv (symmetric reply) and a problem document can come back as a table.
    ///
    /// Accepts the three shapes decode can produce or a caller can build: a bare array of row
    /// objects, the `{"value": [rows]}` wrapper an array root projects under, and a single row
    /// object (encoded as a one-row table). With a header, the column set is the union of every
    /// row's keys, so a row missing a late-added key still lines up under a blank cell.
    /// Headerless rows are arrays of cells, emitted positionally.
    ///
    /// **Column ORDER is alphabetical, not the source header's order.** `serde_json::Map` is a
    /// `BTreeMap` in this workspace, so a decoded row has already lost the order its header
    /// carried — there is nothing left to preserve. Encoding is therefore value-faithful and
    /// order-normalising: `decode(encode(rows)) == rows`, while `encode(decode(bytes))` returns
    /// the same table with its columns sorted. Deterministic, which is what a wire format needs.
    ///
    /// Cells render through the same canonical text the rest of the engine uses: a string as
    /// itself, a number in its exact written form (`serde_json`'s arbitrary precision, so
    /// `"0.0250"` stays `"0.0250"`), a bool as `true`/`false`, a null as an EMPTY cell (which
    /// round-trips to the absent-optional an empty cell decodes as). A nested object/array in a
    /// cell is an error, not a silently-stringified blob — csv is flat by construction.
    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let CodecValue::Json(tree) = payload else {
            return Err("format 'csv' encodes a JSON tree of rows, not raw text/bytes".to_string());
        };
        let rows = rows_of(tree)?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = String::new();
        if self.has_header {
            let columns = header_of(&rows)?;
            write_row(&mut out, columns.iter().map(String::as_str), self.delimiter);
            for row in &rows {
                let cells: Result<Vec<String>, String> = columns
                    .iter()
                    .map(|c| cell_text(row.get(c.as_str()).unwrap_or(&serde_json::Value::Null)))
                    .collect();
                write_row(&mut out, cells?.iter().map(String::as_str), self.delimiter);
            }
        } else {
            for row in &rows {
                let serde_json::Value::Array(cells) = row else {
                    return Err(
                        "format 'csv' without a header encodes rows as arrays of cells".to_string(),
                    );
                };
                let cells: Result<Vec<String>, String> = cells.iter().map(cell_text).collect();
                write_row(&mut out, cells?.iter().map(String::as_str), self.delimiter);
            }
        }
        Ok(out.into_bytes())
    }
}

/// The row list inside the three accepted encode shapes.
fn rows_of(tree: &serde_json::Value) -> Result<Vec<serde_json::Value>, String> {
    match tree {
        serde_json::Value::Array(rows) => Ok(rows.clone()),
        // The wrapper an array root projects under (`payload.value`), so a decoded batch
        // re-encodes without the caller unwrapping it first.
        serde_json::Value::Object(map) => match map.get("value") {
            Some(serde_json::Value::Array(rows)) => Ok(rows.clone()),
            _ => Ok(vec![tree.clone()]),
        },
        _ => {
            Err("format 'csv' encodes rows (an array, or one row object), not a scalar".to_string())
        }
    }
}

/// The header: the union of every row's keys (alphabetical — see `encode`).
fn header_of(rows: &[serde_json::Value]) -> Result<Vec<String>, String> {
    let mut columns: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for row in rows {
        let serde_json::Value::Object(map) = row else {
            return Err(
                "format 'csv' with a header encodes rows as name→scalar objects".to_string(),
            );
        };
        columns.extend(map.keys().cloned());
    }
    Ok(columns.into_iter().collect())
}

/// One cell's canonical text. A null is an empty cell; a composite is refused (csv is flat).
fn cell_text(value: &serde_json::Value) -> Result<String, String> {
    Ok(match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            return Err(
                "format 'csv' cannot encode a nested value in a cell — csv rows are flat"
                    .to_string(),
            )
        }
    })
}

/// Write one RFC 4180 row: a cell carrying the delimiter, a quote or CR/LF is quoted, with `"`
/// doubled. `decode` reads back exactly what this writes.
fn write_row<'a>(out: &mut String, cells: impl Iterator<Item = &'a str>, delimiter: char) {
    let mut first = true;
    for cell in cells {
        if !first {
            out.push(delimiter);
        }
        first = false;
        if cell.contains(delimiter)
            || cell.contains('"')
            || cell.contains('\n')
            || cell.contains('\r')
        {
            out.push('"');
            out.push_str(&cell.replace('"', "\"\""));
            out.push('"');
        } else {
            out.push_str(cell);
        }
    }
    out.push('\n');
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
