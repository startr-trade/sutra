//! Offset → 1-based `(line, column)` conversion.
//!
//! Offsets are **character** offsets into the FEEL source (the reference implementation indexes
//! UTF-16 code units; the two coincide for all BMP text — supplementary-plane characters in a
//! FEEL expression would shift offsets by one per astral character; documented divergence).
//! Line breaks are recognised in three flavours — `\n`, `\r\n`, `\r`.

use crate::error::SourceLocation;

/// Synthetic URI used when callers don't pass a real source file.
pub const INLINE_FEEL_URI: &str = "feel:inline";

pub struct FeelSourcePositions {
    source: Vec<char>,
    uri: String,
}

impl FeelSourcePositions {
    pub fn new(source: &str, uri: &str) -> Self {
        FeelSourcePositions {
            source: source.chars().collect(),
            uri: if uri.is_empty() {
                INLINE_FEEL_URI.to_string()
            } else {
                uri.to_string()
            },
        }
    }

    /// Location for a single-point offset (caret only, no end range).
    pub fn location_for(&self, offset: usize) -> SourceLocation {
        let (line, column) = self.offset_to_line_column(self.clamp(offset));
        SourceLocation {
            uri: self.uri.clone(),
            line,
            column,
            end_line: None,
            end_column: None,
        }
    }

    /// Location spanning `[start_offset, end_offset)` — e.g. an unclosed string literal
    /// running from its opening quote to end-of-input.
    pub fn range_for(&self, start_offset: usize, end_offset: usize) -> SourceLocation {
        let (line, column) = self.offset_to_line_column(self.clamp(start_offset));
        let (end_line, end_column) = self.offset_to_line_column(self.clamp(end_offset));
        SourceLocation {
            uri: self.uri.clone(),
            line,
            column,
            end_line: Some(end_line),
            end_column: Some(end_column),
        }
    }

    /// The raw source text spanning `[start_offset, end_offset)`, verbatim (whitespace and all)
    /// — used where the FEEL grammar itself calls for the ORIGINAL written text rather than a
    /// re-synthesized join of token text (a context-entry key's "Name" production, DMN-TCK
    /// 0057-feel-context#004/#005: `{foo bar: ...}`'s key is exactly `"foo bar"`, `{foo+bar:
    /// ...}`'s is exactly `"foo+bar"` — the two shapes only differ in the source's OWN spacing,
    /// which a token-text-joined-with-a-fixed-separator reconstruction can't reproduce).
    pub fn slice(&self, start_offset: usize, end_offset: usize) -> String {
        let start = self.clamp(start_offset);
        let end = self.clamp(end_offset).max(start);
        self.source[start..end].iter().collect()
    }

    fn clamp(&self, offset: usize) -> usize {
        offset.min(self.source.len())
    }

    fn offset_to_line_column(&self, offset: usize) -> (u32, u32) {
        let mut line = 1u32;
        let mut line_start = 0usize;
        let mut i = 0usize;
        while i < offset {
            let c = self.source[i];
            if c == '\r' {
                line += 1;
                // \r\n counts as one line break.
                if i + 1 < self.source.len() && self.source[i + 1] == '\n' {
                    i += 1;
                }
                line_start = i + 1;
            } else if c == '\n' {
                line += 1;
                line_start = i + 1;
            }
            i += 1;
        }
        (line, (offset - line_start + 1) as u32)
    }
}
