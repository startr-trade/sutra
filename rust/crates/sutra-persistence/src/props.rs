//! Properties-line format reader/writer — the snapshot v2 container.
//!
//! The Properties-line escaping rules are part of the persisted snapshot format — changing
//! them is a format break. This module implements them from the spec:
//!
//! - one `key=value` line per entry, `\n`-terminated;
//! - keys escape every space; values escape only a *leading* space;
//! - `\t` `\n` `\r` `\f` escape to their two-character forms; `\` doubles;
//! - `=` `:` `#` `!` are `\`-prefixed;
//! - every UTF-16 code unit outside `0x20..=0x7E` escapes to `\uXXXX` (uppercase hex), so the
//!   emitted bytes are pure ASCII (an ISO-8859-1-safe strict subset);
//! - the writer emits **no comment lines** and sorts lines byte-wise ascending — determinism
//!   is normative (identical logical state ⇒ identical bytes).
//!
//! The reader implements the matching logical-line rules (comment lines,
//! blank lines, `\`-continuations, `=`/`:`/whitespace separators, `\uXXXX` and single-char
//! unescapes), so it accepts the whole format — a deliberate superset of the strict subset
//! the writer emits — and every writer output reads back identically.

/// Escapes one string per the Properties-line escaping rules.
///
/// `escape_space == true` is the key flavour (every space escaped); `false` is the value
/// flavour (only a leading space escaped).
fn save_convert(s: &str, escape_space: bool) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    let mut first = true;
    for unit in s.encode_utf16() {
        match unit {
            0x5C => out.push_str("\\\\"), // backslash doubles
            0x20 => {
                // Keys escape every space; values only a leading one.
                if first || escape_space {
                    out.push('\\');
                }
                out.push(' ');
            }
            0x09 => out.push_str("\\t"),
            0x0A => out.push_str("\\n"),
            0x0D => out.push_str("\\r"),
            0x0C => out.push_str("\\f"),
            0x3D | 0x3A | 0x23 | 0x21 => {
                // '=' ':' '#' '!'
                out.push('\\');
                out.push(char::from(unit as u8));
            }
            _ if (0x20..=0x7E).contains(&unit) => out.push(char::from(unit as u8)),
            _ => {
                // Outside printable ASCII: \uXXXX with uppercase hex.
                out.push_str(&format!("\\u{unit:04X}"));
            }
        }
        first = false;
    }
    out
}

/// Serialises entries to the canonical byte form: escaped `key=value` lines, sorted byte-wise
/// ascending, `\n` after every line, no comments.
///
/// The iteration order of `entries` does not matter — sorting happens on the escaped lines,
/// as the container's canonical byte ordering requires.
pub fn write_lines<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<u8> {
    let mut lines: Vec<String> = entries
        .into_iter()
        .map(|(k, v)| {
            let mut line = save_convert(k, true);
            line.push('=');
            line.push_str(&save_convert(v, false));
            line
        })
        .collect();
    lines.sort_unstable();
    let mut out = Vec::new();
    for line in lines {
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
    }
    out
}

/// Parses Properties-format bytes into `(key, value)` pairs in file order.
///
/// Duplicate keys keep the *last* occurrence (last-write-wins, one entry per pair).
/// Bytes are decoded as ISO-8859-1 (byte == code point), per the Properties-line load rules.
pub fn read_lines(bytes: &[u8]) -> Result<Vec<(String, String)>, String> {
    // ISO-8859-1 decode: every byte maps to the same code point.
    let chars: Vec<char> = bytes.iter().map(|&b| char::from(b)).collect();
    let mut pairs = Vec::new();

    let mut pos = 0usize;
    while pos < chars.len() {
        // --- read one natural line (\n, \r, or \r\n terminated) ---------------------------
        let start = pos;
        while pos < chars.len() && chars[pos] != '\n' && chars[pos] != '\r' {
            pos += 1;
        }
        let mut line: Vec<char> = chars[start..pos].to_vec();
        // Consume the terminator (\r\n counts as one).
        if pos < chars.len() {
            if chars[pos] == '\r' && pos + 1 < chars.len() && chars[pos + 1] == '\n' {
                pos += 2;
            } else {
                pos += 1;
            }
        }

        // --- skip blanks and comment lines -------------------------------------------------
        let first_non_ws = line.iter().position(|c| !is_ws(*c));
        let Some(first_non_ws) = first_non_ws else {
            continue; // blank line
        };
        if line[first_non_ws] == '#' || line[first_non_ws] == '!' {
            continue; // comment line
        }
        line.drain(..first_non_ws);

        // --- logical-line continuation: odd number of trailing backslashes -----------------
        while ends_with_odd_backslashes(&line) {
            line.pop(); // drop the continuation backslash
                        // Read the next natural line and strip its leading whitespace.
            let start = pos;
            while pos < chars.len() && chars[pos] != '\n' && chars[pos] != '\r' {
                pos += 1;
            }
            let next: &[char] = &chars[start..pos];
            if pos < chars.len() {
                if chars[pos] == '\r' && pos + 1 < chars.len() && chars[pos + 1] == '\n' {
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            let skip = next.iter().position(|c| !is_ws(*c)).unwrap_or(next.len());
            line.extend_from_slice(&next[skip..]);
        }

        // --- split key / value at the first unescaped separator ----------------------------
        let mut key_end = line.len();
        let mut i = 0usize;
        while i < line.len() {
            let c = line[i];
            if c == '\\' {
                i += 2; // escaped char: skip it (never a separator)
                continue;
            }
            if c == '=' || c == ':' || is_ws(c) {
                key_end = i;
                break;
            }
            i += 1;
        }
        let key: String = load_convert(&line[..key_end])?;
        // Skip whitespace, one optional '=' or ':', then whitespace again.
        let mut v = key_end;
        while v < line.len() && is_ws(line[v]) {
            v += 1;
        }
        if v < line.len() && (line[v] == '=' || line[v] == ':') {
            v += 1;
            while v < line.len() && is_ws(line[v]) {
                v += 1;
            }
        }
        let value: String = load_convert(&line[v..])?;
        pairs.push((key, value));
    }
    Ok(pairs)
}

fn is_ws(c: char) -> bool {
    c == ' ' || c == '\t' || c == '\u{c}'
}

fn ends_with_odd_backslashes(line: &[char]) -> bool {
    let mut n = 0usize;
    for c in line.iter().rev() {
        if *c == '\\' {
            n += 1;
        } else {
            break;
        }
    }
    n % 2 == 1
}

/// Unescapes one key or value per the Properties-line load rules, reassembling UTF-16
/// surrogate pairs written as consecutive `\uXXXX` escapes.
fn load_convert(chars: &[char]) -> Result<String, String> {
    let mut units: Vec<u16> = Vec::with_capacity(chars.len());
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c != '\\' {
            let mut buf = [0u16; 2];
            units.extend_from_slice(c.encode_utf16(&mut buf));
            i += 1;
            continue;
        }
        i += 1;
        let Some(&esc) = chars.get(i) else {
            // Trailing lone backslash: the load rules would have consumed it as a
            // continuation; treat as literal end.
            break;
        };
        i += 1;
        match esc {
            'u' => {
                if i + 4 > chars.len() {
                    return Err("malformed \\uXXXX encoding".to_owned());
                }
                let mut value: u16 = 0;
                for &h in &chars[i..i + 4] {
                    let digit = h
                        .to_digit(16)
                        .ok_or_else(|| "malformed \\uXXXX encoding".to_owned())?;
                    value = (value << 4) | (digit as u16);
                }
                units.push(value);
                i += 4;
            }
            't' => units.push(u16::from(b'\t')),
            'n' => units.push(u16::from(b'\n')),
            'r' => units.push(u16::from(b'\r')),
            'f' => units.push(u16::from(0x0Cu8)),
            other => {
                let mut buf = [0u16; 2];
                units.extend_from_slice(other.encode_utf16(&mut buf));
            }
        }
    }
    String::from_utf16(&units).map_err(|_| "invalid UTF-16 sequence in \\u escapes".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(pairs: &[(&str, &str)]) {
        let bytes = write_lines(pairs.iter().copied());
        let read = read_lines(&bytes).unwrap();
        let mut expected: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        expected.sort();
        let mut actual = read;
        actual.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn plain_pairs_round_trip() {
        round_trip(&[("a", "1"), ("b", "two"), ("c.d.e", "x,y,z")]);
    }

    #[test]
    fn special_characters_round_trip() {
        round_trip(&[
            ("key with spaces", "value with spaces"),
            ("k=e:y", "v=a:l#u!e"),
            ("tab\tkey", "line1\nline2\rline3\u{c}line4"),
            ("back\\slash", "trailing space "),
            (" leading", " leading-value-space"),
            ("", "empty key ok"),
            ("empty-value", ""),
        ]);
    }

    #[test]
    fn unicode_round_trips_via_utf16_escapes() {
        round_trip(&[("caf\u{e9}", "\u{4e16}\u{754c}"), ("emoji", "\u{1F600}")]);
        // Supplementary chars are written as surrogate-pair escapes.
        let bytes = write_lines([("e", "\u{1F600}")]);
        assert_eq!(String::from_utf8(bytes).unwrap(), "e=\\uD83D\\uDE00\n");
    }

    #[test]
    fn writer_output_is_sorted_ascii_lines() {
        let bytes = write_lines([("b", "2"), ("a", "1")]);
        assert_eq!(String::from_utf8(bytes).unwrap(), "a=1\nb=2\n");
    }

    #[test]
    fn value_spaces_only_leading_escaped() {
        let bytes = write_lines([("k", " a b ")]);
        assert_eq!(String::from_utf8(bytes).unwrap(), "k=\\ a b \n");
        let bytes = write_lines([("a b", "v")]);
        assert_eq!(String::from_utf8(bytes).unwrap(), "a\\ b=v\n");
    }

    #[test]
    fn reader_handles_java_load_variants() {
        // ':' separator, whitespace separator, comments, blank lines, continuation lines.
        let text = "# comment\n! also comment\n\nk1:v1\nk2 v2\nk3 = v3\nlong\\\n    tail=joined\n";
        let pairs = read_lines(text.as_bytes()).unwrap();
        assert_eq!(
            pairs,
            vec![
                ("k1".to_owned(), "v1".to_owned()),
                ("k2".to_owned(), "v2".to_owned()),
                ("k3".to_owned(), "v3".to_owned()),
                ("longtail".to_owned(), "joined".to_owned()),
            ]
        );
    }

    #[test]
    fn reader_rejects_malformed_unicode_escape() {
        assert!(read_lines(b"k=\\u12").is_err());
        assert!(read_lines(b"k=\\uZZZZ").is_err());
    }
}
