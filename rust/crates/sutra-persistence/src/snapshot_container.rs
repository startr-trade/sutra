//! The binary snapshot container (snapshot format 5, since 0.6.7).
//!
//! A snapshot is a map of string keys to string values (the key families and value encodings are
//! described in [`crate::snapshot`]). Up to format 4 that map was written as Properties-format
//! text: every character of every key and value went through the Properties escaping rules on
//! write and back through them on read, which made encoding and decoding a noticeable share of a
//! step's CPU. Format 5 writes the same map as length-prefixed UTF-8:
//!
//! ```text
//! MAGIC                       b"\\uSNAPSHOT/5\n"   (13 bytes)
//! entry count                 u32, little-endian
//! per entry, keys ascending:  u32 key length, key bytes, u32 value length, value bytes
//! ```
//!
//! * **Deterministic.** Entries are sorted by key bytes and keys are unique, so identical
//!   logical state still produces identical bytes.
//! * **Detected by its marker.** Every earlier snapshot starts with a Properties line for a
//!   `sutra.` key; none can start with the magic. A reader that does not know format 5 (a 0.6.6 or
//!   older engine) parses the magic as a Properties key, meets the invalid `\uSNAP` escape, and
//!   fails the decode with "malformed \uXXXX encoding" — an error, never a misread instance.
//! * **Fail-closed.** A truncated or overlong body, a length past the end, trailing bytes,
//!   non-UTF-8 text or a count that does not match is an error, never a partial map.
//!
//! Older snapshots are read through [`crate::props`] exactly as before; the writer only writes
//! format 5.

/// The marker every format-5 snapshot starts with.
pub(crate) const MAGIC: &[u8] = b"\\uSNAPSHOT/5\n";

/// Whether `bytes` is a format-5 (binary) snapshot.
pub(crate) fn is_binary(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

/// Encode `entries` (keys need not be sorted; a repeated key keeps its last value, as the text
/// reader always did) into the format-5 container.
pub(crate) fn encode<'a>(entries: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<u8> {
    let mut sorted: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    for (k, v) in entries {
        sorted.insert(k, v);
    }
    let body: usize = sorted.iter().map(|(k, v)| 8 + k.len() + v.len()).sum();
    let mut out = Vec::with_capacity(MAGIC.len() + 4 + body);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&len_u32(sorted.len()).to_le_bytes());
    for (k, v) in sorted {
        out.extend_from_slice(&len_u32(k.len()).to_le_bytes());
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(&len_u32(v.len()).to_le_bytes());
        out.extend_from_slice(v.as_bytes());
    }
    out
}

fn len_u32(n: usize) -> u32 {
    u32::try_from(n).expect("a snapshot key or value longer than 4 GiB")
}

/// Decode a format-5 container into `(key, value)` pairs, in stored (ascending key) order.
pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<(String, String)>, String> {
    let mut rest = bytes
        .strip_prefix(MAGIC)
        .ok_or_else(|| "not a format-5 snapshot (missing marker)".to_owned())?;
    let count = take_u32(&mut rest, "entry count")? as usize;
    // Each entry needs at least its two length words: an absurd count fails here, before any
    // allocation is sized from it.
    if count > rest.len() / 8 {
        return Err(format!(
            "format-5 snapshot declares {count} entries but holds {} bytes",
            rest.len()
        ));
    }
    let mut pairs = Vec::with_capacity(count);
    for i in 0..count {
        let key = take_text(&mut rest, i, "key")?;
        let value = take_text(&mut rest, i, "value")?;
        pairs.push((key, value));
    }
    if !rest.is_empty() {
        return Err(format!(
            "format-5 snapshot has {} trailing bytes after its {count} entries",
            rest.len()
        ));
    }
    Ok(pairs)
}

fn take_u32(rest: &mut &[u8], what: &str) -> Result<u32, String> {
    if rest.len() < 4 {
        return Err(format!("format-5 snapshot truncated in its {what}"));
    }
    let (word, tail) = rest.split_at(4);
    *rest = tail;
    Ok(u32::from_le_bytes([word[0], word[1], word[2], word[3]]))
}

fn take_text(rest: &mut &[u8], entry: usize, what: &str) -> Result<String, String> {
    let len = take_u32(rest, what)? as usize;
    if rest.len() < len {
        return Err(format!(
            "format-5 snapshot entry {entry}: {what} length {len} runs past the end"
        ));
    }
    let (text, tail) = rest.split_at(len);
    *rest = tail;
    String::from_utf8(text.to_vec())
        .map_err(|e| format!("format-5 snapshot entry {entry}: {what} is not UTF-8: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_sorted_and_last_write_wins() {
        let bytes = encode([
            ("b", "2"),
            ("a", "1"),
            ("b", "3"),
            ("", ""),
            ("ü=:#\n\\", "\u{1F600} x"),
        ]);
        assert!(is_binary(&bytes));
        assert_eq!(
            decode(&bytes).unwrap(),
            vec![
                ("".to_owned(), "".to_owned()),
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), "3".to_owned()),
                ("ü=:#\n\\".to_owned(), "\u{1F600} x".to_owned()),
            ]
        );
        assert_eq!(
            bytes,
            encode([
                ("a", "1"),
                ("", ""),
                ("ü=:#\n\\", "\u{1F600} x"),
                ("b", "3")
            ])
        );
    }

    #[test]
    fn every_truncation_and_trailing_byte_is_an_error() {
        let bytes = encode([("sutra.status", "SUSPENDED"), ("sutra.var.x", "1")]);
        for cut in MAGIC.len()..bytes.len() {
            assert!(decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(decode(&longer).is_err());
        let mut bad_utf8 = bytes.clone();
        let last = bad_utf8.len() - 1;
        bad_utf8[last] = 0xFF;
        assert!(decode(&bad_utf8).is_err());
        let mut huge = MAGIC.to_vec();
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&huge).is_err());
    }

    /// Property: any map of arbitrary strings round-trips exactly (deterministic pseudo-random
    /// corpus — no extra test dependency).
    #[test]
    fn arbitrary_maps_round_trip() {
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet: Vec<char> = "ab=:#! \\\t\n\r\u{0}\u{7f}é€\u{1F600}sutra."
            .chars()
            .collect();
        for _ in 0..500 {
            let mut map = std::collections::BTreeMap::new();
            for _ in 0..(next() % 12) {
                let mut word = |max: u64| -> String {
                    let n = next() % max;
                    (0..n)
                        .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                        .collect()
                };
                let k = word(9);
                let v = word(40);
                map.insert(k, v);
            }
            let bytes = encode(map.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            let back: std::collections::BTreeMap<String, String> =
                decode(&bytes).unwrap().into_iter().collect();
            assert_eq!(back, map);
        }
    }

    /// A reader that only knows the Properties text format fails on the marker instead of
    /// misreading the instance — what an older engine does with a format-5 snapshot.
    #[test]
    fn the_text_reader_rejects_the_marker() {
        let bytes = encode([("sutra.status", "SUSPENDED")]);
        let err = crate::props::read_lines(&bytes).expect_err("text reader must fail");
        assert!(err.contains("malformed"), "{err}");
    }
}
