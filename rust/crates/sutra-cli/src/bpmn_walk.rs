//! Shared streaming-XML plumbing for the read-only BPMN inspectors (`describe`,
//! `dispatch-graph`, `compat-baseline`, `simulate --dry-run`). Deliberately NOT the
//! engine's BPMN loader: inspectors report file structure and must not couple to the
//! deploy-time validation rules.

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

/// Simplified event stream: element open (with attributes), self-closing element,
/// element close (local name only).
pub(crate) enum WalkEvent<'a, 'b> {
    Start(&'a BytesStart<'b>),
    Empty(&'a BytesStart<'b>),
    End(String),
}

/// Streams `xml` through `on_event`. Errors on malformed XML, including unclosed
/// elements at end-of-input (the underlying reader is lenient about those on its own).
pub(crate) fn walk_bpmn(
    xml: &str,
    mut on_event: impl FnMut(WalkEvent<'_, '_>),
) -> Result<(), String> {
    let mut reader = Reader::from_str(xml);
    let mut depth: u64 = 0;
    loop {
        match reader.read_event().map_err(|e| e.to_string())? {
            Event::Start(e) => {
                depth += 1;
                on_event(WalkEvent::Start(&e));
            }
            Event::Empty(e) => on_event(WalkEvent::Empty(&e)),
            Event::End(e) => {
                depth = depth.saturating_sub(1);
                let name = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                on_event(WalkEvent::End(name));
            }
            Event::Eof => {
                if depth > 0 {
                    return Err(format!(
                        "unexpected end of file: {depth} unclosed element(s)"
                    ));
                }
                return Ok(());
            }
            _ => {}
        }
    }
}

/// Local element name (prefix stripped).
pub(crate) fn local_name(e: &BytesStart<'_>) -> String {
    String::from_utf8_lossy(e.local_name().as_ref()).into_owned()
}

/// First attribute whose LOCAL name matches, regardless of prefix — captures both plain
/// BPMN attributes and `q:`-prefixed extension attributes.
pub(crate) fn attr(e: &BytesStart<'_>, name: &str) -> Option<String> {
    for a in e.attributes().with_checks(false).flatten() {
        if a.key.local_name().as_ref() == name.as_bytes() {
            return Some(String::from_utf8_lossy(&a.value).into_owned());
        }
    }
    None
}
