//! The `xml` codec — XXE-hardened parse + the map projection into a
//! FEEL-walkable map.
//!
//! Security posture: quick-xml never resolves DTDs
//! or external entities (XXE-safe by construction, CWE-611), and a `<!DOCTYPE …>`
//! declaration is rejected outright — the load-bearing `disallow-doctype-decl` equivalent.
//!
//! Projection rules (the map projection, chosen for natural FEEL access —
//! `payload.Doc.Amt`, `payload.Items[0].Id`):
//! - a child element with nested elements → a nested map (recurse);
//! - a leaf element (text only) → its trimmed text string;
//! - repeated siblings of the same name → a list, in document order;
//! - attributes → keys prefixed `@`; a leaf that also has attributes becomes a small map
//!   of its `@attrs` plus a `#text` entry (omitted when blank);
//! - element/attribute names use the LOCAL name (namespace prefix dropped); `xmlns`
//!   declarations and attributes in the XML Schema-instance namespace (`schemaLocation`,
//!   `type`, `nil`) are structure, not data — skipped. The schema-instance test matches the
//!   RESOLVED NAMESPACE, never the `xsi` prefix, which is only a convention: a conformant
//!   sender may bind that namespace to any prefix, and the XSD validator already ignores it
//!   by namespace (`sutra_xsd`), so the projection must agree or the two disagree about what
//!   counts as data.
//!
//! The decode payload is the projection of the document element (its children +
//! attributes — the same shape the XSD validate-and-project step yields), and the
//! document element's local name is stamped as [`DecodeResult::message_type`].

use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::name::{QName, ResolveResult};
use quick_xml::reader::NsReader;
use quick_xml::writer::Writer;

use sutra_codec_spi::codec::{sanitize, PayloadCodec};
use sutra_codec_spi::codes;
use sutra_codec_spi::issue::ValidationIssue;
use sutra_codec_spi::result::{CodecValue, DecodeResult};

pub struct XmlCodec;

// Self-registers as a zero-config global built-in (inventory pull model).
// xml is a nested-map format — interchangeable with json/yaml by content-type (BuiltinFormat only).
inventory::submit! {
    sutra_codec_spi::BuiltinFormat {
        name: "xml",
        shape_class: sutra_codec_spi::ShapeClass::NestedMap,
        make: || std::sync::Arc::new(XmlCodec),
    }
}

impl PayloadCodec for XmlCodec {
    fn name(&self) -> &str {
        "xml"
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec![
            "application/xml".to_string(),
            "text/xml".to_string(),
            "application/*+xml".to_string(),
        ]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let ct = content_type.unwrap_or("application/xml");
        match parse_element(body) {
            Ok(root) => {
                let message_type = root.local.clone();
                DecodeResult::ok(CodecValue::Json(project(&root)), ct)
                    .with_message_type(&message_type)
            }
            Err(e) => DecodeResult::fatal(
                vec![ValidationIssue::error(
                    codes::PARSE_XML_PARSE_ERROR,
                    "",
                    format!("XML parse failed: {}", sanitize(&e)),
                )],
                ct,
            ),
        }
    }

    /// Encode a single-rooted map back to XML — the `TreeToXml` counterpart. The payload
    /// must be a JSON object with exactly ONE top-level key (the root element name, the
    /// same shape a decode + `message_type` re-wrap yields); `@`-prefixed keys become
    /// attributes, `#text` becomes text content, lists repeat the element.
    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let CodecValue::Json(serde_json::Value::Object(map)) = payload else {
            return Err("xml encode requires a single-rooted map payload".to_string());
        };
        if map.len() != 1 {
            return Err(format!(
                "xml encode requires exactly one root element key, got {}",
                map.len()
            ));
        }
        let (root_name, root_value) = map.iter().next().expect("len checked");
        let mut writer = Writer::new(Vec::new());
        writer
            .write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
            .map_err(|e| sanitize(&e.to_string()))?;
        write_element(&mut writer, root_name, root_value)?;
        Ok(writer.into_inner())
    }
}

// ---- parse (mini-tree; local names only) ---------------------------------------------

struct Element {
    local: String,
    /// `(local name, value)` — namespace declarations already skipped.
    attrs: Vec<(String, String)>,
    children: Vec<Element>,
    text: String,
}

/// The XML Schema-instance namespace. Attributes bound to it (`schemaLocation`, `type`, `nil`)
/// are schema-instance METADATA: the XSD validator accepts and ignores them, so the projection
/// must not surface them as data.
const XSI_NS: &[u8] = b"http://www.w3.org/2001/XMLSchema-instance";

/// Is this attribute STRUCTURE rather than data — a namespace declaration, or an attribute in the
/// XML Schema-instance namespace?
///
/// The schema-instance test is by RESOLVED NAMESPACE, not by prefix: `xsi` is a convention, and
/// `xmlns:t="http://www.w3.org/2001/XMLSchema-instance" t:schemaLocation="…"` is the very same
/// attribute. Unprefixed attributes are in no namespace (they never inherit the default `xmlns`),
/// which `resolve_attribute` already reflects, so ordinary data attributes are untouched.
fn is_structure(reader: &NsReader<&[u8]>, key: QName<'_>) -> bool {
    let raw = key.as_ref();
    if raw == b"xmlns" || raw.starts_with(b"xmlns:") {
        return true;
    }
    matches!(
        reader.resolver().resolve_attribute(key),
        (ResolveResult::Bound(ns), _) if ns.as_ref() == XSI_NS
    )
}

fn local_of(name: &[u8]) -> String {
    let s = String::from_utf8_lossy(name);
    match s.rsplit_once(':') {
        Some((_, local)) => local.to_string(),
        None => s.into_owned(),
    }
}

// `Attribute::unescape_value` is deprecated in quick-xml 0.41 in favour of
// `normalized_value`, but that additionally collapses in-value whitespace (tab/CR/LF →
// space) per XML attribute-value normalization — a behaviour change. We keep the exact
// 0.37 semantics (entity unescaping only), so the deprecation is allowed deliberately.
#[allow(deprecated)]
fn parse_element(bytes: &[u8]) -> Result<Element, String> {
    let mut reader = NsReader::from_reader(bytes);
    reader.config_mut().expand_empty_elements = true;

    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;

    loop {
        match reader.read_event().map_err(|e| e.to_string())? {
            Event::Start(e) => {
                let mut attrs = Vec::new();
                for a in e.attributes() {
                    let a = a.map_err(|err| err.to_string())?;
                    if is_structure(&reader, a.key) {
                        continue;
                    }
                    attrs.push((
                        local_of(a.key.as_ref()),
                        a.unescape_value()
                            .map_err(|err| err.to_string())?
                            .into_owned(),
                    ));
                }
                stack.push(Element {
                    local: local_of(e.name().as_ref()),
                    attrs,
                    children: Vec::new(),
                    text: String::new(),
                });
            }
            Event::End(_) => {
                let el = stack.pop().ok_or("unbalanced end tag")?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(el),
                    None => {
                        if root.is_some() {
                            return Err("multiple document elements".to_string());
                        }
                        root = Some(el);
                    }
                }
            }
            Event::Text(t) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| e.to_string())?);
                }
            }
            Event::GeneralRef(r) => {
                if let Some(top) = stack.last_mut() {
                    push_reference(&mut top.text, &r)?;
                }
            }
            Event::CData(c) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&String::from_utf8_lossy(c.as_ref()));
                }
            }
            Event::DocType(_) => {
                // The load-bearing XXE guard — mirror of `disallow-doctype-decl`.
                return Err("DOCTYPE is not allowed".to_string());
            }
            Event::Eof => break,
            _ => {} // declaration, comments, processing instructions
        }
    }

    if !stack.is_empty() {
        return Err("premature end of document (unclosed element)".to_string());
    }
    root.ok_or_else(|| "no document element".to_string())
}

/// Resolve one general reference (`&name;` or `&#nn;`) into `out`, reproducing the
/// quick-xml 0.37 text-unescape behaviour: only the five predefined entities and numeric
/// character references; any other (DTD-defined or unknown) entity is an error — the
/// XXE-safe posture. quick-xml 0.41 surfaces references as their own `Event::GeneralRef`
/// rather than expanding them inside `Event::Text`, so text is reassembled here.
fn push_reference(out: &mut String, r: &quick_xml::events::BytesRef<'_>) -> Result<(), String> {
    if let Some(ch) = r.resolve_char_ref().map_err(|e| e.to_string())? {
        out.push(ch);
    } else {
        let name = r.decode().map_err(|e| e.to_string())?;
        match quick_xml::escape::resolve_predefined_entity(&name) {
            Some(rep) => out.push_str(rep),
            None => return Err(format!("unknown entity reference '&{name};'")),
        }
    }
    Ok(())
}

// ---- project (XmlToMap conventions) ----------------------------------------------------

/// Project an element's children (and attributes) into a map — `XmlToMap.toMap`.
fn project(el: &Element) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    put_attributes(el, &mut map);
    for child in &el.children {
        merge(&mut map, &child.local, value_of(child));
    }
    serde_json::Value::Object(map)
}

/// `XmlToMap.valueOf`: nested elements → map; plain leaf → trimmed text; leaf with
/// attributes → `{@attrs…, #text}` (text omitted when blank).
fn value_of(el: &Element) -> serde_json::Value {
    if !el.children.is_empty() {
        return project(el);
    }
    let text = el.text.trim();
    if el.attrs.is_empty() {
        return serde_json::Value::String(text.to_string());
    }
    let mut leaf = serde_json::Map::new();
    put_attributes(el, &mut leaf);
    if !text.is_empty() {
        leaf.insert(
            "#text".to_string(),
            serde_json::Value::String(text.to_string()),
        );
    }
    serde_json::Value::Object(leaf)
}

fn put_attributes(el: &Element, into: &mut serde_json::Map<String, serde_json::Value>) {
    for (local, value) in &el.attrs {
        into.insert(
            format!("@{local}"),
            serde_json::Value::String(value.clone()),
        );
    }
}

/// `XmlToMap.merge`: repeated siblings of one name collect into a list, in document order.
fn merge(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: serde_json::Value,
) {
    match map.get_mut(key) {
        None => {
            map.insert(key.to_string(), value);
        }
        Some(serde_json::Value::Array(list)) => list.push(value),
        Some(existing) => {
            let first = existing.take();
            *existing = serde_json::Value::Array(vec![first, value]);
        }
    }
}

// ---- encode (TreeToXml counterpart) -----------------------------------------------------

fn write_element(
    writer: &mut Writer<Vec<u8>>,
    name: &str,
    value: &serde_json::Value,
) -> Result<(), String> {
    let mut start = BytesStart::new(name);
    let mut text: Option<String> = None;
    let mut children: Vec<(&str, &serde_json::Value)> = Vec::new();

    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                if let Some(attr) = k.strip_prefix('@') {
                    start.push_attribute((attr, scalar_text(v).as_str()));
                } else if k == "#text" {
                    text = Some(scalar_text(v));
                } else {
                    children.push((k.as_str(), v));
                }
            }
        }
        other => text = Some(scalar_text(other)),
    }

    writer
        .write_event(Event::Start(start))
        .map_err(|e| sanitize(&e.to_string()))?;
    if let Some(t) = &text {
        writer
            .write_event(Event::Text(BytesText::new(t)))
            .map_err(|e| sanitize(&e.to_string()))?;
    }
    for (k, v) in children {
        if let serde_json::Value::Array(items) = v {
            for item in items {
                write_element(writer, k, item)?;
            }
        } else {
            write_element(writer, k, v)?;
        }
    }
    writer
        .write_event(Event::End(BytesEnd::new(name)))
        .map_err(|e| sanitize(&e.to_string()))
}

fn scalar_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}
