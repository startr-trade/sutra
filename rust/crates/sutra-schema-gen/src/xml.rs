//! A tiny namespace-aware DOM over `quick-xml`, retaining document order — the property
//! the emitter's byte-identical output depends on (struct order, field order, facet-pool
//! numbering all follow XSD document order).
//!
//! sutra-xsd's compiled `Schema` deliberately discards this (its type table is a
//! `BTreeMap`, alphabetically ordered, and its simple-type chains are flattened for the
//! streaming validator). Codegen needs the raw document tree, so the generator carries
//! this focused reader over the shared `quick-xml` dependency instead.

use quick_xml::events::Event;
use quick_xml::reader::Reader;

pub const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema";

/// One parsed element: its resolved namespace, local name, unprefixed attributes (in
/// document order) and child elements (in document order).
#[derive(Debug)]
pub struct Node {
    pub ns: String,
    pub local: String,
    pub attrs: Vec<(String, String)>,
    pub children: Vec<Node>,
    pub text: String,
}

impl Node {
    /// An attribute value by local name.
    pub fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// Direct children in the XSD namespace with the given local name, in order.
    pub fn xsd_children<'n>(&'n self, local: &'n str) -> impl Iterator<Item = &'n Node> {
        self.children
            .iter()
            .filter(move |c| c.ns == XSD_NS && c.local == local)
    }

    /// The first direct child in the XSD namespace with the given local name.
    pub fn first_xsd_child(&self, local: &str) -> Option<&Node> {
        self.children
            .iter()
            .find(|c| c.ns == XSD_NS && c.local == local)
    }

    /// Whether this node is the given XSD-namespace element.
    pub fn is_xsd(&self, local: &str) -> bool {
        self.ns == XSD_NS && self.local == local
    }
}

/// Prefix → namespace bindings, scoped by element depth.
struct Scope {
    bindings: Vec<(String, String)>,
    frames: Vec<usize>,
}

impl Scope {
    fn new() -> Scope {
        Scope {
            bindings: Vec::new(),
            frames: Vec::new(),
        }
    }

    fn resolve(&self, prefix: &str) -> String {
        self.bindings
            .iter()
            .rev()
            .find(|(p, _)| p == prefix)
            .map(|(_, uri)| uri.clone())
            .unwrap_or_default()
    }

    fn pop_frame(&mut self) {
        if let Some(count) = self.frames.pop() {
            for _ in 0..count {
                self.bindings.pop();
            }
        }
    }
}

/// Parse a schema/binding document into its root [`Node`]. Returns an error string on a
/// malformed document (well-formedness is the corpus's own guarantee; this is a tool).
pub fn parse(bytes: &[u8]) -> Result<Node, String> {
    let mut reader = Reader::from_reader(bytes);
    reader.config_mut().check_end_names = false;
    reader.config_mut().expand_empty_elements = false;

    let mut scope = Scope::new();
    let mut stack: Vec<Node> = Vec::new();
    let mut root: Option<Node> = None;
    let mut buf = Vec::new();

    loop {
        match reader
            .read_event_into(&mut buf)
            .map_err(|e| format!("xml parse error: {e}"))?
        {
            Event::Start(e) => {
                let node = open_element(&e, &mut scope)?;
                stack.push(node);
            }
            Event::Empty(e) => {
                let node = open_element(&e, &mut scope)?;
                scope.pop_frame();
                attach(node, &mut stack, &mut root);
            }
            Event::End(_) => {
                let node = stack.pop().ok_or("unbalanced end tag")?;
                scope.pop_frame();
                attach(node, &mut stack, &mut root);
            }
            Event::Text(t) => {
                if let Some(top) = stack.last_mut() {
                    let text = t.decode().map_err(|e| format!("text decode error: {e}"))?;
                    top.text.push_str(text.as_ref());
                }
            }
            Event::GeneralRef(r) => {
                // quick-xml 0.41 emits entity/char references as their own event instead
                // of expanding them inside `Text`; reassemble the same string.
                if let Some(top) = stack.last_mut() {
                    push_reference(&mut top.text, &r)
                        .map_err(|e| format!("text decode error: {e}"))?;
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    root.ok_or_else(|| "empty document".to_string())
}

/// Resolve one general reference (`&name;` or `&#nn;`) into `out`, reproducing the
/// quick-xml 0.37 text-unescape behaviour: only the five predefined entities and numeric
/// character references; any other entity is an error (no DTD entity expansion).
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

/// Build a node from a start/empty tag, pushing its namespace frame.
// `Attribute::unescape_value` is deprecated in quick-xml 0.41 in favour of
// `normalized_value` (which additionally collapses in-value whitespace); we keep the
// exact 0.37 entity-only semantics, so the deprecation is allowed deliberately.
#[allow(deprecated)]
fn open_element(e: &quick_xml::events::BytesStart<'_>, scope: &mut Scope) -> Result<Node, String> {
    let mut pushed = 0usize;
    let mut attrs: Vec<(String, String)> = Vec::new();

    for attr in e.attributes().with_checks(false) {
        let attr = attr.map_err(|err| format!("attribute error: {err}"))?;
        let key = attr.key.as_ref();
        let value = attr
            .unescape_value()
            .map_err(|err| format!("attribute decode error: {err}"))?
            .into_owned();
        if key == b"xmlns" {
            scope.bindings.push((String::new(), value));
            pushed += 1;
        } else if let Some(prefix) = key.strip_prefix(b"xmlns:") {
            scope
                .bindings
                .push((String::from_utf8_lossy(prefix).into_owned(), value));
            pushed += 1;
        } else {
            // Attributes key by local name; the corpus never collides local attribute
            // names across prefixes.
            attrs.push((local_of(key), value));
        }
    }
    scope.frames.push(pushed);

    let qname = e.name();
    let (prefix, local) = split_qname(qname.as_ref());
    let ns = scope.resolve(&prefix);

    Ok(Node {
        ns,
        local,
        attrs,
        children: Vec::new(),
        text: String::new(),
    })
}

fn attach(node: Node, stack: &mut [Node], root: &mut Option<Node>) {
    match stack.last_mut() {
        Some(parent) => parent.children.push(node),
        None => *root = Some(node),
    }
}

fn split_qname(raw: &[u8]) -> (String, String) {
    match raw.iter().position(|&b| b == b':') {
        Some(i) => (
            String::from_utf8_lossy(&raw[..i]).into_owned(),
            String::from_utf8_lossy(&raw[i + 1..]).into_owned(),
        ),
        None => (String::new(), String::from_utf8_lossy(raw).into_owned()),
    }
}

fn local_of(raw: &[u8]) -> String {
    match raw.iter().position(|&b| b == b':') {
        Some(i) => String::from_utf8_lossy(&raw[i + 1..]).into_owned(),
        None => String::from_utf8_lossy(raw).into_owned(),
    }
}
