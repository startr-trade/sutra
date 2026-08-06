//! Namespace-aware mini-DOM over quick-xml — the stand-in for the hardened XML load in the
//! BPMN loader.
//!
//! Security posture: quick-xml never resolves external entities or DTDs (XXE-safe by
//! construction), and a `<!DOCTYPE …>` declaration is rejected outright.
//!
//! In addition to the DMN tree, each element carries its **in-scope namespace bindings**
//! (prefix → URI, cumulative) so QName-valued attributes
//! (`calledElement="payments:processBatchPayment"`) resolve exactly per `xs:QName` semantics.

use std::collections::BTreeMap;

use quick_xml::events::Event;
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

#[derive(Debug, Clone)]
pub(crate) struct XmlAttr {
    /// Resolved attribute namespace URI (`None` for unprefixed attributes, like
    /// `Element.getAttributeNS(null, …)`).
    pub ns: Option<String>,
    pub local: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub(crate) struct XmlElement {
    /// Resolved element namespace URI (default namespace applies).
    pub ns: Option<String>,
    pub local: String,
    pub attrs: Vec<XmlAttr>,
    pub children: Vec<XmlElement>,
    /// Concatenated direct text content (unescaped).
    pub text: String,
    /// In-scope namespace bindings at this element (prefix → URI; `""` = default ns).
    pub bindings: BTreeMap<String, String>,
}

impl XmlElement {
    /// Attribute lookup by (namespace, local name). `ns: None` matches unprefixed attributes.
    pub fn attr(&self, ns: Option<&str>, local: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|a| a.ns.as_deref() == ns && a.local == local)
            .map(|a| a.value.as_str())
    }

    /// Unprefixed attribute, mirroring `Element.getAttribute` (returns `""` when absent).
    pub fn attr_or_empty(&self, local: &str) -> &str {
        self.attr(None, local).unwrap_or("")
    }

    /// True when this element has the given BPMN-namespace identity.
    pub fn is(&self, ns: &str, local: &str) -> bool {
        self.ns.as_deref() == Some(ns) && self.local == local
    }

    /// Direct children with the given namespace + local name, in document order.
    pub fn children_ns<'a, 'b>(
        &'a self,
        ns: &'b str,
        local: &'b str,
    ) -> impl Iterator<Item = &'a XmlElement> + use<'a, 'b> {
        self.children.iter().filter(move |c| c.is(ns, local))
    }

    /// First direct child with the given namespace + local name.
    pub fn child_ns(&self, ns: &str, local: &str) -> Option<&XmlElement> {
        self.children_ns(ns, local).next()
    }

    /// All descendant elements (self excluded) with the given namespace + local name, in
    /// document order — mirror of `getElementsByTagNameNS`.
    pub fn descendants_ns<'a>(&'a self, ns: &str, local: &str, out: &mut Vec<&'a XmlElement>) {
        for c in &self.children {
            if c.is(ns, local) {
                out.push(c);
            }
            c.descendants_ns(ns, local, out);
        }
    }

    /// Convenience wrapper over [`Self::descendants_ns`] returning the collected vector.
    pub fn collect_descendants_ns(&self, ns: &str, local: &str) -> Vec<&XmlElement> {
        let mut out = Vec::new();
        self.descendants_ns(ns, local, &mut out);
        out
    }

    /// Resolve a QName-valued attribute string against this element's in-scope bindings —
    /// `(namespace URI, local part)`. An unprefixed QName resolves against the default
    /// namespace (`xs:QName` semantics).
    pub fn resolve_qname(&self, value: &str) -> (Option<String>, String) {
        match value.split_once(':') {
            Some((prefix, local)) => (self.bindings.get(prefix).cloned(), local.to_string()),
            None => (self.bindings.get("").cloned(), value.to_string()),
        }
    }

    pub fn trimmed_text(&self) -> &str {
        self.text.trim()
    }
}

// `Attribute::unescape_value` is deprecated in quick-xml 0.41 in favour of
// `normalized_value`, but that additionally collapses in-value whitespace (tab/CR/LF →
// space) — a behaviour change. We keep the exact 0.37 entity-only semantics, so the
// deprecation is allowed deliberately.
#[allow(deprecated)]
pub(crate) fn parse(bytes: &[u8]) -> Result<XmlElement, String> {
    let mut reader = NsReader::from_reader(bytes);
    reader.config_mut().expand_empty_elements = true;

    let mut stack: Vec<XmlElement> = Vec::new();
    let mut root: Option<XmlElement> = None;

    loop {
        match reader.read_resolved_event().map_err(|e| e.to_string())? {
            (res, Event::Start(e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                let ns = bound_ns(&res);
                let mut bindings = stack.last().map(|p| p.bindings.clone()).unwrap_or_default();
                let mut attrs = Vec::new();
                for a in e.attributes() {
                    let a = a.map_err(|err| err.to_string())?;
                    let key = a.key;
                    // Namespace declarations (xmlns / xmlns:*) are bindings, not data
                    // attributes — record them for QName resolution and skip as attrs.
                    if key.as_ref() == b"xmlns" || key.as_ref().starts_with(b"xmlns:") {
                        let prefix = if key.as_ref() == b"xmlns" {
                            String::new()
                        } else {
                            String::from_utf8_lossy(&key.as_ref()[b"xmlns:".len()..]).into_owned()
                        };
                        bindings.insert(
                            prefix,
                            a.unescape_value()
                                .map_err(|err| err.to_string())?
                                .into_owned(),
                        );
                        continue;
                    }
                    let (ares, alocal) = reader.resolver().resolve_attribute(key);
                    attrs.push(XmlAttr {
                        ns: bound_ns(&ares),
                        local: String::from_utf8_lossy(alocal.as_ref()).into_owned(),
                        value: a
                            .unescape_value()
                            .map_err(|err| err.to_string())?
                            .into_owned(),
                    });
                }
                stack.push(XmlElement {
                    ns,
                    local,
                    attrs,
                    children: Vec::new(),
                    text: String::new(),
                    bindings,
                });
            }
            (_, Event::End(_)) => {
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
            (_, Event::Text(t)) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&t.decode().map_err(|e| e.to_string())?);
                }
            }
            (_, Event::GeneralRef(r)) => {
                if let Some(top) = stack.last_mut() {
                    push_reference(&mut top.text, &r)?;
                }
            }
            (_, Event::CData(c)) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&String::from_utf8_lossy(c.as_ref()));
                }
            }
            (_, Event::DocType(_)) => {
                // The hardened loader disallows DOCTYPE declarations.
                return Err("DOCTYPE is not allowed".to_string());
            }
            (_, Event::Eof) => break,
            _ => {} // XML declaration, comments, processing instructions
        }
    }

    if !stack.is_empty() {
        return Err("premature end of document (unclosed element)".to_string());
    }
    root.ok_or_else(|| "no document element".to_string())
}

fn bound_ns(res: &ResolveResult) -> Option<String> {
    match res {
        ResolveResult::Bound(ns) => Some(String::from_utf8_lossy(ns.as_ref()).into_owned()),
        _ => None,
    }
}

/// Resolve one general reference (`&name;` or `&#nn;`) into `out`, reproducing the
/// quick-xml 0.37 text-unescape behaviour: only the five predefined entities and numeric
/// character references; any other (DTD-defined or unknown) entity is an error — the
/// XXE-safe posture. quick-xml 0.41 surfaces references as their own `Event::GeneralRef`
/// rather than expanding them inside `Event::Text`.
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
