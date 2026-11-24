//! What a BPMN document's `<q:audit>` elements say, read straight from the XML rather than from
//! the loaded model — for the checks that must see what the model cannot: a declaration written
//! version-only, and the `sink` / `target` attributes the loader refuses (so a tool can report
//! and fix them before the document will load).

use std::collections::BTreeMap;

use crate::error::SutraError;
use crate::xml;

const BPMN_NS: &str = "http://www.omg.org/spec/BPMN/20100524/MODEL";
const Q_NS: &str = "urn:sutra:q:1.0";

/// The `<q:audit>` facts of one `<bpmn:process>`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessAuditSource {
    pub process_id: String,
    /// The attributes of the process-level declaration (the first process-level `<q:audit>`),
    /// or `None` when the process has none.
    pub declaration: Option<BTreeMap<String, String>>,
    /// Every `sink` / `target` attribute on a `<q:audit>` in this process, the process-level one
    /// included: `(host element id, attribute, value)`, in document order.
    pub removed_attributes: Vec<(String, String, String)>,
}

impl ProcessAuditSource {
    /// True when the process-level declaration carries only `version`: written as a version pin,
    /// it now also turns audit on.
    pub fn is_version_only(&self) -> bool {
        self.declaration
            .as_ref()
            .is_some_and(|attrs| !attrs.is_empty() && attrs.keys().all(|k| k == "version"))
    }
}

/// The audit facts of every process in `bpmn`, in document order.
pub fn scan_audit_sources(bpmn: &[u8]) -> Result<Vec<ProcessAuditSource>, SutraError> {
    let root = xml::parse(bpmn).map_err(|e| {
        SutraError::new(
            crate::codes::PARSE_BPMN_MISSING_PROCESS,
            format!("BPMN does not parse: {e}"),
        )
    })?;
    let mut out = Vec::new();
    for process in root.collect_descendants_ns(BPMN_NS, "process") {
        let process_id = process.attr_or_empty("id").to_string();
        let mut source = ProcessAuditSource {
            process_id: process_id.clone(),
            ..ProcessAuditSource::default()
        };
        for ext in process.children_ns(BPMN_NS, "extensionElements") {
            for audit in ext.children_ns(Q_NS, "audit") {
                let attrs: BTreeMap<String, String> = audit
                    .attrs
                    .iter()
                    .filter(|a| a.ns.is_none())
                    .map(|a| (a.local.clone(), a.value.clone()))
                    .collect();
                if source.declaration.is_none() {
                    source.declaration = Some(attrs);
                }
            }
        }
        let mut audits = Vec::new();
        process.descendants_ns(Q_NS, "audit", &mut audits);
        for audit in audits {
            for attr in ["sink", "target"] {
                if let Some(value) = audit.attr(None, attr) {
                    let host = host_id_of(process, audit).unwrap_or_else(|| process_id.clone());
                    source
                        .removed_attributes
                        .push((host, attr.to_string(), value.to_string()));
                }
            }
        }
        out.push(source);
    }
    Ok(out)
}

/// The id of the element whose `extensionElements` holds `audit`.
fn host_id_of(root: &xml::XmlElement, audit: &xml::XmlElement) -> Option<String> {
    fn walk(el: &xml::XmlElement, target: *const xml::XmlElement) -> Option<String> {
        for ext in el.children_ns(BPMN_NS, "extensionElements") {
            if ext.children.iter().any(|c| std::ptr::eq(c, target)) {
                return Some(el.attr_or_empty("id").to_string());
            }
        }
        el.children.iter().find_map(|c| walk(c, target))
    }
    walk(root, audit as *const _)
}

/// Where the `<q:audit>` elements of one process sit in the document's TEXT — what a
/// byte-preserving rewrite (`sutra lint --fix audit`) needs to add, complete or strip them.
/// Offsets are byte offsets into the document.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProcessAuditEdits {
    pub process_id: String,
    /// The prefix the process element is written with (`"bpmn"`, or `""` for the default
    /// namespace), so an inserted `extensionElements` matches it.
    pub bpmn_prefix: String,
    /// The whitespace that starts the process element's line.
    pub indent: String,
    /// Where a new first child of the process goes: after its start tag, or after its last
    /// leading `documentation` child.
    pub insert_child_at: usize,
    /// The process's direct `extensionElements`: the byte range of its start tag, and whether it
    /// is written self-closing (`<bpmn:extensionElements/>`).
    pub extension_elements: Option<(std::ops::Range<usize>, bool)>,
    /// The process-level `<q:audit>`: its start tag's range, whether self-closing, and the
    /// attribute names it carries.
    pub process_audit: Option<(std::ops::Range<usize>, bool, Vec<String>)>,
    /// Every `sink` / `target` attribute on a `<q:audit>` in this process: its byte range
    /// (leading whitespace included), its name, its value, and whether it is on the
    /// process-level `<q:audit>`.
    pub removed_attributes: Vec<(std::ops::Range<usize>, String, String, bool)>,
}

/// The `<q:audit>` edit sites of every process in `bpmn`, plus the prefix the document binds to
/// the `q:` namespace (`None` when it binds none).
pub fn audit_edit_sites(
    bpmn: &str,
) -> Result<(Vec<ProcessAuditEdits>, Option<String>), SutraError> {
    use quick_xml::events::Event;
    use quick_xml::name::ResolveResult;
    use quick_xml::reader::NsReader;

    let fail = |e: String| {
        SutraError::new(
            crate::codes::PARSE_BPMN_MISSING_PROCESS,
            format!("BPMN does not parse: {e}"),
        )
    };
    let q_prefix = regex::Regex::new(r#"xmlns:([A-Za-z_][\w.-]*)\s*=\s*["']urn:sutra:q:1\.0["']"#)
        .ok()
        .and_then(|re| re.captures(bpmn).map(|c| c[1].to_string()));
    let attr_re = regex::Regex::new(r#"\s+(sink|target)\s*=\s*("[^"]*"|'[^']*')"#)
        .map_err(|e| fail(e.to_string()))?;
    let name_re =
        regex::Regex::new(r#"\s([A-Za-z_][\w.:-]*)\s*="#).map_err(|e| fail(e.to_string()))?;

    // (namespace, local name) of each open element.
    let mut stack: Vec<(Option<String>, String)> = Vec::new();
    let mut out: Vec<ProcessAuditEdits> = Vec::new();
    // The depth of the open process element, and of its direct extensionElements.
    let mut process_depth: Option<usize> = None;
    let mut process_ext_depth: Option<usize> = None;
    let mut leading_children = true;

    let mut reader = NsReader::from_reader(bpmn.as_bytes());
    reader.config_mut().expand_empty_elements = false;
    loop {
        let start = reader.buffer_position() as usize;
        let event = reader.read_event().map_err(|e| fail(e.to_string()))?;
        let end = reader.buffer_position() as usize;
        match event {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let empty = matches!(event, Event::Empty(_));
                let ns = match reader.resolver().resolve_element(e.name()).0 {
                    ResolveResult::Bound(ns) => {
                        Some(String::from_utf8_lossy(ns.as_ref()).into_owned())
                    }
                    _ => None,
                };
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                let raw = &bpmn[start..end];
                let depth = stack.len();
                let is =
                    |want_ns: &str, want: &str| ns.as_deref() == Some(want_ns) && local == want;
                if is(BPMN_NS, "process") && process_depth.is_none() {
                    let qname = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                    let line_start = bpmn[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
                    let indent: String = bpmn[line_start..start]
                        .chars()
                        .take_while(|c| c.is_whitespace())
                        .collect();
                    let id = e
                        .try_get_attribute("id")
                        .ok()
                        .flatten()
                        .map(|a| String::from_utf8_lossy(&a.value).into_owned())
                        .unwrap_or_default();
                    out.push(ProcessAuditEdits {
                        process_id: id,
                        bpmn_prefix: qname
                            .split_once(':')
                            .map(|(p, _)| p.to_string())
                            .unwrap_or_default(),
                        indent,
                        insert_child_at: end,
                        ..ProcessAuditEdits::default()
                    });
                    leading_children = true;
                    if !empty {
                        process_depth = Some(depth);
                    }
                } else if let (Some(pd), Some(current)) = (process_depth, out.last_mut()) {
                    if depth == pd + 1 {
                        if is(BPMN_NS, "documentation") && leading_children {
                            // `insert_child_at` moves past it at its end tag.
                        } else {
                            leading_children = false;
                        }
                        if is(BPMN_NS, "extensionElements") && current.extension_elements.is_none()
                        {
                            current.extension_elements = Some((start..end, empty));
                            if !empty {
                                process_ext_depth = Some(depth);
                            }
                        }
                    }
                    if is(Q_NS, "audit") {
                        let process_level =
                            process_ext_depth == Some(depth.saturating_sub(1)) && depth == pd + 2;
                        if process_level && current.process_audit.is_none() {
                            let names = name_re
                                .captures_iter(raw)
                                .map(|c| c[1].to_string())
                                .filter(|n| !n.starts_with("xmlns"))
                                .collect();
                            current.process_audit = Some((start..end, empty, names));
                        }
                        for c in attr_re.captures_iter(raw) {
                            let m = c.get(0).expect("whole match");
                            let value = c[2][1..c[2].len() - 1].to_string();
                            current.removed_attributes.push((
                                start + m.start()..start + m.end(),
                                c[1].to_string(),
                                value,
                                process_level,
                            ));
                        }
                    }
                }
                if !empty {
                    stack.push((ns, local));
                }
            }
            Event::End(_) => {
                let closed = stack.pop();
                let depth = stack.len();
                if process_depth == Some(depth) {
                    process_depth = None;
                    process_ext_depth = None;
                } else if process_ext_depth == Some(depth) {
                    process_ext_depth = None;
                } else if let (Some(pd), Some(current)) = (process_depth, out.last_mut()) {
                    let is_doc = closed.as_ref().is_some_and(|(ns, l)| {
                        ns.as_deref() == Some(BPMN_NS) && l == "documentation"
                    });
                    if depth == pd + 1 && is_doc && leading_children {
                        current.insert_child_at = end;
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok((out, q_prefix))
}
