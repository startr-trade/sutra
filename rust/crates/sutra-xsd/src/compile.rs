//! The schema compiler: parses one self-contained XSD document into the compiled
//! [`Schema`] model, rejecting everything outside the supported Tier-1 subset with a
//! collected "not in the supported subset" finding per occurrence (the module-codec
//! authoring contract).
//!
//! Supported Tier-1 profile: single-file schemas with
//! `targetNamespace` + `elementFormDefault="qualified"`; named/inline `complexType`
//! with `sequence`/`choice` particles and occurrence bounds; `simpleType` restriction
//! chains onto the supported builtins with the nine facet kinds; `simpleContent` +
//! `extension` with attributes; `xs:any` with `processContents="lax"|"skip"`.
//! Everything else — import/include, redefine/override, group/attributeGroup, `xs:all`,
//! list/union, identity constraints, notation, nillable, fixed/default, block/final/
//! form, substitution groups, abstract, element ref, complex-content derivation, mixed
//! content, anyAttribute, the excluded facets, XSD 1.1 — is rejected at compile time.

use std::collections::BTreeMap;

use bigdecimal::BigDecimal;
use quick_xml::events::{BytesStart, Event};
use quick_xml::reader::Reader;
use regex::Regex;

use crate::datatype::Builtin;
use crate::diag::{CompileError, CompileFinding, SourceMap};
use crate::facet::FacetStep;
use crate::model::{
    AttrDecl, ComplexDef, Content, ElementDecl, GroupDef, GroupKind, Max, Occurs, Particle, Schema,
    SimpleDef, TypeDef, TypeRef,
};

const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema";

impl Schema {
    /// Compile a self-contained schema document. All subset findings are collected
    /// before failing, so one compile reports the full authoring-contract delta.
    pub fn compile(xsd: &[u8]) -> Result<Schema, CompileError> {
        let source_map = SourceMap::new(xsd);
        let mut findings: Vec<Finding> = Vec::new();
        let root = match parse_tree(xsd) {
            Ok(root) => root,
            Err((pos, message)) => {
                return Err(CompileError {
                    findings: vec![CompileFinding {
                        pos: source_map.pos(pos),
                        message,
                    }],
                })
            }
        };
        let schema = build_schema(&root, &mut findings);
        if findings.is_empty() {
            Ok(schema)
        } else {
            Err(CompileError {
                findings: findings
                    .into_iter()
                    .map(|f| CompileFinding {
                        pos: source_map.pos(f.0),
                        message: f.1,
                    })
                    .collect(),
            })
        }
    }
}

/// (byte offset, message) — mapped to line:col at the end.
struct Finding(usize, String);

// ---------------------------------------------------------------------------
// Tree parse
// ---------------------------------------------------------------------------

/// One XSD-namespace schema-document element. `type`/`base` attribute values are
/// QNames; their prefixes resolve against the in-scope bindings at parse time into
/// [`Node::q_refs`].
struct Node {
    local: String,
    /// Unqualified attributes, in document order.
    attrs: Vec<(String, String)>,
    /// Prefixed (non-xmlns) attributes — always findings downstream.
    foreign_attrs: Vec<String>,
    /// Resolved QName-valued attributes (`type`, `base`): name → (namespace, local).
    q_refs: Vec<(String, (String, String))>,
    children: Vec<Node>,
    /// Byte offset of the start tag's `<`.
    pos: usize,
}

impl Node {
    fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    fn q_ref(&self, name: &str) -> Option<&(String, String)> {
        self.q_refs.iter().find(|(n, _)| n == name).map(|(_, q)| q)
    }

    fn children_named<'n>(&'n self, local: &'n str) -> impl Iterator<Item = &'n Node> {
        self.children.iter().filter(move |c| c.local == local)
    }
}

/// Prefix → namespace bindings, scoped by element depth.
struct NamespaceScope {
    /// (prefix — empty string is the default namespace, uri); later entries shadow.
    bindings: Vec<(String, String)>,
    /// How many bindings each open element added.
    frames: Vec<usize>,
}

impl NamespaceScope {
    fn new() -> NamespaceScope {
        NamespaceScope {
            bindings: Vec::new(),
            frames: Vec::new(),
        }
    }

    fn push(&mut self, added: Vec<(String, String)>) {
        self.frames.push(added.len());
        self.bindings.extend(added);
    }

    fn pop(&mut self) {
        if let Some(n) = self.frames.pop() {
            self.bindings.truncate(self.bindings.len() - n);
        }
    }

    fn resolve(&self, prefix: &str) -> Option<&str> {
        self.bindings
            .iter()
            .rev()
            .find(|(p, _)| p == prefix)
            .map(|(_, uri)| uri.as_str())
    }
}

type TreeError = (usize, String);

fn parse_tree(xsd: &[u8]) -> Result<Node, TreeError> {
    let mut reader = Reader::from_reader(xsd);
    reader.config_mut().expand_empty_elements = true;
    let mut scope = NamespaceScope::new();
    let mut stack: Vec<Node> = Vec::new();
    let mut skip_depth: usize = 0;
    let mut root: Option<Node> = None;

    loop {
        let tag_start = reader.buffer_position() as usize;
        match reader.read_event() {
            Err(e) => {
                return Err((
                    reader.error_position() as usize,
                    format!("schema is not well-formed XML: {e}"),
                ))
            }
            Ok(Event::Start(e)) => {
                let added = namespace_bindings(&e)?;
                scope.push(added);
                if skip_depth > 0 {
                    skip_depth += 1;
                    continue;
                }
                let (prefix, local) = split_name(e.name().as_ref());
                let ns = scope.resolve(&prefix).unwrap_or("");
                if ns != XSD_NS {
                    return Err((
                        tag_start,
                        format!("element '{local}' is not in the XML Schema namespace"),
                    ));
                }
                if local == "annotation" {
                    // Documentation subtree — no structural meaning; skip wholesale.
                    skip_depth = 1;
                    continue;
                }
                let node = read_node(&e, local, tag_start, &scope)?;
                stack.push(node);
            }
            Ok(Event::End(_)) => {
                scope.pop();
                if skip_depth > 0 {
                    skip_depth -= 1;
                    continue;
                }
                let node = stack
                    .pop()
                    .ok_or_else(|| (tag_start, "unbalanced end tag".to_string()))?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(node),
                    None => {
                        if root.is_some() {
                            return Err((tag_start, "multiple root elements".to_string()));
                        }
                        root = Some(node);
                    }
                }
            }
            Ok(Event::DocType(_)) => {
                return Err((tag_start, "DOCTYPE is not allowed in a schema".to_string()))
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
        }
    }
    match root {
        Some(node) if node.local == "schema" => Ok(node),
        Some(node) => Err((
            node.pos,
            format!(
                "document element is 'xs:{}', expected 'xs:schema'",
                node.local
            ),
        )),
        None => Err((0, "schema document is empty".to_string())),
    }
}

// `Attribute::unescape_value` is deprecated in quick-xml 0.41 in favour of
// `normalized_value` (which additionally collapses in-value whitespace); we keep the
// exact 0.37 entity-only semantics, so the deprecation is allowed deliberately.
#[allow(deprecated)]
fn namespace_bindings(e: &BytesStart<'_>) -> Result<Vec<(String, String)>, TreeError> {
    let mut added = Vec::new();
    for attr in e.attributes() {
        let attr = attr.map_err(|err| (0usize, format!("bad attribute: {err}")))?;
        let key = String::from_utf8_lossy(attr.key.as_ref()).into_owned();
        if key == "xmlns" || key.starts_with("xmlns:") {
            let prefix = key.strip_prefix("xmlns:").unwrap_or("").to_string();
            let uri = attr
                .unescape_value()
                .map_err(|err| (0usize, format!("bad attribute value: {err}")))?
                .into_owned();
            added.push((prefix, uri));
        }
    }
    Ok(added)
}

#[allow(deprecated)] // `unescape_value`: exact 0.37 entity-only semantics (see above)
fn read_node(
    e: &BytesStart<'_>,
    local: String,
    pos: usize,
    scope: &NamespaceScope,
) -> Result<Node, TreeError> {
    let mut node = Node {
        local,
        attrs: Vec::new(),
        foreign_attrs: Vec::new(),
        q_refs: Vec::new(),
        children: Vec::new(),
        pos,
    };
    for attr in e.attributes() {
        let attr = attr.map_err(|err| (pos, format!("bad attribute: {err}")))?;
        let key = String::from_utf8_lossy(attr.key.as_ref()).into_owned();
        if key == "xmlns" || key.starts_with("xmlns:") {
            continue;
        }
        let value = attr
            .unescape_value()
            .map_err(|err| (pos, format!("bad attribute value: {err}")))?
            .into_owned();
        if key.contains(':') {
            node.foreign_attrs.push(key);
            continue;
        }
        if key == "type" || key == "base" {
            let (prefix, ref_local) = match value.split_once(':') {
                Some((p, l)) => (p.to_string(), l.to_string()),
                None => (String::new(), value.clone()),
            };
            let ns = scope.resolve(&prefix).unwrap_or("").to_string();
            node.q_refs.push((key.clone(), (ns, ref_local)));
        }
        node.attrs.push((key, value));
    }
    Ok(node)
}

fn split_name(name: &[u8]) -> (String, String) {
    let name = String::from_utf8_lossy(name);
    match name.split_once(':') {
        Some((p, l)) => (p.to_string(), l.to_string()),
        None => (String::new(), name.into_owned()),
    }
}

// ---------------------------------------------------------------------------
// Model build
// ---------------------------------------------------------------------------

struct Builder<'t> {
    target_namespace: String,
    /// Named top-level simpleType/complexType nodes.
    named_simple: BTreeMap<&'t str, &'t Node>,
    named_complex: BTreeMap<&'t str, &'t Node>,
    findings: Vec<Finding>,
}

fn build_schema(root: &Node, findings: &mut Vec<Finding>) -> Schema {
    let mut builder = Builder {
        target_namespace: String::new(),
        named_simple: BTreeMap::new(),
        named_complex: BTreeMap::new(),
        findings: Vec::new(),
    };
    builder.schema_attrs(root);
    builder.index_named_types(root);

    let mut roots = BTreeMap::new();
    let mut types = BTreeMap::new();

    for child in &root.children {
        match child.local.as_str() {
            "element" => {
                if let Some(decl) = builder.element_decl(child, false) {
                    roots.insert(decl.name.clone(), decl);
                }
            }
            "simpleType" | "complexType" => {}
            other => builder.reject(child.pos, &format!("xs:{other}")),
        }
    }
    // Named types compile after the index is complete so forward references resolve.
    let simple_names: Vec<&str> = builder.named_simple.keys().copied().collect();
    for name in simple_names {
        let node = builder.named_simple[name];
        let def = builder.simple_type(node, &mut Vec::new());
        types.insert(name.to_string(), TypeDef::Simple(def));
    }
    let complex_names: Vec<&str> = builder.named_complex.keys().copied().collect();
    for name in complex_names {
        let node = builder.named_complex[name];
        let def = builder.complex_type(node);
        types.insert(name.to_string(), TypeDef::Complex(def));
    }

    findings.append(&mut builder.findings);
    Schema {
        target_namespace: builder.target_namespace,
        roots,
        types,
    }
}

impl<'t> Builder<'t> {
    fn finding(&mut self, pos: usize, message: impl Into<String>) {
        self.findings.push(Finding(pos, message.into()));
    }

    fn reject(&mut self, pos: usize, construct: &str) {
        self.finding(pos, format!("{construct} is not in the supported subset"));
    }

    /// Complain about any attribute not in `allowed` (plus every prefixed attribute).
    fn check_attrs(&mut self, node: &Node, allowed: &[&str]) {
        for (name, _) in &node.attrs {
            if !allowed.contains(&name.as_str()) {
                self.reject(
                    node.pos,
                    &format!("attribute '{name}' on xs:{}", node.local),
                );
            }
        }
        for name in &node.foreign_attrs {
            self.reject(
                node.pos,
                &format!("attribute '{name}' on xs:{}", node.local),
            );
        }
    }

    fn schema_attrs(&mut self, root: &Node) {
        self.check_attrs(
            root,
            &[
                "targetNamespace",
                "elementFormDefault",
                "attributeFormDefault",
            ],
        );
        match root.attr("targetNamespace") {
            Some(ns) if !ns.is_empty() => self.target_namespace = ns.to_string(),
            _ => self.finding(
                root.pos,
                "a schema without targetNamespace is not in the supported subset",
            ),
        }
        if root.attr("elementFormDefault") != Some("qualified") {
            self.finding(
                root.pos,
                "elementFormDefault=\"qualified\" is required (the supported subset)",
            );
        }
        if let Some(form) = root.attr("attributeFormDefault") {
            if form != "unqualified" {
                self.finding(
                    root.pos,
                    "attributeFormDefault=\"qualified\" is not in the supported subset",
                );
            }
        }
    }

    fn index_named_types(&mut self, root: &'t Node) {
        for child in &root.children {
            let name = child.attr("name");
            match (child.local.as_str(), name) {
                ("simpleType", Some(name)) => {
                    self.named_simple.insert(name, child);
                }
                ("complexType", Some(name)) => {
                    self.named_complex.insert(name, child);
                }
                ("simpleType" | "complexType", None) => {
                    self.finding(
                        child.pos,
                        format!("top-level xs:{} without a name", child.local),
                    );
                }
                _ => {}
            }
        }
    }

    // ---- elements ---------------------------------------------------------

    fn element_decl(&mut self, node: &Node, local: bool) -> Option<ElementDecl> {
        let mut allowed = vec!["name", "type"];
        if local {
            allowed.extend(["minOccurs", "maxOccurs"]);
        }
        // Name the specifically-excluded attributes in the finding (the authoring
        // contract), then catch the remainder generically.
        for excluded in [
            "ref",
            "substitutionGroup",
            "abstract",
            "nillable",
            "fixed",
            "default",
            "block",
            "final",
            "form",
        ] {
            if node.attr(excluded).is_some() {
                self.reject(node.pos, &format!("xs:element with '{excluded}'"));
            }
        }
        let allowed_all: Vec<&str> = allowed
            .iter()
            .copied()
            .chain([
                "ref",
                "substitutionGroup",
                "abstract",
                "nillable",
                "fixed",
                "default",
                "block",
                "final",
                "form",
            ])
            .collect();
        self.check_attrs(node, &allowed_all);

        let Some(name) = node.attr("name") else {
            // Covered above for ref=; anything else nameless is malformed.
            if node.attr("ref").is_none() {
                self.finding(node.pos, "xs:element without a name");
            }
            return None;
        };
        let occurs = if local {
            self.occurs(node)
        } else {
            Occurs::ONE
        };
        let type_ref = self.element_type(node)?;
        Some(ElementDecl {
            name: name.to_string(),
            type_ref,
            occurs,
        })
    }

    fn element_type(&mut self, node: &Node) -> Option<TypeRef> {
        let inline_complex = node.children_named("complexType").next();
        let inline_simple = node.children_named("simpleType").next();
        for child in &node.children {
            if !matches!(child.local.as_str(), "complexType" | "simpleType") {
                self.reject(child.pos, &format!("xs:{} under xs:element", child.local));
            }
        }
        match (node.attr("type"), inline_complex, inline_simple) {
            (Some(_), None, None) => {
                let q = node.q_ref("type")?.clone();
                self.type_ref(node.pos, &q)
            }
            (None, Some(complex), None) => Some(TypeRef::Inline(Box::new(TypeDef::Complex(
                self.complex_type(complex),
            )))),
            (None, None, Some(simple)) => Some(TypeRef::Inline(Box::new(TypeDef::Simple(
                self.simple_type(simple, &mut Vec::new()),
            )))),
            (None, None, None) => {
                self.finding(
                    node.pos,
                    "an untyped element (implicit xs:anyType) is not in the supported subset",
                );
                None
            }
            _ => {
                self.finding(
                    node.pos,
                    "xs:element with both a type attribute and an inline type",
                );
                None
            }
        }
    }

    /// Resolve a `type=` QName: a supported builtin, or a named type of this schema.
    fn type_ref(&mut self, pos: usize, (ns, local): &(String, String)) -> Option<TypeRef> {
        if ns == XSD_NS {
            match Builtin::by_name(local) {
                Some(builtin) => return Some(TypeRef::Builtin(builtin)),
                None => {
                    self.reject(pos, &format!("builtin type xs:{local}"));
                    return None;
                }
            }
        }
        if *ns == self.target_namespace {
            if self.named_simple.contains_key(local.as_str())
                || self.named_complex.contains_key(local.as_str())
            {
                return Some(TypeRef::Named(local.clone()));
            }
            self.finding(pos, format!("reference to undeclared type '{local}'"));
            return None;
        }
        self.finding(
            pos,
            format!("type reference into foreign namespace '{ns}' (schemas are self-contained)"),
        );
        None
    }

    fn occurs(&mut self, node: &Node) -> Occurs {
        let min = match node.attr("minOccurs") {
            None => 1,
            Some(raw) => match raw.parse::<u32>() {
                Ok(n) => n,
                Err(_) => {
                    self.finding(node.pos, format!("invalid minOccurs '{raw}'"));
                    1
                }
            },
        };
        let max = match node.attr("maxOccurs") {
            None => Max::Bounded(1),
            Some("unbounded") => Max::Unbounded,
            Some(raw) => match raw.parse::<u32>() {
                Ok(n) => Max::Bounded(n),
                Err(_) => {
                    self.finding(node.pos, format!("invalid maxOccurs '{raw}'"));
                    Max::Bounded(1)
                }
            },
        };
        if let Max::Bounded(n) = max {
            if n < min {
                self.finding(
                    node.pos,
                    format!("maxOccurs {n} is less than minOccurs {min}"),
                );
            }
        }
        Occurs { min, max }
    }

    // ---- complex types ----------------------------------------------------

    fn complex_type(&mut self, node: &Node) -> ComplexDef {
        for excluded in ["abstract", "mixed", "block", "final"] {
            if node.attr(excluded).is_some() {
                self.reject(node.pos, &format!("xs:complexType with '{excluded}'"));
            }
        }
        self.check_attrs(node, &["name", "abstract", "mixed", "block", "final"]);

        let mut content = Content::Empty;
        let mut attributes = Vec::new();
        for child in &node.children {
            match child.local.as_str() {
                "sequence" | "choice" => match content {
                    Content::Empty => content = Content::Group(self.group(child)),
                    _ => self.finding(child.pos, "multiple content models on one complexType"),
                },
                "simpleContent" => match content {
                    Content::Empty => {
                        if let Some((simple, attrs)) = self.simple_content(child) {
                            content = Content::Simple(simple);
                            attributes.extend(attrs);
                        }
                    }
                    _ => self.finding(child.pos, "multiple content models on one complexType"),
                },
                "attribute" => {
                    if let Some(attr) = self.attribute_decl(child) {
                        attributes.push(attr);
                    }
                }
                other => self.reject(child.pos, &format!("xs:{other} under xs:complexType")),
            }
        }
        ComplexDef {
            content,
            attributes,
        }
    }

    fn group(&mut self, node: &Node) -> GroupDef {
        self.check_attrs(node, &["minOccurs", "maxOccurs"]);
        let kind = if node.local == "sequence" {
            GroupKind::Sequence
        } else {
            GroupKind::Choice
        };
        let occurs = self.occurs(node);
        let mut items = Vec::new();
        for child in &node.children {
            match child.local.as_str() {
                "element" => {
                    if let Some(decl) = self.element_decl(child, true) {
                        items.push(Particle::Element(decl));
                    }
                }
                "sequence" | "choice" => items.push(Particle::Group(self.group(child))),
                "any" => {
                    if let Some(wildcard) = self.wildcard(child) {
                        items.push(wildcard);
                    }
                }
                other => self.reject(child.pos, &format!("xs:{other} inside a content model")),
            }
        }
        let inner_nullable = match kind {
            GroupKind::Sequence => items.iter().all(Particle::nullable),
            GroupKind::Choice => items.iter().any(Particle::nullable),
        };
        GroupDef {
            kind,
            items,
            occurs,
            inner_nullable,
        }
    }

    fn wildcard(&mut self, node: &Node) -> Option<Particle> {
        self.check_attrs(
            node,
            &["namespace", "processContents", "minOccurs", "maxOccurs"],
        );
        let occurs = self.occurs(node);
        // Any namespace constraint is treated as accept-any (the SupplementaryData
        // idiom); the constraint that matters is the processContents mode.
        let lax = match node.attr("processContents") {
            Some("lax") => true,
            Some("skip") => false,
            other => {
                self.reject(
                    node.pos,
                    &format!(
                        "xs:any with processContents=\"{}\"",
                        other.unwrap_or("strict")
                    ),
                );
                return None;
            }
        };
        Some(Particle::Wildcard { occurs, lax })
    }

    fn simple_content(&mut self, node: &Node) -> Option<(SimpleDef, Vec<AttrDecl>)> {
        self.check_attrs(node, &[]);
        let mut extension = None;
        for child in &node.children {
            match child.local.as_str() {
                "extension" => extension = Some(child),
                other => self.reject(child.pos, &format!("xs:{other} under xs:simpleContent")),
            }
        }
        let extension = extension?;
        self.check_attrs(extension, &["base"]);
        let base = match extension.q_ref("base") {
            Some(q) => q.clone(),
            None => {
                self.finding(extension.pos, "xs:extension without a base");
                return None;
            }
        };
        let simple = self.simple_ref(extension.pos, &base)?;
        let mut attrs = Vec::new();
        for child in &extension.children {
            match child.local.as_str() {
                "attribute" => {
                    if let Some(attr) = self.attribute_decl(child) {
                        attrs.push(attr);
                    }
                }
                other => self.reject(child.pos, &format!("xs:{other} under xs:extension")),
            }
        }
        Some((simple, attrs))
    }

    fn attribute_decl(&mut self, node: &Node) -> Option<AttrDecl> {
        for excluded in ["ref", "default", "fixed", "form"] {
            if node.attr(excluded).is_some() {
                self.reject(node.pos, &format!("xs:attribute with '{excluded}'"));
            }
        }
        self.check_attrs(
            node,
            &["name", "type", "use", "ref", "default", "fixed", "form"],
        );
        let name = node.attr("name")?.to_string();
        let required = match node.attr("use") {
            None | Some("optional") => false,
            Some("required") => true,
            Some(other) => {
                self.reject(node.pos, &format!("xs:attribute with use=\"{other}\""));
                false
            }
        };
        let inline = node.children_named("simpleType").next();
        let value_type = match (node.attr("type"), inline) {
            (Some(_), None) => {
                let q = node.q_ref("type")?.clone();
                self.simple_ref(node.pos, &q)?
            }
            (None, Some(simple)) => self.simple_type(simple, &mut Vec::new()),
            (None, None) => {
                self.finding(
                    node.pos,
                    "an untyped attribute is not in the supported subset",
                );
                return None;
            }
            _ => {
                self.finding(
                    node.pos,
                    "xs:attribute with both a type attribute and an inline type",
                );
                return None;
            }
        };
        Some(AttrDecl {
            name,
            value_type,
            required,
        })
    }

    // ---- simple types -----------------------------------------------------

    /// Resolve a QName that must denote a simple type (attribute values,
    /// simpleContent bases) to a flattened [`SimpleDef`].
    fn simple_ref(&mut self, pos: usize, (ns, local): &(String, String)) -> Option<SimpleDef> {
        if ns == XSD_NS {
            return match Builtin::by_name(local) {
                Some(builtin) => Some(SimpleDef {
                    builtin,
                    steps: Vec::new(),
                }),
                None => {
                    self.reject(pos, &format!("builtin type xs:{local}"));
                    None
                }
            };
        }
        if *ns == self.target_namespace {
            if let Some(node) = self.named_simple.get(local.as_str()).copied() {
                return Some(self.simple_type(node, &mut vec![local.clone()]));
            }
            if self.named_complex.contains_key(local.as_str()) {
                self.reject(pos, &format!("deriving from complex type '{local}' here"));
                return None;
            }
            self.finding(pos, format!("reference to undeclared type '{local}'"));
            return None;
        }
        self.finding(
            pos,
            format!("type reference into foreign namespace '{ns}' (schemas are self-contained)"),
        );
        None
    }

    /// Compile a simpleType node, flattening its restriction chain. `seen` guards
    /// against derivation cycles.
    fn simple_type(&mut self, node: &Node, seen: &mut Vec<String>) -> SimpleDef {
        self.check_attrs(node, &["name"]);
        let fallback = SimpleDef {
            builtin: Builtin::String,
            steps: Vec::new(),
        };
        let mut restriction = None;
        for child in &node.children {
            match child.local.as_str() {
                "restriction" => restriction = Some(child),
                other => self.reject(child.pos, &format!("xs:{other} under xs:simpleType")),
            }
        }
        let Some(restriction) = restriction else {
            self.finding(node.pos, "xs:simpleType without a restriction");
            return fallback;
        };
        self.check_attrs(restriction, &["base"]);
        let Some((base_ns, base_local)) = restriction.q_ref("base").cloned() else {
            self.finding(
                restriction.pos,
                "xs:restriction without a base is not in the supported subset",
            );
            return fallback;
        };

        // Resolve the base first (builtin terminates the chain; a named simple type
        // recurses), then prepend this step's facets.
        let mut base_def = if base_ns == XSD_NS {
            match Builtin::by_name(&base_local) {
                Some(builtin) => SimpleDef {
                    builtin,
                    steps: Vec::new(),
                },
                None => {
                    self.reject(restriction.pos, &format!("builtin type xs:{base_local}"));
                    fallback
                }
            }
        } else if base_ns == self.target_namespace {
            if seen.contains(&base_local) {
                self.finding(
                    restriction.pos,
                    format!("circular simpleType derivation through '{base_local}'"),
                );
                fallback
            } else if let Some(base_node) = self.named_simple.get(base_local.as_str()).copied() {
                seen.push(base_local.clone());
                let def = self.simple_type(base_node, seen);
                seen.pop();
                def
            } else {
                self.finding(
                    restriction.pos,
                    format!("reference to undeclared simple type '{base_local}'"),
                );
                fallback
            }
        } else {
            self.finding(
                restriction.pos,
                format!("type reference into foreign namespace '{base_ns}' (schemas are self-contained)"),
            );
            fallback
        };

        let step = self.facet_step(restriction, base_def.builtin);
        if !step.is_empty() {
            base_def.steps.insert(0, step);
        }
        base_def
    }

    fn facet_step(&mut self, restriction: &Node, builtin: Builtin) -> FacetStep {
        let mut step = FacetStep::default();
        for facet in &restriction.children {
            let value = match facet.attr("value") {
                Some(v) => v,
                None => {
                    self.finding(facet.pos, format!("xs:{} without a value", facet.local));
                    continue;
                }
            };
            self.check_attrs(facet, &["value"]);
            match facet.local.as_str() {
                "enumeration" => step
                    .enumeration
                    .get_or_insert_with(Vec::new)
                    .push(value.to_string()),
                "pattern" => match Regex::new(&format!("^(?:{value})$")) {
                    Ok(regex) => {
                        step.patterns.push(regex);
                        step.pattern_texts.push(value.to_string());
                    }
                    Err(_) => self.finding(
                        facet.pos,
                        format!("pattern '{value}' is outside the supported regex dialect"),
                    ),
                },
                "length" => step.length = self.count_facet(facet, value),
                "minLength" => step.min_length = self.count_facet(facet, value),
                "maxLength" => step.max_length = self.count_facet(facet, value),
                "totalDigits" | "fractionDigits" | "minInclusive" | "maxInclusive"
                    if !builtin.is_numeric() =>
                {
                    self.finding(
                        facet.pos,
                        format!(
                            "xs:{} on a non-numeric base is not in the supported subset",
                            facet.local
                        ),
                    );
                }
                "totalDigits" => step.total_digits = self.count_facet(facet, value),
                "fractionDigits" => step.fraction_digits = self.count_facet(facet, value),
                "minInclusive" => step.min_inclusive = self.decimal_facet(facet, value),
                "maxInclusive" => step.max_inclusive = self.decimal_facet(facet, value),
                other => self.reject(facet.pos, &format!("facet xs:{other}")),
            }
        }
        if (step.length.is_some() || step.min_length.is_some() || step.max_length.is_some())
            && builtin.is_numeric()
        {
            self.finding(
                restriction.pos,
                "length facets on a numeric base are not in the supported subset",
            );
        }
        step
    }

    fn count_facet(&mut self, facet: &Node, value: &str) -> Option<u64> {
        match value.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => {
                self.finding(
                    facet.pos,
                    format!("invalid xs:{} value '{value}'", facet.local),
                );
                None
            }
        }
    }

    fn decimal_facet(&mut self, facet: &Node, value: &str) -> Option<BigDecimal> {
        match value.parse::<BigDecimal>() {
            Ok(n) => Some(n),
            Err(_) => {
                self.finding(
                    facet.pos,
                    format!("invalid xs:{} value '{value}'", facet.local),
                );
                None
            }
        }
    }
}
