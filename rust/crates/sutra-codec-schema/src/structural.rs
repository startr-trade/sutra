//! The version-scoped module codec compiled from a module's `schemas/<name>/*.xsd` — the
//! [`StructuralCodec`]: multi-format decode (xml/json/yaml per the codec-manifest
//! `formats`), message type from the document root, payload = the root's children as a
//! FEEL-walkable map, and leaf-value coercion per the XSD's declared leaf types
//! (`xs:decimal`/`xs:integer`/… → number, `xs:boolean` → boolean).
//!
//! Two construction modes:
//!
//! - [`StructuralCodec::compile`] — the original slice (scan-based roots + coercion, decode-only,
//!   no structural validation). Preserved byte-for-byte for the existing engine-assembly /
//!   loader callers.
//! - [`StructuralCodec::compile_with_formats`] — the validating mode: XSD validation is
//!   wired through the [`sutra_xsd`] subset validator. XML validates directly; json/yaml are
//!   transcoded into the schema's target namespace (canonicalised to the declared child order
//!   the serde tree loses) and validated against the same schema set. An XSD-invalid document
//!   is `SOFT_ERRORS` with `SUTRA.PARSE.XSD.SCHEMA_VIOLATION` (still projected + routable); a
//!   parse/transcode failure is `SUTRA.RUNTIME.CODEC.DECODE_FAILED` (FATAL). `compile_with_formats`
//!   is fallible — an XSD outside the subset (or a malformed one) is a deploy error the
//!   `SchemaCodecLoader` surfaces as `SUTRA.CONFIG.SCHEMA.INVALID`.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use quick_xml::events::Event;
use quick_xml::reader::Reader;
use sutra_xsd::{DiagnosticProfile, SchemaSet, Severity};

use sutra_codec_spi::codec::PayloadCodec;
use sutra_codec_spi::codes;
use sutra_codec_spi::issue::{IssueSeverity, ValidationIssue};
use sutra_codec_spi::result::{CodecValue, DecodeOutcome, DecodeResult};
use sutra_formats::json::JsonCodec;
use sutra_formats::xml::XmlCodec;
use sutra_formats::yaml::YamlCodec;

/// The default wire encodings for the legacy [`StructuralCodec::compile`] (the full superset).
const LEGACY_CONTENT_TYPES: &[&str] = &[
    "application/xml",
    "text/xml",
    "application/*+xml",
    "application/json",
    "application/*+json",
    "application/x-yaml",
    "application/yaml",
    "text/yaml",
];

/// A module's structural codec, registered under its version-scoped URN
/// (`<module namespace>:<schema folder base name>`).
pub struct StructuralCodec {
    urn: String,
    roots: HashSet<String>,
    number_fields: HashSet<String>,
    boolean_fields: HashSet<String>,
    accepted_content_types: Vec<String>,
    /// `Some` only in the validating (`compile_with_formats`) mode: the compiled schema set,
    /// per-root namespace, and per-root declared child order for json/yaml canonicalisation.
    validation: Option<Validation>,
}

struct Validation {
    schema_set: SchemaSet,
    root_to_namespace: BTreeMap<String, String>,
    child_orders: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}

impl StructuralCodec {
    /// Decode-only slice: scan-based roots + coercion, no structural validation.
    /// Behaviour is preserved verbatim for the existing engine-assembly / loader callers.
    pub fn compile(urn: &str, xsds: &[&[u8]]) -> StructuralCodec {
        let mut roots = HashSet::new();
        let mut number_fields = HashSet::new();
        let mut boolean_fields = HashSet::new();
        for xsd in xsds {
            scan_xsd(xsd, &mut roots, &mut number_fields, &mut boolean_fields);
        }
        StructuralCodec {
            urn: urn.to_string(),
            roots,
            number_fields,
            boolean_fields,
            accepted_content_types: LEGACY_CONTENT_TYPES.iter().map(|s| s.to_string()).collect(),
            validation: None,
        }
    }

    /// Validating mode: wire XSD validation through [`sutra_xsd`], with `accepted`
    /// content-types driven by the codec-manifest `formats` (a subset of `xml`/`json`/`yaml`).
    /// `Err` when the XSD set does not compile (outside the supported subset or malformed) —
    /// the `SchemaCodecLoader` maps that to `SUTRA.CONFIG.SCHEMA.INVALID`.
    pub fn compile_with_formats(
        urn: &str,
        xsds: &[&[u8]],
        formats: &[&str],
    ) -> Result<StructuralCodec, String> {
        let schema_set = SchemaSet::compile(xsds).map_err(|e| e.to_string())?;

        let mut roots = HashSet::new();
        let mut number_fields = HashSet::new();
        let mut boolean_fields = HashSet::new();
        let mut root_to_namespace = BTreeMap::new();
        let mut child_orders = BTreeMap::new();
        for schema in schema_set.schemas() {
            let ns = schema.target_namespace().to_string();
            for root in schema.root_names() {
                roots.insert(root.to_string());
                root_to_namespace
                    .entry(root.to_string())
                    .or_insert_with(|| ns.clone());
                child_orders.insert(root.to_string(), schema.child_order(root));
            }
            let coercion = schema.value_coercion();
            number_fields.extend(coercion.number_elements.iter().cloned());
            boolean_fields.extend(coercion.boolean_elements.iter().cloned());
        }

        Ok(StructuralCodec {
            urn: urn.to_string(),
            roots,
            number_fields,
            boolean_fields,
            accepted_content_types: content_types_for(formats),
            validation: Some(Validation {
                schema_set,
                root_to_namespace,
                child_orders,
            }),
        })
    }

    /// The message-type roots this codec covers (diagnostics / routing introspection).
    pub fn roots(&self) -> &HashSet<String> {
        &self.roots
    }

    fn coerce(&self, value: serde_json::Value, key: Option<&str>) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(k, v)| {
                        let coerced = self.coerce(v, Some(&k));
                        (k, coerced)
                    })
                    .collect(),
            ),
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(|v| self.coerce(v, key)).collect())
            }
            serde_json::Value::String(s) => {
                if let Some(k) = key {
                    if self.number_fields.contains(k) {
                        if let Ok(n) = s.trim().parse::<serde_json::Number>() {
                            return serde_json::Value::Number(n);
                        }
                    }
                    if self.boolean_fields.contains(k) {
                        match s.trim() {
                            t if t.eq_ignore_ascii_case("true") => {
                                return serde_json::Value::Bool(true)
                            }
                            t if t.eq_ignore_ascii_case("false") => {
                                return serde_json::Value::Bool(false)
                            }
                            _ => {}
                        }
                    }
                }
                serde_json::Value::String(s)
            }
            other => other,
        }
    }

    // ---- validating decode (compile_with_formats) --------------------------------------------

    fn decode_validating(&self, validation: &Validation, body: &[u8], ct: &str) -> DecodeResult {
        // 1. Canonical XML bytes: XML direct; json/yaml transcoded to the target namespace.
        let xml_bytes = match self.to_xml_bytes(validation, body, Some(ct)) {
            Ok(bytes) => bytes,
            Err(message) => return fatal_decode(&message, ct),
        };
        // 2. Parse + project via the XML codec (XmlToMap: string leaves, repeated → list).
        let projected = XmlCodec.decode(&xml_bytes, Some("application/xml"));
        let Some(CodecValue::Json(payload_json)) = projected.payload else {
            let message = projected
                .issues
                .first()
                .map(|i| i.message.clone())
                .unwrap_or_else(|| "document is not well-formed".to_string());
            return fatal_decode(&message, ct);
        };
        let root = projected.message_type.clone().unwrap_or_default();
        // 3. Structural validation against the member declaring this root (or the first member).
        let mut issues = Vec::new();
        let mut outcome = DecodeOutcome::Ok;
        let schema = validation
            .schema_set
            .schema_for_root(&root)
            .or_else(|| validation.schema_set.schemas().first());
        if let Some(schema) = schema {
            match schema.validate(&xml_bytes) {
                Ok(violations) => {
                    for v in &violations {
                        issues.push(to_issue(v.diagnostic(DiagnosticProfile::MODULE_CODEC)));
                    }
                    if !issues.is_empty() {
                        outcome = DecodeOutcome::SoftErrors;
                    }
                }
                Err(document_error) => return fatal_decode(&document_error.to_string(), ct),
            }
        }
        // 4. Apply the schema's leaf types to the projected string leaves.
        let coerced = self.coerce(payload_json, None);
        DecodeResult {
            outcome,
            payload: Some(CodecValue::Json(coerced)),
            issues,
            content_type: ct.to_string(),
            message_type: (!root.is_empty()).then_some(root),
        }
    }

    fn to_xml_bytes(
        &self,
        validation: &Validation,
        body: &[u8],
        content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        match detect(body, content_type) {
            Kind::Xml => Ok(body.to_vec()),
            Kind::Json => {
                let tree: serde_json::Value =
                    serde_json::from_slice(body).map_err(|e| format!("JSON parse failed: {e}"))?;
                self.transcode(validation, &tree)
            }
            Kind::Yaml => {
                let tree: serde_json::Value = serde_yaml_ng::from_slice(body)
                    .map_err(|e| format!("YAML parse failed: {e}"))?;
                self.transcode(validation, &tree)
            }
        }
    }

    /// Transcode a `{Root: {…}}` tree into XML in the root's target namespace, re-emitting
    /// children in the schema's declared order (the serde tree lost insertion order).
    fn transcode(
        &self,
        validation: &Validation,
        tree: &serde_json::Value,
    ) -> Result<Vec<u8>, String> {
        let serde_json::Value::Object(map) = tree else {
            return Err("transcode requires a single-rooted object".to_string());
        };
        if map.len() != 1 {
            return Err(format!(
                "transcode requires exactly one root element, got {}",
                map.len()
            ));
        }
        let (root_name, root_value) = map.iter().next().expect("len checked");
        let namespace = self.namespace_for(validation, root_name);
        let order = validation.child_orders.get(root_name);
        let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        emit_element(&mut out, root_name, root_value, Some(&namespace), "", order);
        Ok(out.into_bytes())
    }

    /// The XSD target namespace to transcode into: the single shared namespace when the whole
    /// set has one; otherwise the declaring file's namespace for this root.
    fn namespace_for(&self, validation: &Validation, root: &str) -> String {
        let distinct: BTreeSet<&String> = validation.root_to_namespace.values().collect();
        if distinct.len() == 1 {
            return distinct.into_iter().next().cloned().unwrap_or_default();
        }
        validation
            .root_to_namespace
            .get(root)
            .cloned()
            .unwrap_or_default()
    }

    fn encode_validating(
        &self,
        validation: &Validation,
        payload: &CodecValue,
        content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        let CodecValue::Json(tree) = payload else {
            return Err(encode_failed("reply payload must be a map/tree"));
        };
        // Transcode + defensive XSD conformance check (outbound conformance is a
        // deploy-time guarantee; this never fires at runtime, but a non-conformant reply is
        // an encode error).
        let xml_bytes = self
            .transcode(validation, tree)
            .map_err(|e| encode_failed(&e))?;
        let root = match tree {
            serde_json::Value::Object(map) => map.keys().next().cloned().unwrap_or_default(),
            _ => String::new(),
        };
        let schema = validation
            .schema_set
            .schema_for_root(&root)
            .or_else(|| validation.schema_set.schemas().first());
        if let Some(schema) = schema {
            match schema.validate(&xml_bytes) {
                Ok(violations) if !violations.is_empty() => {
                    return Err(encode_failed(&format!(
                        "reply does not conform to schema '{}': {}",
                        self.urn, violations[0].message
                    )));
                }
                Err(e) => return Err(encode_failed(&e.to_string())),
                _ => {}
            }
        }
        match detect_for_encode(content_type) {
            Kind::Xml => Ok(xml_bytes),
            Kind::Json => serde_json::to_vec(tree).map_err(|e| encode_failed(&e.to_string())),
            Kind::Yaml => serde_yaml_ng::to_string(tree)
                .map(String::into_bytes)
                .map_err(|e| encode_failed(&e.to_string())),
        }
    }

    // ---- legacy decode (compile) -------------------------------------------------------------

    fn decode_legacy(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let inner = match detect(body, content_type) {
            Kind::Xml => XmlCodec.decode(body, content_type),
            Kind::Json => JsonCodec.decode(body, content_type),
            Kind::Yaml => YamlCodec.decode(body, content_type),
        };
        if inner.payload.is_none() {
            return inner; // FATAL rides through untouched
        }
        let (payload, message_type) = match (&inner.payload, &inner.message_type) {
            (Some(CodecValue::Json(v)), Some(mt)) => (v.clone(), Some(mt.clone())),
            (Some(CodecValue::Json(serde_json::Value::Object(map))), None) if map.len() == 1 => {
                let (root, value) = map.iter().next().expect("len checked");
                (value.clone(), Some(root.clone()))
            }
            (Some(CodecValue::Json(v)), None) => (v.clone(), None),
            _ => {
                return inner;
            }
        };
        let coerced = self.coerce(payload, None);
        let mut out = inner;
        out.payload = Some(CodecValue::Json(coerced));
        out.message_type = message_type;
        out
    }
}

fn to_issue(diag: sutra_xsd::Diagnostic) -> ValidationIssue {
    ValidationIssue {
        code: diag.code,
        severity: match diag.severity {
            Severity::Error => IssueSeverity::Error,
            Severity::Warning => IssueSeverity::Warning,
        },
        path: diag.path,
        message: diag.message,
        value: diag.value,
    }
}

fn fatal_decode(message: &str, ct: &str) -> DecodeResult {
    DecodeResult::fatal(
        vec![ValidationIssue::error(
            codes::RUNTIME_CODEC_DECODE_FAILED,
            "",
            format!("decode failed: {}", message.replace(['\n', '\r'], " ")),
        )],
        ct,
    )
}

fn encode_failed(message: &str) -> String {
    format!(
        "{}: {}",
        codes::OUTBOUND_ENCODE_FAILED,
        message.replace(['\n', '\r'], " ")
    )
}

/// The content-types accepted for each declared format (xml/json/yaml).
fn content_types_for(formats: &[&str]) -> Vec<String> {
    let mut cts = Vec::new();
    for f in formats {
        match *f {
            "xml" => {
                cts.push("application/xml".to_string());
                cts.push("text/xml".to_string());
                cts.push("application/*+xml".to_string());
            }
            "json" => {
                cts.push("application/json".to_string());
                cts.push("application/*+json".to_string());
            }
            "yaml" => {
                cts.push("application/x-yaml".to_string());
                cts.push("application/yaml".to_string());
                cts.push("text/yaml".to_string());
            }
            _ => {}
        }
    }
    cts
}

// ---- XML emission (schema-ordered transcode) -------------------------------------------------

/// Emit `<name …>…</name>` from a serde tree. `ns` (root only) becomes a default `xmlns`;
/// `@key` object entries are attributes, `#text` is character content, and child elements are
/// emitted in the schema's declared order (`order`) then any remaining keys.
fn emit_element(
    out: &mut String,
    name: &str,
    value: &serde_json::Value,
    ns: Option<&str>,
    path: &str,
    order: Option<&BTreeMap<String, Vec<String>>>,
) {
    out.push('<');
    out.push_str(name);
    if let Some(ns) = ns {
        if !ns.is_empty() {
            out.push_str(" xmlns=\"");
            out.push_str(&escape_attr(ns));
            out.push('"');
        }
    }
    match value {
        serde_json::Value::Object(obj) => {
            for (k, v) in obj {
                if let Some(attr) = k.strip_prefix('@') {
                    out.push(' ');
                    out.push_str(attr);
                    out.push_str("=\"");
                    out.push_str(&escape_attr(&scalar_text(v)));
                    out.push('"');
                }
            }
            out.push('>');
            if let Some(t) = obj.get("#text") {
                out.push_str(&escape_text(&scalar_text(t)));
            }
            for child_name in ordered_children(obj, path, order) {
                let child = &obj[&child_name];
                let child_path = if path.is_empty() {
                    child_name.clone()
                } else {
                    format!("{path}.{child_name}")
                };
                match child {
                    serde_json::Value::Array(items) => {
                        for item in items {
                            emit_element(out, &child_name, item, None, &child_path, order);
                        }
                    }
                    other => emit_element(out, &child_name, other, None, &child_path, order),
                }
            }
            out.push_str("</");
            out.push_str(name);
            out.push('>');
        }
        serde_json::Value::Null => {
            out.push_str("></");
            out.push_str(name);
            out.push('>');
        }
        scalar => {
            out.push('>');
            out.push_str(&escape_text(&scalar_text(scalar)));
            out.push_str("</");
            out.push_str(name);
            out.push('>');
        }
    }
}

/// Child element names in declared order (schema `order` first), then any remaining
/// non-attribute, non-text keys — so authored/unknown children are never dropped.
fn ordered_children(
    obj: &serde_json::Map<String, serde_json::Value>,
    path: &str,
    order: Option<&BTreeMap<String, Vec<String>>>,
) -> Vec<String> {
    let mut result = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    if let Some(names) = order.and_then(|o| o.get(path)) {
        for n in names {
            if obj.contains_key(n) && seen.insert(n.clone()) {
                result.push(n.clone());
            }
        }
    }
    for k in obj.keys() {
        if k.starts_with('@') || k == "#text" {
            continue;
        }
        if seen.insert(k.clone()) {
            result.push(k.clone());
        }
    }
    result
}

fn scalar_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn escape_attr(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn escape_text(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// ---- XSD scan (legacy roots + coercion) ------------------------------------------------------

/// One pass over an XSD: depth-2 `<xs:element name>` = a global root (message type);
/// any named element with a numeric/boolean built-in type feeds coercion.
// `Attribute::unescape_value` is deprecated in quick-xml 0.41 in favour of
// `normalized_value` (which additionally collapses in-value whitespace); we keep the
// exact 0.37 entity-only semantics, so the deprecation is allowed deliberately.
#[allow(deprecated)]
fn scan_xsd(
    xsd: &[u8],
    roots: &mut HashSet<String>,
    number_fields: &mut HashSet<String>,
    boolean_fields: &mut HashSet<String>,
) {
    let mut reader = Reader::from_reader(xsd);
    reader.config_mut().expand_empty_elements = true;
    let mut depth = 0usize;
    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                depth += 1;
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if local == "element" {
                    let mut name = None;
                    let mut type_ref = None;
                    for a in e.attributes().flatten() {
                        let key = String::from_utf8_lossy(a.key.as_ref()).into_owned();
                        let value = a.unescape_value().unwrap_or_default().into_owned();
                        match key.as_str() {
                            "name" => name = Some(value),
                            "type" => type_ref = Some(value),
                            _ => {}
                        }
                    }
                    if let Some(name) = name {
                        if depth == 2 {
                            roots.insert(name.clone());
                        }
                        match type_ref.as_deref().map(local_type) {
                            Some("decimal")
                            | Some("integer")
                            | Some("int")
                            | Some("long")
                            | Some("short")
                            | Some("byte")
                            | Some("double")
                            | Some("float")
                            | Some("nonNegativeInteger")
                            | Some("positiveInteger") => {
                                number_fields.insert(name);
                            }
                            Some("boolean") => {
                                boolean_fields.insert(name);
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(Event::End(_)) => depth = depth.saturating_sub(1),
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
}

enum Kind {
    Xml,
    Json,
    Yaml,
}

/// Format detection: content-type first, else sniff the first byte.
fn detect(body: &[u8], content_type: Option<&str>) -> Kind {
    let ct = content_type.unwrap_or("").to_lowercase();
    if ct.contains("xml") {
        return Kind::Xml;
    }
    if ct.contains("json") {
        return Kind::Json;
    }
    if ct.contains("yaml") || ct.contains("yml") {
        return Kind::Yaml;
    }
    for b in body {
        match b {
            b' ' | b'\n' | b'\r' | b'\t' => continue,
            b'<' => return Kind::Xml,
            b'{' | b'[' => return Kind::Json,
            _ => break,
        }
    }
    Kind::Yaml
}

/// Outbound counterpart of [`detect`] — content-type only (no body to sniff), XSD-native
/// default.
fn detect_for_encode(content_type: Option<&str>) -> Kind {
    let ct = content_type.unwrap_or("").to_lowercase();
    if ct.contains("xml") {
        return Kind::Xml;
    }
    if ct.contains("json") {
        return Kind::Json;
    }
    if ct.contains("yaml") || ct.contains("yml") {
        return Kind::Yaml;
    }
    Kind::Xml
}

fn local_type(type_ref: &str) -> &str {
    type_ref
        .rsplit_once(':')
        .map(|(_, l)| l)
        .unwrap_or(type_ref)
}

impl PayloadCodec for StructuralCodec {
    fn name(&self) -> &str {
        &self.urn
    }

    /// The navigation shape of `message_type`, derived from the compiled XSD. Only the
    /// validating build ([`StructuralCodec::compile_with_formats`]) retains the schema set, so
    /// the decode-only compile exposes none. A blank/unknown type or an XSD outside the
    /// `sutra_xsd` subset yields `None` (an Unverifiable WARNING upstream, never a false error).
    fn shape_of(&self, message_type: Option<&str>) -> Option<sutra_codec_spi::shape::SchemaShape> {
        let mt = message_type?.trim();
        if mt.is_empty() {
            return None;
        }
        let validation = self.validation.as_ref()?;
        let schema = validation.schema_set.schema_for_root(mt)?;
        Some(navigation_shape_to_schema(schema.navigation_shape(mt)))
    }

    fn accepted_content_types(&self) -> Vec<String> {
        self.accepted_content_types.clone()
    }

    /// The declared message types — the XSD global elements (sorted).
    fn declared_message_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self.roots.iter().cloned().collect();
        types.sort_unstable();
        types
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        match &self.validation {
            Some(validation) => {
                let ct = content_type.unwrap_or("application/octet-stream");
                self.decode_validating(validation, body, ct)
            }
            None => self.decode_legacy(body, content_type),
        }
    }

    fn encode(&self, payload: &CodecValue, content_type: Option<&str>) -> Result<Vec<u8>, String> {
        match &self.validation {
            Some(validation) => self.encode_validating(validation, payload, content_type),
            None => Err(format!(
                "structural codec '{}' is decode-only (replies are template renders); \
                 schema-driven reply encoding arrives with the schema-codec family",
                self.urn
            )),
        }
    }
}

/// Bridge a `sutra_xsd` navigation shape into the shared [`sutra_codec_spi::shape::SchemaShape`] (the
/// two field-kind enums mirror one another variant-for-variant). Formerly the loader lint's
/// `module_shape`; it lives here now so `StructuralCodec::shape_of` is the single source.
fn navigation_shape_to_schema(
    nav: sutra_xsd::NavigationShape,
) -> sutra_codec_spi::shape::SchemaShape {
    let mut shape = sutra_codec_spi::shape::SchemaShape::default();
    for (path, kind) in &nav.paths {
        shape = shape.path(path, field_kind_to_shape_type(*kind));
    }
    for open in &nav.open {
        shape = shape.open(open);
    }
    shape
}

fn field_kind_to_shape_type(kind: sutra_xsd::FieldKind) -> sutra_codec_spi::shape::ShapeFieldType {
    use sutra_codec_spi::shape::ShapeFieldType;
    match kind {
        sutra_xsd::FieldKind::String => ShapeFieldType::String,
        sutra_xsd::FieldKind::Number => ShapeFieldType::Number,
        sutra_xsd::FieldKind::Boolean => ShapeFieldType::Boolean,
        sutra_xsd::FieldKind::Object => ShapeFieldType::Object,
        sutra_xsd::FieldKind::Array => ShapeFieldType::Array,
        sutra_xsd::FieldKind::Any => ShapeFieldType::Any,
    }
}
