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
    /// `Some` when the manifest declared a tabular format (`csv` / `fixed-width`) — the BATCH
    /// intake. A tabular body is a table, not a document, so it is validated ROW-WISE: each row
    /// becomes one instance of the declared root and is validated on its own, and every row's
    /// violations are reported together with a row-indexed path
    /// together with a row-indexed path).
    batch: Option<BatchFormats>,
}

/// The batch (tabular) intakes a codec accepts alongside its document formats — `csv` and/or
/// `fixed-width`. Held as trait objects because the two differ only in how a line is split;
/// everything downstream (wrap as a record, transcode, validate, project) is identical.
///
/// Both may be declared together: their content types are disjoint (`text/csv` /
/// `application/csv` against `text/plain` / `application/x-fixed-width`), so an inbound body
/// selects its parser unambiguously and one schema can serve two wire forms.
struct BatchFormats {
    parsers: Vec<std::sync::Arc<dyn PayloadCodec>>,
    /// True when the tabular formats are the ONLY ones declared, which is what lets a body with
    /// no content-type at all be read as a table rather than guessed at. With several tabular
    /// parsers and no content-type, the FIRST declared wins.
    sole: bool,
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
            batch: None,
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

        let codec = StructuralCodec {
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
            batch: formats
                .contains(&sutra_formats::CsvCodec::NAME)
                .then(|| BatchFormats {
                    parsers: vec![std::sync::Arc::new(sutra_formats::CsvCodec::default())],
                    sole: formats.len() == 1,
                }),
        };
        // `fixed-width` has no zero-config default, so it can only arrive through
        // `compile_with_layout` carrying its declared columns. Reaching here with it declared
        // means a caller skipped that path — fail closed rather than decode a table as a document.
        if formats.contains(&sutra_formats::FixedWidthCodec::NAME) {
            return Err(format!(
                "codec '{urn}' declares format 'fixed-width', which carries no default layout — \
                 build it with compile_with_layout so its declared columns travel with it"
            ));
        }
        Ok(codec)
    }

    /// Build the validating codec with its manifest-declared tabular parser: the `csv:` block's
    /// delimiter/header, or the `fixed-width:` block's column layout. `batch` is `None` for a
    /// codec that declares no tabular format.
    ///
    /// A fixed-width layout is additionally checked AGAINST THE SCHEMA here, which is the whole
    /// reason the layout lives in the manifest rather than being inferred: the columns are the
    /// only names a fixed-width record has, so if they disagree with the bound type's declared
    /// elements, every row would fail validation at runtime for a reason that is really a
    /// configuration mistake. Catching it at package time turns that into one clear error.
    /// (csv gets no equivalent check — its column names come from the header at RUNTIME, so there
    /// is nothing to compare at package time.)
    pub fn compile_with_layout(
        urn: &str,
        xsds: &[&[u8]],
        formats: &[&str],
        batch: Vec<std::sync::Arc<dyn PayloadCodec>>,
        declared_columns: &[&str],
    ) -> Result<StructuralCodec, LayoutCompileError> {
        let document_formats: Vec<&str> = formats
            .iter()
            .copied()
            .filter(|f| *f != sutra_formats::FixedWidthCodec::NAME)
            .collect();
        let mut codec = StructuralCodec::compile_with_formats(urn, xsds, &document_formats)
            .map_err(LayoutCompileError::Schema)?;
        codec.accepted_content_types = content_types_for(formats);
        if !batch.is_empty() {
            let tabular = batch.len();
            codec.batch = Some(BatchFormats {
                parsers: batch,
                sole: formats.len() == tabular,
            });
        }
        // Verified HERE, not by the caller: a layout that disagrees with the schema is the one
        // failure mode this format has that csv does not, and leaving the check to whoever
        // happens to build the codec would make it skippable by construction.
        if !declared_columns.is_empty() {
            codec
                .verify_flat_columns(declared_columns)
                .map_err(LayoutCompileError::Layout)?;
        }
        Ok(codec)
    }

    /// Check a DECLARED column set against the single root this codec covers: every column must
    /// be a declared element of that type, and every REQUIRED element must have a column.
    ///
    /// This is for a wire form whose field names are configuration rather than data — a
    /// fixed-width layout, where the columns are the record's only names. If they disagree with
    /// the bound type, every row fails validation at runtime for what is really a configuration
    /// mistake; checking at package time turns that into one clear error. A csv codec does NOT
    /// call this: its column names come from the header at runtime, so there is nothing to
    /// compare against here.
    ///
    /// Skipped (`Ok`) when the codec covers several roots — a flat layout cannot be attributed
    /// to one of them, and `decode_batch` already refuses that case with its own message.
    fn verify_flat_columns(&self, columns: &[&str]) -> Result<(), String> {
        if self.roots.len() != 1 {
            return Ok(());
        }
        let root = self.roots.iter().next().expect("len checked");
        let Some(validation) = &self.validation else {
            return Ok(());
        };
        let Some(schema) = validation.schema_set.schema_for_root(root) else {
            return Ok(());
        };
        let Some(declared) = schema.fields_of(root) else {
            return Ok(());
        };
        let unknown: Vec<&str> = columns
            .iter()
            .copied()
            .filter(|c| !declared.iter().any(|d| d.name == *c))
            .collect();
        if !unknown.is_empty() {
            let names: Vec<&str> = declared.iter().map(|d| d.name.as_str()).collect();
            return Err(format!(
                "fixed-width layout declares column(s) {unknown:?} that type '{root}' does not \
                 declare (it declares {names:?}) — the columns ARE the record's only field names, \
                 so every row would fail validation"
            ));
        }
        let missing: Vec<&str> = declared
            .iter()
            .filter(|d| d.occurs_min > 0 && !columns.iter().any(|c| *c == d.name))
            .map(|d| d.name.as_str())
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "fixed-width layout has no column for required element(s) {missing:?} of type \
                 '{root}' — no row could ever validate"
            ));
        }
        Ok(())
    }

    /// Whether this decode is a tabular BATCH: the inbound content-type selects the tabular
    /// parser, or there is no content-type at all and the tabular format is the only one
    /// declared. A codec that declares csv alongside xml/json still reads an XML body as one
    /// document.
    fn batch_selected(&self, content_type: Option<&str>) -> bool {
        self.batch_parser(content_type).is_some()
    }

    /// The tabular parser an inbound content-type selects: the first declared whose accepted
    /// types admit it. With no content-type at all, the first declared parser — but only when
    /// the codec declares nothing BUT tabular formats, so a codec that also serves xml/json
    /// still reads an untyped body as a document.
    fn batch_parser(
        &self,
        content_type: Option<&str>,
    ) -> Option<&std::sync::Arc<dyn PayloadCodec>> {
        let batch = self.batch.as_ref()?;
        match content_type.map(str::trim).filter(|c| !c.is_empty()) {
            Some(ct) => batch.parsers.iter().find(|p| {
                sutra_codec_spi::content_type::accepts(&p.accepted_content_types(), Some(ct))
            }),
            None => batch.sole.then(|| &batch.parsers[0]),
        }
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
        if self.batch_selected(Some(ct)) {
            return self.decode_batch(validation, body, ct);
        }
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

    /// The BATCH decode: every row of a table validated as its own instance of the declared
    /// root, in one pass, before the process sees anything (design R2).
    ///
    /// Each row is wrapped as `{Root: row}` and put through exactly the same transcode →
    /// validate → project → coerce pipeline a single document takes, so a cell gets the identical
    /// facet checking an element would — pattern, enumeration, `xs:dateTime`, numeric range —
    /// and the row-indexed path (`value[3].durationSec`) names the offending cell.
    ///
    /// Outcomes follow the design's split: an unparseable FILE is FATAL (there are no rows to
    /// speak of), while any row's violation is SOFT_ERRORS — the payload still projects and stays
    /// routable, so `<q:onValidation>` decides whether the batch is refused or triaged in-flow.
    fn decode_batch(&self, validation: &Validation, body: &[u8], ct: &str) -> DecodeResult {
        let parser = self
            .batch_parser(Some(ct))
            .expect("batch_selected checked")
            .clone();
        // A csv codec covers ONE root: a table carries no root element to disambiguate with.
        let root = match self.roots.len() {
            1 => self.roots.iter().next().expect("len checked").clone(),
            n => {
                return fatal_decode(
                    &format!(
                        "codec '{}' declares {n} root elements, so a tabular body cannot be typed                          — a csv row carries no root element to select one. Bind one root per csv                          codec.",
                        self.urn
                    ),
                    ct,
                )
            }
        };
        let parsed = parser.decode(body, Some(ct));
        if parsed.outcome == DecodeOutcome::Fatal {
            return DecodeResult::fatal(parsed.issues, ct);
        }
        let Some(CodecValue::Json(serde_json::Value::Array(rows))) = parsed.payload else {
            return fatal_decode("tabular body did not parse to rows", ct);
        };

        let schema = validation
            .schema_set
            .schema_for_root(&root)
            .or_else(|| validation.schema_set.schemas().first());
        // Elements the type declares OPTIONAL. A tabular row has a cell for every column whether
        // or not it carries a value, so an empty cell must read as ABSENT for these — otherwise
        // `minOccurs="0"` is unusable with any tabular format: an empty optional column would
        // emit `<x></x>` and fail the element's own facets (an enumeration rejects `''`), and
        // every optional column would have to be populated in every row. A REQUIRED element with
        // an empty cell is left alone: that is a genuine data error and must still be reported.
        let optional: BTreeSet<String> = schema
            .and_then(|s| s.fields_of(&root))
            .map(|fields| {
                fields
                    .into_iter()
                    .filter(|f| f.occurs_min == 0)
                    .map(|f| f.name)
                    .collect()
            })
            .unwrap_or_default();
        let mut issues: Vec<ValidationIssue> = parsed.issues;
        let mut projected_rows: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
        for (index, row) in rows.into_iter().enumerate() {
            let row = match row {
                serde_json::Value::Object(mut map) => {
                    map.retain(|k, v| {
                        !(optional.contains(k) && v.as_str().is_some_and(str::is_empty))
                    });
                    serde_json::Value::Object(map)
                }
                other => other,
            };
            let mut document = serde_json::Map::new();
            document.insert(root.clone(), row);
            let xml_bytes = match self.transcode(validation, &serde_json::Value::Object(document)) {
                Ok(bytes) => bytes,
                Err(message) => {
                    issues.push(row_issue(index, "", &message));
                    continue;
                }
            };
            if let Some(schema) = schema {
                match schema.validate(&xml_bytes) {
                    Ok(violations) => {
                        for v in &violations {
                            let mut issue = to_issue(v.diagnostic(DiagnosticProfile::MODULE_CODEC));
                            issue.path = row_path(index, &issue.path);
                            issues.push(issue);
                        }
                    }
                    // A row that cannot even be read as a document is that ROW's failure, not the
                    // file's — the rest of the batch is still reported on.
                    Err(document_error) => {
                        issues.push(row_issue(index, "", &document_error.to_string()));
                        continue;
                    }
                }
            }
            let projected = XmlCodec.decode(&xml_bytes, Some("application/xml"));
            match projected.payload {
                Some(CodecValue::Json(row_json)) => {
                    projected_rows.push(self.coerce(row_json, None))
                }
                _ => issues.push(row_issue(index, "", "row did not project")),
            }
        }

        let outcome = if issues.is_empty() {
            DecodeOutcome::Ok
        } else {
            DecodeOutcome::SoftErrors
        };
        // An array root projects under `value` — the same shape the JSON-schema path produces, so
        // a flow reads `payload.value[0].<field>` whichever schema kind typed the table.
        let mut payload = serde_json::Map::new();
        payload.insert(
            BATCH_ROOT_KEY.to_string(),
            serde_json::Value::Array(projected_rows),
        );
        DecodeResult {
            outcome,
            payload: Some(CodecValue::Json(serde_json::Value::Object(payload))),
            issues,
            content_type: ct.to_string(),
            message_type: Some(root),
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

/// Why `compile_with_layout` refused. The two are NOT interchangeable, and conflating them is
/// how a layout fault once vanished: a caller that falls back to a shape-only codec on an
/// unsupported XSD must NOT do the same for a bad layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayoutCompileError {
    /// The XSD set is outside the supported subset. A caller may legitimately fall back to the
    /// shape-only build: that is a deployment which used to load and still should, with a
    /// narrower guarantee.
    Schema(String),
    /// The declared column layout disagrees with the schema. A configuration fault with exactly
    /// one correct outcome — refuse — because every row of every upload would fail after it.
    Layout(String),
}

impl std::fmt::Display for LayoutCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayoutCompileError::Schema(m) | LayoutCompileError::Layout(m) => f.write_str(m),
        }
    }
}

/// The key an array (batch) root projects under, so `payload.value[0].field` navigates a table
/// the same way whichever schema kind typed it.
pub const BATCH_ROOT_KEY: &str = "value";

/// A row-indexed path: `value[3].durationSec`, or `value[3]` for a whole-row failure.
///
/// The XSD validator reports a POSITION (`line 1:362`) into the document it validated — which,
/// for a batch, is a one-line XML fragment this codec synthesised, so the offset means nothing to
/// whoever sent the file. A positional path is therefore dropped rather than appended: the row
/// index is the part that locates the problem, and the message already names the element.
fn row_path(index: usize, path: &str) -> String {
    let path = path.trim().trim_start_matches('/').replace('/', ".");
    if path.is_empty() || is_positional(&path) {
        format!("{BATCH_ROOT_KEY}[{index}]")
    } else {
        format!("{BATCH_ROOT_KEY}[{index}].{path}")
    }
}

/// A `line <n>:<col>` document position rather than a field path.
fn is_positional(path: &str) -> bool {
    path.starts_with("line ") && path.contains(':')
}

fn row_issue(index: usize, path: &str, message: &str) -> ValidationIssue {
    ValidationIssue::error(
        sutra_codec_spi::codes::RUNTIME_CODEC_DECODE_FAILED,
        &row_path(index, path),
        message.to_string(),
    )
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
            "csv" => {
                cts.push("text/csv".to_string());
                cts.push("application/csv".to_string());
            }
            "fixed-width" => {
                cts.push("text/plain".to_string());
                cts.push("application/x-fixed-width".to_string());
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
