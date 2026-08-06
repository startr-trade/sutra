//! The JSON-schema half of the schema-codec tier — the
//! [`JsonNodeFormat`], [`JsonSchema`], [`json_schema_shape`], and [`JsonSchemaCodec`] types.
//!
//! - [`JsonNodeFormat`] — a [`MessageFormat`] parsing bytes to a `serde_json` tree
//!   (malformed / empty ⇒ FATAL, mirroring the `json` builtin).
//! - [`JsonSchema`] — a [`MessageSchema`] that validates a parsed tree and projects it to a
//!   FEEL-navigable map (scalars keep their JSON types). Schema violations are `SOFT_ERRORS`
//!   with `SUTRA.PARSE.JSON_SCHEMA.SCHEMA_VIOLATION`. The validation surface covers the
//!   keywords the codec test suite exercises — the
//!   `type` / `required` / `properties` / `items` keywords — no invented scope.
//! - [`json_schema_shape`] — the `JsonSchemaShape.of` navigation-shape introspection
//!   (open-by-default; only an explicit `additionalProperties: false` closes a container).
//! - [`JsonSchemaCodec`] — a multi-schema module codec (validate-first-pass type resolution).

use std::collections::BTreeSet;
use std::sync::Arc;

use sutra_codec_spi::codec::PayloadCodec;
use sutra_codec_spi::codes;
use sutra_codec_spi::issue::{IssueSeverity, ValidationIssue};
use sutra_codec_spi::result::{CodecValue, DecodeOutcome, DecodeResult};
use sutra_codec_spi::schema::{
    FormatParse, MessageFormat, MessageSchema, SchemaBoundCodec, SchemaKind,
};
use sutra_codec_spi::shape::{SchemaShape, ShapeFieldType};
use sutra_xsd::{Builtin, FieldDecl, FieldFacets, FieldShape, WILDCARD_FIELD};

// ---- JsonNodeFormat --------------------------------------------------------------------------

/// The `json` [`MessageFormat`]: bytes → a `serde_json` tree; malformed / empty is FATAL.
#[derive(Default)]
pub struct JsonNodeFormat;

impl MessageFormat for JsonNodeFormat {
    fn name(&self) -> &str {
        "json"
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec![
            "application/json".to_string(),
            "application/*+json".to_string(),
        ]
    }

    fn parse(&self, body: &[u8], content_type: Option<&str>) -> FormatParse {
        let ct = content_type.unwrap_or("application/json");
        if body.is_empty() {
            return FormatParse::fatal(
                vec![ValidationIssue::error(
                    codes::PARSE_JSON_PARSE_ERROR,
                    "",
                    "JSON body is empty",
                )],
                ct,
            );
        }
        match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(value) => FormatParse::ok(value, ct),
            Err(e) => FormatParse::fatal(
                vec![ValidationIssue::error(
                    codes::PARSE_JSON_PARSE_ERROR,
                    "",
                    format!(
                        "JSON parse failed: {}",
                        e.to_string().replace(['\n', '\r'], " ")
                    ),
                )],
                ct,
            ),
        }
    }

    fn encode_tree(
        &self,
        tree: &serde_json::Value,
        _content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        serde_json::to_vec(tree).map_err(|e| e.to_string().replace(['\n', '\r'], " "))
    }
}

// ---- JsonSchema (MessageSchema) --------------------------------------------------------------

/// A compiled JSON Schema bound to a message type — the `JSON_SCHEMA` half of a typed codec.
pub struct JsonSchema {
    name: String,
    message_type: Option<String>,
    document: serde_json::Value,
    shape: SchemaShape,
}

impl JsonSchema {
    /// Compile a schema from JSON-schema bytes. `Err` (a compile-time
    /// rejection) when the schema document is not valid JSON.
    pub fn compile(
        name: &str,
        schema_json: &[u8],
        message_type: Option<&str>,
    ) -> Result<JsonSchema, String> {
        if name.trim().is_empty() {
            return Err("schema name is required".to_string());
        }
        let document: serde_json::Value = serde_json::from_slice(schema_json)
            .map_err(|e| format!("invalid JSON-schema '{name}': {e}"))?;
        if document.is_null() {
            return Err(format!("invalid JSON-schema '{name}': empty document"));
        }
        let shape = json_schema_shape(&document);
        let message_type = message_type
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string);
        Ok(JsonSchema {
            name: name.to_string(),
            message_type,
            document,
            shape,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The navigation shape (single-type schema — the requested type is ignored).
    pub fn shape(&self) -> &SchemaShape {
        &self.shape
    }

    /// This schema's declared fields — see [`json_schema_fields`] for the vocabulary and its
    /// limits. Single-type schema, so no message type is taken.
    pub fn fields(&self) -> Option<Vec<FieldDecl>> {
        json_schema_fields(&self.document)
    }
}

impl MessageSchema for JsonSchema {
    fn kind(&self) -> SchemaKind {
        SchemaKind::JsonSchema
    }

    fn message_type(&self) -> Option<String> {
        self.message_type.clone()
    }

    fn shape_of(&self, _message_type: &str) -> Option<SchemaShape> {
        Some(self.shape.clone())
    }

    fn validate_and_project(&self, tree: &serde_json::Value, content_type: &str) -> DecodeResult {
        let mut issues = Vec::new();
        validate_node(&self.document, tree, "", &mut issues);
        let projected = project_to_map(tree);
        let payload = CodecValue::Json(projected);
        let result = if issues.is_empty() {
            DecodeResult::ok(payload, content_type)
        } else {
            DecodeResult::soft_errors(payload, issues, content_type)
        };
        match &self.message_type {
            Some(mt) => result.with_message_type(mt),
            None => result,
        }
    }

    fn project_to_tree(
        &self,
        payload: &serde_json::Value,
        _content_type: &str,
    ) -> Result<serde_json::Value, String> {
        // Object root projects as itself; a `{value: …}` wrapper unwraps back to the scalar.
        Ok(match payload {
            serde_json::Value::Object(map) if map.len() == 1 && map.contains_key("value") => {
                map.get("value").cloned().unwrap_or(serde_json::Value::Null)
            }
            other => other.clone(),
        })
    }
}

/// Project a parse tree to a FEEL-navigable map — object root → itself; any other root wrapped
/// under `value` (`JsonSchema.toMap`).
fn project_to_map(node: &serde_json::Value) -> serde_json::Value {
    if node.is_object() {
        return node.clone();
    }
    let mut wrap = serde_json::Map::new();
    let inner = if node.is_null() {
        serde_json::Value::Null
    } else {
        node.clone()
    };
    wrap.insert("value".to_string(), inner);
    serde_json::Value::Object(wrap)
}

/// Recursive validation over the `type` / `required` / `properties` / `items` keyword subset
/// the codec test suite exercises. Collects every violation; each is one
/// `SUTRA.PARSE.JSON_SCHEMA.SCHEMA_VIOLATION` at its instance location.
fn validate_node(
    schema: &serde_json::Value,
    instance: &serde_json::Value,
    location: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let serde_json::Value::Object(schema_obj) = schema else {
        return; // `true`/bare schema — accept anything.
    };

    // `type` keyword.
    if let Some(declared) = schema_obj.get("type").and_then(|t| t.as_str()) {
        if !instance_matches_type(instance, declared) {
            issues.push(violation(
                location,
                format!(
                    "{}: {} found, {} expected",
                    display_location(location),
                    json_type_name(instance),
                    declared
                ),
            ));
            // A type mismatch means the deeper keywords cannot apply meaningfully.
            return;
        }
    }

    // `required` keyword — only meaningful over an object.
    if let (Some(required), serde_json::Value::Object(map)) = (
        schema_obj.get("required").and_then(|r| r.as_array()),
        instance,
    ) {
        for name in required.iter().filter_map(|n| n.as_str()) {
            if !map.contains_key(name) {
                issues.push(violation(
                    location,
                    format!(
                        "{}: required property '{name}' not found",
                        display_location(location)
                    ),
                ));
            }
        }
    }

    // `properties` keyword — validate the present properties.
    if let (Some(serde_json::Value::Object(props)), serde_json::Value::Object(map)) =
        (schema_obj.get("properties"), instance)
    {
        for (name, sub_schema) in props {
            if let Some(child) = map.get(name) {
                validate_node(sub_schema, child, &join(location, name), issues);
            }
        }
    }

    // `items` keyword — validate each array element.
    if let (Some(items_schema), serde_json::Value::Array(items)) =
        (schema_obj.get("items"), instance)
    {
        for (i, item) in items.iter().enumerate() {
            validate_node(items_schema, item, &join(location, &i.to_string()), issues);
        }
    }
}

fn instance_matches_type(instance: &serde_json::Value, declared: &str) -> bool {
    match declared {
        "string" => instance.is_string(),
        "number" => instance.is_number(),
        "integer" => instance.is_i64() || instance.is_u64(),
        "boolean" => instance.is_boolean(),
        "object" => instance.is_object(),
        "array" => instance.is_array(),
        "null" => instance.is_null(),
        _ => true, // an unmodelled type name accepts anything.
    }
}

fn json_type_name(node: &serde_json::Value) -> &'static str {
    match node {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn violation(location: &str, message: String) -> ValidationIssue {
    ValidationIssue {
        code: codes::PARSE_JSON_SCHEMA_VIOLATION.to_string(),
        severity: IssueSeverity::Error,
        path: location.to_string(),
        message,
        value: None,
    }
}

fn display_location(location: &str) -> String {
    if location.is_empty() {
        "$".to_string()
    } else {
        format!("${location}")
    }
}

fn join(location: &str, segment: &str) -> String {
    format!("{location}/{segment}")
}

// ---- JsonSchemaShape -------------------------------------------------------------------------

/// Introspect a parsed JSON-schema document into a navigation [`SchemaShape`] — the
/// `JsonSchemaShape.of` walk. Conservative: a container is closed only when it explicitly
/// declares `additionalProperties: false`; an un-modellable node is `Any` / open.
pub fn json_schema_shape(schema: &serde_json::Value) -> SchemaShape {
    walk_shape(schema, "", SchemaShape::default())
}

fn walk_shape(node: &serde_json::Value, prefix: &str, mut shape: SchemaShape) -> SchemaShape {
    let props = node.get("properties");
    let Some(serde_json::Value::Object(props)) = props else {
        // Not an introspectable object here — the child set is unknown, so it is open.
        return shape.open(prefix);
    };
    if !declares_closed(node) {
        shape = shape.open(prefix);
    }
    for (name, child) in props {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let ft = shape_field_type(child);
        shape = shape.path(&path, ft);
        if ft == ShapeFieldType::Object {
            shape = walk_shape(child, &path, shape);
        }
    }
    shape
}

/// Closed only for an explicit `additionalProperties: false` (omitted / schema-valued = open).
fn declares_closed(object_schema: &serde_json::Value) -> bool {
    matches!(
        object_schema.get("additionalProperties"),
        Some(serde_json::Value::Bool(false))
    )
}

fn shape_field_type(node: &serde_json::Value) -> ShapeFieldType {
    let type_str = match node.get("type") {
        Some(serde_json::Value::Array(arr)) => arr.first().and_then(|t| t.as_str()),
        Some(serde_json::Value::String(s)) => Some(s.as_str()),
        _ => None,
    };
    let Some(type_str) = type_str else {
        return if node.get("properties").is_some() {
            ShapeFieldType::Object
        } else {
            ShapeFieldType::Any
        };
    };
    match type_str {
        "string" => ShapeFieldType::String,
        "number" | "integer" => ShapeFieldType::Number,
        "boolean" => ShapeFieldType::Boolean,
        "object" => ShapeFieldType::Object,
        "array" => ShapeFieldType::Array,
        _ => ShapeFieldType::Any,
    }
}

// ---- Declared-field enumeration ---------------------------------------------------------------

/// Enumerate a JSON-schema object node's declared fields in the same [`FieldDecl`] vocabulary
/// [`sutra_xsd::Schema::fields_of`] produces, so a consumer that projects a declared structure
/// (the typed-column data store, and the lint over it) speaks to both schema tiers through one
/// type rather than two.
///
/// What maps, and what does not:
///
/// | JSON Schema | [`FieldDecl`] |
/// |---|---|
/// | `properties.<name>` | one field per entry |
/// | `required: [...]` | `occurs_min` 1, else 0 |
/// | `type: string \| number \| integer \| boolean` | [`FieldShape::Scalar`] |
/// | `format: date \| date-time \| time` (on a string) | the temporal [`Builtin`] |
/// | `type: object`, or a node with its own `properties` | [`FieldShape::Complex`] |
/// | `type: array` | [`FieldShape::Complex`] with `occurs_max: None` (repeated) |
/// | no usable `type` | [`FieldShape::Any`] |
/// | `additionalProperties` (`true` or a schema) / `patternProperties` | a trailing [`FieldShape::Any`] field named [`WILDCARD_FIELD`] |
///
/// **No facets map.** Every field carries [`FieldFacets::default`]: the JSON-Schema tier has no
/// length/precision equivalent this crate retains, so lint checks over a JSON-Schema-declared
/// structure are correspondingly weaker than over an XSD one — a stated limit of the projection
/// design, not an oversight to be papered over with invented precision.
///
/// **Ordering is by property name**, not by document order. A JSON object is an unordered set of
/// members (RFC 8259), so authoring order carries no meaning and — depending on whether
/// `serde_json`'s `preserve_order` feature is active in a given build — is not even observable.
/// Sorting is therefore the only order reproducible across every build of every consumer, which
/// is what a projection's column list needs. The XSD back-end reports authored declared order
/// instead, because an XML sequence genuinely is ordered.
///
/// The open-content entry, when present, is appended last rather than sorted into place.
///
/// **Openness** follows the projection design rather than [`json_schema_shape`]'s navigation
/// posture: an *omitted* `additionalProperties` is treated as closed here (the projected store
/// rejects an undeclared field at write time, so the structure is closed in practice), where the
/// navigation shape treats it as open so an unknown path never false-errors. Two questions, two
/// answers.
///
/// `None` when the node declares no `properties` object — there is nothing to enumerate.
pub fn json_schema_fields(schema: &serde_json::Value) -> Option<Vec<FieldDecl>> {
    let serde_json::Value::Object(properties) = schema.get("properties")? else {
        return None;
    };
    let required: BTreeSet<&str> = schema
        .get("required")
        .and_then(|r| r.as_array())
        .map(|names| names.iter().filter_map(|n| n.as_str()).collect())
        .unwrap_or_default();

    let mut fields = Vec::with_capacity(properties.len() + 1);
    for (name, node) in properties {
        let (shape, repeated) = json_field_shape(node);
        fields.push(FieldDecl {
            name: name.clone(),
            // JSON has no attribute/element distinction.
            is_attribute: false,
            occurs_min: u32::from(required.contains(name.as_str())),
            occurs_max: if repeated { None } else { Some(1) },
            // JSON Schema's `oneOf`/`anyOf` are outside the validated keyword subset, so no
            // field is ever reported as a choice branch.
            in_choice: false,
            shape,
        });
    }
    // Reproducible across builds regardless of `serde_json`'s map implementation.
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    if admits_undeclared_properties(schema) {
        fields.push(FieldDecl {
            name: WILDCARD_FIELD.to_string(),
            is_attribute: false,
            occurs_min: 0,
            occurs_max: None,
            in_choice: false,
            shape: FieldShape::Any,
        });
    }
    Some(fields)
}

/// Whether the object schema admits properties it does not declare — an explicit
/// `additionalProperties` that is not `false` (a `true` or a sub-schema both permit extras), or
/// any `patternProperties`. An omitted `additionalProperties` is closed here; see
/// [`json_schema_fields`].
fn admits_undeclared_properties(object_schema: &serde_json::Value) -> bool {
    let additional = match object_schema.get("additionalProperties") {
        None | Some(serde_json::Value::Bool(false)) => false,
        Some(_) => true,
    };
    additional || object_schema.get("patternProperties").is_some()
}

/// One property node's declared shape, plus whether it is repeated (a JSON array).
fn json_field_shape(node: &serde_json::Value) -> (FieldShape, bool) {
    let declared = match node.get("type") {
        Some(serde_json::Value::Array(arr)) => arr.first().and_then(|t| t.as_str()),
        Some(serde_json::Value::String(s)) => Some(s.as_str()),
        _ => None,
    };
    let scalar = |builtin| {
        (
            FieldShape::Scalar {
                builtin,
                facets: FieldFacets::default(),
            },
            false,
        )
    };
    match declared {
        Some("string") => scalar(string_builtin(node)),
        Some("number") => scalar(Builtin::Decimal),
        Some("integer") => scalar(Builtin::Integer),
        Some("boolean") => scalar(Builtin::Boolean),
        Some("object") => (FieldShape::Complex, false),
        // An array is repeated by definition; its element shape is irrelevant to a rule that
        // rejects repetition outright.
        Some("array") => (FieldShape::Complex, true),
        // `null`, an unmodelled type name, or no type at all: unconstrained, unless the node
        // introspects as an object by carrying its own `properties`.
        _ => {
            if node.get("properties").is_some() {
                (FieldShape::Complex, false)
            } else {
                (FieldShape::Any, false)
            }
        }
    }
}

/// The RFC 3339 string formats that carry a temporal builtin; every other `format` (and none at
/// all) reads as plain text.
fn string_builtin(node: &serde_json::Value) -> Builtin {
    match node.get("format").and_then(|f| f.as_str()) {
        Some("date") => Builtin::Date,
        Some("date-time") => Builtin::DateTime,
        Some("time") => Builtin::Time,
        _ => Builtin::String,
    }
}

// ---- JsonSchemaCodec -------------------------------------------------------------------------

/// A module JSON-Schema codec: the `json` format bound to one or more compiled schemas, each
/// contributing one message type (its file base name). Decode resolves the type by validating
/// against each schema and taking the first that passes; none clean ⇒ the first schema's
/// `SOFT_ERRORS` projection (still routable).
pub struct JsonSchemaCodec {
    name: String,
    format: JsonNodeFormat,
    schemas: Vec<JsonSchema>,
}

impl JsonSchemaCodec {
    /// Build a codec over one or more compiled schemas.
    pub fn of(name: &str, schemas: Vec<JsonSchema>) -> Result<JsonSchemaCodec, String> {
        if name.trim().is_empty() {
            return Err("codec name is required".to_string());
        }
        if schemas.is_empty() {
            return Err("at least one JSON schema is required".to_string());
        }
        Ok(JsonSchemaCodec {
            name: name.to_string(),
            format: JsonNodeFormat,
            schemas,
        })
    }

    pub fn shape_of(&self, message_type: &str) -> Option<SchemaShape> {
        self.schemas
            .iter()
            .find(|s| s.message_type().as_deref() == Some(message_type))
            .map(|s| s.shape().clone())
    }
}

impl PayloadCodec for JsonSchemaCodec {
    fn name(&self) -> &str {
        &self.name
    }

    fn accepted_content_types(&self) -> Vec<String> {
        self.format.accepted_content_types()
    }

    /// The declared message types (each schema's non-blank message type).
    fn declared_message_types(&self) -> Vec<String> {
        self.schemas
            .iter()
            .filter_map(JsonSchema::message_type)
            .collect()
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let parsed = self.format.parse(body, content_type);
        let Some(tree) = parsed.tree else {
            return DecodeResult::fatal(parsed.issues, &parsed.content_type);
        };
        let mut first: Option<DecodeResult> = None;
        for schema in &self.schemas {
            let r = schema.validate_and_project(&tree, &parsed.content_type);
            if r.outcome == DecodeOutcome::Ok {
                return r;
            }
            if first.is_none() {
                first = Some(r);
            }
        }
        first.expect("codec has at least one schema")
    }

    fn encode(&self, payload: &CodecValue, content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let CodecValue::Json(value) = payload else {
            return Err("json-schema codec encode requires a map payload".to_string());
        };
        if self.schemas.len() == 1 {
            let tree = self.schemas[0].project_to_tree(value, content_type.unwrap_or(""))?;
            return self.format.encode_tree(&tree, content_type);
        }
        Err(format!(
            "multi-type JSON codec '{}' cannot encode a reply without an explicit outbound message type",
            self.name
        ))
    }
}

/// Compose the `json` format with a single [`JsonSchema`] into a [`SchemaBoundCodec`] — the
/// format-plus-schema pairing the codec tests use.
pub fn json_schema_bound_codec(name: &str, schema: JsonSchema) -> SchemaBoundCodec {
    SchemaBoundCodec::new(name, Arc::new(JsonNodeFormat), Arc::new(schema))
}
