//! `json_schema_fields` — the JSON-Schema half of the declared-field vocabulary the data-store
//! projection consumes, and the honest statement of what it cannot carry.

use sutra_codec_schema::{json_schema_fields, JsonSchema};
use sutra_xsd::{Builtin, FieldDecl, FieldFacets, FieldShape, WILDCARD_FIELD};

fn fields_of(json: &str) -> Vec<FieldDecl> {
    json_schema_fields(&serde_json::from_str(json).expect("schema is JSON"))
        .expect("schema declares properties")
}

fn field<'f>(fields: &'f [FieldDecl], name: &str) -> &'f FieldDecl {
    fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("no field '{name}'"))
}

#[test]
fn scalar_types_and_required_map_to_shapes_and_occurrences() {
    let fields = fields_of(
        r#"{ "type":"object", "additionalProperties": false,
             "required": ["id", "amount"],
             "properties": {
               "id": {"type":"string"},
               "amount": {"type":"number"},
               "count": {"type":"integer"},
               "active": {"type":"boolean"},
               "note": {"type":"string"} } }"#,
    );

    // (name, builtin, occurs_min, optional)
    let expected = [
        ("active", Builtin::Boolean, 0u32, true),
        ("amount", Builtin::Decimal, 1, false),
        ("count", Builtin::Integer, 0, true),
        ("id", Builtin::String, 1, false),
        ("note", Builtin::String, 0, true),
    ];
    assert_eq!(
        fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        expected.iter().map(|e| e.0).collect::<Vec<_>>(),
        "sorted by property name — reproducible whatever serde_json's map does"
    );
    for (name, builtin, min, optional) in expected {
        let f = field(&fields, name);
        assert_eq!(f.scalar().map(|(b, _)| b), Some(builtin), "{name}");
        assert_eq!(f.occurs_min, min, "{name}.occurs_min");
        assert_eq!(f.occurs_max, Some(1), "{name}.occurs_max");
        assert!(!f.is_repeated(), "{name}.is_repeated");
        assert_eq!(f.is_optional(), optional, "{name}.is_optional");
        assert!(!f.is_attribute, "JSON has no attributes");
    }
}

#[test]
fn the_rfc3339_string_formats_carry_a_temporal_builtin() {
    let fields = fields_of(
        r#"{ "type":"object", "additionalProperties": false, "properties": {
               "openedOn": {"type":"string", "format":"date"},
               "seenAt":   {"type":"string", "format":"date-time"},
               "cutOff":   {"type":"string", "format":"time"},
               "email":    {"type":"string", "format":"email"},
               "plain":    {"type":"string"} } }"#,
    );
    for (name, builtin) in [
        ("openedOn", Builtin::Date),
        ("seenAt", Builtin::DateTime),
        ("cutOff", Builtin::Time),
        // An unmodelled format is plain text, never a guess.
        ("email", Builtin::String),
        ("plain", Builtin::String),
    ] {
        assert_eq!(
            field(&fields, name).scalar().map(|(b, _)| b),
            Some(builtin),
            "{name}"
        );
    }
}

#[test]
fn objects_arrays_and_untyped_nodes_are_the_three_non_scalar_shapes() {
    let fields = fields_of(
        r##"{ "type":"object", "additionalProperties": false, "properties": {
               "addr":  {"type":"object", "properties": {"city": {"type":"string"}}},
               "implied": {"properties": {"city": {"type":"string"}}},
               "lines": {"type":"array", "items": {"type":"string"}},
               "ref":   {"$ref":"#/$defs/Whatever"},
               "loose": {} } }"##,
    );

    // An object is complex and is NOT descended — one level deep, like the XSD back-end.
    assert_eq!(field(&fields, "addr").shape, FieldShape::Complex);
    assert!(!fields.iter().any(|f| f.name == "city"));
    // A node with its own `properties` but no `type` still reads as an object.
    assert_eq!(field(&fields, "implied").shape, FieldShape::Complex);
    // An array is repeated by definition.
    assert!(field(&fields, "lines").is_repeated());
    assert_eq!(field(&fields, "lines").occurs_max, None);
    // Un-modellable nodes are unconstrained.
    assert_eq!(field(&fields, "ref").shape, FieldShape::Any);
    assert_eq!(field(&fields, "loose").shape, FieldShape::Any);
}

#[test]
fn open_content_is_reported_only_when_the_schema_admits_undeclared_properties() {
    let closed_explicit = r#"{ "type":"object", "additionalProperties": false,
                               "properties": {"id": {"type":"string"}} }"#;
    let closed_omitted = r#"{ "type":"object", "properties": {"id": {"type":"string"}} }"#;
    for closed in [closed_explicit, closed_omitted] {
        let fields = fields_of(closed);
        assert!(
            !fields.iter().any(|f| f.shape == FieldShape::Any),
            "closed for projection: {closed}"
        );
    }

    for open in [
        r#"{ "type":"object", "additionalProperties": true,
             "properties": {"id": {"type":"string"}} }"#,
        r#"{ "type":"object", "additionalProperties": {"type":"string"},
             "properties": {"id": {"type":"string"}} }"#,
        r#"{ "type":"object", "additionalProperties": false, "patternProperties": {"^x-": {}},
             "properties": {"id": {"type":"string"}} }"#,
    ] {
        let fields = fields_of(open);
        let wildcard = field(&fields, WILDCARD_FIELD);
        assert_eq!(wildcard.shape, FieldShape::Any, "{open}");
        assert_eq!(
            fields.last().map(|f| f.name.as_str()),
            Some(WILDCARD_FIELD),
            "reported last, after the declared properties: {open}"
        );
    }
}

/// The stated weakness, pinned so it cannot quietly become a half-truth: no facet the projection
/// could bound a column with survives the JSON-Schema tier, however the document spells it.
#[test]
fn no_facets_are_carried_and_the_gap_is_explicit() {
    let fields = fields_of(
        r#"{ "type":"object", "additionalProperties": false, "properties": {
               "code":   {"type":"string", "maxLength": 3, "minLength": 3},
               "amount": {"type":"number", "multipleOf": 0.01, "maximum": 1000},
               "status": {"type":"string", "enum": ["NEW", "DONE"]} } }"#,
    );
    for name in ["code", "amount", "status"] {
        let (_, facets) = field(&fields, name).scalar().expect("scalar");
        assert_eq!(
            facets,
            &FieldFacets::default(),
            "{name}: JSON Schema carries no projectable facets"
        );
    }
}

#[test]
fn a_schema_with_no_properties_has_nothing_to_enumerate() {
    for json in [
        r#"{ "type":"string" }"#,
        r#"{ "type":"object" }"#,
        r#"{ "properties": "not-an-object" }"#,
        "{}",
    ] {
        assert_eq!(
            json_schema_fields(&serde_json::from_str(json).unwrap()),
            None,
            "{json}"
        );
    }
}

#[test]
fn exposed_through_the_compiled_schema() {
    let schema = JsonSchema::compile(
        "account",
        r#"{ "type":"object", "additionalProperties": false, "required": ["id"],
             "properties": {"id": {"type":"string"}, "openedOn": {"type":"string","format":"date"}} }"#
            .as_bytes(),
        Some("account"),
    )
    .expect("schema compiles");

    let fields = schema.fields().expect("declares properties");
    assert_eq!(
        fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        ["id", "openedOn"],
        "sorted by property name"
    );
    assert!(!field(&fields, "id").is_optional());
    assert!(field(&fields, "openedOn").is_optional());
}
