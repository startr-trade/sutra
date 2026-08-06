//! Port of `JsonSchemaShapeTest` (7) — introspecting a JSON-schema into a `SchemaShape` for
//! navigation analysis, honouring JSON-schema's open-by-default rule (only an explicit
//! `additionalProperties: false` closes a container).

use sutra_codec_schema::{json_schema_shape, JsonSchema};
use sutra_codec_spi::{PathResolution, SchemaShape, ShapeFieldType};

fn shape_of(json: &str) -> SchemaShape {
    json_schema_shape(&serde_json::from_str(json).expect("schema is JSON"))
}

#[test]
fn declares_scalar_paths_with_types() {
    let s = shape_of(
        r#"{ "type":"object", "properties": {
              "id": {"type":"string"}, "amount": {"type":"number"}, "ok": {"type":"boolean"} } }"#,
    );

    assert_eq!(s.type_of("id"), Some(ShapeFieldType::String));
    assert_eq!(s.type_of("amount"), Some(ShapeFieldType::Number));
    assert_eq!(s.type_of("ok"), Some(ShapeFieldType::Boolean));
    assert_eq!(
        s.resolve("amount"),
        PathResolution::DeclaredField(ShapeFieldType::Number)
    );
}

#[test]
fn open_by_default_makes_unknown_fields_unverifiable_not_errors() {
    // additionalProperties omitted ⇒ open ⇒ an unknown field is a warn (Unverifiable).
    let s = shape_of(r#"{ "type":"object", "properties": { "id": {"type":"string"} } }"#);

    assert!(s.open_at(""));
    assert!(matches!(s.resolve("typo"), PathResolution::Unverifiable(_)));
}

#[test]
fn additional_properties_false_makes_unknown_field_a_provable_error() {
    let s = shape_of(
        r#"{ "type":"object", "additionalProperties": false, "properties": { "id": {"type":"string"} } }"#,
    );

    assert!(!s.open_at(""));
    assert_eq!(
        s.resolve("typo"),
        PathResolution::UnknownInClosed {
            container: String::new(),
            path: "typo".to_string()
        }
    );
    assert_eq!(
        s.resolve("id"),
        PathResolution::DeclaredField(ShapeFieldType::String)
    );
}

#[test]
fn descends_into_nested_closed_objects() {
    let s = shape_of(
        r#"{ "type":"object", "additionalProperties": false, "properties": {
              "addr": { "type":"object", "additionalProperties": false,
                        "properties": { "city": {"type":"string"} } } } }"#,
    );

    assert_eq!(s.type_of("addr"), Some(ShapeFieldType::Object));
    assert_eq!(
        s.resolve("addr.city"),
        PathResolution::DeclaredField(ShapeFieldType::String)
    );
    assert_eq!(
        s.resolve("addr.typo"),
        PathResolution::UnknownInClosed {
            container: "addr".to_string(),
            path: "addr.typo".to_string()
        }
    );
}

#[test]
fn arrays_and_unmodellable_nodes_become_array_or_any() {
    let s = shape_of(
        r##"{ "type":"object", "properties": {
              "lines": {"type":"array"},
              "ref": {"$ref":"#/$defs/Whatever"},
              "loose": {} } }"##,
    );

    assert_eq!(s.type_of("lines"), Some(ShapeFieldType::Array));
    assert_eq!(s.type_of("ref"), Some(ShapeFieldType::Any)); // bare $ref not modelled — ANY
    assert_eq!(s.type_of("loose"), Some(ShapeFieldType::Any));
}

#[test]
fn numeric_compatibility_distinguishes_number_from_string() {
    let s = shape_of(
        r#"{ "type":"object", "properties": { "amount": {"type":"number"}, "id": {"type":"string"} } }"#,
    );

    assert!(s.type_of("amount").unwrap().is_numeric_compatible());
    assert!(!s.type_of("id").unwrap().is_numeric_compatible());
}

#[test]
fn exposed_through_json_schema_shape_of() {
    let schema = JsonSchema::compile(
        "order",
        r#"{ "type":"object", "additionalProperties": false, "properties": { "id": {"type":"string"} } }"#
            .as_bytes(),
        Some("order"),
    )
    .expect("schema compiles");

    assert!(matches!(
        schema.shape().resolve("typo"),
        PathResolution::UnknownInClosed { .. }
    ));
}
