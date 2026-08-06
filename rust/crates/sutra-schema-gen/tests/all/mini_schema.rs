//! End-to-end mini-schema generation. The default (slim) emission carries the
//! data-driven decode tables (`TypeDecl` / `FieldDecl` / enum value tables / pooled
//! facets) plus the shape metadata and the cross-schema registry/lib files; a
//! separate `--full` test pins the opt-in typed model. Assertions run on the raw
//! (pre-rustfmt) emission — the surface the generator fixes.

use std::path::Path;

use sutra_schema_gen::{generate_all, generate_all_with_mode, Mode};

fn mini_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/mini")
}

#[test]
fn slim_generates_decode_tables_registry_and_lib() {
    let files = generate_all(&mini_dir()).expect("generation succeeds");
    let by_name = |name: &str| -> &str {
        &files
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("{name} generated"))
            .content
    };

    let module = by_name("test001v01.rs");

    // Slim carries data tables, not typed structs.
    assert!(!module.contains("pub struct Document"));
    assert!(!module.contains("pub fn decode_body"));
    assert!(module.contains("#![allow(non_upper_case_globals)]"));
    assert!(module.contains("pub static TYPES: &[support::TypeDecl] = &["));
    // Root / message-type constants the registry dispatches on.
    assert!(module.contains(
        "pub const NAMESPACE: &str = \"urn:iso:std:iso:20022:tech:xsd:test.001.001.01\";"
    ));
    assert!(module.contains("pub const MESSAGE_TYPE: &str = \"test.001.001.01\";"));
    assert!(module.contains("pub const ROOT_ELEMENT: &str = \"Document\";"));
    assert!(module.contains("pub const ROOT_TYPE: &str = \"Document\";"));
    // TypeDecl rows: nested child, choice content model, repeated element.
    assert!(module.contains("name: \"Document\","));
    assert!(module.contains("content: support::Content::Nested(\"Body\")"));
    assert!(module.contains("name: \"PartyChoice\",\n        is_choice: true,"));
    assert!(module.contains("xml: \"Pick\"") && module.contains("repeated: true"));
    // simpleContent value + required attribute idiom (Amount/Ccy).
    assert!(module.contains(
        "xml: \"value\", slot: support::Slot::Value, content: support::Content::Scalar(support::ScalarKind::Decimal)"
    ));
    assert!(module.contains("xml: \"Ccy\", slot: support::Slot::Attribute"));
    // Enum value table (xml_value → canonical_name), projected as a code string.
    assert!(module.contains("static E_CodeType: &[(&str, &str)] = &["));
    assert!(module.contains("(\"DEBT\", \"DEBT\"),"));
    assert!(module.contains("content: support::Content::Enum(E_CodeType)"));
    // Pooled facets: string lengths, pattern, decimal digits/min.
    assert!(module.contains("min_len: Some(1),") && module.contains("max_len: Some(35),"));
    assert!(module.contains("patterns: &[\"[A-Z]{3,3}\"]"));
    assert!(module.contains("total_digits: Some(18),"));
    assert!(module.contains("fraction_digits: Some(5),"));
    // Shape metadata is retained unchanged.
    assert!(module.contains(
        "support::FieldShape { name: \"MsgId\", kind: support::FieldKind::Scalar, type_name: \"string\" }"
    ));
    assert!(module
        .contains("pub fn shape_of(type_name: &str) -> Option<&'static [support::FieldShape]>"));

    // Registry: the data-driven Schema table + generic-decoder dispatch (no AnyDocument).
    let registry = by_name("registry.rs");
    assert!(!registry.contains("AnyDocument"));
    assert!(registry.contains("struct Schema {"));
    assert!(registry.contains("namespace: crate::test001v01::NAMESPACE"));
    assert!(registry.contains("support::decode_document("));
    assert!(registry.contains("UnknownNamespace { namespace: String }"));

    // `support.rs` is hand-maintained and never emitted; `lib.rs` still declares it.
    let lib = by_name("lib.rs");
    assert!(lib.contains("pub mod test001v01;"));
    assert!(lib.contains("pub mod support;"));
    assert!(!files.iter().any(|f| f.name == "support.rs"));
}

#[test]
fn full_mode_emits_the_typed_model() {
    // The `--full` opt-in still emits the typed model (structs + decoder +
    // projection) — kept available for anyone who wants typed access.
    let files = generate_all_with_mode(&mini_dir(), Mode::Full).expect("full generation succeeds");
    let by_name = |name: &str| -> &str {
        &files
            .iter()
            .find(|f| f.name == name)
            .unwrap_or_else(|| panic!("{name} generated"))
            .content
    };
    let module = by_name("test001v01.rs");
    assert!(module.contains("pub struct Document {"));
    assert!(module.contains("pub struct Body {"));
    assert!(module.contains("pub enum CodeType {"));
    assert!(module.contains("pub fn canonical_name(self) -> &'static str"));
    assert!(module.contains("pub body: Option<Box<Body>>,"));
    assert!(module.contains("pub pick: Vec<PartyChoice>,"));
    assert!(module.contains("pub fn decode_body(node: &support::XmlNode"));
    assert!(module.contains("support::check_choice(node, ELEMS, path, ctx);"));
    assert!(module.contains("support::AttrSpec { name: \"Ccy\", required: true }"));
    assert!(module.contains("pub fn map_document(v: &Document) -> support::MapValue"));
    assert!(module.contains("\"DEBT\" => Some(Self::DEBT),"));

    let registry = by_name("registry.rs");
    assert!(registry.contains("Test001v01(Box<crate::test001v01::Document>)"));
    assert!(registry.contains("UnknownNamespace { namespace: String }"));
}

#[test]
fn generated_body_comments_are_contract_neutral() {
    // The generated body comments carry contract-first wording — no foreign class names.
    let files = generate_all(&mini_dir()).expect("generation succeeds");
    for f in &files {
        for banned in [
            "Java enum-constant",
            "JaxbToMap",
            "JaxbSchemaShape",
            "StAX",
            "MxMessage",
        ] {
            assert!(
                !f.content.contains(banned),
                "{} still mentions {banned}",
                f.name
            );
        }
    }
}
