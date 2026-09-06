//! Turning a module version's `schemas/` folder into URN-keyed typed codecs, with the
//! fail-closed `SUTRA.CONFIG.CODEC_*` / `SUTRA.CONFIG.SCHEMA.INVALID` deploy gates. XSD
//! fixtures carry an explicit `targetNamespace` (the `sutra-xsd` subset contract).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sutra_codec_schema::schema_codec_loader::{codec_urn, load, CodecLoadError};

const MODULE_NS: &str = "urn:sutra:module:demo:1.0.0";
const XSD_MANIFEST: &str = "schemaKind: xsd\nformats: [xml, json, yaml]\n";
const JSON_MANIFEST: &str = "schemaKind: json-schema\nformats: [json]\n";

const XSD_PAYMENT: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Payment"><xs:complexType><xs:sequence>
    <xs:element name="Id" type="xs:string"/>
    <xs:element name="Amount" type="xs:decimal"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"#;

const XSD_REVERSAL: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Reversal"><xs:complexType><xs:sequence>
    <xs:element name="OrigId" type="xs:string"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"#;

const JSON_SCHEMA: &str = r#"{ "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
  "required": ["id", "amount"],
  "properties": { "id": { "type": "string" }, "amount": { "type": "number" } } }"#;

// ---- self-contained temp dir -----------------------------------------------------------------

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new() -> Scratch {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sutra-wp3-loader-{}-{n}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        Scratch { root }
    }

    /// A `schemas/` directory under a fresh scratch root.
    fn schemas(&self) -> PathBuf {
        let p = self.root.join("schemas");
        fs::create_dir_all(&p).unwrap();
        p
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Create `schemas/<codec>/` with a manifest + the given `(file, content)` schema files.
fn write_codec(schemas: &Path, codec: &str, manifest: &str, files: &[(&str, &str)]) {
    let folder = schemas.join(codec);
    fs::create_dir_all(&folder).unwrap();
    fs::write(folder.join("codec-manifest.yaml"), manifest).unwrap();
    for (name, content) in files {
        fs::write(folder.join(name), content).unwrap();
    }
}

fn expect_code(schemas: &Path, expected: &str) {
    let err: CodecLoadError = match load(schemas, MODULE_NS) {
        Ok(_) => panic!("load must fail with {expected}"),
        Err(e) => e,
    };
    assert_eq!(err.code(), expected, "message: {}", err.message());
}

// ---- positive composition --------------------------------------------------------------------

#[test]
fn loads_an_xsd_codec_keyed_by_folder_name() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "payment",
        XSD_MANIFEST,
        &[("payment.xsd", XSD_PAYMENT)],
    );

    let codecs = load(&schemas, MODULE_NS).expect("loads");
    assert_eq!(codecs.len(), 1);
    let codec = &codecs[0];
    assert_eq!(codec.name(), "urn:sutra:module:demo:1.0.0:payment");
    let json = codec.decode(
        br#"{"Payment":{"Id":"INB-7","Amount":100}}"#,
        Some("application/json"),
    );
    assert_eq!(json.outcome, sutra_codec_spi::DecodeOutcome::Ok);
    assert_eq!(json.message_type.as_deref(), Some("Payment"));
}

#[test]
fn multiple_xsds_in_one_folder_compose_one_codec() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "payments",
        XSD_MANIFEST,
        &[("payment.xsd", XSD_PAYMENT), ("reversal.xsd", XSD_REVERSAL)],
    );

    let codecs = load(&schemas, MODULE_NS).expect("loads");
    assert_eq!(codecs.len(), 1);
    let mut types = codecs[0].declared_message_types();
    types.sort();
    assert_eq!(types, vec!["Payment", "Reversal"]);
}

#[test]
fn loads_a_json_schema_codec_with_the_file_base_name_as_type() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "order",
        JSON_MANIFEST,
        &[("order.json", JSON_SCHEMA)],
    );

    let codecs = load(&schemas, MODULE_NS).expect("loads");
    assert_eq!(codecs.len(), 1);
    let codec = &codecs[0];
    assert_eq!(codec.name(), "urn:sutra:module:demo:1.0.0:order");
    assert_eq!(codec.declared_message_types(), vec!["order"]);
    let ok = codec.decode(br#"{"id":"INB-7","amount":100}"#, Some("application/json"));
    assert_eq!(ok.outcome, sutra_codec_spi::DecodeOutcome::Ok);
    assert_eq!(ok.message_type.as_deref(), Some("order"));
}

#[test]
fn multiple_json_schemas_in_one_folder_make_a_multi_type_codec() {
    let schema_a = r#"{ "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
          "required": ["alpha"], "properties": { "alpha": { "type": "string" } } }"#;
    let schema_b = r#"{ "$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
          "required": ["beta"], "properties": { "beta": { "type": "string" } } }"#;
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "msgs",
        JSON_MANIFEST,
        &[("a.json", schema_a), ("b.json", schema_b)],
    );

    let codecs = load(&schemas, MODULE_NS).expect("loads");
    assert_eq!(codecs.len(), 1);
    let mut types = codecs[0].declared_message_types();
    types.sort();
    assert_eq!(types, vec!["a", "b"]);
    // Validate-first-pass resolution: a doc matching schema b resolves to type "b".
    let b = codecs[0].decode(br#"{"beta":"y"}"#, Some("application/json"));
    assert_eq!(b.outcome, sutra_codec_spi::DecodeOutcome::Ok);
    assert_eq!(b.message_type.as_deref(), Some("b"));
}

#[test]
fn xsd_and_json_schema_codecs_coexist_in_one_module() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "payment",
        XSD_MANIFEST,
        &[("payment.xsd", XSD_PAYMENT)],
    );
    write_codec(
        &schemas,
        "order",
        JSON_MANIFEST,
        &[("order.json", JSON_SCHEMA)],
    );

    let codecs = load(&schemas, MODULE_NS).expect("loads");
    let mut names: Vec<String> = codecs.iter().map(|c| c.name().to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "urn:sutra:module:demo:1.0.0:order",
            "urn:sutra:module:demo:1.0.0:payment",
        ]
    );
}

#[test]
fn formats_drive_accepted_content_types() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "payment",
        "schemaKind: xsd\nformats: [xml]\n",
        &[("payment.xsd", XSD_PAYMENT)],
    );

    let codecs = load(&schemas, MODULE_NS).expect("loads");
    let cts = codecs[0].accepted_content_types();
    assert!(cts.iter().any(|c| c == "application/xml"));
    assert!(!cts.iter().any(|c| c == "application/json"));
}

#[test]
fn absent_schemas_folder_yields_no_codecs() {
    let scratch = Scratch::new();
    let codecs = load(&scratch.root.join("nope"), MODULE_NS).expect("no error");
    assert!(codecs.is_empty());
}

#[test]
fn codec_urn_composes_namespace_and_name() {
    assert_eq!(
        codec_urn(MODULE_NS, "payment"),
        "urn:sutra:module:demo:1.0.0:payment"
    );
}

// ---- fail-closed layout / manifest / schema gates --------------------------------------------

#[test]
fn a_codec_folder_without_a_manifest_is_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    let folder = schemas.join("payment");
    fs::create_dir_all(&folder).unwrap();
    fs::write(folder.join("payment.xsd"), XSD_PAYMENT).unwrap();

    expect_code(&schemas, "SUTRA.CONFIG.CODEC_MANIFEST.MISSING");
}

#[test]
fn a_loose_file_directly_under_schemas_is_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    fs::write(schemas.join("payment.xsd"), XSD_PAYMENT).unwrap();

    expect_code(&schemas, "SUTRA.CONFIG.CODEC_LAYOUT.INVALID");
}

#[test]
fn mixing_schema_kinds_in_one_codec_folder_is_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "payment",
        XSD_MANIFEST,
        &[("payment.xsd", XSD_PAYMENT), ("extra.json", JSON_SCHEMA)],
    );

    expect_code(&schemas, "SUTRA.CONFIG.CODEC_LAYOUT.INVALID");
}

#[test]
fn an_empty_codec_folder_is_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(&schemas, "payment", XSD_MANIFEST, &[]);

    expect_code(&schemas, "SUTRA.CONFIG.CODEC_LAYOUT.INVALID");
}

#[test]
fn an_unknown_schema_kind_is_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "payment",
        "schemaKind: protobuf\nformats: [json]\n",
        &[("payment.json", JSON_SCHEMA)],
    );

    expect_code(&schemas, "SUTRA.CONFIG.CODEC_MANIFEST.INVALID");
}

#[test]
fn a_format_not_allowed_for_the_kind_is_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "order",
        "schemaKind: json-schema\nformats: [xml]\n",
        &[("order.json", JSON_SCHEMA)],
    );

    expect_code(&schemas, "SUTRA.CONFIG.CODEC_MANIFEST.INVALID");
}

#[test]
fn an_invalid_xsd_is_a_deploy_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "broken",
        XSD_MANIFEST,
        &[("broken.xsd", "<xs:schema><not-valid")],
    );

    expect_code(&schemas, "SUTRA.CONFIG.SCHEMA.INVALID");
}

#[test]
fn an_invalid_json_schema_is_a_deploy_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "broken",
        JSON_MANIFEST,
        &[("broken.json", "{ not valid json")],
    );

    expect_code(&schemas, "SUTRA.CONFIG.SCHEMA.INVALID");
}

// ---- the fixed-width layout block (design `schema-format-binding.md` R3) ---------------------

/// A three-column record whose names ARE the type's elements.
const FW_XSD: &str = r#"<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           targetNamespace="urn:ex" xmlns="urn:ex" elementFormDefault="qualified">
  <xs:element name="Rec"><xs:complexType><xs:sequence>
    <xs:element name="id" type="xs:string"/>
    <xs:element name="amount" type="xs:decimal"/>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"#;

const FW_MANIFEST: &str = "schemaKind: xsd\nformats: [fixed-width]\n\
     fixed-width:\n  fields:\n    - {name: id, width: 8}\n    - {name: amount, width: 10}\n";

#[test]
fn a_fixed_width_layout_block_loads_and_decodes_records() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(&schemas, "rec", FW_MANIFEST, &[("rec.xsd", FW_XSD)]);

    let codecs = load(&schemas, MODULE_NS).expect("loads");
    let codec = codecs.first().expect("one codec").clone();
    assert!(
        codec
            .accepted_content_types()
            .iter()
            .any(|c| c == "text/plain"),
        "a fixed-width codec must admit its own content type: {:?}",
        codec.accepted_content_types()
    );

    // 8 + 10 columns; the schema types `amount` as a decimal, so it comes back as a number.
    let body = format!("{:<8}{:<10}\n", "R-1", "12.50");
    let result = codec.decode(body.as_bytes(), Some("text/plain"));
    assert_eq!(
        result.outcome,
        sutra_codec_spi::DecodeOutcome::Ok,
        "issues: {:?}",
        result.issues
    );
    assert_eq!(result.message_type.as_deref(), Some("Rec"));
}

#[test]
fn declaring_fixed_width_without_the_layout_block_is_a_manifest_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "rec",
        "schemaKind: xsd\nformats: [fixed-width]\n",
        &[("rec.xsd", FW_XSD)],
    );
    expect_code(&schemas, "SUTRA.CONFIG.CODEC_MANIFEST.INVALID");
}

#[test]
fn a_layout_column_the_type_does_not_declare_is_a_deploy_error() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "rec",
        "schemaKind: xsd\nformats: [fixed-width]\n\
         fixed-width:\n  fields:\n    - {name: id, width: 8}\n    - {name: amount, width: 10}\n\
         \x20   - {name: nope, width: 4}\n",
        &[("rec.xsd", FW_XSD)],
    );
    // A MANIFEST fault, not a schema one: the XSD is fine; the columns declared against it are
    // not. Distinguishing the two is what stops a layout fault being swallowed by the
    // "unsupported XSD subset" fallback (which serves a shape-only codec instead of refusing).
    expect_code(&schemas, "SUTRA.CONFIG.CODEC_MANIFEST.INVALID");
}

#[test]
fn a_malformed_fixed_width_block_is_a_manifest_error() {
    for manifest in [
        // fields missing
        "schemaKind: xsd\nformats: [fixed-width]\nfixed-width:\n  widths: [1]\n",
        // width absent
        "schemaKind: xsd\nformats: [fixed-width]\nfixed-width:\n  fields:\n    - {name: id}\n",
        // zero width
        "schemaKind: xsd\nformats: [fixed-width]\nfixed-width:\n  fields:\n    - {name: id, width: 0}\n",
    ] {
        let scratch = Scratch::new();
        let schemas = scratch.schemas();
        write_codec(&schemas, "rec", manifest, &[("rec.xsd", FW_XSD)]);
        expect_code(&schemas, "SUTRA.CONFIG.CODEC_MANIFEST.INVALID");
    }
}

/// Both tabular formats may be declared together: their content types are disjoint, so one
/// schema serves a CSV feed and a fixed-width feed over the same channel.
#[test]
fn both_tabular_formats_can_be_declared_and_the_content_type_selects() {
    let scratch = Scratch::new();
    let schemas = scratch.schemas();
    write_codec(
        &schemas,
        "rec",
        "schemaKind: xsd\nformats: [csv, fixed-width]\n\
         fixed-width:\n  fields:\n    - {name: id, width: 8}\n    - {name: amount, width: 10}\n",
        &[("rec.xsd", FW_XSD)],
    );
    let codecs = load(&schemas, MODULE_NS).expect("loads");
    let codec = codecs.first().expect("one codec").clone();

    let accepted = codec.accepted_content_types();
    for ct in ["text/csv", "text/plain"] {
        assert!(accepted.iter().any(|c| c == ct), "{ct} in {accepted:?}");
    }

    // text/csv -> the header line names the columns.
    let csv = codec.decode(b"id,amount\nR-1,12.50\n", Some("text/csv"));
    assert_eq!(
        csv.outcome,
        sutra_codec_spi::DecodeOutcome::Ok,
        "issues: {:?}",
        csv.issues
    );

    // text/plain -> the SAME schema, read through the declared column widths.
    let fixed = format!("{:<8}{:<10}\n", "R-1", "12.50");
    let fixed = codec.decode(fixed.as_bytes(), Some("text/plain"));
    assert_eq!(
        fixed.outcome,
        sutra_codec_spi::DecodeOutcome::Ok,
        "issues: {:?}",
        fixed.issues
    );
    assert_eq!(fixed.message_type, csv.message_type, "one schema, one type");
}
