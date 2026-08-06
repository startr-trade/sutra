//! The built-in codecs: the raw byte/text formats, the value formats (JSON/YAML/XML),
//! encode round-trips asserted over the map projection, the XML→map projection
//! (repeated-sibling lists, attribute/`#text` conventions, malformed XML is FATAL), the
//! data-only YAML posture, and the XXE hardening posture (DOCTYPE rejection).

use serde_json::json;
use sutra_codec_spi::{builtin_codecs, CodecValue, DecodeOutcome, PayloadCodec};
use sutra_formats::{JsonCodec, RawBytesCodec, RawTextCodec, XmlCodec, YamlCodec};

fn payload(result: &sutra_codec_spi::DecodeResult) -> &CodecValue {
    result.payload.as_ref().expect("payload present")
}

fn json_payload(result: &sutra_codec_spi::DecodeResult) -> &serde_json::Value {
    match payload(result) {
        CodecValue::Json(v) => v,
        other => panic!("expected Json payload, got {other:?}"),
    }
}

// ---- raw byte/text formats ----------------------------------------------------------------

#[test]
fn raw_text_decodes_utf8() {
    let codec = RawTextCodec;
    let r = codec.decode("héllo".as_bytes(), Some("text/plain"));
    assert_eq!(codec.name(), "raw-text");
    assert_eq!(r.outcome, DecodeOutcome::Ok);
    assert_eq!(payload(&r), &CodecValue::Text("héllo".to_string()));
}

#[test]
fn raw_bytes_pass_through() {
    let codec = RawBytesCodec;
    let body = [1u8, 2, 3, 4];
    let r = codec.decode(&body, Some("application/octet-stream"));
    assert_eq!(codec.name(), "raw-bytes");
    assert_eq!(r.outcome, DecodeOutcome::Ok);
    assert_eq!(payload(&r), &CodecValue::Bytes(vec![1, 2, 3, 4]));
}

// ---- built-in value formats ---------------------------------------------------------------

#[test]
fn json_codec_parses_to_json_tree() {
    let codec = JsonCodec;
    let r = codec.decode(br#"{"Id":"INB-7","Amount":100}"#, Some("application/json"));
    assert_eq!(codec.name(), "json");
    assert_eq!(r.outcome, DecodeOutcome::Ok);
    let obj = json_payload(&r);
    assert_eq!(obj["Id"], json!("INB-7"));
    assert_eq!(obj["Amount"].as_i64(), Some(100));
}

#[test]
fn malformed_json_is_fatal() {
    let r = JsonCodec.decode(b"{", Some("application/json"));
    assert_eq!(r.outcome, DecodeOutcome::Fatal);
    assert!(r.payload.is_none());
    assert_eq!(r.issues[0].code, "SUTRA.PARSE.JSON.PARSE_ERROR");
}

#[test]
fn yaml_codec_parses_to_json_tree_superset_of_json() {
    let codec = YamlCodec;
    let r = codec.decode(b"Id: INB-7\nAmount: 100\n", Some("application/yaml"));
    assert_eq!(codec.name(), "yaml");
    assert_eq!(r.outcome, DecodeOutcome::Ok);
    let obj = json_payload(&r);
    assert_eq!(obj["Id"], json!("INB-7"));
    assert_eq!(obj["Amount"].as_i64(), Some(100));
}

#[test]
fn malformed_yaml_is_fatal() {
    let r = YamlCodec.decode(b"key: [1, 2", Some("application/yaml"));
    assert_eq!(r.outcome, DecodeOutcome::Fatal);
    assert_eq!(r.issues[0].code, "SUTRA.PARSE.YAML.PARSE_ERROR");
}

// ---- data-only YAML posture --------------------------------------------------------------

#[test]
fn yaml_duplicate_keys_are_rejected() {
    // A duplicate-key-rejecting data-only loader refuses this; so does this codec.
    let r = YamlCodec.decode(b"a: 1\na: 2\n", Some("application/yaml"));
    assert_eq!(r.outcome, DecodeOutcome::Fatal);
}

#[test]
fn empty_json_body_is_fatal() {
    let r = JsonCodec.decode(b"", Some("application/json"));
    assert_eq!(r.outcome, DecodeOutcome::Fatal);
    assert!(r.issues[0].message.contains("JSON body is empty"));
}

// ---- XmlToMap projection ------------------------------------------------------------------

#[test]
fn valid_xml_decodes_to_typed_navigable_map() {
    let xml = "<Payment><Id>INB-7</Id><Amount>1234.56</Amount>\
               <Item><Sku>A</Sku></Item><Item><Sku>B</Sku></Item></Payment>";
    let r = XmlCodec.decode(xml.as_bytes(), Some("application/xml"));

    assert_eq!(r.outcome, DecodeOutcome::Ok);
    // The document element's local name is the message type (StructuralCodec convention).
    assert_eq!(r.message_type.as_deref(), Some("Payment"));
    let m = json_payload(&r);
    assert_eq!(m["Id"], json!("INB-7"));
    assert_eq!(m["Amount"], json!("1234.56"));
    // Repeated elements → list of nested maps (FEEL: payload.Item[0].Sku).
    let items = m["Item"].as_array().expect("Item list");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["Sku"], json!("A"));
    assert_eq!(items[1]["Sku"], json!("B"));
}

#[test]
fn malformed_xml_is_fatal() {
    let r = XmlCodec.decode(b"<Payment><Id>oops", Some("application/xml"));
    assert_eq!(r.outcome, DecodeOutcome::Fatal);
    assert!(r.payload.is_none());
    assert_eq!(r.issues[0].code, "SUTRA.PARSE.XML.PARSE_ERROR");
}

#[test]
fn xml_attributes_project_with_at_prefix_and_leaf_text_as_hash_text() {
    // XmlToMap: attributes are @-prefixed; a leaf with attributes becomes {@attr, #text};
    // xmlns declarations are structure, not data.
    let xml = r#"<Doc xmlns="urn:x" version="2">
                   <Amt Ccy="USD">10.00</Amt>
                   <Plain>ok</Plain>
                 </Doc>"#;
    let r = XmlCodec.decode(xml.as_bytes(), Some("application/xml"));
    assert_eq!(r.outcome, DecodeOutcome::Ok);
    let m = json_payload(&r);
    assert_eq!(m["@version"], json!("2"));
    assert!(m.get("@xmlns").is_none(), "xmlns must be skipped: {m}");
    assert_eq!(m["Amt"]["@Ccy"], json!("USD"));
    assert_eq!(m["Amt"]["#text"], json!("10.00"));
    assert_eq!(m["Plain"], json!("ok"));
}

#[test]
fn xml_namespace_prefixes_drop_to_local_names() {
    let xml = r#"<p:Doc xmlns:p="urn:x"><p:Id>7</p:Id></p:Doc>"#;
    let r = XmlCodec.decode(xml.as_bytes(), Some("application/xml"));
    assert_eq!(r.message_type.as_deref(), Some("Doc"));
    assert_eq!(json_payload(&r)["Id"], json!("7"));
}

// ---- XXE hardening ------------------------------------------------------------------------

#[test]
fn doctype_declaration_is_rejected_outright() {
    // The CWE-611 guard: a DOCTYPE (internal or external subset) never parses —
    // the entire external-entity attack surface is neutralised.
    let xml = br#"<?xml version="1.0"?>
        <!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
        <foo>&xxe;</foo>"#;
    let r = XmlCodec.decode(xml, Some("application/xml"));
    assert_eq!(r.outcome, DecodeOutcome::Fatal);
    assert!(
        r.issues[0].message.contains("DOCTYPE"),
        "{}",
        r.issues[0].message
    );
}

// ---- encode round-trips ---------------------------------------------------------------------

#[test]
fn raw_text_encodes_utf8() {
    let codec = RawTextCodec;
    let out = codec
        .encode(&CodecValue::Text("héllo".to_string()), Some("text/plain"))
        .unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "héllo");
    // round-trip
    let parsed = codec.decode(b"round trip", Some("text/plain"));
    let out = codec.encode(payload(&parsed), Some("text/plain")).unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "round trip");
}

#[test]
fn raw_bytes_encode_passes_through() {
    let out = RawBytesCodec
        .encode(
            &CodecValue::Bytes(vec![1, 2, 3, 4]),
            Some("application/octet-stream"),
        )
        .unwrap();
    assert_eq!(out, vec![1, 2, 3, 4]);
}

#[test]
fn json_round_trips_through_the_tree() {
    let codec = JsonCodec;
    let r1 = codec.decode(
        br#"{"a":1,"b":"x","c":[true,null]}"#,
        Some("application/json"),
    );
    let out = codec
        .encode(payload(&r1), Some("application/json"))
        .unwrap();
    let r2 = codec.decode(&out, Some("application/json"));
    assert_eq!(json_payload(&r2), json_payload(&r1));
}

#[test]
fn yaml_round_trips_through_the_tree() {
    let codec = YamlCodec;
    let r1 = codec.decode(
        b"a: 1\nb: x\nc:\n  - true\n  - 2\n",
        Some("application/yaml"),
    );
    let out = codec
        .encode(payload(&r1), Some("application/yaml"))
        .unwrap();
    let r2 = codec.decode(&out, Some("application/yaml"));
    assert_eq!(json_payload(&r2), json_payload(&r1));
}

#[test]
fn xml_round_trips_through_the_projection() {
    let codec = XmlCodec;
    let r1 = codec.decode(b"<r><a>1</a><b>two</b></r>", Some("application/xml"));
    assert_eq!(r1.message_type.as_deref(), Some("r"));
    // Re-wrap the projection under its root name (the message type) for the encode side.
    let rooted = CodecValue::Json(json!({ "r": json_payload(&r1) }));
    let out = codec.encode(&rooted, Some("application/xml")).unwrap();
    let r2 = codec.decode(&out, Some("application/xml"));
    assert_eq!(r2.message_type.as_deref(), Some("r"));
    assert_eq!(json_payload(&r2)["a"], json!("1"));
    assert_eq!(json_payload(&r2)["b"], json!("two"));
}

#[test]
fn xml_encode_renders_attributes_text_and_repeats() {
    let rooted = CodecValue::Json(json!({
        "Doc": {
            "@version": "2",
            "Amt": {"@Ccy": "USD", "#text": "10.00"},
            "Item": [{"Sku": "A"}, {"Sku": "B"}]
        }
    }));
    let out = XmlCodec.encode(&rooted, Some("application/xml")).unwrap();
    let xml = String::from_utf8(out).unwrap();
    assert!(xml.contains(r#"<Doc version="2">"#), "{xml}");
    assert!(xml.contains(r#"<Amt Ccy="USD">10.00</Amt>"#), "{xml}");
    assert_eq!(xml.matches("<Sku>").count(), 2, "{xml}");
}

// ---- BuiltinFormats -----------------------------------------------------------------------

#[test]
fn builtin_formats_ship_the_global_schema_less_set_sorted_by_name() {
    let names: Vec<&str> = sutra_codec_spi::builtin_formats()
        .iter()
        .map(|f| f.name)
        .collect();
    // The NEUTRAL framework's own self-registered (inventory) formats, sorted by name. Formats are
    // NOT codecs — they self-register as BuiltinFormat, never BuiltinCodec, so builtin_codecs() is
    // empty of them (this crate links no schema-backed codec). Config-bearing / schema-bound codecs
    // (structural / json-schema) do NOT self-register — they need per-package configuration.
    assert_eq!(
        names,
        vec!["csv", "json", "raw-bytes", "raw-text", "xml", "yaml"]
    );
    // A format carries a shape class and a fresh parser instance whose name matches.
    for f in sutra_codec_spi::builtin_formats() {
        assert_eq!(f.name, f.codec.name());
    }
    // Formats are absent from the codec registry (no dual identity).
    assert!(builtin_codecs().is_empty());
}
