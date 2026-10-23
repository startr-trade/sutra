//! Message attributes on a decode result, and the attribute vocabulary a codec declares.
//!
//! These are the two halves of intake filtering that live in the SPI: what a codec REPORTS about
//! one message, and what it PROMISES it can report. The engine routes on the first; the
//! deploy-time lint checks a start event's predicate against the second.

use std::collections::BTreeMap;

use sutra_codec_spi::result::{
    is_valid_attribute_key, AttributeKind, AttributeLiteral, AttributeSpec, CodecValue,
    DecodeOutcome, DecodeResult,
};
use sutra_codec_spi::{FeelValue, PayloadCodec};

fn ok_result() -> DecodeResult {
    DecodeResult::ok(CodecValue::Text("body".to_string()), "text/plain")
}

#[test]
fn decode_result_defaults_to_no_attributes() {
    // A codec that says nothing about a message must not look like one reporting an empty-valued
    // attribute: the absence has to be distinguishable from a declaration.
    for result in [
        ok_result(),
        DecodeResult::soft_errors(CodecValue::Text("b".to_string()), Vec::new(), "text/plain"),
        DecodeResult::fatal(Vec::new(), "text/plain"),
    ] {
        assert!(result.attributes.is_empty());
    }
}

#[test]
fn with_attribute_accumulates_and_last_write_wins() {
    let result = ok_result()
        .with_attribute("direction", FeelValue::String("incoming".to_string()))
        .with_attribute("urgency", FeelValue::String("high".to_string()))
        .with_attribute("direction", FeelValue::String("outgoing".to_string()));

    assert_eq!(result.attributes.len(), 2);
    assert_eq!(
        result.attributes.get("direction"),
        Some(&FeelValue::String("outgoing".to_string()))
    );
    assert_eq!(
        result.attributes.get("urgency"),
        Some(&FeelValue::String("high".to_string()))
    );
}

#[test]
fn with_attributes_merges_over_what_is_already_there() {
    let mut more = BTreeMap::new();
    more.insert("urgency".to_string(), FeelValue::String("low".to_string()));
    more.insert("retry".to_string(), FeelValue::Boolean(true));

    let result = ok_result()
        .with_attribute("urgency", FeelValue::String("high".to_string()))
        .with_attributes(more);

    assert_eq!(
        result.attributes.get("urgency"),
        Some(&FeelValue::String("low".to_string()))
    );
    assert_eq!(
        result.attributes.get("retry"),
        Some(&FeelValue::Boolean(true))
    );
}

#[test]
fn an_invalid_attribute_key_is_dropped_not_stored() {
    // A key no predicate could ever spell would route nothing, so carrying it buys nothing.
    let result = ok_result()
        .with_attribute("has space", FeelValue::Boolean(true))
        .with_attribute("9lives", FeelValue::Boolean(true))
        .with_attribute("", FeelValue::Boolean(true))
        .with_attribute("dotted.key", FeelValue::Boolean(true))
        .with_attribute("kept_one", FeelValue::Boolean(true));

    assert_eq!(
        result.attributes.keys().collect::<Vec<_>>(),
        vec!["kept_one"]
    );
}

#[test]
fn attribute_keys_are_identifier_shaped() {
    for good in [
        "direction",
        "_private",
        "a",
        "camelCase",
        "with_9_digits",
        "A1",
    ] {
        assert!(is_valid_attribute_key(good), "{good} should be valid");
    }
    for bad in [
        "",
        " ",
        "has space",
        "9lives",
        "dotted.key",
        "dash-ed",
        "ünicode",
    ] {
        assert!(!is_valid_attribute_key(bad), "{bad} should be invalid");
    }
}

#[test]
fn attributes_iterate_in_a_deterministic_order() {
    // These values reach diagnostics and the audit trail, so the order cannot depend on hashing.
    let result = ok_result()
        .with_attribute("zulu", FeelValue::Boolean(true))
        .with_attribute("alpha", FeelValue::Boolean(true))
        .with_attribute("mike", FeelValue::Boolean(true));

    assert_eq!(
        result.attributes.keys().collect::<Vec<_>>(),
        vec!["alpha", "mike", "zulu"]
    );
}

#[test]
fn attribute_literals_order_for_use_in_domain_sets() {
    // The deploy-time analysis holds declared domains in sorted sets; that needs a total order.
    let mut domain = vec![
        AttributeLiteral::Text("b".to_string()),
        AttributeLiteral::Text("a".to_string()),
        AttributeLiteral::Integer(2),
        AttributeLiteral::Integer(1),
        AttributeLiteral::Boolean(true),
    ];
    domain.sort();
    domain.dedup();
    assert_eq!(domain.len(), 5);

    let deduped: std::collections::BTreeSet<_> = [
        AttributeLiteral::Text("same".to_string()),
        AttributeLiteral::Text("same".to_string()),
    ]
    .into_iter()
    .collect();
    assert_eq!(deduped.len(), 1);
}

/// A codec that declares nothing — the default, and the OPEN posture.
struct SilentCodec;

impl PayloadCodec for SilentCodec {
    fn name(&self) -> &str {
        "silent"
    }
    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/plain".to_string()]
    }
    fn decode(&self, _body: &[u8], _content_type: Option<&str>) -> DecodeResult {
        ok_result()
    }
    fn encode(
        &self,
        _payload: &CodecValue,
        _content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }
}

/// A codec that classifies each message by a closed attribute, which is the shape intake
/// filtering exists for: one message type arriving in more than one flavour.
struct ClassifyingCodec;

impl PayloadCodec for ClassifyingCodec {
    fn name(&self) -> &str {
        "classifying"
    }
    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/plain".to_string()]
    }
    fn decode(&self, body: &[u8], _content_type: Option<&str>) -> DecodeResult {
        let direction = if body.starts_with(b"<") {
            "incoming"
        } else {
            "outgoing"
        };
        ok_result()
            .with_message_type("Transfer")
            .with_attribute("direction", FeelValue::String(direction.to_string()))
    }
    fn declared_message_types(&self) -> Vec<String> {
        vec!["Transfer".to_string()]
    }
    fn declared_attributes(&self, _message_type: Option<&str>) -> Vec<AttributeSpec> {
        vec![AttributeSpec {
            key: "direction".to_string(),
            kind: AttributeKind::Text,
            domain: vec![
                AttributeLiteral::Text("incoming".to_string()),
                AttributeLiteral::Text("outgoing".to_string()),
            ],
            required: true,
        }]
    }
    fn encode(
        &self,
        _payload: &CodecValue,
        _content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }
}

#[test]
fn declared_attributes_defaults_to_open() {
    assert!(SilentCodec.declared_attributes(None).is_empty());
    assert!(SilentCodec.declared_attributes(Some("anything")).is_empty());
}

#[test]
fn a_codec_declares_a_closed_attribute_and_reports_it_per_message() {
    let declared = ClassifyingCodec.declared_attributes(Some("Transfer"));
    assert_eq!(declared.len(), 1);
    assert_eq!(declared[0].key, "direction");
    assert_eq!(declared[0].kind, AttributeKind::Text);
    assert!(declared[0].required);
    assert_eq!(declared[0].domain.len(), 2);

    // Same message type, different attribute — the case a single message_type cannot express.
    let incoming = ClassifyingCodec.decode(b"<doc/>", None);
    let outgoing = ClassifyingCodec.decode(b"plain", None);
    assert_eq!(incoming.message_type.as_deref(), Some("Transfer"));
    assert_eq!(outgoing.message_type, incoming.message_type);
    assert_ne!(incoming.attributes, outgoing.attributes);
    assert_eq!(
        incoming.attributes.get("direction"),
        Some(&FeelValue::String("incoming".to_string()))
    );

    // Every reported value is inside what the codec declared it could report.
    for result in [&incoming, &outgoing] {
        let FeelValue::String(reported) = result.attributes.get("direction").unwrap() else {
            panic!("direction should be text");
        };
        assert!(
            declared[0]
                .domain
                .contains(&AttributeLiteral::Text(reported.clone())),
            "{reported} should be in the declared domain"
        );
    }
    assert_eq!(incoming.outcome, DecodeOutcome::Ok);
}
