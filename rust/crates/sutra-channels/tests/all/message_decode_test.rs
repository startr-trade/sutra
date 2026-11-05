//! `InboundChain` as the `<q:decode>` seam: codec choice, the size cap, redactors, and what a failure
//! may say.

use std::sync::Arc;

use sutra_bpmn::qbindings::MessageDecodeBinding;
use sutra_channels::{
    CodecRegistry, ContentRedactor, FormatRegistry, InboundChain, RedactionLocator,
    RedactorRegistry, ValidatorRegistry, REDACTED_MARKER,
};
use sutra_codec_spi::{CodecValue, DecodeResult, PayloadCodec};
use sutra_executor::{
    archive_key, logical_of, DeploymentId, MessageDecodeFailureKind, MessageDecodeSeam, Variables,
};
use sutra_feel::FeelValue;

const SECRET: &str = "SECRET-IN-THE-TEXT";

fn deployment(last: &str) -> DeploymentId {
    DeploymentId::of(&format!("dep-00000000000000000000000{last}")).expect("valid id")
}

fn binding(codec: &str, redactors: &[&str]) -> MessageDecodeBinding {
    MessageDecodeBinding {
        name: "message".to_string(),
        expression: "text".to_string(),
        codec: codec.to_string(),
        content_type_expression: None,
        redactors: redactors.iter().map(|r| r.to_string()).collect(),
    }
}

fn chain() -> InboundChain {
    InboundChain::new(
        CodecRegistry::with_builtins(),
        FormatRegistry::with_builtins(),
        ValidatorRegistry::new(),
    )
}

fn field(value: &FeelValue, name: &str) -> FeelValue {
    match value {
        FeelValue::Map(map) => map.get(name).cloned().expect("field"),
        other => panic!("not a map: {other:?}"),
    }
}

#[test]
fn decode_parses_by_format_and_the_content_type_picks_the_parser() {
    let json = chain()
        .decode_message(
            &deployment("1"),
            &binding("json", &[]),
            r#"{"subject":"hello"}"#,
            Some("application/json"),
            &Variables::new(),
        )
        .unwrap();
    assert_eq!(
        field(&json.value, "subject"),
        FeelValue::String("hello".to_string())
    );
    assert!(json.masked.is_none());

    // json, xml and yaml are one shape contract, so the content type chooses among them.
    let xml = chain()
        .decode_message(
            &deployment("1"),
            &binding("json", &[]),
            "<note><subject>hello</subject></note>",
            Some("application/xml"),
            &Variables::new(),
        )
        .unwrap();
    assert_ne!(xml.value, FeelValue::Null);
}

#[test]
fn decode_failure_diagnostic_never_quotes_input() {
    let failure = chain()
        .decode_message(
            &deployment("1"),
            &binding("json", &[]),
            &format!("{{not json {SECRET}"),
            Some("application/json"),
            &Variables::new(),
        )
        .unwrap_err();
    assert_eq!(failure.kind, MessageDecodeFailureKind::Fatal);
    assert!(!failure.detail.contains(SECRET), "{}", failure.detail);
}

#[test]
fn decode_refuses_a_content_type_outside_the_contract() {
    let failure = chain()
        .decode_message(
            &deployment("1"),
            &binding("json", &[]),
            "{}",
            Some("image/png"),
            &Variables::new(),
        )
        .unwrap_err();
    assert_eq!(failure.kind, MessageDecodeFailureKind::Fatal);
    assert!(failure.detail.contains("<q:decode name='message'>"));
}

#[test]
fn decode_too_large_is_refused() {
    let chain = chain().with_message_decode_cap(4);
    let failure = chain
        .decode_message(
            &deployment("1"),
            &binding("json", &[]),
            "{\"a\":1}",
            None,
            &Variables::new(),
        )
        .unwrap_err();
    assert_eq!(failure.kind, MessageDecodeFailureKind::TooLarge);
    assert!(chain
        .decode_message(
            &deployment("1"),
            &binding("json", &[]),
            "{}",
            None,
            &Variables::new()
        )
        .is_ok());
}

#[test]
fn a_disabled_intake_cap_still_keeps_the_default_ceiling() {
    let over = "x".repeat(sutra_channels::PayloadCapPolicy::DEFAULT_MAX_PAYLOAD_BYTES as usize + 1);
    let failure = chain()
        .with_message_decode_cap(0)
        .decode_message(
            &deployment("1"),
            &binding("raw-text", &[]),
            &over,
            None,
            &Variables::new(),
        )
        .unwrap_err();
    assert_eq!(failure.kind, MessageDecodeFailureKind::TooLarge);
}

/// Masks `/card`, or crashes when told to.
struct CardRedactor {
    crash: bool,
}

impl ContentRedactor for CardRedactor {
    fn name(&self) -> &str {
        "card"
    }

    fn locate(
        &self,
        _payload: &FeelValue,
        _variables: &Variables,
    ) -> Result<Vec<RedactionLocator>, String> {
        if self.crash {
            return Err(format!("crashed reading {SECRET}"));
        }
        Ok(vec![RedactionLocator::new("/card", "test")])
    }
}

fn with_card(crash: bool) -> InboundChain {
    let mut redactors = RedactorRegistry::new();
    redactors.register_under("urn:sutra:redactor:card:internal", CardRedactor { crash });
    chain().with_redactors(redactors)
}

#[test]
fn decode_redactors_write_companion_and_fail_closed() {
    let text = format!(r#"{{"ref":"r-1","card":"{SECRET}"}}"#);
    let masked = with_card(false)
        .decode_message(
            &deployment("1"),
            &binding("json", &["card"]),
            &text,
            None,
            &Variables::new(),
        )
        .unwrap();
    assert_eq!(
        field(&masked.value, "card"),
        FeelValue::String(SECRET.to_string())
    );
    let companion = masked.masked.expect("companion");
    assert_eq!(
        field(&companion, "card"),
        FeelValue::String(REDACTED_MARKER.to_string())
    );
    assert_eq!(
        field(&companion, "ref"),
        FeelValue::String("r-1".to_string())
    );

    // A crashed redactor masks the whole value.
    let crashed = with_card(true)
        .decode_message(
            &deployment("1"),
            &binding("json", &["card"]),
            &text,
            None,
            &Variables::new(),
        )
        .unwrap();
    assert_eq!(
        crashed.masked,
        Some(FeelValue::String(REDACTED_MARKER.to_string()))
    );

    // An unknown redactor refuses the step, naming no input.
    let failure = chain()
        .decode_message(
            &deployment("1"),
            &binding("json", &["card"]),
            &text,
            None,
            &Variables::new(),
        )
        .unwrap_err();
    assert_eq!(failure.kind, MessageDecodeFailureKind::NotFound);
    assert!(!failure.detail.contains(SECRET));
}

struct SchemaCodec;

impl PayloadCodec for SchemaCodec {
    fn name(&self) -> &str {
        "note"
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec!["*/*".to_string()]
    }

    fn decode(&self, body: &[u8], _content_type: Option<&str>) -> DecodeResult {
        DecodeResult::ok(
            CodecValue::Text(format!("decoded:{}", String::from_utf8_lossy(body))),
            "text/plain",
        )
    }

    fn encode(&self, _value: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        Err("decode-only".to_string())
    }
}

#[test]
fn decode_resolves_package_schema_codec_by_deployment() {
    let mut codecs = CodecRegistry::with_builtins();
    codecs.register_under(
        &archive_key(&logical_of("codec", "urn:note"), &deployment("1")),
        Arc::new(SchemaCodec),
    );
    let chain = InboundChain::new(
        codecs,
        FormatRegistry::with_builtins(),
        ValidatorRegistry::new(),
    );

    let own = chain
        .decode_message(
            &deployment("1"),
            &binding("urn:note", &[]),
            "x",
            None,
            &Variables::new(),
        )
        .unwrap();
    assert_eq!(own.value, FeelValue::String("decoded:x".to_string()));

    // Another deployment does not see it, so a redeploy cannot change an old instance's decode.
    let other = chain
        .decode_message(
            &deployment("2"),
            &binding("urn:note", &[]),
            "x",
            None,
            &Variables::new(),
        )
        .unwrap_err();
    assert_eq!(other.kind, MessageDecodeFailureKind::NotFound);
}
