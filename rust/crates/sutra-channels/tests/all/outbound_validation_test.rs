//! Outbound contract enforcement: a message the flow produced is read back by the very codec
//! that is about to put it on the wire, and a contract breach stops it leaving.
//!
//! The asymmetry this closes is worth stating, because it is not obvious from either side alone:
//! the engine validated every byte it ACCEPTED and none of the bytes it EMITTED. A reply or send
//! whose body is a template render never touches a codec at all, and the encode path that does
//! only projects — projection is the non-validating half of the schema pair. So a flow could
//! answer a strictly-validated request with a structurally invalid message, and nothing in the
//! engine would notice.

use std::sync::Arc;

use sutra_channels::{CodecRegistry, Diagnostic, FormatRegistry, InboundChain, ValidatorRegistry};
use sutra_codec_spi::{CodecValue, DecodeResult, IssueSeverity, PayloadCodec, ValidationIssue};
use sutra_executor::DeploymentId;

pub(crate) const CODEC: &str = "contract-codec";
const CHANNEL: &str = "orders-out";

/// A codec with a contract small enough to state in one line: the body must contain
/// `<Document>`. Standing in for every schema-backed codec, whose real contract is an XSD.
pub(crate) struct ContractCodec;

impl PayloadCodec for ContractCodec {
    fn name(&self) -> &str {
        CODEC
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec!["application/xml".to_string()]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let text = String::from_utf8_lossy(body).to_string();
        let mut result = DecodeResult::ok(
            CodecValue::Text(text.clone()),
            content_type.unwrap_or("application/xml"),
        )
        .with_message_type("Packet");
        if !text.contains("<Document>") {
            result.issues.push(ValidationIssue {
                code: "TEST.MISSING_DOCUMENT".to_string(),
                severity: IssueSeverity::Error,
                path: "/Packet/Document".to_string(),
                message: "the packet carries no <Document>".to_string(),
                value: None,
            });
        }
        result
    }

    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        match payload {
            CodecValue::Text(s) => Ok(s.clone().into_bytes()),
            other => Err(format!("cannot encode {other:?}")),
        }
    }
}

fn chain() -> InboundChain {
    let mut codecs = CodecRegistry::with_builtins();
    codecs.register_shared(Arc::new(ContractCodec));
    InboundChain::new(
        codecs,
        FormatRegistry::with_builtins(),
        ValidatorRegistry::new(),
    )
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000031").expect("valid deployment id")
}

/// The two codec arguments are the point of the signature: `declared` is the author's
/// declaration on the output, `channel_codec` is the channel's binding, and which of the two
/// governs is the behaviour under test.
fn check(declared: Option<&str>, channel_codec: &str, body: &[u8]) -> Result<(), Diagnostic> {
    chain().validate_outbound(
        &deployment(),
        CHANNEL,
        declared,
        channel_codec,
        body,
        Some("application/xml"),
        "the reply of process 'p'",
    )
}

const GOOD: &[u8] = b"<Packet><Document>ok</Document></Packet>";
const BAD: &[u8] = b"<Packet></Packet>";

#[test]
fn a_conforming_render_leaves_unobstructed() {
    assert!(
        check(None, CODEC, GOOD).is_ok(),
        "a valid message must not be held back"
    );
}

#[test]
fn a_render_the_destination_codec_rejects_is_not_sent() {
    let err =
        check(None, CODEC, BAD).expect_err("a packet with no <Document> breaches the contract");
    assert_eq!(err.code, sutra_channels::codes::OUTBOUND_VALIDATION_REJECT);
    // The operator must be able to see WHICH field failed, not merely that something did:
    // a bare refusal on an outbound message is close to undebuggable from the far end.
    assert!(
        err.message.contains("/Packet/Document"),
        "the offending path must be named: {}",
        err.message
    );
    assert!(
        err.message.contains("the reply of process 'p'"),
        "the producing site must be named: {}",
        err.message
    );
    assert_eq!(
        err.attributes.get("issueCount").map(String::as_str),
        Some("1")
    );
    // An operator must be able to tell whose declaration to change — the output's or the
    // channel's. Naming only the codec leaves that ambiguous.
    assert_eq!(
        err.attributes.get("codecSource").map(String::as_str),
        Some("channel")
    );
    assert!(
        err.message
            .contains(&format!("bound to channel '{CHANNEL}'")),
        "the provenance must be in the message too: {}",
        err.message
    );
}

#[test]
fn schema_less_egress_stays_fail_open() {
    // A channel with no codec never rejected anything on the way in either — a bare format is
    // fail-open by construction, so there is no contract to fail on the way out. Tightening
    // this would make every schema-less channel unusable, which is the inbound R5 carve-out's
    // reasoning applied unchanged.
    assert!(
        check(None, "", b"not a packet at all").is_ok(),
        "no codec means no contract to breach"
    );
}

#[test]
fn a_bare_format_binding_is_not_a_contract() {
    assert!(
        check(None, "raw-text", b"not a packet at all").is_ok(),
        "a format contract asserts a shape, not a schema"
    );
}

#[test]
fn an_unresolvable_channel_codec_does_not_masquerade_as_a_contract_breach() {
    // The send path reports a missing codec on its own, with the name and the known set. A
    // second, differently-worded refusal from here would send the operator looking for a
    // validation bug that is really a wiring one.
    assert!(
        check(None, "urn:nobody:registered-this", BAD).is_ok(),
        "a wiring fault is not a contract breach"
    );
}

#[test]
fn a_codec_stated_on_the_output_governs_instead_of_the_channels() {
    // A flow is a translator: it may accept one format and emit another, so the codec that
    // decoded the request is not necessarily the one that governs the answer. The stated codec
    // wins — here it rejects a body the channel's own (schema-less) binding would have waved
    // through.
    let err = check(Some(CODEC), "raw-text", BAD)
        .expect_err("the stated codec's contract applies, not the channel's");
    assert_eq!(err.code, sutra_channels::codes::OUTBOUND_VALIDATION_REJECT);
    assert_eq!(err.attributes.get("codec").map(String::as_str), Some(CODEC));
    assert_eq!(
        err.attributes.get("codecSource").map(String::as_str),
        Some("declared")
    );
    assert!(
        err.message.contains("declared on the output"),
        "the provenance must be in the message too: {}",
        err.message
    );
}

#[test]
fn a_stated_codec_that_resolves_to_nothing_fails_closed() {
    // The inferred case stays quiet because the send path reports a wiring fault itself. A
    // STATED one must not: the single declaration that asked for validation would otherwise be
    // the one case silently exempt from it.
    let err = check(Some("urn:nobody:registered-this"), CODEC, GOOD)
        .expect_err("a named codec that resolves to nothing is a fail-closed wiring fault");
    assert!(
        err.message.contains("urn:nobody:registered-this"),
        "the unresolvable name must be reported: {}",
        err.message
    );
}

#[test]
fn a_stated_bare_format_asserts_no_schema() {
    assert!(
        check(Some("raw-text"), CODEC, b"anything at all").is_ok(),
        "stating a format states a shape, not a schema"
    );
}

// ---- the two outbound forms share one model -------------------------------------------------
//
// `<q:send>` and `<q:reply>` are held to their contract by the same call, with the same
// fail-closed posture and the same precedence of a stated codec over an inferred one. Only the
// INFERRED codec differs: a send names its own channel, while a reply rides the channel the
// request arrived on and names none.
//
// The case that is easy to get wrong, and that these pin: a reply splits by whether it resolved
// a destination. Without one it rides the inbound connection and is checked as the synchronous
// reply; WITH one it is delivered out of band and so arrives on the emission path instead. A
// check that read only the `send` binding there would skip every destination-bearing reply —
// including one that stated its codec, which is the single declaration that asked to be
// validated.

#[test]
fn a_reply_infers_no_contract_from_the_channel_it_rides_back_out_on() {
    // The tempting fallback — a destination-less reply rides the inbound channel, so use that
    // channel's codec — is wrong in the COMMON case: a channel's codec is the contract its
    // REQUESTS satisfy, and a reply is a different message. A schema codec bound to validate a
    // transfer request has no declaration for the acceptance that answers it, so inferring it
    // rejects correct replies. Three conformance examples refused to answer at all before this
    // was pinned.
    //
    // `check` passes the channel codec in the position `build_reply` now fills with the
    // no-inference marker, so this asserts the marker's behaviour, not the call site's wiring —
    // `a_render_with_no_declared_reply_answers_nothing` and the conformance suites cover that.
    assert!(
        check(None, "", BAD).is_ok(),
        "with nothing stated there is no reply contract to fail"
    );
}

#[test]
fn both_forms_refuse_and_accept_on_the_same_terms() {
    // One body, one codec, two provenances — the verdict must not depend on which of the two
    // declarations named the codec, only on what the codec says about the bytes.
    for declared in [None, Some(CODEC)] {
        let channel_codec = if declared.is_some() {
            "raw-text"
        } else {
            CODEC
        };
        assert!(
            check(declared, channel_codec, GOOD).is_ok(),
            "a conforming message passes either way (declared={declared:?})"
        );
        let err = check(declared, channel_codec, BAD)
            .expect_err("a breaching message is refused either way");
        assert_eq!(
            err.code,
            sutra_channels::codes::OUTBOUND_VALIDATION_REJECT,
            "the same code either way (declared={declared:?})"
        );
        assert_eq!(
            err.attributes.get("codec").map(String::as_str),
            Some(CODEC),
            "the governing codec either way (declared={declared:?})"
        );
    }
}
