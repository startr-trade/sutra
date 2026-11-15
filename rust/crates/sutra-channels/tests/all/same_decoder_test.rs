//! `InboundChain::same_decoder`: when one decode of a message can stand for another version's.

use std::sync::Arc;

use sutra_channels::{ChannelBinding, CodecRegistry, InboundChain, Namespace, ValidatorRegistry};
use sutra_codec_spi::{CodecValue, DecodeResult, PayloadCodec};
use sutra_executor::{archive_key, logical_urn, DeploymentId};

struct ShapeCodec;

impl PayloadCodec for ShapeCodec {
    fn name(&self) -> &str {
        "shape"
    }
    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/plain".to_string()]
    }
    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        DecodeResult::ok(
            CodecValue::Text(String::from_utf8_lossy(body).into_owned()),
            content_type.unwrap_or("text/plain"),
        )
    }
    fn encode(&self, _p: &CodecValue, _ct: Option<&str>) -> Result<Vec<u8>, String> {
        Err("decode-only".to_string())
    }
}

fn dep(n: u8) -> DeploymentId {
    DeploymentId::of(&format!("dep-0000000000000000000000{n:02x}")).unwrap()
}

fn binding(d: &DeploymentId, codec: &str) -> ChannelBinding {
    ChannelBinding::new("in", Namespace::new("t", "m", "1.0.0"), d.clone(), codec)
}

fn chain() -> InboundChain {
    let mut codecs = CodecRegistry::with_builtins();
    // Byte-identical archive codecs, one per version, each under its own deployment scope.
    for d in [dep(1), dep(2)] {
        codecs.register_under(
            &archive_key(&logical_urn("codec", "shape"), &d),
            Arc::new(ShapeCodec),
        );
    }
    InboundChain::new(
        codecs,
        sutra_channels::FormatRegistry::with_builtins(),
        ValidatorRegistry::new(),
    )
}

#[test]
fn same_decoder_is_false_for_two_archive_codecs() {
    let chain = chain();
    assert!(!chain.same_decoder(&binding(&dep(1), "shape"), &binding(&dep(2), "shape")));
    // The same version's binding is its own decoder.
    assert!(chain.same_decoder(&binding(&dep(1), "shape"), &binding(&dep(1), " shape ")));
}

#[test]
fn a_bare_format_or_no_codec_is_one_decoder_everywhere() {
    let chain = chain();
    assert!(chain.same_decoder(&binding(&dep(1), "json"), &binding(&dep(2), "json")));
    assert!(chain.same_decoder(&binding(&dep(1), ""), &binding(&dep(2), "")));
    assert!(!chain.same_decoder(&binding(&dep(1), "json"), &binding(&dep(2), "xml")));
    assert!(!chain.same_decoder(&binding(&dep(1), "json"), &binding(&dep(2), "shape")));
}

#[test]
fn an_unknown_codec_is_never_the_same_decoder() {
    let chain = chain();
    assert!(!chain.same_decoder(&binding(&dep(1), "nope"), &binding(&dep(2), "nope")));
}
