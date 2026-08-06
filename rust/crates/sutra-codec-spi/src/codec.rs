//! The payload-codec contract for the built-in, schema-less codecs
//! (the format-only view over a `MessageFormat`).

use crate::result::{CodecValue, DecodeResult};
use crate::shape::SchemaShape;

/// A payload codec: name + accepted content types + total decode + encode (the inverse).
///
/// **`Send + Sync` is required** (execution scale-out §2 row 10): a compiled codec is
/// immutable after deployment, and the engine builds the codec registry ONCE per activation
/// and shares it across every actor lane rather than compiling one copy per lane. A codec is a
/// pure function over its compiled schema — the bound rules out hidden per-decode mutable
/// state (an interior-mutable cache), which under lanes would be a data race rather than an
/// optimisation. Every codec in the tree satisfies it without change.
pub trait PayloadCodec: Send + Sync {
    /// Registry name (`raw-text`, `json`, … — or a version-scoped URN for module codecs).
    fn name(&self) -> &str;

    /// Accepted media-type patterns (exact, `*/*`, `type/*`, RFC 6839 `type/*+suffix`).
    /// Empty means "declares none" — the engine's capability gate then admits everything.
    fn accepted_content_types(&self) -> Vec<String>;

    /// Total decode: malformed input is a FATAL [`DecodeResult`], never a panic/Err.
    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult;

    /// The closed set of message types this codec can emit (schema-bound codecs); empty by
    /// default (opaque / open-typed codecs) — the declared-message-types
    /// default. Consumed by the deploy-time lint's message-type declaration cross-check.
    fn declared_message_types(&self) -> Vec<String> {
        Vec::new()
    }

    /// The navigation SHAPE of one message type — the field/path map deploy-time static
    /// validation walks (`<q:alias>`/flow-condition/template payload paths). `None` by
    /// default: opaque / format-only codecs (xml/json/yaml/csv/raw-*) expose no fixed shape.
    /// A SCHEMA-AWARE codec overrides it — the message-standard codecs (supplied by
    /// proprietary extension crates) return their static shape, and a user schema-backed
    /// [`crate::StructuralCodec`] derives it from the package XSD at call time. The codec is
    /// the source of truth; the lint asks it rather than hardcoding per-standard logic.
    /// Owned, so the user codec can build a fresh shape.
    fn shape_of(&self, message_type: Option<&str>) -> Option<SchemaShape> {
        let _ = message_type;
        None
    }

    /// Encode a reply payload to wire bytes (the inverse of decode). Errors carry a
    /// human-readable message; the engine wraps them as `SUTRA.OUTBOUND.ENCODE_FAILED`.
    fn encode(&self, payload: &CodecValue, content_type: Option<&str>) -> Result<Vec<u8>, String>;
}

/// Collapse newlines so a parser's error text stays a single log/diagnostic line. `pub` so the
/// format/codec crates built on this SPI can sanitize their own parser messages uniformly.
pub fn sanitize(message: &str) -> String {
    message.replace(['\n', '\r'], " ")
}
