//! Payload **codec / format / schema SPI** — the neutral contract layer every codec, format and
//! schema builds against, with no concrete parser of its own. It holds:
//!
//! - [`codec::PayloadCodec`] — the decode/encode + `shape_of` contract; a codec with a non-`None`
//!   `shape_of` is schema-backed, otherwise it is a bare format.
//! - [`schema`] — the format × schema composition: [`schema::MessageFormat`] (pure parser),
//!   [`schema::MessageSchema`] (typed contract), and the [`schema::SchemaBoundCodec`] /
//!   [`schema::FormatOnlyCodec`] adapters.
//! - [`shape`], [`result`], [`issue`], [`codes`], [`mapped`], [`projection`] — the shared value /
//!   navigation-shape / diagnostic types the codecs and domain crates project through.
//! - [`BuiltinCodec`] + [`builtin_codecs`] — the self-registration registry (inventory pull).
//!
//! Concrete parsers live above this crate: the schema-less formats in `sutra-formats`
//! (json/xml/yaml/raw/csv), the schema-backed codecs in `sutra-codec-schema`
//! (structural/json-schema), and the business standards in the `sutra-codec-<standard>` crates.
#![forbid(unsafe_code)]

pub mod codec;
pub mod codes;
pub mod content_type;
pub mod issue;
pub mod mapped;
// `pub` (not `pub(crate)`) so codec crates OUTSIDE this workspace — the proprietary domain
// codecs moved to the rails repo, and any third-party extension crate — can build their map
// projections against the shared helper. Part of the SPI surface, not an internal detail.
pub mod projection;
pub mod result;
pub mod schema;
pub mod shape;

pub use codec::PayloadCodec;
pub use issue::{IssueSeverity, ValidationIssue};
pub use mapped::{MappedDecodeResult, MappedMap, MappedValue};
pub use result::{CodecValue, DecodeOutcome, DecodeResult};
pub use schema::{
    FormatOnlyCodec, FormatParse, MessageFormat, MessageSchema, PayloadCodecFormat,
    SchemaBoundCodec, SchemaKind, ShapeClass,
};
pub use shape::{PathResolution, SchemaShape, ShapeFieldType};

/// A self-registered global built-in codec. A codec module `inventory::submit!`s one of
/// these next to its impl, so IMPLEMENTING a zero-config codec IS registering it — there is
/// no central push-list to forget (the mitigation for the historical missed-registration gap). The
/// `make` fn-pointer keeps the submitted static `Sync`; the `Arc` is built at call time, never
/// stored.
pub struct BuiltinCodec {
    /// The codec's short token — the same value its [`PayloadCodec::name`] returns.
    pub name: &'static str,
    /// Construct a fresh instance (fn-pointer, so the inventory static stays `Sync`).
    pub make: fn() -> std::sync::Arc<dyn PayloadCodec>,
}

inventory::collect!(BuiltinCodec);

/// The zero-config **global** codecs the engine ships — the single canonical source of
/// built-in codec identity, collected from every [`BuiltinCodec`] a codec module
/// self-registers (pull, not a hardcoded list; the lint derives the reserved-name set from
/// HERE). Every entry is usable WITHOUT per-package configuration and is referenced as
/// `urn:sutra:codec:<name>` (e.g. `urn:sutra:codec:json`, `urn:sutra:codec:csv`).
/// Schema-backed codecs (`sutra-codec-schema`'s structural / json-schema) do NOT self-register —
/// a channel binds a per-package schema-backed instance. Sorted by name (inventory link order is
/// unspecified; the catalog / conformance need a stable order).
pub fn builtin_codecs() -> Vec<std::sync::Arc<dyn PayloadCodec>> {
    let mut entries: Vec<&BuiltinCodec> = inventory::iter::<BuiltinCodec>().collect();
    entries.sort_by_key(|e| e.name);
    entries.into_iter().map(|e| (e.make)()).collect()
}

/// A self-registered global built-in **format** — a schema-less parser (`json`, `xml`, `yaml`,
/// `raw-*`, `csv`) carrying its [`ShapeClass`]. A format module `inventory::submit!`s one next to
/// its impl, so implementing a format IS registering it. Distinct from [`BuiltinCodec`]: a format
/// has no schema, hence no static type validation — a channel binds it as a shape contract and the
/// parser is chosen by the inbound content-type. The format's parser is exposed as a
/// [`PayloadCodec`] (its `decode` yields the tree + content-type the negotiation projects/echoes);
/// `shape_of` is `None` by construction.
pub struct BuiltinFormat {
    /// The format's short token (`json`, `csv`, …) — matches [`PayloadCodec::name`].
    pub name: &'static str,
    /// The observable data shape this format targets.
    pub shape_class: ShapeClass,
    /// Construct a fresh instance (fn-pointer, so the inventory static stays `Sync`).
    pub make: fn() -> std::sync::Arc<dyn PayloadCodec>,
}

inventory::collect!(BuiltinFormat);

/// One resolved built-in format: its name, shape class, and a fresh parser instance.
pub struct FormatEntry {
    pub name: &'static str,
    pub shape_class: ShapeClass,
    pub codec: std::sync::Arc<dyn PayloadCodec>,
}

/// The zero-config **global formats** the engine ships — collected from every [`BuiltinFormat`] a
/// format module self-registers (pull, not a hardcoded list; the reserved-name set unions this with
/// [`builtin_codecs`]). Sorted by name (inventory link order is unspecified).
pub fn builtin_formats() -> Vec<FormatEntry> {
    let mut entries: Vec<&BuiltinFormat> = inventory::iter::<BuiltinFormat>().collect();
    entries.sort_by_key(|e| e.name);
    entries
        .into_iter()
        .map(|e| FormatEntry {
            name: e.name,
            shape_class: e.shape_class,
            codec: (e.make)(),
        })
        .collect()
}
