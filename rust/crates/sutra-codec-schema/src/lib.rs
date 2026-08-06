//! Schema-backed **codecs** — a codec is a format bound to a schema (a `SchemaShape` →
//! static type validation). Unlike the schema-less `sutra-formats`, these carry a typed
//! contract, so they are true codecs (`sutra_codec_spi::PayloadCodec` with a non-`None`
//! `shape_of`). None self-register: they are built on demand from the deployment archive's
//! user-supplied schema (the archive model).
//!
//! - [`StructuralCodec`] — an XSD-backed codec: XML validates directly through `sutra_xsd`;
//!   json/yaml are parsed by their [`sutra_formats`] format then validated against the same
//!   subset schema. Derives its [`sutra_codec_spi::shape::SchemaShape`] from the package XSD.
//! - [`JsonSchemaCodec`] — a JSON-Schema-backed codec over a JSON/YAML format.
//! - [`bundle`] — the schema-BUNDLE kind SPI: a `schemaKind` served by a codec crate of its own
//!   (a whole standards profile — envelope grammar plus versioned editions the deployment maps in
//!   its own manifest), registered through inventory so this crate names none of them.
//! - [`schema_codec_loader`] — loads the above from a package's `schemas/` codec manifest.
#![forbid(unsafe_code)]

pub mod bundle;
pub mod json_schema;
pub mod schema_codec_loader;
pub mod structural;

pub use bundle::{plan as plan_schema_bundles, BundleCodecKind, BundleSource, PlannedBundle};
pub use json_schema::{
    json_schema_bound_codec, json_schema_fields, json_schema_shape, JsonNodeFormat, JsonSchema,
    JsonSchemaCodec,
};
pub use schema_codec_loader::{codec_urn, load as load_schema_codecs, CodecLoadError};
pub use structural::StructuralCodec;
