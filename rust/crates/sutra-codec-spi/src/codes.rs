//! Stable diagnostic-code strings used by the built-in codecs — the canonical
//! `SUTRA.*` diagnostic-code values.

pub const PARSE_XML_PARSE_ERROR: &str = "SUTRA.PARSE.XML.PARSE_ERROR";
pub const PARSE_JSON_PARSE_ERROR: &str = "SUTRA.PARSE.JSON.PARSE_ERROR";
pub const PARSE_YAML_PARSE_ERROR: &str = "SUTRA.PARSE.YAML.PARSE_ERROR";
pub const PARSE_CSV_PARSE_ERROR: &str = "SUTRA.PARSE.CSV.PARSE_ERROR";
pub const OUTBOUND_ENCODE_FAILED: &str = "SUTRA.OUTBOUND.ENCODE_FAILED";

// ---- schema-codec tier (T2) ------------------------------------------------------------------

/// A structurally-parsed document failed XSD validation (soft-error, routable).
pub const PARSE_XSD_SCHEMA_VIOLATION: &str = "SUTRA.PARSE.XSD.SCHEMA_VIOLATION";
/// A structurally-parsed document failed JSON-schema validation (soft-error, routable).
pub const PARSE_JSON_SCHEMA_VIOLATION: &str = "SUTRA.PARSE.JSON_SCHEMA.SCHEMA_VIOLATION";
/// A module codec could not decode the bytes at all (transcode/parse failure) — FATAL.
pub const RUNTIME_CODEC_DECODE_FAILED: &str = "SUTRA.RUNTIME.CODEC.DECODE_FAILED";

// ---- schema-codec load gates (deploy-time, fail-closed) --------------------------------------

/// A schema document (XSD / JSON-schema) is itself invalid — a deploy error.
pub const CONFIG_SCHEMA_INVALID: &str = "SUTRA.CONFIG.SCHEMA.INVALID";
/// A codec folder is missing its required `codec-manifest.yaml`.
pub const CONFIG_CODEC_MANIFEST_MISSING: &str = "SUTRA.CONFIG.CODEC_MANIFEST.MISSING";
/// A `codec-manifest.yaml` is malformed or declares an unknown `schemaKind`/`formats`.
pub const CONFIG_CODEC_MANIFEST_INVALID: &str = "SUTRA.CONFIG.CODEC_MANIFEST.INVALID";
/// The `schemas/` folder layout is invalid (loose file, mixed kinds, empty codec folder).
pub const CONFIG_CODEC_LAYOUT_INVALID: &str = "SUTRA.CONFIG.CODEC_LAYOUT.INVALID";
