//! Built-in **formats** — the schema-less generic parsers the engine ships: `json`, `xml`,
//! `yaml`, `raw-text`, `raw-bytes`, `csv`. A format is a pure parser (bytes ⇄ a format-native
//! tree); it carries no schema and no static type validation. Bound to a schema it becomes a
//! codec (`sutra_codec_spi::SchemaBoundCodec`); used bare it is a format-only decode.
//!
//! - `raw-text` — bytes → UTF-8 string, always OK.
//! - `raw-bytes` — passthrough, always OK.
//! - `json` — arbitrary-precision JSON tree; malformed / empty input is FATAL, never thrown.
//! - `yaml` — YAML 1.2 (a JSON superset) into the SAME JSON tree envelope as `json` (uniform
//!   FEEL paths); data-only posture (no object instantiation, duplicate keys rejected).
//! - `xml` — XXE-hardened parse (`<!DOCTYPE>` rejected; no external-entity resolution)
//!   projected to a FEEL-walkable map (local names, repeated siblings → list, `@`-attributes).
//! - `csv` — comma-delimited, header row → a flat name→value row map. A flat format; it carries
//!   no composite/nested structure, so it is a format, never schema-bound.
//!
//! Each self-registers as a `sutra_codec_spi::BuiltinCodec` via `inventory` (the pull model), so
//! the neutral registry collects them generically. The concrete crate is bundled by the binary
//! (force-linked) so linker DCE does not drop the unreferenced registrations.
#![forbid(unsafe_code)]

pub mod csv;
pub mod json;
pub mod raw;
pub mod xml;
pub mod yaml;

pub use csv::CsvCodec;
pub use json::JsonCodec;
pub use raw::{RawBytesCodec, RawTextCodec};
pub use xml::XmlCodec;
pub use yaml::YamlCodec;
