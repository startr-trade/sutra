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
//! - `fixed-width` — fixed-column records → one flat name→value map per line, and the inverse on
//!   encode. Schema-bindable like `csv`, but it carries NO zero-config default and so registers no
//!   `BuiltinFormat`: without the column widths a line is an undifferentiated string, so a channel
//!   can only reach it through a schema codec whose manifest declares the layout.
//! - `csv` — comma-delimited, header row → a flat name→value row map, and the inverse on encode
//!   (so a csv channel can answer in csv). A flat format: it carries no composite structure of
//!   its own, but it IS schema-bindable — a tabular body is a BATCH, validated row-wise against
//!   the bound schema's declared root (design `schema-format-binding.md`), which is what turns
//!   untyped cells into typed fields.
//!
//! Each self-registers as a `sutra_codec_spi::BuiltinCodec` via `inventory` (the pull model), so
//! the neutral registry collects them generically. The concrete crate is bundled by the binary
//! (force-linked) so linker DCE does not drop the unreferenced registrations.
#![forbid(unsafe_code)]

pub mod csv;
pub mod fixed_width;
pub mod json;
pub mod raw;
pub mod xml;
pub mod yaml;

pub use csv::CsvCodec;
pub use fixed_width::{FixedWidthCodec, FixedWidthField};
pub use json::JsonCodec;
pub use raw::{RawBytesCodec, RawTextCodec};
pub use xml::XmlCodec;
pub use yaml::YamlCodec;
