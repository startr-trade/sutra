//! The language-neutral schema model the emitter consumes — one [`ParseResult`] per
//! XSD document. It mirrors the requirements fixed for the ISO 20022 corpus: one
//! struct per named `complexType`, one enum per enumerated `simpleType`, fields in
//! document order carrying their XML name, resolved scalar/enum/nested type, occurrence
//! posture and simple-type facets.
//!
//! Only the fields the emitter reads are modelled; the corpus is flat (no inheritance,
//! element ref, substitution groups, xs:all, mixed content or anonymous types — verified
//! over all 116 schemas), so those postures are represented but never populated.

/// One parsed schema document.
#[derive(Debug, Default)]
pub struct ParseResult {
    pub target_namespace: Option<String>,
    pub classes: Vec<ClassModel>,
    pub enums: Vec<EnumModel>,
}

/// A named `complexType` → one generated struct + decoder + projection + shape row.
#[derive(Debug)]
pub struct ClassModel {
    /// PascalCase Rust type name.
    pub name: String,
    /// The raw XSD type name (drives the `*Choice` exactly-one-member detection).
    pub xml_type_name: String,
    /// The document-element name when a top-level `xs:element` binds this type as a root.
    pub root_element_name: Option<String>,
    /// Documentation carried onto the struct (absent throughout the ISO corpus).
    pub javadoc: Option<String>,
    pub fields: Vec<FieldModel>,
}

/// One field: an element, attribute, simple-content value, or `xs:any`.
#[derive(Debug, Default)]
pub struct FieldModel {
    /// The XML name (element/attribute local name, or `"value"` for simple content).
    pub xml_name: String,
    /// The resolved field type.
    pub field_type: FieldType,
    pub required: bool,
    pub is_list: bool,
    pub is_attribute: bool,
    pub is_xml_value: bool,
    pub is_any_element: bool,
    pub is_mixed: bool,
    pub has_substitution_members: bool,
    pub facets: Facets,
}

/// The resolved type of a field — sutra-native, no foreign type-name vocabulary.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum FieldType {
    /// A builtin-mapped scalar (possibly through a typedef alias chain).
    Scalar(Scalar),
    /// A same-schema named type — a complexType (nested struct) or enumerated
    /// simpleType (enum); which one is resolved at emit time.
    Named(String),
    /// Wildcard / unresolvable content: kept in structural specs, not decoded.
    #[default]
    Opaque,
}

/// The neutral scalar kinds the generated decoder/projection surface fixes. The
/// generated Rust representation per kind (`String`, `bool`, `i64`,
/// `bigdecimal::BigDecimal`, `Vec<u8>`) is the emit-side contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scalar {
    /// Text-shaped content (also URI/token/name-flavored builtins).
    Text,
    Boolean,
    /// Bounded integers (fits the generated `i64`).
    Int,
    /// Arbitrary-precision decimals (also float/double per the canonical mapping).
    Decimal,
    /// Unbounded integers — decimal-represented, integer-shaped in shape metadata.
    BigInt,
    /// Date/time-flavored builtins (kept as validated text).
    DateTime,
    Duration,
    QName,
    /// Base64/hex binary content.
    Bytes,
}

/// An enumerated `simpleType` → one generated enum with `from_xml`/`canonical_name`/`xml_value`.
#[derive(Debug)]
pub struct EnumModel {
    pub name: String,
    pub values: Vec<EnumValue>,
}

/// One enumeration value: its canonical constant name and the on-the-wire XML value.
#[derive(Debug)]
pub struct EnumValue {
    pub canonical_name: String,
    pub xml_value: String,
}

/// The `xs:restriction` facets that drive lexical/range/length checks.
#[derive(Debug, Default, Clone)]
pub struct Facets {
    pub min_length: Option<i64>,
    pub max_length: Option<i64>,
    pub length: Option<i64>,
    pub patterns: Vec<String>,
    pub min_inclusive: Option<String>,
    pub max_inclusive: Option<String>,
    pub min_exclusive: Option<String>,
    pub max_exclusive: Option<String>,
    pub total_digits: Option<i64>,
    pub fraction_digits: Option<i64>,
}

impl Facets {
    pub fn is_empty(&self) -> bool {
        self.min_length.is_none()
            && self.max_length.is_none()
            && self.length.is_none()
            && self.patterns.is_empty()
            && self.min_inclusive.is_none()
            && self.max_inclusive.is_none()
            && self.min_exclusive.is_none()
            && self.max_exclusive.is_none()
            && self.total_digits.is_none()
            && self.fraction_digits.is_none()
    }
}

/// Per-schema module fact registered during generation and consumed when emitting the
/// cross-schema `registry.rs` / `lib.rs`.
#[derive(Debug, Clone)]
pub struct ModuleInfo {
    pub module_name: String,
    pub namespace: String,
    pub message_type: String,
    /// The root struct name, or `None` when the schema binds no root element.
    pub root_rust_type: Option<String>,
}
