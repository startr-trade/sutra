//! The outcome of a codec decode.

use std::collections::BTreeMap;

use sutra_feel::value::FeelValue;

use crate::issue::ValidationIssue;

/// Decode outcome per the three-way contract: `OK` (payload, no issues),
/// `SOFT_ERRORS` (usable payload + routable structural issues), `FATAL` (no payload;
/// issues carry the cause — the engine rejects per the configured posture).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeOutcome {
    Ok,
    SoftErrors,
    Fatal,
}

/// The typed payload a built-in codec produces. Where the contract is generically typed
/// (string / bytes / JSON value / DOM projection), this closed set makes the variants
/// explicit. `Json` doubles as the FEEL-walkable tree for `json`, `yaml`, and the `xml`
/// map projection (numbers keep arbitrary precision via `serde_json`).
#[derive(Debug, Clone, PartialEq)]
pub enum CodecValue {
    Text(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

/// The scalar kinds a declared message attribute may take — deliberately narrow, because the
/// deploy-time selector analysis reasons about attribute domains as finite sets and cannot do
/// that over arbitrary structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributeKind {
    Text,
    Integer,
    Decimal,
    Boolean,
}

/// One literal in a declared attribute domain.
///
/// `Eq + Ord` is load-bearing: the deploy-time analysis holds domains in sorted sets to decide
/// whether two intake predicates could both match one message. That ordering exists for set
/// membership and dedup ONLY — `Decimal` carries a canonical decimal string, so its `Ord` is
/// lexicographic rather than numeric, and the analyser normalises before comparing magnitudes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AttributeLiteral {
    Text(String),
    Integer(i64),
    Decimal(String),
    Boolean(bool),
}

/// A codec's declaration of ONE attribute it can report.
///
/// Mirrors [`crate::PayloadCodec::declared_message_types`]: an empty `domain` leaves the value
/// side OPEN, a non-empty one CLOSES it — which is what lets the deploy-time lint refuse a
/// predicate comparing against a value this codec can never emit, instead of letting the start
/// event sit silently unreachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeSpec {
    /// The name a predicate spells after its `attr.` root.
    pub key: String,
    pub kind: AttributeKind,
    /// The closed set of values, or empty for an open value side.
    pub domain: Vec<AttributeLiteral>,
    /// `false` when a message may legitimately carry no value for this attribute. A predicate
    /// reading an absent attribute sees FEEL `null`, and a comparison against null is false, so
    /// the start event simply does not match.
    pub required: bool,
}

/// Attribute names are `[A-Za-z_][A-Za-z0-9_]*`.
///
/// Not a style rule. An intake predicate is parsed once and evaluated per message, while FEEL's
/// names-with-spaces resolution happens during lexing against the names then in scope — so a key
/// containing a space would resolve differently on the precompiled path than at every other FEEL
/// site in the engine. Forbidding the character class removes that divergence rather than
/// documenting it.
pub fn is_valid_attribute_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Decode result — outcome + optional typed payload + accumulated tier-1 issues +
/// effective content type + the recognised message type (multi-type codecs) + the message
/// attributes a start event's intake predicate selects on.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodeResult {
    pub outcome: DecodeOutcome,
    pub payload: Option<CodecValue>,
    pub issues: Vec<ValidationIssue>,
    pub content_type: String,
    pub message_type: Option<String>,
    /// Facts about THIS message that a codec recognised and the engine routes on: the
    /// classification a single `message_type` string cannot carry (a message type that occurs in
    /// more than one direction, say, where the direction decides which process handles it).
    ///
    /// Carried as FEEL values so the intake predicate evaluates them with no conversion and the
    /// process reads exactly what the filter matched. Keys obey [`is_valid_attribute_key`];
    /// `BTreeMap` so iteration order is deterministic, which matters because these values reach
    /// diagnostics and the audit trail.
    ///
    /// A codec that reports a value outside [`AttributeKind`]'s four scalars is not refused, but
    /// the deploy-time analysis cannot reason about it and degrades to a warning.
    pub attributes: BTreeMap<String, FeelValue>,
}

impl DecodeResult {
    pub fn ok(payload: CodecValue, content_type: &str) -> DecodeResult {
        DecodeResult {
            outcome: DecodeOutcome::Ok,
            payload: Some(payload),
            issues: Vec::new(),
            content_type: content_type.to_string(),
            message_type: None,
            attributes: BTreeMap::new(),
        }
    }

    pub fn soft_errors(
        payload: CodecValue,
        issues: Vec<ValidationIssue>,
        content_type: &str,
    ) -> DecodeResult {
        DecodeResult {
            outcome: DecodeOutcome::SoftErrors,
            payload: Some(payload),
            issues,
            content_type: content_type.to_string(),
            message_type: None,
            attributes: BTreeMap::new(),
        }
    }

    pub fn fatal(issues: Vec<ValidationIssue>, content_type: &str) -> DecodeResult {
        DecodeResult {
            outcome: DecodeOutcome::Fatal,
            payload: None,
            issues,
            content_type: content_type.to_string(),
            message_type: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Copy carrying the recognised `message_type` (blank clears it) — the
    /// message-type builder.
    pub fn with_message_type(mut self, message_type: &str) -> DecodeResult {
        let t = message_type.trim();
        self.message_type = if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        };
        self
    }

    /// Copy carrying one more recognised attribute; a later write to the same key wins.
    ///
    /// A key failing [`is_valid_attribute_key`] is DROPPED rather than stored, because an
    /// unreachable key would route nothing and an invalid one cannot be written in a predicate
    /// at all — there is no value in carrying it. Codecs validate their own vocabulary with
    /// [`is_valid_attribute_key`], and the deploy-time lint checks the declared side.
    pub fn with_attribute(mut self, key: &str, value: FeelValue) -> DecodeResult {
        if is_valid_attribute_key(key) {
            self.attributes.insert(key.to_string(), value);
        }
        self
    }

    /// Copy carrying a whole attribute map, merged over whatever is already there. Invalid keys
    /// are dropped, as in [`Self::with_attribute`].
    pub fn with_attributes(
        mut self,
        attributes: impl IntoIterator<Item = (String, FeelValue)>,
    ) -> DecodeResult {
        for (key, value) in attributes {
            if is_valid_attribute_key(&key) {
                self.attributes.insert(key, value);
            }
        }
        self
    }
}
