//! The format × schema composition SPI —
//! `MessageFormat`, `MessageSchema`, `SchemaBoundCodec`, `FormatOnlyCodec`, `SchemaKind`.
//!
//! A [`MessageFormat`] is a pure parser (bytes ⇄ a format-native tree); a [`MessageSchema`]
//! is a typed contract (validate + project to a FEEL-navigable map, and the inverse). Bound
//! together they are a [`SchemaBoundCodec`] — a [`PayloadCodec`] that decodes to a validated,
//! FEEL-navigable map and stamps the schema's message type. Used bare (no schema), a format
//! is a degenerate [`FormatOnlyCodec`] (opaque, no typed message type).
//!
//! Unlike a tree-generic SPI (generic over the parse-tree type `R`), this design fixes the tree to
//! [`serde_json::Value`] — the universal FEEL-walkable tree every Rust format already produces
//! — so the composition needs no type parameter while pinning the same observable contract.

use std::sync::Arc;

use crate::codec::PayloadCodec;
use crate::issue::ValidationIssue;
use crate::result::{CodecValue, DecodeOutcome, DecodeResult};
use crate::shape::SchemaShape;

/// The schema family a [`MessageSchema`] belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaKind {
    Xsd,
    JsonSchema,
}

/// The observable **data shape** a format targets — the contract a channel declares when it binds
/// a bare format (no schema). Content negotiation keys on it: a channel declaring one member of a
/// class accepts any content-type whose parser lands in the same class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeClass {
    /// Opaque bytes / text — `raw-text` (`text/plain`), `raw-bytes` (`application/octet-stream`).
    /// No map projection; not interchangeable.
    Opaque,
    /// A (possibly nested) Map of name→value — `json`, `xml`, `yaml`. Interchangeable: the parser
    /// is chosen by the inbound content-type and the reply echoes it.
    NestedMap,
    /// A FLAT Map / rows of name→scalar — `csv`. Admits `text/csv` plus a *flat* json/xml/yaml
    /// instance; a nested inbound is rejected (the discriminator is flatness, not syntax).
    FlatMap,
}

/// A format's parse outcome: the format-native tree (absent on a FATAL parse) plus the
/// tier-0 issues. Formats produce `Ok` (tree present) or `Fatal` (tree absent) only — the
/// soft/hard schema tier belongs to the [`MessageSchema`].
#[derive(Debug, Clone, PartialEq)]
pub struct FormatParse {
    pub outcome: DecodeOutcome,
    pub tree: Option<serde_json::Value>,
    pub issues: Vec<ValidationIssue>,
    pub content_type: String,
}

impl FormatParse {
    pub fn ok(tree: serde_json::Value, content_type: &str) -> FormatParse {
        FormatParse {
            outcome: DecodeOutcome::Ok,
            tree: Some(tree),
            issues: Vec::new(),
            content_type: content_type.to_string(),
        }
    }

    pub fn fatal(issues: Vec<ValidationIssue>, content_type: &str) -> FormatParse {
        FormatParse {
            outcome: DecodeOutcome::Fatal,
            tree: None,
            issues,
            content_type: content_type.to_string(),
        }
    }

    /// Adapt an existing [`DecodeResult`] (a format's `PayloadCodec::decode`) into a
    /// [`FormatParse`] — the tree is the JSON payload (text lifts to a JSON string).
    pub fn from_decode(result: DecodeResult) -> FormatParse {
        let tree = match (&result.outcome, result.payload) {
            (DecodeOutcome::Fatal, _) => None,
            (_, Some(CodecValue::Json(v))) => Some(v),
            (_, Some(CodecValue::Text(s))) => Some(serde_json::Value::String(s)),
            _ => None,
        };
        FormatParse {
            outcome: result.outcome,
            tree,
            issues: result.issues,
            content_type: result.content_type,
        }
    }
}

/// A pure message format (bytes ⇄ tree). `Send + Sync` for the same reason
/// [`PayloadCodec`] is: a bound codec is shared across engine lanes, never rebuilt per lane.
pub trait MessageFormat: Send + Sync {
    fn name(&self) -> &str;

    fn accepted_content_types(&self) -> Vec<String>;

    /// Parse bytes into the format-native tree; a malformed input is a FATAL [`FormatParse`]
    /// (tree absent), never a panic.
    fn parse(&self, body: &[u8], content_type: Option<&str>) -> FormatParse;

    /// Serialize a tree back to bytes. The default is the unsupported-operation
    /// posture (a parse-only format) — `Err`.
    fn encode_tree(
        &self,
        _tree: &serde_json::Value,
        _content_type: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        Err(format!(
            "format '{}' does not support encode()",
            self.name()
        ))
    }
}

/// A typed schema contract over a format-native tree. `Send + Sync` for the same reason
/// [`PayloadCodec`] is: a compiled schema is immutable and shared across engine lanes.
pub trait MessageSchema: Send + Sync {
    fn kind(&self) -> SchemaKind;

    /// The single message type this schema declares, if any.
    fn message_type(&self) -> Option<String>;

    /// The navigation shape for `message_type`, when this schema declares it.
    fn shape_of(&self, message_type: &str) -> Option<SchemaShape>;

    /// Validate the parse tree and project it to a FEEL-navigable map. Recoverable schema
    /// violations are a `SOFT_ERRORS` [`DecodeResult`] (payload still projected + routable);
    /// the result is stamped with [`Self::message_type`].
    fn validate_and_project(&self, tree: &serde_json::Value, content_type: &str) -> DecodeResult;

    /// The inverse projection for outbound encode (map → format-native tree). The default is
    /// the unsupported-operation posture — `Err`.
    fn project_to_tree(
        &self,
        _payload: &serde_json::Value,
        _content_type: &str,
    ) -> Result<serde_json::Value, String> {
        Err("schema does not support projectToTree()".to_string())
    }
}

/// The typed half of the split: a [`MessageFormat`] bound to a [`MessageSchema`], exposed as a
/// [`PayloadCodec`].
pub struct SchemaBoundCodec {
    name: String,
    format: Arc<dyn MessageFormat>,
    schema: Arc<dyn MessageSchema>,
}

impl SchemaBoundCodec {
    pub fn new(
        name: impl Into<String>,
        format: Arc<dyn MessageFormat>,
        schema: Arc<dyn MessageSchema>,
    ) -> SchemaBoundCodec {
        let name = name.into();
        assert!(!name.trim().is_empty(), "codec name is required");
        SchemaBoundCodec {
            name,
            format,
            schema,
        }
    }

    /// The single message type this codec is bound to, when the schema declares one.
    pub fn declared_message_types(&self) -> Vec<String> {
        self.schema.message_type().into_iter().collect()
    }

    /// The navigation shape for `message_type`, delegated to the bound schema.
    pub fn shape_of(&self, message_type: &str) -> Option<SchemaShape> {
        self.schema.shape_of(message_type)
    }
}

impl PayloadCodec for SchemaBoundCodec {
    fn name(&self) -> &str {
        &self.name
    }

    fn accepted_content_types(&self) -> Vec<String> {
        self.format.accepted_content_types()
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let parsed = self.format.parse(body, content_type);
        match parsed.tree {
            None => {
                // Format-level parse failure — no tree to validate; propagate FATAL.
                let mut result = DecodeResult::fatal(parsed.issues, &parsed.content_type);
                if let Some(mt) = self.schema.message_type() {
                    result = result.with_message_type(&mt);
                }
                result
            }
            Some(tree) => self
                .schema
                .validate_and_project(&tree, &parsed.content_type),
        }
    }

    fn encode(&self, payload: &CodecValue, content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let value = codec_value_as_json(payload)?;
        let tree = self
            .schema
            .project_to_tree(&value, content_type.unwrap_or(""))?;
        self.format.encode_tree(&tree, content_type)
    }
}

/// A format used without a schema — the degenerate opaque mode:
/// it adopts the format's name and emits no typed message type.
pub struct FormatOnlyCodec {
    format: Arc<dyn MessageFormat>,
}

impl FormatOnlyCodec {
    pub fn new(format: Arc<dyn MessageFormat>) -> FormatOnlyCodec {
        FormatOnlyCodec { format }
    }
}

impl PayloadCodec for FormatOnlyCodec {
    fn name(&self) -> &str {
        self.format.name()
    }

    fn accepted_content_types(&self) -> Vec<String> {
        self.format.accepted_content_types()
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let parsed = self.format.parse(body, content_type);
        match parsed.tree {
            None => DecodeResult::fatal(parsed.issues, &parsed.content_type),
            Some(tree) => DecodeResult::ok(CodecValue::Json(tree), &parsed.content_type),
        }
    }

    fn encode(&self, payload: &CodecValue, content_type: Option<&str>) -> Result<Vec<u8>, String> {
        let value = codec_value_as_json(payload)?;
        self.format.encode_tree(&value, content_type)
    }
}

fn codec_value_as_json(payload: &CodecValue) -> Result<serde_json::Value, String> {
    match payload {
        CodecValue::Json(v) => Ok(v.clone()),
        CodecValue::Text(s) => Ok(serde_json::Value::String(s.clone())),
        CodecValue::Bytes(_) => {
            Err("schema-bound encode requires a map/tree payload, not raw bytes".to_string())
        }
    }
}
