//! The shared codec navigation-schema contract — the [`SchemaShape`] SPI.
//!
//! [`SchemaShape`] is the declared field contract of one message type, for the
//! `navigation ⇒ schema` static analysis at deploy time: a flat map of message-relative
//! dotted path → [`ShapeFieldType`], plus which object containers are *open* (accept unknown
//! children). Conservative by construction: anything not provable resolves to
//! [`PathResolution::Unverifiable`] (a warning upstream), never a false
//! [`PathResolution::UnknownInClosed`] error.
//!
//! Lifted out of the first schema-bound codec's own `shape` module when a second one landed —
//! every schema-bound codec now serves shapes over this one contract.

use std::collections::{BTreeMap, BTreeSet};

/// The coarse type of a declared field — enough for FEEL path existence + numeric-usage
/// checks (the field-type axis of the shape).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShapeFieldType {
    String,
    Number,
    Boolean,
    Object,
    Array,
    /// Unknown/unconstrained (xs:any, anyType, an unresolved ref) — never flagged.
    Any,
}

impl ShapeFieldType {
    /// Whether a value of this type can stand in a numeric (relational/arithmetic) FEEL
    /// position.
    pub fn is_numeric_compatible(&self) -> bool {
        matches!(self, ShapeFieldType::Number | ShapeFieldType::Any)
    }
}

/// The outcome of resolving a navigation path against a [`SchemaShape`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathResolution {
    /// The path is declared with this field type.
    DeclaredField(ShapeFieldType),
    /// The path is absent from a *closed* container — a provable typo (ERROR upstream).
    UnknownInClosed { container: String, path: String },
    /// The path cannot be verified (open container, wildcard, or it descends through a
    /// non-object) — WARN upstream.
    Unverifiable(String),
}

/// The declared field contract of one message type (paths are message-relative, e.g.
/// `body.Text.Field.Tag`; `""` names the root container).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaShape {
    paths: BTreeMap<String, ShapeFieldType>,
    open_containers: BTreeSet<String>,
}

impl SchemaShape {
    /// Declare `path` with `field_type`.
    pub fn path(mut self, path: &str, field_type: ShapeFieldType) -> SchemaShape {
        self.paths.insert(path.to_string(), field_type);
        self
    }

    /// Mark a container (`""` = root) as open — it accepts unknown children.
    pub fn open(mut self, container_path: &str) -> SchemaShape {
        self.open_containers.insert(container_path.to_string());
        self
    }

    /// All declared message-relative paths and their types.
    pub fn declared_paths(&self) -> &BTreeMap<String, ShapeFieldType> {
        &self.paths
    }

    /// The declared type of `path`, if any.
    pub fn type_of(&self, path: &str) -> Option<ShapeFieldType> {
        self.paths.get(path).copied()
    }

    /// Whether the object container at `container_path` (`""` = the message root) accepts
    /// unknown children.
    pub fn open_at(&self, container_path: &str) -> bool {
        self.open_containers.contains(container_path)
    }

    /// Resolve a message-relative path. A fully-declared path is
    /// [`PathResolution::DeclaredField`]; an undeclared leaf whose immediate container is a
    /// closed object is [`PathResolution::UnknownInClosed`] (a provable typo); everything
    /// else is [`PathResolution::Unverifiable`]. Conservative: when in doubt, unverifiable
    /// (warn), never error.
    pub fn resolve(&self, path: &str) -> PathResolution {
        if path.is_empty() {
            return PathResolution::Unverifiable("empty path".to_string());
        }
        if let Some(t) = self.paths.get(path) {
            return PathResolution::DeclaredField(*t);
        }
        let container = match path.rfind('.') {
            Some(dot) => &path[..dot],
            None => "",
        };
        if self.is_closed_object_container(container) {
            return PathResolution::UnknownInClosed {
                container: container.to_string(),
                path: path.to_string(),
            };
        }
        PathResolution::Unverifiable(format!("path '{path}' is not provable against this schema"))
    }

    /// The root (`""`) is closed unless declared open; a nested container must be a
    /// declared, closed OBJECT.
    fn is_closed_object_container(&self, container: &str) -> bool {
        if container.is_empty() {
            return !self.open_containers.contains("");
        }
        self.paths.get(container) == Some(&ShapeFieldType::Object)
            && !self.open_containers.contains(container)
    }
}
