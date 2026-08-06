//! The compiled schema model — one parsed representation feeding two back-ends: the
//! streaming instance validator ([`crate::validate`]) and navigation-shape emission
//! ([`crate::shape`]).

use std::collections::BTreeMap;

use crate::datatype::Builtin;
use crate::facet::FacetStep;

/// A compiled, self-contained schema: one target namespace, global element roots, and
/// the named-type table. Produced by [`Schema::compile`](crate::Schema::compile);
/// immutable and shareable across threads afterwards.
#[derive(Debug)]
pub struct Schema {
    pub(crate) target_namespace: String,
    /// Global element declarations (the message-type roots), by local name.
    pub(crate) roots: BTreeMap<String, ElementDecl>,
    /// Named types (simple and complex share one symbol space in XSD).
    pub(crate) types: BTreeMap<String, TypeDef>,
}

impl Schema {
    /// The schema's target namespace URI.
    pub fn target_namespace(&self) -> &str {
        &self.target_namespace
    }

    /// The global element names — the message types this schema can validate.
    pub fn root_names(&self) -> impl Iterator<Item = &str> {
        self.roots.keys().map(String::as_str)
    }

    pub(crate) fn resolve<'s>(&'s self, type_ref: &'s TypeRef) -> Resolved<'s> {
        match type_ref {
            TypeRef::Builtin(builtin) => Resolved::Builtin(*builtin),
            TypeRef::Inline(def) => Resolved::from_def(def),
            TypeRef::Named(name) => match self.types.get(name) {
                Some(def) => Resolved::from_def(def),
                // Compile guarantees resolvability; treat a gap defensively as string.
                None => Resolved::Builtin(Builtin::String),
            },
        }
    }
}

/// A resolved type reference.
pub(crate) enum Resolved<'s> {
    Builtin(Builtin),
    Simple(&'s SimpleDef),
    Complex(&'s ComplexDef),
}

impl<'s> Resolved<'s> {
    fn from_def(def: &'s TypeDef) -> Resolved<'s> {
        match def {
            TypeDef::Simple(simple) => Resolved::Simple(simple),
            TypeDef::Complex(complex) => Resolved::Complex(complex),
        }
    }
}

#[derive(Debug)]
pub(crate) enum TypeDef {
    Simple(SimpleDef),
    Complex(ComplexDef),
}

/// A simple type flattened through its restriction chain: the ultimate builtin plus
/// one facet step per derivation level (all steps must accept a value).
#[derive(Debug, Clone)]
pub(crate) struct SimpleDef {
    pub builtin: Builtin,
    pub steps: Vec<FacetStep>,
}

/// A complex type: content plus attribute declarations.
#[derive(Debug)]
pub(crate) struct ComplexDef {
    pub content: Content,
    pub attributes: Vec<AttrDecl>,
}

#[derive(Debug)]
pub(crate) enum Content {
    /// `simpleContent`/`extension`: character content typed by a simple type.
    Simple(SimpleDef),
    /// Element-only content: the top-level particle group.
    Group(GroupDef),
    /// No content model at all: the element must be empty.
    Empty,
}

#[derive(Debug)]
pub(crate) struct AttrDecl {
    pub name: String,
    pub value_type: SimpleDef,
    pub required: bool,
}

/// minOccurs/maxOccurs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Occurs {
    pub min: u32,
    pub max: Max,
}

impl Occurs {
    pub(crate) const ONE: Occurs = Occurs {
        min: 1,
        max: Max::Bounded(1),
    };

    pub(crate) fn admits(&self, count: u32) -> bool {
        match self.max {
            Max::Unbounded => true,
            Max::Bounded(max) => count < max,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Max {
    Bounded(u32),
    Unbounded,
}

#[derive(Debug)]
pub(crate) enum Particle {
    Element(ElementDecl),
    Group(GroupDef),
    /// `xs:any` — accept any well-formed subtree; `lax` re-validates a child whose
    /// declaration happens to be known, `skip` never does.
    Wildcard {
        occurs: Occurs,
        lax: bool,
    },
}

impl Particle {
    /// Whether this particle can match the empty sequence.
    pub(crate) fn nullable(&self) -> bool {
        match self {
            Particle::Element(decl) => decl.occurs.min == 0,
            Particle::Wildcard { occurs, .. } => occurs.min == 0,
            Particle::Group(group) => group.occurs.min == 0 || group.inner_nullable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupKind {
    Sequence,
    Choice,
}

#[derive(Debug)]
pub(crate) struct GroupDef {
    pub kind: GroupKind,
    pub items: Vec<Particle>,
    pub occurs: Occurs,
    /// Whether ONE repetition of this group can match the empty sequence
    /// (sequence: all items nullable; choice: any item nullable).
    pub inner_nullable: bool,
}

/// One element declaration (global root or local particle member).
#[derive(Debug)]
pub(crate) struct ElementDecl {
    pub name: String,
    pub type_ref: TypeRef,
    pub occurs: Occurs,
}

#[derive(Debug)]
pub(crate) enum TypeRef {
    Builtin(Builtin),
    /// A named type in this schema's symbol space.
    Named(String),
    /// An anonymous inline type.
    Inline(Box<TypeDef>),
}
