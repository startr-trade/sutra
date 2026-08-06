//! Navigation-shape emission — the second back-end over the compiled schema model.
//!
//! Emits, per message-type root, the flat table the T3 (`navigation ⇒ schema`) static
//! analysis consumes: message-relative dotted path → field kind, plus the set of *open*
//! containers (whose unknown children warn instead of erroring). The variants mirror
//! `sutra_codec_spi::shape::ShapeFieldType` one-to-one so the post-merge consumer swap in
//! `sutra-codec-spi` is mechanical (this crate stays dependency-free of the codec crate —
//! the codec crate will depend on this one).
//!
//! Semantics are conservative by construction (warn, never false-error):
//!
//! - a `choice` anywhere in a type's content marks that container open (a missing
//!   branch sibling is not a typo) while still declaring every branch member;
//! - `xs:any` marks the container open;
//! - a repeated element (`maxOccurs` > 1) declares as [`FieldKind::Array`] and is not
//!   descended (the flat model);
//! - `simpleContent` elements declare as an open [`FieldKind::Object`] (attribute plus
//!   text projection);
//! - recursion and depth beyond [`MAX_DEPTH`] leave the container open.
//!
//! This supersedes both the first-cut roots+coercion scan in
//! `sutra-codec-spi/src/structural.rs` (see [`Schema::value_coercion`] for that half) and
//! the reference implementation’s runtime schema-walking shape builder, with one deliberate
//! improvement: inline groups nested *inside* a sequence are walked too (their members
//! are declared; a nested choice opens the container), where the predecessor ignored
//! them — strictly fewer false warnings, still never a false error.

use std::collections::{BTreeMap, BTreeSet};

use crate::datatype::Builtin;
use crate::model::{Content, GroupDef, GroupKind, Max, Particle, Resolved, Schema, TypeRef};

/// Depth cap: schema trees are deep but finite; beyond this the container stays open
/// with no deeper declarations.
const MAX_DEPTH: usize = 24;

/// The coarse declared kind of one field path. Mirrors
/// `sutra_codec_spi::shape::ShapeFieldType` variant-for-variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    String,
    Number,
    Boolean,
    Object,
    Array,
    /// Unknown/unconstrained — never flagged by the analysis.
    Any,
}

/// The declared navigation contract of one message-type root: a flat path table plus
/// the open-container set (`""` names the root container).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NavigationShape {
    pub paths: BTreeMap<String, FieldKind>,
    pub open: BTreeSet<String>,
}

/// The value-coercion index for multi-format decode: element names whose declared
/// type resolves to a numeric or boolean builtin (anywhere in the schema), so string
/// leaves parsed from JSON/YAML/XML coerce to the FEEL-arithmetic-friendly kind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ValueCoercion {
    pub number_elements: BTreeSet<String>,
    pub boolean_elements: BTreeSet<String>,
}

impl Schema {
    /// Build the navigation shape rooted at the global element `root`. An unknown root
    /// yields the fully-open shape (everything unverifiable — warn, never error).
    pub fn navigation_shape(&self, root: &str) -> NavigationShape {
        let mut shape = NavigationShape::default();
        let Some(decl) = self.roots.get(root) else {
            shape.open.insert(String::new());
            return shape;
        };
        match self.resolve(&decl.type_ref) {
            Resolved::Complex(complex) => {
                let mut stack: Vec<*const GroupDef> = Vec::new();
                self.walk_content(&complex.content, "", &mut shape, &mut stack, 0);
            }
            _ => {
                // A scalar root has nothing to descend into; leave it open (opaque).
                shape.open.insert(String::new());
            }
        }
        shape
    }

    /// The coercion index superseding the structural first-cut scan: every element
    /// declaration (global or local, through restriction chains) whose type resolves
    /// to a numeric builtin feeds `number_elements`, boolean likewise.
    pub fn value_coercion(&self) -> ValueCoercion {
        let mut coercion = ValueCoercion::default();
        for decl in self.roots.values() {
            self.coerce_element(&decl.name, &decl.type_ref, &mut coercion);
            // Roots with inline complex types carry locals the named-type walk below
            // never sees.
            if let Resolved::Complex(complex) = self.resolve(&decl.type_ref) {
                if let Content::Group(group) = &complex.content {
                    self.coerce_group(group, &mut coercion);
                }
            }
        }
        for def in self.types.values() {
            if let crate::model::TypeDef::Complex(complex) = def {
                if let Content::Group(group) = &complex.content {
                    self.coerce_group(group, &mut coercion);
                }
            }
        }
        coercion
    }

    /// Ordered child-element names per element path, for canonicalising a transcoded
    /// (JSON/YAML) tree into schema-sequence order before instance validation — the module
    /// XSD codec swap consumes this so a serde tree (unordered map keys) is re-emitted in the
    /// declared order the streaming validator expects. Key `""` is the root's direct children
    /// in declared order; a nested complex element's dotted path (e.g. `Item`) maps to its own
    /// children. An unknown root yields an empty map (no ordering constraint to apply).
    pub fn child_order(&self, root: &str) -> BTreeMap<String, Vec<String>> {
        let mut order: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let Some(decl) = self.roots.get(root) else {
            return order;
        };
        if let Resolved::Complex(complex) = self.resolve(&decl.type_ref) {
            let mut stack: Vec<*const GroupDef> = Vec::new();
            self.order_content(&complex.content, "", &mut order, &mut stack, 0);
        }
        order
    }

    fn order_content(
        &self,
        content: &Content,
        prefix: &str,
        order: &mut BTreeMap<String, Vec<String>>,
        stack: &mut Vec<*const GroupDef>,
        depth: usize,
    ) {
        if let Content::Group(group) = content {
            if depth > MAX_DEPTH || stack.contains(&(group as *const GroupDef)) {
                return;
            }
            stack.push(group as *const GroupDef);
            self.order_group(group, prefix, order, stack, depth);
            stack.pop();
        }
    }

    fn order_group(
        &self,
        group: &GroupDef,
        prefix: &str,
        order: &mut BTreeMap<String, Vec<String>>,
        stack: &mut Vec<*const GroupDef>,
        depth: usize,
    ) {
        for item in &group.items {
            match item {
                Particle::Element(decl) => {
                    order
                        .entry(prefix.to_string())
                        .or_default()
                        .push(decl.name.clone());
                    let path = if prefix.is_empty() {
                        decl.name.clone()
                    } else {
                        format!("{prefix}.{}", decl.name)
                    };
                    if let Resolved::Complex(complex) = self.resolve(&decl.type_ref) {
                        self.order_content(&complex.content, &path, order, stack, depth + 1);
                    }
                }
                Particle::Group(sub) => self.order_group(sub, prefix, order, stack, depth),
                Particle::Wildcard { .. } => {}
            }
        }
    }

    fn coerce_group(&self, group: &GroupDef, coercion: &mut ValueCoercion) {
        for item in &group.items {
            match item {
                Particle::Element(decl) => {
                    self.coerce_element(&decl.name, &decl.type_ref, coercion);
                    // Inline complex types hold nested locals of their own.
                    if let TypeRef::Inline(def) = &decl.type_ref {
                        if let crate::model::TypeDef::Complex(complex) = def.as_ref() {
                            if let Content::Group(sub) = &complex.content {
                                self.coerce_group(sub, coercion);
                            }
                        }
                    }
                }
                Particle::Group(sub) => self.coerce_group(sub, coercion),
                Particle::Wildcard { .. } => {}
            }
        }
    }

    fn coerce_element(&self, name: &str, type_ref: &TypeRef, coercion: &mut ValueCoercion) {
        let builtin = match self.resolve(type_ref) {
            Resolved::Builtin(builtin) => builtin,
            Resolved::Simple(simple) => simple.builtin,
            Resolved::Complex(complex) => match &complex.content {
                Content::Simple(simple) => simple.builtin,
                _ => return,
            },
        };
        if builtin == Builtin::Boolean {
            coercion.boolean_elements.insert(name.to_string());
        } else if builtin.is_numeric() {
            coercion.number_elements.insert(name.to_string());
        }
    }

    fn walk_content(
        &self,
        content: &Content,
        prefix: &str,
        shape: &mut NavigationShape,
        stack: &mut Vec<*const GroupDef>,
        depth: usize,
    ) {
        match content {
            Content::Simple(_) | Content::Empty => {
                // simpleContent projects as attributes + text; empty content declares
                // nothing. Both stay open so their projection never false-errors.
                shape.open.insert(prefix.to_string());
            }
            Content::Group(group) => {
                if depth > MAX_DEPTH || stack.contains(&(group as *const GroupDef)) {
                    shape.open.insert(prefix.to_string());
                    return;
                }
                stack.push(group as *const GroupDef);
                self.walk_group(group, prefix, shape, stack, depth);
                stack.pop();
            }
        }
    }

    fn walk_group(
        &self,
        group: &GroupDef,
        prefix: &str,
        shape: &mut NavigationShape,
        stack: &mut Vec<*const GroupDef>,
        depth: usize,
    ) {
        if group.kind == GroupKind::Choice {
            // "One of" content: absence of a branch sibling is never a typo.
            shape.open.insert(prefix.to_string());
        }
        for item in &group.items {
            match item {
                Particle::Wildcard { .. } => {
                    shape.open.insert(prefix.to_string());
                }
                Particle::Group(sub) => self.walk_group(sub, prefix, shape, stack, depth),
                Particle::Element(decl) => {
                    let path = if prefix.is_empty() {
                        decl.name.clone()
                    } else {
                        format!("{prefix}.{}", decl.name)
                    };
                    let repeated = !matches!(decl.occurs.max, Max::Bounded(0 | 1));
                    if repeated {
                        // Flat model: arrays are declared, their items not descended.
                        shape.paths.insert(path, FieldKind::Array);
                        continue;
                    }
                    match self.resolve(&decl.type_ref) {
                        Resolved::Builtin(builtin) => {
                            shape.paths.insert(path, scalar_kind(builtin));
                        }
                        Resolved::Simple(simple) => {
                            shape.paths.insert(path, scalar_kind(simple.builtin));
                        }
                        Resolved::Complex(complex) => {
                            shape.paths.insert(path.clone(), FieldKind::Object);
                            self.walk_content(&complex.content, &path, shape, stack, depth + 1);
                        }
                    }
                }
            }
        }
    }
}

/// Builtin → coarse field kind: numeric builtins are Number, boolean Boolean, and
/// every other lexical space (strings, dates, binary) reads as text.
fn scalar_kind(builtin: Builtin) -> FieldKind {
    if builtin == Builtin::Boolean {
        FieldKind::Boolean
    } else if builtin.is_numeric() {
        FieldKind::Number
    } else {
        FieldKind::String
    }
}
