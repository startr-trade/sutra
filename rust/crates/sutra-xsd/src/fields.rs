//! Declared-field enumeration — the third back-end over the compiled schema model, beside
//! the streaming validator ([`crate::validate`]) and navigation-shape emission
//! ([`crate::shape`]).
//!
//! [`crate::shape`] answers "what paths exist, coarsely" for the navigation checks, which is
//! deliberately lossy: it flattens a whole message to dotted paths and keeps only a five-way
//! [`FieldKind`](crate::FieldKind). This module answers the *other* question — "what are one
//! type's own children, precisely" — and keeps what column typing needs: the builtin, the
//! **effective** facets down the restriction chain, the occurrence bounds, attribute-ness and
//! `choice` membership.
//!
//! It is one level deep by construction. A child whose declared type is complex reports as
//! [`FieldShape::Complex`] and is never descended, so this back-end cannot recurse, cannot
//! cycle, and needs no depth cap.
//!
//! ## Stability
//!
//! [`FieldDecl`], [`FieldShape`] and [`FieldFacets`] are the published vocabulary consumers
//! project schemas through; the internal model types ([`crate::model`], [`crate::facet`]) stay
//! private. New facet kinds and shapes are additive changes to these three types, never a
//! widening of the model surface.

use crate::datatype::Builtin;
use crate::facet::FacetStep;
use crate::model::{
    Content, GroupDef, GroupKind, Max, Particle, Resolved, Schema, SimpleDef, TypeDef, TypeRef,
};

/// The synthetic field name a `simpleContent` type's character content is reported under.
///
/// Such a type is *text plus attributes*: the text is a scalar with no declared name, so it can
/// carry no column of its own and the type is not flatly projectable. Reporting it as a named
/// child with shape [`FieldShape::Complex`] makes that visible instead of silently dropping it.
/// The parentheses keep the name outside the XML NCName space, so it can never collide with a
/// real declaration.
pub const TEXT_CONTENT_FIELD: &str = "(text)";

/// The synthetic field name an open-content wildcard (`xs:any`) is reported under. Wildcards
/// are unnamed; the parenthesised placeholder can never collide with a real declaration.
pub const WILDCARD_FIELD: &str = "(any)";

/// The **effective** value-space constraints of one scalar field — the facets accumulated down
/// its whole restriction chain, narrowest wins.
///
/// XSD keeps one facet step per derivation level and requires a value to satisfy every step;
/// this is that chain collapsed to the single tightest constraint per kind, which is what a
/// column type has to hold. `None` means the chain constrains that kind not at all.
///
/// Only the kinds a column type can act on are published. `pattern` and
/// `minInclusive`/`maxInclusive` are deliberately withheld: exposing them would leak `regex`
/// and `bigdecimal` types into this crate's public API for no column-typing gain. Both can be
/// added additively if a consumer needs them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldFacets {
    /// `xs:length` — the exact Unicode scalar count. The narrowest declaration in the chain
    /// (a well-formed chain restates one value; a contradictory one collapses to the smaller).
    pub length: Option<u64>,
    /// `xs:minLength` — the **largest** declared minimum in the chain (the narrowest floor).
    pub min_length: Option<u64>,
    /// `xs:maxLength` — the **smallest** declared maximum in the chain (the narrowest ceiling).
    pub max_length: Option<u64>,
    /// `xs:totalDigits` — the smallest declared total-digit cap in the chain.
    pub total_digits: Option<u64>,
    /// `xs:fractionDigits` — the smallest declared fraction-digit cap in the chain.
    pub fraction_digits: Option<u64>,
    /// `xs:enumeration` — the **intersection** of every step's allowed set, in the order the
    /// most-derived step declares it. A well-formed chain only narrows, so this equals the
    /// most-derived declaration; a contradictory one collapses to what all steps admit.
    pub enumeration: Option<Vec<String>>,
}

/// The declared shape of one field: a scalar leaf that can become a column, or one of the two
/// things that cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldShape {
    /// A scalar leaf — a builtin datatype plus the effective facets of its restriction chain.
    /// The only shape a typed column can be derived from.
    Scalar {
        /// The ultimate builtin the declared type restricts.
        builtin: Builtin,
        /// The chain's effective facets ([`FieldFacets`]).
        facets: FieldFacets,
    },
    /// Not a scalar leaf: a child whose declared type has element content, or is
    /// `simpleContent` (text plus attributes). Never descended.
    Complex,
    /// Open content — an `xs:any` wildcard. The child set is unbounded, so no closed column
    /// list can describe it.
    Any,
}

/// One declared child of a type: an attribute, a local element, or an open-content wildcard.
///
/// Occurrence bounds are **effective** — a member of a repeatable inner group carries that
/// group's repetition, so a `maxOccurs="1"` element inside an `<xs:sequence maxOccurs="3">`
/// reports `occurs_max: Some(3)`, which is what a flatness rule has to see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDecl {
    /// The declared name — an attribute or element local name, or one of
    /// [`TEXT_CONTENT_FIELD`] / [`WILDCARD_FIELD`] for the two unnamed shapes.
    pub name: String,
    /// Whether this child is an XML attribute rather than a child element.
    pub is_attribute: bool,
    /// Effective `minOccurs` (an optional attribute is `0`, a required one `1`).
    pub occurs_min: u32,
    /// Effective `maxOccurs`; `None` is `unbounded`.
    pub occurs_max: Option<u32>,
    /// Whether the child sits inside a `choice` (at any nesting depth within this type's own
    /// content model) — at most one branch of a choice is ever populated, so a member is
    /// optional in practice however its own `minOccurs` reads.
    pub in_choice: bool,
    /// The declared shape.
    pub shape: FieldShape,
}

impl FieldDecl {
    /// Whether the child may occur more than once (`maxOccurs` > 1 or `unbounded`).
    pub fn is_repeated(&self) -> bool {
        !matches!(self.occurs_max, Some(0 | 1))
    }

    /// Whether the child may be absent — `minOccurs = 0`, or membership in a `choice` (whose
    /// unselected branches are absent by construction).
    pub fn is_optional(&self) -> bool {
        self.occurs_min == 0 || self.in_choice
    }

    /// The scalar builtin and effective facets, when this child is a scalar leaf.
    pub fn scalar(&self) -> Option<(Builtin, &FieldFacets)> {
        match &self.shape {
            FieldShape::Scalar { builtin, facets } => Some((*builtin, facets)),
            _ => None,
        }
    }
}

impl Schema {
    /// Enumerate the declared children of one type, in **declared order**: the attributes
    /// first (declaration order), then the content model's element particles and wildcards
    /// (declaration order, inner `sequence`/`choice` groups flattened in place — the same
    /// flattening [`Schema::child_order`](crate::Schema::child_order) applies).
    ///
    /// `type_or_root_name` is resolved against the **global element roots first**, then the
    /// named-type table — the two symbol spaces a `structure`-style declaration may name.
    ///
    /// Returns:
    ///
    /// - `None` — the name is neither a global element nor a named type of this schema.
    /// - `Some(vec![])` — the name resolves to something with no declared children at all: a
    ///   simple type, a builtin-typed root, or a `complexType` with empty content and no
    ///   attributes. Callers that need a record shape should treat an empty list as "nothing
    ///   to project", not as "an empty record".
    /// - `Some(fields)` otherwise.
    ///
    /// The walk is one level deep: a child whose type is complex reports as
    /// [`FieldShape::Complex`] and is not descended.
    ///
    /// A `simpleContent` type contributes its attributes plus one entry named
    /// [`TEXT_CONTENT_FIELD`] with shape [`FieldShape::Complex`] — see that constant for why.
    pub fn fields_of(&self, type_or_root_name: &str) -> Option<Vec<FieldDecl>> {
        let complex = match self.roots.get(type_or_root_name) {
            Some(decl) => match self.resolve(&decl.type_ref) {
                Resolved::Complex(complex) => complex,
                // A scalar-typed root declares no named children.
                Resolved::Builtin(_) | Resolved::Simple(_) => return Some(Vec::new()),
            },
            None => match self.types.get(type_or_root_name)? {
                TypeDef::Complex(complex) => complex,
                TypeDef::Simple(_) => return Some(Vec::new()),
            },
        };

        let mut fields = Vec::with_capacity(complex.attributes.len() + 4);
        for attr in &complex.attributes {
            fields.push(FieldDecl {
                name: attr.name.clone(),
                is_attribute: true,
                occurs_min: u32::from(attr.required),
                occurs_max: Some(1),
                in_choice: false,
                shape: simple_shape(&attr.value_type),
            });
        }
        match &complex.content {
            Content::Empty => {}
            Content::Simple(_) => fields.push(FieldDecl {
                name: TEXT_CONTENT_FIELD.to_string(),
                is_attribute: false,
                occurs_min: 1,
                occurs_max: Some(1),
                in_choice: false,
                shape: FieldShape::Complex,
            }),
            Content::Group(group) => self.collect_group(group, 1, Some(1), false, &mut fields),
        }
        Some(fields)
    }

    /// Flatten one content-model group into `out`, multiplying the enclosing occurrence
    /// bounds through so a member of a repeatable group reports as repeated.
    fn collect_group(
        &self,
        group: &GroupDef,
        outer_min: u32,
        outer_max: Option<u32>,
        outer_choice: bool,
        out: &mut Vec<FieldDecl>,
    ) {
        let min = outer_min.saturating_mul(group.occurs.min);
        let max = mul_max(outer_max, group.occurs.max);
        let in_choice = outer_choice || group.kind == GroupKind::Choice;
        for item in &group.items {
            match item {
                Particle::Group(sub) => self.collect_group(sub, min, max, in_choice, out),
                Particle::Wildcard { occurs, .. } => out.push(FieldDecl {
                    name: WILDCARD_FIELD.to_string(),
                    is_attribute: false,
                    occurs_min: min.saturating_mul(occurs.min),
                    occurs_max: mul_max(max, occurs.max),
                    in_choice,
                    shape: FieldShape::Any,
                }),
                Particle::Element(decl) => out.push(FieldDecl {
                    name: decl.name.clone(),
                    is_attribute: false,
                    occurs_min: min.saturating_mul(decl.occurs.min),
                    occurs_max: mul_max(max, decl.occurs.max),
                    in_choice,
                    shape: self.element_shape(&decl.type_ref),
                }),
            }
        }
    }

    fn element_shape(&self, type_ref: &TypeRef) -> FieldShape {
        match self.resolve(type_ref) {
            Resolved::Builtin(builtin) => FieldShape::Scalar {
                builtin,
                facets: FieldFacets::default(),
            },
            Resolved::Simple(simple) => simple_shape(simple),
            Resolved::Complex(_) => FieldShape::Complex,
        }
    }
}

fn simple_shape(simple: &SimpleDef) -> FieldShape {
    FieldShape::Scalar {
        builtin: simple.builtin,
        facets: effective_facets(&simple.steps),
    }
}

/// Collapse a restriction chain's per-step facets to the single narrowest constraint of each
/// kind. Order-independent by construction (min/max/intersection are commutative), so it does
/// not depend on which end of `steps` the most-derived level sits at.
fn effective_facets(steps: &[FacetStep]) -> FieldFacets {
    let mut effective = FieldFacets::default();
    for step in steps {
        effective.length = narrowest(effective.length, step.length, u64::min);
        effective.min_length = narrowest(effective.min_length, step.min_length, u64::max);
        effective.max_length = narrowest(effective.max_length, step.max_length, u64::min);
        effective.total_digits = narrowest(effective.total_digits, step.total_digits, u64::min);
        effective.fraction_digits =
            narrowest(effective.fraction_digits, step.fraction_digits, u64::min);
        effective.enumeration =
            intersect(effective.enumeration.take(), step.enumeration.as_deref());
    }
    effective
}

/// Combine an accumulated bound with one step's, `tighten` picking the narrower direction.
/// Either side being unconstrained yields the other.
fn narrowest(
    accumulated: Option<u64>,
    step: Option<u64>,
    tighten: fn(u64, u64) -> u64,
) -> Option<u64> {
    match (accumulated, step) {
        (Some(a), Some(b)) => Some(tighten(a, b)),
        (a, None) => a,
        (None, b) => b,
    }
}

/// Intersect the accumulated enumeration with one step's, preserving the accumulated order (so
/// the first step that declares one fixes the reported order). A step that declares no
/// enumeration constrains nothing and is skipped.
fn intersect(accumulated: Option<Vec<String>>, step: Option<&[String]>) -> Option<Vec<String>> {
    match (accumulated, step) {
        (accumulated, None) => accumulated,
        (None, Some(step)) => Some(step.to_vec()),
        (Some(accumulated), Some(step)) => Some(
            accumulated
                .into_iter()
                .filter(|value| step.contains(value))
                .collect(),
        ),
    }
}

/// Multiply an effective `maxOccurs` by an inner particle's. `None` (unbounded) on either side
/// is absorbing; bounded products saturate rather than wrap.
fn mul_max(outer: Option<u32>, inner: Max) -> Option<u32> {
    match (outer, inner) {
        (_, Max::Unbounded) | (None, _) => None,
        (Some(outer), Max::Bounded(inner)) => Some(outer.saturating_mul(inner)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn narrowest_picks_the_tighter_bound_either_way_round() {
        assert_eq!(narrowest(Some(35), Some(10), u64::min), Some(10));
        assert_eq!(narrowest(Some(10), Some(35), u64::min), Some(10));
        assert_eq!(narrowest(Some(1), Some(3), u64::max), Some(3));
        assert_eq!(narrowest(None, Some(7), u64::min), Some(7));
        assert_eq!(narrowest(Some(7), None, u64::min), Some(7));
        assert_eq!(narrowest(None, None, u64::min), None);
    }

    #[test]
    fn intersect_narrows_and_keeps_the_first_declared_order() {
        let base = Some(vec!["C".to_string(), "A".to_string(), "B".to_string()]);
        let step = ["A".to_string(), "B".to_string()];
        assert_eq!(
            intersect(base, Some(&step)),
            Some(vec!["A".to_string(), "B".to_string()])
        );
        // A step declaring no enumeration constrains nothing.
        assert_eq!(
            intersect(Some(vec!["A".to_string()]), None),
            Some(vec!["A".to_string()])
        );
    }

    #[test]
    fn mul_max_saturates_and_absorbs_unbounded() {
        assert_eq!(mul_max(Some(1), Max::Bounded(1)), Some(1));
        assert_eq!(mul_max(Some(3), Max::Bounded(4)), Some(12));
        assert_eq!(mul_max(Some(2), Max::Unbounded), None);
        assert_eq!(mul_max(None, Max::Bounded(1)), None);
        assert_eq!(mul_max(Some(u32::MAX), Max::Bounded(2)), Some(u32::MAX));
    }
}
