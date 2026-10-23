//! Intake-selector analysis — the decision procedure behind the package-time
//! `<q:source filterExpression>` checks.
//!
//! A start event's selector is `MT ∧ P`: the message-type clause
//! (`messageTypeValue` / `messageTypePattern`) conjoined with an optional FEEL predicate over the
//! attributes the bound codec reports. Two subscribers on one non-broadcast channel may both be
//! eligible for the same message unless one of the two conjuncts separates them, so disjointness
//! decomposes as `mt_disjoint ∨ pred_disjoint`. The message-type half is decided by the lint's
//! existing exact/pattern logic; this module decides the predicate half.
//!
//! # Recognised Predicate Normal Form
//!
//! Only a deliberately small language is analysed, because the whole value of the gate is that a
//! recognised predicate's *denotation is exactly its cube set* — no over- or under-approximation
//! that could let an overlapping pair package, and none that could reject a disjoint one:
//!
//! - `and` / `or` / `not` over atoms;
//! - `attr.<key> = <literal>`, `!= <literal>`;
//! - `attr.<key> in [<literal>, …]` (and its negation);
//! - `<` `<=` `>` `>=` against a numeric literal, on a key the codec declares with a numeric
//!   kind — a relational operator on a key of unknown or non-numeric kind is NOT recognised,
//!   because FEEL also orders strings and reading that as a number range would be unsound;
//! - a bare `attr.<key>` on a key the codec declares `Boolean`;
//! - `matches(attr.<key>, "<re>")` over a key whose declared domain is CLOSED, decided by
//!   evaluating the call against every domain member through the engine's own FEEL evaluator
//!   (the brute-force-membership trick the message-type pattern check already uses). Nothing
//!   here re-implements the XPath regex flavour, so the analysis cannot drift from the runtime;
//! - the literals `true` / `false`.
//!
//! **Anything else makes the WHOLE predicate [`Analysis::Opaque`]** — arithmetic, `if`,
//! quantifiers, `for`, function definitions, temporals, a path that is not a two-segment
//! `attr`-rooted reference. That is the soundness invariant: one unrecognised sub-expression and
//! no cube set is claimed at all. The gate is permanent and load-bearing, not a limitation
//! awaiting removal.
//!
//! # Extraction
//!
//! `not` is pushed inward as the AST is walked (a polarity flag, so nothing is rebuilt), the
//! result is expanded to disjunctive normal form, and each cube meets its atoms per key into the
//! [`Constraint`] lattice `Any | OneOf | NoneOf | NumRange | Unsat`. Expansion is capped at
//! [`MAX_CUBES`] cubes and [`MAX_ATOMS`] atoms: a pathological predicate becomes `Opaque`, never
//! a package-time hang.
//!
//! One refinement earns its keep: against a CLOSED declared domain, `NoneOf` normalises into
//! `OneOf` of the complement. Without it `attr.direction != "incoming"` and
//! `attr.direction != "outgoing"` are correctly not provably disjoint; with a closed
//! `{incoming, outgoing}` domain they are.
//!
//! The module is pure and I/O-free so `sutra-lint-core` can re-export it to the WASM LSP
//! unchanged.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bigdecimal::BigDecimal;
use sutra_codec_spi::{AttributeKind, AttributeLiteral, AttributeSpec};
use sutra_feel::ast::{CompareOp, FeelExpr, LogicalOp};
use sutra_feel::value::{FeelContext, FeelValue};

/// The DNF cube budget. Beyond it the predicate is [`Analysis::Opaque`].
pub const MAX_CUBES: usize = 64;
/// The atom budget. Beyond it the predicate is [`Analysis::Opaque`].
pub const MAX_ATOMS: usize = 32;

/// The root every attribute reference is spelled under, in a predicate and in the in-process
/// projection alike.
pub const ATTR_ROOT: &str = "attr";

// ---- literals ---------------------------------------------------------------------------

/// A scalar the analyser can reason about — the lattice's value domain.
///
/// FEEL has ONE number type, so [`AttributeKind::Integer`] and [`AttributeKind::Decimal`]
/// collapse to [`Lit::Number`] here; the integer/decimal distinction survives where it matters,
/// in the kind check ([`Lit::matches_kind`]). `Eq + Ord` is load-bearing (the lattice holds
/// domains in `BTreeSet`s) and, unlike [`AttributeLiteral::Decimal`]'s canonical string, this
/// ordering is NUMERIC: a declared decimal is parsed on the way in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Lit {
    Bool(bool),
    Number(BigDecimal),
    Text(String),
}

impl Lit {
    /// The declared-domain literal as a lattice literal. `None` when a
    /// [`AttributeLiteral::Decimal`] string does not parse — the domain then degrades to OPEN
    /// rather than silently losing a member, which would be the unsound direction.
    pub fn from_attribute_literal(literal: &AttributeLiteral) -> Option<Lit> {
        match literal {
            AttributeLiteral::Text(s) => Some(Lit::Text(s.clone())),
            AttributeLiteral::Boolean(b) => Some(Lit::Bool(*b)),
            AttributeLiteral::Integer(i) => Some(Lit::Number(BigDecimal::from(*i))),
            AttributeLiteral::Decimal(s) => s.parse::<BigDecimal>().ok().map(Lit::Number),
        }
    }

    /// A FEEL literal as a lattice literal. `None` for every non-scalar value (a temporal, a
    /// list, a context, a function, a range) — the caller turns that into `Opaque`.
    pub fn from_feel_value(value: &FeelValue) -> Option<Lit> {
        match value {
            FeelValue::Boolean(b) => Some(Lit::Bool(*b)),
            FeelValue::Number(n) => Some(Lit::Number(n.clone())),
            FeelValue::String(s) => Some(Lit::Text(s.clone())),
            _ => None,
        }
    }

    /// The literal as a FEEL value, for the brute-force membership evaluation.
    pub fn to_feel_value(&self) -> FeelValue {
        match self {
            Lit::Bool(b) => FeelValue::Boolean(*b),
            Lit::Number(n) => FeelValue::Number(n.clone()),
            Lit::Text(s) => FeelValue::String(s.clone()),
        }
    }

    /// Whether this literal inhabits a declared kind's value space.
    pub fn matches_kind(&self, kind: AttributeKind) -> bool {
        match (self, kind) {
            (Lit::Text(_), AttributeKind::Text) => true,
            (Lit::Bool(_), AttributeKind::Boolean) => true,
            (Lit::Number(n), AttributeKind::Integer) => n.is_integer(),
            (Lit::Number(_), AttributeKind::Decimal) => true,
            _ => false,
        }
    }

    /// The FEEL type name, for a type-mismatch diagnostic.
    pub fn type_name(&self) -> &'static str {
        match self {
            Lit::Bool(_) => "boolean",
            Lit::Number(_) => "number",
            Lit::Text(_) => "string",
        }
    }

    fn as_number(&self) -> Option<&BigDecimal> {
        match self {
            Lit::Number(n) => Some(n),
            _ => None,
        }
    }
}

impl fmt::Display for Lit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lit::Bool(b) => write!(f, "{b}"),
            Lit::Number(n) => write!(f, "{n}"),
            Lit::Text(s) => write!(f, "\"{s}\""),
        }
    }
}

/// The display name of a declared kind, for diagnostics.
pub fn kind_name(kind: AttributeKind) -> &'static str {
    match kind {
        AttributeKind::Text => "text",
        AttributeKind::Integer => "integer",
        AttributeKind::Decimal => "decimal",
        AttributeKind::Boolean => "boolean",
    }
}

/// A declared attribute's CLOSED value set, or `None` when the value side is open (an empty
/// `domain`) or a declared decimal does not parse.
pub fn closed_domain(spec: &AttributeSpec) -> Option<BTreeSet<Lit>> {
    if spec.domain.is_empty() {
        return None;
    }
    let mut out = BTreeSet::new();
    for literal in &spec.domain {
        out.insert(Lit::from_attribute_literal(literal)?);
    }
    Some(out)
}

// ---- numeric bounds ---------------------------------------------------------------------

/// One end of a [`NumBounds`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumBound {
    pub value: BigDecimal,
    pub inclusive: bool,
}

/// A possibly half-open numeric interval — the `NumRange` constraint's payload.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NumBounds {
    pub low: Option<NumBound>,
    pub high: Option<NumBound>,
}

impl NumBounds {
    fn at_least(value: BigDecimal, inclusive: bool) -> NumBounds {
        NumBounds {
            low: Some(NumBound { value, inclusive }),
            high: None,
        }
    }

    fn at_most(value: BigDecimal, inclusive: bool) -> NumBounds {
        NumBounds {
            low: None,
            high: Some(NumBound { value, inclusive }),
        }
    }

    /// Whether a number inhabits the interval.
    pub fn contains(&self, n: &BigDecimal) -> bool {
        if let Some(low) = &self.low {
            match n.cmp(&low.value) {
                Ordering::Less => return false,
                Ordering::Equal if !low.inclusive => return false,
                _ => {}
            }
        }
        if let Some(high) = &self.high {
            match n.cmp(&high.value) {
                Ordering::Greater => return false,
                Ordering::Equal if !high.inclusive => return false,
                _ => {}
            }
        }
        true
    }

    /// Provably empty — the bounds cross, or they coincide on a point at least one end excludes.
    pub fn is_empty(&self) -> bool {
        let (Some(low), Some(high)) = (&self.low, &self.high) else {
            return false;
        };
        match low.value.cmp(&high.value) {
            Ordering::Greater => true,
            Ordering::Equal => !(low.inclusive && high.inclusive),
            Ordering::Less => false,
        }
    }

    /// The tighter of two intervals.
    pub fn meet(&self, other: &NumBounds) -> NumBounds {
        NumBounds {
            low: tighter(self.low.clone(), other.low.clone(), true),
            high: tighter(self.high.clone(), other.high.clone(), false),
        }
    }

    /// Some inhabitant, for an overlap witness. `None` only when the interval is empty.
    pub fn sample(&self) -> Option<BigDecimal> {
        if self.is_empty() {
            return None;
        }
        let one = BigDecimal::from(1);
        let mut candidates: Vec<BigDecimal> = Vec::new();
        match (&self.low, &self.high) {
            (None, None) => candidates.push(BigDecimal::from(0)),
            (Some(low), None) => {
                candidates.push(low.value.clone());
                candidates.push(&low.value + &one);
            }
            (None, Some(high)) => {
                candidates.push(high.value.clone());
                candidates.push(&high.value - &one);
            }
            (Some(low), Some(high)) => {
                candidates.push(low.value.clone());
                candidates.push(high.value.clone());
                candidates.push((&low.value + &high.value) / BigDecimal::from(2));
            }
        }
        candidates.into_iter().find(|c| self.contains(c))
    }

    fn describe(&self) -> String {
        match (&self.low, &self.high) {
            (None, None) => "any number".to_string(),
            (Some(low), None) => {
                format!("{} {}", if low.inclusive { ">=" } else { ">" }, low.value)
            }
            (None, Some(high)) => {
                format!("{} {}", if high.inclusive { "<=" } else { "<" }, high.value)
            }
            (Some(low), Some(high)) => format!(
                "{} {} and {} {}",
                if low.inclusive { ">=" } else { ">" },
                low.value,
                if high.inclusive { "<=" } else { "<" },
                high.value
            ),
        }
    }
}

/// The tighter of two same-side bounds (`low = true` for the lower end). A bound that is present
/// on one side only always wins over an absent one.
fn tighter(a: Option<NumBound>, b: Option<NumBound>, low: bool) -> Option<NumBound> {
    match (a, b) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) => {
            let keep_a = match a.value.cmp(&b.value) {
                Ordering::Greater => low,
                Ordering::Less => !low,
                // Same point: the exclusive bound is the tighter one.
                Ordering::Equal => !a.inclusive,
            };
            Some(if keep_a { a } else { b })
        }
    }
}

// ---- the per-key constraint lattice -----------------------------------------------------

/// What one cube says about one attribute.
///
/// Invariants kept by every constructor: a [`Constraint::OneOf`] is never empty (an empty
/// permitted set is [`Constraint::Unsat`]) and a [`Constraint::NoneOf`] is never empty (an empty
/// forbidden set is [`Constraint::Any`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Constraint {
    /// No constraint — every value, and an absent attribute too.
    Any,
    /// Exactly these values.
    OneOf(BTreeSet<Lit>),
    /// Any value except these. Over an OPEN value side this is genuinely unbounded, which is
    /// why two `NoneOf`s can never prove disjointness without a closed domain.
    NoneOf(BTreeSet<Lit>),
    /// A numeric interval. Only built for a key the codec declares with a numeric kind.
    NumRange(NumBounds),
    /// Provably no value — the cube is dead.
    Unsat,
}

impl Constraint {
    /// `OneOf` of a set, collapsing the empty set to [`Constraint::Unsat`].
    pub fn one_of(values: BTreeSet<Lit>) -> Constraint {
        if values.is_empty() {
            Constraint::Unsat
        } else {
            Constraint::OneOf(values)
        }
    }

    /// `NoneOf` of a set, collapsing the empty set to [`Constraint::Any`].
    pub fn none_of(values: BTreeSet<Lit>) -> Constraint {
        if values.is_empty() {
            Constraint::Any
        } else {
            Constraint::NoneOf(values)
        }
    }

    /// `NumRange` of an interval, collapsing a provably empty one to [`Constraint::Unsat`].
    pub fn num_range(bounds: NumBounds) -> Constraint {
        if bounds.is_empty() {
            Constraint::Unsat
        } else {
            Constraint::NumRange(bounds)
        }
    }

    /// One value satisfying the constraint, rendered for an overlap witness. `None` for
    /// [`Constraint::Unsat`].
    fn witness(&self) -> Option<String> {
        match self {
            Constraint::Any => Some("(any value)".to_string()),
            Constraint::OneOf(values) => values.iter().next().map(|v| v.to_string()),
            Constraint::NoneOf(values) => Some(format!(
                "(any value other than {})",
                values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
            Constraint::NumRange(bounds) => Some(
                bounds
                    .sample()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| bounds.describe()),
            ),
            Constraint::Unsat => None,
        }
    }
}

/// The conjunction of two constraints on ONE key.
///
/// Exact wherever the lattice can be, and otherwise deliberately WEAKER than the true
/// conjunction (`NoneOf ∧ NumRange` keeps only the range). Weakening is the sound direction for
/// both verdicts this module supports: an `Unsat` meet of an over-approximation implies the
/// exact conjunction is empty too, so neither a disjointness proof nor an unsatisfiability proof
/// can be manufactured by it.
pub fn meet(a: &Constraint, b: &Constraint) -> Constraint {
    use Constraint::*;
    match (a, b) {
        (Unsat, _) | (_, Unsat) => Unsat,
        (Any, other) | (other, Any) => other.clone(),
        (OneOf(x), OneOf(y)) => Constraint::one_of(x.intersection(y).cloned().collect()),
        (OneOf(x), NoneOf(y)) | (NoneOf(y), OneOf(x)) => {
            Constraint::one_of(x.difference(y).cloned().collect())
        }
        (NoneOf(x), NoneOf(y)) => Constraint::none_of(x.union(y).cloned().collect()),
        (OneOf(x), NumRange(r)) | (NumRange(r), OneOf(x)) => Constraint::one_of(
            x.iter()
                .filter(|v| v.as_number().map(|n| r.contains(n)).unwrap_or(false))
                .cloned()
                .collect(),
        ),
        // Over-approximation, as documented above: an open-ended exclusion set cannot be
        // intersected with an interval without enumerating the interval.
        (NoneOf(_), NumRange(r)) | (NumRange(r), NoneOf(_)) => NumRange(r.clone()),
        (NumRange(x), NumRange(y)) => Constraint::num_range(x.meet(y)),
    }
}

// ---- cubes ------------------------------------------------------------------------------

/// One DNF cube — a conjunction of per-key constraints. An absent key is [`Constraint::Any`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Cube {
    keys: BTreeMap<String, Constraint>,
    /// Set by a literal `false` in the predicate — a contradiction no single key carries.
    contradiction: bool,
}

impl Cube {
    /// The cube every message satisfies.
    pub fn any() -> Cube {
        Cube::default()
    }

    /// The cube no message satisfies.
    pub fn never() -> Cube {
        Cube {
            keys: BTreeMap::new(),
            contradiction: true,
        }
    }

    /// A cube constraining one key.
    pub fn of(key: &str, constraint: Constraint) -> Cube {
        let mut cube = Cube::any();
        cube.keys.insert(key.to_string(), constraint);
        cube
    }

    /// This cube's constraint on a key — [`Constraint::Any`] when it says nothing about it.
    pub fn constraint(&self, key: &str) -> Constraint {
        self.keys.get(key).cloned().unwrap_or(Constraint::Any)
    }

    /// The constrained keys, in key order.
    pub fn constraints(&self) -> impl Iterator<Item = (&String, &Constraint)> {
        self.keys.iter()
    }

    /// Provably satisfied by no message.
    pub fn is_unsat(&self) -> bool {
        self.contradiction || self.keys.values().any(|c| *c == Constraint::Unsat)
    }

    /// The conjunction of two cubes.
    pub fn meet(&self, other: &Cube) -> Cube {
        let mut keys = self.keys.clone();
        for (key, constraint) in &other.keys {
            let merged = match keys.get(key) {
                Some(existing) => meet(existing, constraint),
                None => constraint.clone(),
            };
            keys.insert(key.clone(), merged);
        }
        Cube {
            keys,
            contradiction: self.contradiction || other.contradiction,
        }
    }

    /// A satisfying assignment, for an overlap witness. An empty map means "any message at all"
    /// (the cube constrains nothing).
    pub fn witness(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        for (key, constraint) in &self.keys {
            if let Some(value) = constraint.witness() {
                out.insert(format!("{ATTR_ROOT}.{key}"), value);
            }
        }
        out
    }
}

// ---- the analysis result ----------------------------------------------------------------

/// What the analyser made of a predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Analysis {
    /// The predicate is in Recognised Predicate Normal Form; these cubes are EXACTLY its
    /// denotation (modulo the documented weakening in [`meet`]).
    Cubes(Vec<Cube>),
    /// The predicate left the recognised form. `construct` names what did it, `offset` is its
    /// 0-based character offset in the predicate source.
    Opaque { construct: String, offset: usize },
}

impl Analysis {
    pub fn is_opaque(&self) -> bool {
        matches!(self, Analysis::Opaque { .. })
    }

    /// The cubes, or `None` when opaque.
    pub fn cubes(&self) -> Option<&[Cube]> {
        match self {
            Analysis::Cubes(cubes) => Some(cubes),
            Analysis::Opaque { .. } => None,
        }
    }

    /// Provably matched by no message — a dead start event.
    pub fn is_unsatisfiable(&self) -> bool {
        match self {
            Analysis::Cubes(cubes) => !cubes.is_empty() && cubes.iter().all(Cube::is_unsat),
            Analysis::Opaque { .. } => false,
        }
    }
}

/// Analyse a parsed predicate against the codec's declared attribute vocabulary.
///
/// `specs` is keyed by attribute name. An EMPTY map means the vocabulary is open: equality,
/// inequality and membership atoms still analyse (their denotation does not depend on a
/// declaration), while the atoms that need a declared kind or a closed domain — relational
/// operators, a bare boolean key, `matches` — become `Opaque`.
pub fn analyse(expr: &FeelExpr, specs: &BTreeMap<String, AttributeSpec>) -> Analysis {
    let mut atoms = 0usize;
    match dnf(expr, false, specs, &mut atoms) {
        Ok(cubes) => Analysis::Cubes(cubes),
        Err(opaque) => opaque,
    }
}

fn opaque(construct: impl Into<String>, offset: usize) -> Analysis {
    Analysis::Opaque {
        construct: construct.into(),
        offset,
    }
}

/// Disjunctive-normal-form expansion with `not` pushed inward via `negated`.
fn dnf(
    expr: &FeelExpr,
    negated: bool,
    specs: &BTreeMap<String, AttributeSpec>,
    atoms: &mut usize,
) -> Result<Vec<Cube>, Analysis> {
    match expr {
        FeelExpr::Not { arg, .. } => dnf(arg, !negated, specs, atoms),
        FeelExpr::BoolOp {
            left,
            op,
            right,
            start,
            ..
        } => {
            // De Morgan: a negated `and` is a disjunction of negations, and vice versa.
            let conjunction = matches!(op, LogicalOp::And) != negated;
            let l = dnf(left, negated, specs, atoms)?;
            let r = dnf(right, negated, specs, atoms)?;
            let width = if conjunction {
                l.len().saturating_mul(r.len())
            } else {
                l.len().saturating_add(r.len())
            };
            if width > MAX_CUBES {
                return Err(opaque(
                    format!(
                        "a predicate whose disjunctive normal form needs {width} cubes (the \
                         analysable limit is {MAX_CUBES})"
                    ),
                    *start,
                ));
            }
            if conjunction {
                let mut out = Vec::with_capacity(width);
                for a in &l {
                    for b in &r {
                        out.push(a.meet(b));
                    }
                }
                Ok(out)
            } else {
                let mut out = l;
                out.extend(r);
                Ok(out)
            }
        }
        other => {
            *atoms += 1;
            if *atoms > MAX_ATOMS {
                return Err(opaque(
                    format!("a predicate with more than {MAX_ATOMS} atoms (the analysable limit)"),
                    other.start(),
                ));
            }
            atom(other, negated, specs)
        }
    }
}

/// The two-segment `attr`-rooted key this expression references, if that is what it is.
fn key_of(expr: &FeelExpr) -> Option<&str> {
    match expr {
        FeelExpr::Path { segments, .. } if segments.len() == 2 && segments[0] == ATTR_ROOT => {
            Some(segments[1].as_str())
        }
        _ => None,
    }
}

/// The scalar literal this expression is, if that is what it is. A unary minus over a numeric
/// literal counts: the parser keeps `-1` as a distinct `Negate` node.
fn lit_of(expr: &FeelExpr) -> Option<Lit> {
    match expr {
        FeelExpr::Literal { value, .. } => Lit::from_feel_value(value),
        FeelExpr::Negate { arg, .. } => match lit_of(arg) {
            Some(Lit::Number(n)) => Some(Lit::Number(-n)),
            _ => None,
        },
        _ => None,
    }
}

fn flip(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
        other => other,
    }
}

fn negate(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Neq,
        CompareOp::Neq => CompareOp::Eq,
        CompareOp::Lt => CompareOp::Ge,
        CompareOp::Le => CompareOp::Gt,
        CompareOp::Gt => CompareOp::Le,
        CompareOp::Ge => CompareOp::Lt,
    }
}

fn numeric_kind(specs: &BTreeMap<String, AttributeSpec>, key: &str) -> bool {
    specs
        .get(key)
        .is_some_and(|s| matches!(s.kind, AttributeKind::Integer | AttributeKind::Decimal))
}

/// Narrow one atom's constraint against the key's declared domain when that domain is CLOSED.
///
/// This is where `NoneOf` becomes `OneOf` of the complement and a numeric interval becomes the
/// enumerated set it admits — the refinement that makes `!=` vs `!=` provable. `OneOf` is left
/// alone deliberately: a literal outside the closed domain is reported by the vocabulary check
/// as `SELECT_VALUE_UNKNOWN`, and collapsing the cube here as well would report one authoring
/// fault twice under two different codes.
fn narrow(
    key: &str,
    constraint: Constraint,
    specs: &BTreeMap<String, AttributeSpec>,
) -> Constraint {
    let Some(domain) = specs.get(key).and_then(closed_domain) else {
        return constraint;
    };
    match constraint {
        Constraint::NoneOf(excluded) => {
            Constraint::one_of(domain.difference(&excluded).cloned().collect())
        }
        Constraint::NumRange(bounds) => Constraint::one_of(
            domain
                .into_iter()
                .filter(|v| v.as_number().map(|n| bounds.contains(n)).unwrap_or(false))
                .collect(),
        ),
        other => other,
    }
}

fn single_cube(
    key: &str,
    constraint: Constraint,
    specs: &BTreeMap<String, AttributeSpec>,
) -> Result<Vec<Cube>, Analysis> {
    Ok(vec![Cube::of(key, narrow(key, constraint, specs))])
}

fn atom(
    expr: &FeelExpr,
    negated: bool,
    specs: &BTreeMap<String, AttributeSpec>,
) -> Result<Vec<Cube>, Analysis> {
    match expr {
        // A constant predicate.
        FeelExpr::Literal {
            value: FeelValue::Boolean(b),
            ..
        } => Ok(vec![if *b != negated {
            Cube::any()
        } else {
            Cube::never()
        }]),

        // A bare boolean key: eligible only when the attribute is FEEL `true` (and, negated,
        // only when it is `false` — `not(null)` is `null`, which is not eligible).
        FeelExpr::Path { start, .. } => {
            let Some(key) = key_of(expr) else {
                return Err(opaque(
                    "a path that is not a two-segment `attr.<key>` reference",
                    *start,
                ));
            };
            if !specs
                .get(key)
                .is_some_and(|s| s.kind == AttributeKind::Boolean)
            {
                return Err(opaque(
                    format!(
                        "a bare `{ATTR_ROOT}.{key}` predicate, which is analysable only when the \
                         codec declares that attribute `boolean`"
                    ),
                    *start,
                ));
            }
            let mut set = BTreeSet::new();
            set.insert(Lit::Bool(!negated));
            single_cube(key, Constraint::one_of(set), specs)
        }

        FeelExpr::Compare {
            left,
            op,
            right,
            start,
            ..
        } => {
            let (key, literal, op) = match (key_of(left), lit_of(right)) {
                (Some(key), Some(literal)) => (key, literal, *op),
                _ => match (lit_of(left), key_of(right)) {
                    (Some(literal), Some(key)) => (key, literal, flip(*op)),
                    // Name the operand that spoiled it, not the comparison: an author reading
                    // "arithmetic at offset 24" can act, "a comparison" is a shrug.
                    _ => {
                        let culprit = [&**left, &**right]
                            .into_iter()
                            .find(|side| key_of(side).is_none() && lit_of(side).is_none());
                        return Err(match culprit {
                            Some(side) => opaque(construct_name(side), side.start()),
                            None => opaque(
                                "a comparison between two literals or two attributes",
                                *start,
                            ),
                        });
                    }
                },
            };
            let op = if negated { negate(op) } else { op };
            match op {
                CompareOp::Eq => {
                    let mut set = BTreeSet::new();
                    set.insert(literal);
                    single_cube(key, Constraint::one_of(set), specs)
                }
                CompareOp::Neq => {
                    let mut set = BTreeSet::new();
                    set.insert(literal);
                    single_cube(key, Constraint::none_of(set), specs)
                }
                CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
                    let Some(n) = literal.as_number().cloned() else {
                        return Err(opaque(
                            format!(
                                "a relational operator against a non-numeric literal \
                                 ({literal})"
                            ),
                            *start,
                        ));
                    };
                    if !numeric_kind(specs, key) {
                        return Err(opaque(
                            format!(
                                "a relational operator on `{ATTR_ROOT}.{key}`, which the codec \
                                 does not declare with a numeric kind (FEEL orders strings too, \
                                 so this cannot be read as a number range)"
                            ),
                            *start,
                        ));
                    }
                    let bounds = match op {
                        CompareOp::Lt => NumBounds::at_most(n, false),
                        CompareOp::Le => NumBounds::at_most(n, true),
                        CompareOp::Gt => NumBounds::at_least(n, false),
                        _ => NumBounds::at_least(n, true),
                    };
                    single_cube(key, Constraint::num_range(bounds), specs)
                }
            }
        }

        FeelExpr::In {
            value, test, start, ..
        } => {
            let Some(key) = key_of(value) else {
                return Err(opaque(
                    "an `in` test whose left side is not a two-segment `attr.<key>` reference",
                    *start,
                ));
            };
            let items: Vec<&FeelExpr> = match &**test {
                FeelExpr::ListLit { items, .. } => items.iter().collect(),
                other => vec![other],
            };
            let mut set = BTreeSet::new();
            for item in items {
                let Some(literal) = lit_of(item) else {
                    return Err(opaque(
                        "an `in` test over something other than scalar literals",
                        item.start(),
                    ));
                };
                set.insert(literal);
            }
            let constraint = if negated {
                Constraint::none_of(set)
            } else {
                Constraint::one_of(set)
            };
            single_cube(key, constraint, specs)
        }

        FeelExpr::Call {
            name,
            args,
            arg_names,
            start,
            ..
        } if name == "matches" => {
            let positional = arg_names.iter().all(Option::is_none);
            if !positional || args.len() < 2 || args.len() > 3 {
                return Err(opaque(
                    "a `matches()` call the analyser does not recognise",
                    *start,
                ));
            }
            let Some(key) = key_of(&args[0]) else {
                return Err(opaque(
                    "a `matches()` call whose first argument is not a two-segment \
                     `attr.<key>` reference",
                    *start,
                ));
            };
            if !args[1..]
                .iter()
                .all(|a| matches!(lit_of(a), Some(Lit::Text(_))))
            {
                return Err(opaque(
                    "a `matches()` call whose pattern or flags are not string literals",
                    *start,
                ));
            }
            let Some(domain) = specs.get(key).and_then(closed_domain) else {
                return Err(opaque(
                    format!(
                        "`matches()` on `{ATTR_ROOT}.{key}`, whose declared value domain is open \
                         — a regex is analysable only over a closed domain, by testing every \
                         member"
                    ),
                    *start,
                ));
            };
            // Decided by the engine's OWN evaluator over each domain member, so the analysis
            // cannot drift from the runtime's XPath regex flavour.
            let mut matched = BTreeSet::new();
            for candidate in &domain {
                let mut attributes = BTreeMap::new();
                attributes.insert(key.to_string(), candidate.to_feel_value());
                let mut context: FeelContext = BTreeMap::new();
                context.insert(ATTR_ROOT.to_string(), FeelValue::Map(attributes));
                match sutra_feel::evaluator::eval_boolean(expr, &context) {
                    Ok(true) => {
                        matched.insert(candidate.clone());
                    }
                    Ok(false) => {}
                    Err(e) => {
                        return Err(opaque(
                            format!("a `matches()` call that does not evaluate: {}", e.message),
                            *start,
                        ))
                    }
                }
            }
            let permitted = if negated {
                domain.difference(&matched).cloned().collect()
            } else {
                matched
            };
            single_cube(key, Constraint::one_of(permitted), specs)
        }

        other => Err(opaque(construct_name(other), other.start())),
    }
}

/// A human name for an unrecognised construct, for the `NOT_ANALYSABLE` diagnostic.
fn construct_name(expr: &FeelExpr) -> &'static str {
    match expr {
        FeelExpr::Literal { .. } => "a non-scalar literal (a temporal, list or context value)",
        FeelExpr::Arith { .. } => "arithmetic",
        FeelExpr::Negate { .. } => "arithmetic negation",
        FeelExpr::Call { .. } => "a function call other than `matches()`",
        FeelExpr::FunctionDef { .. } => "a function definition",
        FeelExpr::IfThenElse { .. } => "an `if` expression",
        FeelExpr::ListLit { .. } => "a list literal in a predicate position",
        FeelExpr::ContextLit { .. } => "a context literal",
        FeelExpr::Range { .. } => "an interval literal",
        FeelExpr::Quantifier { .. } => "a quantifier (`some` / `every`)",
        FeelExpr::For { .. } => "a `for` iteration",
        FeelExpr::Filter { .. } => "a list filter",
        FeelExpr::FieldAccess { .. } => "field access on a computed value",
        FeelExpr::InstanceOf { .. } => "an `instance of` type test",
        FeelExpr::Invoke { .. } => "an invocation of a computed callee",
        FeelExpr::OpenRange { .. } => "a bare comparison range (`< x`)",
        FeelExpr::Path { .. } => "a path that is not a two-segment `attr.<key>` reference",
        FeelExpr::Compare { .. } | FeelExpr::In { .. } => "an unrecognised atom",
        FeelExpr::BoolOp { .. } | FeelExpr::Not { .. } => "a boolean operator",
    }
}

// ---- disjointness -----------------------------------------------------------------------

/// Whether two cube sets provably cannot both admit one message.
pub fn cubes_disjoint(a: &[Cube], b: &[Cube]) -> bool {
    overlap_witness(a, b).is_none()
}

/// The first assignment that satisfies a cube from each side, or `None` when the two sets are
/// provably disjoint. An EMPTY map is a witness too: it means any message at all matches both.
pub fn overlap_witness(a: &[Cube], b: &[Cube]) -> Option<BTreeMap<String, String>> {
    for x in a {
        if x.is_unsat() {
            continue;
        }
        for y in b {
            if y.is_unsat() {
                continue;
            }
            let both = x.meet(y);
            if !both.is_unsat() {
                return Some(both.witness());
            }
        }
    }
    None
}

/// The verdict on two subscribers' predicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disjointness {
    /// Proven: no message satisfies both.
    Disjoint,
    /// Proven otherwise — the witnessing assignment (empty = any message).
    Overlap(BTreeMap<String, String>),
    /// Neither proven: one side left the recognised form.
    Unanalysable { construct: String, offset: usize },
}

/// Decide two predicates' disjointness. An opaque predicate yields
/// [`Disjointness::Unanalysable`], never a guess: the caller turns that into
/// `SELECTOR_NOT_ANALYSABLE` where a proof is required, and accepts it where one is not (a lone
/// subscriber, a broadcast channel).
pub fn pred_disjoint(a: &Analysis, b: &Analysis) -> Disjointness {
    for side in [a, b] {
        if let Analysis::Opaque { construct, offset } = side {
            return Disjointness::Unanalysable {
                construct: construct.clone(),
                offset: *offset,
            };
        }
    }
    let (Some(left), Some(right)) = (a.cubes(), b.cubes()) else {
        unreachable!("both sides are Cubes");
    };
    match overlap_witness(left, right) {
        Some(witness) => Disjointness::Overlap(witness),
        None => Disjointness::Disjoint,
    }
}

// ---- vocabulary extraction (works on an OPAQUE predicate too) ---------------------------

/// One `attr.<key>` reference in the predicate source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeRef {
    pub key: String,
    pub start: usize,
    pub end: usize,
    /// The reference sits in a relational / arithmetic position, so it must be numeric.
    pub numeric: bool,
}

/// Every two-segment `attr`-rooted reference the predicate dereferences, deduplicated by
/// (key, offset). Derived from [`sutra_feel::paths`], so it sees inside the constructs the
/// analyser refuses — which is what lets the vocabulary checks run on an opaque predicate.
pub fn attribute_refs(expr: &FeelExpr) -> Vec<AttributeRef> {
    sutra_feel::paths::extract(expr)
        .into_iter()
        .filter(|p| p.segments.len() == 2 && p.segments[0] == ATTR_ROOT)
        .map(|p| AttributeRef {
            key: p.segments[1].clone(),
            start: p.start,
            end: p.end,
            numeric: p.usage == sutra_feel::paths::Usage::Numeric,
        })
        .collect()
}

/// One `attr.<key>` compared against a scalar literal, wherever it occurs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeComparison {
    pub key: String,
    pub literal: Lit,
    pub start: usize,
}

/// Every `attr.<key>` / literal pair the predicate compares, including inside constructs the
/// analyser does not recognise — the input to the type-mismatch and value-domain checks.
pub fn attribute_comparisons(expr: &FeelExpr) -> Vec<AttributeComparison> {
    let mut out = Vec::new();
    collect_comparisons(expr, &mut out);
    out
}

fn collect_comparisons(expr: &FeelExpr, out: &mut Vec<AttributeComparison>) {
    match expr {
        FeelExpr::Compare {
            left,
            right,
            start,
            op,
            ..
        } => {
            // Only equality/membership pins a value; a relational operand is a kind question,
            // which `attribute_refs` already reports via its numeric usage.
            if matches!(op, CompareOp::Eq | CompareOp::Neq) {
                let pair = match (key_of(left), lit_of(right)) {
                    (Some(key), Some(literal)) => Some((key, literal)),
                    _ => match (lit_of(left), key_of(right)) {
                        (Some(literal), Some(key)) => Some((key, literal)),
                        _ => None,
                    },
                };
                if let Some((key, literal)) = pair {
                    out.push(AttributeComparison {
                        key: key.to_string(),
                        literal,
                        start: *start,
                    });
                }
            }
            collect_comparisons(left, out);
            collect_comparisons(right, out);
        }
        FeelExpr::In {
            value, test, start, ..
        } => {
            if let Some(key) = key_of(value) {
                let items: Vec<&FeelExpr> = match &**test {
                    FeelExpr::ListLit { items, .. } => items.iter().collect(),
                    other => vec![other],
                };
                for item in items {
                    if let Some(literal) = lit_of(item) {
                        out.push(AttributeComparison {
                            key: key.to_string(),
                            literal,
                            start: item.start().max(*start),
                        });
                    }
                }
            }
            collect_comparisons(value, out);
            collect_comparisons(test, out);
        }
        FeelExpr::BoolOp { left, right, .. } | FeelExpr::Arith { left, right, .. } => {
            collect_comparisons(left, out);
            collect_comparisons(right, out);
        }
        FeelExpr::Not { arg, .. } | FeelExpr::Negate { arg, .. } => collect_comparisons(arg, out),
        FeelExpr::IfThenElse {
            cond,
            then,
            otherwise,
            ..
        } => {
            collect_comparisons(cond, out);
            collect_comparisons(then, out);
            collect_comparisons(otherwise, out);
        }
        FeelExpr::Call { args, .. } | FeelExpr::Invoke { args, .. } => {
            for arg in args {
                collect_comparisons(arg, out);
            }
        }
        FeelExpr::ListLit { items, .. } => {
            for item in items {
                collect_comparisons(item, out);
            }
        }
        FeelExpr::ContextLit { entries, .. } => {
            for (_, value) in entries {
                collect_comparisons(value, out);
            }
        }
        FeelExpr::Range { from, to, .. } => {
            collect_comparisons(from, out);
            collect_comparisons(to, out);
        }
        FeelExpr::Quantifier {
            source, condition, ..
        } => {
            collect_comparisons(source, out);
            collect_comparisons(condition, out);
        }
        FeelExpr::For { bindings, body, .. } => {
            for (_, source) in bindings {
                collect_comparisons(source, out);
            }
            collect_comparisons(body, out);
        }
        FeelExpr::Filter {
            source, predicate, ..
        } => {
            collect_comparisons(source, out);
            collect_comparisons(predicate, out);
        }
        FeelExpr::FieldAccess { base, .. } => collect_comparisons(base, out),
        FeelExpr::InstanceOf { expr, .. } => collect_comparisons(expr, out),
        FeelExpr::FunctionDef { body, .. } => collect_comparisons(body, out),
        FeelExpr::OpenRange { bound, .. } => collect_comparisons(bound, out),
        FeelExpr::Literal { .. } | FeelExpr::Path { .. } => {}
    }
}

/// The declared key closest to a misspelling, for a did-you-mean hint. `None` when nothing is
/// close enough to be worth suggesting (more than a third of the name would have to change).
pub fn nearest_key<'a>(
    key: &str,
    declared: impl IntoIterator<Item = &'a String>,
) -> Option<&'a String> {
    let budget = (key.chars().count() / 3).max(1);
    declared
        .into_iter()
        .map(|candidate| (edit_distance(key, candidate), candidate))
        .filter(|(distance, _)| *distance <= budget)
        .min_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)))
        .map(|(_, candidate)| candidate)
}

/// Levenshtein distance over chars — two rolling rows, no allocation per cell.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substitute = previous[j] + usize::from(ca != cb);
            current[j + 1] = substitute.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(key: &str, kind: AttributeKind, domain: &[AttributeLiteral]) -> AttributeSpec {
        AttributeSpec {
            key: key.to_string(),
            kind,
            domain: domain.to_vec(),
            required: false,
        }
    }

    fn vocabulary(specs: Vec<AttributeSpec>) -> BTreeMap<String, AttributeSpec> {
        specs.into_iter().map(|s| (s.key.clone(), s)).collect()
    }

    fn open() -> BTreeMap<String, AttributeSpec> {
        BTreeMap::new()
    }

    /// The canonical closed direction vocabulary — neutral by design: the engine never names a
    /// rail, so the worked example is a direction, not a message standard.
    fn closed_direction() -> BTreeMap<String, AttributeSpec> {
        vocabulary(vec![spec(
            "direction",
            AttributeKind::Text,
            &[
                AttributeLiteral::Text("incoming".into()),
                AttributeLiteral::Text("outgoing".into()),
            ],
        )])
    }

    fn analysis(source: &str, specs: &BTreeMap<String, AttributeSpec>) -> Analysis {
        let expr = sutra_feel::expressions::parse(source).expect("parses");
        analyse(&expr, specs)
    }

    fn disjoint(left: &str, right: &str, specs: &BTreeMap<String, AttributeSpec>) -> Disjointness {
        pred_disjoint(&analysis(left, specs), &analysis(right, specs))
    }

    #[test]
    fn eq_against_eq_on_different_values_is_disjoint() {
        assert_eq!(
            disjoint(
                r#"attr.direction = "incoming""#,
                r#"attr.direction = "outgoing""#,
                &open()
            ),
            Disjointness::Disjoint
        );
    }

    #[test]
    fn eq_against_eq_on_the_same_value_overlaps_with_a_witness() {
        let witness = match disjoint(
            r#"attr.direction = "incoming""#,
            r#"attr.direction = "incoming""#,
            &open(),
        ) {
            Disjointness::Overlap(witness) => witness,
            other => panic!("expected an overlap, got {other:?}"),
        };
        assert_eq!(
            witness.get("attr.direction").map(String::as_str),
            Some("\"incoming\"")
        );
    }

    #[test]
    fn eq_against_neq_on_the_same_value_is_disjoint() {
        assert_eq!(
            disjoint(
                r#"attr.direction = "incoming""#,
                r#"attr.direction != "incoming""#,
                &open()
            ),
            Disjointness::Disjoint
        );
    }

    #[test]
    fn neq_against_neq_is_not_provable_over_an_open_domain() {
        assert!(matches!(
            disjoint(
                r#"attr.direction != "incoming""#,
                r#"attr.direction != "outgoing""#,
                &open()
            ),
            Disjointness::Overlap(_)
        ));
    }

    #[test]
    fn neq_against_neq_is_provable_over_a_closed_domain() {
        assert_eq!(
            disjoint(
                r#"attr.direction != "incoming""#,
                r#"attr.direction != "outgoing""#,
                &closed_direction()
            ),
            Disjointness::Disjoint
        );
    }

    #[test]
    fn a_closed_domain_turns_none_of_into_one_of() {
        let analysis = analysis(r#"attr.direction != "incoming""#, &closed_direction());
        let cubes = analysis.cubes().expect("recognised");
        assert_eq!(cubes.len(), 1);
        let mut expected = BTreeSet::new();
        expected.insert(Lit::Text("outgoing".into()));
        assert_eq!(
            cubes[0].constraint("direction"),
            Constraint::OneOf(expected)
        );
    }

    #[test]
    fn excluding_every_closed_domain_member_is_unsatisfiable() {
        let analysis = analysis(
            r#"attr.direction != "incoming" and attr.direction != "outgoing""#,
            &closed_direction(),
        );
        assert!(analysis.is_unsatisfiable());
    }

    #[test]
    fn a_contradictory_cube_is_unsat() {
        let analysis = analysis(
            r#"attr.direction = "incoming" and attr.direction = "outgoing""#,
            &open(),
        );
        let cubes = analysis.cubes().expect("recognised");
        assert_eq!(cubes.len(), 1);
        assert!(cubes[0].is_unsat());
        assert!(analysis.is_unsatisfiable());
    }

    #[test]
    fn an_or_expands_to_one_cube_per_disjunct() {
        let analysis = analysis(
            r#"attr.direction = "incoming" or attr.direction = "outgoing""#,
            &open(),
        );
        assert_eq!(analysis.cubes().expect("recognised").len(), 2);
    }

    #[test]
    fn dnf_expands_a_conjunction_of_disjunctions_to_the_cross_product() {
        let analysis = analysis(
            r#"(attr.direction = "incoming" or attr.direction = "outgoing") and (attr.kind = "a" or attr.kind = "b")"#,
            &open(),
        );
        assert_eq!(analysis.cubes().expect("recognised").len(), 4);
    }

    #[test]
    fn not_is_pushed_through_and_into_a_disjunction() {
        let analysis = analysis(
            r#"not(attr.direction = "incoming" and attr.kind = "a")"#,
            &open(),
        );
        let cubes = analysis.cubes().expect("recognised");
        assert_eq!(cubes.len(), 2);
        let mut incoming = BTreeSet::new();
        incoming.insert(Lit::Text("incoming".into()));
        assert_eq!(
            cubes[0].constraint("direction"),
            Constraint::NoneOf(incoming)
        );
        let mut a = BTreeSet::new();
        a.insert(Lit::Text("a".into()));
        assert_eq!(cubes[1].constraint("kind"), Constraint::NoneOf(a));
    }

    #[test]
    fn a_double_negation_cancels() {
        assert_eq!(
            analysis(r#"not(not(attr.direction = "incoming"))"#, &open()),
            analysis(r#"attr.direction = "incoming""#, &open())
        );
    }

    #[test]
    fn in_becomes_one_of_and_its_negation_none_of() {
        let positive = analysis(r#"attr.direction in ["incoming", "outgoing"]"#, &open());
        let mut both = BTreeSet::new();
        both.insert(Lit::Text("incoming".into()));
        both.insert(Lit::Text("outgoing".into()));
        assert_eq!(
            positive.cubes().expect("recognised")[0].constraint("direction"),
            Constraint::OneOf(both.clone())
        );
        let negative = analysis(
            r#"not(attr.direction in ["incoming", "outgoing"])"#,
            &open(),
        );
        assert_eq!(
            negative.cubes().expect("recognised")[0].constraint("direction"),
            Constraint::NoneOf(both)
        );
    }

    #[test]
    fn numeric_ranges_meet_and_prove_disjointness() {
        let specs = vocabulary(vec![spec("priority", AttributeKind::Integer, &[])]);
        assert_eq!(
            disjoint("attr.priority >= 5", "attr.priority < 5", &specs),
            Disjointness::Disjoint
        );
        assert!(matches!(
            disjoint("attr.priority >= 5", "attr.priority < 9", &specs),
            Disjointness::Overlap(_)
        ));
        assert!(analysis("attr.priority > 5 and attr.priority < 5", &specs).is_unsatisfiable());
    }

    #[test]
    fn a_numeric_range_over_a_closed_domain_enumerates_it() {
        let specs = vocabulary(vec![spec(
            "priority",
            AttributeKind::Integer,
            &[
                AttributeLiteral::Integer(1),
                AttributeLiteral::Integer(2),
                AttributeLiteral::Integer(3),
            ],
        )]);
        let cubes = analysis("attr.priority >= 3", &specs)
            .cubes()
            .expect("recognised")
            .to_vec();
        let mut expected = BTreeSet::new();
        expected.insert(Lit::Number(BigDecimal::from(3)));
        assert_eq!(cubes[0].constraint("priority"), Constraint::OneOf(expected));
    }

    #[test]
    fn a_relational_operator_needs_a_numeric_declared_kind() {
        // Open vocabulary: the kind is unknown, and FEEL orders strings too.
        assert!(analysis("attr.priority >= 5", &open()).is_opaque());
        let text = vocabulary(vec![spec("priority", AttributeKind::Text, &[])]);
        assert!(analysis("attr.priority >= 5", &text).is_opaque());
    }

    #[test]
    fn arithmetic_makes_the_whole_predicate_opaque() {
        let analysis = analysis(
            r#"attr.direction = "incoming" and attr.priority + 1 > 2"#,
            &vocabulary(vec![spec("priority", AttributeKind::Integer, &[])]),
        );
        match analysis {
            Analysis::Opaque { construct, offset } => {
                assert!(construct.contains("arithmetic"), "got {construct}");
                assert!(offset > 0, "the offset must point at the construct");
            }
            other => panic!("expected opaque, got {other:?}"),
        }
    }

    #[test]
    fn a_non_attr_path_is_opaque() {
        assert!(analysis(r#"payload.direction = "incoming""#, &open()).is_opaque());
        assert!(analysis(r#"attr.a.b = "incoming""#, &open()).is_opaque());
    }

    #[test]
    fn an_opaque_side_makes_disjointness_unanalysable() {
        assert!(matches!(
            disjoint(
                r#"attr.direction = "incoming""#,
                r#"if true then attr.direction = "outgoing" else false"#,
                &open()
            ),
            Disjointness::Unanalysable { .. }
        ));
    }

    #[test]
    fn a_bare_boolean_key_is_recognised_only_when_declared_boolean() {
        let specs = vocabulary(vec![spec("urgent", AttributeKind::Boolean, &[])]);
        assert_eq!(
            disjoint("attr.urgent", "not(attr.urgent)", &specs),
            Disjointness::Disjoint
        );
        assert!(analysis("attr.urgent", &open()).is_opaque());
    }

    #[test]
    fn matches_is_decided_by_brute_force_over_a_closed_domain() {
        let specs = closed_direction();
        let cubes = analysis(r#"matches(attr.direction, "^in")"#, &specs)
            .cubes()
            .expect("recognised")
            .to_vec();
        let mut expected = BTreeSet::new();
        expected.insert(Lit::Text("incoming".into()));
        assert_eq!(
            cubes[0].constraint("direction"),
            Constraint::OneOf(expected)
        );
        assert_eq!(
            disjoint(
                r#"matches(attr.direction, "^in")"#,
                r#"matches(attr.direction, "^out")"#,
                &specs
            ),
            Disjointness::Disjoint
        );
        // Open domain — nothing to enumerate.
        assert!(analysis(r#"matches(attr.direction, "^in")"#, &open()).is_opaque());
    }

    #[test]
    fn a_literal_false_predicate_is_unsatisfiable_and_true_is_a_catch_all() {
        assert!(analysis("false", &open()).is_unsatisfiable());
        let catch_all = analysis("true", &open());
        assert!(matches!(
            pred_disjoint(
                &catch_all,
                &analysis(r#"attr.direction = "incoming""#, &open())
            ),
            Disjointness::Overlap(_)
        ));
    }

    #[test]
    fn exceeding_the_cube_cap_yields_opaque_rather_than_a_hang() {
        // Seven three-way disjunctions conjoined: 3^7 = 2187 cubes, well past the cap.
        let clause =
            |n: usize| format!(r#"(attr.k{n} = "a" or attr.k{n} = "b" or attr.k{n} = "c")"#);
        let source = (0..7).map(clause).collect::<Vec<_>>().join(" and ");
        let analysis = analysis(&source, &open());
        match analysis {
            Analysis::Opaque { construct, .. } => {
                assert!(construct.contains("cubes"), "got {construct}")
            }
            other => panic!("expected opaque, got {other:?}"),
        }
    }

    #[test]
    fn exceeding_the_atom_cap_yields_opaque() {
        let source = (0..MAX_ATOMS + 2)
            .map(|n| format!(r#"attr.k = "v{n}""#))
            .collect::<Vec<_>>()
            .join(" or ");
        match analysis(&source, &open()) {
            Analysis::Opaque { construct, .. } => {
                assert!(construct.contains("atoms"), "got {construct}")
            }
            other => panic!("expected opaque, got {other:?}"),
        }
    }

    #[test]
    fn meet_is_exact_on_the_set_cases_and_weakens_only_where_documented() {
        let incoming: BTreeSet<Lit> = [Lit::Text("incoming".into())].into_iter().collect();
        let outgoing: BTreeSet<Lit> = [Lit::Text("outgoing".into())].into_iter().collect();
        assert_eq!(
            meet(&Constraint::Any, &Constraint::OneOf(incoming.clone())),
            Constraint::OneOf(incoming.clone())
        );
        assert_eq!(
            meet(
                &Constraint::OneOf(incoming.clone()),
                &Constraint::OneOf(outgoing.clone())
            ),
            Constraint::Unsat
        );
        assert_eq!(
            meet(
                &Constraint::OneOf(incoming.clone()),
                &Constraint::NoneOf(incoming.clone())
            ),
            Constraint::Unsat
        );
        let both: BTreeSet<Lit> = incoming.union(&outgoing).cloned().collect();
        assert_eq!(
            meet(&Constraint::NoneOf(incoming), &Constraint::NoneOf(outgoing)),
            Constraint::NoneOf(both)
        );
        let range = NumBounds::at_least(BigDecimal::from(5), true);
        assert_eq!(
            meet(
                &Constraint::NoneOf([Lit::Number(BigDecimal::from(7))].into_iter().collect()),
                &Constraint::NumRange(range.clone())
            ),
            Constraint::NumRange(range)
        );
    }

    #[test]
    fn nearest_key_suggests_a_near_miss_and_stays_quiet_on_a_stranger() {
        let declared: Vec<String> = vec!["direction".to_string(), "priority".to_string()];
        assert_eq!(
            nearest_key("directon", declared.iter()).map(String::as_str),
            Some("direction")
        );
        assert_eq!(nearest_key("colour", declared.iter()), None);
    }
}
