//! The streaming instance validator: one forward pass over the document, a counting
//! automaton per open element, collect-ALL violations with line:col positions.
//!
//! Deterministic matching, no unique-particle-attribution analysis — the schema is
//! trusted (the compile step is the authoring gate). The behavioural contract pinned by
//! `tests/all/validate_behavior.rs` — issue presence, severity and position, never
//! message prose — is:
//!
//! - a content-model violation is reported at the offending element's start tag, after
//!   which the parent's content model is *poisoned*: no further content-model or
//!   completeness violations for that parent, and each remaining child validates on
//!   its own when its name matches an element declaration reachable in the parent's
//!   content model (otherwise its subtree is skipped silently);
//! - missing trailing content is reported at the parent's end tag;
//! - a bad simple value yields TWO violations at the element's end tag (the specific
//!   facet/datatype violation plus the "value not valid" companion) — same for
//!   attribute values at the start tag;
//! - well-formedness failures are a [`DocumentError`] (the consumer's FATAL parse
//!   code), never violations: schema validation presumes a parseable document.

use std::collections::HashMap;

use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use crate::datatype::Builtin;
use crate::diag::{DocumentError, SourceMap, SourcePos, Violation};
use crate::facet::FacetStep;
use crate::model::{
    AttrDecl, Content, ElementDecl, GroupDef, GroupKind, Particle, Resolved, Schema,
};

const XSI_NS: &[u8] = b"http://www.w3.org/2001/XMLSchema-instance";

impl Schema {
    /// Validate an instance document, collecting every violation. `Ok(vec![])` is a
    /// clean document; a non-empty list is the SOFT_ERRORS outcome (the payload stays
    /// projectable/routable — that mapping belongs to the consuming codec). `Err` means
    /// the document itself is unusable (malformed XML / DOCTYPE / no root).
    pub fn validate(&self, doc: &[u8]) -> Result<Vec<Violation>, DocumentError> {
        Validator::new(self, doc).run()
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// Which companion violation a bad simple value carries (message flavour only; both
/// are one extra ERROR at the same position).
#[derive(Clone, Copy)]
enum ValueContext {
    /// An element declared with a simple type.
    SimpleElement,
    /// An element whose complex type has simpleContent.
    SimpleContent,
}

enum Frame<'s> {
    /// An element with element-only (or empty) content.
    Complex {
        name: String,
        /// `None` for empty content (no children allowed).
        matcher: Option<GroupMatcher<'s>>,
        /// After a content-model violation: name → declaration index for best-effort
        /// per-child validation; no further content-model checks.
        poisoned: Option<HashMap<&'s str, &'s ElementDecl>>,
        stray_text: bool,
    },
    /// An element with character content typed by a simple type.
    Simple {
        name: String,
        builtin: Builtin,
        steps: &'s [FacetStep],
        context: ValueContext,
        text: String,
        had_child: bool,
    },
    /// Inside an accepted wildcard subtree (`lax` re-validates known declarations) or
    /// a skipped one.
    Wild { lax: bool },
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

struct Validator<'s, 'd> {
    schema: &'s Schema,
    map: SourceMap<'d>,
    reader: NsReader<&'d [u8]>,
    violations: Vec<Violation>,
    stack: Vec<Frame<'s>>,
    seen_root: bool,
}

impl<'s, 'd> Validator<'s, 'd> {
    fn new(schema: &'s Schema, doc: &'d [u8]) -> Validator<'s, 'd> {
        let mut reader = NsReader::from_reader(doc);
        let config = reader.config_mut();
        config.expand_empty_elements = true;
        config.check_end_names = true;
        Validator {
            schema,
            map: SourceMap::new(doc),
            reader,
            violations: Vec::new(),
            stack: Vec::new(),
            seen_root: false,
        }
    }

    fn violate(&mut self, pos: SourcePos, message: String) {
        self.violations.push(Violation::error(pos, message));
    }

    fn run(mut self) -> Result<Vec<Violation>, DocumentError> {
        loop {
            // Extract the resolved namespace as owned bytes right away so the reader
            // borrow ends before positions are computed.
            let (ns, event) = match self.reader.read_resolved_event() {
                Ok((resolve, event)) => {
                    let ns: Option<Vec<u8>> = match resolve {
                        ResolveResult::Bound(bound) => Some(bound.as_ref().to_vec()),
                        _ => None,
                    };
                    (ns, event)
                }
                Err(e) => {
                    let pos = self.map.pos(self.reader.error_position() as usize);
                    return Err(DocumentError {
                        pos: Some(pos),
                        message: format!("XML is not well-formed: {e}"),
                    });
                }
            };
            // The position just after the event's closing '>' — the reporting
            // convention for both start-tag and end-tag located violations.
            let after = self.map.pos(self.reader.buffer_position() as usize);
            match event {
                Event::Start(e) => {
                    if self.stack.is_empty() && self.seen_root {
                        return Err(DocumentError {
                            pos: Some(after),
                            message: "multiple root elements".to_string(),
                        });
                    }
                    self.seen_root = true;
                    let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                    self.start_element(ns.as_deref(), &local, &e, after);
                }
                Event::End(_) => {
                    if let Some(frame) = self.stack.pop() {
                        self.end_element(frame, after);
                    }
                }
                Event::Text(t) => {
                    let text = t.decode().map_err(|e| DocumentError {
                        pos: Some(after),
                        message: format!("bad character data: {e}"),
                    })?;
                    self.character_data(&text);
                }
                Event::GeneralRef(r) => {
                    // quick-xml 0.41 surfaces entity/char references as their own event
                    // instead of expanding them inside `Text`; reassemble the same string.
                    let mut text = String::new();
                    push_reference(&mut text, &r).map_err(|message| DocumentError {
                        pos: Some(after),
                        message: format!("bad character data: {message}"),
                    })?;
                    self.character_data(&text);
                }
                Event::CData(c) => {
                    let text = String::from_utf8_lossy(c.as_ref()).into_owned();
                    self.character_data(&text);
                }
                Event::DocType(_) => {
                    return Err(DocumentError {
                        pos: Some(after),
                        message: "DOCTYPE is not allowed".to_string(),
                    })
                }
                Event::Decl(_) | Event::PI(_) | Event::Comment(_) => {}
                Event::Empty(_) => unreachable!("empty elements are expanded"),
                Event::Eof => break,
            }
        }
        if !self.stack.is_empty() {
            return Err(DocumentError {
                pos: None,
                message: "unexpected end of document: unclosed elements remain".to_string(),
            });
        }
        if !self.seen_root {
            return Err(DocumentError {
                pos: None,
                message: "document has no root element".to_string(),
            });
        }
        Ok(self.violations)
    }

    fn character_data(&mut self, text: &str) {
        match self.stack.last_mut() {
            Some(Frame::Simple { text: buf, .. }) => buf.push_str(text),
            Some(Frame::Complex { stray_text, .. }) if !text.trim().is_empty() => {
                *stray_text = true;
            }
            _ => {}
        }
    }

    // ---- element start ----------------------------------------------------

    fn start_element(
        &mut self,
        ns: Option<&[u8]>,
        local: &str,
        tag: &BytesStart<'_>,
        after: SourcePos,
    ) {
        // Root element: must be a declared global in the target namespace.
        if self.stack.is_empty() {
            let declared = ns == Some(self.schema.target_namespace.as_bytes())
                && self.schema.roots.contains_key(local);
            if declared {
                let decl = &self.schema.roots[local];
                self.open_declared(decl, tag, after);
            } else {
                self.violate(after, format!("no declaration found for element '{local}'"));
                self.stack.push(Frame::Wild { lax: false });
            }
            return;
        }

        let in_target_ns = ns == Some(self.schema.target_namespace.as_bytes());
        // What the parent frame says this child is.
        enum Disposition<'s> {
            Declared(&'s ElementDecl),
            Wild { lax: bool },
            Skip,
            ModelViolation,
            ChildInSimple,
        }
        let disposition = match self.stack.last_mut() {
            Some(Frame::Wild { lax }) => Disposition::Wild { lax: *lax },
            Some(Frame::Simple { had_child, .. }) => {
                *had_child = true;
                Disposition::ChildInSimple
            }
            Some(Frame::Complex {
                poisoned: Some(index),
                ..
            }) => match index.get(local).copied() {
                Some(decl) if in_target_ns => Disposition::Declared(decl),
                _ => Disposition::Skip,
            },
            Some(Frame::Complex {
                matcher: None,
                poisoned,
                ..
            }) => {
                *poisoned = Some(HashMap::new());
                Disposition::ModelViolation
            }
            Some(Frame::Complex {
                matcher: Some(matcher),
                ..
            }) => {
                let name = if in_target_ns { Some(local) } else { None };
                match matcher.try_consume(name) {
                    Step::Took(Took::Element(decl)) => Disposition::Declared(decl),
                    Step::Took(Took::Wildcard { lax }) => Disposition::Wild { lax },
                    Step::No => Disposition::ModelViolation,
                }
            }
            None => Disposition::Skip,
        };
        match disposition {
            Disposition::Declared(decl) => self.open_declared(decl, tag, after),
            Disposition::Wild { lax } => self.open_wildcard(lax, ns, local, tag, after),
            Disposition::Skip => self.stack.push(Frame::Wild { lax: false }),
            Disposition::ChildInSimple => self.stack.push(Frame::Wild { lax: false }),
            Disposition::ModelViolation => {
                self.violate(
                    after,
                    format!("element '{local}' is not expected at this point of the content model"),
                );
                // Poison the parent, then give the offending element itself the same
                // best-effort treatment as its following siblings.
                let mut child_decl = None;
                if let Some(Frame::Complex {
                    matcher, poisoned, ..
                }) = self.stack.last_mut()
                {
                    let index = matcher
                        .as_ref()
                        .map(|m| element_index(m.group))
                        .unwrap_or_default();
                    child_decl = index.get(local).copied();
                    *poisoned = Some(index);
                }
                match child_decl {
                    Some(decl) if in_target_ns => self.open_declared(decl, tag, after),
                    _ => self.stack.push(Frame::Wild { lax: false }),
                }
            }
        }
    }

    /// A wildcard-matched element: `lax` validates it when its declaration is known
    /// (a global root of this schema), otherwise the subtree is accepted opaquely.
    fn open_wildcard(
        &mut self,
        lax: bool,
        ns: Option<&[u8]>,
        local: &str,
        tag: &BytesStart<'_>,
        after: SourcePos,
    ) {
        if lax
            && ns == Some(self.schema.target_namespace.as_bytes())
            && self.schema.roots.contains_key(local)
        {
            let decl = &self.schema.roots[local];
            self.open_declared(decl, tag, after);
        } else {
            self.stack.push(Frame::Wild { lax });
        }
    }

    fn open_declared(&mut self, decl: &'s ElementDecl, tag: &BytesStart<'_>, after: SourcePos) {
        match self.schema.resolve(&decl.type_ref) {
            Resolved::Builtin(builtin) => {
                self.check_attributes(&decl.name, tag, after, &[]);
                self.stack.push(Frame::Simple {
                    name: decl.name.clone(),
                    builtin,
                    steps: &[],
                    context: ValueContext::SimpleElement,
                    text: String::new(),
                    had_child: false,
                });
            }
            Resolved::Simple(simple) => {
                self.check_attributes(&decl.name, tag, after, &[]);
                self.stack.push(Frame::Simple {
                    name: decl.name.clone(),
                    builtin: simple.builtin,
                    steps: &simple.steps,
                    context: ValueContext::SimpleElement,
                    text: String::new(),
                    had_child: false,
                });
            }
            Resolved::Complex(complex) => {
                self.check_attributes(&decl.name, tag, after, &complex.attributes);
                match &complex.content {
                    Content::Simple(simple) => self.stack.push(Frame::Simple {
                        name: decl.name.clone(),
                        builtin: simple.builtin,
                        steps: &simple.steps,
                        context: ValueContext::SimpleContent,
                        text: String::new(),
                        had_child: false,
                    }),
                    Content::Group(group) => self.stack.push(Frame::Complex {
                        name: decl.name.clone(),
                        matcher: Some(GroupMatcher::new(group)),
                        poisoned: None,
                        stray_text: false,
                    }),
                    Content::Empty => self.stack.push(Frame::Complex {
                        name: decl.name.clone(),
                        matcher: None,
                        poisoned: None,
                        stray_text: false,
                    }),
                }
            }
        }
    }

    // ---- attributes ---------------------------------------------------------

    // `Attribute::unescape_value` (below) is deprecated in quick-xml 0.41 in favour of
    // `normalized_value` (which additionally collapses in-value whitespace); we keep the
    // exact 0.37 entity-only semantics, so the deprecation is allowed deliberately.
    #[allow(deprecated)]
    fn check_attributes(
        &mut self,
        element: &str,
        tag: &BytesStart<'_>,
        after: SourcePos,
        declared: &[AttrDecl],
    ) {
        let mut seen = vec![false; declared.len()];
        for attr in tag.attributes().flatten() {
            let raw_key = attr.key.as_ref();
            if raw_key == b"xmlns" || raw_key.starts_with(b"xmlns:") {
                continue;
            }
            // Extract owned facts before emitting violations (the resolver borrow
            // must end first).
            let (name, bound_ns) = {
                let (resolve, local) = self.reader.resolver().resolve_attribute(attr.key);
                let bound_ns: Option<Vec<u8>> = match resolve {
                    ResolveResult::Bound(bound) => Some(bound.as_ref().to_vec()),
                    _ => None,
                };
                (
                    String::from_utf8_lossy(local.as_ref()).into_owned(),
                    bound_ns,
                )
            };
            if let Some(ns) = bound_ns {
                if ns == XSI_NS {
                    continue; // xsi:* instance attributes are always accepted
                }
                self.violate(
                    after,
                    format!("attribute '{name}' is not allowed on element '{element}'"),
                );
                continue;
            }
            let Some(index) = declared.iter().position(|d| d.name == name) else {
                self.violate(
                    after,
                    format!("attribute '{name}' is not allowed on element '{element}'"),
                );
                continue;
            };
            seen[index] = true;
            let decl = &declared[index];
            let raw_value = attr.unescape_value().unwrap_or_default();
            let normalized = decl.value_type.builtin.normalize(&raw_value);
            let specific = match decl.value_type.builtin.check(&normalized) {
                Err(reason) => Some(reason),
                Ok(()) => first_facet_violation(
                    &decl.value_type.steps,
                    &normalized,
                    decl.value_type.builtin,
                ),
            };
            if let Some(reason) = specific {
                self.violate(after, reason);
                self.violate(
                    after,
                    format!("the value of attribute '{name}' on element '{element}' is not valid"),
                );
            }
        }
        for (index, decl) in declared.iter().enumerate() {
            if decl.required && !seen[index] {
                self.violate(
                    after,
                    format!(
                        "attribute '{}' must appear on element '{element}'",
                        decl.name
                    ),
                );
            }
        }
    }

    // ---- element end --------------------------------------------------------

    fn end_element(&mut self, frame: Frame<'s>, after: SourcePos) {
        match frame {
            Frame::Wild { .. } => {}
            Frame::Complex {
                name,
                matcher,
                poisoned,
                stray_text,
            } => {
                if stray_text {
                    self.violate(
                        after,
                        format!(
                            "element '{name}' has character content, but its type is element-only"
                        ),
                    );
                }
                if poisoned.is_none() {
                    if let Some(matcher) = matcher {
                        if !matcher.can_complete() {
                            self.violate(
                                after,
                                format!("the content of element '{name}' is not complete"),
                            );
                        }
                    }
                }
            }
            Frame::Simple {
                name,
                builtin,
                steps,
                context,
                text,
                had_child,
            } => {
                if had_child {
                    self.violate(
                        after,
                        format!("element '{name}' must not have element children"),
                    );
                }
                let normalized = builtin.normalize(&text);
                let specific = match builtin.check(&normalized) {
                    Err(reason) => Some(reason),
                    Ok(()) => first_facet_violation(steps, &normalized, builtin),
                };
                if let Some(reason) = specific {
                    self.violate(after, reason);
                    let companion = match context {
                        ValueContext::SimpleElement => {
                            format!("the value of element '{name}' is not valid")
                        }
                        ValueContext::SimpleContent => format!(
                            "element '{name}' must have no element children, and its value must be valid"
                        ),
                    };
                    self.violate(after, companion);
                }
            }
        }
    }
}

fn first_facet_violation(steps: &[FacetStep], value: &str, builtin: Builtin) -> Option<String> {
    steps
        .iter()
        .find_map(|step| step.check(value, builtin).err())
}

/// All element declarations reachable in a content model, by name (first declaration
/// wins) — the best-effort child index a poisoned parent validates against.
fn element_index(group: &GroupDef) -> HashMap<&str, &ElementDecl> {
    fn walk<'s>(group: &'s GroupDef, out: &mut HashMap<&'s str, &'s ElementDecl>) {
        for item in &group.items {
            match item {
                Particle::Element(decl) => {
                    out.entry(decl.name.as_str()).or_insert(decl);
                }
                Particle::Group(sub) => walk(sub, out),
                Particle::Wildcard { .. } => {}
            }
        }
    }
    let mut out = HashMap::new();
    walk(group, &mut out);
    out
}

// ---------------------------------------------------------------------------
// The counting automaton
// ---------------------------------------------------------------------------

enum Took<'s> {
    Element(&'s ElementDecl),
    Wildcard { lax: bool },
}

enum Step<'s> {
    Took(Took<'s>),
    No,
}

/// Internal answer: on `No`, whether the (sub)particle can be considered finished so
/// an enclosing sequence may move past it.
enum Attempt<'s> {
    Took(Took<'s>),
    No { satisfiable: bool },
}

struct GroupMatcher<'s> {
    group: &'s GroupDef,
    /// Completed repetitions of the whole group.
    reps_done: u32,
    /// Sequence: index of the item the current repetition is at.
    pos: usize,
    /// Choice: the branch the current repetition committed to.
    chosen: Option<usize>,
    /// Per-item state for the current repetition (lazily initialised).
    states: Vec<Option<ItemState<'s>>>,
    /// Whether the current repetition consumed anything.
    started: bool,
}

enum ItemState<'s> {
    Count(u32),
    Sub(Box<GroupMatcher<'s>>),
}

impl<'s> GroupMatcher<'s> {
    fn new(group: &'s GroupDef) -> GroupMatcher<'s> {
        let mut states = Vec::new();
        states.resize_with(group.items.len(), || None);
        GroupMatcher {
            group,
            reps_done: 0,
            pos: 0,
            chosen: None,
            states,
            started: false,
        }
    }

    fn try_consume(&mut self, name: Option<&str>) -> Step<'s> {
        match self.attempt(name) {
            Attempt::Took(took) => Step::Took(took),
            Attempt::No { .. } => Step::No,
        }
    }

    fn attempt(&mut self, name: Option<&str>) -> Attempt<'s> {
        match self.group.kind {
            GroupKind::Sequence => self.attempt_sequence(name),
            GroupKind::Choice => self.attempt_choice(name),
        }
    }

    fn max_reps_reached(&self) -> bool {
        !self.group.occurs.admits(self.reps_done)
    }

    fn attempt_sequence(&mut self, name: Option<&str>) -> Attempt<'s> {
        loop {
            while self.pos < self.group.items.len() {
                match self.attempt_item(self.pos, name) {
                    Attempt::Took(took) => {
                        self.started = true;
                        return Attempt::Took(took);
                    }
                    Attempt::No { satisfiable: true } => self.pos += 1,
                    Attempt::No { satisfiable: false } => {
                        if self.started {
                            // A repetition in progress is stuck on missing required
                            // content.
                            return Attempt::No { satisfiable: false };
                        }
                        // A fresh repetition would not start: the group stands or
                        // falls on the repetitions already completed.
                        return Attempt::No {
                            satisfiable: self.reps_done >= self.group.occurs.min,
                        };
                    }
                }
            }
            // One full repetition scanned.
            if !self.started {
                return Attempt::No {
                    satisfiable: self.reps_done >= self.group.occurs.min
                        || self.group.inner_nullable,
                };
            }
            self.reps_done += 1;
            if self.max_reps_reached() {
                return Attempt::No { satisfiable: true };
            }
            self.reset_repetition();
        }
    }

    fn attempt_choice(&mut self, name: Option<&str>) -> Attempt<'s> {
        loop {
            if let Some(branch) = self.chosen {
                match self.attempt_item(branch, name) {
                    Attempt::Took(took) => return Attempt::Took(took),
                    Attempt::No { satisfiable: false } => {
                        return Attempt::No { satisfiable: false }
                    }
                    Attempt::No { satisfiable: true } => {
                        // The chosen branch is complete — the repetition closes.
                        self.reps_done += 1;
                        self.chosen = None;
                        self.reset_repetition();
                        if self.max_reps_reached() {
                            return Attempt::No { satisfiable: true };
                        }
                    }
                }
            } else {
                for branch in 0..self.group.items.len() {
                    if let Attempt::Took(took) = self.attempt_item(branch, name) {
                        self.chosen = Some(branch);
                        self.started = true;
                        return Attempt::Took(took);
                    }
                }
                return Attempt::No {
                    satisfiable: self.reps_done >= self.group.occurs.min
                        || self.group.inner_nullable,
                };
            }
        }
    }

    fn attempt_item(&mut self, index: usize, name: Option<&str>) -> Attempt<'s> {
        match &self.group.items[index] {
            Particle::Element(decl) => {
                let count = match self.states[index] {
                    Some(ItemState::Count(c)) => c,
                    _ => 0,
                };
                if name == Some(decl.name.as_str()) && decl.occurs.admits(count) {
                    self.states[index] = Some(ItemState::Count(count + 1));
                    Attempt::Took(Took::Element(decl))
                } else {
                    Attempt::No {
                        satisfiable: count >= decl.occurs.min,
                    }
                }
            }
            Particle::Wildcard { occurs, lax } => {
                let count = match self.states[index] {
                    Some(ItemState::Count(c)) => c,
                    _ => 0,
                };
                if occurs.admits(count) {
                    self.states[index] = Some(ItemState::Count(count + 1));
                    Attempt::Took(Took::Wildcard { lax: *lax })
                } else {
                    Attempt::No {
                        satisfiable: count >= occurs.min,
                    }
                }
            }
            Particle::Group(sub) => {
                if !matches!(self.states[index], Some(ItemState::Sub(_))) {
                    self.states[index] = Some(ItemState::Sub(Box::new(GroupMatcher::new(sub))));
                }
                match &mut self.states[index] {
                    Some(ItemState::Sub(matcher)) => matcher.attempt(name),
                    _ => unreachable!("state initialised above"),
                }
            }
        }
    }

    fn reset_repetition(&mut self) {
        self.pos = 0;
        self.started = false;
        for state in &mut self.states {
            *state = None;
        }
    }

    /// Whether the group can end here (the parent element is closing).
    fn can_complete(&self) -> bool {
        let current_ok = match self.group.kind {
            GroupKind::Sequence => {
                (self.pos..self.group.items.len()).all(|index| self.item_can_complete(index))
            }
            GroupKind::Choice => match self.chosen {
                Some(branch) => self.item_can_complete(branch),
                None => true,
            },
        };
        if !current_ok {
            return false;
        }
        let effective = self.reps_done + u32::from(self.started);
        effective >= self.group.occurs.min || self.group.inner_nullable
    }

    fn item_can_complete(&self, index: usize) -> bool {
        match (&self.group.items[index], &self.states[index]) {
            (_, Some(ItemState::Sub(matcher))) => matcher.can_complete(),
            (Particle::Element(decl), Some(ItemState::Count(c))) => *c >= decl.occurs.min,
            (Particle::Wildcard { occurs, .. }, Some(ItemState::Count(c))) => *c >= occurs.min,
            (item, None) => item.nullable(),
            // A group item only ever holds Sub state.
            (Particle::Group(_), Some(ItemState::Count(_))) => true,
        }
    }
}

/// Resolve one general reference (`&name;` or `&#nn;`) into `out`, reproducing the
/// quick-xml 0.37 text-unescape behaviour: only the five predefined entities and numeric
/// character references; any other (DTD-defined or unknown) entity is an error — the
/// XXE-safe posture. quick-xml 0.41 surfaces references as their own `Event::GeneralRef`.
fn push_reference(out: &mut String, r: &quick_xml::events::BytesRef<'_>) -> Result<(), String> {
    if let Some(ch) = r.resolve_char_ref().map_err(|e| e.to_string())? {
        out.push(ch);
    } else {
        let name = r.decode().map_err(|e| e.to_string())?;
        match quick_xml::escape::resolve_predefined_entity(&name) {
            Some(rep) => out.push_str(rep),
            None => return Err(format!("unknown entity reference '&{name};'")),
        }
    }
    Ok(())
}
