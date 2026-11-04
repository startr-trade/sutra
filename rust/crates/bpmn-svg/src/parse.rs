//! Reads standard BPMN 2.0 into the neutral [`Model`].
//!
//! Elements are matched by namespace, so BPMN-DI and vendor extensions fall away without being
//! named. No entity is ever expanded and a DOCTYPE is refused.

use std::collections::{HashMap, HashSet};
use std::fmt;

use quick_xml::events::{BytesRef, BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;

use crate::model::{
    EventGlyph, FlowNode, GatewayType, LoopMarker, Model, NodeKind, Process, SequenceFlow,
    SubProcessKind, TaskType,
};

const BPMN_NS: &str = "http://www.omg.org/spec/BPMN/20100524/MODEL";

/// Deeper nesting is refused: scopes are walked recursively, so an unbounded document could
/// exhaust the stack.
const MAX_DEPTH: usize = 256;

/// Why a document could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The document is not well-formed XML.
    Malformed(String),
    /// The document is well-formed, but its root is not a BPMN `<definitions>`.
    NotBpmn(String),
    /// A BPMN element cannot be drawn: an id is missing or repeated, or a boundary event has no
    /// host.
    Invalid(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Malformed(detail) => write!(f, "malformed XML: {detail}"),
            Error::NotBpmn(detail) => write!(f, "not a BPMN document: {detail}"),
            Error::Invalid(detail) => write!(f, "invalid BPMN: {detail}"),
        }
    }
}

impl std::error::Error for Error {}

/// Read a BPMN 2.0 document into a [`Model`].
///
/// BPMN-DI is not needed. Elements and attributes outside the diagram's concern are ignored,
/// including vendor extensions.
pub fn parse(bpmn_xml: &str) -> Result<Model, Error> {
    let root = read_document(bpmn_xml)?;
    if !root.is_bpmn("definitions") {
        return Err(Error::NotBpmn(format!(
            "the root element is <{}>, expected <definitions> in {BPMN_NS}",
            root.local
        )));
    }
    let processes = root
        .children
        .iter()
        .filter(|c| c.is_bpmn("process"))
        .map(read_scope)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Model { processes })
}

// ---- the document ----------------------------------------------------------------------------

/// An element reduced to what classification reads: text content is never used.
struct Element {
    bpmn: bool,
    local: String,
    /// Unprefixed attributes.
    attrs: HashMap<String, String>,
    children: Vec<Element>,
}

impl Element {
    fn is_bpmn(&self, local: &str) -> bool {
        self.bpmn && self.local == local
    }

    fn attr(&self, name: &str) -> &str {
        self.attrs.get(name).map_or("", String::as_str)
    }

    fn has_child(&self, local: &str) -> bool {
        self.children.iter().any(|c| c.is_bpmn(local))
    }
}

fn malformed(detail: impl fmt::Display) -> Error {
    Error::Malformed(detail.to_string())
}

fn read_document(xml: &str) -> Result<Element, Error> {
    let mut reader = NsReader::from_str(xml);
    reader.config_mut().expand_empty_elements = true;
    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;
    loop {
        match reader.read_resolved_event().map_err(malformed)? {
            (resolved, Event::Start(start)) => {
                if stack.len() >= MAX_DEPTH {
                    return Err(Error::Invalid(format!(
                        "elements are nested more than {MAX_DEPTH} levels deep"
                    )));
                }
                let bpmn = matches!(&resolved, ResolveResult::Bound(ns) if ns.as_ref() == BPMN_NS.as_bytes());
                stack.push(read_element(bpmn, &start)?);
            }
            (_, Event::End(_)) => {
                let element = stack.pop().ok_or_else(|| malformed("unbalanced end tag"))?;
                match stack.last_mut() {
                    Some(parent) => parent.children.push(element),
                    None if root.is_some() => return Err(malformed("more than one root element")),
                    None => root = Some(element),
                }
            }
            (_, Event::GeneralRef(reference)) => check_reference(&reference)?,
            (_, Event::DocType(_)) => return Err(malformed("a DOCTYPE is not allowed")),
            (_, Event::Eof) => break,
            _ => {}
        }
    }
    if !stack.is_empty() {
        return Err(malformed("the document ends inside an element"));
    }
    root.ok_or_else(|| malformed("the document has no root element"))
}

// `unescape_value` is deprecated in favour of `normalized_value`, which also collapses whitespace
// inside a value. A label is shown as written, so the entity-only form is kept deliberately.
#[allow(deprecated)]
fn read_element(bpmn: bool, start: &BytesStart<'_>) -> Result<Element, Error> {
    let mut attrs = HashMap::new();
    for attribute in start.attributes() {
        let attribute = attribute.map_err(malformed)?;
        if attribute.key.prefix().is_none() {
            let value = attribute.unescape_value().map_err(malformed)?;
            attrs.insert(
                String::from_utf8_lossy(attribute.key.as_ref()).into_owned(),
                value.into_owned(),
            );
        }
    }
    Ok(Element {
        bpmn,
        local: String::from_utf8_lossy(start.local_name().as_ref()).into_owned(),
        attrs,
        children: Vec::new(),
    })
}

/// A reference in text content is unused, but one that names an undeclared entity is not
/// well-formed.
fn check_reference(reference: &BytesRef<'_>) -> Result<(), Error> {
    if reference.resolve_char_ref().map_err(malformed)?.is_some() {
        return Ok(());
    }
    let name = reference.decode().map_err(malformed)?;
    match quick_xml::escape::resolve_predefined_entity(&name) {
        Some(_) => Ok(()),
        None => Err(malformed(format!("unknown entity reference '&{name};'"))),
    }
}

// ---- classification --------------------------------------------------------------------------

/// A process or sub-process element's direct flow nodes and sequence flows. Sequence flows draw
/// from their own attributes, so an unusable one is simply not drawn; a flow node needs an id
/// because everything refers to it by that id.
fn read_scope(scope: &Element) -> Result<Process, Error> {
    let mut nodes = Vec::new();
    let mut flows = Vec::new();
    let mut seen = HashSet::new();
    for element in scope.children.iter().filter(|c| c.bpmn) {
        if element.local == "sequenceFlow" {
            flows.push(SequenceFlow {
                id: element.attr("id").to_string(),
                source_ref: element.attr("sourceRef").to_string(),
                target_ref: element.attr("targetRef").to_string(),
            });
            continue;
        }
        let Some(kind) = classify(element)? else {
            continue;
        };
        let id = element.attr("id");
        if id.is_empty() {
            return Err(Error::Invalid(format!("a <{}> has no id", element.local)));
        }
        if !seen.insert(id) {
            return Err(Error::Invalid(format!("the id '{id}' is repeated")));
        }
        nodes.push(FlowNode {
            id: id.to_string(),
            label: non_blank(element.attr("name")),
            kind,
            markers: markers(element),
        });
    }
    Ok(Process {
        id: scope.attr("id").to_string(),
        nodes,
        flows,
    })
}

fn non_blank(text: &str) -> Option<String> {
    (!text.trim().is_empty()).then(|| text.to_string())
}

/// The visual type of one element, or `None` when it is not a flow node this model draws.
fn classify(e: &Element) -> Result<Option<NodeKind>, Error> {
    Ok(Some(match e.local.as_str() {
        "startEvent" => NodeKind::StartEvent(if e.has_child("timerEventDefinition") {
            EventGlyph::Timer
        } else {
            // A start event with no timer draws the message glyph, including a none start.
            EventGlyph::Message
        }),
        "endEvent" => NodeKind::EndEvent(end_glyph(e)),
        "intermediateThrowEvent" | "implicitThrowEvent" => {
            NodeKind::IntermediateThrowEvent(throw_glyph(e))
        }
        "intermediateCatchEvent" => NodeKind::IntermediateCatchEvent(catch_glyph(e)),
        "boundaryEvent" => {
            let attached = e.attr("attachedToRef");
            if attached.trim().is_empty() {
                return Err(Error::Invalid(format!(
                    "the boundary event '{}' has no attachedToRef",
                    e.attr("id")
                )));
            }
            NodeKind::BoundaryEvent {
                // A QName: the id is its local part.
                attached_to_ref: attached.rsplit(':').next().unwrap_or(attached).to_string(),
                glyph: boundary_glyph(e),
            }
        }
        "serviceTask" => NodeKind::Task(TaskType::Service),
        "scriptTask" => NodeKind::Task(TaskType::Script),
        "sendTask" => NodeKind::Task(TaskType::Send),
        "userTask" => NodeKind::Task(TaskType::User),
        "businessRuleTask" => NodeKind::Task(TaskType::BusinessRule),
        "manualTask" | "task" => NodeKind::Task(TaskType::Manual),
        "receiveTask" => NodeKind::Task(TaskType::Receive),
        "callActivity" => NodeKind::CallActivity,
        "subProcess" | "transaction" | "adHocSubProcess" => NodeKind::SubProcess {
            kind: sub_process_kind(e),
            inner: read_scope(e)?,
        },
        "exclusiveGateway" => NodeKind::Gateway(GatewayType::Exclusive),
        "eventBasedGateway" => NodeKind::Gateway(GatewayType::EventBased),
        "inclusiveGateway" => NodeKind::Gateway(GatewayType::Inclusive),
        "parallelGateway" => NodeKind::Gateway(GatewayType::Parallel),
        "complexGateway" => NodeKind::Gateway(GatewayType::Complex),
        _ => return Ok(None),
    }))
}

fn sub_process_kind(e: &Element) -> SubProcessKind {
    match e.local.as_str() {
        "adHocSubProcess" => SubProcessKind::AdHoc,
        _ if e.attr("triggeredByEvent") == "true" => SubProcessKind::Event,
        "transaction" => SubProcessKind::Transaction,
        _ => SubProcessKind::Embedded,
    }
}

fn end_glyph(e: &Element) -> Option<EventGlyph> {
    if e.has_child("errorEventDefinition") {
        Some(EventGlyph::Error)
    } else if e.has_child("terminateEventDefinition") {
        Some(EventGlyph::Terminate)
    } else if e.has_child("cancelEventDefinition") {
        Some(EventGlyph::Cancel)
    } else {
        None
    }
}

/// The last event definition wins. A throw with none draws the message glyph.
fn throw_glyph(e: &Element) -> EventGlyph {
    let mut glyph = EventGlyph::Message;
    for definition in e.children.iter().filter(|c| c.bpmn) {
        glyph = match definition.local.as_str() {
            "compensateEventDefinition" => EventGlyph::Compensate,
            "messageEventDefinition" => EventGlyph::Message,
            "signalEventDefinition" => EventGlyph::Signal,
            "escalationEventDefinition" => EventGlyph::Escalation,
            "linkEventDefinition" => EventGlyph::Link,
            _ => glyph,
        };
    }
    glyph
}

fn catch_glyph(e: &Element) -> EventGlyph {
    if e.has_child("linkEventDefinition") {
        EventGlyph::Link
    } else if e.has_child("timerEventDefinition") {
        EventGlyph::Timer
    } else {
        EventGlyph::Message
    }
}

/// The last event definition wins. A boundary event with none draws the error glyph.
fn boundary_glyph(e: &Element) -> EventGlyph {
    let mut glyph = EventGlyph::Error;
    for definition in e.children.iter().filter(|c| c.bpmn) {
        glyph = match definition.local.as_str() {
            "errorEventDefinition" => EventGlyph::Error,
            "escalationEventDefinition" => EventGlyph::Escalation,
            "compensateEventDefinition" => EventGlyph::Compensate,
            "cancelEventDefinition" => EventGlyph::Cancel,
            "timerEventDefinition" => EventGlyph::Timer,
            _ => glyph,
        };
    }
    glyph
}

/// An activity's loop marker. A standard loop takes precedence over multi-instance.
fn markers(e: &Element) -> Vec<LoopMarker> {
    if e.has_child("standardLoopCharacteristics") {
        return vec![LoopMarker::Loop];
    }
    let multi_instance = e
        .children
        .iter()
        .find(|c| c.is_bpmn("multiInstanceLoopCharacteristics"));
    match multi_instance {
        Some(mi) if mi.attr("isSequential") == "true" => vec![LoopMarker::SequentialMultiInstance],
        Some(_) => vec![LoopMarker::ParallelMultiInstance],
        None => Vec::new(),
    }
}
