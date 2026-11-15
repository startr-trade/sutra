//! Draw BPMN 2.0 process diagrams as self-contained SVG.
//!
//! A hand-written or generated BPMN file usually carries no BPMN-DI, so it has no coordinates to
//! draw from. This crate computes a layered left-to-right layout from the flow nodes and sequence
//! flows alone, and emits BPMN notation with no embedded fonts and no external CSS.
//!
//! ```
//! let xml = r#"<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL">
//!   <process id="p">
//!     <startEvent id="start"/>
//!     <userTask id="review" name="Review"/>
//!     <endEvent id="end"/>
//!     <sequenceFlow id="f1" sourceRef="start" targetRef="review"/>
//!     <sequenceFlow id="f2" sourceRef="review" targetRef="end"/>
//!   </process>
//! </definitions>"#;
//!
//! let model = bpmn_svg::parse(xml)?;
//! let svg = bpmn_svg::render(&model, &bpmn_svg::Options::default()).expect("has flow nodes");
//! assert!(svg.starts_with("<svg "));
//! # Ok::<(), bpmn_svg::Error>(())
//! ```
//!
//! [`parse`] reads standard BPMN and ignores everything a diagram does not use, including BPMN-DI
//! and vendor extensions. A caller with its own BPMN loader builds a [`Model`] with the
//! constructors and builder methods instead:
//!
//! ```
//! use bpmn_svg::{FlowNode, Model, NodeAttrs, NodeKind, Options, Process, SequenceFlow, TaskType};
//!
//! let model = Model::new().process(
//!     Process::new("p")
//!         .node(FlowNode::new("start", NodeKind::StartEvent(None)))
//!         .node(FlowNode::new("review", NodeKind::Task(TaskType::User)).label("Review"))
//!         .node(FlowNode::new("end", NodeKind::EndEvent(None)))
//!         .flow(SequenceFlow::new("f1", "start", "review"))
//!         .flow(SequenceFlow::new("f2", "review", "end").label("Done")),
//! );
//! assert!(model.validate().is_empty());
//!
//! // Element ids and a link on one node, for a page that inlines the diagram.
//! let options = Options::new()
//!     .id_prefix("bpmn-")
//!     .node_attrs("review", NodeAttrs::new().href("/review").title("Open the review"));
//! assert!(options.validate(&model).is_empty());
//! let svg = bpmn_svg::render(&model, &options).expect("has flow nodes");
//! assert!(svg.contains("id=\"bpmn-review\""));
//! assert_eq!(model.element_ids("bpmn-")["review"], "bpmn-review");
//! ```
//!
//! The same input always renders to the same bytes.

#![forbid(unsafe_code)]

mod attrs;
mod diagram;
mod model;
mod parse;
mod theme;

use std::collections::BTreeMap;

pub use attrs::element_id;
pub use model::{
    EventGlyph, FlowNode, GatewayType, LoopMarker, MessageFlow, Model, NodeKind, Participant,
    Process, SequenceFlow, SubProcessKind, TaskType, Warning,
};
pub use parse::{parse, Error};

/// How [`render`] draws a model. Built with [`Options::new`] (the same as `default()`) and the
/// builder methods.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// Emit each colour as `var(--bpmn-*, <default>)`, so the page holding the SVG can set the
    /// palette, for example per light and dark theme. The fallback in each `var()` is the default
    /// colour, so a page that sets nothing looks the same. The variables are `--bpmn-stroke`,
    /// `--bpmn-text`, `--bpmn-muted`, `--bpmn-surface` and `--bpmn-label-background`.
    ///
    /// Custom properties only reach an SVG inlined into the page: one loaded through `<img>` always
    /// shows the fallback colours.
    pub css_variables: bool,
    /// Draw each node's `(id)` line and use a flow's id as its caption when it has no label.
    /// Default `true`.
    pub show_ids: bool,
    /// Give every node, flow and pool group an `id` attribute: this prefix followed by
    /// [`element_id`]'s sanitised id (see [`Model::element_ids`] for collisions). The `<defs>`
    /// marker ids take the prefix too, so several diagrams can share one page when each has its
    /// own prefix. `None` (the default) emits no group ids and unprefixed marker ids.
    pub id_prefix: Option<String>,
    /// Caller attributes for node groups, keyed by node id.
    pub node_attrs: BTreeMap<String, NodeAttrs>,
    /// Caller attributes for flow groups, keyed by sequence-flow or message-flow id.
    pub flow_attrs: BTreeMap<String, NodeAttrs>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            css_variables: false,
            show_ids: true,
            id_prefix: None,
            node_attrs: BTreeMap::new(),
            flow_attrs: BTreeMap::new(),
        }
    }
}

impl Options {
    /// The default options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set [`Options::css_variables`].
    #[must_use]
    pub fn css_variables(mut self, on: bool) -> Self {
        self.css_variables = on;
        self
    }

    /// Set [`Options::show_ids`].
    #[must_use]
    pub fn show_ids(mut self, on: bool) -> Self {
        self.show_ids = on;
        self
    }

    /// Set [`Options::id_prefix`].
    #[must_use]
    pub fn id_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.id_prefix = Some(prefix.into());
        self
    }

    /// Attributes for the node with this id, replacing any given before.
    #[must_use]
    pub fn node_attrs(mut self, id: impl Into<String>, attrs: NodeAttrs) -> Self {
        self.node_attrs.insert(id.into(), attrs);
        self
    }

    /// Attributes for the sequence or message flow with this id, replacing any given before.
    #[must_use]
    pub fn flow_attrs(mut self, id: impl Into<String>, attrs: NodeAttrs) -> Self {
        self.flow_attrs.insert(id.into(), attrs);
        self
    }

    /// What these options would drop or not use when rendering `model`: attributes keyed by an id
    /// the model does not have, invalid classes and `data-*` keys, and links holding `"` or `<`.
    /// Node attributes come first, then flow attributes, each in key order.
    pub fn validate(&self, model: &Model) -> Vec<Warning> {
        let nodes = attrs::node_ids(model);
        let flows = attrs::flow_ids(model);
        let mut out = Vec::new();
        for (map, known) in [(&self.node_attrs, &nodes), (&self.flow_attrs, &flows)] {
            for (id, a) in map {
                if !known.contains(id.as_str()) {
                    out.push(Warning::UnknownAttrsTarget { id: id.clone() });
                }
                if let Some(href) = a.href.as_deref().filter(|h| attrs::unsafe_href(h)) {
                    out.push(Warning::UnsafeHref {
                        element_id: id.clone(),
                        href: href.to_string(),
                    });
                }
                for class in a.classes.iter().filter(|c| !attrs::valid_class(c)) {
                    out.push(Warning::InvalidClass {
                        element_id: id.clone(),
                        class: class.clone(),
                    });
                }
                for key in a.data.keys().filter(|k| !attrs::valid_data_key(k)) {
                    out.push(Warning::InvalidDataKey {
                        element_id: id.clone(),
                        key: key.clone(),
                    });
                }
            }
        }
        out
    }
}

/// Caller attributes for one node's or flow's group. Built with [`NodeAttrs::new`] and the builder
/// methods.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeAttrs {
    /// Wrap the group's content in `<a href="…">`.
    pub href: Option<String>,
    /// The group's first child, `<title>…</title>`: a tooltip and an accessible name.
    pub title: Option<String>,
    /// Classes appended after the built-in ones, in order. One with a character outside
    /// `[A-Za-z0-9_-]` is dropped.
    pub classes: Vec<String>,
    /// `data-<key>="<value>"` attributes, in key order. A key outside `[a-z][a-z0-9-]*` is
    /// dropped.
    pub data: BTreeMap<String, String>,
}

impl NodeAttrs {
    /// No attributes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the link.
    #[must_use]
    pub fn href(mut self, href: impl Into<String>) -> Self {
        self.href = Some(href.into());
        self
    }

    /// Set the title.
    #[must_use]
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Append a class.
    #[must_use]
    pub fn class(mut self, class: impl Into<String>) -> Self {
        self.classes.push(class.into());
        self
    }

    /// Set a `data-*` attribute.
    #[must_use]
    pub fn data(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.data.insert(key.into(), value.into());
        self
    }
}

/// Render `model` as one SVG document: its pools and their processes, then the processes no pool
/// holds, one above the other, and the message flows between them. `None` when there is neither a
/// flow node nor a pool to draw (an empty diagram is worse than none).
pub fn render(model: &Model, options: &Options) -> Option<String> {
    let svg = diagram::render_model_svg(model, options)?;
    Some(if options.css_variables {
        theme::with_css_variables(&svg)
    } else {
        svg
    })
}
