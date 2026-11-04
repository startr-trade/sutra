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
//! and vendor extensions. A caller with its own BPMN loader can build a [`Model`] directly instead.
//! The same input always renders to the same bytes.

#![forbid(unsafe_code)]

mod diagram;
mod model;
mod parse;
mod theme;

pub use model::{
    EventGlyph, FlowNode, GatewayType, LoopMarker, Model, NodeKind, Process, SequenceFlow,
    SubProcessKind, TaskType,
};
pub use parse::{parse, Error};

/// How [`render`] draws a model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
}

/// Render every process of `model` as one SVG document, or `None` when it has no flow nodes (an
/// empty diagram is worse than none).
pub fn render(model: &Model, options: &Options) -> Option<String> {
    let svg = diagram::render_module_svg(&model.processes)?;
    Some(if options.css_variables {
        theme::with_css_variables(&svg)
    } else {
        svg
    })
}
