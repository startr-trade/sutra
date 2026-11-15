# bpmn-svg

Draws BPMN 2.0 process diagrams as self-contained SVG. It needs no BPMN-DI: the layout is computed
from the flow nodes and sequence flows, so it works on hand-written and generated files that carry
no coordinates. The output is BPMN notation with no embedded fonts and no external CSS, ready to
inline in HTML or Markdown.

The same input always gives byte-identical SVG, so a generated diagram can be committed and diffed.

```rust
let xml = r#"<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <process id="p">
    <startEvent id="start"/>
    <exclusiveGateway id="check" name="Approved?"/>
    <userTask id="review" name="Review"/>
    <endEvent id="end"/>
    <sequenceFlow id="f1" sourceRef="start" targetRef="check"/>
    <sequenceFlow id="f2" sourceRef="check" targetRef="review"/>
    <sequenceFlow id="f3" sourceRef="check" targetRef="end"/>
    <sequenceFlow id="f4" sourceRef="review" targetRef="end"/>
  </process>
</definitions>"#;

let model = bpmn_svg::parse(xml)?;
let svg = bpmn_svg::render(&model, &bpmn_svg::Options::default());
```

`render` returns `None` when the model has no flow nodes.

## What it reads

`parse` reads standard BPMN 2.0, matching elements by namespace:

- events, tasks, call activities and gateways;
- sequence flows, with their `name` (or a condition of up to 40 characters) as the caption, a
  slash on a source's `default` flow, and a diamond on a conditional flow leaving an activity;
- embedded, transaction, ad-hoc and event sub-processes, drawn expanded around their content;
- boundary events, pinned to the activity they attach to;
- standard-loop and multi-instance markers;
- collaborations: each participant draws as a pool around the process it names, or as an empty
  black-box pool; message flows draw dashed between nodes or pool borders;
- several processes in one file, drawn one above the other.

Everything else is ignored, including BPMN-DI, vendor extensions and timer expressions. A label is
the element's `name`, and each shape also shows its `id` unless `show_ids` is off.

`parse` returns an error when the XML is malformed, when the root is not a BPMN `<definitions>`, or
when an element cannot be drawn because its `id` is missing or repeated, a participant has no `id`,
or a boundary event has no `attachedToRef`. A DOCTYPE is refused and no entity is ever expanded.

## Building a model

A program with its own BPMN loader builds a `Model` with the constructors and builder methods
instead, and calls `render`:

```rust
use bpmn_svg::{FlowNode, Model, NodeKind, Process, SequenceFlow, TaskType};

let model = Model::new().process(
    Process::new("p")
        .node(FlowNode::new("start", NodeKind::StartEvent(None)))
        .node(FlowNode::new("review", NodeKind::Task(TaskType::User)).label("Review"))
        .flow(SequenceFlow::new("f1", "start", "review").label("New request")),
);
let svg = bpmn_svg::render(&model, &bpmn_svg::Options::default());
```

`Model::validate` lists what will not draw: sequence flows naming a node outside their scope,
message flows naming neither a node nor a participant, and participants naming a missing process.
`render` draws what it can either way.

## Hooks for a host page

Every element draws in a `<g>` with fixed classes, so a page can style or script it:

| Element | Classes |
|---|---|
| Event | `bpmn-node bpmn-event bpmn-event-start` (`-end`, `-intermediate-throw`, `-intermediate-catch`, `-boundary`) |
| Task | `bpmn-node bpmn-task bpmn-task-user` (`-plain`, `-service`, `-script`, `-send`, `-business-rule`, `-manual`, `-receive`) |
| Call activity | `bpmn-node bpmn-call-activity` |
| Sub-process | `bpmn-node bpmn-subprocess bpmn-subprocess-embedded` (`-transaction`, `-ad-hoc`, `-event`) |
| Gateway | `bpmn-node bpmn-gateway bpmn-gateway-exclusive` (`-event-based`, `-inclusive`, `-parallel`, `-complex`) |
| Sequence flow | `bpmn-flow`, plus `bpmn-flow-default` or `bpmn-flow-conditional` |
| Message flow | `bpmn-message-flow` |
| Pool | `bpmn-pool`, plus `bpmn-pool-collapsed` for a black box |

An edge draws as two groups with the same classes: its line, then later its caption, so captions
stay on top.

`Options::id_prefix` gives each group an `id`: the prefix and the BPMN id with every character
outside `[A-Za-z0-9_-]` replaced by `_` (`bpmn_svg::element_id`). An id that collides with an
earlier one after that gets `-2`, `-3`, and so on; `Model::element_ids` returns the exact ids
without rendering. The arrowhead markers take the prefix too, so give each diagram inlined into one
page its own prefix. With no prefix, the default, groups carry no `id`.

`Options::node_attrs` and `Options::flow_attrs` add a link, a tooltip, classes and `data-*`
attributes to a node's or flow's group, at any depth:

```rust
use bpmn_svg::{NodeAttrs, Options};

let options = Options::new().id_prefix("bpmn-").node_attrs(
    "review",
    NodeAttrs::new()
        .href("/tasks/review")
        .title("Open the review task")
        .class("is-current")
        .data("status", "open"),
);
let svg = bpmn_svg::render(&model, &options);
```

Every value is escaped. A class outside `[A-Za-z0-9_-]` and a `data-*` key outside
`[a-z][a-z0-9-]*` are left out; `Options::validate` reports them, a link holding `"` or `<`, and
attributes for an id the model does not have.

## Light and dark themes

By default colours are plain SVG attributes. With `css_variables`, each one is written as a CSS
variable whose fallback is the default colour:

```rust
let options = bpmn_svg::Options::new().css_variables(true);
let svg = bpmn_svg::render(&model, &options);
```

| Variable | Default | Used for |
|---|---|---|
| `--bpmn-stroke` | `#333` | Outlines, edges, glyphs |
| `--bpmn-text` | `#222` | Labels |
| `--bpmn-muted` | `#888` | Ids and edge captions |
| `--bpmn-surface` | `#fff` | Shape fill |
| `--bpmn-label-background` | `#ffffffe6` | Behind edge captions |

A page sets them where the SVG is inlined:

```css
@media (prefers-color-scheme: dark) {
  :root {
    --bpmn-stroke: #ccc;
    --bpmn-text: #eee;
    --bpmn-muted: #999;
    --bpmn-surface: #1e1e1e;
    --bpmn-label-background: #1e1e1ee6;
  }
}
```

An SVG loaded through `<img>` can't see the page's variables, so it shows the fallback colours.

## Known limits

- **Label widths are estimated.** There are no font metrics, so a label counts as 6.2 units per
  character. A task box grows to fit that estimate. Wide glyphs, such as CJK at about 11 units each,
  therefore overflow the box, and labels are never wrapped.
- **Message flows are routed simply.** Each runs straight through the gap beside its target's
  band, so two flows can share a stretch of line, and a flow can cross the shapes between its node
  and the pool border.
- **The layout is not interactive or configurable.** Any BPMN-DI in the file is not used.

## Changes

### 0.2.0

Breaking:

- The model types (`Model`, `Process`, `FlowNode`, `SequenceFlow`), `Options` and the enums are
  `#[non_exhaustive]`. Build them with the constructors and builder methods; fields stay public to
  read.
- `NodeKind::StartEvent`, `IntermediateThrowEvent` and `IntermediateCatchEvent` take an
  `Option<EventGlyph>`; `None` draws the plain event.
- `parse` reads a start event with no event definition as `StartEvent(None)`, drawn as a plain
  circle, and an untyped `<task>` as `TaskType::Plain`, drawn with no glyph.
- The output changes: every element is wrapped in a classed `<g>`, an event-based gateway draws a
  double ring around a pentagon, and an event sub-process has a dotted border.

Added: sequence-flow labels and default and conditional markers; `show_ids`; participants, pools
and message flows; element hooks (`id_prefix`, `node_attrs`, `flow_attrs`, `element_id`,
`Model::element_ids`); `Model::validate` and `Options::validate`; the conditional, multiple and
parallel-multiple glyphs.

### 0.1.0

First release.

## Licence

MIT OR Apache-2.0.
