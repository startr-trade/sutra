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
- sequence flows;
- embedded, transaction, ad-hoc and event sub-processes, drawn expanded around their content;
- boundary events, pinned to the activity they attach to;
- standard-loop and multi-instance markers;
- several processes in one file, drawn one above the other.

Everything else is ignored, including BPMN-DI, vendor extensions, conditions and timer expressions.
A label is the element's `name`, and each shape also shows its `id`.

`parse` returns an error when the XML is malformed, when the root is not a BPMN `<definitions>`, or
when a flow node cannot be drawn because its `id` is missing or repeated or a boundary event has no
`attachedToRef`. A DOCTYPE is refused and no entity is ever expanded.

A program with its own BPMN loader can build a `Model` directly and call `render`.

## Light and dark themes

By default colours are plain SVG attributes. With `css_variables`, each one is written as a CSS
variable whose fallback is the default colour:

```rust
let mut options = bpmn_svg::Options::default();
options.css_variables = true;
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
- **Some shapes are simplified.** An event-based gateway draws like an exclusive one, an event
  sub-process like an embedded one, a start event with no timer shows the message glyph, and an
  untyped `<task>` shows the manual glyph.
- **The layout is not interactive or configurable.** Any BPMN-DI in the file is not used.

## Licence

MIT OR Apache-2.0.
