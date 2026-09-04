//! BPMN diagrams **without BPMN-DI** — a layered auto-layout over the semantic model, emitted as
//! self-contained SVG in BPMN notation.
//!
//! Authored and scaffolded BPMN carries no `<bpmndi:BPMNDiagram>`: coordinates are layout, not
//! source, and `sutra create bpmn` has no business inventing them. So the diagram is computed
//! from what the file *does* say — flow nodes and sequence flows, via
//! [`sutra_bpmn::BpmnModelLoader`], never raw XML.
//!
//! The pipeline, per scope (a process, or a sub-process recursively):
//!
//! 1. **Rank** every box by longest path from the sources ([`longest_path_ranks`]) — one column
//!    per rank, so flow reads left to right.
//! 2. **Order** within each rank by the barycenter of its predecessors ([`barycenter_order`]),
//!    a single left-to-right down-sweep. This turns a gateway fan-out into clean lanes and keeps
//!    a short branch's terminal beside its own predecessor. (Up-sweeps by successors oscillate on
//!    two sequential gateways and strand a short branch — deliberately not done.)
//! 3. **Place** each node at the MEDIAN of its predecessors' y (lower median for an even count),
//!    then pack downward within the rank. The median — not the mean — keeps a single-predecessor
//!    chain on one horizontal line and keeps a merge aligned with its spine predecessor, so those
//!    edges stay straight instead of both bending.
//! 4. **Route** edges right-edge → left-edge, entering at the source's level when that lies within
//!    the target's span (a straight horizontal), else dog-legging into the middle of the target's
//!    left edge. Fan-out edges turn near the source (a comb); fan-in edges turn near the target so
//!    each stays on its own row through empty space.
//!
//! Everything is deterministic — document order drives node order and every tie-break — so a
//! regenerated catalog is byte-stable.
//!
//! The SVG is BPMN notation: thin-stroke circles for start events, thick for end, double rings for
//! intermediate/boundary, diamonds with the X/+/O/✳ marker for gateways, rounded rects for
//! activities (thick-bordered for a call activity), and expanded containers enclosing their
//! children. No embedded fonts, no external CSS — it inlines into Markdown as-is.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use sutra_bpmn::{Node, ProcessDefinition};

// ---- geometry constants (ported 1:1 from the Java renderer) ----------------------------------

const PADDING: f64 = 28.0;
const COL_GAP: f64 = 120.0; // horizontal gap between rank columns (room for edge id labels)
const ROW_GAP: f64 = 56.0; // vertical gap between rows within a rank (room for 2-line captions)
const SUB_PAD: f64 = 16.0; // inner padding around an expanded container's content
const SUB_HEADER: f64 = 24.0; // header strip (label) at the top of an expanded container
const BOUNDARY_SIZE: f64 = 36.0; // event circle drawn for a boundary event
const BOUNDARY_DROP: f64 = 30.0; // how far a boundary edge drops below the host before turning
const LEFT_STUB: f64 = 22.0; // horizontal stub before an edge enters a left edge
const PROCESS_GAP: f64 = 60.0; // vertical gap when a file declares several processes
const TASK_PAD_X: f64 = 12.0; // horizontal padding each side of a task box's centred label
const TASK_MIN_W: f64 = 120.0; // minimum task-box width
const TASK_H: f64 = 64.0; // task-box height
const EVENT_SIZE: f64 = 36.0;
const GATEWAY_SIZE: f64 = 50.0;

// Text-extent estimation (no font metrics available): a label centred at (cx, cy) is treated as a
// box (char count × char width) wide by one line tall, so the viewBox can grow to never clip it.
const NAME_CHAR_W: f64 = 6.2; // ~11px name font
const ID_CHAR_W: f64 = 5.2; // ~9px id / edge-label font
const LINE_ASCENT: f64 = 11.0;
const LINE_DESCENT: f64 = 3.0;

// ---- the visual classification of one BPMN node ----------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Class {
    StartEvent,
    EndEvent,
    IntermediateEvent,
    BoundaryEvent,
    Gateway,
    Task,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Gw {
    Exclusive,
    Parallel,
    Inclusive,
    Complex,
}

/// How one node draws: its shape family, BPMN glyph, activity markers and border weight.
struct Visual<'a> {
    class: Class,
    gateway: Gw,
    /// `<bpmn:transaction>` draws the doubled inline boundary; other containers a single rect.
    transaction: bool,
    label: Option<&'a str>,
    /// Event-definition glyph (`message` / `timer` / `error` / `escalation` / `signal` /
    /// `terminate` / `link` / `cancel` / `compensate`), or `""`.
    icon: &'static str,
    /// Task-type glyph (`service` / `script` / `send` / `user` / `rule` / `manual`), or `""`.
    task_icon: &'static str,
    /// A throwing event fills its glyph; a catching one leaves it hollow.
    throwing: bool,
    /// Call activities carry the BPMN thick border.
    thick: bool,
    /// Bottom-centre activity markers (`loop` / `parallel-mi` / `sequential-mi` / `ad-hoc`).
    markers: Vec<&'static str>,
    /// The enclosed scope of an expanded container.
    inner: Option<&'a ProcessDefinition>,
}

/// A plain shape with no glyphs or markers — every arm starts here and adds what it needs.
fn base(class: Class, label: Option<&str>) -> Visual<'_> {
    Visual {
        class,
        gateway: Gw::Exclusive,
        transaction: false,
        label,
        icon: "",
        task_icon: "",
        throwing: false,
        thick: false,
        markers: Vec::new(),
        inner: None,
    }
}

/// A BPMN `name`, treating blank as absent (an unnamed node shows only its id).
fn opt(o: &Option<String>) -> Option<&str> {
    o.as_deref().filter(|s| !s.trim().is_empty())
}

fn classify(n: &Node) -> Visual<'_> {
    match n {
        Node::StartEvent { name, timer, .. } => {
            let mut v = base(Class::StartEvent, opt(name));
            v.icon = if timer.is_some() { "timer" } else { "message" };
            v
        }
        Node::EndEvent { name, .. } => base(Class::EndEvent, opt(name)),
        Node::TerminateEndEvent { name, .. } => {
            let mut v = base(Class::EndEvent, opt(name));
            v.icon = "terminate";
            v.throwing = true;
            v
        }
        Node::ErrorEvent { name, .. } => {
            let mut v = base(Class::EndEvent, opt(name));
            v.icon = "error";
            v.throwing = true;
            v
        }
        Node::CancelEndEvent { name, .. } => {
            let mut v = base(Class::EndEvent, opt(name));
            v.icon = "cancel";
            v.throwing = true;
            v
        }
        Node::IntermediateThrowEvent { name, kind, .. } => {
            let mut v = base(Class::IntermediateEvent, opt(name));
            v.icon = match format!("{kind:?}").as_str() {
                k if k.starts_with("Signal") => "signal",
                k if k.starts_with("Escalation") => "escalation",
                k if k.starts_with("Link") => "link",
                k if k.starts_with("Compensat") => "compensate",
                _ => "message",
            };
            v.throwing = true;
            v
        }
        Node::LinkCatchEvent { name, .. } => {
            let mut v = base(Class::IntermediateEvent, opt(name));
            v.icon = "link";
            v
        }
        Node::MessageCatchEvent { name, .. } => {
            let mut v = base(Class::IntermediateEvent, opt(name));
            v.icon = "message";
            v
        }
        Node::TimerCatchEvent { name, .. } => {
            let mut v = base(Class::IntermediateEvent, opt(name));
            v.icon = "timer";
            v
        }
        Node::BoundaryEvent { name, kind, .. } => {
            let mut v = base(Class::BoundaryEvent, opt(name));
            v.icon = match format!("{kind:?}").as_str() {
                k if k.starts_with("Timer") => "timer",
                k if k.starts_with("Escalation") => "escalation",
                k if k.starts_with("Cancel") => "cancel",
                k if k.starts_with("Compensat") => "compensate",
                _ => "error",
            };
            v
        }
        Node::ServiceTask { name, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.task_icon = "service";
            v
        }
        Node::DataTask { name, .. } => base(Class::Task, opt(name)),
        Node::ScriptTask { name, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.task_icon = "script";
            v
        }
        Node::ManualTask { name, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.task_icon = "manual";
            v
        }
        Node::SendTask { name, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.task_icon = "send";
            v
        }
        Node::BusinessRuleTask { name, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.task_icon = "rule";
            v
        }
        Node::UserTask { name, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.task_icon = "user";
            v
        }
        Node::CallActivity { name, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.thick = true;
            v
        }
        Node::SubProcess { name, inner, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.inner = Some(inner);
            v
        }
        Node::TransactionSubProcess { name, inner, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.inner = Some(inner);
            v.transaction = true;
            v
        }
        Node::AdHocSubProcess { name, inner, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.inner = Some(inner);
            v.markers.push("ad-hoc");
            v
        }
        Node::EventSubProcess { name, inner, .. } => {
            let mut v = base(Class::Task, opt(name));
            v.inner = Some(inner);
            v
        }
        Node::ExclusiveGateway { name, .. } => base(Class::Gateway, opt(name)),
        Node::InclusiveGateway { name, .. } => {
            let mut v = base(Class::Gateway, opt(name));
            v.gateway = Gw::Inclusive;
            v
        }
        Node::ParallelGateway { name, .. } => {
            let mut v = base(Class::Gateway, opt(name));
            v.gateway = Gw::Parallel;
            v
        }
        Node::ComplexGateway { name, .. } => {
            let mut v = base(Class::Gateway, opt(name));
            v.gateway = Gw::Complex;
            v
        }
        // A loop wrapper decorates the activity it wraps: draw the inner shape, add the marker.
        Node::MultiInstance {
            inner, sequential, ..
        } => {
            let mut v = classify(inner);
            v.markers.push(if *sequential {
                "sequential-mi"
            } else {
                "parallel-mi"
            });
            v
        }
        Node::StandardLoop { inner, .. } => {
            let mut v = classify(inner);
            v.markers.push("loop");
            v
        }
    }
}

// ---- draw model -------------------------------------------------------------------------------

struct Shape {
    id: String,
    class: Class,
    gateway: Gw,
    transaction: bool,
    /// An expanded container encloses its children instead of drawing as a plain box.
    expanded: bool,
    x: f64,
    y: f64,
    w: f64,
    h: f64,
    label: String,
    icon: &'static str,
    task_icon: &'static str,
    throwing: bool,
    thick: bool,
    markers: Vec<&'static str>,
}

struct Edge {
    id: String,
    points: Vec<(f64, f64)>,
}

#[derive(Default)]
struct Bounds {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    seen: bool,
}

impl Bounds {
    fn expand(&mut self, x0: f64, y0: f64, x1: f64, y1: f64) {
        if !self.seen {
            self.seen = true;
            self.min_x = x0;
            self.min_y = y0;
            self.max_x = x1;
            self.max_y = y1;
            return;
        }
        self.min_x = self.min_x.min(x0);
        self.min_y = self.min_y.min(y0);
        self.max_x = self.max_x.max(x1);
        self.max_y = self.max_y.max(y1);
    }
}

#[derive(Default)]
struct ScopeLayout {
    shapes: Vec<Shape>,
    edges: Vec<Edge>,
    w: f64,
    h: f64,
}

/// One laid-out box during placement.
struct BoxInfo {
    class: Class,
    gateway: Gw,
    transaction: bool,
    label: String,
    icon: &'static str,
    task_icon: &'static str,
    throwing: bool,
    thick: bool,
    markers: Vec<&'static str>,
    w: f64,
    h: f64,
    cx: f64,
    cy: f64,
    inner: Option<ScopeLayout>,
}

// ---- public entry ------------------------------------------------------------------------------

/// Render every process in `module` as one self-contained SVG document, or `None` when the file
/// declares no flow nodes at all (an empty diagram is worse than none).
pub fn render_module_svg(processes: &[&ProcessDefinition]) -> Option<String> {
    let mut shapes: Vec<Shape> = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();
    let mut bounds = Bounds::default();

    let mut offset_y = 0.0;
    for p in processes {
        let sl = layout_scope(p);
        if sl.shapes.is_empty() {
            continue;
        }
        for s in sl.shapes {
            let s = offset_shape(s, 0.0, offset_y);
            bounds.expand(s.x, s.y, s.x + s.w, s.y + s.h);
            shapes.push(s);
        }
        for e in sl.edges {
            let e = offset_edge(e, 0.0, offset_y);
            for p in &e.points {
                bounds.expand(p.0, p.1, p.0, p.1);
            }
            edges.push(e);
        }
        offset_y += sl.h + PROCESS_GAP;
    }
    if shapes.is_empty() {
        return None;
    }
    Some(build_svg(&shapes, &edges, bounds))
}

// ---- layout ------------------------------------------------------------------------------------

/// Lay out one scope from its direct nodes. Containers recurse and are drawn expanded around their
/// content; boundary events are pinned to the bottom edge of the box they attach to. The result is
/// normalised so its min corner is `(0, 0)`, ready for a parent to offset.
fn layout_scope(p: &ProcessDefinition) -> ScopeLayout {
    let mut out = ScopeLayout::default();

    // Boundary events are pinned to a host edge, never laid out as free graph nodes.
    let mut boundary_host: BTreeMap<String, String> = BTreeMap::new();
    let mut boundary_order: Vec<String> = Vec::new();
    for n in p.nodes() {
        if let Node::BoundaryEvent {
            id,
            attached_to_ref,
            ..
        } = n
        {
            boundary_host.insert(id.clone(), attached_to_ref.clone());
            boundary_order.push(id.clone());
        }
    }

    // Boxes in document order.
    let mut ids: Vec<String> = Vec::new();
    let mut boxes: HashMap<String, BoxInfo> = HashMap::new();
    for n in p.nodes() {
        let id = n.id().to_string();
        if boundary_host.contains_key(&id) {
            continue;
        }
        let v = classify(n);
        let inner = v
            .inner
            .filter(|ip| !ip.nodes().is_empty())
            .map(layout_scope);
        let label = v.label.unwrap_or("").to_string();
        let (w, h) = match &inner {
            Some(il) if !il.shapes.is_empty() => {
                (il.w + 2.0 * SUB_PAD, il.h + SUB_HEADER + 2.0 * SUB_PAD)
            }
            _ => box_size(v.class, &label, &id),
        };
        boxes.insert(
            id.clone(),
            BoxInfo {
                class: v.class,
                gateway: v.gateway,
                transaction: v.transaction,
                label,
                icon: v.icon,
                task_icon: v.task_icon,
                throwing: v.throwing,
                thick: v.thick,
                markers: v.markers,
                w,
                h,
                cx: 0.0,
                cy: 0.0,
                inner: inner.filter(|il| !il.shapes.is_empty()),
            },
        );
        ids.push(id);
    }
    if boxes.is_empty() {
        return out;
    }

    // Ranking graph: box→box flows, plus a synthetic host→target edge for every boundary flow so a
    // box reached only via a boundary event still ranks after its host.
    let mut adj: HashMap<String, Vec<String>> = ids.iter().map(|i| (i.clone(), vec![])).collect();
    let mut indeg: HashMap<String, i64> = ids.iter().map(|i| (i.clone(), 0)).collect();
    for f in p.flows() {
        if boxes.contains_key(&f.source_ref) && boxes.contains_key(&f.target_ref) {
            adj.get_mut(&f.source_ref)
                .unwrap()
                .push(f.target_ref.clone());
            *indeg.get_mut(&f.target_ref).unwrap() += 1;
        } else if let Some(host) = boundary_host.get(&f.source_ref) {
            if boxes.contains_key(host) && boxes.contains_key(&f.target_ref) {
                adj.get_mut(host).unwrap().push(f.target_ref.clone());
                *indeg.get_mut(&f.target_ref).unwrap() += 1;
            }
        }
    }
    let rank = longest_path_ranks(&ids, &adj, &indeg);
    let max_rank = ids.iter().map(|i| rank[i]).max().unwrap_or(0);

    let mut by_rank: Vec<Vec<String>> = vec![Vec::new(); max_rank + 1];
    for id in &ids {
        by_rank[rank[id]].push(id.clone());
    }

    // Crossing reduction: a single left-to-right down-sweep, ordering each rank by the barycenter
    // of its predecessors' positions.
    let mut preds: HashMap<String, Vec<String>> = ids.iter().map(|i| (i.clone(), vec![])).collect();
    for src in &ids {
        for dst in &adj[src] {
            preds.get_mut(dst).unwrap().push(src.clone());
        }
    }
    let mut pos: HashMap<String, usize> = HashMap::new();
    for col in by_rank.iter() {
        for (i, id) in col.iter().enumerate() {
            pos.insert(id.clone(), i);
        }
    }
    for col in by_rank.iter_mut().skip(1) {
        barycenter_order(col, &preds, &mut pos);
    }

    // Column x: one column per rank, width = the widest box in it, so an expanded container
    // reserves its own column instead of overlapping neighbours.
    let mut max_w = vec![0.0f64; max_rank + 1];
    for id in &ids {
        let r = rank[id];
        max_w[r] = max_w[r].max(boxes[id].w);
    }
    // Widen the gap after a rank to fit the widest edge-id caption crossing it, so a straight
    // edge's "(id)" always has room on the line rather than bailing far from its edge.
    let mut gap_after = vec![COL_GAP; max_rank + 1];
    for f in p.flows() {
        if !boxes.contains_key(&f.source_ref) || !boxes.contains_key(&f.target_ref) {
            continue;
        }
        let (rs, rt) = (rank[&f.source_ref], rank[&f.target_ref]);
        if rt == rs + 1 && !f.id.is_empty() {
            let label_w = (f.id.chars().count() as f64 + 2.0) * ID_CHAR_W + LEFT_STUB + 40.0;
            gap_after[rs] = gap_after[rs].max(label_w);
        }
    }
    let mut col_left_x = vec![0.0f64; max_rank + 1];
    let mut acc = 0.0;
    for r in 0..=max_rank {
        col_left_x[r] = acc;
        acc += max_w[r] + gap_after[r];
    }

    // Vertical placement: each node at the MEDIAN of its predecessors' y (lower median for an even
    // count), then packed downward within the rank. Ranks left-to-right, so every predecessor's y
    // is final before its successors read it.
    for r in 0..=max_rank {
        let col = by_rank[r].clone();
        let mut prev_bottom: Option<f64> = None;
        for id in &col {
            let (bw, bh) = (boxes[id].w, boxes[id].h);
            let cx = col_left_x[r] + max_w[r] / 2.0;
            let mut pcy: Vec<f64> = preds[id]
                .iter()
                .filter(|pid| rank[*pid] < r)
                .map(|pid| boxes[pid].cy)
                .collect();
            let desired = if pcy.is_empty() {
                match prev_bottom {
                    None => bh / 2.0,
                    Some(pb) => pb + ROW_GAP + bh / 2.0,
                }
            } else {
                pcy.sort_by(|a, b| a.partial_cmp(b).unwrap());
                pcy[(pcy.len() - 1) / 2]
            };
            let min_cy = match prev_bottom {
                None => desired,
                Some(pb) => pb + ROW_GAP + bh / 2.0,
            };
            let cy = desired.max(min_cy);
            prev_bottom = Some(cy + bh / 2.0);
            let b = boxes.get_mut(id).unwrap();
            b.cx = cx;
            b.cy = cy;
            let _ = bw;
        }
    }

    // Fan-in straightening: when every incoming edge of a target comes from a leaf source, spread
    // those sources centred on the target and spaced by their caption footprint, so each edge still
    // enters straight and no caption is overlaid.
    straighten_leaf_fan_in(p, &mut boxes, &boundary_host, &indeg);

    // A box reached ONLY via a boundary edge drops to the boundary's exit level, so its incoming
    // flow is a single clean drop-then-straight rather than a double dog-leg around a tall host.
    drop_boundary_only_targets(p, &mut boxes, &boundary_host);

    // Emit boxes. A container emits its own rect first, then its offset children on top.
    for id in &ids {
        let b = boxes.get_mut(id).unwrap();
        let x = b.cx - b.w / 2.0;
        let y = b.cy - b.h / 2.0;
        let inner = b.inner.take();
        out.shapes.push(Shape {
            id: id.clone(),
            class: b.class,
            gateway: b.gateway,
            transaction: b.transaction,
            expanded: inner.is_some(),
            x,
            y,
            w: b.w,
            h: b.h,
            label: b.label.clone(),
            icon: b.icon,
            task_icon: b.task_icon,
            throwing: b.throwing,
            thick: b.thick,
            markers: b.markers.clone(),
        });
        if let Some(il) = inner {
            let ox = x + SUB_PAD;
            let oy = y + SUB_HEADER + SUB_PAD;
            for s in il.shapes {
                out.shapes.push(offset_shape(s, ox, oy));
            }
            for e in il.edges {
                out.edges.push(offset_edge(e, ox, oy));
            }
        }
    }

    // Boundary events: pinned straddling the bottom edge of their host, spread evenly when several
    // share one host.
    let mut boundary_pos: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
    let mut by_host: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for be in &boundary_order {
        if let Some(host) = boundary_host.get(be) {
            if boxes.contains_key(host) {
                by_host.entry(host.clone()).or_default().push(be.clone());
            }
        }
    }
    for (host_id, bes) in &by_host {
        let host = &boxes[host_id];
        let left = host.cx - host.w / 2.0;
        let bottom_y = host.cy + host.h / 2.0;
        let n = bes.len() as f64;
        for (i, be_id) in bes.iter().enumerate() {
            let bcx = left + host.w * (i as f64 + 1.0) / (n + 1.0);
            boundary_pos.insert(be_id.clone(), (bcx, bottom_y, BOUNDARY_SIZE, BOUNDARY_SIZE));
            let v = p.nodes().iter().find(|n| n.id() == be_id).map(classify);
            out.shapes.push(Shape {
                id: be_id.clone(),
                class: Class::BoundaryEvent,
                gateway: Gw::Exclusive,
                transaction: false,
                expanded: false,
                x: bcx - BOUNDARY_SIZE / 2.0,
                y: bottom_y - BOUNDARY_SIZE / 2.0,
                w: BOUNDARY_SIZE,
                h: BOUNDARY_SIZE,
                label: v.as_ref().and_then(|v| v.label).unwrap_or("").to_string(),
                icon: v.as_ref().map(|v| v.icon).unwrap_or("error"),
                task_icon: "",
                throwing: false,
                thick: false,
                markers: Vec::new(),
            });
        }
    }

    // Sequence flows, orthogonally routed right-edge → left-edge.
    for f in p.flows() {
        if boundary_host.contains_key(&f.target_ref) {
            continue; // never draw an edge INTO a boundary event
        }
        let Some(s) = position_of(&f.source_ref, &boxes, &boundary_pos) else {
            continue;
        };
        let Some(t) = position_of(&f.target_ref, &boxes, &boundary_pos) else {
            continue;
        };
        let tx = t.0 - t.2 / 2.0;
        let margin = 6.0f64.min(t.3 / 2.0);

        let pts = if boundary_pos.contains_key(&f.source_ref) {
            // Boundary source: exit the bottom of the pinned event, drop clear of the host, then
            // run across into the target's left edge at its centre.
            let ty = t.1;
            let bx = s.0;
            let by_bottom = s.1 + s.3 / 2.0;
            let route_y = by_bottom + BOUNDARY_DROP;
            let approach_x = tx - LEFT_STUB;
            orthogonal_path(&[
                (bx, by_bottom),
                (bx, route_y),
                (approach_x, route_y),
                (approach_x, ty),
                (tx, ty),
            ])
        } else {
            let sx = s.0 + s.2 / 2.0;
            let sy = s.1;
            // Straight when the source lies within the target's span; else dog-leg into the middle
            // of the target's left edge, so a branch drop lands centred on the node.
            let ty = if (sy - t.1).abs() <= t.3 / 2.0 - margin {
                sy
            } else {
                t.1
            };
            // Fan-out turns near the SOURCE (a comb); fan-in near the TARGET, so each convergence
            // line stays on its own source row through empty space instead of along the hub's row.
            let fan_in = preds.get(&f.target_ref).map(|v| v.len()).unwrap_or(0) > 1;
            let turn_x = if fan_in {
                (tx - LEFT_STUB).max(sx + LEFT_STUB)
            } else {
                (sx + LEFT_STUB).min(tx - LEFT_STUB)
            };
            if (sy - ty).abs() > 0.5 {
                orthogonal_path(&[(sx, sy), (turn_x, sy), (turn_x, ty), (tx, ty)])
            } else {
                orthogonal_path(&[(sx, sy), (tx, ty)])
            }
        };
        out.edges.push(Edge {
            id: f.id.clone(),
            points: pts,
        });
    }

    normalize(&mut out);
    out
}

/// Distribute leaf sources that all feed one target, centred on it and spaced by their caption
/// footprint — so every edge still enters straight and no caption is overlaid by the next node.
fn straighten_leaf_fan_in(
    p: &ProcessDefinition,
    boxes: &mut HashMap<String, BoxInfo>,
    boundary_host: &BTreeMap<String, String>,
    indeg: &HashMap<String, i64>,
) {
    let mut in_total: BTreeMap<String, usize> = BTreeMap::new();
    let mut all_leaf: BTreeMap<String, bool> = BTreeMap::new();
    let mut leaf_sources: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for f in p.flows() {
        if boundary_host.contains_key(&f.target_ref) || !boxes.contains_key(&f.target_ref) {
            continue;
        }
        let leaf_src = boxes.contains_key(&f.source_ref)
            && indeg.get(&f.source_ref).copied().unwrap_or(0) == 0;
        *in_total.entry(f.target_ref.clone()).or_insert(0) += 1;
        let e = all_leaf.entry(f.target_ref.clone()).or_insert(true);
        *e = *e && leaf_src;
        if leaf_src {
            leaf_sources
                .entry(f.target_ref.clone())
                .or_default()
                .push(f.source_ref.clone());
        }
    }
    for (tgt, srcs) in &leaf_sources {
        if srcs.len() < 2
            || in_total.get(tgt).copied().unwrap_or(0) != srcs.len()
            || !all_leaf.get(tgt).copied().unwrap_or(false)
        {
            continue;
        }
        let t_cy = boxes[tgt].cy;
        let spacing = srcs
            .iter()
            .map(|s| boxes[s].h + 40.0)
            .fold(0.0f64, f64::max);
        let start = t_cy - spacing * (srcs.len() as f64 - 1.0) / 2.0;
        for (k, sid) in srcs.iter().enumerate() {
            boxes.get_mut(sid).unwrap().cy = start + k as f64 * spacing;
        }
    }
}

/// Drop a box reached only via a boundary edge to that boundary's exit level, so the flow is a
/// single clean drop-then-straight through the empty band beside a tall host.
fn drop_boundary_only_targets(
    p: &ProcessDefinition,
    boxes: &mut HashMap<String, BoxInfo>,
    boundary_host: &BTreeMap<String, String>,
) {
    let mut only_boundary_in: BTreeMap<String, bool> = BTreeMap::new();
    let mut route_y: BTreeMap<String, f64> = BTreeMap::new();
    for f in p.flows() {
        if !boxes.contains_key(&f.target_ref) {
            continue;
        }
        let from_boundary = boundary_host.contains_key(&f.source_ref);
        let e = only_boundary_in.entry(f.target_ref.clone()).or_insert(true);
        *e = *e && from_boundary;
        if from_boundary {
            if let Some(host) = boundary_host.get(&f.source_ref) {
                if let Some(h) = boxes.get(host) {
                    let y = h.cy + h.h / 2.0 + BOUNDARY_SIZE / 2.0 + BOUNDARY_DROP;
                    let slot = route_y.entry(f.target_ref.clone()).or_insert(y);
                    *slot = slot.max(y);
                }
            }
        }
    }
    for (id, y) in &route_y {
        if only_boundary_in.get(id).copied().unwrap_or(false) {
            if let Some(b) = boxes.get_mut(id) {
                b.cy = *y;
            }
        }
    }
}

/// Longest-path ranking: every node one column right of its deepest predecessor. Cycles (which BPMN
/// permits) are broken by seeding the next unprocessed node in document order, keeping it total.
fn longest_path_ranks(
    ids: &[String],
    adj: &HashMap<String, Vec<String>>,
    indeg: &HashMap<String, i64>,
) -> HashMap<String, usize> {
    let mut rank: HashMap<String, usize> = ids.iter().map(|i| (i.clone(), 0usize)).collect();
    let mut remaining = indeg.clone();
    let mut queue: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    for id in ids {
        if indeg.get(id).copied().unwrap_or(0) == 0 {
            queue.push_back(id.clone());
        }
    }
    let mut processed: std::collections::HashSet<String> = std::collections::HashSet::new();
    while processed.len() < ids.len() {
        if queue.is_empty() {
            if let Some(next) = ids.iter().find(|i| !processed.contains(*i)) {
                queue.push_back(next.clone());
            } else {
                break;
            }
        }
        let Some(u) = queue.pop_front() else { break };
        if !processed.insert(u.clone()) {
            continue;
        }
        let ru = rank[&u];
        for v in adj.get(&u).map(|v| v.as_slice()).unwrap_or(&[]) {
            if rank[v] < ru + 1 {
                rank.insert(v.clone(), ru + 1);
            }
            let left = remaining.entry(v.clone()).or_insert(0);
            *left -= 1;
            if *left <= 0 && !processed.contains(v) {
                queue.push_back(v.clone());
            }
        }
    }
    rank
}

/// One barycenter sweep over a single rank: stably reorder by the mean position of each node's
/// neighbours in the adjacent rank. A node with no neighbour keeps its position; ties break on the
/// prior position, so the pass is deterministic and idempotent once settled.
fn barycenter_order(
    rank_nodes: &mut [String],
    neighbours: &HashMap<String, Vec<String>>,
    pos: &mut HashMap<String, usize>,
) {
    if rank_nodes.len() < 2 {
        return;
    }
    let mut key: HashMap<String, f64> = HashMap::new();
    for n in rank_nodes.iter() {
        let mut sum = 0.0;
        let mut count = 0.0;
        for m in neighbours.get(n).map(|v| v.as_slice()).unwrap_or(&[]) {
            if let Some(p) = pos.get(m) {
                sum += *p as f64;
                count += 1.0;
            }
        }
        key.insert(
            n.clone(),
            if count == 0.0 {
                pos[n] as f64
            } else {
                sum / count
            },
        );
    }
    rank_nodes.sort_by(|a, b| {
        key[a]
            .partial_cmp(&key[b])
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| pos[a].cmp(&pos[b]))
    });
    for (i, n) in rank_nodes.iter().enumerate() {
        pos.insert(n.clone(), i);
    }
}

/// Build a polyline from candidate waypoints, dropping consecutive duplicates and collapsing three
/// collinear points into two, so the path stays clean even when a leg degenerates to zero length.
fn orthogonal_path(candidates: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let mut pts: Vec<(f64, f64)> = Vec::with_capacity(candidates.len());
    for p in candidates {
        if let Some(last) = pts.last() {
            if (last.0 - p.0).abs() < 0.01 && (last.1 - p.1).abs() < 0.01 {
                continue;
            }
        }
        pts.push(*p);
        while pts.len() >= 3 {
            let n = pts.len();
            let (a, b, c) = (pts[n - 3], pts[n - 2], pts[n - 1]);
            let collinear = ((b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)).abs() < 0.01;
            if collinear {
                pts.remove(n - 2);
            } else {
                break;
            }
        }
    }
    if pts.len() < 2 {
        pts = candidates.to_vec();
    }
    pts
}

/// Centre + size of a routing endpoint: a laid-out box, else a pinned boundary event.
fn position_of(
    id: &str,
    boxes: &HashMap<String, BoxInfo>,
    boundary_pos: &HashMap<String, (f64, f64, f64, f64)>,
) -> Option<(f64, f64, f64, f64)> {
    if let Some(b) = boxes.get(id) {
        return Some((b.cx, b.cy, b.w, b.h));
    }
    boundary_pos.get(id).copied()
}

/// Text-aware box size. Events and gateways keep fixed geometry (their captions sit OUTSIDE the
/// shape); a task box widens to contain its own centred captions so a long label never clips.
fn box_size(class: Class, label: &str, id: &str) -> (f64, f64) {
    match class {
        Class::StartEvent | Class::EndEvent | Class::IntermediateEvent | Class::BoundaryEvent => {
            (EVENT_SIZE, EVENT_SIZE)
        }
        Class::Gateway => (GATEWAY_SIZE, GATEWAY_SIZE),
        Class::Task => (
            TASK_MIN_W.max(caption_width(label, id) + 2.0 * TASK_PAD_X),
            TASK_H,
        ),
    }
}

/// Estimated width of a caption block — the wider of its name and its `(id)` line.
fn caption_width(label: &str, id: &str) -> f64 {
    let name_w = label.chars().count() as f64 * NAME_CHAR_W;
    let id_w = if id.is_empty() {
        0.0
    } else {
        (id.chars().count() as f64 + 2.0) * ID_CHAR_W
    };
    name_w.max(id_w)
}

fn offset_shape(mut s: Shape, dx: f64, dy: f64) -> Shape {
    s.x += dx;
    s.y += dy;
    s
}

fn offset_edge(mut e: Edge, dx: f64, dy: f64) -> Edge {
    for p in &mut e.points {
        p.0 += dx;
        p.1 += dy;
    }
    e
}

/// Shift a laid-out scope so its min corner is `(0, 0)`, recording its extent.
fn normalize(sl: &mut ScopeLayout) {
    let mut b = Bounds::default();
    for s in &sl.shapes {
        b.expand(s.x, s.y, s.x + s.w, s.y + s.h);
    }
    for e in &sl.edges {
        for p in &e.points {
            b.expand(p.0, p.1, p.0, p.1);
        }
    }
    if !b.seen {
        return;
    }
    let (dx, dy) = (-b.min_x, -b.min_y);
    for s in sl.shapes.iter_mut() {
        s.x += dx;
        s.y += dy;
    }
    for e in sl.edges.iter_mut() {
        for p in &mut e.points {
            p.0 += dx;
            p.1 += dy;
        }
    }
    sl.w = b.max_x - b.min_x;
    sl.h = b.max_y - b.min_y;
}

// ---- SVG ----------------------------------------------------------------------------------------

fn build_svg(shapes: &[Shape], edges: &[Edge], mut bounds: Bounds) -> String {
    // Place every edge caption FIRST (needs global knowledge of all routed segments) so each is
    // offset off its own line and kept clear of every other line and node.
    let mut segments: Vec<(f64, f64, f64, f64)> = Vec::new();
    for e in edges {
        for w in e.points.windows(2) {
            segments.push((w[0].0, w[0].1, w[1].0, w[1].1));
        }
    }
    // Expanded containers are excluded: their inner edge captions legitimately sit inside them.
    let node_boxes: Vec<(f64, f64, f64, f64)> = shapes
        .iter()
        .filter(|s| !s.expanded)
        .map(|s| (s.x, s.y, s.w, s.h))
        .collect();

    let mut label_boxes: Vec<(f64, f64, f64, f64)> = Vec::new();
    let mut label_svgs: Vec<String> = Vec::new();
    for e in edges {
        place_edge_label(e, &segments, &node_boxes, &mut label_boxes, &mut label_svgs);
    }

    expand_for_node_labels(&mut bounds, shapes);
    for b in &label_boxes {
        bounds.expand(b.0, b.1, b.0 + b.2, b.1 + b.3);
    }

    let min_x = bounds.min_x - PADDING;
    let min_y = bounds.min_y - PADDING;
    let width = (bounds.max_x - bounds.min_x) + 2.0 * PADDING;
    let height = (bounds.max_y - bounds.min_y) + 2.0 * PADDING;

    let mut sb = String::with_capacity(1024 + 64 * (shapes.len() + edges.len()));
    let _ = write!(
        sb,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"{} {} {} {}\" \
         width=\"{}\" height=\"{}\" style=\"max-width:100%;height:auto\" \
         role=\"img\" aria-label=\"BPMN diagram\">",
        fmt(min_x),
        fmt(min_y),
        fmt(width),
        fmt(height),
        fmt(width),
        fmt(height)
    );
    sb.push_str(
        "<defs><marker id=\"arrow\" viewBox=\"0 0 10 10\" refX=\"9\" refY=\"5\" \
         markerWidth=\"8\" markerHeight=\"8\" orient=\"auto-start-reverse\">\
         <path d=\"M0,0 L10,5 L0,10 z\" fill=\"#333\"/></marker></defs>",
    );
    // Edge PATHS first so shapes overpaint their endpoints; shapes next; edge LABELS last so a
    // caption stays legible on top of lines and beside shapes.
    for e in edges {
        sb.push_str(&render_edge_path(e));
    }
    for s in shapes {
        sb.push_str(&render_shape(s));
    }
    for l in &label_svgs {
        sb.push_str(l);
    }
    sb.push_str("</svg>");
    sb
}

fn render_shape(s: &Shape) -> String {
    if s.expanded {
        return render_container(s);
    }
    match s.class {
        Class::Gateway => render_gateway(s),
        Class::Task => render_task(s),
        _ => render_event(s),
    }
}

/// An expanded sub-process / transaction: a rounded rect (doubled for a transaction, per the BPMN
/// inline-boundary convention) enclosing its already-offset children, labelled at the top-left.
fn render_container(s: &Shape) -> String {
    let mut sb = String::with_capacity(320);
    let _ = write!(
        sb,
        "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" rx=\"6\" ry=\"6\" \
         fill=\"none\" stroke=\"#333\" stroke-width=\"1.5\"/>",
        fmt(s.x),
        fmt(s.y),
        fmt(s.w),
        fmt(s.h)
    );
    if s.transaction {
        let i = 3.0;
        let _ = write!(
            sb,
            "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" rx=\"4\" ry=\"4\" \
             fill=\"none\" stroke=\"#333\" stroke-width=\"1\"/>",
            fmt(s.x + i),
            fmt(s.y + i),
            fmt(s.w - 2.0 * i),
            fmt(s.h - 2.0 * i)
        );
    }
    let has_name = !s.label.trim().is_empty();
    let ly = s.y + 15.0;
    if has_name {
        let _ = write!(
            sb,
            "<text x=\"{}\" y=\"{}\" text-anchor=\"start\" font-family=\"system-ui, sans-serif\" \
             font-size=\"11\" font-weight=\"600\" fill=\"#222\">{}</text>",
            fmt(s.x + 10.0),
            fmt(ly),
            escape_text(&s.label)
        );
    }
    if !s.id.is_empty() {
        let iy = if has_name { ly + 11.0 } else { ly };
        let _ = write!(
            sb,
            "<text x=\"{}\" y=\"{}\" text-anchor=\"start\" font-family=\"system-ui, sans-serif\" \
             font-size=\"9\" fill=\"#888\">({})</text>",
            fmt(s.x + 10.0),
            fmt(iy),
            escape_text(&s.id)
        );
    }
    sb
}

fn render_event(s: &Shape) -> String {
    let cx = s.x + s.w / 2.0;
    let cy = s.y + s.h / 2.0;
    let r = s.w.min(s.h) / 2.0;
    let end = s.class == Class::EndEvent;
    let ring = matches!(s.class, Class::IntermediateEvent | Class::BoundaryEvent);
    let mut sb = String::with_capacity(220);
    let _ = write!(
        sb,
        "<circle cx=\"{}\" cy=\"{}\" r=\"{}\" fill=\"#fff\" stroke=\"#333\" stroke-width=\"{}\"/>",
        fmt(cx),
        fmt(cy),
        fmt(r),
        if end { "3" } else { "1.5" }
    );
    if ring {
        let _ = write!(
            sb,
            "<circle cx=\"{}\" cy=\"{}\" r=\"{}\" fill=\"none\" stroke=\"#333\" \
             stroke-width=\"1\"/>",
            fmt(cx),
            fmt(cy),
            fmt(r - 3.0)
        );
    }
    if !s.icon.is_empty() {
        sb.push_str(&event_icon_svg(s.icon, cx, cy, r * 0.55, s.throwing));
    }
    append_label(&mut sb, &s.label, &s.id, cx, s.y + s.h + 14.0);
    sb
}

fn render_gateway(s: &Shape) -> String {
    let cx = s.x + s.w / 2.0;
    let cy = s.y + s.h / 2.0;
    let mut sb = String::with_capacity(220);
    let _ = write!(
        sb,
        "<polygon points=\"{},{} {},{} {},{} {},{}\" fill=\"#fff\" stroke=\"#333\" \
         stroke-width=\"1.5\"/>",
        fmt(cx),
        fmt(s.y),
        fmt(s.x + s.w),
        fmt(cy),
        fmt(cx),
        fmt(s.y + s.h),
        fmt(s.x),
        fmt(cy)
    );
    sb.push_str(&gateway_marker(s.gateway, cx, cy, s.w.min(s.h) * 0.28));
    append_label(&mut sb, &s.label, &s.id, cx, s.y + s.h + 14.0);
    sb
}

/// The BPMN symbol inside a gateway diamond: X exclusive, + parallel, O inclusive, ✳ complex.
fn gateway_marker(gw: Gw, cx: f64, cy: f64, m: f64) -> String {
    const STROKE: &str = "stroke=\"#333\" stroke-width=\"2\" fill=\"none\"";
    match gw {
        Gw::Parallel => format!(
            "<path d=\"M{},{} H{} M{},{} V{}\" {}/>",
            fmt(cx - m),
            fmt(cy),
            fmt(cx + m),
            fmt(cx),
            fmt(cy - m),
            fmt(cy + m),
            STROKE
        ),
        Gw::Inclusive => format!(
            "<circle cx=\"{}\" cy=\"{}\" r=\"{}\" {}/>",
            fmt(cx),
            fmt(cy),
            fmt(m),
            STROKE
        ),
        Gw::Complex => format!(
            "<path d=\"M{},{} H{} M{},{} V{} M{},{} L{},{} M{},{} L{},{}\" {}/>",
            fmt(cx - m),
            fmt(cy),
            fmt(cx + m),
            fmt(cx),
            fmt(cy - m),
            fmt(cy + m),
            fmt(cx - m * 0.7),
            fmt(cy - m * 0.7),
            fmt(cx + m * 0.7),
            fmt(cy + m * 0.7),
            fmt(cx - m * 0.7),
            fmt(cy + m * 0.7),
            fmt(cx + m * 0.7),
            fmt(cy - m * 0.7),
            STROKE
        ),
        Gw::Exclusive => {
            let d = m * 0.8;
            format!(
                "<path d=\"M{},{} L{},{} M{},{} L{},{}\" {}/>",
                fmt(cx - d),
                fmt(cy - d),
                fmt(cx + d),
                fmt(cy + d),
                fmt(cx - d),
                fmt(cy + d),
                fmt(cx + d),
                fmt(cy - d),
                STROKE
            )
        }
    }
}

fn render_task(s: &Shape) -> String {
    let mut sb = String::with_capacity(256);
    let _ = write!(
        sb,
        "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" rx=\"6\" ry=\"6\" fill=\"#fff\" \
         stroke=\"#333\" stroke-width=\"{}\"/>",
        fmt(s.x),
        fmt(s.y),
        fmt(s.w),
        fmt(s.h),
        if s.thick { "3.5" } else { "1.5" }
    );
    if !s.task_icon.is_empty() {
        sb.push_str(&task_icon_svg(s.task_icon, s.x + 4.0, s.y + 4.0));
    }
    append_label(&mut sb, &s.label, &s.id, s.x + s.w / 2.0, s.y + s.h / 2.0);
    if !s.markers.is_empty() {
        sb.push_str(&markers_svg(&s.markers, s.x + s.w / 2.0, s.y + s.h - 9.0));
    }
    sb
}

/// A node caption: the BPMN `name` on the first line, its element id in parentheses below in a
/// smaller, subtler style. An unnamed node shows only `(id)`.
fn append_label(sb: &mut String, label: &str, id: &str, cx: f64, cy: f64) {
    let has_name = !label.trim().is_empty();
    let mut y = cy;
    if has_name {
        let _ = write!(
            sb,
            "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\" \
             font-family=\"system-ui, sans-serif\" font-size=\"11\" fill=\"#222\">{}</text>",
            fmt(cx),
            fmt(y),
            escape_text(label)
        );
        y += 11.0;
    }
    if !id.is_empty() {
        let _ = write!(
            sb,
            "<text x=\"{}\" y=\"{}\" text-anchor=\"middle\" \
             font-family=\"system-ui, sans-serif\" font-size=\"9\" fill=\"#888\">({})</text>",
            fmt(cx),
            fmt(y),
            escape_text(id)
        );
    }
}

fn render_edge_path(e: &Edge) -> String {
    let mut d = String::new();
    for (i, p) in e.points.iter().enumerate() {
        let _ = write!(
            d,
            "{}{},{} ",
            if i == 0 { "M" } else { "L" },
            fmt(p.0),
            fmt(p.1)
        );
    }
    format!(
        "<path d=\"{}\" fill=\"none\" stroke=\"#333\" stroke-width=\"1.2\" \
         marker-end=\"url(#arrow)\"/>",
        d.trim_end()
    )
}

/// Place a flow's `(id)` caption so it stays legible: offset perpendicular off its own line (so the
/// line never bisects the text) and, among candidate offsets, chosen to avoid overlapping any
/// routed segment, any node shape, and any previously-placed caption. If none is fully clear the
/// least-bad wins — a node overlap is penalised worst, then another caption, then a line (which the
/// caption's own background rect mostly masks anyway).
fn place_edge_label(
    e: &Edge,
    segments: &[(f64, f64, f64, f64)],
    node_boxes: &[(f64, f64, f64, f64)],
    label_boxes: &mut Vec<(f64, f64, f64, f64)>,
    label_svgs: &mut Vec<String>,
) {
    if e.id.is_empty() || e.points.len() < 2 {
        return;
    }
    // The longest leg carries the caption — a degenerate stub would push it off the diagram.
    let mut best = 0usize;
    let mut best_len = -1.0f64;
    for (i, w) in e.points.windows(2).enumerate() {
        let len = ((w[1].0 - w[0].0).powi(2) + (w[1].1 - w[0].1).powi(2)).sqrt();
        if len > best_len {
            best_len = len;
            best = i;
        }
    }
    let (a, b) = (e.points[best], e.points[best + 1]);
    let (mx, my) = ((a.0 + b.0) / 2.0, (a.1 + b.1) / 2.0);
    let horizontal = (b.1 - a.1).abs() <= (b.0 - a.0).abs();

    let text = format!("({})", e.id);
    let w = text.chars().count() as f64 * ID_CHAR_W + 6.0;
    let h = LINE_ASCENT + LINE_DESCENT;

    // Perpendicular candidates, nearest first, both sides.
    let offsets: [f64; 6] = [-11.0, 11.0, -22.0, 22.0, -33.0, 33.0];
    let mut chosen = (mx - w / 2.0, my - h / 2.0);
    let mut best_score = i32::MAX;
    for off in offsets {
        let (cx, cy) = if horizontal {
            (mx, my + off)
        } else {
            (mx + off, my)
        };
        let box_ = (cx - w / 2.0, cy - h / 2.0, w, h);
        let score = 5 * count_box_hits(&box_, node_boxes)
            + 3 * count_box_hits(&box_, label_boxes)
            + count_segment_hits(&box_, segments);
        if score < best_score {
            best_score = score;
            chosen = (box_.0, box_.1);
            if score == 0 {
                break;
            }
        }
    }
    let box_ = (chosen.0, chosen.1, w, h);
    label_boxes.push(box_);
    label_svgs.push(format!(
        "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" fill=\"#ffffffe6\" stroke=\"none\"/>\
         <text x=\"{}\" y=\"{}\" text-anchor=\"middle\" font-family=\"system-ui, sans-serif\" \
         font-size=\"9\" fill=\"#888\">{}</text>",
        fmt(box_.0),
        fmt(box_.1),
        fmt(box_.2),
        fmt(box_.3),
        fmt(box_.0 + w / 2.0),
        fmt(box_.1 + LINE_ASCENT),
        escape_text(&text)
    ));
}

fn count_box_hits(b: &(f64, f64, f64, f64), others: &[(f64, f64, f64, f64)]) -> i32 {
    others
        .iter()
        .filter(|o| b.0 < o.0 + o.2 && o.0 < b.0 + b.2 && b.1 < o.1 + o.3 && o.1 < b.1 + b.3)
        .count() as i32
}

fn count_segment_hits(b: &(f64, f64, f64, f64), segments: &[(f64, f64, f64, f64)]) -> i32 {
    segments
        .iter()
        .filter(|s| seg_intersects_rect(s.0, s.1, s.2, s.3, b))
        .count() as i32
}

fn seg_intersects_rect(x1: f64, y1: f64, x2: f64, y2: f64, r: &(f64, f64, f64, f64)) -> bool {
    let (rx, ry, rw, rh) = *r;
    // Either endpoint inside, or the segment crosses one of the four edges.
    let inside = |x: f64, y: f64| x >= rx && x <= rx + rw && y >= ry && y <= ry + rh;
    if inside(x1, y1) || inside(x2, y2) {
        return true;
    }
    let edges = [
        (rx, ry, rx + rw, ry),
        (rx + rw, ry, rx + rw, ry + rh),
        (rx + rw, ry + rh, rx, ry + rh),
        (rx, ry + rh, rx, ry),
    ];
    edges
        .iter()
        .any(|e| seg_seg(x1, y1, x2, y2, e.0, e.1, e.2, e.3))
}

#[allow(clippy::too_many_arguments)]
fn seg_seg(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64, dx: f64, dy: f64) -> bool {
    let d1 = cross(cx, cy, dx, dy, ax, ay);
    let d2 = cross(cx, cy, dx, dy, bx, by);
    let d3 = cross(ax, ay, bx, by, cx, cy);
    let d4 = cross(ax, ay, bx, by, dx, dy);
    ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
}

fn cross(p1x: f64, p1y: f64, p2x: f64, p2y: f64, px: f64, py: f64) -> f64 {
    (p2x - p1x) * (py - p1y) - (p2y - p1y) * (px - p1x)
}

/// Grow `bounds` to enclose every rendered node caption — an event/gateway caption below the shape,
/// a task's centred two-line caption, a container's top-left name+id. Widths are estimated as
/// `char count × char width`; there are no font metrics available.
fn expand_for_node_labels(bounds: &mut Bounds, shapes: &[Shape]) {
    for s in shapes {
        let has_name = !s.label.trim().is_empty();
        let lines = usize::from(has_name) + usize::from(!s.id.is_empty());
        if lines == 0 {
            continue;
        }
        let w = caption_width(&s.label, &s.id);
        let (cx, top) = if s.expanded {
            (s.x + 10.0 + w / 2.0, s.y + 15.0 - LINE_ASCENT)
        } else if s.class == Class::Task {
            (s.x + s.w / 2.0, s.y + s.h / 2.0 - LINE_ASCENT)
        } else {
            (s.x + s.w / 2.0, s.y + s.h + 14.0 - LINE_ASCENT)
        };
        let h = lines as f64 * LINE_ASCENT + LINE_DESCENT;
        bounds.expand(cx - w / 2.0, top, cx + w / 2.0, top + h);
    }
}

// ---- glyphs -------------------------------------------------------------------------------------

/// The event-definition glyph inside an event circle. A throwing event fills it; a catching one
/// leaves it hollow — the BPMN convention.
fn event_icon_svg(icon: &str, cx: f64, cy: f64, r: f64, throwing: bool) -> String {
    let fill = if throwing { "#333" } else { "none" };
    let stroke = "stroke=\"#333\" stroke-width=\"1.2\"";
    match icon {
        "timer" => format!(
            "<circle cx=\"{}\" cy=\"{}\" r=\"{}\" fill=\"none\" {}/>\
             <path d=\"M{},{} V{} M{},{} H{}\" {} fill=\"none\"/>",
            fmt(cx),
            fmt(cy),
            fmt(r),
            stroke,
            fmt(cx),
            fmt(cy - r * 0.6),
            fmt(cy),
            fmt(cx),
            fmt(cy),
            fmt(cx + r * 0.5),
            stroke
        ),
        "error" => format!(
            "<path d=\"M{},{} L{},{} L{},{} L{},{} L{},{} L{},{} Z\" fill=\"{}\" {}/>",
            fmt(cx - r * 0.8),
            fmt(cy + r * 0.7),
            fmt(cx - r * 0.1),
            fmt(cy - r * 0.2),
            fmt(cx + r * 0.1),
            fmt(cy + r * 0.2),
            fmt(cx + r * 0.8),
            fmt(cy - r * 0.7),
            fmt(cx + r * 0.1),
            fmt(cy + r * 0.7),
            fmt(cx - r * 0.1),
            fmt(cy - r * 0.2),
            fill,
            stroke
        ),
        "escalation" => format!(
            "<path d=\"M{},{} L{},{} L{},{} Z\" fill=\"{}\" {}/>",
            fmt(cx),
            fmt(cy - r * 0.8),
            fmt(cx + r * 0.6),
            fmt(cy + r * 0.8),
            fmt(cx - r * 0.6),
            fmt(cy + r * 0.8),
            fill,
            stroke
        ),
        "signal" => format!(
            "<path d=\"M{},{} L{},{} L{},{} Z\" fill=\"{}\" {}/>",
            fmt(cx),
            fmt(cy - r * 0.8),
            fmt(cx + r * 0.8),
            fmt(cy + r * 0.6),
            fmt(cx - r * 0.8),
            fmt(cy + r * 0.6),
            fill,
            stroke
        ),
        "terminate" => format!(
            "<circle cx=\"{}\" cy=\"{}\" r=\"{}\" fill=\"#333\"/>",
            fmt(cx),
            fmt(cy),
            fmt(r * 0.85)
        ),
        "cancel" => format!(
            "<path d=\"M{},{} L{},{} M{},{} L{},{}\" stroke=\"#333\" stroke-width=\"2\" \
             fill=\"none\"/>",
            fmt(cx - r * 0.7),
            fmt(cy - r * 0.7),
            fmt(cx + r * 0.7),
            fmt(cy + r * 0.7),
            fmt(cx - r * 0.7),
            fmt(cy + r * 0.7),
            fmt(cx + r * 0.7),
            fmt(cy - r * 0.7)
        ),
        "compensate" => format!(
            "<path d=\"M{},{} L{},{} L{},{} Z M{},{} L{},{} L{},{} Z\" fill=\"{}\" {}/>",
            fmt(cx),
            fmt(cy - r * 0.7),
            fmt(cx),
            fmt(cy + r * 0.7),
            fmt(cx - r * 0.8),
            fmt(cy),
            fmt(cx + r * 0.8),
            fmt(cy - r * 0.7),
            fmt(cx + r * 0.8),
            fmt(cy + r * 0.7),
            fmt(cx),
            fmt(cy),
            fill,
            stroke
        ),
        "link" => format!(
            "<path d=\"M{},{} H{} L{},{} L{},{} H{} Z\" fill=\"{}\" {}/>",
            fmt(cx - r * 0.8),
            fmt(cy - r * 0.3),
            fmt(cx + r * 0.2),
            fmt(cx + r * 0.2),
            fmt(cy - r * 0.7),
            fmt(cx + r * 0.8),
            fmt(cy),
            fmt(cx - r * 0.8),
            fill,
            stroke
        ),
        // message (the default)
        _ => format!(
            "<rect x=\"{}\" y=\"{}\" width=\"{}\" height=\"{}\" fill=\"{}\" {}/>\
             <path d=\"M{},{} L{},{} L{},{}\" fill=\"none\" {}/>",
            fmt(cx - r * 0.85),
            fmt(cy - r * 0.6),
            fmt(r * 1.7),
            fmt(r * 1.2),
            fill,
            stroke,
            fmt(cx - r * 0.85),
            fmt(cy - r * 0.6),
            fmt(cx),
            fmt(cy + r * 0.05),
            fmt(cx + r * 0.85),
            fmt(cy - r * 0.6),
            stroke
        ),
    }
}

/// The task-type glyph in an activity's top-left corner (BPMN convention). Drawn in a 14×14 box.
fn task_icon_svg(icon: &str, x: f64, y: f64) -> String {
    let s = "stroke=\"#333\" stroke-width=\"1.1\" fill=\"none\"";
    let (cx, cy) = (x + 7.0, y + 7.0);
    match icon {
        "service" => format!(
            "<circle cx=\"{}\" cy=\"{}\" r=\"4.2\" {}/>\
             <circle cx=\"{}\" cy=\"{}\" r=\"1.6\" {}/>",
            fmt(cx),
            fmt(cy),
            s,
            fmt(cx),
            fmt(cy),
            s
        ),
        "script" => format!(
            "<rect x=\"{}\" y=\"{}\" width=\"9\" height=\"11\" rx=\"1\" {}/>\
             <path d=\"M{},{} H{} M{},{} H{} M{},{} H{}\" {}/>",
            fmt(x + 2.5),
            fmt(y + 1.5),
            s,
            fmt(x + 4.5),
            fmt(y + 4.5),
            fmt(x + 9.0),
            fmt(x + 4.5),
            fmt(y + 7.0),
            fmt(x + 9.0),
            fmt(x + 4.5),
            fmt(y + 9.5),
            fmt(x + 8.0),
            s
        ),
        "send" => format!(
            "<rect x=\"{}\" y=\"{}\" width=\"11\" height=\"8\" {} fill=\"#333\"/>",
            fmt(x + 1.5),
            fmt(y + 3.0),
            s
        ),
        "user" => format!(
            "<circle cx=\"{}\" cy=\"{}\" r=\"2.6\" {}/>\
             <path d=\"M{},{} a4.5,4.5 0 0 1 9,0\" {}/>",
            fmt(cx),
            fmt(y + 4.5),
            s,
            fmt(cx - 4.5),
            fmt(y + 12.5),
            s
        ),
        "rule" => format!(
            "<rect x=\"{}\" y=\"{}\" width=\"11\" height=\"9\" {}/>\
             <path d=\"M{},{} H{} M{},{} V{}\" {}/>",
            fmt(x + 1.5),
            fmt(y + 2.5),
            s,
            fmt(x + 1.5),
            fmt(y + 5.5),
            fmt(x + 12.5),
            fmt(x + 5.0),
            fmt(y + 2.5),
            fmt(y + 11.5),
            s
        ),
        // manual
        _ => format!(
            "<path d=\"M{},{} h7 a2,2 0 0 1 0,4 h-7 z\" {}/>",
            fmt(x + 2.0),
            fmt(y + 5.0),
            s
        ),
    }
}

/// Activity markers along the bottom-centre edge (loop / multi-instance / ad-hoc).
fn markers_svg(markers: &[&str], center_x: f64, y: f64) -> String {
    let n = markers.len() as f64;
    let mut sb = String::new();
    for (i, m) in markers.iter().enumerate() {
        let mx = center_x - (n - 1.0) * 6.0 + i as f64 * 12.0;
        sb.push_str(&marker_glyph(m, mx, y));
    }
    sb
}

fn marker_glyph(marker: &str, mx: f64, y: f64) -> String {
    let s = "stroke=\"#333\" stroke-width=\"1.2\" fill=\"none\"";
    match marker {
        "parallel-mi" => format!(
            "<path d=\"M{},{} V{} M{},{} V{} M{},{} V{}\" {}/>",
            fmt(mx - 3.0),
            fmt(y - 4.0),
            fmt(y + 4.0),
            fmt(mx),
            fmt(y - 4.0),
            fmt(y + 4.0),
            fmt(mx + 3.0),
            fmt(y - 4.0),
            fmt(y + 4.0),
            s
        ),
        "sequential-mi" => format!(
            "<path d=\"M{},{} H{} M{},{} H{} M{},{} H{}\" {}/>",
            fmt(mx - 4.0),
            fmt(y - 3.0),
            fmt(mx + 4.0),
            fmt(mx - 4.0),
            fmt(y),
            fmt(mx + 4.0),
            fmt(mx - 4.0),
            fmt(y + 3.0),
            fmt(mx + 4.0),
            s
        ),
        "ad-hoc" => format!(
            "<path d=\"M{},{} q3,-4 5,0 q2,4 5,0\" {}/>",
            fmt(mx - 5.0),
            fmt(y),
            s
        ),
        // loop
        _ => format!(
            "<path d=\"M{},{} a4,4 0 1 1 -3,-3.6\" {}/>",
            fmt(mx + 1.0),
            fmt(y + 3.5),
            s
        ),
    }
}

// ---- formatting ---------------------------------------------------------------------------------

/// Compact, locale-independent, byte-stable number formatting: at most two decimals, trailing
/// zeros trimmed, and never `-0`.
fn fmt(v: f64) -> String {
    let r = (v * 100.0).round() / 100.0;
    let r = if r == 0.0 { 0.0 } else { r };
    let mut s = format!("{r:.2}");
    while s.contains('.') && (s.ends_with('0') || s.ends_with('.')) {
        s.pop();
    }
    s
}

fn escape_text(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_is_compact_and_stable() {
        assert_eq!(fmt(10.0), "10");
        assert_eq!(fmt(10.5), "10.5");
        assert_eq!(fmt(10.125), "10.13");
        assert_eq!(fmt(-0.001), "0", "never emits -0");
    }

    #[test]
    fn orthogonal_path_drops_duplicates_and_collinear_points() {
        let p = orthogonal_path(&[(0.0, 0.0), (0.0, 0.0), (10.0, 0.0), (20.0, 0.0)]);
        assert_eq!(p, vec![(0.0, 0.0), (20.0, 0.0)], "one straight run");
    }

    #[test]
    fn barycenter_pulls_a_node_towards_its_predecessors() {
        let mut rank = vec!["b".to_string(), "a".to_string()];
        let mut neighbours: HashMap<String, Vec<String>> = HashMap::new();
        neighbours.insert("a".into(), vec!["p0".into()]);
        neighbours.insert("b".into(), vec!["p9".into()]);
        let mut pos: HashMap<String, usize> = HashMap::new();
        pos.insert("b".into(), 0);
        pos.insert("a".into(), 1);
        pos.insert("p0".into(), 0);
        pos.insert("p9".into(), 9);
        barycenter_order(&mut rank, &neighbours, &mut pos);
        assert_eq!(rank, vec!["a".to_string(), "b".to_string()]);
    }
}
