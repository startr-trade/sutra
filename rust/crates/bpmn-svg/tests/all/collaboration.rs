//! F3 participants and message flows: pools, black-box pools and message-flow routing.

use bpmn_svg::{
    parse, render, FlowNode, MessageFlow, Model, NodeKind, Options, Participant, Process,
    SequenceFlow, TaskType,
};

use super::support::{bpmn, expect_svg, fixtures_dir};

/// The `d` of the path inside the group whose id is `id`.
fn path_of(svg: &str, id: &str) -> Vec<(f64, f64)> {
    let at = svg.find(&format!(" id=\"{id}\"")).unwrap();
    let d = svg[at..].split("<path d=\"").nth(1).unwrap();
    d[..d.find('"').unwrap()]
        .split(' ')
        .map(|p| {
            let (x, y) = p[1..].split_once(',').unwrap();
            (x.parse().unwrap(), y.parse().unwrap())
        })
        .collect()
}

/// The `(y, width, height)` of the outline of the pool whose id is `id`.
fn pool_rect(svg: &str, id: &str) -> (f64, f64, f64) {
    let at = svg.find(&format!(" id=\"{id}\"")).unwrap();
    let rect = format!(" {}", svg[at..].split("<rect ").nth(1).unwrap());
    let attr = |name: &str| -> f64 {
        let v = rect.split(&format!(" {name}=\"")).nth(1).unwrap();
        v[..v.find('"').unwrap()].parse().unwrap()
    };
    (attr("y"), attr("width"), attr("height"))
}

/// The `(x, y, width, height)` of the first shape in the node group whose id is `id`.
fn node_rect(svg: &str, id: &str) -> (f64, f64, f64, f64) {
    let at = svg.find(&format!(" id=\"{id}\"")).unwrap();
    let rect = format!(" {}", svg[at..].split("<rect ").nth(1).unwrap());
    let attr = |name: &str| -> f64 {
        let v = rect.split(&format!(" {name}=\"")).nth(1).unwrap();
        v[..v.find('"').unwrap()].parse().unwrap()
    };
    (attr("x"), attr("y"), attr("width"), attr("height"))
}

fn ided() -> Options {
    Options::new().id_prefix("")
}

#[test]
fn pools_and_black_box_pools() {
    let model = parse(&bpmn("collaboration")).unwrap();
    let svg = render(&model, &ided()).unwrap();
    assert!(svg.contains("<g class=\"bpmn-pool\" id=\"customer\">"));
    assert!(svg.contains("<g class=\"bpmn-pool\" id=\"shop\">"));
    assert!(svg.contains("<g class=\"bpmn-pool bpmn-pool-collapsed\" id=\"bank\">"));
    assert_eq!(svg.matches("<g class=\"bpmn-message-flow\" id=").count(), 3);
    assert!(
        svg.contains("<marker id=\"open-circle\"") && svg.contains("<marker id=\"open-arrow\"")
    );
    assert!(svg.contains(
        "stroke-dasharray=\"6 4\" marker-start=\"url(#open-circle)\" marker-end=\"url(#open-arrow)\""
    ));
    assert!(
        svg.contains("transform=\"rotate(-90 15 "),
        "the name is rotated in the header"
    );

    // Pools in document order, one width, stacked with the process gap; a black box is 60 high.
    let (customer, shop, bank) = (
        pool_rect(&svg, "customer"),
        pool_rect(&svg, "shop"),
        pool_rect(&svg, "bank"),
    );
    assert!(customer.1 == shop.1 && shop.1 == bank.1, "one width");
    assert_eq!(shop.0, customer.0 + customer.2 + 60.0);
    assert_eq!(bank.0, shop.0 + shop.2 + 60.0);
    assert_eq!(bank.2, 60.0);

    // Downward: node bottom, to the middle of the gap above the target's band, across, into its top.
    let order = path_of(&svg, "m-order");
    let (place, take) = (node_rect(&svg, "place"), node_rect(&svg, "take"));
    // The two share a column here, so the route collapses to one vertical line.
    assert_eq!(place.0 + place.2 / 2.0, take.0 + take.2 / 2.0);
    assert_eq!(
        order,
        [
            (place.0 + place.2 / 2.0, place.1 + place.3),
            (take.0 + take.2 / 2.0, take.1)
        ]
    );
    // Upward: node top, to the middle of the gap below the target's band.
    let confirm = path_of(&svg, "m-confirm");
    let (ship, receive) = (node_rect(&svg, "ship"), node_rect(&svg, "receive"));
    assert_eq!(confirm.first().unwrap(), &(ship.0 + ship.2 / 2.0, ship.1));
    let gap = customer.0 + customer.2 + 30.0;
    assert_eq!(confirm[1], (ship.0 + ship.2 / 2.0, gap));
    assert_eq!(confirm[2], (receive.0 + receive.2 / 2.0, gap));
    assert_eq!(
        confirm.last().unwrap(),
        &(receive.0 + receive.2 / 2.0, receive.1 + receive.3)
    );
    // To a pool border: straight down at the source's x, onto the pool's top edge.
    let pay = path_of(&svg, "m-pay");
    let charge = node_rect(&svg, "charge");
    let x = charge.0 + charge.2 / 2.0;
    assert_eq!(pay, [(x, charge.1 + charge.3), (x, bank.0)]);

    assert!(svg.contains(">Order</text>") && svg.contains(">(m-confirm)</text>"));
    expect_svg(
        "collaboration",
        &render(&model, &Options::default()).unwrap(),
    );
}

#[test]
fn message_flow_within_one_pool_routes_below() {
    let model = Model::new()
        .participant(Participant::new("pool").name("Team").process_ref("p"))
        .process(
            Process::new("p")
                .node(FlowNode::new("a", NodeKind::Task(TaskType::Send)))
                .node(FlowNode::new("b", NodeKind::Task(TaskType::Receive)))
                .flow(SequenceFlow::new("f", "a", "b")),
        )
        .message_flow(MessageFlow::new("m", "a", "b"));
    let svg = render(&model, &ided()).unwrap();
    let pool = pool_rect(&svg, "pool");
    let (a, b) = (node_rect(&svg, "a"), node_rect(&svg, "b"));
    let below = pool.0 + pool.2 + 30.0;
    assert_eq!(
        path_of(&svg, "m"),
        [
            (a.0 + a.2 / 2.0, a.1 + a.3),
            (a.0 + a.2 / 2.0, below),
            (b.0 + b.2 / 2.0, below),
            (b.0 + b.2 / 2.0, b.1 + b.3),
        ]
    );
    // The process sits inside the pool, right of the 30-unit header and a 16-unit pad.
    assert_eq!(a.0, 46.0);
    assert_eq!(a.1, pool.0 + 16.0);
}

#[test]
fn processes_without_participants_stack_as_before() {
    let several = parse(&bpmn("several-processes")).unwrap();
    let expected = std::fs::read_to_string(fixtures_dir().join("several-processes.svg")).unwrap();
    assert!(render(&several, &Options::default()).unwrap() == expected);

    // A pool for the second process only: it comes first, then the others in document order with
    // no pool drawn, at the left edge.
    let mut pooled = several.clone();
    let second = pooled.processes[1].id.clone();
    pooled = pooled.participant(Participant::new("only").process_ref(second.clone()));
    let svg = render(&pooled, &ided()).unwrap();
    assert_eq!(svg.matches("<g class=\"bpmn-pool\"").count(), 1);
    let first_node = |svg: &str, process: usize| {
        let id = &pooled.processes[process].nodes[0].id;
        let at = svg.find(&format!(" id=\"{id}\"")).unwrap();
        svg[..at].matches("<g class=\"bpmn-node").count()
    };
    assert!(
        first_node(&svg, 1) < first_node(&svg, 0),
        "the pooled process is drawn first"
    );
    assert!(
        !svg.contains("<text x=\"15\""),
        "a pool with no name draws no name"
    );

    // Message flows alone draw no pools and leave the stacking as it was.
    let start = |p: usize| several.processes[p].nodes[0].id.clone();
    let linked = several
        .clone()
        .message_flow(MessageFlow::new("m", start(0), start(1)));
    let svg = render(&linked, &ided()).unwrap();
    assert!(!svg.contains("bpmn-pool"));
    let plain = render(&several, &ided()).unwrap();
    for p in 0..several.processes.len() {
        let id = start(p);
        let circle = |svg: &str| {
            let at = svg.find(&format!(" id=\"{id}\"")).unwrap();
            let c = svg[at..].split("<circle ").nth(1).unwrap();
            c[..c.find('/').unwrap()].to_string()
        };
        assert_eq!(circle(&svg), circle(&plain), "{id}");
    }
}
