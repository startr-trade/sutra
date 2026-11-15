//! The builders: a model built by hand equals the one `parse` reads.

use std::fs;
use std::path::PathBuf;

use bpmn_svg::{FlowNode, LoopMarker, Model, NodeKind, Process, SequenceFlow, TaskType};

#[test]
fn builders_give_the_same_model_as_parse() {
    let xml = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tasks.bpmn"),
    )
    .unwrap();
    let parsed = bpmn_svg::parse(&xml).unwrap();

    let task =
        |id: &str, label: &str, t: TaskType| FlowNode::new(id, NodeKind::Task(t)).label(label);
    let mut process = Process::new("tasks")
        .node(FlowNode::new("begin", NodeKind::StartEvent(None)).label("Begin"))
        .node(task("service", "Call service", TaskType::Service))
        .node(task("script", "Run script", TaskType::Script))
        .node(task("manual", "Do by hand", TaskType::Manual))
        .node(FlowNode::new("untyped", NodeKind::Task(TaskType::Plain)).label("Untyped"))
        .node(task("user", "Ask a person", TaskType::User))
        .node(task("rule", "Apply rule", TaskType::BusinessRule))
        .node(FlowNode::new("call", NodeKind::CallActivity).label("Call another"))
        .node(task("looped", "Repeat", TaskType::Manual).markers([LoopMarker::Loop]))
        .node(
            task("parallel-mi", "Fan out", TaskType::Manual)
                .markers([LoopMarker::ParallelMultiInstance]),
        )
        .node(
            task("sequential-mi", "One by one", TaskType::Manual)
                .markers([LoopMarker::SequentialMultiInstance]),
        )
        .node(task("escaped", "A & B <c>", TaskType::Manual))
        .node(FlowNode::new("finish", NodeKind::EndEvent(None)).label("Finish"));
    let chain = [
        "begin",
        "service",
        "script",
        "manual",
        "untyped",
        "user",
        "rule",
        "call",
        "looped",
        "parallel-mi",
        "sequential-mi",
        "escaped",
        "finish",
    ];
    for (i, pair) in chain.windows(2).enumerate() {
        process = process.flow(SequenceFlow::new(format!("f{}", i + 1), pair[0], pair[1]));
    }
    let built = Model::new().process(process);
    assert_eq!(built, parsed);
}
