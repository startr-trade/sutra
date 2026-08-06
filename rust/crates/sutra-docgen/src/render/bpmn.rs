//! BPMN process pages: a readable summary (process id, tasks, gateways, wait states, events,
//! sequence flows) built from the engine's own [`sutra_bpmn::BpmnModelLoader`] — never raw XML.
//!
//! Sub-process / ad-hoc / event-sub-process containers are flattened recursively so nested
//! tasks and gateways still show up in the top-level tables (tagged with their containing
//! sub-process id); `MultiInstance` / `StandardLoop` wrappers are unwrapped onto the task/event
//! they decorate, annotated as such.

use std::fmt::Write as _;

use anyhow::{Context, Result};
use sutra_bpmn::{BpmnModelLoader, Node, ProcessDefinition};

use crate::util::cell;

/// Render one `.bpmn` file's page body (no header/footer — the caller wraps those).
pub fn render_bpmn_page(bytes: &[u8], rel: &str) -> Result<String> {
    let module = BpmnModelLoader::new()
        .load(bytes)
        .map_err(|e| anyhow::anyhow!("{}", e))
        .with_context(|| format!("parsing BPMN {rel}"))?;

    let mut out = String::new();
    let _ = writeln!(out, "**Target namespace:** `{}`\n", module.target_namespace);
    if let Some(v) = &module.version {
        let _ = writeln!(out, "**Module version:** `{v}`\n");
    }
    if !module.imports.is_empty() {
        out.push_str("**Imports:**\n\n");
        for imp in &module.imports {
            let _ = writeln!(
                out,
                "- `{}` (namespace `{}`, type `{}`)",
                imp.location, imp.namespace, imp.import_type
            );
        }
        out.push('\n');
    }

    for process in module.processes() {
        render_process(&mut out, process);
    }
    Ok(out)
}

fn render_process(out: &mut String, p: &ProcessDefinition) {
    let _ = writeln!(out, "## Process `{}`\n", p.id);
    if let Some(name) = &p.name {
        let _ = writeln!(out, "**Name:** {name}\n");
    }
    let _ = writeln!(
        out,
        "**Executable:** {} · **Module version:** `{}`\n",
        p.is_executable, p.module_version
    );

    let rows = flatten(p.nodes(), None);

    render_task_table(out, &rows);
    render_gateway_table(out, &rows);
    render_wait_state_table(out, &rows);
    render_event_table(out, &rows);
    render_container_table(out, &rows);
    render_flows(out, p);

    if !p.declared_variables.is_empty() {
        out.push_str("### Declared variables\n\n");
        out.push_str("| Name | Type | Schema | Flags |\n|---|---|---|---|\n");
        for v in &p.declared_variables {
            let schema = v.schema.as_deref().unwrap_or("—");
            let mut flags = Vec::new();
            if v.transient {
                flags.push("transient");
            }
            if v.sensitive {
                flags.push("sensitive");
            }
            let flags = if flags.is_empty() {
                "—".to_string()
            } else {
                flags.join(", ")
            };
            let _ = writeln!(out, "| `{}` | {:?} | {schema} | {flags} |", v.name, v.ty);
        }
        out.push('\n');
    }

    if !p.coverage_paths.is_empty() {
        out.push_str("### Coverage paths\n\n");
        out.push_str("| Path id | Flows |\n|---|---|\n");
        for cp in &p.coverage_paths {
            let _ = writeln!(out, "| `{}` | {} |", cp.id, cell(&cp.flows.join(" → ")));
        }
        out.push('\n');
    }
}

// ---- node flattening -------------------------------------------------------------------------

struct Row<'a> {
    node: &'a Node,
    parent: Option<&'a str>,
    annotations: Vec<String>,
}

fn flatten<'a>(nodes: &'a [Node], parent: Option<&'a str>) -> Vec<Row<'a>> {
    let mut out = Vec::new();
    for n in nodes {
        flatten_node(n, parent, Vec::new(), &mut out);
    }
    out
}

fn flatten_node<'a>(
    n: &'a Node,
    parent: Option<&'a str>,
    annotations: Vec<String>,
    out: &mut Vec<Row<'a>>,
) {
    match n {
        Node::SubProcess { id, inner, .. }
        | Node::TransactionSubProcess { id, inner, .. }
        | Node::AdHocSubProcess { id, inner, .. }
        | Node::EventSubProcess { id, inner, .. } => {
            out.push(Row {
                node: n,
                parent,
                annotations,
            });
            let mut nested = flatten(inner.nodes(), Some(id.as_str()));
            out.append(&mut nested);
        }
        Node::MultiInstance {
            inner, sequential, ..
        } => {
            let mut ann = annotations;
            ann.push(format!(
                "multi-instance ({})",
                if *sequential {
                    "sequential"
                } else {
                    "parallel"
                }
            ));
            flatten_node(inner, parent, ann, out);
        }
        Node::StandardLoop { inner, .. } => {
            let mut ann = annotations;
            ann.push("loop".to_string());
            flatten_node(inner, parent, ann, out);
        }
        _ => out.push(Row {
            node: n,
            parent,
            annotations,
        }),
    }
}

#[derive(PartialEq, Eq)]
enum Kind {
    Task,
    Gateway,
    Event,
    Container,
}

fn kind_of(n: &Node) -> Kind {
    match n {
        Node::ServiceTask { .. }
        | Node::DataTask { .. }
        | Node::ScriptTask { .. }
        | Node::ManualTask { .. }
        | Node::SendTask { .. }
        | Node::BusinessRuleTask { .. }
        | Node::UserTask { .. }
        | Node::CallActivity { .. } => Kind::Task,
        Node::ExclusiveGateway { .. }
        | Node::InclusiveGateway { .. }
        | Node::ParallelGateway { .. }
        | Node::ComplexGateway { .. } => Kind::Gateway,
        Node::SubProcess { .. }
        | Node::TransactionSubProcess { .. }
        | Node::AdHocSubProcess { .. }
        | Node::EventSubProcess { .. } => Kind::Container,
        // MultiInstance / StandardLoop never reach here — unwrapped by `flatten_node`.
        _ => Kind::Event,
    }
}

fn name_of(n: &Node) -> Option<&str> {
    match n {
        Node::StartEvent { name, .. }
        | Node::EndEvent { name, .. }
        | Node::TerminateEndEvent { name, .. }
        | Node::ErrorEvent { name, .. }
        | Node::IntermediateThrowEvent { name, .. }
        | Node::LinkCatchEvent { name, .. }
        | Node::MessageCatchEvent { name, .. }
        | Node::TimerCatchEvent { name, .. }
        | Node::BoundaryEvent { name, .. }
        | Node::ServiceTask { name, .. }
        | Node::DataTask { name, .. }
        | Node::ScriptTask { name, .. }
        | Node::ManualTask { name, .. }
        | Node::SendTask { name, .. }
        | Node::BusinessRuleTask { name, .. }
        | Node::UserTask { name, .. }
        | Node::CallActivity { name, .. }
        | Node::SubProcess { name, .. }
        | Node::TransactionSubProcess { name, .. }
        | Node::AdHocSubProcess { name, .. }
        | Node::EventSubProcess { name, .. }
        | Node::CancelEndEvent { name, .. }
        | Node::ExclusiveGateway { name, .. }
        | Node::InclusiveGateway { name, .. }
        | Node::ParallelGateway { name, .. }
        | Node::ComplexGateway { name, .. }
        | Node::MultiInstance { name, .. }
        | Node::StandardLoop { name, .. } => name.as_deref(),
    }
}

/// The documentation word for a timer's scheduling form — `duration` / `date` / `cycle` — so a
/// rendered page says what KIND of schedule a timer is, not just its text.
fn timer_label(timer: &sutra_bpmn::timer::TimerDefinition) -> &'static str {
    match timer {
        sutra_bpmn::timer::TimerDefinition::Duration(_) => "duration",
        sutra_bpmn::timer::TimerDefinition::Date(_) => "date",
        sutra_bpmn::timer::TimerDefinition::Cycle(_) => "cycle",
    }
}

/// `(label, detail)` for a leaf/container node — never raw XML, a hand-picked readable summary
/// of the fields that matter for documentation.
fn label_and_detail(n: &Node) -> (&'static str, String) {
    match n {
        Node::StartEvent {
            channels, timer, ..
        } => (
            "Start event",
            match (channels.is_empty(), timer) {
                // A schedule-triggered start: what fires it is the interesting fact.
                (_, Some(t)) => format!("{}={}", timer_label(t), t.spec_text()),
                (false, None) => format!("channels: {}", channels.join(", ")),
                (true, None) => String::new(),
            },
        ),
        Node::EndEvent { .. } => ("End event", String::new()),
        Node::TerminateEndEvent { .. } => ("Terminate end event", String::new()),
        Node::ErrorEvent { error_code, .. } => (
            "Error end event",
            error_code
                .as_ref()
                .map(|c| format!("errorCode={c}"))
                .unwrap_or_default(),
        ),
        Node::IntermediateThrowEvent {
            kind, reference, ..
        } => (
            "Intermediate throw event",
            format!(
                "{:?}{}",
                kind,
                reference
                    .as_ref()
                    .map(|r| format!(" ref={r}"))
                    .unwrap_or_default()
            ),
        ),
        Node::LinkCatchEvent { link_name, .. } => ("Link catch event", format!("link={link_name}")),
        Node::MessageCatchEvent {
            channels,
            message_ref,
            ..
        } => {
            let mut parts = Vec::new();
            if !channels.is_empty() {
                parts.push(format!("channels: {}", channels.join(", ")));
            }
            if let Some(m) = message_ref {
                parts.push(format!("messageRef={m}"));
            }
            ("Message catch event", parts.join("; "))
        }
        Node::TimerCatchEvent { timer, .. } => (
            "Timer catch event",
            format!("{}={}", timer_label(timer), timer.spec_text()),
        ),
        Node::BoundaryEvent {
            attached_to_ref,
            kind,
            error_code,
            escalation_code,
            interrupting,
            timer,
            ..
        } => {
            let mut d =
                format!("attachedTo={attached_to_ref}, kind={kind:?}, interrupting={interrupting}");
            if let Some(c) = error_code {
                let _ = write!(d, ", errorCode={c}");
            }
            if let Some(c) = escalation_code {
                let _ = write!(d, ", escalationCode={c}");
            }
            if let Some(t) = timer {
                let _ = write!(d, ", {}={}", timer_label(t), t.spec_text());
            }
            ("Boundary event", d)
        }
        Node::ServiceTask {
            implementation,
            params,
            ..
        } => {
            let mut d = format!("implementation={implementation}");
            if !params.is_empty() {
                let names: Vec<&str> = params.iter().map(|p| p.name.as_str()).collect();
                let _ = write!(d, "; params: {}", names.join(", "));
            }
            ("Service task", d)
        }
        Node::DataTask { data_mapping, .. } => (
            "Data task",
            format!(
                "store reads={}, assignments={}, store writes={}",
                data_mapping.store_reads.len(),
                data_mapping.assignments.len(),
                data_mapping.store_writes.len()
            ),
        ),
        Node::ScriptTask { script_file, .. } => ("Script task", format!("script={script_file}")),
        Node::ManualTask { .. } => ("Manual task", String::new()),
        Node::SendTask { .. } => ("Send task", String::new()),
        Node::BusinessRuleTask { decision_file, .. } => {
            ("Business rule task", format!("decision={decision_file}"))
        }
        Node::UserTask { channels, .. } => (
            "User task",
            if channels.is_empty() {
                String::new()
            } else {
                format!("channels: {}", channels.join(", "))
            },
        ),
        Node::CallActivity {
            called_element,
            called_namespace,
            ..
        } => {
            let mut d = format!("calledElement={called_element}");
            if let Some(ns) = called_namespace {
                let _ = write!(d, ", namespace={ns}");
            }
            ("Call activity", d)
        }
        Node::SubProcess { .. } => ("Sub-process", String::new()),
        Node::TransactionSubProcess { .. } => ("Transaction sub-process", String::new()),
        Node::AdHocSubProcess {
            completion_condition,
            parallel,
            ..
        } => {
            let mut d = format!("parallel={parallel}");
            if let Some(c) = completion_condition {
                let _ = write!(d, ", completion={c}");
            }
            ("Ad-hoc sub-process", d)
        }
        Node::EventSubProcess {
            error_code,
            interrupting,
            ..
        } => {
            let mut d = format!("interrupting={interrupting}");
            if let Some(c) = error_code {
                let _ = write!(d, ", errorCode={c}");
            }
            ("Event sub-process", d)
        }
        Node::CancelEndEvent { .. } => ("Cancel end event", String::new()),
        Node::ExclusiveGateway {
            default_flow_id, ..
        } => (
            "Exclusive gateway (XOR)",
            default_flow_id
                .as_ref()
                .map(|f| format!("default={f}"))
                .unwrap_or_default(),
        ),
        Node::InclusiveGateway {
            default_flow_id, ..
        } => (
            "Inclusive gateway (OR)",
            default_flow_id
                .as_ref()
                .map(|f| format!("default={f}"))
                .unwrap_or_default(),
        ),
        Node::ParallelGateway { .. } => ("Parallel gateway (AND)", String::new()),
        Node::ComplexGateway {
            default_flow_id,
            activation_condition,
            ..
        } => {
            let mut parts = Vec::new();
            if let Some(f) = default_flow_id {
                parts.push(format!("default={f}"));
            }
            if let Some(c) = activation_condition {
                parts.push(format!("activation={c}"));
            }
            ("Complex gateway", parts.join(", "))
        }
        Node::MultiInstance { .. } | Node::StandardLoop { .. } => {
            unreachable!("unwrapped before classification")
        }
    }
}

fn render_task_table(out: &mut String, rows: &[Row]) {
    let items: Vec<&Row> = rows
        .iter()
        .filter(|r| kind_of(r.node) == Kind::Task)
        .collect();
    out.push_str("### Tasks\n\n");
    if items.is_empty() {
        out.push_str("_No tasks._\n\n");
        return;
    }
    out.push_str(
        "| Id | Kind | Name | Detail | Container | Modifiers |\n|---|---|---|---|---|---|\n",
    );
    for r in items {
        let (label, detail) = label_and_detail(r.node);
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} | {} |",
            r.node.id(),
            label,
            cell(name_of(r.node).unwrap_or("")),
            cell(&detail),
            r.parent.map(|p| format!("`{p}`")).unwrap_or_default(),
            cell(&r.annotations.join(", "))
        );
    }
    out.push('\n');
}

fn render_gateway_table(out: &mut String, rows: &[Row]) {
    let items: Vec<&Row> = rows
        .iter()
        .filter(|r| kind_of(r.node) == Kind::Gateway)
        .collect();
    out.push_str("### Gateways\n\n");
    if items.is_empty() {
        out.push_str("_No gateways._\n\n");
        return;
    }
    out.push_str("| Id | Kind | Name | Detail | Container |\n|---|---|---|---|---|\n");
    for r in items {
        let (label, detail) = label_and_detail(r.node);
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} |",
            r.node.id(),
            label,
            cell(name_of(r.node).unwrap_or("")),
            cell(&detail),
            r.parent.map(|p| format!("`{p}`")).unwrap_or_default()
        );
    }
    out.push('\n');
}

fn render_wait_state_table(out: &mut String, rows: &[Row]) {
    let items: Vec<&Row> = rows.iter().filter(|r| r.node.is_wait_state()).collect();
    out.push_str("### Wait states\n\n");
    if items.is_empty() {
        out.push_str("_No wait states — this process runs to completion synchronously._\n\n");
        return;
    }
    out.push_str("| Id | Kind | Name | Detail | Container |\n|---|---|---|---|---|\n");
    for r in items {
        let (label, detail) = label_and_detail(r.node);
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} |",
            r.node.id(),
            label,
            cell(name_of(r.node).unwrap_or("")),
            cell(&detail),
            r.parent.map(|p| format!("`{p}`")).unwrap_or_default()
        );
    }
    out.push('\n');
}

fn render_event_table(out: &mut String, rows: &[Row]) {
    let items: Vec<&Row> = rows
        .iter()
        .filter(|r| kind_of(r.node) == Kind::Event)
        .collect();
    out.push_str("### Events\n\n");
    if items.is_empty() {
        out.push_str("_No events._\n\n");
        return;
    }
    out.push_str("| Id | Kind | Name | Detail | Container |\n|---|---|---|---|---|\n");
    for r in items {
        let (label, detail) = label_and_detail(r.node);
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} |",
            r.node.id(),
            label,
            cell(name_of(r.node).unwrap_or("")),
            cell(&detail),
            r.parent.map(|p| format!("`{p}`")).unwrap_or_default()
        );
    }
    out.push('\n');
}

fn render_container_table(out: &mut String, rows: &[Row]) {
    let items: Vec<&Row> = rows
        .iter()
        .filter(|r| kind_of(r.node) == Kind::Container)
        .collect();
    if items.is_empty() {
        return;
    }
    out.push_str("### Sub-processes\n\n");
    out.push_str("| Id | Kind | Name | Detail | Container |\n|---|---|---|---|---|\n");
    for r in items {
        let (label, detail) = label_and_detail(r.node);
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} |",
            r.node.id(),
            label,
            cell(name_of(r.node).unwrap_or("")),
            cell(&detail),
            r.parent.map(|p| format!("`{p}`")).unwrap_or_default()
        );
    }
    out.push('\n');
}

fn render_flows(out: &mut String, p: &ProcessDefinition) {
    let mut flows: Vec<_> = p.flows().iter().collect();
    flows.sort_by(|a, b| {
        (a.source_ref.as_str(), a.id.as_str()).cmp(&(b.source_ref.as_str(), b.id.as_str()))
    });
    out.push_str("### Sequence flows\n\n");
    if flows.is_empty() {
        out.push_str("_No sequence flows._\n\n");
        return;
    }
    out.push_str("| Id | Source → Target | Condition |\n|---|---|---|\n");
    for f in flows {
        let _ = writeln!(
            out,
            "| `{}` | `{}` → `{}` | {} |",
            f.id,
            f.source_ref,
            f.target_ref,
            f.condition
                .as_deref()
                .map(cell)
                .unwrap_or_else(|| "—".to_string())
        );
    }
    out.push('\n');
}
