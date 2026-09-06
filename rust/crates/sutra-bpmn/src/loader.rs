//! BPMN reader producing the engine-internal [`ProcessModule`] — the hardened XML load
//! collapsed onto one quick-xml tree; every load-time validation and its `SUTRA.*` diagnostic
//! code string is enforced here.
//!
//! Documented divergences from the reference implementation (message-shape only):
//! - the reference implementation raises a plain uncoded error for unknown enum attribute
//!   values (`ack`, `dataClass`, `onNoMatch`, `onConflict`, `capture`, `auth`) and
//!   for a multi-instance wrapper missing both `loopCardinality`/`loopDataInputRef`; this
//!   loader raises a coded `SUTRA.PARSE.QXSD.INVALID_SOURCE` error with the same message text.

use std::collections::{HashMap, HashSet};

use crate::codes;
use crate::duration::parse_iso8601_duration;
use crate::error::SutraError;
use crate::model::{
    Assignment, BoundaryKind, BpmnImport, CoveragePath, DataMapping, DeclaredVariable, FieldType,
    Node, ParamBinding, ProcessAudit, ProcessDefinition, ProcessModule, SequenceFlow, StoreRead,
    StoreWrite, ThrowKind, CHANNEL_CALL_PREFIX,
};
use crate::qbindings::{
    AckMode, AliasBinding, AliasConflict, AuditBinding, AuditCapture, CaseEntry, DataClass,
    DispatchTable, HeaderAttr, NodeBindings, OnNoMatch, OnValidationBinding, OnValidationMode,
    OutboundAuthScheme, OutputBinding, ReplyBinding, ReplyMode, RetryBinding, SendBinding,
    SimpleValidator, SourceBinding, TimeoutBinding,
};
use crate::xml::{self, XmlElement};

const BPMN_NS: &str = "http://www.omg.org/spec/BPMN/20100524/MODEL";
/// The `q:` namespace for BPMN extension elements (channel bindings, validators, etc.).
const Q_NS: &str = "urn:sutra:q:1.0";

/// A parsed `<bpmn:dataStoreReference>`: the store it names + its `<q:store>` key /
/// forUpdate / field / expect attributes.
#[derive(Debug, Clone)]
struct StoreBinding {
    store_name: Option<String>,
    key: Option<String>,
    for_update: bool,
    field: Option<String>,
    expect_unchanged: bool,
}

/// Document-wide capture maps recovered up front (the loader's DOM-scan phase).
struct DocScan {
    channels_by_event_id: HashMap<String, Vec<String>>,
    script_file_by_task_id: HashMap<String, String>,
    bindings_by_node_id: HashMap<String, NodeBindings>,
    store_bindings_by_ref_id: HashMap<String, StoreBinding>,
    params_by_service_task_id: HashMap<String, Vec<ParamBinding>>,
    version_by_process_id: HashMap<String, String>,
    variables_by_process_id: HashMap<String, Vec<DeclaredVariable>>,
    coverage_paths_by_process_id: HashMap<String, Vec<CoveragePath>>,
    /// B1 — process-level `<q:audit sink capture>` policy (single sink + capture level).
    audit_by_process_id: HashMap<String, ProcessAudit>,
    /// Process-level `<q:process idempotent>` assertion (absent ⇒ fail-closed `false`).
    idempotent_by_process_id: HashMap<String, bool>,
    error_code_by_id: HashMap<String, String>,
    escalation_code_by_id: HashMap<String, String>,
    signal_name_by_id: HashMap<String, String>,
}

/// The BPMN model loader. Stateless; construct once and [`Self::load`] any number of files.
#[derive(Debug, Clone, Copy, Default)]
pub struct BpmnModelLoader;

impl BpmnModelLoader {
    pub fn new() -> Self {
        BpmnModelLoader
    }

    pub fn load(&self, bpmn_bytes: &[u8]) -> Result<ProcessModule, SutraError> {
        if bpmn_bytes.is_empty() {
            return Err(err(codes::PARSE_BPMN_MISSING_PROCESS, "BPMN body is empty"));
        }
        let root = xml::parse(bpmn_bytes).map_err(|e| {
            err(
                codes::PARSE_BPMN_MISSING_PROCESS,
                format!("BPMN parse failed: {e}"),
            )
        })?;
        if !root.is(BPMN_NS, "definitions") {
            return Err(err(
                codes::PARSE_BPMN_MISSING_PROCESS,
                format!(
                    "Expected <bpmn:definitions> as document element, got {}",
                    root.local
                ),
            ));
        }

        let scan = self.scan_document(&root)?;
        validate_variable_sources(&root)?;

        let target_namespace = root.attr_or_empty("targetNamespace").to_string();
        let mut imports = Vec::new();
        for imp in root.children_ns(BPMN_NS, "import") {
            imports.push(BpmnImport {
                import_type: imp.attr_or_empty("importType").to_string(),
                namespace: imp.attr_or_empty("namespace").to_string(),
                location: imp.attr_or_empty("location").to_string(),
            });
        }

        let mut processes = Vec::new();
        for p in root.children_ns(BPMN_NS, "process") {
            processes.push(self.build_process(p, &scan)?);
        }
        if processes.is_empty() {
            return Err(err(
                codes::PARSE_BPMN_MISSING_PROCESS,
                "<bpmn:definitions> has no <bpmn:process>",
            ));
        }
        ProcessModule::of(target_namespace, imports, processes)
    }

    // ---- document-wide capture (the DOM-scan phase) -------------------------

    fn scan_document(&self, root: &XmlElement) -> Result<DocScan, SutraError> {
        // <bpmn:error>/<bpmn:escalation>/<bpmn:signal> root elements, indexed for
        // QName-based errorRef/escalationRef/signalRef resolution.
        let mut error_code_by_id = HashMap::new();
        let mut escalation_code_by_id = HashMap::new();
        let mut signal_name_by_id = HashMap::new();
        for c in &root.children {
            if c.is(BPMN_NS, "error") && !c.attr_or_empty("id").is_empty() {
                let mut code = c.attr_or_empty("errorCode");
                if code.trim().is_empty() {
                    code = c.attr_or_empty("name");
                }
                error_code_by_id.insert(c.attr_or_empty("id").to_string(), code.to_string());
            } else if c.is(BPMN_NS, "escalation") && !c.attr_or_empty("id").is_empty() {
                let mut code = c.attr_or_empty("escalationCode");
                if code.trim().is_empty() {
                    code = c.attr_or_empty("name");
                }
                escalation_code_by_id.insert(c.attr_or_empty("id").to_string(), code.to_string());
            } else if c.is(BPMN_NS, "signal") && !c.attr_or_empty("id").is_empty() {
                signal_name_by_id.insert(
                    c.attr_or_empty("id").to_string(),
                    c.attr_or_empty("name").to_string(),
                );
            }
        }

        let mut channels_by_event_id = HashMap::new();
        for local in ["startEvent", "intermediateCatchEvent", "userTask"] {
            for event in root.collect_descendants_ns(BPMN_NS, local) {
                let id = event.attr_or_empty("id");
                if id.trim().is_empty() {
                    continue;
                }
                for ext in event.children_ns(BPMN_NS, "extensionElements") {
                    let mut channels = Vec::new();
                    for source in ext.collect_descendants_ns(Q_NS, "source") {
                        let channel = source.attr_or_empty("channel");
                        if !channel.trim().is_empty() {
                            channels.push(channel.to_string());
                        }
                    }
                    if !channels.is_empty() {
                        channels_by_event_id.insert(id.to_string(), channels);
                    }
                }
            }
        }

        let mut script_file_by_task_id = HashMap::new();
        for task in root.collect_descendants_ns(BPMN_NS, "scriptTask") {
            let id = task.attr_or_empty("id");
            if id.trim().is_empty() {
                continue;
            }
            for script in task.children_ns(BPMN_NS, "script") {
                let text = script.trimmed_text();
                if !text.is_empty() {
                    script_file_by_task_id.insert(id.to_string(), text.to_string());
                }
            }
        }

        let mut bindings_by_node_id = HashMap::new();
        collect_node_bindings(root, &mut bindings_by_node_id)?;
        apply_process_level_contracts(root, &mut bindings_by_node_id)?;

        let store_bindings_by_ref_id = collect_store_bindings(root);

        let mut params_by_service_task_id = HashMap::new();
        for task in root.collect_descendants_ns(BPMN_NS, "serviceTask") {
            let id = task.attr_or_empty("id");
            if id.trim().is_empty() {
                continue;
            }
            for ext in task.children_ns(BPMN_NS, "extensionElements") {
                let mut params = Vec::new();
                for p in ext.collect_descendants_ns(Q_NS, "param") {
                    let name = p.attr_or_empty("name");
                    let expression = p.attr_or_empty("expression");
                    if !name.trim().is_empty() && !expression.trim().is_empty() {
                        params.push(ParamBinding {
                            name: name.to_string(),
                            expression: expression.to_string(),
                        });
                    }
                }
                if !params.is_empty() {
                    params_by_service_task_id.insert(id.to_string(), params);
                }
            }
        }

        let mut version_by_process_id = HashMap::new();
        let mut variables_by_process_id = HashMap::new();
        let mut coverage_paths_by_process_id = HashMap::new();
        let mut audit_by_process_id: HashMap<String, ProcessAudit> = HashMap::new();
        let mut idempotent_by_process_id: HashMap<String, bool> = HashMap::new();
        for process in root.collect_descendants_ns(BPMN_NS, "process") {
            let id = process.attr_or_empty("id");
            if id.trim().is_empty() {
                continue;
            }
            // Step 1: <bpmn:process versionTag="..."/> takes precedence for the version pin.
            let version_tag = process.attr_or_empty("versionTag");
            if !version_tag.trim().is_empty() {
                version_by_process_id.insert(id.to_string(), version_tag.to_string());
            }
            // The PROCESS-LEVEL <q:audit> (a direct child of the process extensionElements — NOT a
            // node's, which is why we don't recurse) carries both the module-version pin (`version`)
            // and, B1, the audit policy (`sink` + `capture`). A `<q:audit>` that names a sink or a
            // capture declares the process's single audit sink; `version`-only is just the pin.
            'exts: for ext in process.children_ns(BPMN_NS, "extensionElements") {
                for audit in ext.children_ns(Q_NS, "audit") {
                    if version_tag.trim().is_empty() {
                        let version = audit.attr_or_empty("version");
                        if !version.trim().is_empty() {
                            version_by_process_id
                                .entry(id.to_string())
                                .or_insert_with(|| version.to_string());
                        }
                    }
                    let sink_attr = audit.attr_or_empty("sink");
                    let capture_attr = audit.attr_or_empty("capture");
                    if !sink_attr.trim().is_empty() || !capture_attr.trim().is_empty() {
                        // Process-level capture defaults to METADATA (light/safe); `payload` is
                        // opt-in. Sink defaults to the XSD `"sql"`.
                        let capture = if capture_attr.trim().is_empty() {
                            AuditCapture::Metadata
                        } else {
                            parse_audit_capture(capture_attr)?
                        };
                        let sink = if sink_attr.trim().is_empty() {
                            "sql".to_string()
                        } else {
                            sink_attr.to_string()
                        };
                        audit_by_process_id.insert(id.to_string(), ProcessAudit { sink, capture });
                        break 'exts;
                    }
                }
            }

            // The PROCESS-LEVEL <q:process idempotent="true|false"> retry-safety assertion
            // (a direct child of the process extensionElements). Absent ⇒ fail-closed `false` (an
            // undeclared process is treated as non-idempotent). Distinct from a dedup key.
            'idem: for ext in process.children_ns(BPMN_NS, "extensionElements") {
                for qp in ext.children_ns(Q_NS, "process") {
                    let raw = qp.attr_or_empty("idempotent");
                    if !raw.trim().is_empty() {
                        idempotent_by_process_id
                            .insert(id.to_string(), raw.trim().eq_ignore_ascii_case("true"));
                        break 'idem;
                    }
                }
            }

            let mut decls: Vec<DeclaredVariable> = Vec::new();
            for v in process.collect_descendants_ns(Q_NS, "variable") {
                let name = v.attr_or_empty("name");
                if !name.trim().is_empty() {
                    let ft = scalar_type(v.attr_or_empty("type"));
                    let schema = opt_attr(v, "schema")
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty());
                    let source = opt_attr(v, "source")
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty());
                    let transient = v.attr_or_empty("transient").eq_ignore_ascii_case("true");
                    let sensitive = v.attr_or_empty("sensitive").eq_ignore_ascii_case("true");
                    let subject_key = v.attr_or_empty("subjectKey").eq_ignore_ascii_case("true");
                    match decls.iter_mut().find(|d| d.name == name) {
                        Some(slot) => {
                            slot.ty = ft;
                            if slot.schema.is_none() {
                                slot.schema = schema;
                            }
                            if slot.source.is_none() {
                                slot.source = source;
                            }
                            slot.transient |= transient;
                            slot.sensitive |= sensitive;
                            slot.subject_key |= subject_key;
                        }
                        None => decls.push(DeclaredVariable {
                            name: name.to_string(),
                            ty: ft,
                            schema,
                            source,
                            transient,
                            sensitive,
                            subject_key,
                        }),
                    }
                }
            }
            if !decls.is_empty() {
                variables_by_process_id.insert(id.to_string(), decls);
            }

            let mut paths = Vec::new();
            for c in process.collect_descendants_ns(Q_NS, "coverage") {
                let path_id = c.attr_or_empty("path").trim();
                if path_id.is_empty() {
                    continue;
                }
                let flows: Vec<String> = c
                    .attr_or_empty("flows")
                    .split_whitespace()
                    .map(|s| s.to_string())
                    .collect();
                paths.push(CoveragePath {
                    id: path_id.to_string(),
                    flows,
                });
            }
            if !paths.is_empty() {
                coverage_paths_by_process_id.insert(id.to_string(), paths);
            }
        }

        Ok(DocScan {
            channels_by_event_id,
            script_file_by_task_id,
            bindings_by_node_id,
            store_bindings_by_ref_id,
            params_by_service_task_id,
            version_by_process_id,
            variables_by_process_id,
            coverage_paths_by_process_id,
            audit_by_process_id,
            idempotent_by_process_id,
            error_code_by_id,
            escalation_code_by_id,
            signal_name_by_id,
        })
    }

    // ---- tree → ProcessModule mapping ---------------------------------------

    fn build_process(
        &self,
        p: &XmlElement,
        scan: &DocScan,
    ) -> Result<ProcessDefinition, SutraError> {
        let id = required(p.attr_or_empty("id"), "process", "id")?;
        let name = non_blank(p.attr_or_empty("name"));
        // isExecutable defaults to true when absent (per the BPMN spec).
        let is_executable = p.attr_or_empty("isExecutable") != "false";
        let module_version = scan
            .version_by_process_id
            .get(&id)
            .cloned()
            .unwrap_or_else(|| "1.0".to_string());

        let process = self.assemble_process(
            &id,
            name,
            is_executable,
            &module_version,
            p,
            scan,
            &HashMap::new(),
        )?;
        // Path-coverage is a TOP-LEVEL process property — validate against this process's OWN
        // flow set (a callActivity/subProcess's inner flows are a separate process's coverage).
        let paths = scan
            .coverage_paths_by_process_id
            .get(&id)
            .cloned()
            .unwrap_or_default();
        let paths = validate_coverage_paths(&id, paths, process.flows())?;
        let process = if paths.is_empty() {
            process
        } else {
            process.with_coverage_paths(paths)
        };
        // B1 — the process-level `<q:audit>` policy (single sink + capture); `None` leaves the
        // process unaudited pending the deployment-manifest default (applied at resolution).
        let audit = scan.audit_by_process_id.get(&id).cloned();
        // The process-level idempotency assertion (`<q:process idempotent>`); absent ⇒ false.
        let idempotent = scan
            .idempotent_by_process_id
            .get(&id)
            .copied()
            .unwrap_or(false);
        Ok(process.with_audit(audit).with_idempotent(idempotent))
    }

    /// Assemble a [`ProcessDefinition`] from a container's flow elements — shared by a
    /// top-level `<bpmn:process>` and an embedded sub-process (recursion, any depth).
    fn assemble_process(
        &self,
        id: &str,
        name: Option<String>,
        is_executable: bool,
        module_version: &str,
        container: &XmlElement,
        scan: &DocScan,
        // Data objects visible from the ENCLOSING container. BPMN scopes a `<dataObject>` to
        // its container and to everything nested inside it, so a sub-process sees its parent's.
        inherited_data: &HashMap<String, String>,
    ) -> Result<ProcessDefinition, SutraError> {
        let mut nodes: Vec<Node> = Vec::new();
        let mut flows: Vec<SequenceFlow> = Vec::new();
        let mut activity_ids: HashSet<String> = HashSet::new();
        let mut boundaries_pending: Vec<&XmlElement> = Vec::new();

        // Index the data objects VISIBLE HERE by id → variable name: the enclosing container's
        // first, this container's own overlaid on top so a nested redeclaration shadows rather
        // than collides. Without the inherited half a sub-process could not resolve a
        // process-level `<dataObject>`, and `resolve_var` would fall back to the raw ELEMENT ID —
        // a name no variable answers to — so a store write silently wrote null.
        let mut var_name_by_data_id = inherited_data.clone();
        var_name_by_data_id.extend(index_data_elements(container));

        for fe in &container.children {
            if fe.ns.as_deref() != Some(BPMN_NS) {
                continue;
            }
            match fe.local.as_str() {
                "startEvent" => {
                    let start_id = required(fe.attr_or_empty("id"), "startEvent", "id")?;
                    let channels = scan
                        .channels_by_event_id
                        .get(&start_id)
                        .cloned()
                        .unwrap_or_default();
                    // A start event carries exactly ONE trigger contract. A channel start is
                    // driven by an inbound message and projects its payload into variables; a
                    // timer start is driven by a durable schedule and carries NO payload. A
                    // model declaring both cannot be honoured either way, so fail closed and
                    // make the author pick.
                    let timer = build_start_timer(fe, &start_id)?;
                    if timer.is_some() && !channels.is_empty() {
                        return Err(err(
                            codes::CONFIG_BPMN_TIMER_START_SOURCE_CONFLICT,
                            format!(
                                "<startEvent> {start_id} declares BOTH a <q:source> (channels: \
                                 {}) and a <timerEventDefinition>. A start event has one trigger: \
                                 a channel start carries an inbound payload, a timer start carries \
                                 none. Split them into two start events, or drop one.",
                                channels.join(", ")
                            ),
                        ));
                    }
                    nodes.push(Node::StartEvent {
                        id: start_id,
                        name: non_blank(fe.attr_or_empty("name")),
                        channels,
                        timer,
                    });
                }
                "endEvent" => nodes.push(build_end_or_error_event(fe, &scan.error_code_by_id)?),
                "intermediateThrowEvent" => nodes.push(build_intermediate_throw_event(
                    fe,
                    &scan.escalation_code_by_id,
                    &scan.signal_name_by_id,
                )?),
                "intermediateCatchEvent" => {
                    if has_event_definition(fe, "linkEventDefinition") {
                        nodes.push(build_link_catch_event(fe)?);
                    } else if has_event_definition(fe, "timerEventDefinition") {
                        nodes.push(build_timer_catch_event(fe)?);
                    } else {
                        nodes.push(build_message_catch_event(fe, &scan.channels_by_event_id)?);
                    }
                }
                "boundaryEvent" => boundaries_pending.push(fe),
                "serviceTask" => {
                    let node = build_service_task(fe, &var_name_by_data_id, scan)?;
                    nodes.push(wrap_maybe_loop(fe, node)?);
                    activity_ids.insert(fe.attr_or_empty("id").to_string());
                }
                "scriptTask" => {
                    reject_data_associations(fe, "scriptTask")?;
                    let node = build_script_task(fe, scan)?;
                    nodes.push(wrap_maybe_loop(fe, node)?);
                    activity_ids.insert(fe.attr_or_empty("id").to_string());
                }
                "manualTask" => {
                    let mt_id = required(fe.attr_or_empty("id"), "manualTask", "id")?;
                    nodes.push(wrap_maybe_loop(
                        fe,
                        Node::ManualTask {
                            id: mt_id,
                            name: non_blank(fe.attr_or_empty("name")),
                        },
                    )?);
                    activity_ids.insert(fe.attr_or_empty("id").to_string());
                }
                "sendTask" => {
                    reject_data_associations(fe, "sendTask")?;
                    let snd_id = required(fe.attr_or_empty("id"), "sendTask", "id")?;
                    nodes.push(wrap_maybe_loop(
                        fe,
                        Node::SendTask {
                            id: snd_id,
                            name: non_blank(fe.attr_or_empty("name")),
                        },
                    )?);
                    activity_ids.insert(fe.attr_or_empty("id").to_string());
                }
                "businessRuleTask" => {
                    let brt_id = required(fe.attr_or_empty("id"), "businessRuleTask", "id")?;
                    reject_data_associations(fe, "businessRuleTask")?;
                    let decision_file = fe.attr_or_empty("implementation");
                    if decision_file.trim().is_empty() {
                        return Err(err(
                            codes::RESOLVE_TASK_UNKNOWN,
                            format!(
                                "<businessRuleTask> {brt_id} in process '{id}' has no implementation \
                                 naming a decision file in the module's rules/ folder."
                            ),
                        ));
                    }
                    nodes.push(wrap_maybe_loop(
                        fe,
                        Node::BusinessRuleTask {
                            id: brt_id.clone(),
                            name: non_blank(fe.attr_or_empty("name")),
                            decision_file: decision_file.to_string(),
                        },
                    )?);
                    activity_ids.insert(brt_id);
                }
                "userTask" => {
                    let ut_id = required(fe.attr_or_empty("id"), "userTask", "id")?;
                    let channels = scan
                        .channels_by_event_id
                        .get(&ut_id)
                        .cloned()
                        .unwrap_or_default();
                    nodes.push(Node::UserTask {
                        id: ut_id.clone(),
                        name: non_blank(fe.attr_or_empty("name")),
                        channels,
                    });
                    activity_ids.insert(ut_id);
                }
                "exclusiveGateway" => nodes.push(Node::ExclusiveGateway {
                    id: required(fe.attr_or_empty("id"), "exclusiveGateway", "id")?,
                    name: non_blank(fe.attr_or_empty("name")),
                    default_flow_id: non_blank(fe.attr_or_empty("default")),
                }),
                "inclusiveGateway" => nodes.push(Node::InclusiveGateway {
                    id: required(fe.attr_or_empty("id"), "inclusiveGateway", "id")?,
                    name: non_blank(fe.attr_or_empty("name")),
                    default_flow_id: non_blank(fe.attr_or_empty("default")),
                }),
                "parallelGateway" => nodes.push(Node::ParallelGateway {
                    id: required(fe.attr_or_empty("id"), "parallelGateway", "id")?,
                    name: non_blank(fe.attr_or_empty("name")),
                }),
                "complexGateway" => nodes.push(Node::ComplexGateway {
                    id: required(fe.attr_or_empty("id"), "complexGateway", "id")?,
                    name: non_blank(fe.attr_or_empty("name")),
                    default_flow_id: non_blank(fe.attr_or_empty("default")),
                    activation_condition: fe
                        .child_ns(BPMN_NS, "activationCondition")
                        .map(|c| c.trimmed_text().to_string())
                        .filter(|t| !t.is_empty()),
                }),
                "callActivity" => {
                    reject_data_associations(fe, "callActivity")?;
                    let node = build_call_activity(fe)?;
                    nodes.push(wrap_maybe_loop(fe, node)?);
                    activity_ids.insert(fe.attr_or_empty("id").to_string());
                }
                "subProcess" | "transaction" | "adHocSubProcess" => {
                    let node = self.build_sub_process(fe, id, scan, &var_name_by_data_id)?;
                    nodes.push(wrap_maybe_loop(fe, node)?);
                    activity_ids.insert(fe.attr_or_empty("id").to_string());
                }
                "sequenceFlow" => flows.push(build_sequence_flow(fe)?),
                // Non-flow-node flow elements do not participate in token flow — ignored.
                "dataObject" | "dataObjectReference" | "dataStoreReference" | "dataStore" => {}
                // Fail closed on a flow NODE the engine does not execute.
                "task" | "receiveTask" | "eventBasedGateway" | "implicitThrowEvent" | "event" => {
                    return Err(err(
                        codes::CONFIG_BPMN_UNSUPPORTED_ELEMENT,
                        format!(
                            "<bpmn:{}> '{}' in process '{}' is not a supported flow node. Supported: \
                             startEvent, endEvent (plain, terminate, error, cancel), \
                             intermediateCatchEvent (message, timer, link), \
                             intermediateThrowEvent (compensate, message, signal, escalation, link), \
                             boundaryEvent, serviceTask, scriptTask, sendTask, businessRuleTask, \
                             manualTask, userTask, exclusiveGateway, inclusiveGateway, \
                             parallelGateway, complexGateway, callActivity, subProcess (embedded, \
                             transaction, adHoc, error-triggered event), sequenceFlow. Full matrix: \
                             crates/sutra-bpmn/bpmn-support.md.",
                            fe.local,
                            fe.attr_or_empty("id"),
                            id
                        ),
                    ));
                }
                // Non-flow-element process children (extensionElements, laneSet, artifacts,
                // documentation, …) are inert.
                _ => {}
            }
        }

        for be in boundaries_pending {
            nodes.push(build_boundary_event(
                be,
                id,
                &activity_ids,
                &scan.error_code_by_id,
                &scan.escalation_code_by_id,
            )?);
        }

        // A `<q:timeout duration>` on a channel-call task is the
        // attribute form of an interrupting timer boundary: synthesize the boundary node
        // (`<taskId>#timeout`, no outgoing flows — its fire raises the timeout error) so
        // the executor sees exactly ONE timer-boundary shape.
        synthesize_timeout_boundaries(&mut nodes, &scan.bindings_by_node_id)?;

        // Project the document-wide bindings map down to only this process's nodes.
        let mut process_bindings = HashMap::new();
        for n in &nodes {
            if let Some(b) = scan.bindings_by_node_id.get(n.id()) {
                if !b.is_empty() {
                    process_bindings.insert(n.id().to_string(), b.clone());
                }
            }
        }

        validate_throw_targets(id, &nodes, &process_bindings)?;
        validate_channel_calls_and_timers(id, &nodes, &flows, &process_bindings)?;

        ProcessDefinition::of(
            id,
            name,
            is_executable,
            module_version,
            nodes,
            flows,
            process_bindings,
            scan.variables_by_process_id
                .get(id)
                .cloned()
                .unwrap_or_default(),
        )
    }

    /// Build an embedded / transaction / ad-hoc / (error-triggered) event sub-process
    /// by recursively assembling its child flow elements.
    fn build_sub_process(
        &self,
        sp: &XmlElement,
        parent_id: &str,
        scan: &DocScan,
        inherited_data: &HashMap<String, String>,
    ) -> Result<Node, SutraError> {
        let sp_id = required(sp.attr_or_empty("id"), "subProcess", "id")?;
        let is_transaction = sp.local == "transaction";
        let is_ad_hoc = sp.local == "adHocSubProcess";
        let is_event_sub = !is_ad_hoc && sp.attr_or_empty("triggeredByEvent") == "true";
        let event_start = if is_event_sub {
            sp.children_ns(BPMN_NS, "startEvent").next()
        } else {
            None
        };
        if is_event_sub
            && !event_start
                .map(|s| has_event_definition(s, "errorEventDefinition"))
                .unwrap_or(false)
        {
            return Err(err(
                codes::PARSE_SUBPROCESS_UNSUPPORTED,
                format!(
                    "<subProcess> '{sp_id}' in process '{parent_id}' is an event sub-process whose \
                     start event is not error-triggered; only an error-triggered event sub-process \
                     is supported (Track H). Its non-error triggers (message/timer/signal) are wait \
                     states — model them on the stateful surface, or expand the flow inline."
                ),
            ));
        }
        let inner = self.assemble_process(
            &sp_id,
            non_blank(sp.attr_or_empty("name")),
            true,
            "1.0",
            sp,
            scan,
            inherited_data,
        )?;
        // Channel-call tasks and timer catch events park DURABLE wait states; the
        // inline sub-process runners cannot park mid-scope, so fail closed at load.
        for n in inner.nodes() {
            let unsupported = match n {
                Node::TimerCatchEvent { .. } => true,
                Node::ServiceTask { implementation, .. } => {
                    implementation.starts_with(CHANNEL_CALL_PREFIX)
                }
                _ => false,
            };
            if unsupported {
                return Err(err(
                    codes::DISPATCH_TIMER_UNSUPPORTED,
                    format!(
                        "<{}> '{sp_id}' in process '{parent_id}' contains '{}' — a \
                         channel-call task / timer catch event inside an embedded, \
                         transaction, ad-hoc or event sub-process is not supported: the \
                         inline sub-process runners cannot park a durable wait state \
                         mid-scope. Model it at the top level of the process.",
                        sp.local,
                        n.id()
                    ),
                ));
            }
            // Same inline-runner limitation, stated in the retry policy's own vocabulary: a
            // `<q:retry>` park is a durable timer, and the sub-process runners discard their
            // sub-state's wait frontier.
            if inner.bindings_for(n.id()).retry.is_some() {
                return Err(err(
                    codes::CONFIG_BPMN_RETRY_NOT_APPLICABLE,
                    format!(
                        "<{}> '{sp_id}' in process '{parent_id}' contains '{}', which declares \
                         <q:retry>; a retry parks a durable TIMER wait and the inline \
                         sub-process runners cannot park mid-scope. Model the retried task at \
                         the top level of the process.",
                        sp.local,
                        n.id()
                    ),
                ));
            }
        }
        if is_event_sub {
            let start = event_start.expect("event sub-process start checked above");
            let error_code = extract_error_code(start, &scan.error_code_by_id);
            let interrupting = start.attr_or_empty("isInterrupting") != "false";
            return Ok(Node::EventSubProcess {
                id: sp_id,
                name: non_blank(sp.attr_or_empty("name")),
                inner: Box::new(inner),
                error_code,
                interrupting,
            });
        }
        if is_ad_hoc {
            let completion_condition = sp
                .child_ns(BPMN_NS, "completionCondition")
                .map(|c| c.trimmed_text().to_string())
                .filter(|t| !t.is_empty());
            let parallel = sp.attr_or_empty("ordering") == "Parallel";
            return Ok(Node::AdHocSubProcess {
                id: sp_id,
                name: non_blank(sp.attr_or_empty("name")),
                inner: Box::new(inner),
                completion_condition,
                parallel,
            });
        }
        Ok(if is_transaction {
            Node::TransactionSubProcess {
                id: sp_id,
                name: non_blank(sp.attr_or_empty("name")),
                inner: Box::new(inner),
            }
        } else {
            Node::SubProcess {
                id: sp_id,
                name: non_blank(sp.attr_or_empty("name")),
                inner: Box::new(inner),
            }
        })
    }
}

// ---- flow-node builders ------------------------------------------------------

fn build_end_or_error_event(
    e: &XmlElement,
    error_code_by_id: &HashMap<String, String>,
) -> Result<Node, SutraError> {
    let end_id = required(e.attr_or_empty("id"), "endEvent", "id")?;
    let name = non_blank(e.attr_or_empty("name"));
    let error_code = extract_error_code(e, error_code_by_id);
    if error_code.is_some() || has_event_definition(e, "errorEventDefinition") {
        return Ok(Node::ErrorEvent {
            id: end_id,
            name,
            error_code,
        });
    }
    if has_event_definition(e, "terminateEventDefinition") {
        return Ok(Node::TerminateEndEvent { id: end_id, name });
    }
    if has_event_definition(e, "cancelEventDefinition") {
        return Ok(Node::CancelEndEvent { id: end_id, name });
    }
    Ok(Node::EndEvent { id: end_id, name })
}

fn build_intermediate_throw_event(
    ite: &XmlElement,
    escalation_code_by_id: &HashMap<String, String>,
    signal_name_by_id: &HashMap<String, String>,
) -> Result<Node, SutraError> {
    let ite_id = required(ite.attr_or_empty("id"), "intermediateThrowEvent", "id")?;
    let mut kind = ThrowKind::None;
    let mut activity_ref = None;
    let mut reference: Option<String> = None;
    for d in &ite.children {
        if d.ns.as_deref() != Some(BPMN_NS) {
            continue;
        }
        match d.local.as_str() {
            "compensateEventDefinition" => {
                kind = ThrowKind::Compensate;
                let raw = d.attr_or_empty("activityRef");
                if !raw.trim().is_empty() {
                    let (_, local) = d.resolve_qname(raw);
                    if !local.trim().is_empty() {
                        activity_ref = Some(local);
                    }
                }
            }
            "messageEventDefinition" => kind = ThrowKind::Message,
            "signalEventDefinition" => {
                kind = ThrowKind::Signal;
                reference = resolve_ref(d, d.attr_or_empty("signalRef"), signal_name_by_id);
            }
            "escalationEventDefinition" => {
                kind = ThrowKind::Escalation;
                reference = resolve_ref(d, d.attr_or_empty("escalationRef"), escalation_code_by_id);
            }
            "linkEventDefinition" => {
                kind = ThrowKind::Link;
                let name = d.attr_or_empty("name").trim();
                if name.is_empty() {
                    return Err(err(
                        codes::PARSE_LINK_EVENT_NO_NAME,
                        format!(
                            "<intermediateThrowEvent> {ite_id} has a <linkEventDefinition> with no \
                             @name; a link throw must name the link it jumps to."
                        ),
                    ));
                }
                reference = Some(name.to_string());
            }
            _ => {}
        }
    }
    Ok(Node::IntermediateThrowEvent {
        id: ite_id,
        name: non_blank(ite.attr_or_empty("name")),
        kind,
        activity_ref,
        reference,
    })
}

fn build_link_catch_event(ice: &XmlElement) -> Result<Node, SutraError> {
    let ice_id = required(ice.attr_or_empty("id"), "intermediateCatchEvent", "id")?;
    let mut link_name: Option<String> = None;
    for d in ice.children_ns(BPMN_NS, "linkEventDefinition") {
        let name = d.attr_or_empty("name").trim();
        if !name.is_empty() {
            link_name = Some(name.to_string());
        }
    }
    let link_name = link_name.ok_or_else(|| {
        err(
            codes::PARSE_LINK_EVENT_NO_NAME,
            format!(
                "<intermediateCatchEvent> {ice_id} has a <linkEventDefinition> with no @name; a \
                 link catch must name the link it receives."
            ),
        )
    })?;
    Ok(Node::LinkCatchEvent {
        id: ice_id,
        name: non_blank(ice.attr_or_empty("name")),
        link_name,
    })
}

fn build_message_catch_event(
    ice: &XmlElement,
    channels_by_event_id: &HashMap<String, Vec<String>>,
) -> Result<Node, SutraError> {
    let ice_id = required(ice.attr_or_empty("id"), "intermediateCatchEvent", "id")?;
    let message = ice.children_ns(BPMN_NS, "messageEventDefinition").last();
    let message = message.ok_or_else(|| {
        err(
            codes::PARSE_BPMN_UNSUPPORTED_CATCH_EVENT,
            format!(
                "<intermediateCatchEvent> {ice_id} must carry a <messageEventDefinition>, a \
                 <timerEventDefinition> or a <linkEventDefinition>; signal and conditional catch \
                 events are not supported"
            ),
        )
    })?;
    let message_ref = {
        let raw = message.attr_or_empty("messageRef");
        if raw.trim().is_empty() {
            None
        } else {
            let (_, local) = message.resolve_qname(raw);
            non_blank(&local)
        }
    };
    Ok(Node::MessageCatchEvent {
        id: ice_id.clone(),
        name: non_blank(ice.attr_or_empty("name")),
        channels: channels_by_event_id
            .get(&ice_id)
            .cloned()
            .unwrap_or_default(),
        message_ref,
    })
}

/// Rust-only: an `<intermediateCatchEvent>` carrying a `<timerEventDefinition>` becomes a
/// [`Node::TimerCatchEvent`] wait state. Both single-shot forms are supported —
/// `<timeDuration>` (park + duration) and `<timeDate>` (an absolute instant, possibly already
/// past). `<timeCycle>` is load-time-rejected: a token parks at this node exactly once, so a
/// repeating trigger has no meaning here (it belongs on a START event).
fn build_timer_catch_event(ice: &XmlElement) -> Result<Node, SutraError> {
    let ice_id = required(ice.attr_or_empty("id"), "intermediateCatchEvent", "id")?;
    let def = ice
        .children_ns(BPMN_NS, "timerEventDefinition")
        .last()
        .expect("caller checked timerEventDefinition presence");
    let timer = extract_timer_definition(def, &ice_id, "intermediateCatchEvent", false)?;
    Ok(Node::TimerCatchEvent {
        id: ice_id,
        name: non_blank(ice.attr_or_empty("name")),
        timer,
    })
}

/// The `<timerEventDefinition>` of a `<startEvent>`, when it has one. Start events admit the
/// FULL contract — duration, date and ISO-8601 repeating cycle — because a schedule row, unlike
/// a parked token, can legitimately fire more than once.
///
/// The forms that stay out of contract (cron-syntax cycles, calendar-length durations) fail
/// closed under [`codes::CONFIG_BPMN_TIMER_START_UNSUPPORTED`], whose message names exactly
/// what remains unsupported.
fn build_start_timer(
    fe: &XmlElement,
    start_id: &str,
) -> Result<Option<crate::timer::TimerDefinition>, SutraError> {
    let Some(def) = fe.children_ns(BPMN_NS, "timerEventDefinition").last() else {
        return Ok(None);
    };
    extract_timer_definition(def, start_id, "startEvent", true).map(Some)
}

/// Extract + validate the single time specification of one `<timerEventDefinition>`.
///
/// `allow_cycle` is the host's repeat capability: true on a START event (a durable schedule row
/// may fire many times), false on an intermediate catch / boundary (one park, one fire).
///
/// Diagnostic split, deliberately three-way so an operator can act on it:
/// - out-of-contract FORM (cron cycle, calendar duration) → the host's unsupported-form code
///   ([`codes::CONFIG_BPMN_TIMER_START_UNSUPPORTED`] on a start,
///   [`codes::DISPATCH_TIMER_UNSUPPORTED`] elsewhere) — "we deliberately do not do that";
/// - right form written WRONG → the per-form invalid code (`…DURATION_INVALID`,
///   `…DATE_INVALID`, `…CYCLE_INVALID`) — "fix your text";
/// - a cycle on a non-repeating host, or more than one time child →
///   [`codes::DISPATCH_TIMER_UNSUPPORTED`] — "not here".
fn extract_timer_definition(
    timer: &XmlElement,
    host_id: &str,
    host_kind: &str,
    allow_cycle: bool,
) -> Result<crate::timer::TimerDefinition, SutraError> {
    // Only a START event routes out-of-contract FORMS to their own code — that is the P1-5a
    // narrowing. On a catch / boundary every bad spec keeps the per-form invalid code it has
    // always raised (a calendar duration there has reported DURATION_INVALID since the duration
    // contract shipped, and repurposing a stable code would break every consumer pinned to it).
    let is_start = host_kind == "startEvent";
    let text = |child: &str| -> Option<String> {
        timer
            .child_ns(BPMN_NS, child)
            .map(|d| d.trimmed_text().to_string())
    };
    let duration = text("timeDuration");
    let date = text("timeDate");
    let cycle = text("timeCycle");

    let declared: Vec<&str> = [
        duration.as_ref().map(|_| "timeDuration"),
        date.as_ref().map(|_| "timeDate"),
        cycle.as_ref().map(|_| "timeCycle"),
    ]
    .into_iter()
    .flatten()
    .collect();
    if declared.len() > 1 {
        return Err(err(
            codes::DISPATCH_TIMER_UNSUPPORTED,
            format!(
                "<{host_kind}> {host_id} declares {} time specifications ({}) on one \
                 <timerEventDefinition>; BPMN allows exactly one — pick the trigger you mean.",
                declared.len(),
                declared.join(" + ")
            ),
        ));
    }

    // Map a rejection onto the right code: on a start event an out-of-contract FORM is never
    // reported as a typo, so it gets the narrowed unsupported code instead.
    let classify = |rejection: crate::timer::TimerSpecRejection, invalid_code: &'static str| {
        let code = if is_start && rejection.is_unsupported_form() {
            codes::CONFIG_BPMN_TIMER_START_UNSUPPORTED
        } else {
            invalid_code
        };
        err(
            code,
            format!(
                "<{host_kind}> {host_id} timer is not schedulable: {}",
                rejection.reason()
            ),
        )
    };

    if let Some(cycle) = cycle {
        if !allow_cycle {
            return Err(err(
                codes::DISPATCH_TIMER_UNSUPPORTED,
                format!(
                    "<{host_kind}> {host_id} declares a <bpmn:timeCycle> ('{cycle}'); a repeating \
                     timer is supported only on a START event, where it arms a durable deployment \
                     schedule. A token parks at an intermediate catch / boundary exactly once, so \
                     use <bpmn:timeDuration> or <bpmn:timeDate> here."
                ),
            ));
        }
        let spec = crate::timer::parse_timer_cycle(&cycle)
            .map_err(|r| classify(r, codes::DISPATCH_TIMER_CYCLE_INVALID))?;
        return Ok(crate::timer::TimerDefinition::Cycle(spec));
    }
    if let Some(date) = date {
        crate::timer::parse_timer_instant(&date)
            .map_err(|r| classify(r, codes::DISPATCH_TIMER_DATE_INVALID))?;
        // A date in the PAST is legal by design — it is simply already due and fires on the
        // first tick that observes it. Rejecting it would make an archive's validity depend on
        // the wall clock, so a perfectly good deployment would stop being re-deployable.
        return Ok(crate::timer::TimerDefinition::Date(date));
    }
    let duration = duration.filter(|t| !t.is_empty()).ok_or_else(|| {
        err(
            codes::DISPATCH_TIMER_DURATION_INVALID,
            format!(
                "<{host_kind}> {host_id} has a <timerEventDefinition> with no time specification; \
                 declare a <bpmn:timeDuration> (e.g. PT30S){}.",
                if allow_cycle {
                    ", a <bpmn:timeDate> (e.g. 2026-03-01T09:00:00Z) or a <bpmn:timeCycle> (e.g. \
                     R/PT1H)"
                } else {
                    " or a <bpmn:timeDate> (e.g. 2026-03-01T09:00:00Z)"
                }
            ),
        )
    })?;
    crate::timer::parse_timer_duration(&duration)
        .map_err(|r| classify(r, codes::DISPATCH_TIMER_DURATION_INVALID))?;
    Ok(crate::timer::TimerDefinition::Duration(duration))
}

fn validate_duration(duration: &str, host_id: &str, host_kind: &str) -> Result<(), SutraError> {
    parse_iso8601_duration(duration).map_err(|reason| {
        err(
            codes::DISPATCH_TIMER_DURATION_INVALID,
            format!("<{host_kind}> {host_id} timer duration is invalid: {reason}"),
        )
    })?;
    Ok(())
}

fn build_boundary_event(
    be: &XmlElement,
    process_id: &str,
    activity_ids: &HashSet<String>,
    error_code_by_id: &HashMap<String, String>,
    escalation_code_by_id: &HashMap<String, String>,
) -> Result<Node, SutraError> {
    let be_id = required(be.attr_or_empty("id"), "boundaryEvent", "id")?;
    let attached_raw = be.attr_or_empty("attachedToRef");
    if attached_raw.trim().is_empty() {
        return Err(err(
            codes::PARSE_BOUNDARY_EVENT_INVALID_REF,
            format!("<boundaryEvent> {be_id} missing required @attachedToRef"),
        ));
    }
    let (_, attached_to) = be.resolve_qname(attached_raw);
    if !activity_ids.contains(&attached_to) {
        return Err(err(
            codes::PARSE_BOUNDARY_EVENT_INVALID_REF,
            format!(
                "<boundaryEvent> {be_id} attaches to {attached_to} but no matching activity \
                 exists in process {process_id}"
            ),
        ));
    }

    let mut kind: Option<BoundaryKind> = None;
    let mut error_code = None;
    let mut escalation_code = None;
    let mut timer = None;
    for d in &be.children {
        if d.ns.as_deref() != Some(BPMN_NS) {
            continue;
        }
        match d.local.as_str() {
            "errorEventDefinition" => {
                kind = Some(BoundaryKind::Error);
                error_code = resolve_ref(d, d.attr_or_empty("errorRef"), error_code_by_id);
            }
            "escalationEventDefinition" => {
                kind = Some(BoundaryKind::Escalation);
                escalation_code =
                    resolve_ref(d, d.attr_or_empty("escalationRef"), escalation_code_by_id);
            }
            "compensateEventDefinition" => kind = Some(BoundaryKind::Compensation),
            "cancelEventDefinition" => kind = Some(BoundaryKind::Cancel),
            "timerEventDefinition" => {
                kind = Some(BoundaryKind::Timer);
                timer = Some(extract_timer_definition(
                    d,
                    be_id.as_str(),
                    "boundaryEvent",
                    false,
                )?);
            }
            _ => {}
        }
    }
    let kind = kind.ok_or_else(|| {
        err(
            codes::PARSE_BOUNDARY_EVENT_INVALID_REF,
            format!(
                "<boundaryEvent> {be_id} requires an errorEventDefinition, \
                 escalationEventDefinition, compensateEventDefinition, \
                 cancelEventDefinition or timerEventDefinition"
            ),
        )
    })?;
    // BPMN @cancelActivity defaults to true (interrupting); a bare escalation boundary
    // defaults to NON-interrupting so the raising flow keeps running.
    let interrupting = match be.attr(None, "cancelActivity") {
        Some(v) => v == "true",
        None => kind != BoundaryKind::Escalation,
    };
    // Timer boundaries are INTERRUPTING only (interrupting: cancel the host task's wait);
    // a non-interrupting timer would need its own semantics.
    if kind == BoundaryKind::Timer && !interrupting {
        return Err(err(
            codes::DISPATCH_TIMER_UNSUPPORTED,
            format!(
                "<boundaryEvent> {be_id} is a timer boundary with cancelActivity=\"false\"; \
                 only INTERRUPTING timer boundaries are supported (the fire cancels the host \
                 task's wait)."
            ),
        ));
    }
    Ok(Node::BoundaryEvent {
        id: be_id,
        name: non_blank(be.attr_or_empty("name")),
        attached_to_ref: attached_to,
        kind,
        error_code,
        escalation_code,
        interrupting,
        timer,
    })
}

fn build_service_task(
    st: &XmlElement,
    var_name_by_data_id: &HashMap<String, String>,
    scan: &DocScan,
) -> Result<Node, SutraError> {
    let id = required(st.attr_or_empty("id"), "serviceTask", "id")?;
    let data_mapping =
        build_data_mapping(st, &id, var_name_by_data_id, &scan.store_bindings_by_ref_id)?;
    let implementation = st.attr_or_empty("implementation");
    if implementation.trim().is_empty() {
        // A serviceTask with data-store associations but NO @implementation is a
        // declarative DATA TASK: its store ops ARE its behaviour (no imperative body).
        if data_mapping.has_store_ops() {
            return Ok(Node::DataTask {
                id,
                name: non_blank(st.attr_or_empty("name")),
                data_mapping,
            });
        }
        return Err(err(
            codes::RESOLVE_TASK_UNKNOWN,
            format!("serviceTask {id} has no implementation attribute"),
        ));
    }
    // BPMN often uses ${beanName} form for implementation — strip the braces.
    let implementation = implementation
        .strip_prefix("${")
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(implementation)
        .to_string();
    Ok(Node::ServiceTask {
        id: id.clone(),
        name: non_blank(st.attr_or_empty("name")),
        implementation,
        data_mapping,
        params: scan
            .params_by_service_task_id
            .get(&id)
            .cloned()
            .unwrap_or_default(),
    })
}

fn build_script_task(sct: &XmlElement, scan: &DocScan) -> Result<Node, SutraError> {
    let id = required(sct.attr_or_empty("id"), "scriptTask", "id")?;
    let script_file = scan
        .script_file_by_task_id
        .get(&id)
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if script_file.is_empty() {
        return Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!(
                "<scriptTask> {id} requires a <bpmn:script> naming a file in the module's \
                 scripts/ folder (e.g. <bpmn:script>derive-fee.hbs</bpmn:script>); the engine is \
                 chosen by the file extension, so no scriptFormat is needed."
            ),
        ));
    }
    Ok(Node::ScriptTask {
        id,
        name: non_blank(sct.attr_or_empty("name")),
        script_file,
    })
}

fn build_call_activity(ca: &XmlElement) -> Result<Node, SutraError> {
    let id = required(ca.attr_or_empty("id"), "callActivity", "id")?;
    let raw = ca.attr_or_empty("calledElement");
    if raw.trim().is_empty() {
        return Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("<callActivity> {id} missing required @calledElement"),
        ));
    }
    // calledElement is a QName ("ns:proc" or just "proc"); the engine carries the local part
    // plus the namespace URI separately.
    let (ns, called_element) = ca.resolve_qname(raw);
    if called_element.trim().is_empty() {
        return Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("<callActivity> {id} has empty @calledElement"),
        ));
    }
    let called_namespace = ns.filter(|n| !n.trim().is_empty());
    Ok(Node::CallActivity {
        id,
        name: non_blank(ca.attr_or_empty("name")),
        called_element,
        called_namespace,
    })
}

fn build_sequence_flow(sf: &XmlElement) -> Result<SequenceFlow, SutraError> {
    let id = required(sf.attr_or_empty("id"), "sequenceFlow", "id")?;
    let source = required(sf.attr_or_empty("sourceRef"), "sequenceFlow", "sourceRef")?;
    let target = required(sf.attr_or_empty("targetRef"), "sequenceFlow", "targetRef")?;
    let condition = sf
        .child_ns(BPMN_NS, "conditionExpression")
        .map(|c| c.trimmed_text().to_string())
        .filter(|t| !t.is_empty());
    Ok(SequenceFlow {
        id,
        source_ref: source,
        target_ref: target,
        condition,
    })
}

/// Wrap an activity carrying loop characteristics in the matching marker node.
fn wrap_maybe_loop(activity: &XmlElement, inner: Node) -> Result<Node, SutraError> {
    if let Some(slc) = activity.child_ns(BPMN_NS, "standardLoopCharacteristics") {
        let loop_condition = slc
            .child_ns(BPMN_NS, "loopCondition")
            .map(|c| c.trimmed_text().to_string())
            .filter(|t| !t.is_empty());
        let test_before = slc.attr_or_empty("testBefore") == "true";
        let loop_maximum = slc
            .attr(None, "loopMaximum")
            .and_then(|v| v.trim().parse::<i64>().ok());
        let id = inner.id().to_string();
        let name = node_name(&inner);
        return Ok(Node::StandardLoop {
            id,
            name,
            inner: Box::new(inner),
            loop_condition,
            test_before,
            loop_maximum,
        });
    }
    let Some(mi) = activity.child_ns(BPMN_NS, "multiInstanceLoopCharacteristics") else {
        return Ok(inner);
    };
    let sequential = mi.attr_or_empty("isSequential") == "true";
    let loop_cardinality = mi
        .child_ns(BPMN_NS, "loopCardinality")
        .map(|c| c.trimmed_text().to_string())
        .filter(|t| !t.is_empty());
    let loop_data_input_ref = mi
        .child_ns(BPMN_NS, "loopDataInputRef")
        .map(|c| {
            // QName — the engine carries the local part.
            let t = c.trimmed_text();
            t.rsplit(':').next().unwrap_or(t).to_string()
        })
        .filter(|t| !t.is_empty());
    let input_data_item = mi
        .child_ns(BPMN_NS, "inputDataItem")
        .and_then(|c| c.attr(None, "name"))
        .map(|s| s.to_string())
        .filter(|t| !t.trim().is_empty());
    let completion_condition = mi
        .child_ns(BPMN_NS, "completionCondition")
        .map(|c| c.trimmed_text().to_string())
        .filter(|t| !t.is_empty());
    if loop_cardinality.is_none() && loop_data_input_ref.is_none() {
        return Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!(
                "MultiInstance {} requires loopCardinality or loopDataInputRef",
                inner.id()
            ),
        ));
    }
    let id = inner.id().to_string();
    let name = node_name(&inner);
    Ok(Node::MultiInstance {
        id,
        name,
        inner: Box::new(inner),
        sequential,
        loop_cardinality,
        loop_data_input_ref,
        input_data_item,
        completion_condition,
    })
}

fn node_name(n: &Node) -> Option<String> {
    match n {
        Node::ServiceTask { name, .. }
        | Node::DataTask { name, .. }
        | Node::ScriptTask { name, .. }
        | Node::ManualTask { name, .. }
        | Node::SendTask { name, .. }
        | Node::BusinessRuleTask { name, .. }
        | Node::CallActivity { name, .. }
        | Node::SubProcess { name, .. }
        | Node::TransactionSubProcess { name, .. }
        | Node::AdHocSubProcess { name, .. } => name.clone(),
        _ => None,
    }
}

// ---- data associations ---------------------------------------------------------

/// Index a container's `<bpmn:dataObject>` / `<bpmn:dataObjectReference>` by id → the
/// process-variable name it denotes (the element's `name`, falling back to its id).
fn index_data_elements(container: &XmlElement) -> HashMap<String, String> {
    let mut by_id = HashMap::new();
    for fe in container.children_ns(BPMN_NS, "dataObject") {
        let id = fe.attr_or_empty("id");
        if id.is_empty() {
            continue;
        }
        let name = fe.attr_or_empty("name");
        by_id.insert(
            id.to_string(),
            if name.trim().is_empty() { id } else { name }.to_string(),
        );
    }
    let refs: Vec<(&str, &str, &str)> = container
        .children_ns(BPMN_NS, "dataObjectReference")
        .filter(|r| !r.attr_or_empty("id").is_empty())
        .map(|r| {
            (
                r.attr_or_empty("id"),
                r.attr_or_empty("name"),
                r.attr_or_empty("dataObjectRef"),
            )
        })
        .collect();
    for (id, name, data_object_ref) in refs {
        let resolved = if !name.trim().is_empty() {
            name.to_string()
        } else {
            by_id
                .get(data_object_ref)
                .cloned()
                .unwrap_or_else(|| id.to_string())
        };
        by_id.insert(id.to_string(), resolved);
    }
    by_id
}

fn build_data_mapping(
    st: &XmlElement,
    id: &str,
    var_name_by_data_id: &HashMap<String, String>,
    store_bindings: &HashMap<String, StoreBinding>,
) -> Result<DataMapping, SutraError> {
    let ins: Vec<&XmlElement> = st.children_ns(BPMN_NS, "dataInputAssociation").collect();
    let outs: Vec<&XmlElement> = st.children_ns(BPMN_NS, "dataOutputAssociation").collect();
    if ins.is_empty() && outs.is_empty() {
        return Ok(DataMapping::default());
    }
    let mut mapping = DataMapping::default();

    for a in ins {
        if a.child_ns(BPMN_NS, "transformation").is_some() {
            return Err(data_association_unsupported(
                "serviceTask",
                id,
                "a <transformation> expression",
            ));
        }
        // FEEL <assignment><from>expr</from><to>var</to></assignment> — a data-assignment node.
        let assignments: Vec<&XmlElement> = a.children_ns(BPMN_NS, "assignment").collect();
        if !assignments.is_empty() {
            collect_assignments(&assignments, id, &mut mapping.assignments)?;
            continue;
        }
        // sourceRef → a <dataStoreReference> = a store READ into the targetRef variable.
        let source_ref = first_source_ref(a);
        let sb = source_ref.and_then(|r| store_bindings.get(r));
        if let Some(sb) = sb {
            let target_var = a
                .child_ns(BPMN_NS, "targetRef")
                .map(|t| t.trimmed_text())
                .filter(|t| !t.is_empty())
                .map(|r| resolve_var(r, var_name_by_data_id));
            let Some(target_var) = target_var else {
                return Err(data_association_unsupported(
                    "serviceTask",
                    id,
                    "a data-store read whose <targetRef> names no variable to load into",
                ));
            };
            validate_store(sb, id)?;
            mapping.store_reads.push(StoreRead {
                store: sb.store_name.clone().unwrap_or_default(),
                key_expression: sb.key.clone().unwrap_or_default(),
                for_update: sb.for_update,
                target_var,
            });
            continue;
        }
        // Plain variable scoping (dataObject refs).
        for r in a.children_ns(BPMN_NS, "sourceRef") {
            let t = r.trimmed_text();
            if !t.is_empty() {
                mapping.inputs.push(resolve_var(t, var_name_by_data_id));
            }
        }
    }
    for a in outs {
        if a.child_ns(BPMN_NS, "transformation").is_some() {
            return Err(data_association_unsupported(
                "serviceTask",
                id,
                "a <transformation> expression",
            ));
        }
        let assignments: Vec<&XmlElement> = a.children_ns(BPMN_NS, "assignment").collect();
        if !assignments.is_empty() {
            collect_assignments(&assignments, id, &mut mapping.assignments)?;
            continue;
        }
        // targetRef → a <dataStoreReference> = a store WRITE of the sourceRef variable.
        let target_ref = a
            .child_ns(BPMN_NS, "targetRef")
            .map(|t| t.trimmed_text())
            .filter(|t| !t.is_empty());
        let sb = target_ref.and_then(|r| store_bindings.get(r));
        if let Some(sb) = sb {
            let value_var = first_source_ref(a).map(|r| resolve_var(r, var_name_by_data_id));
            let Some(value_var) = value_var else {
                return Err(data_association_unsupported(
                    "serviceTask",
                    id,
                    "a data-store write whose <sourceRef> names no variable to write",
                ));
            };
            validate_store(sb, id)?;
            mapping.store_writes.push(StoreWrite {
                store: sb.store_name.clone().unwrap_or_default(),
                key_expression: sb.key.clone().unwrap_or_default(),
                field: sb.field.clone(),
                value_var,
                expect_unchanged: sb.expect_unchanged,
            });
            continue;
        }
        if let Some(t) = target_ref {
            mapping.outputs.push(resolve_var(t, var_name_by_data_id));
        }
    }
    Ok(mapping)
}

fn collect_assignments(
    assignments: &[&XmlElement],
    node_id: &str,
    out: &mut Vec<Assignment>,
) -> Result<(), SutraError> {
    for a in assignments {
        let from = a
            .child_ns(BPMN_NS, "from")
            .map(|f| f.trimmed_text())
            .filter(|t| !t.is_empty());
        let to = a
            .child_ns(BPMN_NS, "to")
            .map(|t| t.trimmed_text())
            .filter(|t| !t.is_empty());
        let (Some(from), Some(to)) = (from, to) else {
            return Err(err(
                codes::PARSE_DATA_ASSOCIATION_UNSUPPORTED,
                format!(
                    "<serviceTask> '{node_id}' has an <assignment> missing a <from> FEEL \
                     expression or a <to> target variable."
                ),
            ));
        };
        out.push(Assignment {
            expression: from.to_string(),
            target_var: to.to_string(),
        });
    }
    Ok(())
}

/// A store association must name a real store and carry a `<q:store key>` to key it by.
fn validate_store(sb: &StoreBinding, node_id: &str) -> Result<(), SutraError> {
    if sb
        .store_name
        .as_deref()
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        return Err(err(
            codes::PARSE_STORE_KEY_REQUIRED,
            format!(
                "<serviceTask> '{node_id}' references a <bpmn:dataStoreReference> with no \
                 resolvable store (@dataStoreRef missing)."
            ),
        ));
    }
    if sb
        .key
        .as_deref()
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        return Err(err(
            codes::PARSE_STORE_KEY_REQUIRED,
            format!(
                "<serviceTask> '{node_id}' reads/writes data store '{}' but the \
                 <bpmn:dataStoreReference> declares no <q:store key=\"<feel>\"/>.",
                sb.store_name.as_deref().unwrap_or("")
            ),
        ));
    }
    Ok(())
}

fn first_source_ref(association: &XmlElement) -> Option<&str> {
    association
        .children_ns(BPMN_NS, "sourceRef")
        .map(|r| r.trimmed_text())
        .find(|t| !t.is_empty())
}

fn resolve_var(reference: &str, var_name_by_data_id: &HashMap<String, String>) -> String {
    var_name_by_data_id
        .get(reference)
        .cloned()
        .unwrap_or_else(|| reference.to_string())
}

/// Only a serviceTask carries data associations in this slice; reject elsewhere.
fn reject_data_associations(activity: &XmlElement, kind: &str) -> Result<(), SutraError> {
    let has_in = activity.child_ns(BPMN_NS, "dataInputAssociation").is_some();
    let has_out = activity
        .child_ns(BPMN_NS, "dataOutputAssociation")
        .is_some();
    if has_in || has_out {
        return Err(data_association_unsupported(
            kind,
            activity.attr_or_empty("id"),
            &format!(
                "a data association on a <{kind}>; only <serviceTask> data associations are \
                 supported"
            ),
        ));
    }
    Ok(())
}

fn data_association_unsupported(kind: &str, node_id: &str, detail: &str) -> SutraError {
    err(
        codes::PARSE_DATA_ASSOCIATION_UNSUPPORTED,
        format!(
            "<{kind}> '{node_id}' declares {detail}. Model the mapping with plain \
             <dataInputAssociation><sourceRef>/<dataOutputAssociation><targetRef> referencing \
             <dataObject>/<dataObjectReference> by id."
        ),
    )
}

/// Scan every `<bpmn:dataStoreReference>` into a [`StoreBinding`] keyed by its id.
fn collect_store_bindings(root: &XmlElement) -> HashMap<String, StoreBinding> {
    let mut store_name_by_id = HashMap::new();
    for ds in root.collect_descendants_ns(BPMN_NS, "dataStore") {
        let ds_id = ds.attr_or_empty("id");
        if ds_id.trim().is_empty() {
            continue;
        }
        let name = ds.attr_or_empty("name");
        store_name_by_id.insert(
            ds_id.to_string(),
            if name.trim().is_empty() { ds_id } else { name }.to_string(),
        );
    }
    let mut out = HashMap::new();
    for r in root.collect_descendants_ns(BPMN_NS, "dataStoreReference") {
        let ref_id = r.attr_or_empty("id");
        if ref_id.trim().is_empty() {
            continue;
        }
        let store_name = resolve_store_name(r.attr_or_empty("dataStoreRef"), &store_name_by_id);
        let mut key = None;
        let mut for_update = false;
        let mut field = None;
        let mut expect_unchanged = false;
        if let Some(q) = r.collect_descendants_ns(Q_NS, "store").first() {
            key = trim_to_none(q.attr_or_empty("key"));
            for_update = q.attr_or_empty("forUpdate").eq_ignore_ascii_case("true");
            field = trim_to_none(q.attr_or_empty("field"));
            expect_unchanged = trim_to_none(q.attr_or_empty("expect"))
                .map(|v| v.eq_ignore_ascii_case("unchanged"))
                .unwrap_or(false);
        }
        out.insert(
            ref_id.to_string(),
            StoreBinding {
                store_name,
                key,
                for_update,
                field,
                expect_unchanged,
            },
        );
    }
    out
}

fn resolve_store_name(
    data_store_ref: &str,
    store_name_by_id: &HashMap<String, String>,
) -> Option<String> {
    if data_store_ref.trim().is_empty() {
        return None;
    }
    let local = data_store_ref.rsplit(':').next().unwrap_or(data_store_ref);
    Some(
        store_name_by_id
            .get(local)
            .cloned()
            .unwrap_or_else(|| local.to_string()),
    )
}

// ---- q: bindings collection ----------------------------------------------------

/// Walk every flow element with a `<bpmn:extensionElements>` child (skipping `<bpmn:process>`
/// hosts) and harvest every recognised `<q:*>` extension into a per-node [`NodeBindings`].
fn collect_node_bindings(
    el: &XmlElement,
    out: &mut HashMap<String, NodeBindings>,
) -> Result<(), SutraError> {
    for child in &el.children {
        collect_node_bindings(child, out)?;
    }
    if el.is(BPMN_NS, "process") {
        return Ok(());
    }
    let host_id = el.attr_or_empty("id");
    if host_id.trim().is_empty() {
        return Ok(());
    }
    for ext in el.children_ns(BPMN_NS, "extensionElements") {
        let b = NodeBindings {
            sources: parse_sources(ext, host_id)?,
            on_validation: parse_on_validation(ext, host_id)?,
            dispatch: parse_dispatch(ext, host_id)?,
            reply: parse_reply(ext, host_id)?,
            send: parse_send(ext, host_id)?,
            aliases: parse_aliases(ext, host_id)?,
            audit: parse_per_node_audit(ext)?,
            timeout: parse_timeout(ext, host_id)?,
            output: parse_output(ext, host_id)?,
            retry: parse_retry(ext, host_id)?,
        };
        if !b.is_empty() {
            out.insert(host_id.to_string(), b);
        }
    }
    Ok(())
}

/// `<q:timeout duration="PT30S"/>` — the attribute form of a channel-call
/// timer boundary). Duration is required and must be a parseable ISO-8601 duration.
fn parse_timeout(ext: &XmlElement, host_id: &str) -> Result<Option<TimeoutBinding>, SutraError> {
    let Some(t) = ext.children_ns(Q_NS, "timeout").next() else {
        return Ok(None);
    };
    let duration = t.attr_or_empty("duration").trim().to_string();
    if duration.is_empty() {
        return Err(err(
            codes::DISPATCH_TIMER_DURATION_INVALID,
            format!(
                "<q:timeout> on node {host_id} missing required @duration (an ISO-8601 \
                 duration, e.g. PT30S)."
            ),
        ));
    }
    validate_duration(&duration, host_id, "q:timeout")?;
    Ok(Some(TimeoutBinding { duration }))
}

/// The `<q:retry initialDelay>` default — one second before the second attempt.
const RETRY_DEFAULT_INITIAL_DELAY: &str = "PT1S";
/// The `<q:retry maxDelay>` default — the same five-minute horizon the outbox curve clamps at,
/// so a task retry and a delivery retry age at a comparable rate.
const RETRY_DEFAULT_MAX_DELAY: &str = "PT5M";
/// The `<q:retry backoffCoefficient>` default — plain exponential doubling.
const RETRY_DEFAULT_BACKOFF_COEFFICIENT: f64 = 2.0;

/// `<q:retry maxAttempts="…" initialDelay="PT1S" backoffCoefficient="2.0" maxDelay="PT5M"
/// nonRetryableCodes="A,B"/>` — the per-task retry policy (see [`RetryBinding`]).
///
/// Fail-closed on every attribute: `@maxAttempts` is required and must be a positive integer;
/// the two durations must be parseable ISO-8601 with `maxDelay >= initialDelay`; the coefficient
/// must be a finite number ≥ 1.0; and a `@nonRetryableCodes` that is present but names nothing
/// (`","`, whitespace) is rejected rather than silently read as "retry everything" — an author
/// who wrote the attribute meant something by it.
fn parse_retry(ext: &XmlElement, host_id: &str) -> Result<Option<RetryBinding>, SutraError> {
    let Some(r) = ext.children_ns(Q_NS, "retry").next() else {
        return Ok(None);
    };
    let raw_max_attempts = r.attr_or_empty("maxAttempts").trim().to_string();
    let max_attempts = match raw_max_attempts.parse::<u32>() {
        Ok(n) if n >= 1 => n,
        _ => {
            return Err(err(
                codes::CONFIG_BPMN_RETRY_MAX_ATTEMPTS_INVALID,
                format!(
                    "<q:retry> on node {host_id} needs @maxAttempts as a positive integer \
                     (total attempts INCLUDING the first; 1 means never retry), but found \
                     '{raw_max_attempts}'. It is required with no default: an unbounded task \
                     retry is the failure mode this policy exists to remove."
                ),
            ));
        }
    };

    let initial_delay =
        attr_trimmed(r, "initialDelay").unwrap_or_else(|| RETRY_DEFAULT_INITIAL_DELAY.to_string());
    let max_delay =
        attr_trimmed(r, "maxDelay").unwrap_or_else(|| RETRY_DEFAULT_MAX_DELAY.to_string());
    let initial = parse_iso8601_duration(&initial_delay).map_err(|reason| {
        err(
            codes::CONFIG_BPMN_RETRY_POLICY_INVALID,
            format!(
                "<q:retry> on node {host_id} @initialDelay '{initial_delay}' is not a valid \
                 ISO-8601 duration: {reason}"
            ),
        )
    })?;
    let ceiling = parse_iso8601_duration(&max_delay).map_err(|reason| {
        err(
            codes::CONFIG_BPMN_RETRY_POLICY_INVALID,
            format!(
                "<q:retry> on node {host_id} @maxDelay '{max_delay}' is not a valid ISO-8601 \
                 duration: {reason}"
            ),
        )
    })?;
    if ceiling < initial {
        return Err(err(
            codes::CONFIG_BPMN_RETRY_POLICY_INVALID,
            format!(
                "<q:retry> on node {host_id} declares @maxDelay '{max_delay}' below \
                 @initialDelay '{initial_delay}'; the ceiling clamps the growing delay, so it \
                 can never be the smaller of the two."
            ),
        ));
    }

    let backoff_coefficient = match attr_trimmed(r, "backoffCoefficient") {
        None => RETRY_DEFAULT_BACKOFF_COEFFICIENT,
        Some(raw) => match raw.parse::<f64>() {
            Ok(v) if v.is_finite() && v >= 1.0 => v,
            _ => {
                return Err(err(
                    codes::CONFIG_BPMN_RETRY_POLICY_INVALID,
                    format!(
                        "<q:retry> on node {host_id} @backoffCoefficient '{raw}' must be a \
                         finite number >= 1.0 (1.0 is a fixed delay; a coefficient below 1 \
                         would SHRINK the wait and re-hammer the failing dependency)."
                    ),
                ));
            }
        },
    };

    // Presence, not trimmed-value: a `nonRetryableCodes=" "` is a PRESENT attribute that names
    // nothing, and reading it as absent would silently discard whatever the author meant.
    let non_retryable_codes = match r.attr(None, "nonRetryableCodes") {
        None => Vec::new(),
        Some(raw) => {
            let raw = raw.to_string();
            let mut codes_list: Vec<String> = raw
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect();
            codes_list.dedup();
            if codes_list.is_empty() {
                return Err(err(
                    codes::CONFIG_BPMN_RETRY_POLICY_INVALID,
                    format!(
                        "<q:retry> on node {host_id} declares @nonRetryableCodes='{raw}', which \
                         names no code. Remove the attribute to retry every uncaught failure."
                    ),
                ));
            }
            codes_list
        }
    };

    Ok(Some(RetryBinding {
        max_attempts,
        initial_delay,
        backoff_coefficient,
        max_delay,
        non_retryable_codes,
    }))
}

/// `<q:output variable="…"/>` — the render capture.
fn parse_output(ext: &XmlElement, host_id: &str) -> Result<Option<OutputBinding>, SutraError> {
    let Some(o) = ext.children_ns(Q_NS, "output").next() else {
        return Ok(None);
    };
    let variable = o.attr_or_empty("variable").trim().to_string();
    if variable.is_empty() {
        return Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!(
                "<q:output> on node {host_id} missing required @variable (the process \
                 variable the render is captured into)."
            ),
        ));
    }
    Ok(Some(OutputBinding { variable }))
}

fn parse_sources(ext: &XmlElement, host_id: &str) -> Result<Vec<SourceBinding>, SutraError> {
    let mut out = Vec::new();
    for s in ext.children_ns(Q_NS, "source") {
        let channel = s.attr_or_empty("channel");
        if channel.trim().is_empty() {
            continue;
        }
        // Codec is YAML-authoritative — it is declared on the channel YAML, not the BPMN.
        if !s.attr_or_empty("codec").trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_SOURCE_CODEC_NOT_ALLOWED,
                format!(
                    "<q:source> on node {host_id} declares @codec, but the codec is bound on the \
                     channel YAML (YAML-authoritative). Remove @codec from <q:source>."
                ),
            ));
        }
        // `idempotencyKey` was renamed to `dedupKey` (a dedup key detects redelivery; it is
        // NOT an idempotency assertion — that is the process-level `<q:process idempotent>`). The
        // old attribute is a hard deploy-time error (breaking rename, pre-GA).
        if s.attr(None, "idempotencyKey").is_some() {
            return Err(err(
                codes::PARSE_Q_SOURCE_IDEMPOTENCY_KEY_RENAMED,
                format!(
                    "<q:source> on node {host_id} declares the retired @idempotencyKey attribute. \
                     It was renamed to @dedupKey (a duplicate-detection value; it does not assert \
                     idempotency — declare `<q:process idempotent=\"true\">` for that). Rename \
                     idempotencyKey → dedupKey."
                ),
            ));
        }
        let name = s.attr_or_empty("name");
        let ack = parse_ack_mode(s.attr_or_empty("ack"))?;
        let data_class = parse_data_class(s.attr_or_empty("dataClass"))?;

        let mut complex_validators = Vec::new();
        let mut simple_validators = Vec::new();
        for container in s.children_ns(Q_NS, "validators") {
            parse_validators(
                container,
                host_id,
                &mut complex_validators,
                &mut simple_validators,
            )?;
        }

        let mut redactors = Vec::new();
        for container in s.children_ns(Q_NS, "redactors") {
            parse_redactors(container, &mut redactors);
        }

        let mt_value = attr_trimmed(s, "messageTypeValue");
        let mt_pattern = attr_trimmed(s, "messageTypePattern");
        if mt_value.is_some() && mt_pattern.is_some() {
            return Err(err(
                codes::PARSE_Q_SOURCE_MESSAGE_TYPE_CONFLICT,
                format!(
                    "<q:source> on node {host_id} declares both messageTypeValue and \
                     messageTypePattern; they are mutually exclusive"
                ),
            ));
        }
        out.push(SourceBinding {
            channel: channel.to_string(),
            name: if name.trim().is_empty() {
                "payload".to_string()
            } else {
                name.to_string()
            },
            ack,
            dedup_key: opt_attr(s, "dedupKey"),
            message_type: opt_attr(s, "type"),
            data_class,
            complex_validators,
            simple_validators,
            redactors,
            message_type_value: mt_value,
            message_type_pattern: mt_pattern,
        });
    }
    if out.len() > 1 {
        return Err(err(
            codes::PARSE_Q_SOURCE_MULTIPLE,
            format!(
                "Node {host_id} declares {} <q:source> elements; at most one is allowed — a \
                 start event handles exactly one message type.",
                out.len()
            ),
        ));
    }
    Ok(out)
}

fn parse_validators(
    container: &XmlElement,
    host_id: &str,
    complex: &mut Vec<String>,
    simple: &mut Vec<SimpleValidator>,
) -> Result<(), SutraError> {
    for cv in container.children_ns(Q_NS, "complexValidator") {
        let src = cv.attr_or_empty("source");
        if !src.trim().is_empty() {
            complex.push(src.to_string());
        }
    }
    for sv in container.children_ns(Q_NS, "simpleValidator") {
        let reference = sv.attr_or_empty("ref");
        let path = sv.attr_or_empty("path");
        if reference.trim().is_empty() || path.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_SIMPLE_VALIDATOR_INCOMPLETE,
                format!("<q:simpleValidator> on node {host_id} requires both @ref and @path."),
            ));
        }
        simple.push(SimpleValidator {
            reference: reference.to_string(),
            path: path.to_string(),
        });
    }
    Ok(())
}

/// Parse a `<q:redactors>` container into the list of `<q:redactor ref="…">` names. An empty or
/// missing `ref` is skipped (mirrors `<q:complexValidator source>` handling). Shared by the
/// source-level and process-level parse paths.
fn parse_redactors(container: &XmlElement, out: &mut Vec<String>) {
    for r in container.children_ns(Q_NS, "redactor") {
        let reference = r.attr_or_empty("ref");
        if !reference.trim().is_empty() {
            out.push(reference.to_string());
        }
    }
}

fn parse_on_validation(
    ext: &XmlElement,
    host_id: &str,
) -> Result<Option<OnValidationBinding>, SutraError> {
    let Some(el) = ext.children_ns(Q_NS, "onValidation").next() else {
        return Ok(None);
    };
    let mode = el.attr_or_empty("mode");
    if mode.trim().is_empty() {
        return Err(err(
            codes::PARSE_Q_ON_VALIDATION_INVALID_MODE,
            format!("<q:onValidation> on node {host_id} missing required @mode attribute"),
        ));
    }
    let parsed = match mode {
        "route" => OnValidationMode::Route,
        "reject" => OnValidationMode::Reject,
        "error" => OnValidationMode::Error,
        _ => {
            return Err(err(
                codes::PARSE_Q_ON_VALIDATION_INVALID_MODE,
                format!(
                    "<q:onValidation mode={mode}> on node {host_id} is not one of route / \
                     reject / error"
                ),
            ))
        }
    };
    Ok(Some(OnValidationBinding {
        mode: parsed,
        error_code: opt_attr(el, "errorCode"),
    }))
}

fn parse_dispatch(ext: &XmlElement, host_id: &str) -> Result<Option<DispatchTable>, SutraError> {
    let Some(d) = ext.children_ns(Q_NS, "dispatch").next() else {
        return Ok(None);
    };
    let default_called = opt_attr(d, "default");
    let on_no_match = parse_on_no_match(d.attr_or_empty("onNoMatch"))?;
    let mut cases = Vec::new();
    for c in d.children_ns(Q_NS, "case") {
        let when = c.attr_or_empty("when");
        if when.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_CASE_MISSING_WHEN,
                format!("<q:case> on call activity {host_id} missing required @when attribute"),
            ));
        }
        let called_element = c.attr_or_empty("calledElement");
        if called_element.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_CASE_MISSING_CALLED_ELEMENT,
                format!(
                    "<q:case> on call activity {host_id} missing required @calledElement attribute"
                ),
            ));
        }
        cases.push(CaseEntry {
            when: when.to_string(),
            called_element: called_element.to_string(),
        });
    }
    if cases.is_empty() {
        return Err(err(
            codes::PARSE_Q_CASE_MISSING_WHEN,
            format!("<q:dispatch> on call activity {host_id} requires at least one <q:case> child"),
        ));
    }
    Ok(Some(DispatchTable {
        default_called_element: default_called,
        on_no_match,
        cases,
    }))
}

fn parse_aliases(ext: &XmlElement, host_id: &str) -> Result<Vec<AliasBinding>, SutraError> {
    let mut out = Vec::new();
    for a in ext.children_ns(Q_NS, "alias") {
        let name = a.attr_or_empty("name");
        if name.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_ALIAS_MISSING_NAME,
                format!("<q:alias> on node {host_id} missing required @name attribute"),
            ));
        }
        let expression = a.attr_or_empty("expression");
        if expression.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_ALIAS_MISSING_EXPRESSION,
                format!(
                    "<q:alias {name}> on node {host_id} missing required @expression attribute"
                ),
            ));
        }
        out.push(AliasBinding {
            name: name.to_string(),
            expression: expression.to_string(),
            unique: a.attr_or_empty("unique") == "true",
            on_conflict: parse_alias_conflict(a.attr_or_empty("onConflict"))?,
            multi: a.attr_or_empty("multi") == "true",
        });
    }
    Ok(out)
}

/// Parse the author-declared `<q:header name="…" value="<FEEL>"/>` children of
/// an outbound `<q:send>` / `<q:reply>`. `@name` and `@value` are both required; `@value` is a FEEL
/// expression evaluated against the sending process context at dispatch (not at parse). Domain-neutral:
/// the name is an opaque author string.
fn parse_headers(
    el: &XmlElement,
    host_id: &str,
    element: &str,
) -> Result<Vec<HeaderAttr>, SutraError> {
    let mut out = Vec::new();
    for h in el.children_ns(Q_NS, "header") {
        let name = h.attr_or_empty("name");
        if name.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_HEADER_INCOMPLETE,
                format!(
                    "<q:header> on {element} of node {host_id} missing required @name attribute"
                ),
            ));
        }
        let value = h.attr_or_empty("value");
        if value.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_HEADER_INCOMPLETE,
                format!(
                    "<q:header name={name}> on {element} of node {host_id} missing required @value \
                     attribute (a FEEL expression over the process context)"
                ),
            ));
        }
        out.push(HeaderAttr {
            name: name.to_string(),
            value: value.to_string(),
        });
    }
    Ok(out)
}

fn parse_reply(ext: &XmlElement, host_id: &str) -> Result<Option<ReplyBinding>, SutraError> {
    let Some(r) = ext.children_ns(Q_NS, "reply").next() else {
        return Ok(None);
    };
    let mode = parse_reply_mode(r.attr_or_empty("mode"), "q:reply", host_id)?;
    Ok(Some(ReplyBinding {
        mode,
        destination: opt_attr(r, "destination"),
        content_type: opt_attr(r, "contentType"),
        required: r.attr_or_empty("required") == "true",
        ce_type: opt_attr(r, "type"),
        ce_source: opt_attr(r, "source"),
        ce_subject: opt_attr(r, "subject"),
        ce_data_content_type: opt_attr(r, "datacontenttype"),
        auth: parse_auth_scheme(r.attr_or_empty("auth"))?,
        auth_secret_ref: opt_attr(r, "authSecretRef"),
        auth_header: opt_attr(r, "authHeader"),
        message_type: opt_attr(r, "messageType"),
        continue_after: r.attr_or_empty("continue") == "true",
        headers: parse_headers(r, host_id, "q:reply")?,
    }))
}

fn parse_send(ext: &XmlElement, host_id: &str) -> Result<Option<SendBinding>, SutraError> {
    let Some(s) = ext.children_ns(Q_NS, "send").next() else {
        return Ok(None);
    };
    let destination = s.attr_or_empty("destination").trim().to_string();
    let channel = s.attr_or_empty("channel").trim().to_string();
    let has_dest = !destination.is_empty();
    let has_channel = !channel.is_empty();
    if has_dest == has_channel {
        return Err(err(
            codes::PARSE_Q_SEND_CHANNEL_OR_DESTINATION,
            format!(
                "<q:send> on node {host_id} requires exactly one of @destination (an explicit \
                 endpoint URI) or @channel (a declared outbound channel); {}",
                if has_dest {
                    "both were set."
                } else {
                    "neither was set."
                }
            ),
        ));
    }
    let mode = parse_reply_mode(s.attr_or_empty("mode"), "q:send", host_id)?;
    Ok(Some(SendBinding {
        mode,
        destination: if has_dest { Some(destination) } else { None },
        channel: if has_channel { Some(channel) } else { None },
        content_type: opt_attr(s, "contentType"),
        ce_type: opt_attr(s, "type"),
        ce_source: opt_attr(s, "source"),
        ce_subject: opt_attr(s, "subject"),
        ce_data_content_type: opt_attr(s, "datacontenttype"),
        auth: parse_auth_scheme(s.attr_or_empty("auth"))?,
        auth_secret_ref: opt_attr(s, "authSecretRef"),
        auth_header: opt_attr(s, "authHeader"),
        message_type: opt_attr(s, "messageType"),
        headers: parse_headers(s, host_id, "q:send")?,
    }))
}

fn parse_per_node_audit(ext: &XmlElement) -> Result<Option<AuditBinding>, SutraError> {
    let Some(a) = ext.children_ns(Q_NS, "audit").next() else {
        return Ok(None);
    };
    let sink = a.attr_or_empty("sink");
    Ok(Some(AuditBinding {
        sink: if sink.trim().is_empty() {
            "sql".to_string()
        } else {
            sink.to_string()
        },
        target: opt_attr(a, "target"),
        capture: parse_audit_capture(a.attr_or_empty("capture"))?,
    }))
}

// ---- process-level intake inheritance -------------------------------------------

struct ProcessContract {
    complex_validators: Vec<String>,
    simple_validators: Vec<SimpleValidator>,
    redactors: Vec<String>,
    on_validation: Option<OnValidationBinding>,
    aliases: Vec<AliasBinding>,
}

impl ProcessContract {
    fn is_empty(&self) -> bool {
        self.complex_validators.is_empty()
            && self.simple_validators.is_empty()
            && self.redactors.is_empty()
            && self.on_validation.is_none()
            && self.aliases.is_empty()
    }
    fn has_validators(&self) -> bool {
        !self.complex_validators.is_empty() || !self.simple_validators.is_empty()
    }
    fn has_redactors(&self) -> bool {
        !self.redactors.is_empty()
    }
}

/// Copy each process's shared intake contract down into the intake `<q:source>` bindings
/// that omit it. Scoped per `<bpmn:process>`.
fn apply_process_level_contracts(
    root: &XmlElement,
    bindings: &mut HashMap<String, NodeBindings>,
) -> Result<(), SutraError> {
    for proc in root.collect_descendants_ns(BPMN_NS, "process") {
        let proc_id = proc.attr_or_empty("id");
        let Some(proc_ext) = proc.child_ns(BPMN_NS, "extensionElements") else {
            continue;
        };
        let mut complex = Vec::new();
        let mut simple = Vec::new();
        for container in proc_ext.children_ns(Q_NS, "validators") {
            parse_process_validators(container, &mut complex, &mut simple)?;
        }
        let mut redactors = Vec::new();
        for container in proc_ext.children_ns(Q_NS, "redactors") {
            parse_redactors(container, &mut redactors);
        }
        let contract = ProcessContract {
            complex_validators: complex,
            simple_validators: simple,
            redactors,
            on_validation: parse_on_validation(proc_ext, proc_id)?,
            aliases: parse_aliases(proc_ext, proc_id)?,
        };
        if contract.is_empty() {
            continue;
        }
        // Apply to every intake <q:source> in the process (host = source's grandparent node).
        let mut hosts = Vec::new();
        collect_source_hosts(proc, &mut hosts);
        for host_id in hosts {
            let Some(nb) = bindings.get(&host_id) else {
                continue;
            };
            if nb.sources.is_empty() {
                continue;
            }
            let merged = merge_contract(nb.clone(), &contract);
            bindings.insert(host_id, merged);
        }
    }
    Ok(())
}

/// Parse a process-level `<q:validators>` container (direct child of the process ext).
fn parse_process_validators(
    container: &XmlElement,
    complex: &mut Vec<String>,
    simple: &mut Vec<SimpleValidator>,
) -> Result<(), SutraError> {
    for cv in container.children_ns(Q_NS, "complexValidator") {
        let src = cv.attr_or_empty("source");
        if !src.trim().is_empty() {
            complex.push(src.to_string());
        }
    }
    for sv in container.children_ns(Q_NS, "simpleValidator") {
        let reference = sv.attr_or_empty("ref");
        let path = sv.attr_or_empty("path");
        if reference.trim().is_empty() || path.trim().is_empty() {
            return Err(err(
                codes::PARSE_Q_SIMPLE_VALIDATOR_INCOMPLETE,
                "<q:simpleValidator> in a process-level <q:validators> requires both @ref and \
                 @path.",
            ));
        }
        simple.push(SimpleValidator {
            reference: reference.to_string(),
            path: path.to_string(),
        });
    }
    Ok(())
}

/// The ids of the flow nodes hosting a `<q:source>` (node → extensionElements → q:source).
fn collect_source_hosts(el: &XmlElement, out: &mut Vec<String>) {
    for ext in el.children_ns(BPMN_NS, "extensionElements") {
        if ext.children_ns(Q_NS, "source").next().is_some() {
            let id = el.attr_or_empty("id");
            if !id.trim().is_empty() {
                out.push(id.to_string());
            }
        }
    }
    for child in &el.children {
        collect_source_hosts(child, out);
    }
}

/// Merge the process contract into one intake node's bindings.
fn merge_contract(nb: NodeBindings, contract: &ProcessContract) -> NodeBindings {
    let mut sources = nb.sources;
    let src = &mut sources[0];
    let source_has_validators =
        !src.complex_validators.is_empty() || !src.simple_validators.is_empty();
    if contract.has_validators() && !source_has_validators {
        src.complex_validators = contract.complex_validators.clone();
        src.simple_validators = contract.simple_validators.clone();
    }
    if contract.has_redactors() && src.redactors.is_empty() {
        src.redactors = contract.redactors.clone();
    }
    let on_validation = nb.on_validation.or_else(|| contract.on_validation.clone());
    // Union of process + node aliases; the node's alias wins on a name collision.
    let mut aliases = nb.aliases;
    for a in &contract.aliases {
        if !aliases.iter().any(|own| own.name == a.name) {
            aliases.push(a.clone());
        }
    }
    NodeBindings {
        sources,
        on_validation,
        dispatch: nb.dispatch,
        reply: nb.reply,
        send: nb.send,
        aliases,
        audit: nb.audit,
        timeout: nb.timeout,
        output: nb.output,
        retry: nb.retry,
    }
}

// ---- load-time validations ------------------------------------------------------

/// Fail-closed validation of a process's `<q:coverage>` paths against its OWN flow set.
fn validate_coverage_paths(
    process_id: &str,
    paths: Vec<CoveragePath>,
    flows: &[SequenceFlow],
) -> Result<Vec<CoveragePath>, SutraError> {
    if paths.is_empty() {
        return Ok(paths);
    }
    let by_id: HashMap<&str, &SequenceFlow> = flows.iter().map(|f| (f.id.as_str(), f)).collect();
    let mut seen_path_ids = HashSet::new();
    for p in &paths {
        if !seen_path_ids.insert(p.id.as_str()) {
            return Err(err(
                codes::CONFIG_COVERAGE_DUPLICATE_PATH,
                format!(
                    "<q:coverage path=\"{}\"> is declared more than once in process '{}'.",
                    p.id, process_id
                ),
            ));
        }
        if p.flows.is_empty() {
            return Err(err(
                codes::CONFIG_COVERAGE_INVALID_ROUTE,
                format!(
                    "<q:coverage path=\"{}\"> in process '{}' lists no flows.",
                    p.id, process_id
                ),
            ));
        }
        for fid in &p.flows {
            if !by_id.contains_key(fid.as_str()) {
                return Err(err(
                    codes::CONFIG_COVERAGE_UNKNOWN_FLOW,
                    format!(
                        "<q:coverage path=\"{}\"> in process '{}' references flow '{}', which is \
                         not a <bpmn:sequenceFlow> in the process.",
                        p.id, process_id, fid
                    ),
                ));
            }
        }
        for w in p.flows.windows(2) {
            let a = by_id[w[0].as_str()];
            let b = by_id[w[1].as_str()];
            if !crate::model::flows_contiguous(a, b) {
                return Err(err(
                    codes::CONFIG_COVERAGE_INVALID_ROUTE,
                    format!(
                        "<q:coverage path=\"{}\"> in process '{}' is not a contiguous route: flow \
                         '{}' ends at '{}' but the next flow '{}' starts at '{}'.",
                        p.id, process_id, a.id, a.target_ref, b.id, b.source_ref
                    ),
                ));
            }
        }
    }
    Ok(paths)
}

/// T4-2 — validate every `<q:variable source="channel">` feed-off against the intake channels
/// the process actually subscribes to.
fn validate_variable_sources(root: &XmlElement) -> Result<(), SutraError> {
    for process in root.collect_descendants_ns(BPMN_NS, "process") {
        let pid = process.attr_or_empty("id");
        let mut intake_channels = HashSet::new();
        for s in process.collect_descendants_ns(Q_NS, "source") {
            let ch = s.attr_or_empty("channel");
            if !ch.trim().is_empty() {
                intake_channels.insert(ch.to_string());
            }
        }
        for var in process.collect_descendants_ns(Q_NS, "variable") {
            let source = var.attr_or_empty("source");
            if source.trim().is_empty() {
                continue; // an in-instance variable — no intake link
            }
            if !intake_channels.contains(source) {
                return Err(err(
                    codes::CONFIG_BPMN_VARIABLE_SOURCE_UNKNOWN,
                    format!(
                        "<q:variable name=\"{}\" source=\"{source}\"> in process '{pid}' feeds \
                         off channel '{source}', but no intake node in the process subscribes to \
                         it (no <q:source channel=\"{source}\">) — the variable could never be \
                         initialized. Bind it to a channel a start event / message catch / \
                         userTask consumes, or drop @source if it is in-instance state.",
                        var.attr_or_empty("name")
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Fail closed on unresolvable throw targets (message-send-required, link
/// throw↔catch pairing, sendTask-send-required).
fn validate_throw_targets(
    process_id: &str,
    nodes: &[Node],
    process_bindings: &HashMap<String, NodeBindings>,
) -> Result<(), SutraError> {
    let mut link_catch_by_name: HashMap<&str, &str> = HashMap::new();
    for n in nodes {
        if let Node::LinkCatchEvent { id, link_name, .. } = n {
            if let Some(prior) = link_catch_by_name.insert(link_name, id) {
                return Err(err(
                    codes::PARSE_LINK_CATCH_DUPLICATE,
                    format!(
                        "Process {process_id} has more than one <intermediateCatchEvent> for \
                         link '{link_name}' ({id} and {prior})."
                    ),
                ));
            }
        }
    }
    for n in nodes {
        if let Node::IntermediateThrowEvent {
            id,
            kind,
            reference,
            ..
        } = n
        {
            match kind {
                ThrowKind::Message => {
                    let has_send = process_bindings
                        .get(id)
                        .map(|b| b.send.is_some())
                        .unwrap_or(false);
                    if !has_send {
                        return Err(err(
                            codes::PARSE_THROW_SEND_REQUIRED,
                            format!(
                                "<intermediateThrowEvent> {id} in process {process_id} is a \
                                 message throw but carries no <q:send> — a message throw must \
                                 declare what it emits."
                            ),
                        ));
                    }
                }
                ThrowKind::Link => {
                    let link_name = reference.as_deref().unwrap_or("");
                    if !link_catch_by_name.contains_key(link_name) {
                        return Err(err(
                            codes::PARSE_LINK_CATCH_NOT_FOUND,
                            format!(
                                "<intermediateThrowEvent> {id} in process {process_id} throws \
                                 link '{link_name}' but no <intermediateCatchEvent> in the \
                                 process catches it — the jump would dead-end."
                            ),
                        ));
                    }
                }
                _ => {}
            }
        }
    }
    for n in nodes {
        if let Node::SendTask { id, .. } = n {
            let has_send = process_bindings
                .get(id)
                .map(|b| b.send.is_some())
                .unwrap_or(false);
            if !has_send {
                return Err(err(
                    codes::PARSE_THROW_SEND_REQUIRED,
                    format!(
                        "<sendTask> {id} in process {process_id} carries no <q:send> — a send \
                         task must declare what it emits (a <q:send channel=…> or destination=…)."
                    ),
                ));
            }
        }
    }
    Ok(())
}

// ---- channel-call / timer load-time validation (Rust-only) -------------------------

/// The suffix appended to a channel-call task id to name its `<q:timeout>`-synthesized
/// timer boundary (`<taskId>#timeout`). `#` cannot appear in an XML NCName, so the
/// synthetic id can never collide with an authored node id.
pub const SYNTHETIC_TIMEOUT_SUFFIX: &str = "#timeout";

/// Synthesize an interrupting timer boundary for every channel-call service task carrying
/// a `<q:timeout duration>` binding (the attribute form).
fn synthesize_timeout_boundaries(
    nodes: &mut Vec<Node>,
    bindings_by_node_id: &HashMap<String, NodeBindings>,
) -> Result<(), SutraError> {
    let mut synthesized: Vec<Node> = Vec::new();
    for n in nodes.iter() {
        let Node::ServiceTask {
            id, implementation, ..
        } = n
        else {
            continue;
        };
        if !implementation.starts_with(CHANNEL_CALL_PREFIX) {
            continue;
        }
        let Some(timeout) = bindings_by_node_id.get(id).and_then(|b| b.timeout.as_ref()) else {
            continue;
        };
        synthesized.push(Node::BoundaryEvent {
            id: format!("{id}{SYNTHETIC_TIMEOUT_SUFFIX}"),
            name: None,
            attached_to_ref: id.clone(),
            kind: BoundaryKind::Timer,
            error_code: None,
            escalation_code: None,
            interrupting: true,
            timer: Some(crate::timer::TimerDefinition::Duration(
                timeout.duration.clone(),
            )),
        });
    }
    nodes.extend(synthesized);
    Ok(())
}

/// Fail closed at load time on:
/// - a channel-call task without a timer boundary (BPMN or `<q:timeout>`-synthesized),
/// - a channel-call task without a declared `<q:alias>` (the park key),
/// - a channel-call task wrapped in loop characteristics (cannot park mid-iteration),
/// - a timer boundary attached to a host that can never be parked when it fires,
/// - a `<q:timeout>` on anything but a channel-call service task,
/// - a `<q:retry>` on anything but a service task, or on a channel-call task whose timer
///   boundary carries outgoing flows (the modelled route would shadow the policy — see the
///   placement block below).
fn validate_channel_calls_and_timers(
    process_id: &str,
    nodes: &[Node],
    flows: &[SequenceFlow],
    process_bindings: &HashMap<String, NodeBindings>,
) -> Result<(), SutraError> {
    let is_channel_call = |node_id: &str| -> bool {
        nodes.iter().any(|n| {
            matches!(n, Node::ServiceTask { id, implementation, .. }
                if id == node_id && implementation.starts_with(CHANNEL_CALL_PREFIX))
        })
    };
    for n in nodes {
        match n {
            Node::ServiceTask {
                id, implementation, ..
            } if implementation.starts_with(CHANNEL_CALL_PREFIX) => {
                let has_timer_boundary = nodes.iter().any(|b| {
                    matches!(b, Node::BoundaryEvent { kind: BoundaryKind::Timer, attached_to_ref, .. }
                        if attached_to_ref == id)
                });
                if !has_timer_boundary {
                    return Err(err(
                        codes::DISPATCH_CHANNEL_CALL_TIMEOUT_REQUIRED,
                        format!(
                            "<serviceTask> '{id}' in process '{process_id}' is a channel-call \
                             task (implementation=\"{implementation}\") but declares neither a \
                             timer boundary event nor a <q:timeout duration=…>; every \
                             channel-call task REQUIRES one so a lost response \
                             can never park an instance forever."
                        ),
                    ));
                }
                let has_alias = process_bindings
                    .get(id)
                    .map(|b| !b.aliases.is_empty())
                    .unwrap_or(false);
                if !has_alias {
                    return Err(err(
                        codes::DISPATCH_CHANNEL_CALL_ALIAS_REQUIRED,
                        format!(
                            "<serviceTask> '{id}' in process '{process_id}' is a channel-call \
                             task but declares no <q:alias>; the park is keyed by a DECLARED \
                             correlation alias — without one the \
                             correlated response could never resume the instance."
                        ),
                    ));
                }
            }
            Node::MultiInstance { id, inner, .. } | Node::StandardLoop { id, inner, .. } => {
                if let Node::ServiceTask {
                    id: inner_id,
                    implementation,
                    ..
                } = inner.as_ref()
                {
                    if implementation.starts_with(CHANNEL_CALL_PREFIX) {
                        return Err(err(
                            codes::DISPATCH_TIMER_UNSUPPORTED,
                            format!(
                                "Activity '{id}' in process '{process_id}' wraps a \
                                 channel-call task in loop characteristics; a channel-call \
                                 cannot park a durable wait state mid-iteration."
                            ),
                        ));
                    }
                    // Same reason, different wait: a `<q:retry>` park is a durable TIMER, and
                    // a loop iteration is not a token position the engine can re-enter.
                    if process_bindings
                        .get(inner_id)
                        .map(|b| b.retry.is_some())
                        .unwrap_or(false)
                    {
                        return Err(err(
                            codes::CONFIG_BPMN_RETRY_NOT_APPLICABLE,
                            format!(
                                "Activity '{id}' in process '{process_id}' wraps service task \
                                 '{inner_id}', which declares <q:retry>, in loop \
                                 characteristics; a retry parks a durable timer and the engine \
                                 cannot re-enter a single loop iteration. Move the retry policy \
                                 to an un-looped task."
                            ),
                        ));
                    }
                }
            }
            Node::BoundaryEvent {
                id,
                kind: BoundaryKind::Timer,
                attached_to_ref,
                ..
            } => {
                let host_is_wait_capable = is_channel_call(attached_to_ref)
                    || nodes
                        .iter()
                        .any(|h| matches!(h, Node::UserTask { id, .. } if id == attached_to_ref));
                if !host_is_wait_capable {
                    return Err(err(
                        codes::DISPATCH_TIMER_UNSUPPORTED,
                        format!(
                            "<boundaryEvent> '{id}' in process '{process_id}' is a timer \
                             boundary attached to '{attached_to_ref}', which is not a \
                             wait-capable activity; a timer boundary is supported on a \
                             channel-call service task or a userTask (the activities that \
                             PARK — a synchronous activity can never be interrupted by a \
                             timer in this engine)."
                        ),
                    ));
                }
            }
            _ => {}
        }
    }
    // `<q:retry>` placement: a `<serviceTask>` — registered-task OR channel-call — can honour
    // it; anything else is a structured load ERROR, never a silent no-op. The
    // registered-task-vs-template/decision distinction is a RUNTIME registry question the loader
    // cannot answer, so for non-channel-call tasks the load gate is the syntactic one — the node
    // kind — and the executor's failure arm applies the policy only to an uncaught
    // registered-task failure.
    //
    // On a CHANNEL-CALL task (F1 — retry reachability) the policy governs the task-level
    // failure set: the route-less timeout firing, and a terminally-poisoned request delivery.
    // One extra shape rule applies there: every timer boundary on the task must be the
    // ROUTE-LESS (`<q:timeout>`) form. A timer boundary WITH outgoing flows is an AUTHORED
    // timeout outcome, and a modelled outcome always wins over a retry policy (exactly as a
    // BPMN error does on a registered task) — so combining the two would leave the retry
    // policy structurally unable to ever fire on a timeout. That near-inert combination is
    // refused fail-closed rather than loaded silently.
    for (node_id, b) in process_bindings {
        if b.retry.is_none() {
            continue;
        }
        // Bindings are document-scanned; only flag nodes that live in THIS container.
        let Some(node) = nodes.iter().find(|n| n.id() == node_id) else {
            continue;
        };
        let reason = match node {
            Node::ServiceTask { implementation, .. }
                if implementation.starts_with(CHANNEL_CALL_PREFIX) =>
            {
                let routed_boundary = nodes.iter().find_map(|bnode| match bnode {
                    Node::BoundaryEvent {
                        id: boundary_id,
                        kind: BoundaryKind::Timer,
                        attached_to_ref,
                        ..
                    } if attached_to_ref == node_id
                        && flows.iter().any(|f| &f.source_ref == boundary_id) =>
                    {
                        Some(boundary_id.clone())
                    }
                    _ => None,
                });
                routed_boundary.map(|boundary_id| {
                    format!(
                        "a channel-call task whose timer boundary '{boundary_id}' has outgoing \
                         flows (a MODELLED timeout route); a modelled outcome always wins over \
                         a retry policy, so <q:retry> could never fire on a timeout here. \
                         Declare the route-less <q:timeout duration=…> form instead, or drop \
                         the retry policy and keep the modelled route"
                    )
                })
            }
            Node::ServiceTask { .. } => None,
            _ => Some(
                "not a <serviceTask>; only a service task has an invocation to repeat — a \
                 registered task function to re-run, or a channel-call request to re-emit"
                    .to_string(),
            ),
        };
        if let Some(reason) = reason {
            return Err(err(
                codes::CONFIG_BPMN_RETRY_NOT_APPLICABLE,
                format!(
                    "<q:retry> on node '{node_id}' in process '{process_id}' is not applicable: \
                     the node is {reason}."
                ),
            ));
        }
    }
    for (node_id, b) in process_bindings {
        if b.timeout.is_some() && !is_channel_call(node_id) {
            // Only flag nodes that exist in THIS container (bindings are doc-scanned).
            if nodes.iter().any(|n| n.id() == node_id) {
                return Err(err(
                    codes::DISPATCH_TIMER_UNSUPPORTED,
                    format!(
                        "<q:timeout> on node '{node_id}' in process '{process_id}' is only \
                         valid on a channel-call service task \
                         (implementation=\"channel:<name>\"); use a BPMN timer boundary / \
                         timer catch event elsewhere."
                    ),
                ));
            }
        }
    }
    Ok(())
}

// ---- attribute / enum helpers ---------------------------------------------------

fn extract_error_code(
    event: &XmlElement,
    error_code_by_id: &HashMap<String, String>,
) -> Option<String> {
    for d in event.children_ns(BPMN_NS, "errorEventDefinition") {
        let raw = d.attr_or_empty("errorRef");
        if raw.trim().is_empty() {
            continue;
        }
        let (_, local) = d.resolve_qname(raw);
        if let Some(code) = error_code_by_id.get(&local) {
            if !code.trim().is_empty() {
                return Some(code.clone());
            }
        }
        if !local.trim().is_empty() {
            return Some(local);
        }
    }
    None
}

/// Resolve a `signalRef` / `escalationRef` / `errorRef` QName to the indexed code/name,
/// falling back to the raw local part (None when the ref is absent/blank).
fn resolve_ref(el: &XmlElement, raw: &str, by_id: &HashMap<String, String>) -> Option<String> {
    if raw.trim().is_empty() {
        return None;
    }
    let (_, local) = el.resolve_qname(raw);
    if local.trim().is_empty() {
        return None;
    }
    match by_id.get(&local) {
        Some(resolved) if !resolved.trim().is_empty() => Some(resolved.clone()),
        _ => Some(local),
    }
}

fn has_event_definition(event: &XmlElement, local: &str) -> bool {
    event.children_ns(BPMN_NS, local).next().is_some()
}

fn parse_ack_mode(s: &str) -> Result<AckMode, SutraError> {
    match s {
        "" | "on-persist" => Ok(AckMode::OnPersist),
        "on-complete" => Ok(AckMode::OnComplete),
        _ => Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("Unknown q:source ack mode: {s}"),
        )),
    }
}

fn parse_data_class(s: &str) -> Result<DataClass, SutraError> {
    match s {
        "" | "none" => Ok(DataClass::None),
        "pii" => Ok(DataClass::Pii),
        "pci" => Ok(DataClass::Pci),
        "phi" => Ok(DataClass::Phi),
        "financial" => Ok(DataClass::Financial),
        _ => Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("Unknown q:source dataClass: {s}"),
        )),
    }
}

fn parse_on_no_match(s: &str) -> Result<OnNoMatch, SutraError> {
    match s {
        "" | "error" => Ok(OnNoMatch::Error),
        "skip" => Ok(OnNoMatch::Skip),
        _ => Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("Unknown q:dispatch onNoMatch: {s}"),
        )),
    }
}

fn parse_alias_conflict(s: &str) -> Result<Option<AliasConflict>, SutraError> {
    match s {
        "" => Ok(None),
        "reject" => Ok(Some(AliasConflict::Reject)),
        "correlate" => Ok(Some(AliasConflict::Correlate)),
        _ => Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("Unknown q:alias onConflict: {s}"),
        )),
    }
}

fn parse_audit_capture(s: &str) -> Result<AuditCapture, SutraError> {
    match s {
        "" | "payload" => Ok(AuditCapture::Payload),
        "none" => Ok(AuditCapture::None),
        "metadata" => Ok(AuditCapture::Metadata),
        _ => Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("Unknown q:audit capture: {s}"),
        )),
    }
}

fn parse_auth_scheme(s: &str) -> Result<Option<OutboundAuthScheme>, SutraError> {
    match s {
        "" => Ok(None),
        "mtls" => Ok(Some(OutboundAuthScheme::Mtls)),
        "bearer" => Ok(Some(OutboundAuthScheme::Bearer)),
        "apikey" => Ok(Some(OutboundAuthScheme::Apikey)),
        _ => Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("Unknown q:reply auth scheme: {s}"),
        )),
    }
}

fn parse_reply_mode(s: &str, element: &str, host_id: &str) -> Result<ReplyMode, SutraError> {
    match s {
        "" | "native" => Ok(ReplyMode::Native),
        "cloudevent-binary" => Ok(ReplyMode::CloudeventBinary),
        "cloudevent-structured" => Ok(ReplyMode::CloudeventStructured),
        "match-inbound" => Ok(ReplyMode::MatchInbound),
        _ => Err(err(
            codes::PARSE_Q_REPLY_INVALID_MODE,
            format!(
                "<{element} mode={s}> on node {host_id} is not one of native / \
                 cloudevent-binary / cloudevent-structured / match-inbound"
            ),
        )),
    }
}

fn scalar_type(t: &str) -> FieldType {
    match t.trim().to_lowercase().as_str() {
        "string" => FieldType::String,
        "number" => FieldType::Number,
        "boolean" => FieldType::Boolean,
        _ => FieldType::Any,
    }
}

/// A non-trimmed attribute value as Option, empty when absent/blank.
fn opt_attr(el: &XmlElement, name: &str) -> Option<String> {
    let v = el.attr_or_empty(name);
    if v.trim().is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

/// A trimmed attribute value as Option, empty when absent/blank.
fn attr_trimmed(el: &XmlElement, name: &str) -> Option<String> {
    let v = el.attr_or_empty(name).trim();
    if v.is_empty() {
        None
    } else {
        Some(v.to_string())
    }
}

fn trim_to_none(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

fn non_blank(s: &str) -> Option<String> {
    if s.trim().is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn required(value: &str, element: &str, attr: &str) -> Result<String, SutraError> {
    if value.trim().is_empty() {
        return Err(err(
            codes::PARSE_QXSD_INVALID_SOURCE,
            format!("<{element}> missing required @{attr}"),
        ));
    }
    Ok(value.to_string())
}

fn err(code: &str, message: impl Into<String>) -> SutraError {
    SutraError::new(code, message)
}
