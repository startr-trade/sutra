//! Per-deployment OpenAPI 3.1 surface generator.
//!
//! # Why this is generated, not committed
//!
//! The **platform** system-API spec (`openapi/platform.yaml`) is one fixed route set — it is
//! committed and drift-gated against the engine's `PLATFORM_ROUTES` table. A **deployment's**
//! API surface is different: it is derived entirely from *that archive's* manifest — the
//! channels it declares, the BPMN processes each inbound channel can reach, the inbound and
//! outbound message types, the nature of each endpoint (sync request/reply vs async
//! messaging), and the data-stores it uses. There is one such surface *per `deploymentId`*, so
//! it cannot live as a file in `openapi/`.
//!
//! Instead the engine **generates** it from the already-parsed deployment plan and serves it
//! live at `GET /sutra/deployments/{id}/openapi`; the `sutra openapi <archive>` CLI emits the
//! same document offline. Because the spec and the live routing derive from the *same* parsed
//! inputs, drift between the two is structurally impossible — there is nothing to gate, only a
//! golden-regeneration test that guards the generator itself.
//!
//! # Fidelity (this landing)
//!
//! The structural surface is complete: every HTTP channel becomes a path item; non-HTTP
//! (broker) and outbound channels are described under `x-sutra-*` vendor extensions (they are
//! not REST endpoints); each channel names its reachable processes, message types, endpoint
//! nature, and the deployment's data-stores are inventoried. Request/response **bodies** are
//! represented as generic objects tagged with the channel's codec (`x-sutra-codec`) — a full
//! codec→JSON-Schema projection is a tracked follow-on, not required to describe the surface.
#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{json, Map, Value};
use sutra_bpmn::model::{Node, ProcessModule};
use sutra_channels::ChannelDefinition;
use sutra_datastore::StoreDefinition;

/// The `openapi:` version string every generated document carries.
pub const OPENAPI_VERSION: &str = "3.1.0";

/// The parsed, per-deployment inputs the generator projects into an OpenAPI 3.1 document. All
/// borrowed — the caller (the engine's activation path, or the `sutra openapi` CLI) owns the plan.
pub struct DeploymentApi<'a> {
    /// The content-hash deployment id (`dep-<hex>`).
    pub deployment_id: &'a str,
    pub tenant: &'a str,
    pub module: &'a str,
    pub version: &'a str,
    /// Every channel/binding declared for this deployment — inbound *and* outbound.
    pub channels: &'a [ChannelDefinition],
    /// The deployment's BPMN modules (the channel→process reachability graph).
    pub modules: &'a [Arc<ProcessModule>],
    /// The declared data-stores (parsed `datastores.yaml`); empty when the deployment declares none.
    pub stores: &'a [StoreDefinition],
}

/// Project a deployment's parsed manifest into an OpenAPI 3.1 document (a `serde_json::Value`).
/// Deterministic: array order follows the caller's channel order and id-sorted process/start-event
/// order; object keys are sorted by `serde_json`'s `BTreeMap`-backed `Map`.
pub fn deployment_spec(api: &DeploymentApi) -> Value {
    let process_outbound = collect_process_outbound(api.modules);

    let mut paths = Map::new();
    let mut messaging = Vec::new();
    let mut outbound = Vec::new();

    for def in api.channels {
        if def.is_outbound() {
            outbound.push(outbound_entry(def, &process_outbound));
            continue;
        }
        let reach = reachable_processes(api.modules, &channel_name(def), &process_outbound);
        match def.transport.as_deref() {
            Some("http") => {
                let (method, path) = def.bind_method_and_path();
                let item = paths
                    .entry(path)
                    .or_insert_with(|| Value::Object(Map::new()));
                if let Value::Object(m) = item {
                    m.insert(method.to_lowercase(), inbound_operation(def, &reach));
                }
            }
            // Broker (and transport-less internal) inbound channels are not REST endpoints —
            // they are queue/topic subscriptions, described as messaging bindings.
            _ => messaging.push(messaging_entry(def, &reach)),
        }
    }

    let mut root = Map::new();
    root.insert("openapi".into(), json!(OPENAPI_VERSION));
    root.insert(
        "info".into(),
        json!({
            "title": format!("Sutra deployment — {}/{}/{}", api.tenant, api.module, api.version),
            "version": api.version,
            "description": info_description(api),
            "x-sutra-deployment-id": api.deployment_id,
            "x-sutra-tenant": api.tenant,
            "x-sutra-module": api.module,
        }),
    );
    root.insert(
        "servers".into(),
        json!([{
            "url": "http://{host}:{port}",
            "description": "The engine's HTTP port (channel endpoints share the engine port; dynamic, 8080 in the k8s image).",
            "variables": { "host": { "default": "localhost" }, "port": { "default": "8080" } }
        }]),
    );
    root.insert("paths".into(), Value::Object(paths));
    if !messaging.is_empty() {
        root.insert("x-sutra-messaging".into(), Value::Array(messaging));
    }
    if !outbound.is_empty() {
        root.insert("x-sutra-outbound".into(), Value::Array(outbound));
    }
    root.insert(
        "x-sutra-datastores".into(),
        datastores_section(api.stores, api.modules),
    );

    Value::Object(root)
}

/// Render the spec as pretty-printed JSON.
pub fn render_json(spec: &Value) -> String {
    serde_json::to_string_pretty(spec).expect("openapi value serializes to json")
}

/// Render the spec as YAML. The value is round-tripped through its own JSON string so
/// `serde_json`'s `arbitrary_precision` Number encoding never leaks into the YAML output.
pub fn render_yaml(spec: &Value) -> String {
    let json = serde_json::to_string(spec).expect("openapi value serializes to json");
    let yaml_value: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&json).expect("json is valid yaml input");
    serde_yaml_ng::to_string(&yaml_value).expect("yaml value serializes")
}

// ---------------------------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------------------------

fn channel_name(def: &ChannelDefinition) -> String {
    def.binding.channel_name.clone()
}

/// `sync-request-reply` (http + on-complete) / `async-ack` (http + on-persist) /
/// `async-messaging` (a broker transport) / `internal` (no transport).
fn endpoint_nature(def: &ChannelDefinition) -> &'static str {
    match def.transport.as_deref() {
        Some("http") => {
            if def.effective_ack_mode() == "on-complete" {
                "sync-request-reply"
            } else {
                "async-ack"
            }
        }
        Some(_) => "async-messaging",
        None => "internal",
    }
}

/// One outbound emission (`<q:reply>` or `<q:send>`) declared on a process node.
#[derive(Clone)]
struct OutMsg {
    via: &'static str, // "reply" | "send"
    message_type: Option<String>,
    content_type: Option<String>,
    mode: &'static str,
    channel: Option<String>,
    destination: Option<String>,
}

impl OutMsg {
    fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("via".into(), json!(self.via));
        m.insert("mode".into(), json!(self.mode));
        if let Some(v) = &self.message_type {
            m.insert("messageType".into(), json!(v));
        }
        if let Some(v) = &self.content_type {
            m.insert("contentType".into(), json!(v));
        }
        if let Some(v) = &self.channel {
            m.insert("channel".into(), json!(v));
        }
        if let Some(v) = &self.destination {
            m.insert("destination".into(), json!(v));
        }
        Value::Object(m)
    }
}

fn reply_mode(mode: sutra_bpmn::qbindings::ReplyMode) -> &'static str {
    use sutra_bpmn::qbindings::ReplyMode::*;
    match mode {
        Native => "native",
        CloudeventBinary => "cloudevent-binary",
        CloudeventStructured => "cloudevent-structured",
        MatchInbound => "match-inbound",
    }
}

/// Every outbound emission declared per process, keyed by process id (id-sorted via `BTreeMap`).
fn collect_process_outbound(modules: &[Arc<ProcessModule>]) -> BTreeMap<String, Vec<OutMsg>> {
    let mut out: BTreeMap<String, Vec<OutMsg>> = BTreeMap::new();
    for proc in modules.iter().flat_map(|m| m.processes()) {
        let mut emissions = Vec::new();
        for node in proc.nodes() {
            let b = proc.bindings_for(node.id());
            if let Some(r) = &b.reply {
                emissions.push(OutMsg {
                    via: "reply",
                    message_type: r.message_type.clone(),
                    content_type: r.content_type.clone(),
                    mode: reply_mode(r.mode),
                    channel: None,
                    destination: r.destination.clone(),
                });
            }
            if let Some(s) = &b.send {
                emissions.push(OutMsg {
                    via: "send",
                    message_type: s.message_type.clone(),
                    content_type: s.content_type.clone(),
                    mode: reply_mode(s.mode),
                    channel: s.channel.clone(),
                    destination: s.destination.clone(),
                });
            }
        }
        // A process id may recur across modules only as a load-time error; last-writer is fine.
        out.insert(proc.id.clone(), emissions);
    }
    out
}

/// One process reachable from an inbound channel, with the emissions it can produce.
struct Reach {
    process_id: String,
    start_event_id: String,
    message_type_value: Option<String>,
    message_type_pattern: Option<String>,
    payload_var: String,
    emits: Vec<OutMsg>,
}

/// The `(process, start-event)` pairs an inbound `channel` can start, id-sorted + deduplicated.
fn reachable_processes(
    modules: &[Arc<ProcessModule>],
    channel: &str,
    process_outbound: &BTreeMap<String, Vec<OutMsg>>,
) -> Vec<Reach> {
    // BTreeMap key = (process_id, start_event_id) gives deterministic order + dedup.
    let mut acc: BTreeMap<(String, String), Reach> = BTreeMap::new();
    for proc in modules.iter().flat_map(|m| m.processes()) {
        for start in proc.start_events() {
            let Node::StartEvent { id, channels, .. } = start else {
                continue;
            };
            if !channels.iter().any(|c| c == channel) {
                continue;
            }
            let src = proc.bindings_for(id).source();
            acc.insert(
                (proc.id.clone(), id.clone()),
                Reach {
                    process_id: proc.id.clone(),
                    start_event_id: id.clone(),
                    message_type_value: src.and_then(|s| s.message_type_value.clone()),
                    message_type_pattern: src.and_then(|s| s.message_type_pattern.clone()),
                    payload_var: src
                        .map(|s| s.name.clone())
                        .unwrap_or_else(|| "payload".into()),
                    emits: process_outbound.get(&proc.id).cloned().unwrap_or_default(),
                },
            );
        }
    }
    acc.into_values().collect()
}

fn reach_to_json(r: &Reach) -> Value {
    let mut m = Map::new();
    m.insert("processId".into(), json!(r.process_id));
    m.insert("startEventId".into(), json!(r.start_event_id));
    m.insert("payloadVar".into(), json!(r.payload_var));
    if let Some(v) = &r.message_type_value {
        m.insert("messageTypeValue".into(), json!(v));
    }
    if let Some(v) = &r.message_type_pattern {
        m.insert("messageTypePattern".into(), json!(v));
    }
    if !r.emits.is_empty() {
        m.insert(
            "emits".into(),
            Value::Array(r.emits.iter().map(OutMsg::to_json).collect()),
        );
    }
    Value::Object(m)
}

/// The message-type tags an inbound channel accepts, deduped + sorted; `["*"]` when a reachable
/// start event declares no message-type filter (a catch-all).
fn inbound_message_types(reach: &[Reach]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for r in reach {
        match (&r.message_type_value, &r.message_type_pattern) {
            (Some(v), _) => {
                set.insert(v.clone());
            }
            (None, Some(p)) => {
                set.insert(format!("~{p}"));
            }
            (None, None) => {
                set.insert("*".to_string());
            }
        }
    }
    set.into_iter().collect()
}

/// The distinct outbound message-type tags reachable from an inbound channel (across its
/// processes' replies + sends), deduped + sorted.
fn outbound_message_types(reach: &[Reach]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for r in reach {
        for e in &r.emits {
            if let Some(mt) = &e.message_type {
                set.insert(mt.clone());
            }
        }
    }
    set.into_iter().collect()
}

fn inbound_operation(def: &ChannelDefinition, reach: &[Reach]) -> Value {
    let nature = endpoint_nature(def);
    let sync = nature == "sync-request-reply";
    let name = channel_name(def);
    let codec = def.codec.clone().unwrap_or_default();

    let mut op = Map::new();
    op.insert(
        "operationId".into(),
        json!(format!("channel_{}", sanitize(&name))),
    );
    op.insert(
        "summary".into(),
        json!(format!(
            "Inbound channel '{}' → {} process(es)",
            name,
            reach.len()
        )),
    );
    op.insert("description".into(), json!(inbound_description(def, reach)));
    op.insert("x-sutra-channel".into(), json!(name));
    op.insert(
        "x-sutra-transport".into(),
        json!(def.transport.clone().unwrap_or_else(|| "none".into())),
    );
    op.insert("x-sutra-nature".into(), json!(nature));
    op.insert("x-sutra-ack-mode".into(), json!(def.effective_ack_mode()));
    op.insert("x-sutra-broadcast".into(), json!(def.binding.broadcast));
    op.insert("x-sutra-singleton".into(), json!(def.singleton()));
    if let Some(cap) = def.binding.max_concurrent_instances {
        op.insert("x-sutra-concurrency-cap".into(), json!(cap));
    }
    op.insert(
        "x-sutra-codec".into(),
        json!(if codec.is_empty() {
            "none (schema-less; content-type-driven format)".to_string()
        } else {
            codec.clone()
        }),
    );
    if let Some(ce) = &def.cloud_events_mode {
        op.insert("x-sutra-cloud-events".into(), json!(ce));
    }
    if let Some(auth) = &def.auth_scheme {
        op.insert("x-sutra-auth-scheme".into(), json!(auth));
    }
    op.insert(
        "x-sutra-reachable-processes".into(),
        Value::Array(reach.iter().map(reach_to_json).collect()),
    );
    op.insert(
        "x-sutra-inbound-message-types".into(),
        json!(inbound_message_types(reach)),
    );
    let out_types = outbound_message_types(reach);
    if !out_types.is_empty() {
        op.insert("x-sutra-outbound-message-types".into(), json!(out_types));
    }
    op.insert("requestBody".into(), request_body(&codec));
    op.insert("responses".into(), responses(sync, def));

    Value::Object(op)
}

fn request_body(codec: &str) -> Value {
    let note = if codec.is_empty() {
        "Schema-less: the body is decoded by Content-Type (JSON / XML / YAML interchangeable), raw bytes stay accessible."
    } else {
        "Decoded by the channel codec. For a data-format codec (json/xml/yaml) the body honors Content-Type negotiation; a schema-backed codec takes full control."
    };
    json!({
        "required": true,
        "description": note,
        "content": {
            "application/json": {
                "schema": { "type": "object", "x-sutra-codec": if codec.is_empty() { "none" } else { codec } }
            }
        }
    })
}

fn responses(sync: bool, def: &ChannelDefinition) -> Value {
    let mut r = Map::new();
    if sync {
        r.insert(
            "200".into(),
            json!({
                "description": "The synchronous process reply (the caller waits for completion).",
                "content": { "application/json": { "schema": { "type": "object" } } }
            }),
        );
    } else {
        r.insert(
            "202".into(),
            json!({ "description": "Accepted — the message is persisted/acked; any response is emitted asynchronously on an outbound channel." }),
        );
    }
    if def.binding.max_concurrent_instances.is_some() {
        r.insert(
            "429".into(),
            json!({
                "description": "The channel's concurrency cap is reached.",
                "content": { "application/problem+json": { "schema": { "type": "object" } } }
            }),
        );
    }
    r.insert(
        "default".into(),
        json!({
            "description": "A decode / validation / dispatch failure (RFC 7807 problem detail).",
            "content": { "application/problem+json": { "schema": { "type": "object" } } }
        }),
    );
    Value::Object(r)
}

fn messaging_entry(def: &ChannelDefinition, reach: &[Reach]) -> Value {
    let name = channel_name(def);
    let mut m = Map::new();
    m.insert("channel".into(), json!(name));
    m.insert(
        "transport".into(),
        json!(def.transport.clone().unwrap_or_else(|| "none".into())),
    );
    m.insert("nature".into(), json!(endpoint_nature(def)));
    m.insert("ackMode".into(), json!(def.effective_ack_mode()));
    m.insert("broadcast".into(), json!(def.binding.broadcast));
    m.insert("singleton".into(), json!(def.singleton()));
    if let Some(cap) = def.binding.max_concurrent_instances {
        m.insert("concurrencyCap".into(), json!(cap));
    }
    // The broker queue/topic name (transport-specific) lives in the flattened properties bag.
    if let Some(q) = def
        .properties
        .get("queue")
        .or_else(|| def.properties.get("topic"))
        .or_else(|| def.properties.get("subscription"))
    {
        m.insert("queueOrTopic".into(), json!(q));
    }
    if !def.codec.clone().unwrap_or_default().is_empty() {
        m.insert("codec".into(), json!(def.codec.clone().unwrap()));
    }
    m.insert(
        "inboundMessageTypes".into(),
        json!(inbound_message_types(reach)),
    );
    m.insert(
        "reachableProcesses".into(),
        Value::Array(reach.iter().map(reach_to_json).collect()),
    );
    Value::Object(m)
}

fn outbound_entry(
    def: &ChannelDefinition,
    process_outbound: &BTreeMap<String, Vec<OutMsg>>,
) -> Value {
    let name = channel_name(def);
    // Outbound message types = the `<q:send channel="name">` emissions across every process.
    let mut types = BTreeSet::new();
    for emissions in process_outbound.values() {
        for e in emissions {
            if e.channel.as_deref() == Some(name.as_str()) {
                if let Some(mt) = &e.message_type {
                    types.insert(mt.clone());
                }
            }
        }
    }
    let mut m = Map::new();
    m.insert("channel".into(), json!(name));
    m.insert(
        "transport".into(),
        json!(def.transport.clone().unwrap_or_else(|| "none".into())),
    );
    m.insert("direction".into(), json!("outbound"));
    if let Some(dest) = &def.bind_spec {
        m.insert("destination".into(), json!(dest));
    }
    if !def.codec.clone().unwrap_or_default().is_empty() {
        m.insert("codec".into(), json!(def.codec.clone().unwrap()));
    }
    if !types.is_empty() {
        m.insert(
            "messageTypes".into(),
            json!(types.into_iter().collect::<Vec<_>>()),
        );
    }
    Value::Object(m)
}

/// The data-store inventory: declared stores (`datastores.yaml`) merged with the stores the
/// deployment's processes actually read/write via `<q:store>` data tasks.
fn datastores_section(stores: &[StoreDefinition], modules: &[Arc<ProcessModule>]) -> Value {
    // store name -> (reads, writes, for_update, expect_unchanged, key_exprs, processes)
    #[derive(Default)]
    struct Ops {
        reads: usize,
        writes: usize,
        for_update: bool,
        expect_unchanged: bool,
        key_exprs: BTreeSet<String>,
        processes: BTreeSet<String>,
    }
    let mut refs: BTreeMap<String, Ops> = BTreeMap::new();
    for proc in modules.iter().flat_map(|m| m.processes()) {
        for node in proc.nodes() {
            let dm = match node {
                Node::ServiceTask { data_mapping, .. } | Node::DataTask { data_mapping, .. } => {
                    data_mapping
                }
                _ => continue,
            };
            for rd in &dm.store_reads {
                let e = refs.entry(rd.store.clone()).or_default();
                e.reads += 1;
                e.for_update |= rd.for_update;
                e.key_exprs.insert(rd.key_expression.clone());
                e.processes.insert(proc.id.clone());
            }
            for wr in &dm.store_writes {
                let e = refs.entry(wr.store.clone()).or_default();
                e.writes += 1;
                e.expect_unchanged |= wr.expect_unchanged;
                e.key_exprs.insert(wr.key_expression.clone());
                e.processes.insert(proc.id.clone());
            }
        }
    }

    let declared: BTreeMap<&str, &StoreDefinition> =
        stores.iter().map(|s| (s.name.as_str(), s)).collect();
    let mut names: BTreeSet<String> = BTreeSet::new();
    names.extend(declared.keys().map(|s| s.to_string()));
    names.extend(refs.keys().cloned());

    let mut out = Vec::new();
    for name in names {
        let mut m = Map::new();
        m.insert("name".into(), json!(name));
        match declared.get(name.as_str()) {
            Some(def) => {
                m.insert("declared".into(), json!(true));
                m.insert("type".into(), json!(def.store_type));
                if let Some(dc) = def.properties.get("dataClass") {
                    m.insert("dataClass".into(), json!(dc));
                }
                // The connection is an env-ref (`<key>-ref`) — surface the ref name, never resolve
                // a secret at doc-generation time.
                if let Some(url_ref) = def.properties.get("sql.url-ref") {
                    m.insert("connectionRef".into(), json!(url_ref));
                }
            }
            None => {
                m.insert("declared".into(), json!(false));
            }
        }
        if let Some(ops) = refs.get(&name) {
            let mut mode = Vec::new();
            if ops.reads > 0 {
                mode.push("read");
            }
            if ops.writes > 0 {
                mode.push("write");
            }
            m.insert("access".into(), json!(mode));
            m.insert("readOps".into(), json!(ops.reads));
            m.insert("writeOps".into(), json!(ops.writes));
            if ops.for_update {
                m.insert("selectForUpdate".into(), json!(true));
            }
            if ops.expect_unchanged {
                m.insert("optimisticConcurrency".into(), json!(true));
            }
            m.insert(
                "keyExpressions".into(),
                json!(ops.key_exprs.iter().cloned().collect::<Vec<_>>()),
            );
            m.insert(
                "accessedBy".into(),
                json!(ops.processes.iter().cloned().collect::<Vec<_>>()),
            );
        }
        out.push(Value::Object(m));
    }
    Value::Array(out)
}

fn info_description(api: &DeploymentApi) -> String {
    format!(
        "Generated API surface for deployment `{}` ({}/{}/{}). Derived from the archive manifest: \
         HTTP channels are path items; broker/outbound channels are described under `x-sutra-*` \
         vendor extensions; `x-sutra-datastores` inventories the deployment's data-stores. This \
         document is generated live from the same parsed plan that drives routing — it never drifts.",
        api.deployment_id, api.tenant, api.module, api.version
    )
}

fn inbound_description(def: &ChannelDefinition, reach: &[Reach]) -> String {
    let procs = reach
        .iter()
        .map(|r| format!("{}#{}", r.process_id, r.start_event_id))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "Nature: {}. Reachable process start-events: {}.",
        endpoint_nature(def),
        if procs.is_empty() {
            "(none)".to_string()
        } else {
            procs
        }
    )
}

/// OpenAPI operationId-safe: non-alphanumerics collapse to `_`.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests;
