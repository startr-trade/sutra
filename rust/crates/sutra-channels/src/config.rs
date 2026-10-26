//! `channels.yaml` loading — the channel-config loader (keyed by `DeploymentId`) + the
//! `ChannelBinding` / `ChannelDefinition` shapes.
//!
//! The channel file lives under the tenant tree
//! (`tenants/<tenant>/modules/<module>/<version>/channels.yaml`), so the authoring triple
//! is PATH-DERIVED and passed in; the YAML carries only the transport/codec/auth wiring.
//! A legacy `module:` / `version:` / `process:` key is ignored — the path is
//! authoritative. The tenant label crosses the authoring boundary exactly once: the
//! binding's [`Namespace`] derives the opaque [`DeploymentId`] every runtime lookup keys on.
//!
//! YAML parsing is data-only: only scalar / map / list data,
//! never object instantiation (`serde_yaml_ng` is data-only by construction).

use std::collections::BTreeMap;

use sutra_executor::DeploymentId;

use crate::codes;
use crate::diag::Diagnostic;

/// The `(tenant, module, version)` authoring identity — the namespace subset the
/// channel layer needs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace {
    pub tenant: String,
    pub module: String,
    pub version: String,
}

impl Namespace {
    pub fn new(tenant: &str, module: &str, version: &str) -> Namespace {
        Namespace {
            tenant: tenant.to_string(),
            module: module.to_string(),
            version: version.to_string(),
        }
    }

    /// The version-bearing string view — `"<tenant>/<module>/<version>"`.
    pub fn module_key(&self) -> String {
        format!("{}/{}/{}", self.tenant, self.module, self.version)
    }
}

/// Routing binding for an inbound channel — the channel-binding record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelBinding {
    pub channel_name: String,
    pub namespace: Namespace,
    /// THE deployment this channel is bound to — the binding pointer. A freshly parsed
    /// binding carries [`DeploymentId::unresolved`]; the archive-load path stamps the real
    /// manifest-hash id at activation. Flipping traffic to a new deployment =
    /// re-registering the binding with a new id — nothing else moves.
    pub deployment: DeploymentId,
    /// Optional feature-flag expression (`${feature.X}`); `None` = always enabled.
    pub enabled_expression: Option<String>,
    /// Codec name decoding inbound bytes (YAML-authoritative); `""` = schema-less.
    pub codec: String,
    /// `true` = pub/sub fan-out to all matching processes; default point-to-point.
    pub broadcast: bool,
    /// Optional cap on simultaneously-active instances (enforced on the stateful path).
    pub max_concurrent_instances: Option<u32>,
    /// Cap counting mode — `true` (default) counts only RUNNING instances.
    pub use_only_in_flight_for_concurrency_cap: bool,
    /// `pool:` — the engine-declared capacity pool (`sutra.capacity-pools.<name>`) the
    /// instances of this channel draw a slot from. On an inbound channel a new instance takes a
    /// slot or is rejected busy; on an outbound channel a request waits in the outbox until its
    /// call holds a slot. `None` = the channel draws from no pool.
    pub pool: Option<String>,
    /// `pool-call-key: header.<name>` — the header whose value identifies a call on this pooled
    /// channel (stored lower-cased, without the `header.` prefix). The slot of an inbound
    /// admission, or of an outbound request, is held for that call; absent (or absent from the
    /// message), the delivery's or the outbox entry's own identity is the call key. On a
    /// session channel the same header names the call a `session-end: on-message` ends.
    pub pool_call_key: Option<String>,
    /// `session-end:` (inbound channels only) — what ends the session an instance admitted here
    /// holds, releasing its slot before the instance reaches End. `None` = only End (or a
    /// failure) releases.
    pub session_end: Option<SessionEnd>,
}

/// An inbound channel's `session-end:` block: the events that release an instance's slots (its
/// channel admission slot and capacity-pool slots) while the instance runs on.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SessionEnd {
    /// `on-message: <channel>` — an inbound channel of the same file; a message on it that
    /// correlates to an instance (the usual `<q:alias>` correlation) ends a call of that
    /// instance. When this channel declares `pool-call-key:` and the message carries that
    /// header, only the call it names is released (its pool slot; and the channel slot too when
    /// it is the inbound call this channel admitted). Otherwise the whole session is released.
    pub on_message: Option<String>,
    /// `on-reply: true` — the flow's destination-less reply on this channel ends the replying
    /// instance's inbound call: its channel slot and the pool slot that call took.
    pub on_reply: bool,
}

impl ChannelBinding {
    /// Build a binding bound to `deployment`. Production parses bindings via a struct literal
    /// (deployment = [`DeploymentId::unresolved`], stamped at archive load); this constructor is
    /// for tests/transport fixtures that bind to a known id — pass the same id the module
    /// registry is keyed under so dispatch resolves.
    pub fn new(
        channel_name: &str,
        namespace: Namespace,
        deployment: DeploymentId,
        codec: &str,
    ) -> ChannelBinding {
        ChannelBinding {
            channel_name: channel_name.to_string(),
            deployment,
            namespace,
            enabled_expression: None,
            codec: codec.trim().to_string(),
            broadcast: false,
            max_concurrent_instances: None,
            use_only_in_flight_for_concurrency_cap: true,
            pool: None,
            pool_call_key: None,
            session_end: None,
        }
    }

    /// The call key a message (or request) with `headers` carries on this channel: the value of
    /// the declared `pool-call-key` header, matched case-insensitively. `None` when the channel
    /// declares none or the header is absent or empty.
    pub fn call_key_in(&self, headers: &BTreeMap<String, String>) -> Option<String> {
        let name = self.pool_call_key.as_deref()?;
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.trim().to_string())
            .filter(|v| !v.is_empty())
    }

    /// The single tenant authorised to receive on this channel.
    pub fn tenant(&self) -> &str {
        &self.namespace.tenant
    }

    /// The opaque deployment identity everything downstream keys on.
    pub fn deployment_id(&self) -> DeploymentId {
        self.deployment.clone()
    }

    /// Authoring labels for observability — never runtime identity.
    pub fn labels(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        out.insert("tenant".to_string(), self.namespace.tenant.clone());
        if !self.namespace.module.is_empty() {
            out.insert("module".to_string(), self.namespace.module.clone());
        }
        if !self.namespace.version.is_empty() {
            out.insert("version".to_string(), self.namespace.version.clone());
        }
        out
    }
}

/// Full channel declaration — the routing binding plus transport metadata
/// (`startup.ChannelDefinition`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelDefinition {
    pub binding: ChannelBinding,
    pub transport: Option<String>,
    /// Transport bind spec — `"<METHOD> <path>"` for HTTP.
    pub bind_spec: Option<String>,
    pub codec: Option<String>,
    pub cloud_events_mode: Option<String>,
    pub auth_scheme: Option<String>,
    pub idempotency_key_header: Option<String>,
    pub payload_cap_bytes: Option<u64>,
    /// Every non-reserved key, flattened with dotted keys; `auth.*` sub-keys flatten
    /// WITHOUT the `auth.` prefix (`apikey.value`, `apikey.header`, …).
    pub properties: BTreeMap<String, String>,
}

impl ChannelDefinition {
    pub fn has_transport(&self) -> bool {
        self.transport.as_deref().is_some_and(|t| !t.is_empty())
    }

    /// Per-channel singleton declaration (`consumer: exclusive` / `singleton: true`).
    pub fn singleton(&self) -> bool {
        self.properties
            .get("singleton")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            || self
                .properties
                .get("consumer")
                .is_some_and(|v| v.eq_ignore_ascii_case("exclusive"))
    }

    /// A declared outbound (send-target) channel — `direction: outbound`.
    pub fn is_outbound(&self) -> bool {
        self.properties
            .get("direction")
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("outbound"))
    }

    /// A SYNCHRONOUS outbound channel: `direction: outbound` with an EXPLICITLY declared
    /// `ack-mode: on-complete`. A channel-call on it is made inline and its response merged
    /// without a durable park. Deliberately not [`Self::effective_ack_mode`]: that applies the
    /// inbound per-transport default (`http` → `on-complete`), which must never turn an existing
    /// outbound channel synchronous.
    ///
    /// A channel that draws from a capacity pool (`pool:`) is never synchronous: its request
    /// may have to wait for a slot, and only the outbox can hold a request back without holding
    /// the caller's connection, so it takes the asynchronous path.
    pub fn is_sync_outbound(&self) -> bool {
        self.is_outbound()
            && self.binding.pool.is_none()
            && self
                .properties
                .get("ack-mode")
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("on-complete"))
    }

    /// The channel declares `reply-stream: sse`: a synchronous call marked `q:stream="true"`
    /// in a flow answering this channel's request streams to the caller as server-sent
    /// events. Meaningful only on an `http` channel with an `on-complete` ack (the lint
    /// refuses it anywhere else).
    pub fn wants_reply_stream_sse(&self) -> bool {
        self.properties
            .get("reply-stream")
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("sse"))
    }

    /// Effective ack-mode, per the startup-orchestrator resolution — the per-transport
    /// default rule: the HTTP transport defaults `on-complete` (the synchronous
    /// request/reply contract); broker transports default `on-persist` (ack after
    /// durable intake). A declared `ack-mode:` wins on every transport — a broker
    /// declaring `on-complete` opts into deferred acking (executed transport-side
    /// against the engine's `DeferredAckRegistry`; transports without a deferred settle
    /// path degrade LOUDLY at startup with `SUTRA.ACK.ON_COMPLETE_UNSUPPORTED`).
    pub fn effective_ack_mode(&self) -> &str {
        let default = if self.transport.as_deref() == Some("http") {
            "on-complete"
        } else {
            "on-persist"
        };
        match self.properties.get("ack-mode") {
            Some(declared) if !declared.trim().is_empty() => declared.trim(),
            _ => default,
        }
    }

    /// True when this (broker) definition resolves to `ack-mode: on-complete` — the
    /// deferred-acking opt-in ([`Self::effective_ack_mode`], ASCII case-insensitive).
    pub fn wants_on_complete_ack(&self) -> bool {
        self.effective_ack_mode()
            .eq_ignore_ascii_case("on-complete")
    }

    /// The HTTP path this channel serves — the path-resolution rule:
    /// an explicit `path:` property (slash-prefixed) wins; default `/channels/<name>`.
    pub fn resolve_path(&self) -> String {
        if let Some(declared) = self.properties.get("path") {
            if !declared.trim().is_empty() {
                return if declared.starts_with('/') {
                    declared.clone()
                } else {
                    format!("/{declared}")
                };
            }
        }
        format!("/channels/{}", self.binding.channel_name)
    }

    /// The HTTP method + path from the `bind:` spec (`"POST /channels/x"`), defaulting to
    /// `POST` + [`Self::resolve_path`] when absent.
    pub fn bind_method_and_path(&self) -> (String, String) {
        if let Some(spec) = &self.bind_spec {
            let mut parts = spec.split_whitespace();
            if let (Some(method), Some(path)) = (parts.next(), parts.next()) {
                return (method.to_uppercase(), path.to_string());
            }
        }
        ("POST".to_string(), self.resolve_path())
    }
}

/// Reserved keys consumed as first-class fields; everything else flows to the property
/// bag. `module` / `version` / `process` are reserved-and-ignored (the path wins).
const RESERVED_KEYS: [&str; 14] = [
    "name",
    "module",
    "version",
    "process",
    "transport",
    "bind",
    "codec",
    "cloudevents-mode",
    "auth",
    "auth-scheme",
    "idempotency-key-header",
    "payload-cap-bytes",
    "broadcast",
    "max-concurrent-instances",
];
// NOTE: "use-only-in-flight-for-concurrency-cap" is also reserved — checked separately
// (a 15th entry keeps the array literal readable).
const RESERVED_IN_FLIGHT_KEY: &str = "use-only-in-flight-for-concurrency-cap";

/// The capacity-pool and session keys, consumed as first-class binding fields.
const POOL_KEY: &str = "pool";
const POOL_CALL_KEY_KEY: &str = "pool-call-key";
const SESSION_END_KEY: &str = "session-end";

fn is_reserved(key: &str) -> bool {
    RESERVED_KEYS.contains(&key)
        || key == RESERVED_IN_FLIGHT_KEY
        || key == POOL_KEY
        || key == POOL_CALL_KEY_KEY
        || key == SESSION_END_KEY
}

/// Load a tenant channel YAML into [`ChannelDefinition`]s. The `(tenant, module,
/// version)` triple is path-derived by the scanner and passed in.
pub fn load_channel_definitions(
    yaml: &[u8],
    tenant: &str,
    module: &str,
    version: &str,
    source_for_errors: &str,
) -> Result<Vec<ChannelDefinition>, Diagnostic> {
    if tenant.trim().is_empty() || module.trim().is_empty() || version.trim().is_empty() {
        return Err(Diagnostic::error(
            codes::PARSE_YAML_PARSE_ERROR,
            "tenant, module and version are required (path-derived identity)",
        ));
    }
    let parsed: serde_yaml_ng::Value = serde_yaml_ng::from_slice(yaml).map_err(|e| {
        Diagnostic::error(
            codes::PARSE_YAML_PARSE_ERROR,
            format!("Channel YAML parse failed for {source_for_errors}: {e}"),
        )
    })?;
    if parsed.is_null() {
        return Ok(Vec::new());
    }
    let serde_yaml_ng::Value::Mapping(root) = &parsed else {
        return Err(Diagnostic::error(
            codes::PARSE_YAML_PARSE_ERROR,
            format!("Channel YAML {source_for_errors} must be a mapping at the root"),
        ));
    };
    let Some(channels) = root.get("channels") else {
        return Ok(Vec::new());
    };
    let serde_yaml_ng::Value::Sequence(list) = channels else {
        return Err(Diagnostic::error(
            codes::PARSE_YAML_PARSE_ERROR,
            format!("Channel YAML {source_for_errors} key 'channels' must be a list"),
        ));
    };

    let mut out = Vec::new();
    for (i, item) in list.iter().enumerate() {
        let serde_yaml_ng::Value::Mapping(entry) = item else {
            return Err(Diagnostic::error(
                codes::PARSE_YAML_PARSE_ERROR,
                format!("Channel YAML {source_for_errors} entry {i} must be a mapping"),
            ));
        };
        let name = required_string(entry, "name", i, source_for_errors)?;
        let codec = optional_string(entry, "codec");
        let broadcast = optional_boolean(entry, "broadcast", false);
        let max_concurrent =
            optional_positive_int(entry, "max-concurrent-instances", i, source_for_errors)?;
        let use_only_in_flight = optional_boolean(entry, RESERVED_IN_FLIGHT_KEY, true);
        let pool = optional_string(entry, POOL_KEY).map(|p| p.trim().to_string());
        let pool_call_key = parse_pool_call_key(entry, &name, pool.is_some(), source_for_errors)?;
        let session_end = parse_session_end(entry, &name, source_for_errors)?;

        let namespace = Namespace::new(tenant, module, version);
        let binding = ChannelBinding {
            channel_name: name,
            deployment: DeploymentId::unresolved(),
            namespace,
            enabled_expression: None,
            codec: codec.clone().unwrap_or_default(),
            broadcast,
            max_concurrent_instances: max_concurrent,
            use_only_in_flight_for_concurrency_cap: use_only_in_flight,
            pool,
            pool_call_key,
            session_end,
        };
        out.push(ChannelDefinition {
            binding,
            transport: optional_string(entry, "transport"),
            bind_spec: optional_string(entry, "bind"),
            codec,
            cloud_events_mode: optional_string(entry, "cloudevents-mode"),
            auth_scheme: resolve_auth_scheme(entry),
            idempotency_key_header: optional_string(entry, "idempotency-key-header"),
            payload_cap_bytes: optional_long(entry, "payload-cap-bytes", i, source_for_errors)?,
            properties: extract_properties(entry),
        });
    }
    validate_session_ends(&out, source_for_errors)?;
    Ok(out)
}

/// Parse an entry's optional `pool-call-key: header.<name>`; it needs a `pool:` to key.
fn parse_pool_call_key(
    entry: &serde_yaml_ng::Mapping,
    channel: &str,
    pooled: bool,
    source: &str,
) -> Result<Option<String>, Diagnostic> {
    let invalid = |detail: String| {
        Diagnostic::error(
            codes::CONFIG_CHANNEL_POOL_CALL_KEY_INVALID,
            format!("Channel YAML {source}: channel '{channel}' pool-call-key {detail}"),
        )
        .with_attribute("channel", channel)
    };
    let raw = match get(entry, POOL_CALL_KEY_KEY) {
        None | Some(serde_yaml_ng::Value::Null) => return Ok(None),
        Some(serde_yaml_ng::Value::String(raw)) => raw.trim(),
        Some(_) => return Err(invalid("must be a string (header.<name>)".to_string())),
    };
    let name = raw
        .strip_prefix("header.")
        .map(str::trim)
        .filter(|n| {
            !n.is_empty()
                && n.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
        .ok_or_else(|| invalid(format!("'{raw}' is not of the form header.<name>")))?;
    if !pooled {
        return Err(invalid(
            "is declared on a channel that draws from no capacity pool (add pool:)".to_string(),
        ));
    }
    Ok(Some(name.to_ascii_lowercase()))
}

/// Parse an entry's optional `session-end:` mapping (`on-message`, `on-reply`).
fn parse_session_end(
    entry: &serde_yaml_ng::Mapping,
    channel: &str,
    source: &str,
) -> Result<Option<SessionEnd>, Diagnostic> {
    let invalid = |detail: String| {
        Diagnostic::error(
            codes::CONFIG_CHANNEL_SESSION_END_INVALID,
            format!("Channel YAML {source}: channel '{channel}' session-end {detail}"),
        )
        .with_attribute("channel", channel)
    };
    let block = match get(entry, SESSION_END_KEY) {
        None | Some(serde_yaml_ng::Value::Null) => return Ok(None),
        Some(serde_yaml_ng::Value::Mapping(block)) => block,
        Some(_) => return Err(invalid("must be a mapping".to_string())),
    };
    let mut session_end = SessionEnd::default();
    for (key, value) in block {
        let serde_yaml_ng::Value::String(key) = key else {
            return Err(invalid("keys must be strings".to_string()));
        };
        match (key.as_str(), value) {
            ("on-message", serde_yaml_ng::Value::String(target)) if !target.trim().is_empty() => {
                session_end.on_message = Some(target.trim().to_string());
            }
            ("on-message", _) => {
                return Err(invalid("on-message must name a channel".to_string()));
            }
            ("on-reply", serde_yaml_ng::Value::Bool(b)) => session_end.on_reply = *b,
            ("on-reply", _) => return Err(invalid("on-reply must be true or false".to_string())),
            (other, _) => {
                return Err(invalid(format!(
                    "has unknown key '{other}' (expected on-message, on-reply)"
                )))
            }
        }
    }
    if session_end.on_message.is_none() && !session_end.on_reply {
        return Err(invalid(
            "declares nothing that ends a session (set on-message and/or on-reply: true)"
                .to_string(),
        ));
    }
    Ok(Some(session_end))
}

/// Cross-entry `session-end:` rules: only an inbound channel declares one, and its
/// `on-message` names ANOTHER inbound channel of the same file.
fn validate_session_ends(
    definitions: &[ChannelDefinition],
    source: &str,
) -> Result<(), Diagnostic> {
    for def in definitions {
        let Some(session_end) = &def.binding.session_end else {
            continue;
        };
        let channel = &def.binding.channel_name;
        let invalid = |detail: String| {
            Diagnostic::error(
                codes::CONFIG_CHANNEL_SESSION_END_INVALID,
                format!("Channel YAML {source}: channel '{channel}' session-end {detail}"),
            )
            .with_attribute("channel", channel)
        };
        if def.is_outbound() {
            return Err(invalid(
                "is declared on an outbound channel; only an inbound channel admits sessions"
                    .to_string(),
            ));
        }
        if let Some(target) = &session_end.on_message {
            if target == channel {
                return Err(invalid(format!(
                    "on-message names the session's own channel '{target}'"
                )));
            }
            match definitions
                .iter()
                .find(|d| d.binding.channel_name == *target)
            {
                None => {
                    return Err(invalid(format!(
                        "on-message names '{target}', which is not a channel of this file"
                    )))
                }
                Some(d) if d.is_outbound() => {
                    return Err(invalid(format!(
                        "on-message names '{target}', which is an outbound channel; it must be \
                         an inbound channel"
                    )))
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

fn get<'a>(entry: &'a serde_yaml_ng::Mapping, key: &str) -> Option<&'a serde_yaml_ng::Value> {
    entry.get(serde_yaml_ng::Value::String(key.to_string()))
}

fn required_string(
    entry: &serde_yaml_ng::Mapping,
    key: &str,
    index: usize,
    source: &str,
) -> Result<String, Diagnostic> {
    match get(entry, key) {
        Some(serde_yaml_ng::Value::String(s)) if !s.trim().is_empty() => Ok(s.clone()),
        _ => Err(Diagnostic::error(
            codes::PARSE_YAML_PARSE_ERROR,
            format!("Channel YAML {source} entry {index} missing required string '{key}'"),
        )),
    }
}

fn optional_string(entry: &serde_yaml_ng::Mapping, key: &str) -> Option<String> {
    match get(entry, key) {
        Some(serde_yaml_ng::Value::String(s)) if !s.trim().is_empty() => Some(s.clone()),
        _ => None,
    }
}

fn optional_boolean(entry: &serde_yaml_ng::Mapping, key: &str, default: bool) -> bool {
    match get(entry, key) {
        Some(serde_yaml_ng::Value::Bool(b)) => *b,
        Some(serde_yaml_ng::Value::String(s)) if !s.trim().is_empty() => {
            s.trim().eq_ignore_ascii_case("true")
        }
        _ => default,
    }
}

fn optional_positive_int(
    entry: &serde_yaml_ng::Mapping,
    key: &str,
    index: usize,
    source: &str,
) -> Result<Option<u32>, Diagnostic> {
    match get(entry, key) {
        None | Some(serde_yaml_ng::Value::Null) => Ok(None),
        Some(serde_yaml_ng::Value::Number(n)) => match n.as_i64() {
            Some(v) if v > 0 && u32::try_from(v).is_ok() => Ok(Some(v as u32)),
            _ => Err(positive_int_error(key, index, source)),
        },
        _ => Err(positive_int_error(key, index, source)),
    }
}

fn optional_long(
    entry: &serde_yaml_ng::Mapping,
    key: &str,
    index: usize,
    source: &str,
) -> Result<Option<u64>, Diagnostic> {
    match get(entry, key) {
        None | Some(serde_yaml_ng::Value::Null) => Ok(None),
        Some(serde_yaml_ng::Value::Number(n)) => match n.as_i64() {
            Some(v) if v > 0 => Ok(Some(v as u64)),
            _ => Err(positive_int_error(key, index, source)),
        },
        _ => Err(positive_int_error(key, index, source)),
    }
}

fn positive_int_error(key: &str, index: usize, source: &str) -> Diagnostic {
    Diagnostic::error(
        codes::PARSE_YAML_PARSE_ERROR,
        format!("Channel YAML {source} entry {index} key '{key}' must be a positive integer"),
    )
}

/// Auth scheme accepts a top-level `auth-scheme:` or a nested `auth: {scheme: …}` — the
/// nested form's other sub-keys land in the property bag WITHOUT the `auth.` prefix.
fn resolve_auth_scheme(entry: &serde_yaml_ng::Mapping) -> Option<String> {
    if let Some(top) = optional_string(entry, "auth-scheme") {
        return Some(top);
    }
    match get(entry, "auth") {
        Some(serde_yaml_ng::Value::Mapping(auth)) => optional_string(auth, "scheme"),
        _ => None,
    }
}

/// Captures every non-reserved key as a flattened string property (nested maps use
/// dotted keys); `auth` sub-keys other than `scheme` flatten without the `auth.` prefix.
fn extract_properties(entry: &serde_yaml_ng::Mapping) -> BTreeMap<String, String> {
    let mut props = BTreeMap::new();
    for (k, v) in entry {
        let serde_yaml_ng::Value::String(key) = k else {
            continue;
        };
        if v.is_null() {
            continue;
        }
        if is_reserved(key) {
            if key == "auth" {
                if let serde_yaml_ng::Value::Mapping(auth) = v {
                    for (sk, sv) in auth {
                        if let serde_yaml_ng::Value::String(sub_key) = sk {
                            if sub_key != "scheme" {
                                flatten(&mut props, sub_key, sv);
                            }
                        }
                    }
                }
            }
            continue;
        }
        flatten(&mut props, key, v);
    }
    props
}

/// Recursively flattens nested maps with dotted keys; lists / scalars stringify.
fn flatten(out: &mut BTreeMap<String, String>, prefix: &str, value: &serde_yaml_ng::Value) {
    use serde_yaml_ng::Value as Y;
    match value {
        Y::Null => {}
        Y::Mapping(nested) => {
            for (k, v) in nested {
                if let Y::String(key) = k {
                    flatten(out, &format!("{prefix}.{key}"), v);
                }
            }
        }
        Y::String(s) => {
            out.insert(prefix.to_string(), s.clone());
        }
        Y::Bool(b) => {
            out.insert(prefix.to_string(), b.to_string());
        }
        Y::Number(n) => {
            out.insert(prefix.to_string(), n.to_string());
        }
        other => {
            let s = serde_yaml_ng::to_string(other)
                .map(|s| s.trim_end().to_string())
                .unwrap_or_default();
            out.insert(prefix.to_string(), s);
        }
    }
}
