//! The transport SPI + shared cluster substrate — the neutral seam vendor transports
//! self-register through, so `sutra-engine` composes broker consumers GENERICALLY and
//! names no vendor (domain-neutrality refactor).
//!
//! It hosts three moved-down pieces the per-vendor `sutra-transport-<vendor>` crates need
//! but that must not live in `sutra-engine` (which depends on the transport crates to
//! bundle them — the reverse edge would be a cycle):
//! - [`leadership`] — the DB-lease leader election ([`leadership::DbLeaderElection`],
//!   [`leadership::PgLeaseHandle`], [`leadership::ChannelLeadership`],
//!   [`leadership::channel_role`]). Shared: the timer poller leads through it too.
//! - [`EngineIntake`] — the `EngineHandle` → `InboundIntake` adapter the sources deliver
//!   through.
//! - the [`TransportChannels`] lifecycle trait + the inventory-collected
//!   [`TransportFactory`] each vendor crate `inventory::submit!`s, iterated by the engine
//!   assembly via [`transport_factories`].
#![forbid(unsafe_code)]

use std::sync::Arc;

use sqlx::PgPool;
use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, DeclaredEndpoint, Diagnostic, SinkRegistry};
use tokio::runtime::Handle;

pub mod intake;
pub mod leadership;

pub use intake::EngineIntake;
pub use sutra_channels::{EndpointClaim, EndpointSharing};

/// A resolver for `env:` / `secret:` / `vault:` references + `${…}` placeholders — the
/// engine's `envref` registry, passed into a transport's [`TransportFactory::spawn`] as a
/// plain `fn` pointer so the transport crate resolves inbound-auth + broker credentials
/// without depending on `sutra-engine` (where the registry lives).
pub type EnvRefResolver = fn(&str) -> Result<String, String>;

/// `definition` with the named properties resolved through the engine's `resolver`, so a
/// transport reads `region: "${AWS_REGION}"` the way it reads a credential reference. A property
/// the channel does not set stays unset, and text with no reference passes through unchanged. A
/// reference that cannot be resolved fails with `code`, naming the channel and the property and
/// never a resolved value, so the channel fails closed at wiring instead of connecting to a literal
/// `${…}`. A reference that resolves to blank leaves the property blank, which every reader treats
/// as unset, so `${NAME:}` is how an author writes an optional setting. Any other reference that
/// resolves to blank logs a WARN with [`sutra_channels::codes::CONFIG_PROPERTY_BLANK_REFERENCE`],
/// because the transport's built-in default then applies where the author named a value.
pub fn resolve_channel_properties(
    definition: &ChannelDefinition,
    keys: &[&str],
    code: &str,
    resolver: EnvRefResolver,
) -> Result<ChannelDefinition, Diagnostic> {
    let mut resolved = definition.clone();
    for key in keys {
        if let Some(raw) = definition.properties.get(*key) {
            let value = resolver(raw).map_err(|e| {
                Diagnostic::error(
                    code,
                    format!(
                        "channel '{}' property '{key}' has an unresolved reference: {e}",
                        definition.binding.channel_name
                    ),
                )
            })?;
            if let Some(warning) =
                blank_reference_warning(&definition.binding.channel_name, key, raw, &value)
            {
                tracing::warn!(
                    code = %warning.code,
                    channel = %definition.binding.channel_name,
                    property = %key,
                    "{}",
                    warning.message
                );
            }
            resolved.properties.insert((*key).to_string(), value);
        }
    }
    Ok(resolved)
}

/// The WARN for a property whose reference resolved to blank, unless the author wrote it blank on
/// purpose (`${NAME:}`) or wrote no reference at all. It names the channel, the property and the
/// reference as written, never a resolved value.
fn blank_reference_warning(channel: &str, key: &str, raw: &str, value: &str) -> Option<Diagnostic> {
    let written = raw.trim();
    if written.is_empty()
        || written == value.trim()
        || !value.trim().is_empty()
        || sutra_envref_spi::is_explicit_empty_default(written)
    {
        return None;
    }
    Some(Diagnostic::error(
        sutra_channels::codes::CONFIG_PROPERTY_BLANK_REFERENCE,
        format!(
            "channel '{channel}' property '{key}' reference '{written}' resolved to blank, so the \
             property is unset: the transport's built-in default applies, or a required property \
             is refused. Write the reference with an empty default, such as ${{NAME:}}, to leave \
             it blank on purpose"
        ),
    ))
}

/// Refuses a port that came from a reference and is not a port. A transport's own parser quotes
/// what it could not read, which is fine for a value written in the file and not for one that came
/// out of a secret store, so this runs first and names the channel and the property only. A port
/// written literally is left to the parser, and a blank one means the transport's default.
pub fn require_resolved_port(
    definition: &ChannelDefinition,
    resolved: &ChannelDefinition,
    key: &str,
    code: &str,
) -> Result<(), Diagnostic> {
    let Some(port) = resolved.properties.get(key) else {
        return Ok(());
    };
    let port = port.trim();
    let from_reference = definition.properties.get(key).map(|raw| raw.trim()) != Some(port);
    if from_reference && !port.is_empty() && !port.parse::<u16>().is_ok_and(|n| n != 0) {
        return Err(Diagnostic::error(
            code,
            format!(
                "channel '{}' property '{key}' has a reference that does not resolve to an \
                 integer in 1..=65535",
                definition.binding.channel_name
            ),
        ));
    }
    Ok(())
}

/// True when a channel property value is a secret REFERENCE rather than a literal: any whole
/// reference the engine's resolver resolves (see [`sutra_envref_spi::is_whole_reference`]:
/// `env:NAME`, `secret:KEY`, a registered secret-store scheme, `${NAME}`, `${secret:KEY}`, each
/// placeholder optionally with `:default`), or a `k8s:secret/<name>#<key>` reference. Transports
/// use this to refuse a literal credential at startup.
pub fn is_secret_reference(value: &str) -> bool {
    let value = value.trim();
    sutra_envref_spi::is_whole_reference(value)
        || (value.starts_with("k8s:secret/") && value.contains('#'))
}

/// The uniform inbound-consumer spawner signature every vendor's [`TransportFactory`]
/// exposes: `(defs, engine, pool, envref-resolver, runtime) -> the live manager`.
pub type TransportSpawn = fn(
    &[ChannelDefinition],
    EngineHandle,
    Option<PgPool>,
    EnvRefResolver,
    Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic>;

/// One vendor transport's live consumer set + lifecycle. The engine holds each as an
/// `Arc<dyn TransportChannels>` and drives them GENERICALLY: `rewire` on an activation
/// flip, `drain` on graceful shutdown, `stop_all_detached` on the sync stop path.
#[async_trait::async_trait]
pub trait TransportChannels: Send + Sync {
    /// The `transport:` discriminator this manager owns (e.g. `kafka`, `aws-sqs`).
    fn transport(&self) -> &str;
    /// Number of consumers currently wired.
    fn consumer_count(&self) -> usize;
    /// Reconcile the running consumers to the new ACTIVE definition set (the activation flip):
    /// stop changed/removed, start added, keep unchanged. Idempotent + non-fatal.
    async fn rewire(&self, active: &[ChannelDefinition]);
    /// Await-drain every consumer + release the channel-lease election (graceful shutdown).
    async fn drain(&self);
    /// Detached stop of every consumer + lease release (the sync stop path).
    fn stop_all_detached(&self, runtime: &Handle);
    /// The inbound routes this transport serves over the process's shared HTTP listener, if
    /// any. This is a protocol-NEUTRAL capability, not an HTTP special case: a transport that
    /// receives *inbound over the engine's own listener* (HTTP today; a future ws/grpc-on-the-
    /// same-port transport tomorrow) contributes its axum [`Router`], which the server merges
    /// under its `/sutra/health/*` API. Transports that dial *out* to their own broker
    /// (kafka, rabbitmq, …) own their connection and take the default `None`.
    fn inbound_router(&self) -> Option<axum::Router> {
        None
    }
    /// Would [`Self::rewire`] accept this ACTIVE definition set? Called on every transport before
    /// an activation flip commits, so a set one transport cannot serve aborts the flip with the
    /// old state intact, rather than failing part-way through it. Must not change anything. The
    /// default accepts everything: a transport whose rewire isolates failures per consumer has
    /// nothing to refuse up front.
    fn prepare_rewire(&self, _active: &[ChannelDefinition]) -> Result<(), Diagnostic> {
        Ok(())
    }
}

/// What the running engine offers a channel, for a check that runs before any flip.
#[derive(Debug, Clone, Copy)]
pub struct PreflightHost {
    /// The engine has a database. A channel that needs one cannot be served without it.
    pub persistence: bool,
}

/// Can these definitions be served at all? See [`EndpointClaimFactory::preflight`].
pub type EndpointPreflight =
    fn(&[ChannelDefinition], EnvRefResolver, PreflightHost) -> Result<(), Diagnostic>;

/// A transport's declaration of the inbound endpoints its channels claim. A transport
/// `inventory::submit!`s one beside its [`TransportFactory`]; the engine, `sutra lint` and
/// `sutra deploy` collect them through [`declared_endpoints`] and refuse a deployment that claims
/// an exclusive endpoint another active deployment holds.
///
/// Separate from [`TransportFactory`], so a transport outside this repository keeps compiling
/// unchanged: one that submits nothing simply is not checked, and is reported as unchecked.
pub struct EndpointClaimFactory {
    /// The `transport:` value these claims belong to.
    pub transport: &'static str,
    /// The endpoints one INBOUND channel of this transport claims. Pure: it runs off-line, in
    /// the CLI as well as the engine, with no broker and no secrets to hand. A channel whose
    /// properties are too broken to name an endpoint claims nothing, because the transport's own
    /// validation refuses it.
    pub claims: fn(&ChannelDefinition) -> Vec<EndpointClaim>,
    /// The path prefix this transport serves on the engine's shared listener, if it serves one.
    /// Another transport's channel bound under it would never receive anything.
    pub listener_prefix: Option<&'static str>,
    /// The off-line half of [`TransportChannels::prepare_rewire`]: can these definitions be served
    /// at all? A folder source runs it on each archive, before the archive can displace anything.
    pub preflight: Option<EndpointPreflight>,
}

inventory::collect!(EndpointClaimFactory);

/// What one deployment's inbound channels claim, as far as this binary can tell.
#[derive(Debug, Default, Clone)]
pub struct DeploymentEndpoints {
    pub endpoints: Vec<DeclaredEndpoint>,
    /// `(channel, transport)` of each inbound channel whose transport declares no claims in this
    /// binary. Nothing was checked for these.
    pub unchecked: Vec<(String, String)>,
    /// `(channel, path, transport)`: a channel bound under a path another transport serves on the
    /// shared listener.
    pub shadowed: Vec<(String, String, String)>,
}

/// Can this binary's transports serve `definitions`? The first refusal is returned.
pub fn preflight_definitions(
    definitions: &[ChannelDefinition],
    resolver: EnvRefResolver,
    host: PreflightHost,
) -> Result<(), Diagnostic> {
    for factory in inventory::iter::<EndpointClaimFactory> {
        if let Some(preflight) = factory.preflight {
            preflight(definitions, resolver, host)?;
        }
    }
    Ok(())
}

/// A broker host as an endpoint identity: lower case, because host names are, and without any inline
/// credentials, because identities appear in refusals and in the engine's holder record.
///
/// This runs on authored text, so a `${...}` reference may be in it. A reference keeps its exact
/// spelling, because environment names are case-sensitive, and an `@` inside one is part of its name.
pub fn normalized_endpoint_host(raw: &str) -> String {
    let raw = raw.trim();
    let mut host_starts = 0;
    let mut spans: Vec<(usize, usize)> = Vec::new();
    let mut at = 0;
    while let Some(open) = raw[at..].find("${").map(|i| i + at) {
        let Some(close) = raw[open..].find('}').map(|i| i + open + 1) else {
            break;
        };
        spans.push((open, close));
        at = close;
    }
    for (i, c) in raw.char_indices() {
        if c == '@'
            && !spans
                .iter()
                .any(|(open, close)| (*open..*close).contains(&i))
        {
            host_starts = i + 1;
        }
    }
    let mut host = String::with_capacity(raw.len() - host_starts);
    let mut at = host_starts;
    for (open, close) in spans.iter().filter(|(open, _)| *open >= host_starts) {
        host.push_str(&raw[at..*open].to_ascii_lowercase());
        host.push_str(&raw[*open..*close]);
        at = *close;
    }
    host.push_str(&raw[at..].to_ascii_lowercase());
    host
}

/// Collect the endpoint claims of one deployment's `definitions`, skipping outbound channels
/// (a send target receives nothing).
pub fn declared_endpoints(
    deployment_id: &str,
    deployment_label: &str,
    definitions: &[ChannelDefinition],
) -> DeploymentEndpoints {
    let mut out = DeploymentEndpoints::default();
    for def in definitions.iter().filter(|d| !d.is_outbound()) {
        let Some(transport) = def.transport.as_deref().filter(|t| !t.is_empty()) else {
            continue;
        };
        if def.bind_spec.is_some() {
            let (_, path) = def.bind_method_and_path();
            let owner = inventory::iter::<EndpointClaimFactory>
                .into_iter()
                .find(|f| {
                    f.transport != transport
                        && f.listener_prefix
                            .is_some_and(|prefix| path.starts_with(prefix))
                });
            if let Some(owner) = owner {
                out.shadowed.push((
                    def.binding.channel_name.clone(),
                    path.to_string(),
                    owner.transport.to_string(),
                ));
            }
        }
        let factory = inventory::iter::<EndpointClaimFactory>
            .into_iter()
            .find(|f| f.transport == transport);
        let Some(factory) = factory else {
            out.unchecked
                .push((def.binding.channel_name.clone(), transport.to_string()));
            continue;
        };
        for claim in (factory.claims)(def) {
            out.endpoints.push(DeclaredEndpoint {
                deployment_id: deployment_id.to_string(),
                deployment_label: deployment_label.to_string(),
                owner: sutra_channels::deployment_owner(
                    &def.binding.namespace.tenant,
                    &def.binding.namespace.module,
                    deployment_id,
                ),
                channel: def.binding.channel_name.clone(),
                transport: transport.to_string(),
                claim,
            });
        }
    }
    out
}

/// A vendor transport's self-registration: its `transport:` discriminator + the inbound
/// consumer spawner + the outbound sink registrar. Each `sutra-transport-<vendor>` crate
/// `inventory::submit!`s exactly one; [`transport_factories`] collects them.
pub struct TransportFactory {
    /// The `transport:` value this factory wires (channel definitions are filtered by it).
    pub transport: &'static str,
    /// Construct + start the inbound consumers for this transport's definitions. Signature
    /// is UNIFORM across vendors: `(defs, engine, pool, envref-resolver, runtime)`.
    pub spawn: TransportSpawn,
    /// Register this transport's outbound sink into the dispatcher's [`SinkRegistry`]
    /// (reads its own engine-wide `SUTRA_SINK_<VENDOR>_*` env config internally).
    pub register_sink: fn(&mut SinkRegistry),
    /// Whether this transport REALISES `ack-mode: on-complete`: a broker source
    /// by deferring its settle through the engine's deferred-ack registry
    /// (`InboundIntake::deliver_deferred` — rabbitmq), an on-listener transport by
    /// holding the connection to completion (http). `false` ⇒ the engine assembly emits
    /// the `SUTRA.ACK.ON_COMPLETE_UNSUPPORTED` startup diagnostic for any inbound
    /// definition declaring `on-complete` on this transport (loud degrade to
    /// `on-persist`, never silent) — the assembly stays vendor-neutral by reading THIS
    /// flag instead of naming transports.
    pub handles_on_complete: bool,
}

inventory::collect!(TransportFactory);

/// Every registered [`TransportFactory`], sorted by `transport` for deterministic wiring.
pub fn transport_factories() -> Vec<&'static TransportFactory> {
    let mut factories: Vec<&'static TransportFactory> =
        inventory::iter::<TransportFactory>.into_iter().collect();
    factories.sort_by_key(|f| f.transport);
    factories
}

/// A transport whose outbound sink ANSWERS by re-entering the engine — a request/reply
/// exchange whose reply is delivered back as an inbound message, the way the in-process
/// `local` sink does. The bare [`TransportFactory::register_sink`] fn-ptr has no engine
/// access, so such a transport additionally `inventory::submit!`s one of these; the engine
/// assembly registers every collected sink with the engine handle, naming none.
///
/// The `transport` value doubles as the sink's URI scheme: a bare `<transport>://<channel>`
/// outbound bind is qualified to `<transport>://<module_key>/<channel>` at load, exactly
/// as `local://` is, so the sink can address the reply to the owning deployment.
pub struct ReplySinkFactory {
    /// The `transport:` value (and URI scheme) this sink serves.
    pub transport: &'static str,
    /// Build the sink around the engine handle it re-enters through.
    pub build: fn(EngineHandle) -> Arc<dyn sutra_channels::MessageSink>,
}

inventory::collect!(ReplySinkFactory);

/// Every registered [`ReplySinkFactory`], sorted by `transport` for deterministic wiring.
pub fn reply_sink_factories() -> Vec<&'static ReplySinkFactory> {
    let mut factories: Vec<&'static ReplySinkFactory> =
        inventory::iter::<ReplySinkFactory>.into_iter().collect();
    factories.sort_by_key(|f| f.transport);
    factories
}

/// A transport that can answer an outbound call SYNCHRONOUSLY (an outbound channel declaring
/// `ack-mode: on-complete`): the response is merged into the flow without a durable park. The
/// engine assembly registers every collected caller into the dispatcher's
/// [`sutra_channels::InlineCallerRegistry`], naming none; a sync outbound channel whose scheme
/// has no caller is a load-time error.
pub struct InlineCallerFactory {
    /// The `transport:` value this caller serves.
    pub transport: &'static str,
    pub build: fn() -> Arc<dyn sutra_channels::InlineCaller>,
}

inventory::collect!(InlineCallerFactory);

/// Every registered [`InlineCallerFactory`], sorted by `transport`.
pub fn inline_caller_factories() -> Vec<&'static InlineCallerFactory> {
    let mut factories: Vec<&'static InlineCallerFactory> =
        inventory::iter::<InlineCallerFactory>.into_iter().collect();
    factories.sort_by_key(|f| f.transport);
    factories
}

#[cfg(test)]
mod resolved_port_tests {
    use super::require_resolved_port;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::{ChannelDefinition, DeploymentId};

    fn definition(port: &str) -> ChannelDefinition {
        ChannelDefinition {
            binding: ChannelBinding::new(
                "orders-in",
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("test".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties: [("port".to_string(), port.to_string())]
                .into_iter()
                .collect(),
        }
    }

    #[test]
    fn a_reference_that_is_not_a_port_is_refused_without_quoting_it() {
        for value in ["s3cret-value", "0", "70000"] {
            let err =
                require_resolved_port(&definition("${P}"), &definition(value), "port", "T.CODE")
                    .unwrap_err();
            assert_eq!(err.code, "T.CODE");
            assert!(err.message.contains("orders-in"), "{}", err.message);
            assert!(err.message.contains("'port'"), "{}", err.message);
            assert!(!err.message.contains(value), "{}", err.message);
        }
    }

    #[test]
    fn a_good_port_a_blank_one_and_a_literal_one_are_left_alone() {
        let raw = definition("${P}");
        assert!(require_resolved_port(&raw, &definition("5672"), "port", "C").is_ok());
        assert!(require_resolved_port(&raw, &definition(" "), "port", "C").is_ok());
        let literal = definition("not-a-port");
        assert!(require_resolved_port(&literal, &literal, "port", "C").is_ok());
    }
}

#[cfg(test)]
mod endpoint_host_tests {
    use super::normalized_endpoint_host as host;

    #[test]
    fn a_host_compares_in_lower_case_without_credentials() {
        assert_eq!(host(" Broker.Example:9092 "), "broker.example:9092");
        assert_eq!(host("user:pw@Broker:9092"), "broker:9092");
    }

    /// Environment names are case-sensitive, and an `@` inside a reference is part of its name.
    #[test]
    fn a_reference_keeps_its_exact_spelling() {
        assert_ne!(host("${broker_a}"), host("${BROKER_A}"));
        assert_eq!(host("${BROKER_A}"), "${BROKER_A}");
        assert_eq!(host("Host-${BROKER_A}:9092"), "host-${BROKER_A}:9092");
        assert_eq!(host("${vault:a@b}"), "${vault:a@b}");
        assert_eq!(host("${USER}:${PASS}@Broker:9092"), "broker:9092");
    }
}

#[cfg(test)]
mod blank_reference_tests {
    use super::{blank_reference_warning, resolve_channel_properties};
    use sutra_channels::codes::CONFIG_PROPERTY_BLANK_REFERENCE;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::{ChannelDefinition, DeploymentId};

    #[test]
    fn a_blank_reference_warns_naming_channel_property_and_reference() {
        let warning = blank_reference_warning("orders-in", "host", "${BROKER_HOST}", "")
            .expect("a set-but-empty variable warns");
        assert_eq!(warning.code, CONFIG_PROPERTY_BLANK_REFERENCE);
        for part in ["'orders-in'", "'host'", "'${BROKER_HOST}'"] {
            assert!(warning.message.contains(part), "{}", warning.message);
        }
        assert!(blank_reference_warning("c", "group.id", "env:GROUP", "  ").is_some());
        assert!(blank_reference_warning("c", "queue", "secret:queue-name", "").is_some());
    }

    #[test]
    fn the_explicit_empty_default_and_non_blank_values_do_not_warn() {
        assert!(blank_reference_warning("c", "host", "${BROKER_HOST:}", "").is_none());
        assert!(blank_reference_warning("c", "host", "${secret:host-key:}", "").is_none());
        assert!(blank_reference_warning("c", "host", "${BROKER_HOST}", "broker").is_none());
        assert!(blank_reference_warning("c", "host", "", "").is_none());
        assert!(blank_reference_warning("c", "host", "broker", "broker").is_none());
    }

    #[test]
    fn resolution_keeps_a_blank_reference_blank() {
        let definition = ChannelDefinition {
            binding: ChannelBinding::new(
                "orders-in",
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("test".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties: [("host".to_string(), "${BROKER_HOST}".to_string())]
                .into_iter()
                .collect(),
        };
        fn blank(_: &str) -> Result<String, String> {
            Ok(String::new())
        }
        let resolved =
            resolve_channel_properties(&definition, &["host"], "TEST.CODE", blank).unwrap();
        assert_eq!(
            resolved.properties.get("host").map(String::as_str),
            Some("")
        );
    }
}
