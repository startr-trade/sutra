//! Kafka channel wiring — the assembly touch-point for `transport: kafka` broker
//! consumers: for every inbound `transport: kafka` channel definition this constructs a
//! [`KafkaTriggerSource`], gates it (`singleton: true` ⇒ a
//! [`crate::leadership::DbLeaderElection`] lease role `sutra-channel:<tenant>:<channel>`;
//! otherwise / no datasource ⇒ always-leading), and starts it against an [`InboundIntake`]
//! adapter over the engine actor.
//!
//! Mirrors [`crate::rabbitmq`]: assembly calls [`spawn_kafka_channels`] once and moves on;
//! consumers detach onto the runtime (broker absence is NON-FATAL), and the shared,
//! rewireable [`KafkaChannels`] rides the activation flip via [`KafkaChannels::rewire`]
//! with the same semantics (stop changed/removed, start added, keep unchanged; idempotent;
//! non-fatal on a bad new def; drain on shutdown). Like RabbitMQ, the `sasl.*` credentials and a
//! `tls-ca-pem` are channel-YAML secret references, resolved here (an unresolvable reference ⇒
//! WARN + skip, the broker-absent deployment shape).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqlx::PgPool;
use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, Diagnostic, InboundIntake, SinkRegistry, TriggerSource};
use sutra_persistence::stores::PgLeaseStore;
use sutra_transport_spi::leadership::{
    channel_role, ChannelLeadership, DbLeaderElection, PgLeaseHandle,
};
use sutra_transport_spi::{EngineIntake, EnvRefResolver, TransportChannels, TransportFactory};
use tokio::runtime::Handle;
use tracing::{info, warn};

use crate::kafka::{
    KafkaChannelProperties, KafkaMessageSink, KafkaSinkSecurity, KafkaSourceConfig,
    KafkaTriggerSource, TRANSPORT,
};

/// The env var naming the engine-wide Kafka sink bootstrap servers.
/// Empty/unset ⇒ the sink still registers
/// (so `kafka://` destinations resolve) but publishes fail-closed retryable until set.
pub const SINK_BOOTSTRAP_ENV: &str = "SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS";

/// [`register_kafka_sink`] with TLS and SASL settings. `Err` holds why the engine-wide settings are
/// unusable; every send then fails closed and reports it.
pub fn register_kafka_sink_with_security(
    registry: &mut SinkRegistry,
    bootstrap_servers: &str,
    security: Result<KafkaSinkSecurity, String>,
) {
    registry.register(Arc::new(
        KafkaMessageSink::new(bootstrap_servers).with_security(security),
    ));
}

/// Register the Kafka sink into an outbox [`SinkRegistry`] (the outbox dispatcher's
/// resolution surface) under its claimed scheme (`kafka`), targeting `bootstrap_servers`.
pub fn register_kafka_sink(registry: &mut SinkRegistry, bootstrap_servers: &str) {
    registry.register(Arc::new(KafkaMessageSink::new(bootstrap_servers)));
}

/// The engine-wide Kafka sink bootstrap servers from the environment (empty when unset).
pub fn sink_bootstrap_from_env() -> String {
    std::env::var(SINK_BOOTSTRAP_ENV).unwrap_or_default()
}

/// The wired Kafka consumers + everything needed to re-start them. SHARED (`Arc`) between
/// the engine (shutdown drain) and the activation watcher (topology rewire on flip):
/// [`KafkaChannels::rewire`] reconciles the running consumers to a new active definition
/// set, stopping consumers whose definition changed (or was removed) and starting the new
/// ones — unchanged consumers keep running. `stop` is drain-postured and at-least-once +
/// inbox dedup absorb any redelivery over the brief handover, so a flip loses no messages.
#[derive(Clone, Default)]
pub struct KafkaChannels {
    inner: Option<Arc<BrokerState>>,
}

/// `(tenant, module_key, channel)` — the stable identity of one broker consumer across flips.
type ConsumerKey = (String, String, String);

struct RunningConsumer {
    source: Arc<KafkaTriggerSource>,
    /// The authored-config fingerprint used to detect a changed definition on flip
    /// (`KafkaChannelProperties: Eq`).
    fingerprint: KafkaChannelProperties,
}

struct BrokerState {
    running: Mutex<HashMap<ConsumerKey, RunningConsumer>>,
    /// The shared singleton-role election, when one was constructed at boot (datasource
    /// present AND at least one boot `singleton: true` channel).
    election: Option<Arc<DbLeaderElection>>,
    ctx: BrokerRespawnContext,
}

/// Everything a flip needs to (re)start a consumer after boot.
struct BrokerRespawnContext {
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    /// The `env:`/`secret:`/`vault:` reference resolver, injected by the engine (the
    /// `envref` registry lives there) so this crate depends on no engine module.
    resolver: EnvRefResolver,
    handle: Handle,
}

impl KafkaChannels {
    /// Number of consumers currently wired.
    pub fn consumer_count(&self) -> usize {
        self.inner
            .as_ref()
            .map(|s| s.running.lock().expect("broker registry").len())
            .unwrap_or(0)
    }

    /// Await-drain every consumer + release the channel-lease election (async shutdown).
    pub async fn drain(&self) {
        let Some(state) = &self.inner else {
            return;
        };
        for source in state.snapshot_sources() {
            let _ = source.stop().await;
        }
        if let Some(election) = &state.election {
            election.release_all().await;
        }
    }

    /// Detached stop of every consumer + lease release (the sync `shutdown` path).
    pub fn stop_all_detached(&self, runtime: &Handle) {
        let Some(state) = &self.inner else {
            return;
        };
        for source in state.snapshot_sources() {
            runtime.spawn(async move {
                let _ = source.stop().await;
            });
        }
        if let Some(election) = &state.election {
            let election = Arc::clone(election);
            runtime.spawn(async move { election.release_all().await });
        }
    }

    /// Broker-topology rewire on activation flip: reconcile the running consumers to
    /// the new ACTIVE `transport: kafka` inbound definitions. Consumers whose definition
    /// was removed or CHANGED stop (drain-postured), added/changed ones start on the new
    /// definition; unchanged ones keep running untouched. Idempotent — a no-change flip is a
    /// no-op. Non-fatal: a bad new definition WARNs and is skipped (the engine keeps running).
    pub async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        let Some(state) = &self.inner else {
            return;
        };

        // Desired consumer set from the new active broker inbound definitions. An
        // authored-config error means no consumer would boot either — skip (no churn).
        let mut desired: HashMap<ConsumerKey, (ChannelDefinition, KafkaChannelProperties)> =
            HashMap::new();
        for def in active_definitions
            .iter()
            .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        {
            let props = match KafkaChannelProperties::resolved(def, state.ctx.resolver) {
                Ok(props) => props,
                Err(d) => {
                    warn!(
                        code = %d.code,
                        "kafka consumer NOT started during flip: {}", d.message
                    );
                    continue;
                }
            };
            desired.insert(consumer_key(def), (def.clone(), props));
        }

        // Diff under the lock (no await held): the stop set + the spawn set.
        let (to_stop, to_spawn): (Vec<Arc<KafkaTriggerSource>>, Vec<ChannelDefinition>) = {
            let running = state.running.lock().expect("broker registry");
            let mut stop = Vec::new();
            for (key, consumer) in running.iter() {
                let unchanged = desired
                    .get(key)
                    .is_some_and(|(_, props)| *props == consumer.fingerprint);
                if !unchanged {
                    stop.push(Arc::clone(&consumer.source)); // removed or changed
                }
            }
            let mut spawn = Vec::new();
            for (key, (def, props)) in desired.iter() {
                let unchanged = running
                    .get(key)
                    .is_some_and(|consumer| consumer.fingerprint == *props);
                if !unchanged {
                    spawn.push(def.clone()); // added or changed
                }
            }
            (stop, spawn)
        };

        if to_stop.is_empty() && to_spawn.is_empty() {
            return;
        }

        // Stop changed/removed consumers first (drain posture: in-flight deliveries settle
        // their acks; at-least-once redelivery + inbox dedup absorb the brief handover gap).
        for source in &to_stop {
            if let Err(d) = source.stop().await {
                warn!(code = %d.code, "kafka consumer stop during flip: {}", d.message);
            }
        }
        // Drop the stopped (removed/changed) consumers from the registry.
        {
            let mut running = state.running.lock().expect("broker registry");
            running.retain(|key, consumer| {
                desired
                    .get(key)
                    .is_some_and(|(_, props)| *props == consumer.fingerprint)
            });
        }
        // Start the added/changed consumers on their new definitions.
        for def in &to_spawn {
            match build_and_start_consumer(def, &state.election, &state.ctx) {
                Ok(Some((key, consumer))) => {
                    state
                        .running
                        .lock()
                        .expect("broker registry")
                        .insert(key, consumer);
                }
                // A broker secret that does not resolve here: already WARNed, consumer skipped.
                Ok(None) => {}
                Err(d) => warn!(
                    code = %d.code,
                    "kafka consumer NOT started during flip: {}", d.message
                ),
            }
        }
        info!(
            stopped = to_stop.len(),
            started = to_spawn.len(),
            "kafka broker topology rewired on activation flip"
        );
    }
}

impl BrokerState {
    fn snapshot_sources(&self) -> Vec<Arc<KafkaTriggerSource>> {
        self.running
            .lock()
            .expect("broker registry")
            .values()
            .map(|c| Arc::clone(&c.source))
            .collect()
    }
}

fn consumer_key(def: &ChannelDefinition) -> ConsumerKey {
    (
        def.binding.tenant().to_string(),
        def.binding.namespace.module_key(),
        def.binding.channel_name.clone(),
    )
}

/// Construct + start ONE consumer from its definition (singleton gated), detaching `start`
/// onto the runtime (broker absence stays a background WARN + retry, never a boot
/// failure). Shared by boot and the flip rewire.
///
/// - `Err(diagnostic)` — authored config invalid (missing topic/bootstrap, bad
///   security.protocol): boot fails CLOSED (rewire WARNs and skips — the running engine
///   never crashes on a bad flip).
/// - `Ok(Some(entry))` — the started consumer + its registry key.
fn build_and_start_consumer(
    definition: &ChannelDefinition,
    election: &Option<Arc<DbLeaderElection>>,
    ctx: &BrokerRespawnContext,
) -> Result<Option<(ConsumerKey, RunningConsumer)>, Diagnostic> {
    let channel = definition.binding.channel_name.clone();
    let tenant = definition.binding.tenant().to_string();
    let properties = KafkaChannelProperties::resolved(definition, ctx.resolver)?;
    let fingerprint = properties.clone();

    let security = &properties.security;
    let (username, password, ca_pem) = match (
        resolve_security_reference(
            security.sasl_username.as_deref(),
            "sasl.username",
            &channel,
            ctx.resolver,
        ),
        resolve_security_reference(
            security.sasl_password.as_deref(),
            "sasl.password",
            &channel,
            ctx.resolver,
        ),
        resolve_security_reference(
            security.tls_ca_pem.as_deref(),
            "tls-ca-pem",
            &channel,
            ctx.resolver,
        ),
    ) {
        (Ok(u), Ok(p), Ok(c)) => (u, p, c),
        (Err(diagnostic), _, _) | (_, Err(diagnostic), _) | (_, _, Err(diagnostic)) => {
            warn!(
                channel = %channel,
                code = %diagnostic.code,
                "kafka consumer NOT started — a broker secret is unresolvable in this \
                 environment: {}",
                diagnostic.message
            );
            return Ok(None);
        }
    };
    let properties = properties.with_resolved_security(username, password, ca_pem);

    let singleton = properties.singleton;
    if singleton && ctx.pool.is_none() {
        warn!(
            channel = %channel,
            "singleton kafka channel has no engine datasource — no lease election is \
             possible; consuming on every replica (NoOp leadership posture)"
        );
    }
    let gate = match election {
        Some(e) if singleton => {
            ChannelLeadership::Elected(Arc::clone(e)).gate_for(&channel_role(&tenant, &channel))
        }
        _ => ChannelLeadership::AlwaysLeading.gate_for(""),
    };

    let mut source_config = KafkaSourceConfig::new(
        &tenant,
        &definition.binding.namespace.module_key(),
        &channel,
        properties,
    );
    // Per-message inbound auth (`inbound-auth.*`); the expected-key ref resolves via
    // the envref registry (env:/secret:/vault:). An unresolvable ref fails the channel closed.
    source_config.inbound_auth = sutra_channels::auth::BrokerInboundAuth::from_properties(
        &definition.properties,
        crate::kafka::codes::INBOUND_CONFIG_INVALID,
        ctx.resolver,
    )?;
    let source = Arc::new(KafkaTriggerSource::new(source_config)?);
    let intake = Arc::clone(&ctx.intake);
    let started = Arc::clone(&source);
    let channel_for_log = channel.clone();
    ctx.handle.spawn(async move {
        if let Err(diagnostic) = started.start(intake, gate).await {
            warn!(
                channel = %channel_for_log,
                code = %diagnostic.code,
                "kafka consumer failed to start: {}",
                diagnostic.message
            );
        }
    });
    info!(channel = %channel, tenant = %tenant, singleton, "kafka consumer wired");
    Ok(Some((
        consumer_key(definition),
        RunningConsumer {
            source,
            fingerprint,
        },
    )))
}

/// Resolve one authored secret reference. The resolver's error names the reference, not its value.
fn resolve_security_reference(
    reference: Option<&str>,
    key: &str,
    channel: &str,
    resolver: EnvRefResolver,
) -> Result<Option<String>, Diagnostic> {
    let Some(reference) = reference else {
        return Ok(None);
    };
    resolver(reference).map(Some).map_err(|e| {
        Diagnostic::error(
            crate::kafka::codes::INBOUND_CONFIG_INVALID,
            format!("kafka channel '{channel}' could not resolve the reference for '{key}': {e}"),
        )
    })
}

/// Construct + start one consumer per inbound `transport: kafka` definition, fail-closed on
/// authored errors. Returns the shared, rewireable [`KafkaChannels`].
pub fn spawn_kafka_channels(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<KafkaChannels, Diagnostic> {
    let intake: Arc<dyn InboundIntake> = Arc::new(EngineIntake::new(engine));
    spawn_kafka_channels_with_intake(definitions, intake, pool, resolver, handle)
}

/// The intake-injectable core of [`spawn_kafka_channels`] — the engine wraps its actor as
/// [`EngineIntake`], conformance tests inject a capturing intake. Same boot semantics + the
/// same shared, rewireable state.
pub(crate) fn spawn_kafka_channels_with_intake(
    definitions: &[ChannelDefinition],
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<KafkaChannels, Diagnostic> {
    let inbound: Vec<&ChannelDefinition> = definitions
        .iter()
        .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        .collect();

    // One election for all singleton roles, only when a datasource exists AND a boot
    // channel is `singleton: true`; the AlwaysLeading fallback is the no-election
    // posture. (v1 bound: a flip that introduces the FIRST singleton kafka channel where
    // none existed at boot falls back to AlwaysLeading for it — the election is boot-scoped.)
    let election: Option<Arc<DbLeaderElection>> = match &pool {
        Some(pool) if inbound.iter().any(|d| d.singleton()) => {
            Some(Arc::new(DbLeaderElection::with_defaults(
                Arc::new(PgLeaseHandle(PgLeaseStore::new(pool.clone()))),
                None,
                handle.clone(),
            )))
        }
        _ => None,
    };

    let ctx = BrokerRespawnContext {
        intake,
        pool,
        resolver,
        handle,
    };
    let mut running: HashMap<ConsumerKey, RunningConsumer> = HashMap::new();
    for definition in inbound {
        if let Some((key, consumer)) = build_and_start_consumer(definition, &election, &ctx)? {
            running.insert(key, consumer);
        }
    }
    Ok(KafkaChannels {
        inner: Some(Arc::new(BrokerState {
            running: Mutex::new(running),
            election,
            ctx,
        })),
    })
}

// ---- Neutral transport SPI wiring (domain-neutrality refactor) --------------
// The engine drives every transport through `TransportChannels` + composes them by
// iterating `transport_factories()`; this crate self-registers its factory below.

#[async_trait::async_trait]
impl TransportChannels for KafkaChannels {
    fn transport(&self) -> &str {
        TRANSPORT
    }
    fn consumer_count(&self) -> usize {
        KafkaChannels::consumer_count(self)
    }
    async fn rewire(&self, active: &[ChannelDefinition]) {
        KafkaChannels::rewire(self, active).await
    }
    async fn drain(&self) {
        KafkaChannels::drain(self).await
    }
    fn stop_all_detached(&self, runtime: &Handle) {
        KafkaChannels::stop_all_detached(self, runtime)
    }
}

/// Factory `spawn` adapter — widens the concrete [`KafkaChannels`] to the trait object.
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(spawn_kafka_channels(
        definitions,
        engine,
        pool,
        resolver,
        handle,
    )?))
}

/// Factory `register_sink` adapter — reads the `SUTRA_SINK_KAFKA_*` environment itself.
fn register_sink(registry: &mut SinkRegistry) {
    register_kafka_sink_with_security(
        registry,
        &sink_bootstrap_from_env(),
        KafkaSinkSecurity::from_env(),
    );
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // `ack-mode: on-complete` is honoured — the source defers its offset
        // commit through the engine's deferred-ack registry
        // (`InboundIntake::deliver_deferred`), committing at the per-partition LOW
        // WATERMARK so a settle can never commit past a still-parked earlier record
        // (see `kafka::source` "Deferred acking").
        handles_on_complete: true,
    }
}

/// Consumers in one group share its membership and rebalances whatever each reads, so a group
/// belongs to one deployment on any topic. Two deployments on one topic in different groups each
/// read every message, and that sharing is allowed: the topic is no part of the identity. An unset
/// `group.id` defaults from the channel name, so the same channel in two deployments lands in the
/// same group. Earlier engines let that happen, so deployments already sharing a group keep running
/// and only a new claimant is refused. The server list compares as a set: its order and host case
/// mean nothing to a client.
fn endpoint_claims(def: &ChannelDefinition) -> Vec<sutra_transport_spi::EndpointClaim> {
    let Ok(props) = KafkaChannelProperties::from_definition(def) else {
        return Vec::new();
    };
    if props.topic.is_empty() {
        return Vec::new();
    }
    let mut servers: Vec<String> = props
        .bootstrap_servers
        .split(',')
        .map(sutra_transport_spi::normalized_endpoint_host)
        .filter(|server| !server.is_empty())
        .collect();
    servers.sort();
    servers.dedup();
    vec![
        sutra_transport_spi::EndpointClaim::exclusive_to_new_deployments(
            format!("{} group {}", servers.join(","), props.group_id),
            "Set a group.id of its own on each channel; an unset group.id defaults to \
             sutra-<channel>, so the same channel name in two deployments lands in one group",
        ),
    ]
}

inventory::submit! {
    sutra_transport_spi::EndpointClaimFactory {
        transport: TRANSPORT,
        claims: endpoint_claims,
        listener_prefix: None,
        preflight: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_kafka_destinations_after_registration() {
        // The gate-defect guard: a registered kafka sink MUST resolve kafka:// destinations
        // (an UNREGISTERED broker sink poisons every outbound row).
        let mut registry = SinkRegistry::new();
        register_kafka_sink(&mut registry, "kafka:9092");
        assert!(registry
            .resolve("kafka://transfer-topic/customer-7")
            .is_some());
        assert!(registry.resolve("kafka://payment-replies").is_some());
        assert!(registry.resolve("https://host/cb").is_none());
        assert!(registry.resolve("rabbitmq://broker/q").is_none());
    }

    #[test]
    fn sink_registers_even_with_empty_bootstrap() {
        // Empty bootstrap still registers the scheme (rows resolve, then fail-closed
        // retryable) — the sink is never left UNREGISTERED.
        let mut registry = SinkRegistry::new();
        register_kafka_sink(&mut registry, "");
        assert!(registry.resolve("kafka://t").is_some());
    }
}

#[cfg(test)]
mod connection_references {
    use super::spawn_kafka_channels_with_intake;
    use crate::kafka::KafkaChannelProperties;
    use std::sync::Arc;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::{
        AckDecision, BoxFuture, ChannelDefinition, DeploymentId, InboundIntake, InboundMessage,
    };

    struct NoIntake;
    impl InboundIntake for NoIntake {
        fn deliver(&self, _message: InboundMessage) -> BoxFuture<'_, AckDecision> {
            Box::pin(async { AckDecision::Ack })
        }
    }

    /// Stands in for the engine's registry: the listed references resolve and any other does not.
    fn resolver(reference: &str) -> Result<String, String> {
        match reference {
            "${BOOTSTRAP}" => Ok("broker.test:9093".to_string()),
            "env:TOPIC" => Ok("resolved-topic".to_string()),
            "secret:group" => Ok("resolved-group".to_string()),
            "${CA_FILE}" => Ok("/etc/sutra/ca.pem".to_string()),
            r if r.contains("${") || r.starts_with("env:") || r.starts_with("secret:") => {
                Err(format!("{r} is not set"))
            }
            literal => Ok(literal.to_string()),
        }
    }

    /// A valid inbound definition with `extra` on top.
    fn definition(extra: &[(&str, &str)]) -> ChannelDefinition {
        let properties: std::collections::BTreeMap<String, String> = [
            ("bootstrap.servers", "broker:9092"),
            ("topic", "orders"),
            ("group.id", "g1"),
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
        ChannelDefinition {
            binding: ChannelBinding::new(
                "orders-in",
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("kafka".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties,
        }
    }

    #[test]
    fn references_reach_the_client_configuration() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let channels = spawn_kafka_channels_with_intake(
            &[definition(&[
                ("bootstrap.servers", "${BOOTSTRAP}"),
                ("topic", "env:TOPIC"),
                ("group.id", "secret:group"),
            ])],
            Arc::new(NoIntake),
            None,
            resolver,
            runtime.handle().clone(),
        )
        .unwrap_or_else(|d| panic!("{}", d.message));
        assert_eq!(channels.consumer_count(), 1);
        let state = channels.inner.as_ref().expect("wired");
        let running = state.running.lock().expect("registry");
        let props = &running.values().next().expect("one consumer").fingerprint;
        let client = crate::kafka::source::consumer_client_config(props);
        assert_eq!(client.get("bootstrap.servers"), Some("broker.test:9093"));
        assert_eq!(client.get("group.id"), Some("resolved-group"));
        assert_eq!(props.topic, "resolved-topic");
    }

    #[test]
    fn an_unresolved_reference_fails_wiring_with_the_code() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        for key in ["bootstrap.servers", "topic", "group.id"] {
            for unresolved in [
                "${UNSET_SETTING}",
                "env:UNSET_SETTING",
                "secret:unset-setting",
            ] {
                let def = definition(&[(key, unresolved)]);
                let Err(err) = spawn_kafka_channels_with_intake(
                    &[def],
                    Arc::new(NoIntake),
                    None,
                    resolver,
                    runtime.handle().clone(),
                ) else {
                    panic!("an unresolved reference in '{key}' must not wire");
                };
                assert_eq!(err.code, "SUTRA.INBOUND.KAFKA.CONFIG_INVALID", "for {key}");
                assert!(err.message.contains("orders-in"), "{}", err.message);
                assert!(err.message.contains(&format!("'{key}'")), "{}", err.message);
            }
        }
    }

    #[test]
    fn a_tls_ca_file_reference_resolves_and_an_unresolved_one_fails_wiring() {
        let props = KafkaChannelProperties::resolved(
            &definition(&[("security.protocol", "SSL"), ("tls-ca-file", "${CA_FILE}")]),
            resolver,
        )
        .expect("resolves");
        assert_eq!(
            props.security.tls_ca_file.as_deref(),
            Some("/etc/sutra/ca.pem")
        );
        let err = KafkaChannelProperties::resolved(
            &definition(&[("security.protocol", "SSL"), ("tls-ca-file", "${UNSET_CA}")]),
            resolver,
        )
        .expect_err("unresolved");
        assert_eq!(err.code, "SUTRA.INBOUND.KAFKA.CONFIG_INVALID");
        assert!(err.message.contains("'tls-ca-file'"), "{}", err.message);
    }

    #[test]
    fn sasl_credentials_accept_every_whole_reference_and_refuse_a_literal() {
        let sasl = |password: &str| {
            definition(&[
                ("security.protocol", "SASL_SSL"),
                ("sasl.mechanism", "PLAIN"),
                ("sasl.username", "${KAFKA_USER}"),
                ("sasl.password", password),
            ])
        };
        for reference in [
            "${KAFKA_PASS}",
            "${KAFKA_PASS:fallback}",
            "env:KAFKA_PASS",
            "secret:kafka-pass",
            "${secret:kafka-pass}",
            "k8s:secret/kafka#pass",
        ] {
            assert!(
                KafkaChannelProperties::resolved(&sasl(reference), resolver).is_ok(),
                "{reference}"
            );
        }
        for literal in ["hunter2", "pre${KAFKA_PASS}", "secret:"] {
            let err = KafkaChannelProperties::resolved(&sasl(literal), resolver)
                .expect_err("literal refused");
            assert_eq!(
                err.code, "SUTRA.STARTUP.REFUSED.HARDCODED_SECRET",
                "{literal}"
            );
        }
    }

    #[test]
    fn a_literal_connection_property_is_unaffected() {
        let literal = KafkaChannelProperties::resolved(&definition(&[]), resolver)
            .expect("literal values wire");
        let plain = KafkaChannelProperties::from_definition(&definition(&[])).expect("plain parse");
        assert_eq!(literal, plain);
    }
}

/// Broker-topology rewire conformance for Kafka (mirror of the rabbitmq conformance).
/// The two-phase flip is contract-only, and this IS the pin: on an
/// activation flip, a kafka consumer whose channel definition CHANGED stops and re-starts on
/// the new definition (the moved consumer serves subsequent messages with no loss); an
/// unchanged definition does not churn the consumer; removal stops it.
#[cfg(test)]
mod broker_rewire_conformance {
    use super::spawn_kafka_channels_with_intake;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::util::Timeout;
    use rdkafka::ClientConfig;
    use sutra_channels::{
        AckDecision, BoxFuture, ChannelBinding, ChannelDefinition, DeploymentId, InboundIntake,
        InboundMessage, Namespace,
    };

    /// Captures every delivered body and acks (so records are consumed, not redelivered).
    #[derive(Default)]
    struct CapturingIntake {
        delivered: std::sync::Mutex<Vec<Vec<u8>>>,
    }
    impl CapturingIntake {
        fn count(&self) -> usize {
            self.delivered.lock().expect("intake").len()
        }
    }
    impl InboundIntake for CapturingIntake {
        fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
            Box::pin(async move {
                self.delivered
                    .lock()
                    .expect("intake")
                    .push(message.body.into_inner());
                AckDecision::Ack
            })
        }
    }

    /// A `transport: kafka` inbound channel definition pointing at the test broker. The
    /// `kafka.consumer.client.id` passthrough is the fingerprint knob the "changed
    /// definition" arm flips (inert to consumption — only forces a consumer restart).
    fn broker_def(
        channel: &str,
        bootstrap: &str,
        topic: &str,
        group: &str,
        client_id: Option<&str>,
    ) -> ChannelDefinition {
        let mut properties = BTreeMap::new();
        properties.insert("bootstrap.servers".to_string(), bootstrap.to_string());
        properties.insert("topic".to_string(), topic.to_string());
        properties.insert("group.id".to_string(), group.to_string());
        properties.insert("auto.offset.reset".to_string(), "earliest".to_string());
        if let Some(id) = client_id {
            properties.insert("kafka.consumer.client.id".to_string(), id.to_string());
        }
        ChannelDefinition {
            binding: ChannelBinding::new(
                channel,
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("kafka".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties,
        }
    }

    async fn create_topic(bootstrap: &str, topic: &str) {
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .create()
            .expect("admin client");
        admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new(),
            )
            .await
            .expect("create topic");
    }

    async fn produce(bootstrap: &str, topic: &str, body: &[u8]) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("message.timeout.ms", "10000")
            .create()
            .expect("producer");
        producer
            .send(
                FutureRecord::to(topic).payload(body).key("k"),
                Timeout::After(Duration::from_secs(10)),
            )
            .await
            .expect("produce");
    }

    async fn wait_for_delivered(intake: &CapturingIntake, expected: usize, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if intake.count() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        panic!(
            "timed out waiting for {expected} deliveries (have {})",
            intake.count()
        );
    }

    static REFERENCED_BOOTSTRAP: std::sync::OnceLock<String> = std::sync::OnceLock::new();

    /// Answers for the broker address, which is only known once the container is up.
    fn referenced_settings(reference: &str) -> Result<String, String> {
        match reference {
            "${T2_BOOTSTRAP}" => Ok(REFERENCED_BOOTSTRAP.get().expect("started").clone()),
            "env:T2_TOPIC" => Ok(format!("reference-topic-{}", std::process::id())),
            "secret:t2-group" => Ok(format!("reference-group-{}", std::process::id())),
            other => Err(format!("{other} is not set")),
        }
    }

    /// A consumer whose whole connection comes from references really connects to the broker and
    /// consumes from the topic they name.
    #[test]
    #[ignore = "docker"]
    fn a_consumer_connects_and_consumes_through_references() {
        use testcontainers::runners::SyncRunner;
        use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

        let (container, port): (testcontainers::Container<Kafka>, u16) = std::thread::spawn(|| {
            let c = Kafka::default()
                .start()
                .expect("start apache/kafka-native (docker required)");
            sutra_testkit::reap_on_exit(c.id());
            let port = c.get_host_port_ipv4(KAFKA_PORT).expect("mapped kafka port");
            (c, port)
        })
        .join()
        .expect("broker bootstrap thread");
        let bootstrap = format!("127.0.0.1:{port}");
        REFERENCED_BOOTSTRAP.set(bootstrap.clone()).expect("once");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let intake = Arc::new(CapturingIntake::default());
        let dyn_intake: Arc<dyn InboundIntake> = intake.clone();

        rt.block_on(async {
            let topic = format!("reference-topic-{}", std::process::id());
            create_topic(&bootstrap, &topic).await;
            let def = broker_def(
                "orders",
                "${T2_BOOTSTRAP}",
                "env:T2_TOPIC",
                "secret:t2-group",
                None,
            );
            let channels = spawn_kafka_channels_with_intake(
                &[def],
                dyn_intake.clone(),
                None,
                referenced_settings,
                handle.clone(),
            )
            .expect("wire through references");
            assert_eq!(channels.consumer_count(), 1);
            produce(&bootstrap, &topic, b"through-a-reference").await;
            wait_for_delivered(&intake, 1, Duration::from_secs(30)).await;
            channels.drain().await;
        });

        drop(container);
    }

    #[test]
    #[ignore = "docker"]
    fn flip_moves_kafka_consumer_to_changed_definition_without_loss() {
        use testcontainers::runners::SyncRunner;
        use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

        // Blocking testcontainers runner on a dedicated thread (never inside a tokio worker).
        let (container, port): (testcontainers::Container<Kafka>, u16) = std::thread::spawn(|| {
            let c = Kafka::default()
                .start()
                .expect("start apache/kafka-native (docker required)");
            sutra_testkit::reap_on_exit(c.id());
            let port = c.get_host_port_ipv4(KAFKA_PORT).expect("mapped kafka port");
            (c, port)
        })
        .join()
        .expect("broker bootstrap thread");
        let bootstrap = format!("127.0.0.1:{port}");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let intake = Arc::new(CapturingIntake::default());
        let dyn_intake: Arc<dyn InboundIntake> = intake.clone();

        rt.block_on(async {
            let topic = format!("wsl-kafka-rewire-{}", std::process::id());
            let group = format!("g-{}", std::process::id());
            create_topic(&bootstrap, &topic).await;

            // Wire v1 (topic/group, no client.id) — one consumer comes up.
            let v1 = broker_def("orders", &bootstrap, &topic, &group, None);
            let channels = spawn_kafka_channels_with_intake(
                &[v1],
                dyn_intake.clone(),
                None,
                |r| Ok(r.to_string()),
                handle.clone(),
            )
            .expect("wire v1 kafka channel");
            assert_eq!(channels.consumer_count(), 1);
            produce(&bootstrap, &topic, b"msg-1").await;
            wait_for_delivered(&intake, 1, Duration::from_secs(30)).await;

            // FLIP to v2 — SAME topic/group, CHANGED client.id (definition changed): the
            // consumer stops and re-starts on the new definition, resuming from the committed
            // offset (msg-1 was acked ⇒ committed), so no loss across the flip.
            let v2 = broker_def("orders", &bootstrap, &topic, &group, Some("svc-2"));
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            produce(&bootstrap, &topic, b"msg-2").await;
            wait_for_delivered(&intake, 2, Duration::from_secs(30)).await;

            // Idempotent: rewiring to an IDENTICAL definition does not churn the consumer.
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            produce(&bootstrap, &topic, b"msg-3").await;
            wait_for_delivered(&intake, 3, Duration::from_secs(30)).await;

            // Removing the channel from the active set stops the consumer.
            channels.rewire(&[]).await;
            assert_eq!(channels.consumer_count(), 0);

            // Exactly the three produced messages were delivered — no loss across the flip.
            assert_eq!(
                intake.count(),
                3,
                "every produced message delivered across the rewire"
            );
        });

        drop(container);
    }
}
