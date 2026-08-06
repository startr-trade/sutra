//! RabbitMQ channel wiring — the one assembly touch-point for broker
//! consumers: for every `transport: rabbitmq` channel definition this constructs a
//! [`RabbitMqTriggerSource`] (credentials env-resolved), gates it (`singleton: true` ⇒
//! a [`crate::leadership::DbLeaderElection`] lease role
//! `sutra-channel:<tenant>:<channel>`; otherwise / no datasource ⇒ always-leading), and
//! starts it against an [`InboundIntake`] adapter over the engine actor.
//!
//! Kept deliberately separate from `assembly.rs` (the engine-assembly merge point): assembly calls
//! [`spawn_rabbitmq_channels`] once and moves on. Consumers detach onto the runtime —
//! broker absence is NON-FATAL: a missing broker WARNs and retries in the
//! background without affecting readiness. The Kafka transport is wired the same way by the
//! sibling [`crate::kafka`] module (`transport: kafka` channels — e.g. the money-transfer
//! example's `transfer-topic` — now get a real consumer; an unreachable broker is the same
//! background WARN + retry, never a boot failure).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::rabbitmq::{
    RabbitMqChannelProperties, RabbitMqMessageSink, RabbitMqSourceConfig, RabbitMqTriggerSource,
    TRANSPORT,
};
use sqlx::PgPool;
use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, Diagnostic, InboundIntake, SinkRegistry, TriggerSource};
use sutra_persistence::stores::PgLeaseStore;
use sutra_transport_spi::{EngineIntake, EnvRefResolver, TransportChannels, TransportFactory};
use tokio::runtime::Handle;
use tracing::{info, warn};

use sutra_transport_spi::leadership::{
    channel_role, ChannelLeadership, DbLeaderElection, PgLeaseHandle,
};

/// Register the RabbitMQ sink into an outbox [`SinkRegistry`] (the outbox dispatcher's
/// resolution surface) under its claimed schemes (`rabbitmq`, `amqp`).
pub fn register_rabbitmq_sink(registry: &mut SinkRegistry) {
    registry.register(Arc::new(RabbitMqMessageSink::new()));
}

/// The wired broker consumers + everything needed to re-start them. SHARED (`Arc`) between
/// the engine (shutdown drain) and the activation watcher (topology rewire on flip):
/// [`RabbitMqChannels::rewire`] reconciles the running consumers to a new active definition
/// set, stopping consumers whose definition changed (or was removed) and starting the new
/// ones — unchanged consumers keep running. `stop` is drain-postured and at-least-once +
/// inbox dedup absorb any redelivery over the brief handover, so a flip loses no messages.
#[derive(Clone, Default)]
pub struct RabbitMqChannels {
    inner: Option<Arc<BrokerState>>,
}

/// `(tenant, module_key, channel)` — the stable identity of one broker consumer across flips.
type ConsumerKey = (String, String, String);

struct RunningConsumer {
    source: Arc<RabbitMqTriggerSource>,
    /// Pre-credential-resolution properties — the authored-config fingerprint used to detect
    /// a changed definition on flip (`RabbitMqChannelProperties: Eq`).
    fingerprint: RabbitMqChannelProperties,
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
    /// The `env:`/`secret:`/`vault:` reference resolver, injected by the engine (broker
    /// credentials + inbound-auth resolve through it, so this crate deps on no engine module).
    resolver: EnvRefResolver,
    handle: Handle,
}

impl RabbitMqChannels {
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
    /// the new ACTIVE `transport: rabbitmq` inbound definitions. Consumers whose definition
    /// was removed or CHANGED stop (drain-postured), added/changed ones start on the new
    /// definition; unchanged ones keep running untouched. Idempotent — a no-change flip is a
    /// no-op. Non-fatal: a bad new definition WARNs and is skipped (the engine keeps running).
    pub async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        let Some(state) = &self.inner else {
            return;
        };

        // Desired consumer set from the new active broker inbound definitions. An
        // authored-config error means no consumer would boot either — skip (no churn).
        let mut desired: HashMap<ConsumerKey, (ChannelDefinition, RabbitMqChannelProperties)> =
            HashMap::new();
        for def in active_definitions
            .iter()
            .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        {
            let Ok(props) = RabbitMqChannelProperties::from_definition(def) else {
                continue;
            };
            desired.insert(consumer_key(def), (def.clone(), props));
        }

        // Diff under the lock (no await held): the stop set + the spawn set.
        let (to_stop, to_spawn): (Vec<Arc<RabbitMqTriggerSource>>, Vec<ChannelDefinition>) = {
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
                warn!(code = %d.code, "rabbitmq consumer stop during flip: {}", d.message);
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
                Ok(None) => {} // credentials unresolvable in this env — WARN'd, skipped
                Err(d) => warn!(
                    code = %d.code,
                    "rabbitmq consumer NOT started during flip: {}", d.message
                ),
            }
        }
        info!(
            stopped = to_stop.len(),
            started = to_spawn.len(),
            "rabbitmq broker topology rewired on activation flip"
        );
    }
}

impl BrokerState {
    fn snapshot_sources(&self) -> Vec<Arc<RabbitMqTriggerSource>> {
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

/// Construct + start ONE consumer from its definition (credentials resolved, singleton
/// gated), detaching `start` onto the runtime (broker absence stays a background WARN +
/// retry, never a boot failure). Shared by boot and the flip rewire.
///
/// - `Err(diagnostic)` — authored config invalid (literal credentials, bad port): boot
///   fails CLOSED (rewire WARNs and skips — the running engine never crashes on a bad flip).
/// - `Ok(None)` — a credential REFERENCE resolves to nothing in THIS environment (the
///   broker-absent deployment shape): WARN + skip, never block readiness.
/// - `Ok(Some(entry))` — the started consumer + its registry key.
fn build_and_start_consumer(
    definition: &ChannelDefinition,
    election: &Option<Arc<DbLeaderElection>>,
    ctx: &BrokerRespawnContext,
) -> Result<Option<(ConsumerKey, RunningConsumer)>, Diagnostic> {
    let channel = definition.binding.channel_name.clone();
    let tenant = definition.binding.tenant().to_string();
    let properties = RabbitMqChannelProperties::from_definition(definition)?;
    let fingerprint = properties.clone();

    let (username, password) = match (
        resolve_credential(
            properties.username.as_deref(),
            "username",
            &channel,
            ctx.resolver,
        ),
        resolve_credential(
            properties.password.as_deref(),
            "password",
            &channel,
            ctx.resolver,
        ),
    ) {
        (Ok(u), Ok(p)) => (u, p),
        (Err(diagnostic), _) | (_, Err(diagnostic)) => {
            warn!(
                channel = %channel,
                code = %diagnostic.code,
                "rabbitmq consumer NOT started — broker credentials unresolvable in \
                 this environment: {}",
                diagnostic.message
            );
            return Ok(None);
        }
    };
    let resolved = properties.with_credentials(username, password);

    let singleton = resolved.singleton;
    if singleton && ctx.pool.is_none() {
        warn!(
            channel = %channel,
            "singleton rabbitmq channel has no engine datasource — no lease election \
             is possible; consuming on every replica (NoOp leadership posture)"
        );
    }
    let gate = match election {
        Some(e) if singleton => {
            ChannelLeadership::Elected(Arc::clone(e)).gate_for(&channel_role(&tenant, &channel))
        }
        _ => ChannelLeadership::AlwaysLeading.gate_for(""),
    };

    let mut source_config = RabbitMqSourceConfig::new(
        &tenant,
        &definition.binding.namespace.module_key(),
        &channel,
        resolved,
    );
    // Per-message inbound auth (`inbound-auth.*`); the expected-key ref resolves via
    // the envref registry (env:/secret:/vault:). An unresolvable ref fails the channel closed.
    source_config.inbound_auth = sutra_channels::auth::BrokerInboundAuth::from_properties(
        &definition.properties,
        crate::rabbitmq::codes::INBOUND_CONFIG_INVALID,
        ctx.resolver,
    )?;
    let source = Arc::new(RabbitMqTriggerSource::new(source_config)?);
    let intake = Arc::clone(&ctx.intake);
    let started = Arc::clone(&source);
    let channel_for_log = channel.clone();
    ctx.handle.spawn(async move {
        if let Err(diagnostic) = started.start(intake, gate).await {
            warn!(
                channel = %channel_for_log,
                code = %diagnostic.code,
                "rabbitmq consumer failed to start: {}",
                diagnostic.message
            );
        }
    });
    info!(channel = %channel, tenant = %tenant, singleton, "rabbitmq consumer wired");
    Ok(Some((
        consumer_key(definition),
        RunningConsumer {
            source,
            fingerprint,
        },
    )))
}

/// Construct + start one consumer per inbound `transport: rabbitmq` definition. Credentials
/// (`${ENV}` / `env:NAME` references validated by the properties parser) resolve here,
/// fail-closed on authored errors. Returns the shared, rewireable [`RabbitMqChannels`].
pub fn spawn_rabbitmq_channels(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<RabbitMqChannels, Diagnostic> {
    let intake: Arc<dyn InboundIntake> = Arc::new(EngineIntake::new(engine));
    spawn_broker_channels_with_intake(definitions, intake, pool, resolver, handle)
}

/// The intake-injectable core of [`spawn_rabbitmq_channels`] — the engine wraps its actor as
/// [`EngineIntake`], conformance tests inject a capturing intake. Same boot semantics + the
/// same shared, rewireable state.
pub(crate) fn spawn_broker_channels_with_intake(
    definitions: &[ChannelDefinition],
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<RabbitMqChannels, Diagnostic> {
    let inbound: Vec<&ChannelDefinition> = definitions
        .iter()
        .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        .collect();

    // One election for all singleton roles, only when a datasource exists AND a boot
    // channel is `singleton: true`; the AlwaysLeading fallback is the no-election
    // posture. (v1 bound: a flip that introduces the FIRST singleton broker channel where
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
    Ok(RabbitMqChannels {
        inner: Some(Arc::new(BrokerState {
            running: Mutex::new(running),
            election,
            ctx,
        })),
    })
}

/// Resolve one broker credential reference to its concrete value — `${ENV}` /
/// `${ENV:default}` placeholders and `env:NAME` secret-refs (the two Rust-engine
/// indirection forms); `k8s:secret/…` refs have no resolver in the Rust engine yet and
/// fail closed rather than connecting with a wrong literal.
fn resolve_credential(
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
            crate::rabbitmq::codes::INBOUND_CONNECTION_FAILED,
            format!(
                "rabbitmq channel '{channel}' could not resolve broker credential \
                 reference for '{key}': {e}"
            ),
        )
    })
}

// ---- Neutral transport SPI wiring (domain-neutrality refactor) --------------

#[async_trait::async_trait]
impl TransportChannels for RabbitMqChannels {
    fn transport(&self) -> &str {
        TRANSPORT
    }
    fn consumer_count(&self) -> usize {
        RabbitMqChannels::consumer_count(self)
    }
    async fn rewire(&self, active: &[ChannelDefinition]) {
        RabbitMqChannels::rewire(self, active).await
    }
    async fn drain(&self) {
        RabbitMqChannels::drain(self).await
    }
    fn stop_all_detached(&self, runtime: &Handle) {
        RabbitMqChannels::stop_all_detached(self, runtime)
    }
}

/// Factory `spawn` adapter — widens the concrete [`RabbitMqChannels`] to the trait object.
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(spawn_rabbitmq_channels(
        definitions,
        engine,
        pool,
        resolver,
        handle,
    )?))
}

/// Factory `register_sink` adapter — the rabbitmq sink carries no engine-wide env config
/// (broker host rides the `rabbitmq://` / `amqp://` URI).
fn register_sink(registry: &mut SinkRegistry) {
    register_rabbitmq_sink(registry);
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // `ack-mode: on-complete` is honoured — the source defers its
        // basic.ack/basic.nack through the engine's deferred-ack registry
        // (`InboundIntake::deliver_deferred`; see `rabbitmq::source`).
        handles_on_complete: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_rabbitmq_destinations_after_registration() {
        let mut registry = SinkRegistry::new();
        register_rabbitmq_sink(&mut registry);
        assert!(registry
            .resolve("rabbitmq://broker:5672/orders.response.out")
            .is_some());
        assert!(registry.resolve("amqp://broker/q").is_some());
        assert!(registry.resolve("https://host/cb").is_none());
    }

    #[test]
    fn resolve_credential_delegates_to_the_resolver_and_wraps_outcomes() {
        // The envref FORMS (`${…}` / `env:` / unset-fails-closed) are exercised in the
        // engine's envref module; resolve_credential's OWN contract is pinned here: a
        // missing reference -> Ok(None), a resolved reference -> Some, and a resolver error
        // -> a closed INBOUND_CONNECTION_FAILED diagnostic (never a wrong-literal connect).
        fn ok(reference: &str) -> Result<String, String> {
            Ok(format!("resolved:{reference}"))
        }
        fn fails(_reference: &str) -> Result<String, String> {
            Err("no value".to_string())
        }
        assert_eq!(
            resolve_credential(None, "username", "ch", ok).unwrap(),
            None
        );
        assert_eq!(
            resolve_credential(Some("secret:svc-user"), "username", "ch", ok)
                .unwrap()
                .as_deref(),
            Some("resolved:secret:svc-user")
        );
        let err = resolve_credential(Some("${MISSING}"), "password", "ch", fails).unwrap_err();
        assert_eq!(err.code, crate::rabbitmq::codes::INBOUND_CONNECTION_FAILED);
    }
}

/// Broker-topology rewire conformance (audit follow-up 5c) — a REAL RabbitMQ broker.
/// The two-phase flip is contract-only, and this IS the pin: on an
/// activation flip, a broker consumer whose channel definition CHANGED stops and re-starts
/// on the new definition (the moved consumer serves subsequent messages with no loss);
/// an unchanged definition does not churn the consumer; removal stops it.
#[cfg(test)]
mod broker_rewire_conformance {
    use super::spawn_broker_channels_with_intake;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
    use lapin::types::FieldTable;
    use lapin::{BasicProperties, Connection, ConnectionProperties};
    use sutra_channels::{
        AckDecision, BoxFuture, ChannelBinding, ChannelDefinition, DeploymentId, InboundIntake,
        InboundMessage, Namespace,
    };

    /// Captures every delivered body and acks (so messages are consumed, not redelivered).
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

    /// A `transport: rabbitmq` inbound channel definition pointing at the test broker. The
    /// `prefetch-count` is the fingerprint knob the "changed definition" arm flips.
    fn broker_def(channel: &str, port: u16, queue: &str, prefetch: u16) -> ChannelDefinition {
        let mut properties = BTreeMap::new();
        properties.insert("host".to_string(), "127.0.0.1".to_string());
        properties.insert("port".to_string(), port.to_string());
        properties.insert("queue".to_string(), queue.to_string());
        properties.insert("prefetch-count".to_string(), prefetch.to_string());
        ChannelDefinition {
            binding: ChannelBinding::new(
                channel,
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("rabbitmq".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties,
        }
    }

    async fn connect(port: u16) -> Connection {
        Connection::connect(
            &format!("amqp://127.0.0.1:{port}"),
            ConnectionProperties::default(),
        )
        .await
        .expect("AMQP connection")
    }

    async fn declare_queue(port: u16, queue: &str) {
        let conn = connect(port).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.queue_declare(
            queue,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare");
        conn.close(200, "declared").await.ok();
    }

    async fn publish(port: u16, queue: &str, body: &[u8]) {
        let conn = connect(port).await;
        let ch = conn.create_channel().await.expect("channel");
        ch.basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            body,
            BasicProperties::default(),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
        conn.close(200, "published").await.ok();
    }

    async fn consumer_count(port: u16, queue: &str) -> u32 {
        let conn = connect(port).await;
        let ch = conn.create_channel().await.expect("channel");
        let q = ch
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .expect("queue declare (count)");
        let n = q.consumer_count();
        conn.close(200, "counted").await.ok();
        n
    }

    async fn wait_for_consumer_count(port: u16, queue: &str, expected: u32, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if consumer_count(port, queue).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!("timed out waiting for consumer count {expected} on {queue}");
    }

    async fn wait_for_delivered(intake: &CapturingIntake, expected: usize, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if intake.count() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "timed out waiting for {expected} deliveries (have {})",
            intake.count()
        );
    }

    #[test]
    #[ignore = "docker"]
    fn flip_moves_broker_consumer_to_changed_definition_without_loss() {
        // Blocking testcontainers runner on a dedicated thread (never inside a tokio worker).
        let (container, port): (testcontainers::Container<testcontainers::GenericImage>, u16) =
            std::thread::spawn(|| {
                use testcontainers::core::{IntoContainerPort, WaitFor};
                use testcontainers::runners::SyncRunner;
                use testcontainers::GenericImage;
                let c = GenericImage::new("rabbitmq", "3.13-management-alpine")
                    .with_exposed_port(5672.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
                    .start()
                    .expect("start rabbitmq:3.13-management-alpine (docker required)");
                sutra_testkit::reap_on_exit(c.id());
                let port = c.get_host_port_ipv4(5672).expect("mapped 5672");
                (c, port)
            })
            .join()
            .expect("broker bootstrap thread");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let intake = Arc::new(CapturingIntake::default());
        let dyn_intake: Arc<dyn InboundIntake> = intake.clone();

        rt.block_on(async {
            let queue = format!("wsl-rewire-{}", std::process::id());
            declare_queue(port, &queue).await;

            // Wire v1 (queue, prefetch 1) — one consumer comes up.
            let v1 = broker_def("orders", port, &queue, 1);
            let channels = spawn_broker_channels_with_intake(
                &[v1],
                dyn_intake.clone(),
                None,
                |r| Ok(r.to_string()),
                handle.clone(),
            )
            .expect("wire v1 broker channel");
            wait_for_consumer_count(port, &queue, 1, Duration::from_secs(25)).await;
            publish(port, &queue, b"msg-1").await;
            wait_for_delivered(&intake, 1, Duration::from_secs(20)).await;

            // FLIP to v2 — SAME queue, CHANGED prefetch (definition changed): the consumer
            // stops and re-starts on the new definition; still exactly one consumer on Q.
            let v2 = broker_def("orders", port, &queue, 5);
            channels.rewire(std::slice::from_ref(&v2)).await;
            wait_for_consumer_count(port, &queue, 1, Duration::from_secs(25)).await;
            // The moved consumer serves the next message — no loss across the flip.
            publish(port, &queue, b"msg-2").await;
            wait_for_delivered(&intake, 2, Duration::from_secs(20)).await;

            // Idempotent: rewiring to an IDENTICAL definition does not churn the consumer.
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            publish(port, &queue, b"msg-3").await;
            wait_for_delivered(&intake, 3, Duration::from_secs(20)).await;

            // Removing the channel from the active set stops the consumer.
            channels.rewire(&[]).await;
            wait_for_consumer_count(port, &queue, 0, Duration::from_secs(25)).await;
            assert_eq!(channels.consumer_count(), 0);

            // Exactly the three published messages were delivered — zero loss, no double
            // delivery beyond at-least-once (each was acked, so none redelivered).
            assert_eq!(
                intake.count(),
                3,
                "every published message delivered exactly once"
            );
        });

        drop(container);
    }
}
