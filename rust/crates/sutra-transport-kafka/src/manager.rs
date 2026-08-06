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
//! non-fatal on a bad new def; drain on shutdown). Unlike RabbitMQ there are no channel-YAML
//! credentials (Kafka authenticates at the broker layer; this build is PLAINTEXT), so the
//! credential-resolution step is absent.

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
    KafkaChannelProperties, KafkaMessageSink, KafkaSourceConfig, KafkaTriggerSource, TRANSPORT,
};

/// The env var naming the engine-wide Kafka sink bootstrap servers.
/// Empty/unset ⇒ the sink still registers
/// (so `kafka://` destinations resolve) but publishes fail-closed retryable until set.
pub const SINK_BOOTSTRAP_ENV: &str = "SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS";

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
            let Ok(props) = KafkaChannelProperties::from_definition(def) else {
                continue;
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
                // Kafka has no credential-skip path (no channel-YAML secrets); the Option
                // mirrors rabbitmq's signature so a future SASL case slots in here.
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
    let properties = KafkaChannelProperties::from_definition(definition)?;
    let fingerprint = properties.clone();

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

/// Factory `register_sink` adapter — reads `SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS` itself.
fn register_sink(registry: &mut SinkRegistry) {
    register_kafka_sink(registry, &sink_bootstrap_from_env());
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
