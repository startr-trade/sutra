//! GCP Pub/Sub channel wiring — the assembly touch-point for `transport: gcp-pubsub`
//! broker consumers: for every inbound `transport: gcp-pubsub` channel definition this
//! constructs a [`GcpPubSubTriggerSource`], gates it (`singleton: true` ⇒ a
//! [`crate::leadership::DbLeaderElection`] lease role `sutra-channel:<tenant>:<channel>`;
//! otherwise / no datasource ⇒ always-leading), and starts it against an [`InboundIntake`]
//! adapter over the engine actor.
//!
//! Mirrors [`crate::kafka`]: assembly calls [`spawn_gcp_pubsub_channels`] once and moves on;
//! consumers detach onto the runtime (broker absence is NON-FATAL), and the shared,
//! rewireable [`GcpPubSubChannels`] rides the activation flip via
//! [`GcpPubSubChannels::rewire`] with the same semantics (stop changed/removed, start added,
//! keep unchanged; idempotent; non-fatal on a bad new def; drain on shutdown). Like Kafka
//! there are no channel-YAML credentials (Pub/Sub authenticates via ADC / the emulator; the
//! per-channel `credentials-ref` is deferred), so the credential-resolution step is
//! absent.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::gcp_pubsub::{
    GcpPubSubChannelProperties, GcpPubSubMessageSink, GcpPubSubSourceConfig,
    GcpPubSubTriggerSource, TRANSPORT,
};
use sqlx::PgPool;
use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, Diagnostic, InboundIntake, SinkRegistry, TriggerSource};
use sutra_persistence::stores::PgLeaseStore;
use tokio::runtime::Handle;
use tracing::{info, warn};

use sutra_transport_spi::leadership::{
    channel_role, ChannelLeadership, DbLeaderElection, PgLeaseHandle,
};
use sutra_transport_spi::{EngineIntake, EnvRefResolver, TransportChannels, TransportFactory};

/// The env var naming the engine-wide Pub/Sub sink project. Empty/unset ⇒ the sink still
/// registers (so `gcp-pubsub://` destinations resolve) but publishes fail-closed retryable
/// until set.
pub const SINK_PROJECT_ENV: &str = "SUTRA_SINK_GCP_PUBSUB_PROJECT_ID";
/// The env var overriding the SDK endpoint (the emulator host). Empty/unset ⇒ the client
/// honours `PUBSUB_EMULATOR_HOST` else real GCP.
pub const SINK_ENDPOINT_ENV: &str = "SUTRA_SINK_GCP_PUBSUB_ENDPOINT";

/// Register the Pub/Sub sink into an outbox [`SinkRegistry`] (the outbox dispatcher's
/// resolution surface) under its claimed scheme (`gcp-pubsub`), targeting `project_id`.
pub fn register_gcp_pubsub_sink(
    registry: &mut SinkRegistry,
    project_id: &str,
    endpoint_override: Option<String>,
) {
    registry.register(Arc::new(GcpPubSubMessageSink::new(
        project_id,
        endpoint_override,
    )));
}

/// The engine-wide Pub/Sub sink config from the environment (empty project when unset).
pub fn sink_config_from_env() -> (String, Option<String>) {
    let project = std::env::var(SINK_PROJECT_ENV).unwrap_or_default();
    let endpoint = std::env::var(SINK_ENDPOINT_ENV)
        .ok()
        .filter(|e| !e.trim().is_empty());
    (project, endpoint)
}

/// The wired Pub/Sub consumers + everything needed to re-start them. SHARED (`Arc`) between
/// the engine (shutdown drain) and the activation watcher (topology rewire on flip):
/// [`GcpPubSubChannels::rewire`] reconciles the running consumers to a new active definition
/// set, stopping consumers whose definition changed (or was removed) and starting the new
/// ones — unchanged consumers keep running. `stop` is drain-postured and at-least-once +
/// inbox dedup absorb any redelivery over the brief handover, so a flip loses no messages.
#[derive(Clone, Default)]
pub struct GcpPubSubChannels {
    inner: Option<Arc<BrokerState>>,
}

/// `(tenant, module_key, channel)` — the stable identity of one broker consumer across flips.
type ConsumerKey = (String, String, String);

struct RunningConsumer {
    source: Arc<GcpPubSubTriggerSource>,
    /// The authored-config fingerprint used to detect a changed definition on flip
    /// (`GcpPubSubChannelProperties: Eq`).
    fingerprint: GcpPubSubChannelProperties,
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
    /// The `env:`/`secret:`/`vault:` reference resolver, injected by the engine.
    resolver: EnvRefResolver,
    handle: Handle,
}

impl GcpPubSubChannels {
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
    /// the new ACTIVE `transport: gcp-pubsub` inbound definitions. Consumers whose definition
    /// was removed or CHANGED stop (drain-postured), added/changed ones start on the new
    /// definition; unchanged ones keep running untouched. Idempotent — a no-change flip is a
    /// no-op. Non-fatal: a bad new definition WARNs and is skipped (the engine keeps running).
    pub async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        let Some(state) = &self.inner else {
            return;
        };

        // Desired consumer set from the new active broker inbound definitions. An
        // authored-config error means no consumer would boot either — skip (no churn).
        let mut desired: HashMap<ConsumerKey, (ChannelDefinition, GcpPubSubChannelProperties)> =
            HashMap::new();
        for def in active_definitions
            .iter()
            .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        {
            let Ok(props) = GcpPubSubChannelProperties::from_definition(def) else {
                continue;
            };
            desired.insert(consumer_key(def), (def.clone(), props));
        }

        // Diff under the lock (no await held): the stop set + the spawn set.
        let (to_stop, to_spawn): (Vec<Arc<GcpPubSubTriggerSource>>, Vec<ChannelDefinition>) = {
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
                warn!(code = %d.code, "gcp-pubsub consumer stop during flip: {}", d.message);
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
                // Pub/Sub has no credential-skip path (no channel-YAML secrets); the Option
                // mirrors rabbitmq's signature so a future credentials-ref case slots in here.
                Ok(None) => {}
                Err(d) => warn!(
                    code = %d.code,
                    "gcp-pubsub consumer NOT started during flip: {}", d.message
                ),
            }
        }
        info!(
            stopped = to_stop.len(),
            started = to_spawn.len(),
            "gcp-pubsub broker topology rewired on activation flip"
        );
    }
}

impl BrokerState {
    fn snapshot_sources(&self) -> Vec<Arc<GcpPubSubTriggerSource>> {
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
/// - `Err(diagnostic)` — authored config invalid (missing project-id/subscription): boot
///   fails CLOSED (rewire WARNs and skips — the running engine never crashes on a bad flip).
/// - `Ok(Some(entry))` — the started consumer + its registry key.
fn build_and_start_consumer(
    definition: &ChannelDefinition,
    election: &Option<Arc<DbLeaderElection>>,
    ctx: &BrokerRespawnContext,
) -> Result<Option<(ConsumerKey, RunningConsumer)>, Diagnostic> {
    let channel = definition.binding.channel_name.clone();
    let tenant = definition.binding.tenant().to_string();
    let properties = GcpPubSubChannelProperties::from_definition(definition)?;
    let fingerprint = properties.clone();

    let singleton = properties.singleton;
    if singleton && ctx.pool.is_none() {
        warn!(
            channel = %channel,
            "singleton gcp-pubsub channel has no engine datasource — no lease election is \
             possible; consuming on every replica (NoOp leadership posture)"
        );
    }
    let gate = match election {
        Some(e) if singleton => {
            ChannelLeadership::Elected(Arc::clone(e)).gate_for(&channel_role(&tenant, &channel))
        }
        _ => ChannelLeadership::AlwaysLeading.gate_for(""),
    };

    let mut source_config = GcpPubSubSourceConfig::new(
        &tenant,
        &definition.binding.namespace.module_key(),
        &channel,
        properties,
    );
    // Per-message inbound auth (`inbound-auth.*`); the expected-key ref resolves via
    // the envref registry (env:/secret:/vault:). An unresolvable ref fails the channel closed.
    source_config.inbound_auth = sutra_channels::auth::BrokerInboundAuth::from_properties(
        &definition.properties,
        crate::gcp_pubsub::codes::INBOUND_CONFIG_INVALID,
        ctx.resolver,
    )?;
    let source = Arc::new(GcpPubSubTriggerSource::new(source_config)?);
    let intake = Arc::clone(&ctx.intake);
    let started = Arc::clone(&source);
    let channel_for_log = channel.clone();
    ctx.handle.spawn(async move {
        if let Err(diagnostic) = started.start(intake, gate).await {
            warn!(
                channel = %channel_for_log,
                code = %diagnostic.code,
                "gcp-pubsub consumer failed to start: {}",
                diagnostic.message
            );
        }
    });
    info!(channel = %channel, tenant = %tenant, singleton, "gcp-pubsub consumer wired");
    Ok(Some((
        consumer_key(definition),
        RunningConsumer {
            source,
            fingerprint,
        },
    )))
}

/// Construct + start one consumer per inbound `transport: gcp-pubsub` definition, fail-closed
/// on authored errors. Returns the shared, rewireable [`GcpPubSubChannels`].
pub fn spawn_gcp_pubsub_channels(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<GcpPubSubChannels, Diagnostic> {
    let intake: Arc<dyn InboundIntake> = Arc::new(EngineIntake::new(engine));
    spawn_gcp_pubsub_channels_with_intake(definitions, intake, pool, resolver, handle)
}

/// The intake-injectable core of [`spawn_gcp_pubsub_channels`] — the engine wraps its actor
/// as [`EngineIntake`], conformance tests inject a capturing intake. Same boot semantics + the
/// same shared, rewireable state.
pub(crate) fn spawn_gcp_pubsub_channels_with_intake(
    definitions: &[ChannelDefinition],
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<GcpPubSubChannels, Diagnostic> {
    let inbound: Vec<&ChannelDefinition> = definitions
        .iter()
        .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        .collect();

    // One election for all singleton roles, only when a datasource exists AND a boot
    // channel is `singleton: true`; the AlwaysLeading fallback is the no-election posture.
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
    Ok(GcpPubSubChannels {
        inner: Some(Arc::new(BrokerState {
            running: Mutex::new(running),
            election,
            ctx,
        })),
    })
}

// ---- Neutral transport SPI wiring (domain-neutrality refactor) --------------

#[async_trait::async_trait]
impl TransportChannels for GcpPubSubChannels {
    fn transport(&self) -> &str {
        TRANSPORT
    }
    fn consumer_count(&self) -> usize {
        GcpPubSubChannels::consumer_count(self)
    }
    async fn rewire(&self, active: &[ChannelDefinition]) {
        GcpPubSubChannels::rewire(self, active).await
    }
    async fn drain(&self) {
        GcpPubSubChannels::drain(self).await
    }
    fn stop_all_detached(&self, runtime: &Handle) {
        GcpPubSubChannels::stop_all_detached(self, runtime)
    }
}

/// Factory `spawn` adapter — widens the concrete [`GcpPubSubChannels`] to the trait object.
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(spawn_gcp_pubsub_channels(
        definitions,
        engine,
        pool,
        resolver,
        handle,
    )?))
}

/// Factory `register_sink` adapter — reads the `SUTRA_SINK_GCP_PUBSUB_*` env config itself.
fn register_sink(registry: &mut SinkRegistry) {
    let (project, endpoint) = sink_config_from_env();
    register_gcp_pubsub_sink(registry, &project, endpoint);
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // WIRED — an `ack-mode: on-complete` inbound definition routes through
        // `InboundIntake::deliver_deferred`, and a parked instance's held ack fires from
        // the deferred-ack registry at its terminal event. Bounded by the subscription's
        // ack deadline (see the `gcp_pubsub::source` module docs: no lease-extension
        // heartbeat on the unary pull path — size ackDeadlineSeconds accordingly).
        handles_on_complete: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_gcp_pubsub_destinations_after_registration() {
        // The gate-defect guard: a registered gcp-pubsub sink MUST resolve gcp-pubsub://
        // destinations (an UNREGISTERED broker sink poisons every outbound row).
        let mut registry = SinkRegistry::new();
        register_gcp_pubsub_sink(&mut registry, "acme-payments", None);
        assert!(registry.resolve("gcp-pubsub://payment-replies").is_some());
        assert!(registry.resolve("https://host/cb").is_none());
        assert!(registry.resolve("kafka://topic").is_none());
    }

    #[test]
    fn sink_registers_even_with_empty_project() {
        // Empty project still registers the scheme (rows resolve, then fail-closed
        // retryable) — the sink is never left UNREGISTERED.
        let mut registry = SinkRegistry::new();
        register_gcp_pubsub_sink(&mut registry, "", None);
        assert!(registry.resolve("gcp-pubsub://t").is_some());
    }
}

/// Broker-topology rewire conformance for GCP Pub/Sub (mirror of the rabbitmq/kafka
/// conformance). The two-phase flip is contract-only, and this IS the pin: on
/// an activation flip, a gcp-pubsub consumer whose channel definition CHANGED stops and
/// re-starts on the new definition (the moved consumer serves subsequent messages with no
/// loss); an unchanged definition does not churn the consumer; removal stops it.
#[cfg(test)]
mod broker_rewire_conformance {
    use super::spawn_gcp_pubsub_channels_with_intake;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::gcp_pubsub::build_client_config;
    use google_cloud_googleapis::pubsub::v1::PubsubMessage;
    use google_cloud_pubsub::client::Client;
    use google_cloud_pubsub::subscription::SubscriptionConfig;
    use sutra_channels::{
        AckDecision, BoxFuture, ChannelBinding, ChannelDefinition, DeploymentId, InboundIntake,
        InboundMessage, Namespace,
    };

    const PROJECT: &str = "rewire-project";

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

    /// A `transport: gcp-pubsub` inbound channel definition pointing at the emulator. The
    /// `flow-control.max-outstanding-messages` passthrough is the fingerprint knob the
    /// "changed definition" arm flips (inert to consumption — only forces a restart).
    fn broker_def(
        channel: &str,
        endpoint: &str,
        subscription: &str,
        max_outstanding: Option<&str>,
    ) -> ChannelDefinition {
        let mut properties = BTreeMap::new();
        properties.insert("project-id".to_string(), PROJECT.to_string());
        properties.insert("subscription".to_string(), subscription.to_string());
        properties.insert("endpoint-override".to_string(), endpoint.to_string());
        if let Some(m) = max_outstanding {
            properties.insert(
                "flow-control.max-outstanding-messages".to_string(),
                m.to_string(),
            );
        }
        ChannelDefinition {
            binding: ChannelBinding::new(
                channel,
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("gcp-pubsub".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties,
        }
    }

    async fn client(endpoint: &str) -> Client {
        Client::new(build_client_config(PROJECT, Some(endpoint)))
            .await
            .expect("emulator client")
    }

    async fn create_topic_and_subscription(endpoint: &str, topic: &str, subscription: &str) {
        let client = client(endpoint).await;
        client
            .create_topic(topic, None, None)
            .await
            .expect("create topic");
        client
            .create_subscription(
                subscription,
                topic,
                SubscriptionConfig {
                    ack_deadline_seconds: 10,
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create subscription");
    }

    async fn produce(endpoint: &str, topic: &str, body: &[u8]) {
        let client = client(endpoint).await;
        let publisher = client.topic(topic).new_publisher(None);
        let awaiter = publisher
            .publish(PubsubMessage {
                data: body.to_vec(),
                ..Default::default()
            })
            .await;
        awaiter.get().await.expect("publish");
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
    fn flip_moves_gcp_pubsub_consumer_to_changed_definition_without_loss() {
        use testcontainers::core::{IntoContainerPort, WaitFor};
        use testcontainers::runners::SyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        // Blocking testcontainers runner on a dedicated thread (never inside a tokio worker).
        let (container, port): (testcontainers::Container<GenericImage>, u16) =
            std::thread::spawn(|| {
                let c = GenericImage::new(
                    "gcr.io/google.com/cloudsdktool/google-cloud-cli",
                    "emulators",
                )
                .with_exposed_port(8085.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server started, listening on"))
                .with_cmd([
                    "gcloud",
                    "beta",
                    "emulators",
                    "pubsub",
                    "start",
                    "--host-port=0.0.0.0:8085",
                    "--project=rewire-project",
                ])
                .start()
                .expect("start pubsub emulator (docker required)");
                sutra_testkit::reap_on_exit(c.id());
                let port = c.get_host_port_ipv4(8085).expect("mapped pubsub port");
                (c, port)
            })
            .join()
            .expect("broker bootstrap thread");
        let endpoint = format!("127.0.0.1:{port}");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let intake = Arc::new(CapturingIntake::default());
        let dyn_intake: Arc<dyn InboundIntake> = intake.clone();

        rt.block_on(async {
            let suffix = std::process::id();
            let topic = format!("wsl-gcp-rewire-{suffix}");
            let subscription = format!("wsl-gcp-rewire-sub-{suffix}");
            create_topic_and_subscription(&endpoint, &topic, &subscription).await;

            // Wire v1 (default flow-control) — one consumer comes up.
            let v1 = broker_def("orders", &endpoint, &subscription, None);
            let channels = spawn_gcp_pubsub_channels_with_intake(
                &[v1],
                dyn_intake.clone(),
                None,
                |r| Ok(r.to_string()),
                handle.clone(),
            )
            .expect("wire v1 gcp-pubsub channel");
            assert_eq!(channels.consumer_count(), 1);
            produce(&endpoint, &topic, b"msg-1").await;
            wait_for_delivered(&intake, 1, Duration::from_secs(30)).await;

            // FLIP to v2 — SAME subscription, CHANGED flow-control (definition changed): the
            // consumer stops and re-starts on the new definition, resuming on the same
            // subscription (msg-1 was acked), so no loss across the flip.
            let v2 = broker_def("orders", &endpoint, &subscription, Some("42"));
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            produce(&endpoint, &topic, b"msg-2").await;
            wait_for_delivered(&intake, 2, Duration::from_secs(30)).await;

            // Idempotent: rewiring to an IDENTICAL definition does not churn the consumer.
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            produce(&endpoint, &topic, b"msg-3").await;
            wait_for_delivered(&intake, 3, Duration::from_secs(30)).await;

            // Removing the channel from the active set stops the consumer.
            channels.rewire(&[]).await;
            assert_eq!(channels.consumer_count(), 0);

            assert_eq!(
                intake.count(),
                3,
                "every produced message delivered across the rewire"
            );
        });

        drop(container);
    }
}
