//! AWS SQS channel wiring — the assembly touch-point for `transport: aws-sqs` broker
//! consumers: for every inbound `transport: aws-sqs` channel definition this constructs an
//! [`SqsTriggerSource`], gates it (`singleton: true` ⇒ a
//! [`crate::leadership::DbLeaderElection`] lease role `sutra-channel:<tenant>:<channel>`;
//! otherwise / no datasource ⇒ always-leading), and starts it against an [`InboundIntake`]
//! adapter over the engine actor.
//!
//! Mirrors [`crate::kafka`]: assembly calls [`spawn_sqs_channels`] once and moves on;
//! consumers detach onto the runtime (broker absence is NON-FATAL), and the shared,
//! rewireable [`SqsChannels`] rides the activation flip via [`SqsChannels::rewire`]
//! with the same semantics (stop changed/removed, start added, keep unchanged; idempotent;
//! non-fatal on a bad new def; drain on shutdown). SQS authenticates via the runtime
//! identity (static-credentials provider), so there are no channel-YAML credentials.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::sqs::{
    SqsChannelProperties, SqsMessageSink, SqsSinkSettings, SqsSourceConfig, SqsTriggerSource,
    TRANSPORT,
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

/// The env var naming the engine-wide SQS sink region. Empty/unset ⇒ the sink still
/// registers (so `aws-sqs://` destinations resolve) but publishes fail-closed retryable.
pub const SINK_REGION_ENV: &str = "SUTRA_SINK_AWS_SQS_REGION";
/// The env var naming the engine-wide SQS sink default account id (for
/// `aws-sqs://<queueName>` destinations that carry none).
pub const SINK_ACCOUNT_ID_ENV: &str = "SUTRA_SINK_AWS_SQS_ACCOUNT_ID";
/// The env var overriding the SQS sink endpoint URL (LocalStack).
pub const SINK_ENDPOINT_ENV: &str = "SUTRA_SINK_AWS_SQS_ENDPOINT_OVERRIDE";

/// Register the SQS sink into an outbox [`SinkRegistry`] (the outbox dispatcher's resolution
/// surface) under its claimed scheme (`aws-sqs`), targeting `settings`.
pub fn register_sqs_sink(registry: &mut SinkRegistry, settings: SqsSinkSettings) {
    registry.register(Arc::new(SqsMessageSink::new(settings)));
}

/// The engine-wide SQS sink settings from the environment (empty region when unset).
pub fn sink_settings_from_env() -> SqsSinkSettings {
    SqsSinkSettings {
        region: std::env::var(SINK_REGION_ENV).unwrap_or_default(),
        account_id: std::env::var(SINK_ACCOUNT_ID_ENV)
            .ok()
            .filter(|v| !v.is_empty()),
        endpoint_override: std::env::var(SINK_ENDPOINT_ENV)
            .ok()
            .filter(|v| !v.is_empty()),
    }
}

/// The wired SQS consumers + everything needed to re-start them. SHARED (`Arc`) between the
/// engine (shutdown drain) and the activation watcher (topology rewire on flip):
/// [`SqsChannels::rewire`] reconciles the running consumers to a new active definition set,
/// stopping consumers whose definition changed (or was removed) and starting the new ones —
/// unchanged consumers keep running. `stop` is drain-postured and at-least-once + inbox
/// dedup absorb any redelivery over the brief handover, so a flip loses no messages.
#[derive(Clone, Default)]
pub struct SqsChannels {
    inner: Option<Arc<BrokerState>>,
}

/// `(tenant, module_key, channel)` — the stable identity of one broker consumer across flips.
type ConsumerKey = (String, String, String);

struct RunningConsumer {
    source: Arc<SqsTriggerSource>,
    /// The authored-config fingerprint used to detect a changed definition on flip
    /// (`SqsChannelProperties: Eq`).
    fingerprint: SqsChannelProperties,
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

impl SqsChannels {
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
    /// the new ACTIVE `transport: aws-sqs` inbound definitions. Consumers whose definition
    /// was removed or CHANGED stop (drain-postured), added/changed ones start on the new
    /// definition; unchanged ones keep running untouched. Idempotent — a no-change flip is a
    /// no-op. Non-fatal: a bad new definition WARNs and is skipped (the engine keeps running).
    pub async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        let Some(state) = &self.inner else {
            return;
        };

        // Desired consumer set from the new active broker inbound definitions. An
        // authored-config error means no consumer would boot either — skip (no churn).
        let mut desired: HashMap<ConsumerKey, (ChannelDefinition, SqsChannelProperties)> =
            HashMap::new();
        for def in active_definitions
            .iter()
            .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        {
            let Ok(props) = SqsChannelProperties::from_definition(def) else {
                continue;
            };
            desired.insert(consumer_key(def), (def.clone(), props));
        }

        // Diff under the lock (no await held): the stop set + the spawn set.
        let (to_stop, to_spawn): (Vec<Arc<SqsTriggerSource>>, Vec<ChannelDefinition>) = {
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

        // Stop changed/removed consumers first (drain posture: in-flight deliveries settle;
        // at-least-once redelivery + inbox dedup absorb the brief handover gap).
        for source in &to_stop {
            if let Err(d) = source.stop().await {
                warn!(code = %d.code, "aws-sqs consumer stop during flip: {}", d.message);
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
                Ok(None) => {}
                Err(d) => warn!(
                    code = %d.code,
                    "aws-sqs consumer NOT started during flip: {}", d.message
                ),
            }
        }
        info!(
            stopped = to_stop.len(),
            started = to_spawn.len(),
            "aws-sqs broker topology rewired on activation flip"
        );
    }
}

impl BrokerState {
    fn snapshot_sources(&self) -> Vec<Arc<SqsTriggerSource>> {
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
/// - `Err(diagnostic)` — authored config invalid (missing queue.url/region): boot fails
///   CLOSED (rewire WARNs and skips — the running engine never crashes on a bad flip).
/// - `Ok(Some(entry))` — the started consumer + its registry key.
fn build_and_start_consumer(
    definition: &ChannelDefinition,
    election: &Option<Arc<DbLeaderElection>>,
    ctx: &BrokerRespawnContext,
) -> Result<Option<(ConsumerKey, RunningConsumer)>, Diagnostic> {
    let channel = definition.binding.channel_name.clone();
    let tenant = definition.binding.tenant().to_string();
    let properties = SqsChannelProperties::from_definition(definition)?;
    let fingerprint = properties.clone();

    let singleton = properties.singleton;
    if singleton && ctx.pool.is_none() {
        warn!(
            channel = %channel,
            "singleton aws-sqs channel has no engine datasource — no lease election is \
             possible; consuming on every replica (NoOp leadership posture)"
        );
    }
    let gate = match election {
        Some(e) if singleton => {
            ChannelLeadership::Elected(Arc::clone(e)).gate_for(&channel_role(&tenant, &channel))
        }
        _ => ChannelLeadership::AlwaysLeading.gate_for(""),
    };

    let mut source_config = SqsSourceConfig::new(
        &tenant,
        &definition.binding.namespace.module_key(),
        &channel,
        properties,
    );
    // Per-message inbound auth (`inbound-auth.*`); the expected-key ref resolves via
    // the envref registry (env:/secret:/vault:). An unresolvable ref fails the channel closed.
    source_config.inbound_auth = sutra_channels::auth::BrokerInboundAuth::from_properties(
        &definition.properties,
        crate::sqs::codes::INBOUND_CONFIG_INVALID,
        ctx.resolver,
    )?;
    let source = Arc::new(SqsTriggerSource::new(source_config)?);
    let intake = Arc::clone(&ctx.intake);
    let started = Arc::clone(&source);
    let channel_for_log = channel.clone();
    ctx.handle.spawn(async move {
        if let Err(diagnostic) = started.start(intake, gate).await {
            warn!(
                channel = %channel_for_log,
                code = %diagnostic.code,
                "aws-sqs consumer failed to start: {}",
                diagnostic.message
            );
        }
    });
    info!(channel = %channel, tenant = %tenant, singleton, "aws-sqs consumer wired");
    Ok(Some((
        consumer_key(definition),
        RunningConsumer {
            source,
            fingerprint,
        },
    )))
}

/// Construct + start one consumer per inbound `transport: aws-sqs` definition, fail-closed
/// on authored errors. Returns the shared, rewireable [`SqsChannels`].
pub fn spawn_sqs_channels(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<SqsChannels, Diagnostic> {
    let intake: Arc<dyn InboundIntake> = Arc::new(EngineIntake::new(engine));
    spawn_sqs_channels_with_intake(definitions, intake, pool, resolver, handle)
}

/// The intake-injectable core of [`spawn_sqs_channels`] — the engine wraps its actor as
/// [`EngineIntake`], conformance tests inject a capturing intake. Same boot semantics + the
/// same shared, rewireable state.
pub(crate) fn spawn_sqs_channels_with_intake(
    definitions: &[ChannelDefinition],
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<SqsChannels, Diagnostic> {
    let inbound: Vec<&ChannelDefinition> = definitions
        .iter()
        .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        .collect();

    // One election for all singleton roles, only when a datasource exists AND a boot channel
    // is `singleton: true`; the AlwaysLeading fallback is the no-election posture.
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
    Ok(SqsChannels {
        inner: Some(Arc::new(BrokerState {
            running: Mutex::new(running),
            election,
            ctx,
        })),
    })
}

// ---- Neutral transport SPI wiring (domain-neutrality refactor) --------------

#[async_trait::async_trait]
impl TransportChannels for SqsChannels {
    fn transport(&self) -> &str {
        TRANSPORT
    }
    fn consumer_count(&self) -> usize {
        SqsChannels::consumer_count(self)
    }
    async fn rewire(&self, active: &[ChannelDefinition]) {
        SqsChannels::rewire(self, active).await
    }
    async fn drain(&self) {
        SqsChannels::drain(self).await
    }
    fn stop_all_detached(&self, runtime: &Handle) {
        SqsChannels::stop_all_detached(self, runtime)
    }
}

/// Factory `spawn` adapter — widens the concrete [`SqsChannels`] to the trait object.
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(spawn_sqs_channels(
        definitions,
        engine,
        pool,
        resolver,
        handle,
    )?))
}

/// Factory `register_sink` adapter — reads the `SUTRA_SINK_AWS_SQS_*` env config itself.
fn register_sink(registry: &mut SinkRegistry) {
    register_sqs_sink(registry, sink_settings_from_env());
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // WIRED — an `ack-mode: on-complete` inbound definition routes through
        // `InboundIntake::deliver_deferred`, and a parked instance's held DeleteMessage
        // fires from the deferred-ack registry at its terminal event. Bounded by the
        // queue's visibility timeout (see the `sqs::source` module docs for the sizing
        // rule: visibility timeout >= worst-case park >= sutra.ack.deferred.timeout).
        handles_on_complete: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_aws_sqs_destinations_after_registration() {
        // The gate-defect guard: a registered SQS sink MUST resolve aws-sqs:// destinations
        // (an UNREGISTERED broker sink poisons every outbound row).
        let mut registry = SinkRegistry::new();
        register_sqs_sink(
            &mut registry,
            SqsSinkSettings {
                region: "us-east-1".to_string(),
                account_id: Some("000000000000".to_string()),
                endpoint_override: None,
            },
        );
        assert!(registry.resolve("aws-sqs://payment-replies").is_some());
        assert!(registry.resolve("aws-sqs://42/replies").is_some());
        assert!(registry.resolve("kafka://topic").is_none());
        assert!(registry.resolve("rabbitmq://broker/q").is_none());
    }

    #[test]
    fn sink_registers_even_with_empty_region() {
        // Empty region still registers the scheme (rows resolve, then fail-closed retryable)
        // — the sink is never left UNREGISTERED.
        let mut registry = SinkRegistry::new();
        register_sqs_sink(&mut registry, SqsSinkSettings::default());
        assert!(registry.resolve("aws-sqs://t").is_some());
    }
}

/// Broker-topology rewire conformance for AWS SQS (mirror of the kafka conformance).
/// The two-phase flip is contract-only, and this IS the pin: on an
/// activation flip, an SQS consumer whose channel definition CHANGED stops and re-starts on
/// the new definition (the moved consumer serves subsequent messages with no loss); an
/// unchanged definition does not churn the consumer; removal stops it.
#[cfg(test)]
mod broker_rewire_conformance {
    use super::spawn_sqs_channels_with_intake;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use aws_sdk_sqs::types::MessageAttributeValue;
    use sutra_channels::{
        AckDecision, BoxFuture, ChannelBinding, ChannelDefinition, DeploymentId, InboundIntake,
        InboundMessage, Namespace,
    };

    /// Captures every delivered body and acks (so messages are deleted, not redelivered).
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

    /// A `transport: aws-sqs` inbound channel definition pointing at the LocalStack queue.
    /// The `visibility-timeout-seconds` knob is the fingerprint knob the "changed
    /// definition" arm flips (inert to consumption — only forces a consumer restart).
    fn broker_def(
        channel: &str,
        endpoint: &str,
        queue_url: &str,
        visibility: &str,
    ) -> ChannelDefinition {
        let mut properties = BTreeMap::new();
        properties.insert("region".to_string(), "us-east-1".to_string());
        properties.insert("queue.url".to_string(), queue_url.to_string());
        properties.insert("endpoint-override".to_string(), endpoint.to_string());
        properties.insert("wait-time-seconds".to_string(), "1".to_string());
        properties.insert(
            "visibility-timeout-seconds".to_string(),
            visibility.to_string(),
        );
        ChannelDefinition {
            binding: ChannelBinding::new(
                channel,
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("aws-sqs".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties,
        }
    }

    fn client(endpoint: &str) -> aws_sdk_sqs::Client {
        // Reuse the transport's own static-credentials + ring-TLS client builder.
        crate::sqs::build_client("us-east-1", Some(endpoint))
    }

    async fn create_queue(endpoint: &str, name: &str) -> String {
        let created = client(endpoint)
            .create_queue()
            .queue_name(name)
            .send()
            .await
            .expect("create queue");
        created.queue_url().expect("queue url").to_string()
    }

    async fn send(endpoint: &str, queue_url: &str, body: &[u8]) {
        let attr = MessageAttributeValue::builder()
            .data_type("String")
            .string_value("k")
            .build()
            .expect("attr");
        client(endpoint)
            .send_message()
            .queue_url(queue_url)
            .message_body(String::from_utf8_lossy(body).into_owned())
            .message_attributes("x-probe", attr)
            .send()
            .await
            .expect("send");
    }

    async fn wait_for_delivered(intake: &CapturingIntake, expected: usize, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if intake.count() >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        panic!(
            "timed out waiting for {expected} deliveries (have {})",
            intake.count()
        );
    }

    #[test]
    #[ignore = "docker"]
    fn flip_moves_sqs_consumer_to_changed_definition_without_loss() {
        use testcontainers::core::{IntoContainerPort, WaitFor};
        use testcontainers::runners::SyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        let (container, port): (testcontainers::Container<GenericImage>, u16) =
            std::thread::spawn(|| {
                let c = GenericImage::new("localstack/localstack", "3")
                    .with_exposed_port(4566.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("Ready."))
                    .with_env_var("SERVICES", "sqs")
                    .start()
                    .expect("start localstack/localstack:3 (docker required)");
                sutra_testkit::reap_on_exit(c.id());
                let port = c.get_host_port_ipv4(4566.tcp()).expect("mapped 4566");
                (c, port)
            })
            .join()
            .expect("localstack bootstrap thread");
        let endpoint = format!("http://127.0.0.1:{port}");

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let intake = Arc::new(CapturingIntake::default());
        let dyn_intake: Arc<dyn InboundIntake> = intake.clone();

        rt.block_on(async {
            let queue_name = format!("wsl-sqs-rewire-{}", std::process::id());
            let queue_url = create_queue(&endpoint, &queue_name).await;

            // Wire v1 — one consumer comes up. A SMALL visibility timeout keeps the test
            // fast: stopping a long-polling consumer on the flip can briefly orphan an
            // in-flight receive that hides the next message server-side; the SQS-native
            // at-least-once contract redelivers it after the visibility timeout, so a small
            // value bounds the handover redelivery window (no loss, just a short delay).
            let v1 = broker_def("orders", &endpoint, &queue_url, "3");
            let channels = spawn_sqs_channels_with_intake(
                &[v1],
                dyn_intake.clone(),
                None,
                |r| Ok(r.to_string()),
                handle.clone(),
            )
            .expect("wire v1 aws-sqs channel");
            assert_eq!(channels.consumer_count(), 1);
            send(&endpoint, &queue_url, b"msg-1").await;
            wait_for_delivered(&intake, 1, Duration::from_secs(30)).await;

            // FLIP to v2 — CHANGED visibility (definition changed): the consumer stops and
            // re-starts on the new definition, resuming from the same queue, so no loss.
            let v2 = broker_def("orders", &endpoint, &queue_url, "2");
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            send(&endpoint, &queue_url, b"msg-2").await;
            wait_for_delivered(&intake, 2, Duration::from_secs(30)).await;

            // Idempotent: rewiring to an IDENTICAL definition does not churn the consumer.
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            send(&endpoint, &queue_url, b"msg-3").await;
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
