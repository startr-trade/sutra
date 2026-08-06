//! AMQP 1.0 channel wiring — the assembly touch-point for `transport: amqp`
//! broker consumers: for every inbound `transport: amqp` channel definition this constructs
//! an [`AmqpTriggerSource`] (credentials env-resolved), gates it (`singleton: true` ⇒ a
//! [`crate::leadership::DbLeaderElection`] lease role `sutra-channel:<tenant>:<channel>`;
//! otherwise / no datasource ⇒ always-leading), and starts it against an [`InboundIntake`]
//! adapter over the engine actor.
//!
//! Mirrors [`crate::kafka`] / [`crate::rabbitmq`]: assembly calls [`spawn_amqp_channels`]
//! once and moves on; consumers detach onto the runtime (broker absence is NON-FATAL), and
//! the shared, rewireable [`AmqpChannels`] rides the activation flip via
//! [`AmqpChannels::rewire`] with the same semantics (stop changed/removed, start added, keep
//! unchanged; idempotent; non-fatal on a bad new def; drain on shutdown). Like RabbitMQ (and
//! unlike Kafka) AMQP 1.0 carries channel-YAML `username`/`password` secret references, so the
//! credential-resolution step is present (an unresolvable reference ⇒ WARN + skip, the
//! broker-absent deployment shape).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::amqp::{
    AmqpChannelProperties, AmqpMessageSink, AmqpSourceConfig, AmqpTriggerSource, TRANSPORT,
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

/// The env var naming the engine-wide default AMQP sink username (applied when a destination
/// URI carries no userinfo). Empty/unset ⇒ anonymous.
pub const SINK_USERNAME_ENV: &str = "SUTRA_SINK_AMQP_USERNAME";
/// The env var naming the engine-wide default AMQP sink password.
pub const SINK_PASSWORD_ENV: &str = "SUTRA_SINK_AMQP_PASSWORD";

/// Register the AMQP 1.0 sink into an outbox [`SinkRegistry`] under its claimed schemes
/// (`amqp10` / `amqp10s`), carrying the engine-wide default credentials.
pub fn register_amqp_sink(
    registry: &mut SinkRegistry,
    username: Option<String>,
    password: Option<String>,
) {
    registry.register(Arc::new(AmqpMessageSink::new(username, password)));
}

/// The engine-wide default AMQP sink credentials from the environment (both `None` ⇒
/// anonymous / URI-supplied userinfo only).
pub fn sink_credentials_from_env() -> (Option<String>, Option<String>) {
    let user = std::env::var(SINK_USERNAME_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    let pass = std::env::var(SINK_PASSWORD_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    (user, pass)
}

/// The wired AMQP consumers + everything needed to re-start them. SHARED (`Arc`) between the
/// engine (shutdown drain) and the activation watcher (topology rewire on flip):
/// [`AmqpChannels::rewire`] reconciles the running consumers to a new active definition set,
/// stopping consumers whose definition changed (or was removed) and starting the new ones —
/// unchanged consumers keep running. `stop` is drain-postured and at-least-once + inbox dedup
/// absorb any redelivery over the brief handover, so a flip loses no messages.
#[derive(Clone, Default)]
pub struct AmqpChannels {
    inner: Option<Arc<BrokerState>>,
}

/// `(tenant, module_key, channel)` — the stable identity of one broker consumer across flips.
type ConsumerKey = (String, String, String);

struct RunningConsumer {
    source: Arc<AmqpTriggerSource>,
    /// The PRE-credential-resolution authored-config fingerprint used to detect a changed
    /// definition on flip (`AmqpChannelProperties: Eq`; credentials stay as their references).
    fingerprint: AmqpChannelProperties,
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

impl AmqpChannels {
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
    /// the new ACTIVE `transport: amqp` inbound definitions. Consumers whose definition was
    /// removed or CHANGED stop (drain-postured), added/changed ones start on the new
    /// definition; unchanged ones keep running untouched. Idempotent — a no-change flip is a
    /// no-op. Non-fatal: a bad new definition WARNs and is skipped (the engine keeps running).
    pub async fn rewire(&self, active_definitions: &[ChannelDefinition]) {
        let Some(state) = &self.inner else {
            return;
        };

        // Desired consumer set from the new active broker inbound definitions. An
        // authored-config error means no consumer would boot either — skip (no churn).
        let mut desired: HashMap<ConsumerKey, (ChannelDefinition, AmqpChannelProperties)> =
            HashMap::new();
        for def in active_definitions
            .iter()
            .filter(|d| d.transport.as_deref() == Some(TRANSPORT) && !d.is_outbound())
        {
            let Ok(props) = AmqpChannelProperties::from_definition(def) else {
                continue;
            };
            desired.insert(consumer_key(def), (def.clone(), props));
        }

        // Diff under the lock (no await held): the stop set + the spawn set.
        let (to_stop, to_spawn): (Vec<Arc<AmqpTriggerSource>>, Vec<ChannelDefinition>) = {
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
        // their dispositions; at-least-once redelivery + inbox dedup absorb the handover gap).
        for source in &to_stop {
            if let Err(d) = source.stop().await {
                warn!(code = %d.code, "amqp consumer stop during flip: {}", d.message);
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
                // Credentials unresolvable in this env (the broker-absent shape) — skip.
                Ok(None) => {}
                Err(d) => warn!(
                    code = %d.code,
                    "amqp consumer NOT started during flip: {}", d.message
                ),
            }
        }
        info!(
            stopped = to_stop.len(),
            started = to_spawn.len(),
            "amqp broker topology rewired on activation flip"
        );
    }
}

impl BrokerState {
    fn snapshot_sources(&self) -> Vec<Arc<AmqpTriggerSource>> {
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
/// - `Err(diagnostic)` — authored config invalid (literal credentials, missing host/dest):
///   boot fails CLOSED (rewire WARNs and skips — the running engine never crashes on a flip).
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
    let properties = AmqpChannelProperties::from_definition(definition)?;
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
                "amqp consumer NOT started — broker credentials unresolvable in \
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
            "singleton amqp channel has no engine datasource — no lease election is \
             possible; consuming on every replica (NoOp leadership posture)"
        );
    }
    let gate = match election {
        Some(e) if singleton => {
            ChannelLeadership::Elected(Arc::clone(e)).gate_for(&channel_role(&tenant, &channel))
        }
        _ => ChannelLeadership::AlwaysLeading.gate_for(""),
    };

    let mut source_config = AmqpSourceConfig::new(
        &tenant,
        &definition.binding.namespace.module_key(),
        &channel,
        resolved,
    );
    // Per-message inbound auth (`inbound-auth.*`); the expected-key ref resolves via
    // the envref registry (env:/secret:/vault:). An unresolvable ref fails the channel closed.
    source_config.inbound_auth = sutra_channels::auth::BrokerInboundAuth::from_properties(
        &definition.properties,
        crate::amqp::codes::INBOUND_CONFIG_INVALID,
        ctx.resolver,
    )?;
    let source = Arc::new(AmqpTriggerSource::new(source_config)?);
    let intake = Arc::clone(&ctx.intake);
    let started = Arc::clone(&source);
    let channel_for_log = channel.clone();
    ctx.handle.spawn(async move {
        if let Err(diagnostic) = started.start(intake, gate).await {
            warn!(
                channel = %channel_for_log,
                code = %diagnostic.code,
                "amqp consumer failed to start: {}",
                diagnostic.message
            );
        }
    });
    info!(channel = %channel, tenant = %tenant, singleton, "amqp consumer wired");
    Ok(Some((
        consumer_key(definition),
        RunningConsumer {
            source,
            fingerprint,
        },
    )))
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
            crate::amqp::codes::INBOUND_CONNECTION_FAILED,
            format!(
                "amqp channel '{channel}' could not resolve broker credential \
                 reference for '{key}': {e}"
            ),
        )
    })
}

/// Construct + start one consumer per inbound `transport: amqp` definition, fail-closed on
/// authored errors. Returns the shared, rewireable [`AmqpChannels`].
pub fn spawn_amqp_channels(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<AmqpChannels, Diagnostic> {
    let intake: Arc<dyn InboundIntake> = Arc::new(EngineIntake::new(engine));
    spawn_amqp_channels_with_intake(definitions, intake, pool, resolver, handle)
}

/// The intake-injectable core of [`spawn_amqp_channels`] — the engine wraps its actor as
/// [`EngineIntake`], conformance tests inject a capturing intake. Same boot semantics + the
/// same shared, rewireable state.
pub(crate) fn spawn_amqp_channels_with_intake(
    definitions: &[ChannelDefinition],
    intake: Arc<dyn InboundIntake>,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<AmqpChannels, Diagnostic> {
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
    Ok(AmqpChannels {
        inner: Some(Arc::new(BrokerState {
            running: Mutex::new(running),
            election,
            ctx,
        })),
    })
}

// ---- Neutral transport SPI wiring (domain-neutrality refactor) --------------

#[async_trait::async_trait]
impl TransportChannels for AmqpChannels {
    fn transport(&self) -> &str {
        TRANSPORT
    }
    fn consumer_count(&self) -> usize {
        AmqpChannels::consumer_count(self)
    }
    async fn rewire(&self, active: &[ChannelDefinition]) {
        AmqpChannels::rewire(self, active).await
    }
    async fn drain(&self) {
        AmqpChannels::drain(self).await
    }
    fn stop_all_detached(&self, runtime: &Handle) {
        AmqpChannels::stop_all_detached(self, runtime)
    }
}

/// Factory `spawn` adapter — widens the concrete [`AmqpChannels`] to the trait object.
fn spawn_boxed(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<PgPool>,
    resolver: EnvRefResolver,
    handle: Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    Ok(Arc::new(spawn_amqp_channels(
        definitions,
        engine,
        pool,
        resolver,
        handle,
    )?))
}

/// Factory `register_sink` adapter — reads the `SUTRA_SINK_AMQP_{USERNAME,PASSWORD}` env
/// defaults itself (the broker host rides the `amqp10://` URI).
fn register_sink(registry: &mut SinkRegistry) {
    let (username, password) = sink_credentials_from_env();
    register_amqp_sink(registry, username, password);
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn: spawn_boxed,
        register_sink,
        // `ack-mode: on-complete` is honoured — the source defers its AMQP 1.0
        // disposition (accept / reject) through the engine's deferred-ack registry
        // (`InboundIntake::deliver_deferred`; see `amqp::source`).
        handles_on_complete: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_registry_resolves_amqp10_destinations_after_registration() {
        // The gate-defect guard: a registered amqp10 sink MUST resolve amqp10:// destinations
        // (an UNREGISTERED broker sink poisons every outbound row). It must NOT steal the
        // rabbitmq `amqp` scheme.
        let mut registry = SinkRegistry::new();
        register_amqp_sink(&mut registry, None, None);
        assert!(registry.resolve("amqp10://broker:5672/replies").is_some());
        assert!(registry.resolve("amqp10s://broker/replies").is_some());
        assert!(
            registry.resolve("amqp://broker/q").is_none(),
            "amqp is rabbitmq's, not ours"
        );
        assert!(registry.resolve("kafka://t").is_none());
        assert!(registry.resolve("https://host/cb").is_none());
    }
}

/// Broker-topology rewire conformance for AMQP 1.0 (mirror of the rabbitmq/kafka
/// conformance). The two-phase flip is contract-only, so this module IS the pin:
/// on an activation flip, an amqp consumer whose channel definition CHANGED stops and
/// re-starts on the new definition (the moved consumer serves subsequent messages with no
/// loss); an unchanged definition does not churn the consumer; removal stops it.
#[cfg(test)]
mod broker_rewire_conformance {
    use super::spawn_amqp_channels_with_intake;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use fe2o3_amqp::types::messaging::Message;
    use fe2o3_amqp::{Connection, Sender, Session};
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

    /// A `transport: amqp` inbound channel definition pointing at the test broker. The
    /// `prefetch-count` is the fingerprint knob the "changed definition" arm flips (inert to
    /// consumption — only forces a consumer restart).
    fn broker_def(
        channel: &str,
        host: &str,
        port: u16,
        queue: &str,
        prefetch: Option<&str>,
    ) -> ChannelDefinition {
        let mut properties = BTreeMap::new();
        properties.insert("host".to_string(), host.to_string());
        properties.insert("port".to_string(), port.to_string());
        properties.insert("queue".to_string(), queue.to_string());
        properties.insert("receive-timeout-ms".to_string(), "300".to_string());
        if let Some(p) = prefetch {
            properties.insert("prefetch-count".to_string(), p.to_string());
        }
        ChannelDefinition {
            binding: ChannelBinding::new(
                channel,
                Namespace::new("acme", "orders", "v1"),
                DeploymentId::unresolved(),
                "opaque",
            ),
            transport: Some("amqp".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties,
        }
    }

    async fn produce(host: &str, port: u16, queue: &str, body: &[u8]) {
        let mut connection = Connection::open(
            "sutra-amqp-rewire-producer",
            format!("amqp://{host}:{port}").as_str(),
        )
        .await
        .expect("open producer connection");
        let mut session = Session::begin(&mut connection)
            .await
            .expect("begin session");
        let mut sender = Sender::attach(&mut session, "rewire-producer", queue)
            .await
            .expect("attach sender");
        let message = Message::builder().data(body.to_vec()).build();
        sender
            .send(message)
            .await
            .expect("send")
            .accepted_or_else(|s| panic!("not accepted: {s:?}"))
            .unwrap();
        let _ = sender.close().await;
        let _ = session.end().await;
        let _ = connection.close().await;
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
    fn flip_moves_amqp_consumer_to_changed_definition_without_loss() {
        use testcontainers::core::{IntoContainerPort, WaitFor};
        use testcontainers::runners::SyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        // Blocking testcontainers runner on a dedicated thread (never inside a tokio worker).
        let (container, port): (testcontainers::Container<GenericImage>, u16) =
            std::thread::spawn(|| {
                let c = GenericImage::new("apache/activemq-artemis", "latest-alpine")
                    .with_exposed_port(5672.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("Server is now active"))
                    .with_env_var("ARTEMIS_USER", "artemis")
                    .with_env_var("ARTEMIS_PASSWORD", "artemis")
                    .with_env_var("ANONYMOUS_LOGIN", "true")
                    .start()
                    .expect("start apache/activemq-artemis (docker required)");
                sutra_testkit::reap_on_exit(c.id());
                let port = c.get_host_port_ipv4(5672).expect("mapped amqp port");
                (c, port)
            })
            .join()
            .expect("broker bootstrap thread");
        let host = "127.0.0.1";

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = rt.handle().clone();
        let intake = Arc::new(CapturingIntake::default());
        let dyn_intake: Arc<dyn InboundIntake> = intake.clone();

        rt.block_on(async {
            let queue = format!("wsl-amqp-rewire-{}", std::process::id());

            // Wire v1 (no prefetch override) — one consumer comes up.
            let v1 = broker_def("orders", host, port, &queue, None);
            let channels = spawn_amqp_channels_with_intake(
                &[v1],
                dyn_intake.clone(),
                None,
                |r| Ok(r.to_string()),
                handle.clone(),
            )
            .expect("wire v1 amqp channel");
            assert_eq!(channels.consumer_count(), 1);
            produce(host, port, &queue, b"msg-1").await;
            wait_for_delivered(&intake, 1, Duration::from_secs(90)).await;

            // FLIP to v2 — SAME queue, CHANGED prefetch-count (definition changed): the
            // consumer stops and re-starts on the new definition; msg-1 was accepted so no
            // loss across the flip.
            let v2 = broker_def("orders", host, port, &queue, Some("25"));
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            produce(host, port, &queue, b"msg-2").await;
            wait_for_delivered(&intake, 2, Duration::from_secs(90)).await;

            // Idempotent: rewiring to an IDENTICAL definition does not churn the consumer.
            channels.rewire(std::slice::from_ref(&v2)).await;
            assert_eq!(channels.consumer_count(), 1);
            produce(host, port, &queue, b"msg-3").await;
            wait_for_delivered(&intake, 3, Duration::from_secs(90)).await;

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
