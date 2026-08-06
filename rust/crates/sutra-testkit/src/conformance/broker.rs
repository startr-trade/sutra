//! RabbitMQ fixture + lapin helpers for the broker-transport suites.
//!
//! The broker container is named to match the alias baked into the example's `channels.yaml`
//! (`rabbit` / `rabbitmq` / `rabbitmq-mtmx`) so the engine — on the same docker network —
//! resolves it; each alias is used by exactly one suite. Host-side lapin clients reach it at
//! the dynamically mapped host port.

use std::process::Stdio;
use std::time::{Duration, Instant};

use kube::Client;
use lapin::options::{BasicGetOptions, BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use super::util::{self, Recorder};

/// A running broker fixture. Drop semantics are
/// [`EngineHandle`](super::engine::EngineHandle)'s — park it in the suite's `static` fixture,
/// never in a value a tokio worker can drop.
pub struct BrokerFixture {
    /// The live testcontainers handle. Held so the container outlives the fixture; see
    /// [`EngineHandle`](super::engine::EngineHandle) for why it must not be dropped on a
    /// runtime thread.
    pub container: Container<GenericImage>,
    pub host_port: u16,
    pub user: String,
    pub pass: String,
}

/// Start `rabbitmq:3.13-management-alpine` on the shared network under the given alias (its
/// container name, which the engine resolves), provisioned with a real service account.
pub fn start_broker(suite: &str, alias: &str, user: &str, pass: &str) -> BrokerFixture {
    force_remove(alias);
    let container = GenericImage::new("rabbitmq", "3.13-management-alpine")
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
        .with_startup_timeout(Duration::from_secs(150))
        .with_network(util::network_for(suite))
        .with_container_name(alias)
        .with_env_var("RABBITMQ_DEFAULT_USER", user)
        .with_env_var("RABBITMQ_DEFAULT_PASS", pass)
        .start()
        .expect("start rabbitmq:3.13-management-alpine (docker required)");
    crate::reap_on_exit(container.id());
    let host_port = container.get_host_port_ipv4(5672).expect("mapped 5672");
    BrokerFixture {
        container,
        host_port,
        user: user.to_string(),
        pass: pass.to_string(),
    }
}

/// Best-effort removal of a stale container holding a fixed broker alias name (left by a
/// crashed prior run) before we claim it.
fn force_remove(name: &str) {
    let _ = std::process::Command::new("docker")
        .args(["rm", "-f", name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// A fallible AMQP connect (default vhost `/`) — the reconnect primitive for the self-healing
/// recorder.
async fn try_connect_at(
    host: &str,
    port: u16,
    user: &str,
    pass: &str,
) -> Result<Connection, lapin::Error> {
    let url = format!("amqp://{user}:{pass}@{host}:{port}/%2f");
    Connection::connect(&url, ConnectionProperties::default()).await
}

/// An AMQP connection to an arbitrary host (default vhost `/`).
pub async fn connect_at(host: &str, port: u16, user: &str, pass: &str) -> Connection {
    try_connect_at(host, port, user, pass)
        .await
        .expect("amqp connect")
}

/// A host-side AMQP connection to a mapped local port (default vhost `/`).
pub async fn connect(host_port: u16, user: &str, pass: &str) -> Connection {
    connect_at("127.0.0.1", host_port, user, pass).await
}

/// Passively-safe durable queue declaration (the triggers declare passively, so queues must
/// pre-exist).
pub async fn declare_durable_queue(conn: &Connection, name: &str) {
    let channel = conn.create_channel().await.expect("channel");
    channel
        .queue_declare(
            name,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare");
    channel.close(200, "declared").await.ok();
}

/// Publish to the default exchange keyed on `queue`, carrying `message_id` (→ engine
/// idempotency key) and `content_type` (→ codec selection).
pub async fn publish(
    conn: &Connection,
    queue: &str,
    message_id: &str,
    content_type: &str,
    body: &[u8],
) {
    let channel = conn.create_channel().await.expect("channel");
    let props = BasicProperties::default()
        .with_message_id(message_id.into())
        .with_content_type(content_type.into());
    channel
        .basic_publish("", queue, BasicPublishOptions::default(), body, props)
        .await
        .expect("publish")
        .await
        .expect("confirm");
    channel.close(200, "published").await.ok();
}

/// The active-consumer count on `queue` (via a passive re-declare) — the deterministic
/// singleton proof.
pub async fn consumer_count(host_port: u16, user: &str, pass: &str, queue: &str) -> u32 {
    let conn = connect(host_port, user, pass).await;
    let channel = conn.create_channel().await.expect("channel");
    let q = channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");
    let count = q.consumer_count();
    conn.close(200, "counted").await.ok();
    count
}

/// `(ready_messages, active_consumers)` on `queue` via a passive re-declare — the broker's own
/// view, used to turn a ledger mismatch into a DIAGNOSIS rather than a bare timeout: ready > 0
/// means the drain stalled, ready == 0 with a short ledger means deliveries were consumed but
/// their effect was lost.
pub async fn queue_stats(host_port: u16, user: &str, pass: &str, queue: &str) -> (u32, u32) {
    let conn = connect(host_port, user, pass).await;
    let channel = conn.create_channel().await.expect("channel");
    let q = channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("passive declare");
    let stats = (q.message_count(), q.consumer_count());
    conn.close(200, "counted").await.ok();
    stats
}

/// Where the self-healing recorder connects — and, crucially, how it RE-RESOLVES that endpoint
/// after a sustained outage.
///
/// - `Fixed` is the docker / tier-2 case: a stable host/port that never moves. Its `resolve` is a
///   constant, so the recorder just keeps retrying the same endpoint (no behavior change).
/// - `K8sLoadBalancer` is the tier-3 case: a MetalLB LoadBalancer Service whose ingress IP can be
///   re-announced on a different path by a speaker flap. The variant carries the kube client plus
///   the Service coordinates so the recorder can re-query the CURRENT LB IP and reconnect from
///   scratch, instead of replaying a stale IP forever.
pub enum BrokerHost {
    /// A constant host/port (non-k8s suites).
    Fixed { host: String, port: u16 },
    /// A MetalLB LoadBalancer Service whose ingress IP is re-resolved live on a sustained gap.
    K8sLoadBalancer {
        client: Client,
        namespace: String,
        service: String,
        port: u16,
    },
}

impl BrokerHost {
    /// Resolve the endpoint to connect to RIGHT NOW as `(host, port)`.
    ///
    /// `Fixed` is a constant. `K8sLoadBalancer` re-queries the Service's live MetalLB ingress IP
    /// each call (`None` until one is assigned), so a flap that moves the LB is picked up on the
    /// next resolve.
    async fn resolve(&self) -> Option<(String, u16)> {
        match self {
            BrokerHost::Fixed { host, port } => Some((host.clone(), *port)),
            BrokerHost::K8sLoadBalancer {
                client,
                namespace,
                service,
                port,
            } => super::k8s::service_lb_ip(client, namespace, service)
                .await
                .map(|ip| (ip, *port)),
        }
    }
}

/// A self-healing consumer that records every delivery body on `queue` into `recorder`.
///
/// It owns its OWN connection + channel and RE-ESTABLISHES them on any error. This matters for
/// long-lived suites (e.g. the k8s hot-deploy test runs 400s+): under CPU/IO pressure the broker
/// connection can drop to a heartbeat timeout, and a fixed channel would then error forever,
/// leaving the recorder permanently blind. Because the responses queue is durable and this is its
/// only consumer, messages published during a disconnect PERSIST in the queue and are drained on
/// the next successful poll — so no verdict is lost across a reconnect. Spawn on a long-lived
/// runtime; the passed params let it reconnect without any external state.
///
/// Surviving a SUSTAINED LoadBalancer outage: connection-drop reconnects alone go blind
/// through a MetalLB flap, because the old code replayed a FIXED host/port forever behind a flat
/// 500ms sleep. Now the endpoint comes from a `BrokerHost`, the reconnect backoff is exponential
/// (capped), and once the unhealthy gap is sustained (~15s of failed connects) the endpoint is
/// RE-RESOLVED — so a `K8sLoadBalancer` whose LB IP was re-announced on a new path is chased down
/// and reconnected from scratch, with no manual `metallb` restart. A `Fixed` host re-resolves to
/// the same constant, so its behavior is unchanged.
pub async fn record_queue(
    host: BrokerHost,
    user: String,
    pass: String,
    queue: String,
    recorder: Recorder,
) {
    // Once the recorder has been unable to connect for this long, treat it as a sustained outage
    // (not a transient drop) and re-resolve the endpoint on every subsequent failed cycle.
    const RERESOLVE_AFTER: Duration = Duration::from_secs(15);
    // Exponential reconnect backoff, replacing the old flat 500ms sleep. Starts small so a
    // transient drop recovers fast; capped so a long outage doesn't stretch the retry gap without
    // bound (and keeps re-resolution polling the LB at a sane cadence).
    const BACKOFF_START: Duration = Duration::from_millis(500);
    const BACKOFF_CEIL: Duration = Duration::from_secs(10);

    let mut endpoint = host.resolve().await;
    let mut backoff = BACKOFF_START;
    let mut unhealthy_since: Option<Instant> = None;

    loop {
        // (Re)establish the connection + channel to the CURRENT endpoint; back off and retry on
        // failure.
        let established = match &endpoint {
            Some((h, p)) => match try_connect_at(h, *p, &user, &pass).await {
                Ok(conn) => conn.create_channel().await.ok().map(|ch| (conn, ch)),
                Err(_) => None,
            },
            None => None,
        };
        let Some((_conn, channel)) = established else {
            // Unhealthy. Track how long the streak has run; once it is sustained (or we have no
            // endpoint at all yet) re-resolve — a no-op for `Fixed`, the live LB ingress IP for
            // `K8sLoadBalancer`. A freshly discovered endpoint resets the backoff so we try it
            // promptly.
            let now = Instant::now();
            let since = *unhealthy_since.get_or_insert(now);
            if endpoint.is_none() || now.duration_since(since) >= RERESOLVE_AFTER {
                let refreshed = host.resolve().await;
                if refreshed.is_some() && refreshed != endpoint {
                    endpoint = refreshed;
                    backoff = BACKOFF_START;
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_CEIL);
            continue;
        };
        // Healthy again — clear the unhealthy streak and reset the backoff.
        unhealthy_since = None;
        backoff = BACKOFF_START;
        // Drain until the channel/connection errors, then fall through to reconnect. `_conn` is
        // held for the inner loop's lifetime so the channel stays open.
        loop {
            match channel
                .basic_get(&queue, BasicGetOptions { no_ack: true })
                .await
            {
                Ok(Some(delivery)) => {
                    recorder.record(String::from_utf8_lossy(&delivery.data).into_owned());
                }
                Ok(None) => tokio::time::sleep(Duration::from_millis(50)).await,
                Err(_) => break,
            }
        }
    }
}
