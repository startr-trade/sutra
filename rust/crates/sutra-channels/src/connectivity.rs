//! Broker connectivity events: a transport that holds a connection to a broker reports when the
//! connection is lost and when it comes back, and the engine can hand each report to the
//! deployment that owns the channel, as an inbound JSON message on a channel the operator names
//! (`sutra.engine.connectivity-events.channel`). A deployment that declares an intake channel of
//! that name (normally `transport: local`) can then record its own outage windows, for example
//! in a run's results, without reading the engine's log.
//!
//! The pieces, all protocol-neutral:
//! - [`ConnectivityEvent`] — one change: a channel went down, or came back up after an outage
//!   of a given length. Its [`ConnectivityEvent::to_json`] body is the message a package receives.
//! - [`ConnectivityReporter`] — the handle a transport reports through. It is a shared slot that
//!   is empty (every report a no-op) until the engine installs a [`ConnectivitySink`]; a handle
//!   taken before the install sees it. An inbound source gets it from its intake
//!   ([`crate::source::InboundIntake::connectivity`]); an outbound sink gets it, with the identity
//!   of the channel it is sending for, from [`current_send`] while the outbox dispatcher runs
//!   its `send`.
//! - [`ChannelConnectivityDelivery`] — the engine's sink: a bounded in-memory queue per
//!   deployment (oldest event dropped on overflow, the drop counted and logged) drained by one
//!   task that dispatches each event to the named channel of the owning deployment. Delivery is
//!   best effort: an event is never retried, and a deployment that declares no such channel
//!   simply receives nothing.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};

use crate::dispatch::InboundMessage;
use crate::http::EngineHandle;

/// WARN: connectivity events were dropped because a deployment's queue was full.
pub const CONNECTIVITY_EVENTS_DROPPED: &str = "SUTRA.CONNECTIVITY.EVENTS_DROPPED";
/// WARN: a connectivity event reached its deployment's channel and the dispatch refused it.
pub const CONNECTIVITY_EVENT_UNDELIVERED: &str = "SUTRA.CONNECTIVITY.EVENT_UNDELIVERED";

/// The per-deployment queue bound of [`ChannelConnectivityDelivery`].
pub const CONNECTIVITY_QUEUE_CAPACITY: usize = 1024;

/// The engine-internal header the dispatcher stamps on a send, naming the channel it was sent
/// on, so the outbox dispatcher can tell a sink which channel a delivery belongs to (the row's
/// labels already name the deployment). Engine bookkeeping: removed when the row is rendered
/// for the wire, so no sink and no counterpart ever sees it.
pub const SEND_CHANNEL_HEADER: &str = "sutra-send-channel";

/// The `<tenant>/<module>/<version>` key an outbox row's labels name, when they name a module.
pub fn module_key_of_labels(labels: &BTreeMap<String, String>) -> Option<String> {
    let tenant = labels.get("tenant")?;
    let module = labels.get("module")?;
    let version = labels
        .get("version")
        .map(String::as_str)
        .unwrap_or_default();
    Some(format!("{tenant}/{module}/{version}"))
}

/// Which way the channel carries messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectivityDirection {
    Inbound,
    Outbound,
}

impl ConnectivityDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            ConnectivityDirection::Inbound => "inbound",
            ConnectivityDirection::Outbound => "outbound",
        }
    }
}

/// One connectivity change of one channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectivityEvent {
    /// The owning deployment's tenant.
    pub tenant: String,
    /// The owning deployment's `<tenant>/<module>/<version>` key.
    pub module_key: String,
    /// The transport's own stable code for the change (the code it logs it under).
    pub code: String,
    /// The channel whose connection changed.
    pub channel: String,
    pub direction: ConnectivityDirection,
    /// RFC 3339 UTC time the outage started.
    pub started_at: String,
    /// `Some` (RFC 3339 UTC) when the outage has ended: the channel is up again. `None` while it
    /// is down.
    pub ended_at: Option<String>,
    /// The length of an ended outage in milliseconds.
    pub millis: Option<u64>,
    /// A short human-readable description (which broker, what failed). Never a credential.
    pub detail: String,
}

impl ConnectivityEvent {
    /// `down` while the outage is in progress, `up` once it has ended.
    pub fn state(&self) -> &'static str {
        if self.ended_at.is_some() {
            "up"
        } else {
            "down"
        }
    }

    /// The JSON body a deployment receives:
    /// `{"code","channel","direction","state","startedAt"[,"endedAt","ms"],"detail"}`.
    pub fn to_json(&self) -> Vec<u8> {
        let mut body = serde_json::Map::new();
        body.insert("code".into(), self.code.clone().into());
        body.insert("channel".into(), self.channel.clone().into());
        body.insert("direction".into(), self.direction.as_str().into());
        body.insert("state".into(), self.state().into());
        body.insert("startedAt".into(), self.started_at.clone().into());
        if let Some(ended) = &self.ended_at {
            body.insert("endedAt".into(), ended.clone().into());
        }
        if let Some(ms) = self.millis {
            body.insert("ms".into(), ms.into());
        }
        body.insert("detail".into(), self.detail.clone().into());
        serde_json::to_vec(&serde_json::Value::Object(body)).unwrap_or_default()
    }
}

/// Where reported events go. The engine installs one; see [`ChannelConnectivityDelivery`].
pub trait ConnectivitySink: Send + Sync {
    /// Take one event. Must not block: called from a transport's connection loop.
    fn accept(&self, event: ConnectivityEvent);
}

/// The handle a transport reports connectivity changes through. Cheap to clone; every clone
/// shares one slot, so a handle a transport took at wiring time sees a sink installed later.
/// With no sink installed every report is a no-op.
#[derive(Clone, Default)]
pub struct ConnectivityReporter {
    slot: Arc<OnceLock<Arc<dyn ConnectivitySink>>>,
}

impl std::fmt::Debug for ConnectivityReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectivityReporter")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

impl ConnectivityReporter {
    /// A reporter with an empty slot. Reports are dropped until [`Self::install`].
    pub fn new() -> ConnectivityReporter {
        ConnectivityReporter::default()
    }

    /// Install the sink every clone of this reporter reports to. The first install wins;
    /// `false` if a sink was already installed.
    pub fn install(&self, sink: Arc<dyn ConnectivitySink>) -> bool {
        self.slot.set(sink).is_ok()
    }

    /// Whether a sink is installed, so a transport can skip building events nobody reads.
    pub fn is_enabled(&self) -> bool {
        self.slot.get().is_some()
    }

    /// Report one change. A no-op with no sink installed.
    pub fn report(&self, event: ConnectivityEvent) {
        if let Some(sink) = self.slot.get() {
            sink.accept(event);
        }
    }
}

/// The identity of the channel a sink is sending for, and the reporter to report through.
#[derive(Debug, Clone)]
pub struct SendContext {
    pub tenant: String,
    pub module_key: String,
    pub channel: String,
    pub reporter: ConnectivityReporter,
}

tokio::task_local! {
    static SEND_CONTEXT: SendContext;
}

/// The [`SendContext`] of the delivery the current task is sending, when the outbox dispatcher
/// knows the channel and a connectivity sink is installed. `None` otherwise.
pub fn current_send() -> Option<SendContext> {
    SEND_CONTEXT.try_with(Clone::clone).ok()
}

/// Run `send` with `context` visible to [`current_send`].
pub async fn with_send_context<F: Future>(context: SendContext, send: F) -> F::Output {
    SEND_CONTEXT.scope(context, send).await
}

/// The tenant of a `<tenant>/<module>/<version>` key.
pub fn tenant_of(module_key: &str) -> &str {
    module_key.split('/').next().unwrap_or_default()
}

/// The engine's [`ConnectivitySink`]: queue each event under its deployment, and dispatch it
/// to that deployment's `channel` as an inbound JSON message.
pub struct ChannelConnectivityDelivery {
    channel: String,
    capacity: usize,
    state: Mutex<DeliveryQueues>,
    wake: tokio::sync::Notify,
}

#[derive(Default)]
struct DeliveryQueues {
    queues: HashMap<String, VecDeque<ConnectivityEvent>>,
    /// Round-robin order of deployments with queued events.
    order: VecDeque<String>,
    dropped: BTreeMap<String, u64>,
}

impl ChannelConnectivityDelivery {
    /// A delivery to `channel` with a queue of `capacity` per deployment.
    pub fn new(channel: &str, capacity: usize) -> ChannelConnectivityDelivery {
        ChannelConnectivityDelivery {
            channel: channel.trim().to_string(),
            capacity: capacity.max(1),
            state: Mutex::new(DeliveryQueues::default()),
            wake: tokio::sync::Notify::new(),
        }
    }

    /// Install a delivery to `channel` on `engine`'s reporter and start the task that drains it
    /// on `runtime`. The task ends when the engine stops answering. `false` (and nothing
    /// started) if a sink was already installed.
    pub fn install(engine: &EngineHandle, channel: &str, runtime: &tokio::runtime::Handle) -> bool {
        let delivery = Arc::new(ChannelConnectivityDelivery::new(
            channel,
            CONNECTIVITY_QUEUE_CAPACITY,
        ));
        if !engine.connectivity().install(delivery.clone()) {
            return false;
        }
        let engine = engine.clone();
        runtime.spawn(async move { delivery.run(engine).await });
        true
    }

    /// The next queued event, round-robin across deployments.
    fn pop(&self) -> Option<ConnectivityEvent> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while let Some(module_key) = state.order.pop_front() {
            let Some(queue) = state.queues.get_mut(&module_key) else {
                continue;
            };
            let Some(event) = queue.pop_front() else {
                state.queues.remove(&module_key);
                continue;
            };
            if queue.is_empty() {
                state.queues.remove(&module_key);
            } else {
                state.order.push_back(module_key);
            }
            return Some(event);
        }
        None
    }

    /// The inbound message one event becomes.
    pub fn message_for(&self, event: &ConnectivityEvent) -> InboundMessage {
        InboundMessage {
            tenant: event.tenant.clone(),
            module_key: event.module_key.clone(),
            channel: self.channel.clone(),
            headers: Default::default(),
            body: event.to_json().into(),
            content_type: Some("application/json".to_string()),
            // One change of one channel happens once: its code, channel and start name it.
            idempotency_key: format!(
                "connectivity:{}:{}:{}:{}",
                event.code,
                event.direction.as_str(),
                event.channel,
                event.started_at
            ),
            explicit_event_id: true,
            received_at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            cloud_event: None,
        }
    }

    /// Drain the queues into the engine until it stops answering.
    pub async fn run(self: Arc<Self>, engine: EngineHandle) {
        loop {
            let notified = self.wake.notified();
            let Some(event) = self.pop() else {
                notified.await;
                continue;
            };
            let message = self.message_for(&event);
            match engine.dispatch(message).await {
                Ok(_) => {}
                Err(diagnostic) if crate::sink::is_engine_unavailable(&diagnostic) => return,
                // The deployment declares no channel of that name: it asked for nothing.
                Err(diagnostic) if diagnostic.code == crate::codes::RESOLVE_CHANNEL_UNKNOWN => {
                    tracing::debug!(
                        module_key = %event.module_key,
                        channel = %self.channel,
                        "connectivity event not delivered: the deployment has no such channel"
                    );
                }
                Err(diagnostic) => tracing::warn!(
                    code = CONNECTIVITY_EVENT_UNDELIVERED,
                    module_key = %event.module_key,
                    channel = %self.channel,
                    event_code = %event.code,
                    refusal = %diagnostic.code,
                    "connectivity event refused by the deployment: {}",
                    diagnostic.message
                ),
            }
        }
    }
}

impl ConnectivitySink for ChannelConnectivityDelivery {
    fn accept(&self, event: ConnectivityEvent) {
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let module_key = event.module_key.clone();
            let queue = state.queues.entry(module_key.clone()).or_default();
            let was_empty = queue.is_empty();
            let mut dropped = false;
            if queue.len() >= self.capacity {
                queue.pop_front();
                dropped = true;
            }
            queue.push_back(event);
            if was_empty {
                state.order.push_back(module_key.clone());
            }
            if dropped {
                let total = state.dropped.entry(module_key.clone()).or_default();
                *total += 1;
                // Logged on the first drop and then at each power of two, so a flapping broker
                // cannot flood the log with the record of its own flapping.
                if total.is_power_of_two() {
                    tracing::warn!(
                        code = CONNECTIVITY_EVENTS_DROPPED,
                        module_key = %module_key,
                        dropped_total = *total,
                        capacity = self.capacity,
                        "connectivity event queue full; the oldest event was dropped"
                    );
                }
            }
        }
        self.wake.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(module_key: &str, n: u32, ended: bool) -> ConnectivityEvent {
        ConnectivityEvent {
            tenant: tenant_of(module_key).to_string(),
            module_key: module_key.to_string(),
            code: if ended {
                "X.OUTAGE_ENDED"
            } else {
                "X.OUTAGE_STARTED"
            }
            .to_string(),
            channel: format!("in-{n}"),
            direction: ConnectivityDirection::Inbound,
            started_at: "2026-11-19T10:00:00.120Z".to_string(),
            ended_at: ended.then(|| "2026-11-19T10:01:00.420Z".to_string()),
            millis: ended.then_some(60_300),
            detail: "broker 127.0.0.1:5672".to_string(),
        }
    }

    #[test]
    fn the_json_body_matches_the_documented_shape() {
        let down: serde_json::Value =
            serde_json::from_slice(&event("t/m/1", 1, false).to_json()).unwrap();
        assert_eq!(
            down,
            serde_json::json!({
                "code": "X.OUTAGE_STARTED", "channel": "in-1", "direction": "inbound",
                "state": "down", "startedAt": "2026-11-19T10:00:00.120Z",
                "detail": "broker 127.0.0.1:5672"
            })
        );
        let up: serde_json::Value =
            serde_json::from_slice(&event("t/m/1", 1, true).to_json()).unwrap();
        assert_eq!(up["state"], "up");
        assert_eq!(up["endedAt"], "2026-11-19T10:01:00.420Z");
        assert_eq!(up["ms"], 60_300);
    }

    #[test]
    fn a_reporter_is_a_no_op_until_a_sink_is_installed_and_clones_share_it() {
        struct Count(Mutex<usize>);
        impl ConnectivitySink for Count {
            fn accept(&self, _: ConnectivityEvent) {
                *self.0.lock().unwrap() += 1;
            }
        }
        let reporter = ConnectivityReporter::new();
        let early = reporter.clone();
        early.report(event("t/m/1", 1, false));
        assert!(!early.is_enabled());
        let count = Arc::new(Count(Mutex::new(0)));
        assert!(reporter.install(count.clone()));
        assert!(!reporter.install(count.clone()), "the first install wins");
        early.report(event("t/m/1", 1, false));
        assert!(early.is_enabled());
        assert_eq!(*count.0.lock().unwrap(), 1);
    }

    #[test]
    fn a_full_queue_drops_the_oldest_event_of_that_deployment_only() {
        let delivery = ChannelConnectivityDelivery::new("connectivity-in", 2);
        for n in 0..3 {
            delivery.accept(event("t/a/1", n, false));
        }
        delivery.accept(event("t/b/1", 9, false));
        let drained: Vec<(String, String)> = std::iter::from_fn(|| delivery.pop())
            .map(|e| (e.module_key, e.channel))
            .collect();
        assert_eq!(
            drained,
            vec![
                ("t/a/1".to_string(), "in-1".to_string()),
                ("t/b/1".to_string(), "in-9".to_string()),
                ("t/a/1".to_string(), "in-2".to_string()),
            ]
        );
        let state = delivery.state.lock().unwrap();
        assert_eq!(state.dropped.get("t/a/1"), Some(&1));
        assert_eq!(state.dropped.get("t/b/1"), None);
    }

    #[test]
    fn the_message_goes_to_the_configured_channel_of_the_owning_deployment() {
        let delivery = ChannelConnectivityDelivery::new(" connectivity-in ", 4);
        let message = delivery.message_for(&event("acme/sim/1.0.0", 3, true));
        assert_eq!(message.tenant, "acme");
        assert_eq!(message.module_key, "acme/sim/1.0.0");
        assert_eq!(message.channel, "connectivity-in");
        assert_eq!(message.content_type.as_deref(), Some("application/json"));
        assert!(message.explicit_event_id);
        assert_eq!(
            message.idempotency_key,
            "connectivity:X.OUTAGE_ENDED:inbound:in-3:2026-11-19T10:00:00.120Z"
        );
    }

    #[tokio::test]
    async fn the_send_context_is_visible_only_inside_its_scope() {
        assert!(current_send().is_none());
        let context = SendContext {
            tenant: "t".into(),
            module_key: "t/m/1".into(),
            channel: "out".into(),
            reporter: ConnectivityReporter::new(),
        };
        let seen = with_send_context(context, async { current_send().map(|c| c.channel) }).await;
        assert_eq!(seen.as_deref(), Some("out"));
        assert!(current_send().is_none());
    }
}
