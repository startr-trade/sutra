//! The outbound delivery seam — `MessageSink` (scheme-keyed transport send) and the
//! `SinkRegistry` the outbox dispatcher resolves against. This file defines the trait
//! surface ONLY; the HTTP sink and the broker sinks implement it, and the dispatcher
//! loop drives it.
//!
//! Shape: the message-sink SPI + its sink-registry pair.
//! Like [`crate::bridge`], this seam is persistence-agnostic — the dispatcher owns the
//! outbox rows (`sutra-persistence`); a sink sees only the transport-neutral
//! [`OutboundMessage`] and answers with a tri-state [`SendOutcome`] the dispatcher maps
//! to its row action: `Delivered` → delete, `RetryableFailure` → defer with backoff,
//! `PermanentFailure` → poison isolation (delivery is at-least-once, with consumer
//! idempotency via `outbox_key` / `Idempotency-Key`).
//!
//! Unlike the `Rc`-based intake engine, sinks run on the tokio side (the dispatcher is a
//! spawned interval task), so everything here is `Send + Sync` and the send is async via
//! the dependency-free boxed-future seam ([`BoxFuture`]).

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::diag::Diagnostic;
#[cfg(feature = "transport")]
use crate::dispatch::InboundMessage;
#[cfg(feature = "transport")]
use crate::http::EngineHandle;

/// Dependency-free boxed future — the dyn-compatible async seam every transport
/// trait uses (implementations write `Box::pin(async move { … })`).
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One transport-neutral outbound delivery — what the outbox dispatcher builds from a
/// claimed outbox row and hands to the scheme-resolved sink. Auth material (resolved
/// from the emission's auth-ref by the dispatcher, never by the sink) rides `headers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    /// Scheme-bearing destination URI (`https://host/cb`, `rabbitmq://user:pass@host:5672/queue`).
    pub destination: String,
    /// Transport headers / broker properties (stringly, deterministic order).
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    /// The row's `outbox_key` — the sink MUST put it on the wire as the consumer-side
    /// idempotency key (`Idempotency-Key` header / broker `messageId`).
    pub outbox_key: String,
    /// W3C `traceparent` persisted at enqueue; restores trace context on the
    /// `sutra.outbox.send` delivery span (trace-context bridge) and rides the wire when present.
    pub traceparent: Option<String>,
}

/// Tri-state delivery outcome — the dispatcher's row action is a pure function of this
/// (delete / defer-with-backoff / poison). A sink NEVER panics a delivery: any failure
/// becomes one of the failure arms with a diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendOutcome {
    /// The transport accepted the message — the dispatcher deletes the row.
    Delivered,
    /// Transient failure (connect refused, 5xx, broker down) — the dispatcher defers the
    /// row with exponential backoff and a later tick retries (at-least-once).
    RetryableFailure(Diagnostic),
    /// Non-transient failure (malformed destination, 4xx contract reject) — retrying can
    /// never succeed; the dispatcher isolates the row as poison instead of blocking the
    /// batch.
    PermanentFailure(Diagnostic),
}

/// One outbound transport, keyed by the URI scheme(s) it serves — async send of an
/// [`OutboundMessage`], answering [`SendOutcome`]. Implementations own their connection
/// lifecycle (pooling, reconnect) behind this seam.
pub trait MessageSink: Send + Sync {
    /// The lowercase URI schemes this sink claims (e.g. `["http", "https"]`,
    /// `["rabbitmq"]`).
    fn schemes(&self) -> Vec<String>;

    /// Deliver one message to `message.destination`. Must be infallible in the panic
    /// sense — failures are [`SendOutcome`] arms, so per-entry poison isolation stays a
    /// dispatcher-side policy.
    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome>;
}

/// Scheme → sink resolution for the outbox dispatcher (the sink registry).
/// Registration is by the sink's own claimed [`MessageSink::schemes`]; last registration
/// of a scheme wins (mirrors [`crate::registry::CodecRegistry`]).
#[derive(Default, Clone)]
pub struct SinkRegistry {
    sinks: HashMap<String, Arc<dyn MessageSink>>,
}

impl SinkRegistry {
    pub fn new() -> SinkRegistry {
        SinkRegistry::default()
    }

    /// Register `sink` under every scheme it claims (lowercased).
    pub fn register(&mut self, sink: Arc<dyn MessageSink>) -> &mut SinkRegistry {
        for scheme in sink.schemes() {
            self.sinks
                .insert(scheme.to_ascii_lowercase(), Arc::clone(&sink));
        }
        self
    }

    /// The sink registered for `scheme` (case-insensitive), if any.
    pub fn find(&self, scheme: &str) -> Option<Arc<dyn MessageSink>> {
        self.sinks.get(&scheme.to_ascii_lowercase()).cloned()
    }

    /// Resolve a destination URI to its sink by scheme. `None` when the destination has
    /// no parsable scheme or no sink claims it — the dispatcher maps that to its poison
    /// posture (a retry can never grow a sink).
    pub fn resolve(&self, destination: &str) -> Option<Arc<dyn MessageSink>> {
        scheme_of(destination).and_then(|scheme| self.find(scheme))
    }

    /// Every registered scheme, sorted (diagnostics).
    pub fn schemes(&self) -> Vec<String> {
        let mut schemes: Vec<String> = self.sinks.keys().cloned().collect();
        schemes.sort_unstable();
        schemes
    }
}

/// The URI scheme of a PULL-parked delivery, and the channel `transport:` value that declares
/// one (see [`crate::external_task`]). Model-level rather than transport-gated because the
/// deploy-time lint — which compiles without the transport spine — must recognise a
/// `pull://<channel>` bind and check its target the same way it checks `local://`.
pub const PULL_SCHEME: &str = "pull";

/// The RFC 3986 scheme of a destination URI (`ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`
/// before `"://"`), or `None` when the destination carries none.
pub fn scheme_of(destination: &str) -> Option<&str> {
    let (scheme, _) = destination.split_once("://")?;
    let mut bytes = scheme.bytes();
    let first = bytes.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if bytes.all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'-' || b == b'.') {
        Some(scheme)
    } else {
        None
    }
}

// ---- the in-process delivery sink (scheme `local`) -----------------------------------------

/// The in-process delivery sink — the `local` scheme among the transports. Delivers a
/// co-deployed inter-process hop WITHOUT leaving the engine: instead of dialing a transport,
/// it reconstructs the [`InboundMessage`] the destination encodes and re-enters the engine
/// through the SAME [`EngineHandle::dispatch`] seam every transport uses. Registered directly
/// in `build_engine_runtime` (it captures the engine handle, so it cannot ride the bare
/// `TransportFactory::register_sink` fn-ptr).
///
/// Actor-safe: like every sink it runs on the tokio side (the outbox tick loop), OFF the
/// `Rc`-based actor thread; `dispatch` enqueues an `EngineRequest::Dispatch` on the engine
/// mpsc — a fresh serialized turn, never a nested call (identical to the timer poller).
///
/// Auth-free: delivery is BELOW the transport auth layer — no credential is presented or
/// required. Header propagation is load-bearing: the reconstructed message carries the row's
/// `headers` (so `x-uetr` correlates the hop via `<q:alias expression="header.x-uetr">`) PLUS
/// a `traceparent` header restored from the persisted trace context (so C6 coverage gets its
/// `trace_id`).
#[cfg(feature = "transport")]
pub struct LocalDeliverySink {
    engine: EngineHandle,
}

#[cfg(feature = "transport")]
impl LocalDeliverySink {
    pub fn new(engine: EngineHandle) -> LocalDeliverySink {
        LocalDeliverySink { engine }
    }

    /// Reconstruct the [`InboundMessage`] a `local://<module_key>/<channel>` delivery encodes:
    /// `channel` + `module_key`/`tenant` parsed from the destination; `headers` = the row's
    /// headers PLUS the restored `traceparent`; `idempotency_key` = the `outbox_key` (drives
    /// inbox dedup, so `explicit_event_id` is true). A malformed destination is a permanent
    /// failure (a retry can never re-shape it).
    fn reconstruct(message: &OutboundMessage) -> Result<InboundMessage, Diagnostic> {
        let (tenant, module_key, channel) = parse_local_destination(&message.destination)
            .ok_or_else(|| {
                Diagnostic::error(
                    crate::codes::OUTBOUND_SEND_FAILED,
                    format!(
                        "local delivery destination '{}' is not a \
                         'local://<module_key>/<channel>' URI",
                        message.destination
                    ),
                )
            })?;
        let mut headers = message.headers.clone();
        if let Some(traceparent) = message
            .traceparent
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            headers.insert(
                crate::telemetry::TRACEPARENT_HEADER.to_string(),
                traceparent.to_string(),
            );
        }
        Ok(InboundMessage {
            tenant,
            module_key,
            channel,
            headers,
            body: message.body.clone().into(),
            content_type: message.content_type.clone(),
            idempotency_key: message.outbox_key.clone(),
            explicit_event_id: true,
            received_at: now_rfc3339(),
            cloud_event: None,
        })
    }
}

#[cfg(feature = "transport")]
impl MessageSink for LocalDeliverySink {
    fn schemes(&self) -> Vec<String> {
        vec!["local".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            let inbound = match LocalDeliverySink::reconstruct(message) {
                Ok(inbound) => inbound,
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            // Mirror `EngineIntake::deliver`: Ok ⇒ delivered (first-observer owns it); an
            // unavailable engine actor ⇒ retryable (a later tick redelivers, inbox dedup
            // absorbs it); every reject diagnostic ⇒ permanent (poison isolation).
            match self.engine.dispatch(inbound).await {
                Ok(_) => SendOutcome::Delivered,
                Err(diagnostic) if is_engine_unavailable(&diagnostic) => {
                    SendOutcome::RetryableFailure(diagnostic)
                }
                Err(diagnostic) => SendOutcome::PermanentFailure(diagnostic),
            }
        })
    }
}

/// Split a `local://<module_key>/<channel>` destination into `(tenant, module_key, channel)`.
/// `module_key` is the version-bearing `"<tenant>/<module>/<version>"` triple, so the channel
/// is the LAST path segment and the tenant is the FIRST.
#[cfg(feature = "transport")]
fn parse_local_destination(destination: &str) -> Option<(String, String, String)> {
    let rest = destination.strip_prefix("local://")?;
    let (module_key, channel) = rest.rsplit_once('/')?;
    if module_key.is_empty() || channel.is_empty() {
        return None;
    }
    let tenant = module_key.split('/').next().filter(|t| !t.is_empty())?;
    Some((
        tenant.to_string(),
        module_key.to_string(),
        channel.to_string(),
    ))
}

/// The one transient failure the dispatch surface reports (mirrors
/// `sutra_transport_spi::EngineIntake`): the engine actor being gone. Everything else under
/// `SUTRA.RUNTIME.UNEXPECTED` is a dispatch crash and stays a permanent reject.
#[cfg(feature = "transport")]
fn is_engine_unavailable(diagnostic: &Diagnostic) -> bool {
    diagnostic.code == crate::codes::RUNTIME_UNEXPECTED
        && diagnostic.message == "engine actor is not running"
}

#[cfg(feature = "transport")]
fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedSink {
        schemes: Vec<String>,
        outcome: SendOutcome,
    }

    impl MessageSink for FixedSink {
        fn schemes(&self) -> Vec<String> {
            self.schemes.clone()
        }

        fn send<'a>(&'a self, _message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
            Box::pin(async move { self.outcome.clone() })
        }
    }

    fn message(destination: &str) -> OutboundMessage {
        OutboundMessage {
            destination: destination.to_string(),
            headers: BTreeMap::new(),
            body: b"{}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "ob-1".to_string(),
            traceparent: None,
        }
    }

    #[test]
    fn scheme_of_parses_rfc3986_schemes() {
        assert_eq!(scheme_of("https://host/cb"), Some("https"));
        assert_eq!(scheme_of("rabbitmq://u:p@host:5672/q"), Some("rabbitmq"));
        assert_eq!(scheme_of("amqp+tls://host/q"), Some("amqp+tls"));
        assert_eq!(scheme_of("no-scheme-here"), None);
        assert_eq!(scheme_of("://empty"), None);
        assert_eq!(scheme_of("1nvalid://x"), None);
        assert_eq!(scheme_of("ht tp://x"), None);
    }

    #[test]
    fn registry_resolves_by_destination_scheme_case_insensitively() {
        let mut registry = SinkRegistry::new();
        registry.register(Arc::new(FixedSink {
            schemes: vec!["http".to_string(), "https".to_string()],
            outcome: SendOutcome::Delivered,
        }));
        registry.register(Arc::new(FixedSink {
            schemes: vec!["rabbitmq".to_string()],
            outcome: SendOutcome::Delivered,
        }));

        assert!(registry.resolve("https://host/cb").is_some());
        assert!(registry.resolve("HTTP://host/cb").is_some());
        assert!(registry.resolve("rabbitmq://u:p@host:5672/q").is_some());
        assert!(registry.resolve("kafka://host/topic").is_none());
        assert!(registry.resolve("not-a-uri").is_none());
        assert_eq!(registry.schemes(), vec!["http", "https", "rabbitmq"]);
    }

    #[tokio::test]
    async fn send_answers_the_tri_state_the_dispatcher_maps() {
        let delivered = FixedSink {
            schemes: vec!["http".to_string()],
            outcome: SendOutcome::Delivered,
        };
        let retryable = FixedSink {
            schemes: vec!["http".to_string()],
            outcome: SendOutcome::RetryableFailure(Diagnostic::error(
                "SUTRA.RUNTIME.UNEXPECTED",
                "connect refused",
            )),
        };
        let msg = message("http://host/cb");
        assert_eq!(delivered.send(&msg).await, SendOutcome::Delivered);
        assert!(matches!(
            retryable.send(&msg).await,
            SendOutcome::RetryableFailure(_)
        ));
    }

    #[cfg(feature = "transport")]
    #[test]
    fn parse_local_destination_splits_module_key_and_channel() {
        assert_eq!(
            parse_local_destination("local://acme/demoflow/1.0.0/demoflow-in"),
            Some((
                "acme".to_string(),
                "acme/demoflow/1.0.0".to_string(),
                "demoflow-in".to_string()
            ))
        );
        // Not a local:// URI / no channel segment / empty parts → None.
        assert_eq!(parse_local_destination("https://host/cb"), None);
        assert_eq!(parse_local_destination("local://demoflow-in"), None);
        assert_eq!(parse_local_destination("local:///demoflow-in"), None);
    }

    #[cfg(feature = "transport")]
    #[test]
    fn reconstruct_preserves_x_uetr_and_restores_traceparent() {
        let mut headers = BTreeMap::new();
        headers.insert("x-uetr".to_string(), "UETR-123".to_string());
        headers.insert("content-type".to_string(), "application/json".to_string());
        let msg = OutboundMessage {
            destination: "local://acme/demoflow/1.0.0/demoflow-in".to_string(),
            headers,
            body: b"{\"amount\":10}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "ob-42".to_string(),
            traceparent: Some("00-abc-def-01".to_string()),
        };

        let inbound = LocalDeliverySink::reconstruct(&msg).expect("well-formed local destination");
        assert_eq!(inbound.tenant, "acme");
        assert_eq!(inbound.module_key, "acme/demoflow/1.0.0");
        assert_eq!(inbound.channel, "demoflow-in");
        // Correlation header rides through verbatim (load-bearing for C6 union-find).
        assert_eq!(
            inbound.headers.get("x-uetr").map(String::as_str),
            Some("UETR-123")
        );
        // traceparent restored from the persisted trace context (load-bearing for trace_id).
        assert_eq!(
            inbound
                .headers
                .get(crate::telemetry::TRACEPARENT_HEADER)
                .map(String::as_str),
            Some("00-abc-def-01")
        );
        // outbox_key becomes the explicit idempotency key (inbox dedup).
        assert_eq!(inbound.idempotency_key, "ob-42");
        assert!(inbound.explicit_event_id);
        assert_eq!(inbound.body.into_inner(), b"{\"amount\":10}");
    }

    #[cfg(feature = "transport")]
    #[test]
    fn reconstruct_rejects_a_malformed_local_destination() {
        // A destination with no channel segment can never be re-shaped by a retry — `send`
        // maps this `Err` to `SendOutcome::PermanentFailure` (poison isolation).
        let msg = message("local://no-channel-segment");
        let err = LocalDeliverySink::reconstruct(&msg).expect_err("malformed destination");
        assert_eq!(err.code, crate::codes::OUTBOUND_SEND_FAILED);
    }
}
