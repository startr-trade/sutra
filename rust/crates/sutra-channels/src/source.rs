//! The inbound transport seam — `TriggerSource` (broker/transport consumer lifecycle),
//! the `LeaderGate` singleton token, and the `InboundIntake` delivery hook into the intake
//! pipeline. Trait surface ONLY: the RabbitMQ trigger implements [`TriggerSource`], the
//! DB-lease election daemon implements [`LeaderGate`], and the engine assembly adapts
//! [`crate::http::EngineHandle`] + ack-mode into [`InboundIntake`].
//!
//! Shape: the trigger-source consumer lifecycle + the leader-election handle +
//! the ack-mode callback contract. The HTTP transport ([`crate::http`]) stays
//! axum-native — its "source" is the listening router; brokers are what this seam exists
//! for.

use std::sync::Arc;

use crate::codes;
use crate::diag::Diagnostic;
use crate::dispatch::InboundMessage;
use crate::sink::BoxFuture;
use crate::transport_audit::{
    transport_audit, TransportAuditEvent, TransportAuditOutcome, TransportAuditRegistry,
    TransportAuditScope,
};

/// The leadership token gating a singleton consumer — the minimal view of the
/// DB-lease election daemon (`LeaderElection` role
/// `sutra-channel:<tenant>:<channel>`, with the persistence layer's lease ttl/poll). A gated
/// consumer only consumes while `is_leading()` is true and MUST re-check on every
/// (re)connect and delivery loop turn — leadership can lapse mid-run (lease expiry), and
/// the gate is the only signal.
pub trait LeaderGate: Send + Sync {
    /// True while this replica holds the lease for the gated role.
    fn is_leading(&self) -> bool;
}

/// The no-election gate — non-singleton channels and single-replica hosts (the
/// no-op leader-election posture: everyone leads).
#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysLeading;

impl LeaderGate for AlwaysLeading {
    fn is_leading(&self) -> bool {
        true
    }
}

/// What the source must do with the transport delivery once the engine has decided —
/// aligned with the ack modes so a broker source maps it 1:1 onto
/// `basicAck` / `basicNack`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckDecision {
    /// The engine owns the delivery — `basicAck`. Under `ack-mode: on-persist` this
    /// resolves after inbox-dedup + durable intake; under `on-complete` only at instance
    /// COMPLETED (the deferred-ack registry holds it until then). A dedup DUPLICATE is
    /// also an `Ack` — the first observer owns the message.
    Ack,
    /// Transient failure (persistence unavailable, engine draining) — `basicNack`
    /// with requeue; the broker redelivers and inbox dedup absorbs the duplicate.
    NackRequeue,
    /// Permanent reject (auth/validation/resolve reject, instance FAILED under
    /// `on-complete`) — `basicNack` without requeue (DLQ posture); redelivery can never
    /// succeed.
    NackDrop,
}

/// The per-delivery settle callbacks a broker source hands the engine under
/// `ack-mode: on-complete` — plain `Send` closures over the transport's native ack/nack
/// (`basic.ack` / `basic.nack(requeue=false)` on RabbitMQ). They cross into the engine
/// actor thread with the dispatch request and are registered on the
/// [`crate::DeferredAckRegistry`] when the instance parks; the registry fires exactly one
/// of them at the instance's terminal event (or on timeout/overflow — always the nack).
/// Callbacks must be idempotent and must not block (spawn async transport ops).
pub struct DeferredSettle {
    /// Executed at `INSTANCE_COMPLETED` — the transport's `AckDecision::Ack` action.
    pub ack: Box<dyn FnMut() + Send>,
    /// Executed at `INSTANCE_FAILED` (permanent reject — the transport's
    /// `AckDecision::NackDrop` action) and on registry timeout/overflow.
    pub nack: Box<dyn FnMut() + Send>,
}

/// What a deferred-capable delivery resolved to — the [`InboundIntake::deliver_deferred`]
/// answer a broker source acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryDisposition {
    /// Execute this decision on the transport NOW (the classic settle-at-return path:
    /// the instance ran to a terminal state, was a duplicate, was dead-lettered, or
    /// dispatch rejected).
    Settle(AckDecision),
    /// The engine registered the delivery's [`DeferredSettle`] callbacks — the instance
    /// parked at a wait state. The source must NOT settle; the registered ack/nack fires
    /// at the instance's terminal event (or registry timeout/overflow).
    Deferred,
}

/// The audit event type of a dropped inbound delivery is `inbound.DELIVERY_DROPPED`: the namespace
/// a [`DeliveryDropRecord`] writes under, whichever transport the delivery came from.
const DROPPED_DELIVERY_NAMESPACE: &str = "inbound";
/// See [`DROPPED_DELIVERY_NAMESPACE`].
const DROPPED_DELIVERY_EVENT: &str = "DELIVERY_DROPPED";

/// What the engine records about an inbound delivery it settles without running: the delivery's
/// identity, taken before the message moves into the dispatch. Never the body or the headers,
/// which are business data and must not reach a log line.
#[derive(Debug, Clone)]
pub struct DeliveryDropRecord {
    tenant: String,
    module_key: String,
    channel: String,
    dedup_key: String,
    content_type: Option<String>,
    body_bytes: usize,
}

impl DeliveryDropRecord {
    /// The record of `message`.
    pub fn of(message: &InboundMessage) -> DeliveryDropRecord {
        DeliveryDropRecord {
            tenant: message.tenant.clone(),
            module_key: message.module_key.clone(),
            channel: message.channel.clone(),
            dedup_key: message.idempotency_key.clone(),
            content_type: message.content_type.clone(),
            body_bytes: message.body.len(),
        }
    }

    /// A permanent refusal: a WARN carrying [`codes::INBOUND_DELIVERY_DROPPED`] and the refusal's
    /// own code, and the deployment's audit record. Every delivery the engine refuses for good
    /// passes through here, whichever transport carried it.
    pub fn report(&self, cause: &Diagnostic) {
        tracing::warn!(
            code = codes::INBOUND_DELIVERY_DROPPED,
            cause_code = %cause.code,
            channel = %self.channel,
            module = %self.module_key,
            tenant = %self.tenant,
            dedup_key = %self.dedup_key,
            content_type = self.content_type.as_deref().unwrap_or(""),
            body_bytes = self.body_bytes,
            detail = %cause.message,
            "inbound delivery refused and dropped: the transport settles it and it will not run"
        );
        self.audit(cause);
    }

    /// The audit record alone, for a refusal the transport has already logged under its own code.
    pub fn audit(&self, cause: &Diagnostic) -> TransportAuditOutcome {
        self.audit_into(transport_audit(), cause)
    }

    fn audit_into(
        &self,
        registry: &TransportAuditRegistry,
        cause: &Diagnostic,
    ) -> TransportAuditOutcome {
        let detail = serde_json::json!({
            "channel": self.channel,
            "dedupKey": self.dedup_key,
            "contentType": self.content_type,
            "bodyBytes": self.body_bytes,
            "message": cause.message,
        });
        registry.append(
            TransportAuditScope::Deployment {
                module_key: self.module_key.clone(),
            },
            TransportAuditEvent::new(DROPPED_DELIVERY_NAMESPACE, DROPPED_DELIVERY_EVENT)
                .with_diagnostic_code(&cause.code)
                .with_diagnostic_json(&detail),
        )
    }

    /// A delivery whose event id was already seen: it is settled without running, and the first
    /// delivery owns the event. Not a drop, so INFO and no audit record; a sender that reused an id
    /// for a different message shows up here.
    pub fn report_duplicate(&self) {
        tracing::info!(
            code = codes::INBOUND_DUPLICATE_DELIVERY,
            channel = %self.channel,
            module = %self.module_key,
            tenant = %self.tenant,
            dedup_key = %self.dedup_key,
            "inbound delivery settled without running: its event id was already seen"
        );
    }
}

/// The intake side of the seam — delivery of one transport message into the intake
/// pipeline, answering the ack decision the source executes. The assembly's adapter owns
/// the ack-mode timing (WHEN the future resolves) and the diagnostic→decision mapping;
/// the source just awaits. Mirrors how [`crate::http::EngineHandle::dispatch`] carries
/// HTTP deliveries onto the engine actor.
pub trait InboundIntake: Send + Sync {
    /// Deliver one inbound message; resolves to the source's ack action once the
    /// delivering channel's ack-mode is satisfied.
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision>;

    /// `ack-mode: on-complete` delivery: deliver the message AND hand the engine the
    /// per-delivery settle callbacks. An intake with a deferred-ack path (the engine
    /// actor's `EngineIntake` in `sutra-transport-spi`) registers `settle` when the
    /// instance parks and answers [`DeliveryDisposition::Deferred`]; the default has no
    /// deferral capability and settles immediately with the plain [`Self::deliver`]
    /// decision (`settle` is dropped unfired — the returned decision IS the settle).
    fn deliver_deferred(
        &self,
        message: InboundMessage,
        settle: DeferredSettle,
    ) -> BoxFuture<'_, DeliveryDisposition> {
        Box::pin(async move {
            drop(settle);
            DeliveryDisposition::Settle(self.deliver(message).await)
        })
    }
}

/// One inbound transport consumer serving ONE channel binding — the singleton unit the
/// per-channel lease role gates. Constructed from the channel's [`crate::config::ChannelDefinition`]
/// (connection URL, queue, credentials via `${ENV}` refs); this trait is only the
/// lifecycle the engine drives.
pub trait TriggerSource: Send + Sync {
    /// Transport key, matching the channel's `transport:` value (e.g. `"rabbitmq"`).
    fn transport(&self) -> &str;

    /// The served channel's name (lease-role suffix + diagnostics).
    fn channel(&self) -> &str;

    /// Start consuming: every delivery goes to `intake` and the returned [`AckDecision`]
    /// is executed on the transport (broker ack/nack). A singleton consumer holds its
    /// subscription ONLY while `gate.is_leading()` — on losing leadership it must cancel
    /// the subscription (so `consumerCount` stays 1 across replicas) and re-subscribe if
    /// leadership returns. Resolves once the consumer is up (or the transport declared
    /// broker-absence, which is non-fatal — readiness must not block on it); consumption
    /// itself continues in the background until [`Self::stop`].
    fn start(
        &self,
        intake: Arc<dyn InboundIntake>,
        gate: Arc<dyn LeaderGate>,
    ) -> BoxFuture<'_, Result<(), Diagnostic>>;

    /// Stop consuming and release transport resources (drain posture: cancel the
    /// subscription first, let in-flight deliveries settle their acks). Idempotent.
    fn stop(&self) -> BoxFuture<'_, Result<(), Diagnostic>>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use super::*;

    fn inbound(idempotency_key: &str) -> InboundMessage {
        InboundMessage {
            tenant: "acme".to_string(),
            module_key: "acme/orders/1.0.0".to_string(),
            channel: "transfer-queue".to_string(),
            headers: BTreeMap::new(),
            body: b"{}".to_vec().into(),
            content_type: Some("application/json".to_string()),
            idempotency_key: idempotency_key.to_string(),
            explicit_event_id: true,
            received_at: "2026-07-12T00:00:00Z".to_string(),
            cloud_event: None,
        }
    }

    /// Scripted intake — answers the queued decisions in order.
    struct ScriptedIntake {
        decisions: Mutex<Vec<AckDecision>>,
        delivered: AtomicUsize,
    }

    impl InboundIntake for ScriptedIntake {
        fn deliver(&self, _message: InboundMessage) -> BoxFuture<'_, AckDecision> {
            Box::pin(async move {
                self.delivered.fetch_add(1, Ordering::SeqCst);
                self.decisions
                    .lock()
                    .expect("lock")
                    .pop()
                    .unwrap_or(AckDecision::NackRequeue)
            })
        }
    }

    struct FlippableGate(AtomicBool);

    impl LeaderGate for FlippableGate {
        fn is_leading(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// A minimal in-memory source: on start it delivers its queued messages one by one,
    /// checking the gate per delivery-loop turn (the singleton contract) and recording
    /// each ack decision the way a broker source would basicAck/basicNack.
    struct FakeSource {
        pending: Mutex<Vec<InboundMessage>>,
        acked: Mutex<Vec<AckDecision>>,
        stopped: AtomicBool,
    }

    impl TriggerSource for FakeSource {
        fn transport(&self) -> &str {
            "fake"
        }

        fn channel(&self) -> &str {
            "transfer-queue"
        }

        fn start(
            &self,
            intake: Arc<dyn InboundIntake>,
            gate: Arc<dyn LeaderGate>,
        ) -> BoxFuture<'_, Result<(), Diagnostic>> {
            Box::pin(async move {
                loop {
                    if !gate.is_leading() {
                        break; // gated out — the subscription is cancelled
                    }
                    let Some(message) = self.pending.lock().expect("lock").pop() else {
                        break;
                    };
                    let decision = intake.deliver(message).await;
                    self.acked.lock().expect("lock").push(decision);
                }
                Ok(())
            })
        }

        fn stop(&self) -> BoxFuture<'_, Result<(), Diagnostic>> {
            Box::pin(async move {
                self.stopped.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    /// Runs `body` with a subscriber that keeps what is logged, and answers it.
    fn logged(body: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Kept(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Kept {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("lock").extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let kept = Kept::default();
        let writer = kept.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        let text = String::from_utf8_lossy(&kept.0.lock().expect("lock")).into_owned();
        text
    }

    #[test]
    fn a_dropped_delivery_is_logged_at_warn_with_the_code_and_never_the_body() {
        let mut message = inbound("m-1");
        message.body = b"BODY-MUST-NOT-BE-LOGGED".to_vec().into();
        let record = DeliveryDropRecord::of(&message);
        let cause = Diagnostic::error(codes::INBOUND_VALIDATION_REJECT, "refused by the codec");
        let log = logged(|| record.report(&cause));
        assert!(log.contains(" WARN "), "{log}");
        assert!(log.contains("SUTRA.INBOUND.DELIVERY_DROPPED"), "{log}");
        assert!(log.contains(codes::INBOUND_VALIDATION_REJECT), "{log}");
        assert!(log.contains("transfer-queue"), "{log}");
        assert!(log.contains("refused by the codec"), "{log}");
        assert!(!log.contains("BODY-MUST-NOT-BE-LOGGED"), "{log}");
    }

    #[test]
    fn a_duplicate_is_logged_at_info_with_its_code_and_key() {
        let record = DeliveryDropRecord::of(&inbound("m-7"));
        let log = logged(|| record.report_duplicate());
        assert!(log.contains(" INFO "), "{log}");
        assert!(log.contains("SUTRA.INBOUND.DUPLICATE_DELIVERY"), "{log}");
        assert!(log.contains("m-7"), "{log}");
    }

    #[test]
    fn a_dropped_delivery_is_audited_in_the_deployment_journal() {
        let journal = Arc::new(crate::transport_audit::CapturingTransportAuditJournal::new());
        let registry = TransportAuditRegistry::new();
        registry.install(journal.clone());
        let outcome = DeliveryDropRecord::of(&inbound("m-1")).audit_into(
            &registry,
            &Diagnostic::error(codes::INBOUND_VALIDATION_REJECT, "refused"),
        );
        assert_eq!(outcome, TransportAuditOutcome::Queued);
        let events = journal.events();
        assert_eq!(events.len(), 1);
        let (scope, event) = &events[0];
        assert_eq!(
            *scope,
            TransportAuditScope::Deployment {
                module_key: "acme/orders/1.0.0".to_string()
            }
        );
        assert_eq!(
            event.event_type().as_deref(),
            Ok("inbound.DELIVERY_DROPPED")
        );
        assert_eq!(
            event.diagnostic_code.as_deref(),
            Some(codes::INBOUND_VALIDATION_REJECT)
        );
        // The channel survives metadata capture, which records none of the event's fields.
        let detail = event.diagnostic_json.as_deref().expect("detail");
        assert!(detail.contains("transfer-queue"), "{detail}");
        assert!(detail.contains("m-1"), "{detail}");
        assert!(event.fields.is_empty());
    }

    #[tokio::test]
    async fn source_delivers_and_executes_ack_decisions() {
        let intake = Arc::new(ScriptedIntake {
            decisions: Mutex::new(vec![AckDecision::NackDrop, AckDecision::Ack]),
            delivered: AtomicUsize::new(0),
        });
        let source = FakeSource {
            pending: Mutex::new(vec![inbound("m-2"), inbound("m-1")]),
            acked: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
        };

        source
            .start(intake.clone(), Arc::new(AlwaysLeading))
            .await
            .expect("start");

        assert_eq!(intake.delivered.load(Ordering::SeqCst), 2);
        assert_eq!(
            *source.acked.lock().expect("lock"),
            vec![AckDecision::Ack, AckDecision::NackDrop]
        );
        source.stop().await.expect("stop");
        assert!(source.stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn non_leading_gate_keeps_the_consumer_silent() {
        let intake = Arc::new(ScriptedIntake {
            decisions: Mutex::new(vec![AckDecision::Ack]),
            delivered: AtomicUsize::new(0),
        });
        let source = FakeSource {
            pending: Mutex::new(vec![inbound("m-1")]),
            acked: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
        };

        let gate = Arc::new(FlippableGate(AtomicBool::new(false)));
        source
            .start(intake.clone(), gate.clone())
            .await
            .expect("start");
        assert_eq!(intake.delivered.load(Ordering::SeqCst), 0);

        // Leadership arrives — a (re)start consumes.
        gate.0.store(true, Ordering::SeqCst);
        source.start(intake.clone(), gate).await.expect("restart");
        assert_eq!(intake.delivered.load(Ordering::SeqCst), 1);
    }
}
