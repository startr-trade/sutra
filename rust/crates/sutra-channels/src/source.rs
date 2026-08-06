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

use crate::diag::Diagnostic;
use crate::dispatch::InboundMessage;
use crate::sink::BoxFuture;

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
