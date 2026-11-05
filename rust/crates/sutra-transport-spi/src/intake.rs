//! The engine-actor intake adapter — moved DOWN out of `sutra-engine` (domain-neutrality
//! refactor) so the vendor transport crates can wrap the engine actor without depending on
//! `sutra-engine` (which would be a cycle: the engine must depend on them to bundle them).

use sutra_channels::http::EngineHandle;
use sutra_channels::{
    AckDecision, BoxFuture, DeferredDispatch, DeferredSettle, DeliveryDisposition,
    DeliveryDropRecord, Diagnostic, DispatchOutcome, InboundIntake, InboundMessage,
    ACK_DISPOSITION_ATTR, ACK_DISPOSITION_REQUEUE,
};

/// Adapt the engine actor into the [`InboundIntake`] seam. Decision mapping (the
/// seam contract): `Ok(Completed | Duplicate | DeadLettered)` ⇒ [`AckDecision::Ack`] (the
/// first observer owns the message; a non-idempotent execution failure is dead-lettered —
/// consumed at-most-once with a durable incident recorded), an unavailable engine actor
/// (draining / shut down) OR a retry-safe execution failure of an idempotent process ⇒
/// [`AckDecision::NackRequeue`] (the broker redelivers and inbox dedup absorbs it), every other
/// reject diagnostic ⇒ [`AckDecision::NackDrop`] (the broker's DLX posture, `requeue=false`).
///
/// This is the one place a broker delivery's refusal stops being a diagnostic, because the source
/// only sees the decision. So it is where the refusal is recorded: a WARN carrying a stable code
/// and the deployment's audit record (see [`DeliveryDropRecord`]). A transport whose `NackDrop`
/// deletes the message leaves nothing else behind.
///
/// Ack-mode TIMING note: the engine actor dispatches run-to-completion, so
/// the returned future resolving IS both `on-persist` (durable intake happened) and
/// `on-complete` (the instance reached a terminal state) for the sync path; the two
/// modes diverge once an instance parks — that refinement rides this same seam.
pub struct EngineIntake {
    handle: EngineHandle,
}

impl EngineIntake {
    pub fn new(handle: EngineHandle) -> EngineIntake {
        EngineIntake { handle }
    }
}

impl InboundIntake for EngineIntake {
    fn deliver(&self, message: InboundMessage) -> BoxFuture<'_, AckDecision> {
        Box::pin(async move {
            let record = DeliveryDropRecord::of(&message);
            match self.handle.dispatch(message).await {
                Ok(outcome) => settled(&record, &outcome),
                Err(diagnostic) => refused(&record, &diagnostic),
            }
        })
    }

    /// `ack-mode: on-complete`: the settle callbacks ride the dispatch request
    /// onto the engine actor. A PARKED instance answers `Deferred` (the registry now owns
    /// the settle — do not ack); a dispatch that finished without parking settles NOW
    /// with the SAME decision mapping as [`Self::deliver`] (the terminal listener events
    /// fired inside the dispatch, so the run-to-completion `on-complete` semantics hold).
    fn deliver_deferred(
        &self,
        message: InboundMessage,
        settle: DeferredSettle,
    ) -> BoxFuture<'_, DeliveryDisposition> {
        Box::pin(async move {
            let record = DeliveryDropRecord::of(&message);
            match self.handle.dispatch_deferred(message, settle).await {
                Ok(DeferredDispatch::Deferred { .. }) => DeliveryDisposition::Deferred,
                Ok(DeferredDispatch::Settled(outcome)) => {
                    DeliveryDisposition::Settle(settled(&record, &outcome))
                }
                Err(diagnostic) => DeliveryDisposition::Settle(refused(&record, &diagnostic)),
            }
        })
    }
}

/// The decision for a dispatch that answered. All of them are the engine's to own. A dead letter
/// has logged its own error and recorded its incident; a duplicate is recorded here.
fn settled(record: &DeliveryDropRecord, outcome: &DispatchOutcome) -> AckDecision {
    if matches!(outcome, DispatchOutcome::Duplicate { .. }) {
        record.report_duplicate();
    }
    AckDecision::Ack
}

/// The decision for a dispatch that refused. A permanent refusal is recorded before the source
/// settles it, since nothing downstream knows why the delivery went.
fn refused(record: &DeliveryDropRecord, diagnostic: &Diagnostic) -> AckDecision {
    if wants_requeue(diagnostic) {
        return AckDecision::NackRequeue;
    }
    record.report(diagnostic);
    AckDecision::NackDrop
}

/// A failure the broker should REDELIVER (`NackRequeue`): either the engine actor is transiently
/// unavailable, or the dispatcher tagged the diagnostic retry-safe (the target process asserted
/// `<q:process idempotent="true">`, so re-execution converges — safe to reprocess).
fn wants_requeue(diagnostic: &Diagnostic) -> bool {
    is_engine_unavailable(diagnostic)
        || diagnostic
            .attributes
            .get(ACK_DISPOSITION_ATTR)
            .map(String::as_str)
            == Some(ACK_DISPOSITION_REQUEUE)
}

/// The engine actor being gone is the one transient infrastructure failure the
/// dispatch surface reports (see `sutra_channels::http::engine_gone`) — everything else
/// under `SUTRA.RUNTIME.UNEXPECTED` is a dispatch crash and stays a permanent reject.
fn is_engine_unavailable(diagnostic: &Diagnostic) -> bool {
    diagnostic.code == "SUTRA.RUNTIME.UNEXPECTED"
        && diagnostic.message == "engine actor is not running"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> DeliveryDropRecord {
        DeliveryDropRecord::of(&InboundMessage {
            tenant: "acme".to_string(),
            module_key: "acme/orders/1.0.0".to_string(),
            channel: "orders-in".to_string(),
            headers: Default::default(),
            body: b"{}".to_vec().into(),
            content_type: None,
            idempotency_key: "k-1".to_string(),
            explicit_event_id: true,
            received_at: "2026-11-05T00:00:00Z".to_string(),
            cloud_event: None,
        })
    }

    #[test]
    fn a_permanent_refusal_drops_and_a_retry_safe_one_requeues() {
        let reject = Diagnostic::error("SUTRA.INBOUND.VALIDATION_REJECT", "no");
        assert_eq!(refused(&record(), &reject), AckDecision::NackDrop);
        let retry = Diagnostic::error("SUTRA.RUNTIME.TASK.UNCAUGHT", "boom")
            .with_attribute(ACK_DISPOSITION_ATTR, ACK_DISPOSITION_REQUEUE);
        assert_eq!(refused(&record(), &retry), AckDecision::NackRequeue);
    }

    #[test]
    fn a_duplicate_is_acked_the_first_delivery_owns_it() {
        let duplicate = DispatchOutcome::Duplicate {
            original_instance_id: None,
            dedup_key: "k-1".to_string(),
        };
        assert_eq!(settled(&record(), &duplicate), AckDecision::Ack);
    }

    #[test]
    fn engine_unavailable_maps_to_requeue_everything_else_to_drop() {
        let gone = Diagnostic::error("SUTRA.RUNTIME.UNEXPECTED", "engine actor is not running");
        assert!(is_engine_unavailable(&gone));
        let crash = Diagnostic::error("SUTRA.RUNTIME.UNEXPECTED", "dispatch panicked: boom");
        assert!(!is_engine_unavailable(&crash));
        let reject = Diagnostic::error("SUTRA.INBOUND.VALIDATION_REJECT", "no");
        assert!(!is_engine_unavailable(&reject));
    }

    #[test]
    fn idempotent_retry_tag_requeues_permanent_reject_drops() {
        // Engine gone → requeue (transient infra).
        let gone = Diagnostic::error("SUTRA.RUNTIME.UNEXPECTED", "engine actor is not running");
        assert!(wants_requeue(&gone));
        // Retry-safe tag (idempotent process's execution failure) → requeue.
        let retry = Diagnostic::error("SUTRA.RUNTIME.TASK.UNCAUGHT", "boom")
            .with_attribute(ACK_DISPOSITION_ATTR, ACK_DISPOSITION_REQUEUE);
        assert!(wants_requeue(&retry));
        // A plain execution failure (non-idempotent path is dead-lettered upstream, never reaches
        // Err here) or a validation reject → NOT requeued (NackDrop).
        let reject = Diagnostic::error("SUTRA.INBOUND.VALIDATION_REJECT", "no");
        assert!(!wants_requeue(&reject));
    }
}
