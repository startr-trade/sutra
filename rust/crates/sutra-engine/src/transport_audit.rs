//! The engine's journal for audit events a transport writes
//! ([`sutra_channels::transport_audit`]): it applies the audit level of the instance's process
//! (or, for a deployment-level event, of the deployment's processes), builds the
//! [`AuditEvent`] the engine's own sinks take, and hands it to a bounded queue a background
//! writer drains into the same sinks `<q:audit>` writes through — so a transport event lands in
//! the `audit_event` table (or the JSONL file, or the audit log export) exactly where the
//! instance's own events do.
//!
//! An append never blocks the caller: a full queue drops the event, and the registry counts the
//! drop (`sutra.audit.transport.dropped`). The writer is best-effort like every audit sink: a
//! write failure is logged, never propagated.

use std::collections::HashMap;
use std::sync::Arc;

use sutra_bpmn::model::ProcessAudit;
use sutra_bpmn::qbindings::AuditCapture;
use sutra_channels::{
    deliver_audit_event, transport_audit_payload, AuditEvent, AuditSinkRegistry,
    TransportAuditEvent, TransportAuditJournal, TransportAuditOutcome, TransportAuditScope,
};
use tokio::sync::mpsc;

/// How many transport events may wait for the writer before an append drops one. A write is
/// one INSERT; the bound only matters when the audit store stalls, and then it caps the memory
/// held (each event is a few hundred bytes) instead of growing without limit.
pub const TRANSPORT_AUDIT_QUEUE_CAPACITY: usize = 4096;

/// Where a deployment's deployment-level transport events go, and at what level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentAuditRoute {
    pub deployment_id: String,
    pub tenant: String,
    /// The sink; `None` when no process of the deployment is audited (events are suppressed).
    pub sink: Option<String>,
    pub capture: AuditCapture,
}

impl DeploymentAuditRoute {
    /// The route of a deployment whose processes declare `audits` (`None` for a process with no
    /// `<q:audit>`), given the engine's default sink. A process with no `<q:audit>` is audited at
    /// metadata level into the default sink, as the engine audits its own events; one that
    /// captures `none` is not audited. The deployment is audited when any process is; its events
    /// go to the default sink (or, without one, the first audited process's sink); their fields
    /// are kept only when EVERY audited process captures at payload level — the most
    /// conservative reading of the deployment's audit levels.
    pub fn for_processes<'a>(
        deployment_id: &str,
        tenant: &str,
        audits: impl IntoIterator<Item = Option<&'a ProcessAudit>>,
        default_sink: Option<&str>,
    ) -> DeploymentAuditRoute {
        let mut first_sink: Option<String> = None;
        let mut all_payload = true;
        let mut any = false;
        for audit in audits {
            let (sink, capture) = match audit {
                Some(a) => (Some(a.sink.as_str()), a.capture),
                None => (default_sink, AuditCapture::Metadata),
            };
            let Some(sink) = sink else { continue };
            if capture == AuditCapture::None {
                continue;
            }
            any = true;
            all_payload &= capture == AuditCapture::Payload;
            first_sink.get_or_insert_with(|| sink.to_string());
        }
        DeploymentAuditRoute {
            deployment_id: deployment_id.to_string(),
            tenant: tenant.to_string(),
            sink: any
                .then(|| default_sink.map(str::to_string).or(first_sink))
                .flatten(),
            capture: if any && all_payload {
                AuditCapture::Payload
            } else {
                AuditCapture::Metadata
            },
        }
    }
}

/// The engine's [`TransportAuditJournal`].
pub struct EngineTransportAuditJournal {
    queue: mpsc::Sender<AuditEvent>,
    /// Deployment-level routes by module key.
    modules: HashMap<String, DeploymentAuditRoute>,
    /// Tenant by deployment id (an instance event's `tenant`).
    tenants: HashMap<String, String>,
    default_sink: Option<String>,
    clock: Box<dyn Fn() -> String + Send + Sync>,
}

impl EngineTransportAuditJournal {
    /// A journal over `sinks`, with its writer spawned on `handle`. `routes` are the live
    /// deployments' `(module key, route)`; `default_sink` is the sink a process with no
    /// `<q:audit sink>` writes to. `capacity` bounds the queue
    /// ([`TRANSPORT_AUDIT_QUEUE_CAPACITY`] in production).
    pub fn spawn(
        handle: &tokio::runtime::Handle,
        sinks: AuditSinkRegistry,
        default_sink: Option<String>,
        routes: Vec<(String, DeploymentAuditRoute)>,
        capacity: usize,
    ) -> Arc<EngineTransportAuditJournal> {
        let (queue, mut rx) = mpsc::channel::<AuditEvent>(capacity.max(1));
        handle.spawn(async move {
            while let Some(event) = rx.recv().await {
                deliver_audit_event(&sinks, &event).await;
            }
        });
        let tenants = routes
            .iter()
            .map(|(_, r)| (r.deployment_id.clone(), r.tenant.clone()))
            .collect();
        Arc::new(EngineTransportAuditJournal {
            queue,
            modules: routes.into_iter().collect(),
            tenants,
            default_sink,
            clock: Box::new(|| {
                time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default()
            }),
        })
    }

    /// The audit event `event` becomes under `scope`, or why it is not written.
    fn resolve(
        &self,
        scope: TransportAuditScope,
        event: &TransportAuditEvent,
    ) -> Result<AuditEvent, TransportAuditOutcome> {
        let event_type = event.event_type().map_err(TransportAuditOutcome::Refused)?;
        let (sink, capture, deployment_id, instance_id, tenant) = match scope {
            TransportAuditScope::Instance(context) => {
                let sink = context.sink.clone().or_else(|| self.default_sink.clone());
                let tenant = self
                    .tenants
                    .get(&context.deployment_id)
                    .cloned()
                    .unwrap_or_default();
                (
                    sink,
                    context.capture,
                    context.deployment_id,
                    context.instance_id,
                    tenant,
                )
            }
            TransportAuditScope::Deployment { module_key } => {
                let Some(route) = self.modules.get(&module_key) else {
                    return Err(TransportAuditOutcome::Suppressed);
                };
                (
                    route.sink.clone(),
                    route.capture,
                    route.deployment_id.clone(),
                    String::new(),
                    route.tenant.clone(),
                )
            }
        };
        let Some(sink) = sink else {
            return Err(TransportAuditOutcome::Suppressed);
        };
        if capture == AuditCapture::None {
            return Err(TransportAuditOutcome::Suppressed);
        }
        Ok(AuditEvent {
            sink,
            deployment_id,
            tenant,
            instance_id,
            // The sink numbers a transport event itself (the instance's high-water seq in the
            // SQL journal); it never takes one from the engine's sequence.
            seq: 0,
            at: (self.clock)(),
            event_type,
            node_id: event.node_id.clone(),
            diagnostic_code: event.diagnostic_code.clone(),
            diagnostic_json: None,
            payload_json: transport_audit_payload(capture, event),
        })
    }
}

impl TransportAuditJournal for EngineTransportAuditJournal {
    fn append(
        &self,
        scope: TransportAuditScope,
        event: TransportAuditEvent,
    ) -> TransportAuditOutcome {
        let audit_event = match self.resolve(scope, &event) {
            Ok(audit_event) => audit_event,
            Err(outcome) => return outcome,
        };
        match self.queue.try_send(audit_event) {
            Ok(()) => TransportAuditOutcome::Queued,
            Err(mpsc::error::TrySendError::Full(dropped)) => {
                tracing::warn!(
                    event_type = %dropped.event_type,
                    deployment = %dropped.deployment_id,
                    "a transport audit event was dropped: the audit queue is full"
                );
                TransportAuditOutcome::Dropped
            }
            // The writer is gone (the engine is shutting down): nothing can be written.
            Err(mpsc::error::TrySendError::Closed(_)) => TransportAuditOutcome::Dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use sutra_channels::{AuditSink, BoxFuture, Diagnostic, TransportAuditContext};

    /// A sink that keeps what it is given; `gate` holds every write until it is released.
    struct KeepingSink {
        name: &'static str,
        kept: Arc<Mutex<Vec<AuditEvent>>>,
        gate: Arc<tokio::sync::Semaphore>,
    }

    impl AuditSink for KeepingSink {
        fn name(&self) -> &str {
            self.name
        }
        fn emit<'a>(&'a self, event: &'a AuditEvent) -> BoxFuture<'a, Result<(), Diagnostic>> {
            Box::pin(async move {
                let _permit = self.gate.acquire().await.unwrap();
                self.kept.lock().unwrap().push(event.clone());
                Ok(())
            })
        }
    }

    fn sinks(
        open: bool,
    ) -> (
        AuditSinkRegistry,
        Arc<Mutex<Vec<AuditEvent>>>,
        Arc<tokio::sync::Semaphore>,
    ) {
        let kept = Arc::new(Mutex::new(Vec::new()));
        let gate = Arc::new(tokio::sync::Semaphore::new(if open { 1000 } else { 0 }));
        let mut registry = AuditSinkRegistry::new();
        for name in ["sql", "jsonl"] {
            registry.register(Arc::new(KeepingSink {
                name,
                kept: Arc::clone(&kept),
                gate: Arc::clone(&gate),
            }));
        }
        (registry, kept, gate)
    }

    fn route(sink: Option<&str>, capture: AuditCapture) -> (String, DeploymentAuditRoute) {
        (
            "acme/calls/1.0.0".to_string(),
            DeploymentAuditRoute {
                deployment_id: "dep-000000000000000000000001".into(),
                tenant: "acme".into(),
                sink: sink.map(str::to_string),
                capture,
            },
        )
    }

    fn context(capture: AuditCapture, sink: Option<&str>) -> TransportAuditScope {
        TransportAuditScope::Instance(TransportAuditContext {
            deployment_id: "dep-000000000000000000000001".into(),
            instance_id: "11111111-1111-1111-1111-111111111111".into(),
            capture,
            sink: sink.map(str::to_string),
        })
    }

    fn event() -> TransportAuditEvent {
        TransportAuditEvent::new("media", "REQUEST_REFUSED")
            .with_field("rule", "rate-limit")
            .with_sensitive_field("destination", "+15550100")
            .with_diagnostic_code("MEDIA.REFUSED")
    }

    async fn settle(kept: &Arc<Mutex<Vec<AuditEvent>>>, count: usize) -> Vec<AuditEvent> {
        for _ in 0..200 {
            if kept.lock().unwrap().len() >= count {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        kept.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn an_instance_event_follows_its_process_audit_level_and_sink() {
        let (registry, kept, _gate) = sinks(true);
        let journal = EngineTransportAuditJournal::spawn(
            &tokio::runtime::Handle::current(),
            registry,
            Some("sql".into()),
            vec![route(Some("sql"), AuditCapture::Metadata)],
            16,
        );
        assert_eq!(
            journal.append(context(AuditCapture::Payload, Some("jsonl")), event()),
            TransportAuditOutcome::Queued
        );
        assert_eq!(
            journal.append(context(AuditCapture::Metadata, None), event()),
            TransportAuditOutcome::Queued
        );
        assert_eq!(
            journal.append(context(AuditCapture::None, None), event()),
            TransportAuditOutcome::Suppressed
        );
        let written = settle(&kept, 2).await;
        assert_eq!(written.len(), 2);
        let payload = &written[0];
        assert_eq!(payload.sink, "jsonl");
        assert_eq!(payload.event_type, "media.REQUEST_REFUSED");
        assert_eq!(payload.instance_id, "11111111-1111-1111-1111-111111111111");
        assert_eq!(payload.tenant, "acme");
        assert_eq!(payload.diagnostic_code.as_deref(), Some("MEDIA.REFUSED"));
        let fields: serde_json::Value = serde_json::from_str(&payload.payload_json).unwrap();
        assert_eq!(fields["rule"], "rate-limit");
        assert_eq!(fields["destination"], sutra_bpmn::REDACTED_PLACEHOLDER);
        // Metadata level: into the default sink, no fields.
        assert_eq!(written[1].sink, "sql");
        assert_eq!(written[1].payload_json, "{}");
    }

    #[tokio::test]
    async fn a_deployment_event_routes_by_module_key_and_is_suppressed_when_unaudited() {
        let (registry, kept, _gate) = sinks(true);
        let journal = EngineTransportAuditJournal::spawn(
            &tokio::runtime::Handle::current(),
            registry,
            None,
            vec![route(Some("sql"), AuditCapture::Metadata)],
            16,
        );
        let deployment = |key: &str| TransportAuditScope::Deployment {
            module_key: key.to_string(),
        };
        assert_eq!(
            journal.append(deployment("acme/calls/1.0.0"), event()),
            TransportAuditOutcome::Queued
        );
        assert_eq!(
            journal.append(deployment("acme/other/1.0.0"), event()),
            TransportAuditOutcome::Suppressed
        );
        // No default sink and the process names none: nothing to write to.
        assert_eq!(
            journal.append(context(AuditCapture::Metadata, None), event()),
            TransportAuditOutcome::Suppressed
        );
        assert!(matches!(
            journal.append(
                deployment("acme/calls/1.0.0"),
                TransportAuditEvent::new("media", "not-an-event")
            ),
            TransportAuditOutcome::Refused(_)
        ));
        let written = settle(&kept, 1).await;
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].instance_id, "");
        assert_eq!(written[0].deployment_id, "dep-000000000000000000000001");
        assert_eq!(written[0].payload_json, "{}");

        let (registry, kept, _gate) = sinks(true);
        let unaudited = EngineTransportAuditJournal::spawn(
            &tokio::runtime::Handle::current(),
            registry,
            None,
            vec![route(None, AuditCapture::Metadata)],
            16,
        );
        assert_eq!(
            unaudited.append(deployment("acme/calls/1.0.0"), event()),
            TransportAuditOutcome::Suppressed
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(kept.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_full_queue_drops_without_blocking_and_the_registry_counts_it() {
        // The sink is held shut, so the writer takes one event and blocks on it; the queue of
        // two then fills.
        let (registry, kept, gate) = sinks(false);
        let journal = EngineTransportAuditJournal::spawn(
            &tokio::runtime::Handle::current(),
            registry,
            Some("sql".into()),
            Vec::new(),
            2,
        );
        let counting = sutra_channels::TransportAuditRegistry::new();
        counting.install(journal);
        let mut outcomes = Vec::new();
        for _ in 0..6 {
            outcomes.push(counting.append(context(AuditCapture::Metadata, None), event()));
            tokio::task::yield_now().await;
        }
        assert!(outcomes.contains(&TransportAuditOutcome::Dropped));
        assert_eq!(
            counting.dropped_total(),
            outcomes
                .iter()
                .filter(|o| **o == TransportAuditOutcome::Dropped)
                .count() as u64
        );
        let queued = outcomes
            .iter()
            .filter(|o| **o == TransportAuditOutcome::Queued)
            .count();
        assert!((2..=3).contains(&queued), "{outcomes:?}");
        gate.add_permits(100);
        assert_eq!(settle(&kept, queued).await.len(), queued);
    }

    #[test]
    fn a_deployment_route_reads_its_processes_audit_levels() {
        let audit = |sink: &str, capture| ProcessAudit {
            sink: sink.into(),
            capture,
        };
        let payload = audit("jsonl", AuditCapture::Payload);
        let metadata = audit("sql", AuditCapture::Metadata);
        let none = audit("sql", AuditCapture::None);

        let all_payload = DeploymentAuditRoute::for_processes("dep-1", "t", [Some(&payload)], None);
        assert_eq!(all_payload.sink.as_deref(), Some("jsonl"));
        assert_eq!(all_payload.capture, AuditCapture::Payload);

        let mixed = DeploymentAuditRoute::for_processes(
            "dep-1",
            "t",
            [Some(&payload), Some(&metadata)],
            Some("sql"),
        );
        assert_eq!(mixed.sink.as_deref(), Some("sql"));
        assert_eq!(mixed.capture, AuditCapture::Metadata);

        // A process with no <q:audit> is audited at metadata level into the default sink.
        let undeclared = DeploymentAuditRoute::for_processes("dep-1", "t", [None], Some("sql"));
        assert_eq!(undeclared.sink.as_deref(), Some("sql"));
        assert_eq!(undeclared.capture, AuditCapture::Metadata);

        // Nothing audited: no sink at all.
        assert_eq!(
            DeploymentAuditRoute::for_processes("dep-1", "t", [None], None).sink,
            None
        );
        assert_eq!(
            DeploymentAuditRoute::for_processes("dep-1", "t", [Some(&none)], Some("sql")).sink,
            None
        );
    }
}
