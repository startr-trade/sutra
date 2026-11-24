//! The engine's journal for audit events a transport writes
//! ([`sutra_channels::transport_audit`]): it applies the audit level of the instance's process
//! (or, for a deployment-level event, of the deployment's processes), builds the
//! [`AuditEvent`] the engine's own stores take, and appends it to the durable audit spool the
//! instance's own events go through — so a transport event lands in the same store, shipped by
//! the same shipper.
//!
//! An append never blocks the caller. A transport event belongs to no step commit, so nothing
//! waits for its sync: it is durable within one sync interval.

use std::collections::HashMap;
use std::sync::Arc;

use sutra_bpmn::model::ProcessAudit;
use sutra_bpmn::qbindings::AuditCapture;
use sutra_channels::{
    transport_audit_payload, AuditEvent, AuditSpoolSender, TransportAuditEvent,
    TransportAuditJournal, TransportAuditOutcome, TransportAuditScope,
};

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
    /// `<q:audit>`), each carrying the destination the deploy resolved for it. A process with no
    /// `<q:audit>` is not audited, and neither is one that captures `none`. The deployment is
    /// audited when any process is; its events go to the first audited process's destination
    /// (empty when the deploy resolved none: the spool counts such an event as undeliverable and
    /// never re-routes it); their fields are kept only when EVERY audited process captures at
    /// payload level — the most conservative reading of the deployment's audit levels.
    pub fn for_processes<'a>(
        deployment_id: &str,
        tenant: &str,
        audits: impl IntoIterator<Item = Option<&'a ProcessAudit>>,
    ) -> DeploymentAuditRoute {
        let mut first_sink: Option<String> = None;
        let mut all_payload = true;
        let mut any = false;
        for audit in audits {
            // A process without `<q:audit>` is not audited, and journals nothing.
            let Some(a) = audit else { continue };
            let (sink, capture) = (a.sink.as_deref().unwrap_or_default(), a.capture);
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
            sink: any.then_some(first_sink).flatten(),
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
    spool: AuditSpoolSender,
    /// Deployment-level routes by module key.
    modules: HashMap<String, DeploymentAuditRoute>,
    /// Tenant by deployment id (an instance event's `tenant`).
    tenants: HashMap<String, String>,
    clock: Box<dyn Fn() -> String + Send + Sync>,
}

impl EngineTransportAuditJournal {
    /// A journal appending to `spool`. `routes` are the live deployments' `(module key,
    /// route)`.
    pub fn new(
        spool: AuditSpoolSender,
        routes: Vec<(String, DeploymentAuditRoute)>,
    ) -> Arc<EngineTransportAuditJournal> {
        let tenants = routes
            .iter()
            .map(|(_, r)| (r.deployment_id.clone(), r.tenant.clone()))
            .collect();
        Arc::new(EngineTransportAuditJournal {
            spool,
            modules: routes.into_iter().collect(),
            tenants,
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
                // The destination the deploy resolved for the instance's process; empty when it
                // resolved none, which the spool counts as undeliverable (never re-routed).
                let sink = Some(context.sink.clone().unwrap_or_default());
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
            diagnostic_json: event.diagnostic_json.clone(),
            payload_json: transport_audit_payload(capture, event),
            // A transport event is about a delivery, not an instance pass: no alias keys.
            correlation: Default::default(),
            attempt: None,
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
        self.spool.append(audit_event);
        TransportAuditOutcome::Queued
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::TransportAuditContext;

    /// A spool in a temporary directory that takes the `sql` and `jsonl` stores' events.
    fn spool() -> (sutra_channels::AuditSpool, std::path::PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("sutra-transport-audit-{}", uuid::Uuid::new_v4()));
        let spool = sutra_channels::AuditSpool::open(sutra_channels::AuditSpoolConfig {
            dir: dir.clone(),
            sync_linger: std::time::Duration::ZERO,
            sinks: ["sql", "jsonl"].map(String::from).into_iter().collect(),
            ..Default::default()
        })
        .unwrap();
        (spool, dir)
    }

    /// What `spool` holds once everything appended so far is synced.
    async fn spooled(spool: &sutra_channels::AuditSpool) -> Vec<AuditEvent> {
        spool.sender().barrier().await.unwrap();
        spool
            .reader(0)
            .next_batch(1000, spool.durable_through())
            .unwrap()
            .into_iter()
            .map(|r| r.event)
            .collect()
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

    #[tokio::test]
    async fn an_instance_event_follows_its_process_audit_level_and_sink() {
        let (spool, dir) = spool();
        let journal = EngineTransportAuditJournal::new(
            spool.sender(),
            vec![route(Some("sql"), AuditCapture::Metadata)],
        );
        assert_eq!(
            journal.append(context(AuditCapture::Payload, Some("jsonl")), event()),
            TransportAuditOutcome::Queued
        );
        assert_eq!(
            journal.append(context(AuditCapture::Metadata, Some("sql")), event()),
            TransportAuditOutcome::Queued
        );
        assert_eq!(
            journal.append(context(AuditCapture::None, None), event()),
            TransportAuditOutcome::Suppressed
        );
        let written = spooled(&spool).await;
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
        // Metadata level: into the process's own destination, no fields.
        assert_eq!(written[1].sink, "sql");
        assert_eq!(written[1].payload_json, "{}");
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_deployment_event_routes_by_module_key_and_is_suppressed_when_unaudited() {
        let (spool, dir) = spool();
        let journal = EngineTransportAuditJournal::new(
            spool.sender(),
            vec![route(Some("sql"), AuditCapture::Metadata)],
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
        // The deploy resolved no destination: queued with an empty sink, which the spool
        // counts as undeliverable and never writes or re-routes.
        assert_eq!(
            journal.append(context(AuditCapture::Metadata, None), event()),
            TransportAuditOutcome::Queued
        );
        assert!(matches!(
            journal.append(
                deployment("acme/calls/1.0.0"),
                TransportAuditEvent::new("media", "not-an-event")
            ),
            TransportAuditOutcome::Refused(_)
        ));
        let written = spooled(&spool).await;
        assert_eq!(written.len(), 1, "the unresolved event is never spooled");
        assert_eq!(written[0].instance_id, "");
        assert_eq!(written[0].deployment_id, "dep-000000000000000000000001");
        assert_eq!(written[0].payload_json, "{}");

        let unaudited = EngineTransportAuditJournal::new(
            spool.sender(),
            vec![route(None, AuditCapture::Metadata)],
        );
        assert_eq!(
            unaudited.append(deployment("acme/calls/1.0.0"), event()),
            TransportAuditOutcome::Suppressed
        );
        assert_eq!(spooled(&spool).await.len(), 1, "nothing more was appended");
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// An append never blocks: it queues for the spool's next group sync, with no barrier.
    #[tokio::test]
    async fn an_append_queues_for_the_spool_without_a_barrier() {
        let (spool, dir) = spool();
        let journal = EngineTransportAuditJournal::new(spool.sender(), Vec::new());
        for _ in 0..50 {
            assert_eq!(
                journal.append(context(AuditCapture::Metadata, Some("sql")), event()),
                TransportAuditOutcome::Queued
            );
        }
        assert_eq!(spooled(&spool).await.len(), 50);
        drop(spool);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_deployment_route_reads_its_processes_audit_levels() {
        let audit = |sink: &str, capture| ProcessAudit {
            events: sutra_bpmn::qbindings::AuditEventScope::All,
            capture,
            sink: Some(sink.into()),
        };
        let payload = audit("jsonl", AuditCapture::Payload);
        let metadata = audit("sql", AuditCapture::Metadata);
        let none = audit("sql", AuditCapture::None);

        let all_payload = DeploymentAuditRoute::for_processes("dep-1", "t", [Some(&payload)]);
        assert_eq!(all_payload.sink.as_deref(), Some("jsonl"));
        assert_eq!(all_payload.capture, AuditCapture::Payload);

        let mixed =
            DeploymentAuditRoute::for_processes("dep-1", "t", [Some(&metadata), Some(&payload)]);
        assert_eq!(mixed.sink.as_deref(), Some("sql"));
        assert_eq!(mixed.capture, AuditCapture::Metadata);

        // A process with no <q:audit> is not audited, so it journals nothing.
        let undeclared = DeploymentAuditRoute::for_processes("dep-1", "t", [None]);
        assert_eq!(undeclared.sink, None);

        // Nothing audited: no sink at all.
        assert_eq!(
            DeploymentAuditRoute::for_processes("dep-1", "t", [None]).sink,
            None
        );
        assert_eq!(
            DeploymentAuditRoute::for_processes("dep-1", "t", [Some(&none)]).sink,
            None
        );
    }
}
