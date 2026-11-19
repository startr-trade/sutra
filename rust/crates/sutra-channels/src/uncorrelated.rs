//! What the engine records about a message a wait subscribes to that correlates to no live
//! instance — a reply after its instance ended, a reply for a key nothing ever carried.
//!
//! Such a message is refused (`SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND`), or, on a channel
//! that declares `uncorrelated: { start: <processId> }`, starts that process. Either way the
//! sender's answer is not the only trace: the engine logs a WARN carrying
//! [`codes::INBOUND_RELAY_UNCORRELATED`] and appends `inbound.RELAY_UNCORRELATED` to the
//! deployment's audit journal, whichever transport carried the message.
//!
//! Neither carries the body or the headers. The correlation keys the engine looked for are
//! shown as every observability surface shows a correlation key: masked when the key, or a
//! variable its expression reads, is `sensitive` or redactor-controlled.

use crate::codes;
use crate::dispatch::InboundMessage;
use crate::transport_audit::{
    transport_audit, TransportAuditEvent, TransportAuditOutcome, TransportAuditRegistry,
    TransportAuditScope,
};

/// The audit namespace of the record; with [`UNCORRELATED_EVENT`] the event type is
/// `inbound.RELAY_UNCORRELATED`.
const UNCORRELATED_NAMESPACE: &str = "inbound";
/// See [`UNCORRELATED_NAMESPACE`].
const UNCORRELATED_EVENT: &str = "RELAY_UNCORRELATED";

/// One correlation key the relay path computed and found no live instance for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriedCorrelationKey {
    /// The process whose wait the message was offered to.
    pub process_id: String,
    /// The wait node the key was computed for.
    pub wait_node_id: String,
    /// The `<q:alias>` name.
    pub alias_name: String,
    /// The raw value — for the started instance's `event.uncorrelated`, never for a log or an
    /// audit record.
    pub alias_value: String,
    /// The value as observability surfaces show it (masked when protected).
    pub shown_value: String,
    /// The deployment scope it was looked up in.
    pub deployment_id: String,
    /// The key was carried only by instances that have ended.
    pub ended: bool,
}

/// What became of the uncorrelated message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UncorrelatedOutcome {
    /// Refused with the relay's diagnostic.
    Refused,
    /// Started the channel's `uncorrelated: start` process.
    Started {
        process_id: String,
        instance_id: String,
    },
    /// The `uncorrelated: start` process was tried and refused the message (its intake, a
    /// capacity limit, …); the code is that refusal's.
    StartFailed { process_id: String, code: String },
}

/// The record of one uncorrelated message: its identity and the keys tried. Built before the
/// outcome is known; [`Self::report`] writes it once the outcome is.
#[derive(Debug, Clone)]
pub struct UncorrelatedRelayRecord {
    pub tenant: String,
    pub module_key: String,
    pub channel: String,
    pub deployment_id: String,
    pub message_type: Option<String>,
    pub dedup_key: String,
    pub cause_code: String,
    pub tried: Vec<TriedCorrelationKey>,
}

impl UncorrelatedRelayRecord {
    pub fn of(
        message: &InboundMessage,
        deployment_id: &str,
        message_type: Option<&str>,
        cause_code: &str,
        tried: Vec<TriedCorrelationKey>,
    ) -> UncorrelatedRelayRecord {
        UncorrelatedRelayRecord {
            tenant: message.tenant.clone(),
            module_key: message.module_key.clone(),
            channel: message.channel.clone(),
            deployment_id: deployment_id.to_string(),
            message_type: message_type.map(str::to_string),
            dedup_key: message.idempotency_key.clone(),
            cause_code: cause_code.to_string(),
            tried,
        }
    }

    /// Whether every key tried was carried only by instances that have ended.
    pub fn all_ended(&self) -> bool {
        !self.tried.is_empty() && self.tried.iter().all(|k| k.ended)
    }

    /// The keys as one log field: `alias=value` (shown values), comma-separated.
    fn keys_field(&self) -> String {
        self.tried
            .iter()
            .map(|k| format!("{}={}", k.alias_name, k.shown_value))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The WARN line and the deployment's audit record.
    pub fn report(&self, outcome: &UncorrelatedOutcome) -> TransportAuditOutcome {
        let (what, started_process, instance_id) = describe(outcome);
        tracing::warn!(
            code = codes::INBOUND_RELAY_UNCORRELATED,
            cause_code = %self.cause_code,
            channel = %self.channel,
            module = %self.module_key,
            tenant = %self.tenant,
            deployment.id = %self.deployment_id,
            message_type = self.message_type.as_deref().unwrap_or(""),
            dedup_key = %self.dedup_key,
            keys = %self.keys_field(),
            ended = self.all_ended(),
            outcome = what,
            started_process = started_process.unwrap_or(""),
            instance.id = instance_id.unwrap_or(""),
            "inbound message correlates to no live instance"
        );
        self.audit_into(transport_audit(), outcome)
    }

    /// The audit record alone, into `registry`.
    pub fn audit_into(
        &self,
        registry: &TransportAuditRegistry,
        outcome: &UncorrelatedOutcome,
    ) -> TransportAuditOutcome {
        let (what, started_process, instance_id) = describe(outcome);
        let keys: Vec<serde_json::Value> = self
            .tried
            .iter()
            .map(|k| {
                serde_json::json!({
                    "process": k.process_id,
                    "node": k.wait_node_id,
                    "alias": k.alias_name,
                    "value": k.shown_value,
                    "deployment": k.deployment_id,
                    "ended": k.ended,
                })
            })
            .collect();
        let mut detail = serde_json::json!({
            "channel": self.channel,
            "deployment": self.deployment_id,
            "messageType": self.message_type,
            "dedupKey": self.dedup_key,
            "keys": keys,
            "ended": self.all_ended(),
            "outcome": what,
        });
        if let Some(process) = started_process {
            detail["startedProcess"] = serde_json::Value::String(process.to_string());
        }
        if let Some(instance) = instance_id {
            detail["instanceId"] = serde_json::Value::String(instance.to_string());
        }
        if let UncorrelatedOutcome::StartFailed { code, .. } = outcome {
            detail["startCode"] = serde_json::Value::String(code.clone());
        }
        registry.append(
            TransportAuditScope::Deployment {
                module_key: self.module_key.clone(),
            },
            TransportAuditEvent::new(UNCORRELATED_NAMESPACE, UNCORRELATED_EVENT)
                .with_diagnostic_code(&self.cause_code)
                .with_diagnostic_json(&detail),
        )
    }
}

fn describe(outcome: &UncorrelatedOutcome) -> (&'static str, Option<&str>, Option<&str>) {
    match outcome {
        UncorrelatedOutcome::Refused => ("refused", None, None),
        UncorrelatedOutcome::Started {
            process_id,
            instance_id,
        } => (
            "started",
            Some(process_id.as_str()),
            Some(instance_id.as_str()).filter(|id| !id.is_empty()),
        ),
        UncorrelatedOutcome::StartFailed { process_id, .. } => {
            ("start-failed", Some(process_id.as_str()), None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::transport_audit::CapturingTransportAuditJournal;

    fn record() -> UncorrelatedRelayRecord {
        let message = InboundMessage {
            tenant: "acme".to_string(),
            module_key: "acme/orders/1.0.0".to_string(),
            channel: "answers".to_string(),
            headers: BTreeMap::new(),
            body: b"{\"secret\":\"body\"}".to_vec().into(),
            content_type: Some("application/json".to_string()),
            idempotency_key: "evt-1".to_string(),
            explicit_event_id: true,
            received_at: "2026-11-19T10:00:00Z".to_string(),
            cloud_event: None,
        };
        UncorrelatedRelayRecord::of(
            &message,
            "dep-1",
            Some("Answer"),
            codes::RUNTIME_RELAY_CORRELATION_NOT_FOUND,
            vec![TriedCorrelationKey {
                process_id: "order".to_string(),
                wait_node_id: "Answered".to_string(),
                alias_name: "order".to_string(),
                alias_value: "K-1".to_string(),
                shown_value: "K-1".to_string(),
                deployment_id: "dep-1".to_string(),
                ended: true,
            }],
        )
    }

    #[test]
    fn the_record_is_a_deployment_event_with_the_keys_and_no_body() {
        let registry = TransportAuditRegistry::new();
        let journal = Arc::new(CapturingTransportAuditJournal::new());
        registry.install(journal.clone());
        let outcome = record().audit_into(&registry, &UncorrelatedOutcome::Refused);
        assert_eq!(outcome, TransportAuditOutcome::Queued);
        let events = journal.events();
        assert_eq!(events.len(), 1);
        let (scope, event) = &events[0];
        assert_eq!(
            scope,
            &TransportAuditScope::Deployment {
                module_key: "acme/orders/1.0.0".to_string()
            }
        );
        assert_eq!(
            event.event_type().as_deref(),
            Ok("inbound.RELAY_UNCORRELATED")
        );
        assert_eq!(
            event.diagnostic_code.as_deref(),
            Some("SUTRA.RUNTIME.RELAY.CORRELATION_NOT_FOUND")
        );
        let detail: serde_json::Value =
            serde_json::from_str(event.diagnostic_json.as_deref().unwrap()).unwrap();
        assert_eq!(detail["channel"], "answers");
        assert_eq!(detail["messageType"], "Answer");
        assert_eq!(detail["deployment"], "dep-1");
        assert_eq!(detail["keys"][0]["alias"], "order");
        assert_eq!(detail["keys"][0]["value"], "K-1");
        assert_eq!(detail["ended"], true);
        assert_eq!(detail["outcome"], "refused");
        assert!(event.fields.is_empty(), "no payload fields");
        assert!(!event.diagnostic_json.as_deref().unwrap().contains("secret"));
    }

    #[test]
    fn a_routed_start_names_the_process_and_the_instance() {
        let registry = TransportAuditRegistry::new();
        let journal = Arc::new(CapturingTransportAuditJournal::new());
        registry.install(journal.clone());
        record().audit_into(
            &registry,
            &UncorrelatedOutcome::Started {
                process_id: "late".to_string(),
                instance_id: "i-1".to_string(),
            },
        );
        let detail: serde_json::Value =
            serde_json::from_str(journal.events()[0].1.diagnostic_json.as_deref().unwrap())
                .unwrap();
        assert_eq!(detail["outcome"], "started");
        assert_eq!(detail["startedProcess"], "late");
        assert_eq!(detail["instanceId"], "i-1");
    }

    #[test]
    fn without_an_installed_journal_the_record_is_not_written() {
        let registry = TransportAuditRegistry::new();
        assert_eq!(
            record().audit_into(&registry, &UncorrelatedOutcome::Refused),
            TransportAuditOutcome::NotInstalled
        );
    }
}
