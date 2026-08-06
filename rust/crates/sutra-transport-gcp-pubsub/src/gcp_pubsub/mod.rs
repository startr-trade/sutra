//! Google Cloud Pub/Sub transport — the broker pair behind the transport seams:
//! [`source::GcpPubSubTriggerSource`] implements [`sutra_channels::source::TriggerSource`] (inbound
//! subscription consumer, leader-gated for `singleton: true` channels) and
//! [`sink::GcpPubSubMessageSink`] implements [`sutra_channels::sink::MessageSink`] for the
//! `gcp-pubsub://` destination scheme.
//!
//! Wire strings are conformance invariants — the cross-broker attribute shape IS the
//! requirement; the carriers use the `sutra-*` vocabulary while the CE spec prefix stays:
//!
//! - the outbox key rides the Pub/Sub message **attribute** `sutra-outbox-key` (the dedup /
//!   consumer-idempotency token the non-RabbitMQ brokers share), and is lifted back to the
//!   inbound idempotency key on the subscriber side;
//! - the reply `content-type` rides the **`content-type`** attribute, defaulting to
//!   `application/octet-stream`;
//! - the CloudEvents binary binding uses the **`ce-`** attribute prefix (DASH — Pub/Sub
//!   attribute keys allow the HTTP dash form, so those brokers share it); the CE
//!   projection happens upstream (dispatcher-side, [`sutra_channels::outbox_dispatch`]
//!   `CeBinding::GcpPubsub`), this sink carries the resulting attributes verbatim;
//! - the fallback idempotency key (no `sutra-outbox-key` attribute) is the broker-assigned
//!   `message_id` (NON-explicit — it never suppresses a re-post through inbox dedup).
//!
//! Authentication (real-GCP service-account credentials / per-channel `credentials-ref`) is
//! deferred — this build uses the emulator (`PUBSUB_EMULATOR_HOST`) or Application
//! Default Credentials, matching the way the Kafka transport authenticates at the broker
//! layer with no channel-YAML secrets.

pub mod sink;
pub mod source;

pub use sink::GcpPubSubMessageSink;
pub use source::{GcpPubSubSourceConfig, GcpPubSubTriggerSource};

use std::collections::BTreeMap;

use google_cloud_gax::conn::Environment;
use google_cloud_pubsub::client::ClientConfig;

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact GCP Pub/Sub diagnostic codes this module
/// raises.
pub mod codes {
    pub const INBOUND_CONNECTION_FAILED: &str = "SUTRA.INBOUND.GCP_PUBSUB.CONNECTION_FAILED";
    pub const INBOUND_SUBSCRIPTION_MISSING: &str = "SUTRA.INBOUND.GCP_PUBSUB.SUBSCRIPTION_MISSING";
    pub const INBOUND_RECEIVE_FAILED: &str = "SUTRA.INBOUND.GCP_PUBSUB.RECEIVE_FAILED";
    pub const INBOUND_CONFIG_INVALID: &str = "SUTRA.INBOUND.GCP_PUBSUB.CONFIG_INVALID";
    /// Per-message inbound auth: the credential did not match the expected key.
    pub const INBOUND_AUTH_REJECTED: &str = "SUTRA.INBOUND.GCP_PUBSUB.AUTH_REJECTED";
    /// Per-channel mTLS is unsupported (broker/transport-level TLS applies instead).
    pub const INBOUND_MTLS_UNSUPPORTED: &str = "SUTRA.INBOUND.GCP_PUBSUB.MTLS_UNSUPPORTED";

    pub const OUTBOUND_PUBLISH_FAILED: &str = "SUTRA.OUTBOUND.GCP_PUBSUB.PUBLISH_FAILED";
    pub const OUTBOUND_TOPIC_MISSING: &str = "SUTRA.OUTBOUND.GCP_PUBSUB.TOPIC_MISSING";
    pub const OUTBOUND_CONFIG_INVALID: &str = "SUTRA.OUTBOUND.GCP_PUBSUB.CONFIG_INVALID";

    /// `SUTRA.OUTBOUND.SEND.FAILED` — malformed destination scheme (permanent posture,
    /// shared with the other brokers' destination parsers).
    pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.SEND.FAILED";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "gcp-pubsub";

/// The message attribute carrying the outbox / consumer-idempotency key — the cross-broker
/// string the non-RabbitMQ brokers share (a frozen wire name).
pub const ATTR_OUTBOX_KEY: &str = "sutra-outbox-key";
/// The message attribute naming a reply destination (a frozen wire name).
pub const ATTR_REPLY_TO: &str = "sutra-reply-to";
/// The message attribute carrying the payload content type.
pub const ATTR_CONTENT_TYPE: &str = "content-type";

/// The informational header carrying the broker-assigned message id (a frozen wire name).
pub const HEADER_MESSAGE_ID: &str = "x-gcp-pubsub-message-id";
/// The informational header carrying a message ordering key when present (a frozen wire
/// name).
pub const HEADER_ORDERING_KEY: &str = "x-gcp-pubsub-ordering-key";

/// Effective ack modes of a broker channel — parsed leniently:
/// `on-complete` (ASCII case-insensitive) opts in, anything else is `on-persist`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Ack the message once the intake made the delivery durable (default).
    OnPersist,
    /// Ack only at instance COMPLETED / drop (ack) at FAILED.
    OnComplete,
}

impl AckMode {
    fn parse(raw: Option<&str>) -> AckMode {
        match raw {
            Some(v) if v.trim().eq_ignore_ascii_case("on-complete") => AckMode::OnComplete,
            _ => AckMode::OnPersist,
        }
    }
}

/// Typed view over the transport-specific channel properties. Pub/Sub authenticates
/// via Application Default Credentials / the emulator (this build), so there is no
/// channel-YAML credential and no secret-reference discipline here (the per-channel
/// `credentials-ref` is deferred — omitted deliberately).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcpPubSubChannelProperties {
    /// `project-id` (required) — the GCP project owning the topic/subscription.
    pub project_id: String,
    /// `subscription` (required inbound) — the short subscription name; the canonical path
    /// is `projects/<project-id>/subscriptions/<subscription>`.
    pub subscription: String,
    /// `topic` (optional inbound; the sink composes it from the URI) — short topic name.
    pub topic: String,
    /// `flow-control.max-outstanding-messages` (default 1000; must be >= 1).
    pub max_outstanding_messages: i64,
    /// `flow-control.max-outstanding-request-bytes` (default 100 MiB; must be >= 1).
    pub max_outstanding_request_bytes: i64,
    /// `endpoint-override` (optional) — SDK endpoint `host:port` override; the emulator ITs
    /// set it so the client talks plaintext to the container (equivalent to
    /// `PUBSUB_EMULATOR_HOST`).
    pub endpoint_override: Option<String>,
    /// Engine-level ack semantics (`ack-mode`, default `on-persist`).
    pub ack_mode: AckMode,
    /// Per-channel singleton declaration (`singleton: true` / `consumer: exclusive`).
    pub singleton: bool,
}

impl GcpPubSubChannelProperties {
    pub const DEFAULT_MAX_OUTSTANDING_MESSAGES: i64 = 1_000;
    pub const DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES: i64 = 100 * 1024 * 1024;

    pub const KEY_PROJECT_ID: &'static str = "project-id";
    pub const KEY_SUBSCRIPTION: &'static str = "subscription";
    pub const KEY_TOPIC: &'static str = "topic";
    pub const KEY_ENDPOINT_OVERRIDE: &'static str = "endpoint-override";
    pub const KEY_MAX_OUTSTANDING_MESSAGES: &'static str = "flow-control.max-outstanding-messages";
    pub const KEY_MAX_OUTSTANDING_REQUEST_BYTES: &'static str =
        "flow-control.max-outstanding-request-bytes";

    /// Read the typed properties off a channel definition. Mirrors
    /// `GcpPubSubChannelProperties.from(ChannelConfig)`: `project-id` required
    /// (`SUTRA.INBOUND.GCP_PUBSUB.CONFIG_INVALID` otherwise), flow-control defaults applied
    /// and validated (>= 1).
    pub fn from_definition(
        def: &ChannelDefinition,
    ) -> Result<GcpPubSubChannelProperties, Diagnostic> {
        let props = &def.properties;
        let channel = &def.binding.channel_name;
        let project_id = non_blank(props.get(Self::KEY_PROJECT_ID)).ok_or_else(|| {
            Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!("gcp-pubsub channel '{channel}' requires property 'project-id'"),
            )
        })?;
        let subscription = non_blank(props.get(Self::KEY_SUBSCRIPTION)).unwrap_or_default();
        let topic = non_blank(props.get(Self::KEY_TOPIC)).unwrap_or_default();
        let max_outstanding_messages = parse_i64(
            channel,
            props.get(Self::KEY_MAX_OUTSTANDING_MESSAGES),
            Self::DEFAULT_MAX_OUTSTANDING_MESSAGES,
        )?;
        let max_outstanding_request_bytes = parse_i64(
            channel,
            props.get(Self::KEY_MAX_OUTSTANDING_REQUEST_BYTES),
            Self::DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES,
        )?;
        if max_outstanding_messages < 1 {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "gcp-pubsub channel '{channel}' flow-control.max-outstanding-messages must be >= 1"
                ),
            ));
        }
        if max_outstanding_request_bytes < 1 {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "gcp-pubsub channel '{channel}' flow-control.max-outstanding-request-bytes must be >= 1"
                ),
            ));
        }
        Ok(GcpPubSubChannelProperties {
            project_id,
            subscription,
            topic,
            max_outstanding_messages,
            max_outstanding_request_bytes,
            endpoint_override: non_blank(props.get(Self::KEY_ENDPOINT_OVERRIDE)),
            ack_mode: AckMode::parse(props.get("ack-mode").map(String::as_str)),
            singleton: def.singleton(),
        })
    }

    /// True when an inbound subscription is declared.
    pub fn has_subscription(&self) -> bool {
        !self.subscription.trim().is_empty()
    }

    /// True when a topic is declared.
    pub fn has_topic(&self) -> bool {
        !self.topic.trim().is_empty()
    }

    /// The canonical subscription path `projects/<project-id>/subscriptions/<subscription>`.
    pub fn subscription_path(&self) -> String {
        format!(
            "projects/{}/subscriptions/{}",
            self.project_id, self.subscription
        )
    }
}

/// Build the emulator-aware [`ClientConfig`] for a project. `endpoint_override` (or the
/// process-wide `PUBSUB_EMULATOR_HOST`, honoured by [`ClientConfig::default`]) selects the
/// plaintext emulator environment; otherwise the client targets real GCP with no token
/// source (credential wiring is deferred). The `project_id` always wins over the emulator's
/// synthetic default so per-tenant projects stay distinct.
pub fn build_client_config(project_id: &str, endpoint_override: Option<&str>) -> ClientConfig {
    let mut config = ClientConfig {
        project_id: Some(project_id.to_string()),
        ..Default::default()
    };
    if let Some(endpoint) = endpoint_override {
        let host = endpoint.trim();
        if !host.is_empty() {
            config.environment = Environment::Emulator(host.to_string());
        }
    }
    config
}

fn parse_i64(channel: &str, raw: Option<&String>, fallback: i64) -> Result<i64, Diagnostic> {
    match non_blank(raw) {
        None => Ok(fallback),
        Some(text) => text.parse::<i64>().map_err(|_| {
            Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "gcp-pubsub channel '{channel}' has invalid numeric property value: {text}"
                ),
            )
        }),
    }
}

fn non_blank(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

// ---- destination URIs (sink side) ---------------------------------------------------------

/// A parsed `gcp-pubsub://<topicName>` destination — the URI AUTHORITY names the short topic
/// name. The project id does NOT ride the URI; the sink composes
/// `projects/<project-id>/topics/<topicName>` from its engine-wide project config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcpPubSubDestination {
    pub topic: String,
}

/// Parse a `gcp-pubsub://<topicName>` destination. Failures are PERMANENT (a retry can never
/// fix a malformed URI). A missing/wrong scheme raises [`codes::OUTBOUND_SEND_FAILED`]; a
/// present-scheme-but-empty topic raises [`codes::OUTBOUND_TOPIC_MISSING`].
pub fn parse_destination(destination: &str) -> Result<GcpPubSubDestination, Diagnostic> {
    let Some(scheme) = sutra_channels::sink::scheme_of(destination) else {
        return Err(Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("gcp-pubsub destination '{destination}' has no URI scheme"),
        ));
    };
    if !scheme.eq_ignore_ascii_case(TRANSPORT) {
        return Err(Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("gcp-pubsub destination '{destination}' has scheme '{scheme}' (expected 'gcp-pubsub')"),
        ));
    }
    let rest = &destination[scheme.len() + "://".len()..];
    let authority = match rest.find('/') {
        Some(i) => &rest[..i],
        None => rest,
    };
    if authority.is_empty() {
        return Err(Diagnostic::error(
            codes::OUTBOUND_TOPIC_MISSING,
            format!("gcp-pubsub destination '{destination}' has no topic — expected gcp-pubsub://<topicName>"),
        ));
    }
    Ok(GcpPubSubDestination {
        topic: authority.to_string(),
    })
}

/// Every message attribute as a deterministic (sorted) header map — the projection helpers
/// consume this.
pub(crate) fn attributes_to_headers(
    attributes: &std::collections::HashMap<String, String>,
) -> BTreeMap<String, String> {
    attributes
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "payments", "1.0.0");
        let binding =
            ChannelBinding::new("transfer-sub", namespace, DeploymentId::unresolved(), "");
        ChannelDefinition {
            binding,
            transport: Some("gcp-pubsub".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn properties_apply_defaults_when_optional_keys_absent() {
        let def = definition(&[
            ("project-id", "acme-payments"),
            ("subscription", "transfer-sub"),
        ]);
        let props = GcpPubSubChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.project_id, "acme-payments");
        assert_eq!(props.subscription, "transfer-sub");
        assert_eq!(props.topic, "");
        assert_eq!(
            props.max_outstanding_messages,
            GcpPubSubChannelProperties::DEFAULT_MAX_OUTSTANDING_MESSAGES
        );
        assert_eq!(
            props.max_outstanding_request_bytes,
            GcpPubSubChannelProperties::DEFAULT_MAX_OUTSTANDING_REQUEST_BYTES
        );
        assert_eq!(props.ack_mode, AckMode::OnPersist);
        assert!(!props.singleton);
        assert!(props.has_subscription());
        assert!(!props.has_topic());
        assert_eq!(
            props.subscription_path(),
            "projects/acme-payments/subscriptions/transfer-sub"
        );
    }

    #[test]
    fn properties_read_overrides_and_singleton_flag() {
        let def = definition(&[
            ("project-id", "acme-payments"),
            ("subscription", "transfer-sub"),
            ("topic", "transfer-topic"),
            ("flow-control.max-outstanding-messages", "50"),
            ("flow-control.max-outstanding-request-bytes", "2048"),
            ("endpoint-override", "127.0.0.1:8085"),
            ("ack-mode", "On-Complete"),
            ("singleton", "true"),
        ]);
        let props = GcpPubSubChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.topic, "transfer-topic");
        assert_eq!(props.max_outstanding_messages, 50);
        assert_eq!(props.max_outstanding_request_bytes, 2048);
        assert_eq!(props.endpoint_override.as_deref(), Some("127.0.0.1:8085"));
        assert_eq!(props.ack_mode, AckMode::OnComplete);
        assert!(props.singleton);
        assert!(props.has_topic());

        // `consumer: exclusive` is the other singleton spelling.
        let def = definition(&[
            ("project-id", "p"),
            ("subscription", "s"),
            ("consumer", "exclusive"),
        ]);
        assert!(
            GcpPubSubChannelProperties::from_definition(&def)
                .expect("props")
                .singleton
        );
    }

    #[test]
    fn missing_project_id_fails_closed() {
        let def = definition(&[("subscription", "s")]);
        let err = GcpPubSubChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[test]
    fn non_positive_flow_control_fails_closed() {
        for key in [
            "flow-control.max-outstanding-messages",
            "flow-control.max-outstanding-request-bytes",
        ] {
            let def = definition(&[("project-id", "p"), ("subscription", "s"), (key, "0")]);
            let err = GcpPubSubChannelProperties::from_definition(&def).unwrap_err();
            assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID, "for '{key}'");
        }
    }

    #[test]
    fn non_numeric_flow_control_fails_closed() {
        let def = definition(&[
            ("project-id", "p"),
            ("subscription", "s"),
            ("flow-control.max-outstanding-messages", "lots"),
        ]);
        let err = GcpPubSubChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[test]
    fn destination_authority_is_the_topic() {
        let d = parse_destination("gcp-pubsub://payment-replies").expect("parse");
        assert_eq!(d.topic, "payment-replies");

        // A trailing path segment is ignored — Pub/Sub has no per-message key concern.
        let d = parse_destination("gcp-pubsub://payment-replies/ignored").expect("parse");
        assert_eq!(d.topic, "payment-replies");
    }

    #[test]
    fn empty_topic_is_topic_missing() {
        let err = parse_destination("gcp-pubsub://").unwrap_err();
        assert_eq!(err.code, codes::OUTBOUND_TOPIC_MISSING);
    }

    #[test]
    fn wrong_scheme_is_send_failed() {
        for bad in ["kafka://topic", "rabbitmq://broker/q", "no-scheme"] {
            let err = parse_destination(bad).unwrap_err();
            assert_eq!(err.code, codes::OUTBOUND_SEND_FAILED, "for '{bad}'");
        }
    }
}
