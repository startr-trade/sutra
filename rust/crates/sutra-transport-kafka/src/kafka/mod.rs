//! Kafka transport — the broker pair behind the transport seams:
//! [`source::KafkaTriggerSource`] implements [`sutra_channels::source::TriggerSource`] (inbound
//! consumer over a `group.id`, leader-gated for `singleton: true` channels) and
//! [`sink::KafkaMessageSink`] implements [`sutra_channels::sink::MessageSink`] for the `kafka://`
//! destination scheme.
//!
//! Wire strings are conformance invariants (the cross-broker header shape IS the
//! requirement); the outbox/reply headers use the `sutra-*` vocabulary while the CE spec
//! prefixes stay:
//!
//! - the outbox key rides the Kafka record header **`sutra-outbox-key`** (the dedup /
//!   consumer-idempotency token the five non-RabbitMQ brokers share), and is lifted back to
//!   the inbound idempotency key on the consumer side;
//! - the Kafka record **key** (partition pinning) is the OPTIONAL URI path segment
//!   `kafka://<topic>/<key>` — a SEPARATE concern from the outbox key (a destination with
//!   no path segment produces a record with no key);
//! - the CloudEvents binary binding uses the **`ce_`** attribute prefix (underscore — Kafka
//!   header keys forbid the HTTP `ce-` dash form); the CE projection happens upstream
//!   (dispatcher-side, [`sutra_channels::outbox_dispatch`] `CeBinding::KafkaBinary`), this sink
//!   carries the resulting headers verbatim;
//! - the reply `content-type` rides the **`content-type`** record header, defaulting to
//!   `application/octet-stream`.

pub mod sink;
pub mod source;

pub use sink::KafkaMessageSink;
pub use source::{KafkaSourceConfig, KafkaTriggerSource};

use std::collections::BTreeMap;

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact Kafka diagnostic codes
/// this module raises.
pub mod codes {
    pub const INBOUND_CONSUMER_FAILED: &str = "SUTRA.INBOUND.KAFKA.CONSUMER_FAILED";
    pub const INBOUND_TOPIC_MISSING: &str = "SUTRA.INBOUND.KAFKA.TOPIC_MISSING";
    pub const INBOUND_DESERIALIZE_FAILED: &str = "SUTRA.INBOUND.KAFKA.DESERIALIZE_FAILED";
    pub const INBOUND_CONFIG_INVALID: &str = "SUTRA.INBOUND.KAFKA.CONFIG_INVALID";
    /// An inbound record presented a missing/wrong per-message credential; the
    /// record is dropped (offset committed) and never dispatched.
    pub const INBOUND_AUTH_REJECTED: &str = "SUTRA.INBOUND.KAFKA.AUTH_REJECTED";
    /// `inbound-auth.scheme=mtls` is per-message UNSUPPORTED; a one-time boot WARN,
    /// then allow-through (broker-level SASL/SSL still applies).
    pub const INBOUND_MTLS_UNSUPPORTED: &str = "SUTRA.INBOUND.KAFKA.MTLS_UNSUPPORTED";

    pub const OUTBOUND_PRODUCE_FAILED: &str = "SUTRA.OUTBOUND.KAFKA.PRODUCE_FAILED";
    pub const OUTBOUND_TOPIC_MISSING: &str = "SUTRA.OUTBOUND.KAFKA.TOPIC_MISSING";
    pub const OUTBOUND_CONFIG_INVALID: &str = "SUTRA.OUTBOUND.KAFKA.CONFIG_INVALID";

    /// `SUTRA.OUTBOUND.SEND.FAILED` — malformed destination (permanent posture).
    pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.SEND.FAILED";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "kafka";

/// The record header carrying the outbox / consumer-idempotency key — the cross-broker
/// string the five non-RabbitMQ brokers share (a frozen wire name).
pub const HEADER_OUTBOX_KEY: &str = "sutra-outbox-key";
/// The record header naming a reply destination (a frozen wire name).
pub const HEADER_REPLY_TO: &str = "sutra-reply-to";
/// The record header carrying the payload content type.
pub const HEADER_CONTENT_TYPE: &str = "content-type";

/// The librdkafka client-config keys this module owns explicitly — a passthrough key that
/// collides with one of these is ignored (the typed field wins).
const OWNED_CLIENT_KEYS: [&str; 4] = [
    "bootstrap.servers",
    "group.id",
    "auto.offset.reset",
    "security.protocol",
];

/// The Kafka `security.protocol` values librdkafka accepts (the allow-list). This
/// build is PLAINTEXT-only (no ssl/sasl features); a non-PLAINTEXT protocol parses here
/// but fails closed at client construction — the operator's config error, never a literal
/// this transport silently downgrades.
const SECURITY_PROTOCOLS: [&str; 4] = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];

/// Effective ack modes of a broker channel — parsed leniently:
/// `on-complete` (ASCII case-insensitive) opts in, anything else is `on-persist`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Commit the record offset once the intake made the delivery durable (default).
    OnPersist,
    /// Commit only at instance COMPLETED / skip (drop) at FAILED.
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

/// Typed view over the transport-specific channel properties — the Kafka
/// channel-properties shape (same keys, same defaults). Kafka authenticates via
/// broker-level SASL/TLS (this build is PLAINTEXT-only), so there are no channel-YAML
/// credentials and no secret-reference discipline here (contrast the RabbitMQ properties).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaChannelProperties {
    /// `bootstrap.servers` (required inbound).
    pub bootstrap_servers: String,
    /// `topic` to consume from (required inbound).
    pub topic: String,
    /// `group.id` — defaults to `sutra-<channel>` when unset (the engine-lease singleton,
    /// not the Kafka group, is what makes a channel exactly-once across replicas).
    pub group_id: String,
    /// `auto.offset.reset` (default `earliest` — a late-activating singleton leader reads
    /// from the start and misses nothing).
    pub auto_offset_reset: String,
    /// `security.protocol` (default `PLAINTEXT`, upper-cased, validated to the allow-list).
    pub security_protocol: String,
    /// Verbatim `kafka.*` / `kafka.consumer.*` passthrough (prefix stripped) — extra
    /// librdkafka client config the author is responsible for keeping valid.
    pub client_config: BTreeMap<String, String>,
    /// Engine-level ack semantics (`ack-mode`, default `on-persist`).
    pub ack_mode: AckMode,
    /// Per-channel singleton declaration (`singleton: true` / `consumer: exclusive`).
    pub singleton: bool,
}

impl KafkaChannelProperties {
    pub const DEFAULT_AUTO_OFFSET_RESET: &'static str = "earliest";
    pub const DEFAULT_SECURITY_PROTOCOL: &'static str = "PLAINTEXT";

    /// Read the typed properties off a channel definition. Mirrors
    /// `KafkaChannelProperties.from(ChannelConfig)`: defaults applied, `security.protocol`
    /// validated (`SUTRA.INBOUND.KAFKA.CONFIG_INVALID` otherwise).
    pub fn from_definition(def: &ChannelDefinition) -> Result<KafkaChannelProperties, Diagnostic> {
        let props = &def.properties;
        let channel = &def.binding.channel_name;
        let bootstrap_servers = non_blank(props.get("bootstrap.servers")).unwrap_or_default();
        let topic = non_blank(props.get("topic")).unwrap_or_default();
        let group_id =
            non_blank(props.get("group.id")).unwrap_or_else(|| format!("sutra-{channel}"));
        let auto_offset_reset = non_blank(props.get("auto.offset.reset"))
            .unwrap_or_else(|| Self::DEFAULT_AUTO_OFFSET_RESET.to_string());
        let security_protocol = match non_blank(props.get("security.protocol")) {
            None => Self::DEFAULT_SECURITY_PROTOCOL.to_string(),
            Some(raw) => {
                let upper = raw.to_ascii_uppercase();
                if !SECURITY_PROTOCOLS.contains(&upper.as_str()) {
                    return Err(Diagnostic::error(
                        codes::INBOUND_CONFIG_INVALID,
                        format!(
                            "kafka channel '{channel}' property 'security.protocol' must be one \
                             of {SECURITY_PROTOCOLS:?}, got '{raw}'"
                        ),
                    ));
                }
                upper
            }
        };
        let client_config = passthrough_config(props);
        // The startup-orchestrator resolution (`ChannelDefinition::effective_ack_mode`):
        // a declared `ack-mode:` wins, the broker default is `on-persist`.
        let ack_mode = AckMode::parse(Some(def.effective_ack_mode()));
        Ok(KafkaChannelProperties {
            bootstrap_servers,
            topic,
            group_id,
            auto_offset_reset,
            security_protocol,
            client_config,
            ack_mode,
            singleton: def.singleton(),
        })
    }

    /// True when an inbound topic is declared.
    pub fn has_topic(&self) -> bool {
        !self.topic.trim().is_empty()
    }

    /// True when a bootstrap-servers list is declared.
    pub fn has_bootstrap(&self) -> bool {
        !self.bootstrap_servers.trim().is_empty()
    }
}

/// Collect `kafka.*` / `kafka.consumer.*` passthrough keys into a librdkafka client-config
/// map (prefix stripped). `kafka.producer.*` keys are sink-only and skipped here; owned
/// keys ([`OWNED_CLIENT_KEYS`]) never come through the passthrough.
fn passthrough_config(props: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (key, value) in props {
        let stripped = key
            .strip_prefix("kafka.consumer.")
            .or_else(|| key.strip_prefix("kafka."));
        let Some(stripped) = stripped else { continue };
        // `kafka.producer.*` is not a consumer knob.
        if stripped.starts_with("producer.") {
            continue;
        }
        if OWNED_CLIENT_KEYS.contains(&stripped) {
            continue;
        }
        out.insert(stripped.to_string(), value.clone());
    }
    out
}

fn non_blank(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

// ---- destination URIs (sink side) ---------------------------------------------------------

/// A parsed `kafka://<topic>[/<key>]` destination — the URI AUTHORITY names the topic, an
/// OPTIONAL path segment is the Kafka record key (partition pinning). Bootstrap servers do
/// NOT ride the URI — the sink is engine-wide-configured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaDestination {
    pub topic: String,
    /// The record key (partition key), or `None` for round-robin partitioning.
    pub key: Option<String>,
}

/// Parse a `kafka://<topic>[/<key>]` destination. Failures are PERMANENT (a retry can
/// never fix a malformed URI).
pub fn parse_destination(destination: &str) -> Result<KafkaDestination, Diagnostic> {
    let malformed = |detail: &str| {
        Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("kafka destination '{destination}' {detail}"),
        )
    };
    let Some(scheme) = sutra_channels::sink::scheme_of(destination) else {
        return Err(malformed("has no URI scheme"));
    };
    if !scheme.eq_ignore_ascii_case("kafka") {
        return Err(malformed(&format!(
            "has scheme '{scheme}' (expected 'kafka')"
        )));
    }
    let rest = &destination[scheme.len() + "://".len()..];
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };
    if authority.is_empty() {
        return Err(malformed("has no topic — expected kafka://<topic>[/<key>]"));
    }
    let key = {
        let trimmed = path.trim_matches('/');
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    };
    Ok(KafkaDestination {
        topic: authority.to_string(),
        key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "payments", "1.0.0");
        let binding =
            ChannelBinding::new("transfer-topic", namespace, DeploymentId::unresolved(), "");
        ChannelDefinition {
            binding,
            transport: Some("kafka".to_string()),
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
        let def = definition(&[("bootstrap.servers", "kafka:9092"), ("topic", "transfer")]);
        let props = KafkaChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.bootstrap_servers, "kafka:9092");
        assert_eq!(props.topic, "transfer");
        // group.id defaults to sutra-<channel> when unset.
        assert_eq!(props.group_id, "sutra-transfer-topic");
        assert_eq!(props.auto_offset_reset, "earliest");
        assert_eq!(props.security_protocol, "PLAINTEXT");
        assert_eq!(props.ack_mode, AckMode::OnPersist);
        assert!(!props.singleton);
        assert!(props.has_topic());
        assert!(props.has_bootstrap());
        assert!(props.client_config.is_empty());
    }

    #[test]
    fn properties_read_overrides_and_singleton_flag() {
        let def = definition(&[
            ("bootstrap.servers", "b1:9092,b2:9092"),
            ("topic", "transfer-topic"),
            ("group.id", "money-transfer"),
            ("auto.offset.reset", "latest"),
            ("security.protocol", "ssl"),
            ("ack-mode", "On-Complete"),
            ("singleton", "true"),
            ("kafka.consumer.fetch.min.bytes", "1024"),
            ("kafka.client.id", "svc-1"),
            ("kafka.producer.linger.ms", "5"),
        ]);
        let props = KafkaChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.bootstrap_servers, "b1:9092,b2:9092");
        assert_eq!(props.group_id, "money-transfer");
        assert_eq!(props.auto_offset_reset, "latest");
        // security.protocol upper-cases.
        assert_eq!(props.security_protocol, "SSL");
        assert_eq!(props.ack_mode, AckMode::OnComplete);
        assert!(props.singleton);
        // kafka.consumer.* and kafka.* pass through (prefix stripped); producer.* does not.
        assert_eq!(
            props
                .client_config
                .get("fetch.min.bytes")
                .map(String::as_str),
            Some("1024")
        );
        assert_eq!(
            props.client_config.get("client.id").map(String::as_str),
            Some("svc-1")
        );
        assert!(!props.client_config.contains_key("producer.linger.ms"));

        // `consumer: exclusive` is the other singleton spelling.
        let def = definition(&[
            ("bootstrap.servers", "kafka:9092"),
            ("topic", "t"),
            ("consumer", "exclusive"),
        ]);
        assert!(
            KafkaChannelProperties::from_definition(&def)
                .expect("props")
                .singleton
        );
    }

    #[test]
    fn invalid_security_protocol_fails_closed() {
        let def = definition(&[
            ("bootstrap.servers", "kafka:9092"),
            ("topic", "t"),
            ("security.protocol", "carrier-pigeon"),
        ]);
        let err = KafkaChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[test]
    fn destination_authority_is_the_topic_no_path_means_no_key() {
        let d = parse_destination("kafka://payment-replies").expect("parse");
        assert_eq!(d.topic, "payment-replies");
        assert_eq!(d.key, None);
    }

    #[test]
    fn destination_path_segment_is_the_record_key() {
        let d = parse_destination("kafka://payment-replies/customer-7").expect("parse");
        assert_eq!(d.topic, "payment-replies");
        assert_eq!(d.key.as_deref(), Some("customer-7"));

        // A trailing slash with no key is still keyless.
        let d = parse_destination("kafka://topic/").expect("parse");
        assert_eq!(d.topic, "topic");
        assert_eq!(d.key, None);
    }

    #[test]
    fn malformed_destinations_are_permanent_errors() {
        for bad in [
            "kafka://",
            "kafka:///only-a-key",
            "rabbitmq://broker/q",
            "no-scheme",
        ] {
            let err = parse_destination(bad).unwrap_err();
            assert_eq!(err.code, codes::OUTBOUND_SEND_FAILED, "for '{bad}'");
        }
    }
}
