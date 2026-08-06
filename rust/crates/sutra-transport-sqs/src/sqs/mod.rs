//! AWS SQS transport — the broker pair behind the transport seams:
//! [`source::SqsTriggerSource`] implements [`sutra_channels::source::TriggerSource`] (inbound
//! long-poll consumer, leader-gated for `singleton: true` channels) and
//! [`sink::SqsMessageSink`] implements [`sutra_channels::sink::MessageSink`] for the `aws-sqs://`
//! destination scheme.
//!
//! Wire strings are conformance invariants — the cross-broker attribute shape IS the
//! requirement; the carriers use the `sutra-*` vocabulary while the CE spec prefix stays:
//!
//! - the outbox key rides the SQS message attribute **`sutra-outbox-key`** (the dedup /
//!   consumer-idempotency token the non-RabbitMQ brokers share), and is lifted back to the
//!   inbound idempotency key on the consumer side;
//! - the CloudEvents binary binding uses the **`ce-`** attribute prefix (DASH — SQS
//!   attribute names allow the HTTP dash form); the CE projection happens upstream
//!   (dispatcher-side, [`sutra_channels::outbox_dispatch`] `CeBinding::Sqs`), this sink carries the
//!   resulting attributes verbatim;
//! - the reply `content-type` rides the **`content-type`** message attribute, defaulting to
//!   `application/octet-stream`;
//! - a FIFO queue URL (ends `.fifo`) additionally sets the SQS-native
//!   `MessageDeduplicationId` to the outbox key (5-minute native dedup window).

pub mod sink;
pub mod source;

pub use sink::{SqsMessageSink, SqsSinkSettings};
pub use source::{build_client, SqsSourceConfig, SqsTriggerSource};

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact AWS SQS diagnostic codes this module raises
/// (the `<BROKER>` token in `SUTRA.<DIRECTION>.<BROKER>.<REASON>` is `AWS_SQS`).
pub mod codes {
    pub const INBOUND_CONNECTION_FAILED: &str = "SUTRA.INBOUND.AWS_SQS.CONNECTION_FAILED";
    pub const INBOUND_QUEUE_MISSING: &str = "SUTRA.INBOUND.AWS_SQS.QUEUE_MISSING";
    pub const INBOUND_RECEIVE_FAILED: &str = "SUTRA.INBOUND.AWS_SQS.RECEIVE_FAILED";
    pub const INBOUND_CONFIG_INVALID: &str = "SUTRA.INBOUND.AWS_SQS.CONFIG_INVALID";
    /// Per-message inbound auth: the credential did not match the expected key.
    pub const INBOUND_AUTH_REJECTED: &str = "SUTRA.INBOUND.AWS_SQS.AUTH_REJECTED";
    /// Per-channel mTLS is unsupported (broker/transport-level TLS applies instead).
    pub const INBOUND_MTLS_UNSUPPORTED: &str = "SUTRA.INBOUND.AWS_SQS.MTLS_UNSUPPORTED";

    pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.AWS_SQS.SEND_FAILED";
    pub const OUTBOUND_QUEUE_MISSING: &str = "SUTRA.OUTBOUND.AWS_SQS.QUEUE_MISSING";
    pub const OUTBOUND_CONFIG_INVALID: &str = "SUTRA.OUTBOUND.AWS_SQS.CONFIG_INVALID";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "aws-sqs";

/// The message attribute carrying the outbox / consumer-idempotency key — the cross-broker
/// string the non-RabbitMQ brokers share (a frozen wire name). Kept equal to the
/// kafka `HEADER_OUTBOX_KEY` (pinned by the engine's `transport_bundle` sharing test).
pub const HEADER_OUTBOX_KEY: &str = "sutra-outbox-key";
/// The message attribute naming a reply destination (a frozen wire name).
pub const HEADER_REPLY_TO: &str = "sutra-reply-to";
/// The message attribute carrying the payload content type.
pub const HEADER_CONTENT_TYPE: &str = "content-type";

/// Default long-poll wait per ReceiveMessage (seconds).
pub const DEFAULT_WAIT_TIME: i32 = 10;
/// Default batch size per ReceiveMessage.
pub const DEFAULT_MAX_MESSAGES: i32 = 10;
/// Default per-message visibility timeout (seconds).
pub const DEFAULT_VISIBILITY_TIMEOUT: i32 = 30;

/// Effective ack modes of a broker channel — parsed leniently:
/// `on-complete` (ASCII case-insensitive) opts in, anything else is `on-persist`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Delete the message once the intake made the delivery durable (default).
    OnPersist,
    /// Delete only at instance COMPLETED / drop at FAILED.
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

/// Typed view over the transport-specific channel properties of an AWS SQS channel.
/// Authentication rides the runtime identity (static-credentials provider from the
/// environment), so there are no channel-YAML credentials here (contrast the RabbitMQ
/// properties).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqsChannelProperties {
    /// `region` (required) — AWS region, e.g. `us-east-1`.
    pub region: String,
    /// `queue.url` (required inbound) — the canonical SQS URL.
    pub queue_url: String,
    /// `wait-time-seconds` (default 10; clamp 0..=20) — long-poll wait per ReceiveMessage.
    pub wait_time_seconds: i32,
    /// `max-messages` (default 10; clamp 1..=10) — batch size per ReceiveMessage.
    pub max_messages: i32,
    /// `visibility-timeout-seconds` (default 30; clamp 0..=43200) — per-message timeout.
    pub visibility_timeout_seconds: i32,
    /// `endpoint-override` (optional) — override the SDK endpoint URL (LocalStack tests).
    pub endpoint_override: Option<String>,
    /// Engine-level ack semantics (`ack-mode`, default `on-persist`).
    pub ack_mode: AckMode,
    /// Per-channel singleton declaration (`singleton: true` / `consumer: exclusive`).
    pub singleton: bool,
}

impl SqsChannelProperties {
    /// Read the typed properties off a channel definition: `region` is required
    /// ([`codes::INBOUND_CONFIG_INVALID`]), and the integer knobs are parsed and
    /// range-checked (out-of-range / non-integer ⇒ `INBOUND_CONFIG_INVALID`).
    pub fn from_definition(def: &ChannelDefinition) -> Result<SqsChannelProperties, Diagnostic> {
        let props = &def.properties;
        let channel = &def.binding.channel_name;
        let Some(region) = non_blank(props.get("region")) else {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!("aws-sqs channel '{channel}' requires property 'region'"),
            ));
        };
        let queue_url = non_blank(props.get("queue.url")).unwrap_or_default();
        let wait_time_seconds = clamp(
            channel,
            "wait-time-seconds",
            parse_int(channel, props.get("wait-time-seconds"), DEFAULT_WAIT_TIME)?,
            0,
            20,
        )?;
        let max_messages = clamp(
            channel,
            "max-messages",
            parse_int(channel, props.get("max-messages"), DEFAULT_MAX_MESSAGES)?,
            1,
            10,
        )?;
        let visibility_timeout_seconds = clamp(
            channel,
            "visibility-timeout-seconds",
            parse_int(
                channel,
                props.get("visibility-timeout-seconds"),
                DEFAULT_VISIBILITY_TIMEOUT,
            )?,
            0,
            43200,
        )?;
        let endpoint_override = non_blank(props.get("endpoint-override"));
        let ack_mode = AckMode::parse(props.get("ack-mode").map(String::as_str));
        Ok(SqsChannelProperties {
            region,
            queue_url,
            wait_time_seconds,
            max_messages,
            visibility_timeout_seconds,
            endpoint_override,
            ack_mode,
            singleton: def.singleton(),
        })
    }

    /// True when an inbound queue URL is declared.
    pub fn has_queue_url(&self) -> bool {
        !self.queue_url.trim().is_empty()
    }
}

/// True when a queue URL is a FIFO queue (its name ends in `.fifo`).
pub fn is_fifo_queue_url(queue_url: &str) -> bool {
    queue_url.ends_with(".fifo")
}

fn parse_int(channel: &str, raw: Option<&String>, fallback: i32) -> Result<i32, Diagnostic> {
    match non_blank(raw) {
        None => Ok(fallback),
        Some(v) => v.parse::<i32>().map_err(|_| {
            Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!("aws-sqs channel '{channel}' has invalid integer property value: {v}"),
            )
        }),
    }
}

fn clamp(channel: &str, key: &str, value: i32, min: i32, max: i32) -> Result<i32, Diagnostic> {
    if value < min || value > max {
        return Err(Diagnostic::error(
            codes::INBOUND_CONFIG_INVALID,
            format!(
                "aws-sqs channel '{channel}' property '{key}' value {value} out of range \
                 [{min}..{max}]"
            ),
        ));
    }
    Ok(value)
}

fn non_blank(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

// ---- destination URIs (sink side) ---------------------------------------------------------

/// A parsed SQS destination. The sink is engine-wide-configured (region + account id +
/// optional endpoint override ride the sink settings, NOT the URI):
///
/// - a full `https://` / `http://` URL is used verbatim ([`Self::QueueUrl`]);
/// - `aws-sqs://<queueName>` names a queue whose account id comes from the sink settings;
/// - `aws-sqs://<accountId>/<queueName>` carries the account id in the URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqsDestination {
    /// A full SQS URL — used verbatim as the queue URL.
    QueueUrl(String),
    /// A named queue; the account id is from the URI when present, else the sink settings.
    Named {
        account_id: Option<String>,
        queue_name: String,
    },
}

impl SqsDestination {
    /// Resolve to a concrete SQS queue URL using the engine-wide sink settings. `None` when
    /// no region/account/endpoint can be combined into a URL (the sink maps that to its
    /// poison posture — a retry can never grow the missing config).
    pub fn resolve(&self, settings: &SqsSinkSettings) -> Option<String> {
        match self {
            SqsDestination::QueueUrl(url) => Some(url.clone()),
            SqsDestination::Named {
                account_id,
                queue_name,
            } => {
                let account = account_id.clone().or_else(|| settings.account_id.clone());
                if let Some(endpoint) = &settings.endpoint_override {
                    let base = endpoint.strip_suffix('/').unwrap_or(endpoint);
                    let acct = account.unwrap_or_else(|| "000000000000".to_string());
                    return Some(format!("{base}/{acct}/{queue_name}"));
                }
                let account = account?;
                let region = non_blank_str(&settings.region)?;
                Some(format!(
                    "https://sqs.{region}.amazonaws.com/{account}/{queue_name}"
                ))
            }
        }
    }
}

/// Parse a destination URI into an [`SqsDestination`]. Failures are PERMANENT (a retry can
/// never fix a malformed URI).
pub fn parse_destination(destination: &str) -> Result<SqsDestination, Diagnostic> {
    let malformed = |detail: &str| {
        Diagnostic::error(
            codes::OUTBOUND_QUEUE_MISSING,
            format!(
                "aws-sqs destination '{destination}' {detail} (expected aws-sqs://<queueName>, \
                 aws-sqs://<accountId>/<queueName>, or a full https SQS URL)"
            ),
        )
    };
    let Some(scheme) = sutra_channels::sink::scheme_of(destination) else {
        return Err(malformed("has no URI scheme"));
    };
    if scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https") {
        return Ok(SqsDestination::QueueUrl(destination.to_string()));
    }
    if !scheme.eq_ignore_ascii_case(TRANSPORT) {
        return Err(malformed(&format!(
            "has scheme '{scheme}' (expected 'aws-sqs')"
        )));
    }
    let rest = &destination[scheme.len() + "://".len()..];
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };
    if authority.is_empty() {
        return Err(malformed("has no queue name"));
    }
    let path = path.trim_matches('/');
    if path.is_empty() {
        // aws-sqs://<queueName>
        Ok(SqsDestination::Named {
            account_id: None,
            queue_name: authority.to_string(),
        })
    } else {
        // aws-sqs://<accountId>/<queueName>
        Ok(SqsDestination::Named {
            account_id: Some(authority.to_string()),
            queue_name: path.to_string(),
        })
    }
}

fn non_blank_str(value: &str) -> Option<String> {
    let t = value.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "payments", "1.0.0");
        let binding =
            ChannelBinding::new("transfer-queue", namespace, DeploymentId::unresolved(), "");
        ChannelDefinition {
            binding,
            transport: Some("aws-sqs".to_string()),
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
            ("region", "us-east-1"),
            (
                "queue.url",
                "https://sqs.us-east-1.amazonaws.com/000000000000/transfer",
            ),
        ]);
        let props = SqsChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.region, "us-east-1");
        assert_eq!(props.wait_time_seconds, 10);
        assert_eq!(props.max_messages, 10);
        assert_eq!(props.visibility_timeout_seconds, 30);
        assert_eq!(props.ack_mode, AckMode::OnPersist);
        assert!(!props.singleton);
        assert!(props.has_queue_url());
        assert_eq!(props.endpoint_override, None);
    }

    #[test]
    fn properties_read_overrides_and_singleton_flag() {
        let def = definition(&[
            ("region", "eu-west-1"),
            ("queue.url", "https://sqs.eu-west-1.amazonaws.com/1/q"),
            ("wait-time-seconds", "5"),
            ("max-messages", "3"),
            ("visibility-timeout-seconds", "120"),
            ("endpoint-override", "http://localstack:4566"),
            ("ack-mode", "On-Complete"),
            ("singleton", "true"),
        ]);
        let props = SqsChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.wait_time_seconds, 5);
        assert_eq!(props.max_messages, 3);
        assert_eq!(props.visibility_timeout_seconds, 120);
        assert_eq!(
            props.endpoint_override.as_deref(),
            Some("http://localstack:4566")
        );
        assert_eq!(props.ack_mode, AckMode::OnComplete);
        assert!(props.singleton);

        // `consumer: exclusive` is the other singleton spelling.
        let def = definition(&[
            ("region", "us-east-1"),
            ("queue.url", "https://sqs.us-east-1.amazonaws.com/1/q"),
            ("consumer", "exclusive"),
        ]);
        assert!(
            SqsChannelProperties::from_definition(&def)
                .expect("props")
                .singleton
        );
    }

    #[test]
    fn region_is_required() {
        let def = definition(&[("queue.url", "https://sqs.us-east-1.amazonaws.com/1/q")]);
        let err = SqsChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[test]
    fn out_of_range_and_non_integer_knobs_fail_closed() {
        let base = |extra: (&str, &str)| {
            definition(&[
                ("region", "us-east-1"),
                ("queue.url", "https://sqs.us-east-1.amazonaws.com/1/q"),
                extra,
            ])
        };
        for extra in [
            ("wait-time-seconds", "21"),             // > 20
            ("max-messages", "0"),                   // < 1
            ("max-messages", "11"),                  // > 10
            ("visibility-timeout-seconds", "43201"), // > 43200
            ("wait-time-seconds", "not-a-number"),
        ] {
            let err = SqsChannelProperties::from_definition(&base(extra)).unwrap_err();
            assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID, "for {extra:?}");
        }
    }

    fn settings(region: &str, account: Option<&str>, endpoint: Option<&str>) -> SqsSinkSettings {
        SqsSinkSettings {
            region: region.to_string(),
            account_id: account.map(String::from),
            endpoint_override: endpoint.map(String::from),
        }
    }

    #[test]
    fn full_url_destination_is_used_verbatim() {
        let d =
            parse_destination("https://sqs.us-east-1.amazonaws.com/123/replies").expect("parse");
        assert_eq!(
            d,
            SqsDestination::QueueUrl("https://sqs.us-east-1.amazonaws.com/123/replies".to_string())
        );
        // Resolves verbatim regardless of settings.
        assert_eq!(
            d.resolve(&settings("eu-west-1", None, None)).as_deref(),
            Some("https://sqs.us-east-1.amazonaws.com/123/replies")
        );
    }

    #[test]
    fn named_destination_composes_from_settings_account_and_region() {
        let d = parse_destination("aws-sqs://payment-replies").expect("parse");
        assert_eq!(
            d,
            SqsDestination::Named {
                account_id: None,
                queue_name: "payment-replies".to_string()
            }
        );
        assert_eq!(
            d.resolve(&settings("us-east-1", Some("000000000000"), None))
                .as_deref(),
            Some("https://sqs.us-east-1.amazonaws.com/000000000000/payment-replies")
        );
        // No account id AND no endpoint ⇒ unresolvable.
        assert_eq!(d.resolve(&settings("us-east-1", None, None)), None);
    }

    #[test]
    fn named_destination_with_account_id_in_uri() {
        let d = parse_destination("aws-sqs://999888777/replies").expect("parse");
        assert_eq!(
            d,
            SqsDestination::Named {
                account_id: Some("999888777".to_string()),
                queue_name: "replies".to_string()
            }
        );
        assert_eq!(
            d.resolve(&settings("ap-south-1", None, None)).as_deref(),
            Some("https://sqs.ap-south-1.amazonaws.com/999888777/replies")
        );
    }

    #[test]
    fn endpoint_override_composes_localstack_url() {
        let d = parse_destination("aws-sqs://transfer").expect("parse");
        assert_eq!(
            d.resolve(&settings(
                "us-east-1",
                None,
                Some("http://localstack:4566/")
            ))
            .as_deref(),
            Some("http://localstack:4566/000000000000/transfer")
        );
        // An account id in the URI wins over the placeholder.
        let d = parse_destination("aws-sqs://42/transfer").expect("parse");
        assert_eq!(
            d.resolve(&settings("us-east-1", None, Some("http://localstack:4566")))
                .as_deref(),
            Some("http://localstack:4566/42/transfer")
        );
    }

    #[test]
    fn malformed_destinations_are_permanent_queue_missing() {
        for bad in ["aws-sqs://", "kafka://topic", "no-scheme"] {
            let err = parse_destination(bad).unwrap_err();
            assert_eq!(err.code, codes::OUTBOUND_QUEUE_MISSING, "for '{bad}'");
        }
    }

    #[test]
    fn fifo_detection() {
        assert!(is_fifo_queue_url(
            "https://sqs.us-east-1.amazonaws.com/1/orders.fifo"
        ));
        assert!(!is_fifo_queue_url(
            "https://sqs.us-east-1.amazonaws.com/1/orders"
        ));
    }
}
