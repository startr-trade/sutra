//! AMQP 1.0 transport — the broker pair behind the transport seams:
//! [`source::AmqpTriggerSource`] implements [`sutra_channels::source::TriggerSource`] (inbound
//! consumer over a queue/topic, leader-gated for `singleton: true` channels) and
//! [`sink::AmqpMessageSink`] implements [`sutra_channels::sink::MessageSink`] for the `amqp10://`
//! destination scheme.
//!
//! This transport is AMQP **1.0** (Artemis / Azure Service Bus / Solace / Qpid) and is
//! DISTINCT from the rabbitmq transport, which is AMQP **0.9.1** (lapin, the `cloudEvents:`
//! binding):
//!
//! - the client is `fe2o3-amqp` (native pure-Rust AMQP 1.0), NOT lapin;
//! - the outbox key + CE attributes ride **application-properties** (an AMQP 1.0 map on the
//!   message), NOT the AMQP-0.9 `cloudEvents:` message-header binding;
//! - the CloudEvents binary binding uses the **`ce-`** DASH prefix (`ce-id`, `ce-type`, …):
//!   AMQP 1.0 application-property keys are unrestricted strings, so the hyphenated form the
//!   CloudEvents AMQP binding specifies rides the wire verbatim — no JMS-style `ce_`
//!   sanitisation is needed, because this client is not a JMS client;
//! - the outbox / consumer-idempotency key rides the **`sutra-outbox-key`** application
//!   property — the cross-broker token the five non-RabbitMQ brokers share.
//!
//! Scheme disambiguation: the RabbitMQ (AMQP 0.9.1) sink already claims `amqp`/`amqps`, and
//! the scheme matrix across the transports is a frozen contract. In this single
//! monolith both sinks register, so the AMQP 1.0 destination scheme is **`amqp10`**
//! (`amqp10s` for TLS) — self-documenting and collision-free. The wire connection fe2o3
//! opens still uses standard `amqp://` / `amqps://` (this build is PLAINTEXT-only: an
//! `amqp10s://`/`tls: true` request parses but fails closed at connect, mirroring the
//! Kafka non-PLAINTEXT posture; broker-level TLS via a sidecar/mesh is the supported route).

pub mod sink;
pub mod source;

pub use sink::AmqpMessageSink;
pub use source::{AmqpSourceConfig, AmqpTriggerSource};

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact AMQP diagnostic codes this module raises, all
/// in the `SUTRA.{INBOUND,OUTBOUND}.AMQP.*` namespace.
pub mod codes {
    pub const INBOUND_CONNECTION_FAILED: &str = "SUTRA.INBOUND.AMQP.CONNECTION_FAILED";
    pub const INBOUND_QUEUE_MISSING: &str = "SUTRA.INBOUND.AMQP.QUEUE_MISSING";
    pub const INBOUND_RECEIVE_FAILED: &str = "SUTRA.INBOUND.AMQP.RECEIVE_FAILED";
    pub const INBOUND_CONFIG_INVALID: &str = "SUTRA.INBOUND.AMQP.CONFIG_INVALID";
    /// Per-message inbound auth: the credential did not match the expected key.
    pub const INBOUND_AUTH_REJECTED: &str = "SUTRA.INBOUND.AMQP.AUTH_REJECTED";
    /// Per-channel mTLS is unsupported (broker/transport-level TLS applies instead).
    pub const INBOUND_MTLS_UNSUPPORTED: &str = "SUTRA.INBOUND.AMQP.MTLS_UNSUPPORTED";

    pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.AMQP.SEND_FAILED";
    pub const OUTBOUND_DESTINATION_MISSING: &str = "SUTRA.OUTBOUND.AMQP.DESTINATION_MISSING";
    pub const OUTBOUND_CONFIG_INVALID: &str = "SUTRA.OUTBOUND.AMQP.CONFIG_INVALID";

    /// `SUTRA.STARTUP.REFUSED.HARDCODED_SECRET` — literal broker credentials in channel
    /// YAML are refused (15-factor Factor 3; the shared engine-wide code).
    pub const STARTUP_REFUSED_HARDCODED_SECRET: &str = "SUTRA.STARTUP.REFUSED.HARDCODED_SECRET";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "amqp";

/// The SUTRA destination URI scheme (plaintext) — distinct from rabbitmq's `amqp`.
pub const SCHEME: &str = "amqp10";
/// The SUTRA destination URI scheme (TLS) — distinct from rabbitmq's `amqps`.
pub const SCHEME_TLS: &str = "amqp10s";

/// The application-property carrying the outbox / consumer-idempotency key — the
/// cross-broker string the five non-RabbitMQ brokers share (a frozen wire name).
pub const PROPERTY_OUTBOX_KEY: &str = "sutra-outbox-key";
/// The application-property carrying the payload content type.
pub const PROPERTY_CONTENT_TYPE: &str = "content-type";
/// The application-property naming a reply destination (a frozen wire name).
pub const PROPERTY_REPLY_TO: &str = "sutra-reply-to";

/// Default broker port (plaintext).
pub const DEFAULT_PORT: u16 = 5672;
/// Default broker port (TLS).
pub const DEFAULT_TLS_PORT: u16 = 5671;
/// Default AMQP prefetch / link credit.
pub const DEFAULT_PREFETCH: u32 = 10;
/// Default per-poll receive timeout (ms).
pub const DEFAULT_RECEIVE_TIMEOUT_MS: u64 = 1_000;

/// Effective ack modes of a broker channel — parsed leniently:
/// `on-complete` (ASCII case-insensitive) opts in, anything else is `on-persist`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Settle `accepted` once the intake made the delivery durable (default).
    OnPersist,
    /// Settle only at instance COMPLETED / drop (reject) at FAILED.
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

/// Typed view over the transport-specific channel properties, including the
/// secret-reference discipline for `username`/`password` (authored as a reference, resolved
/// just before connect — see [`AmqpChannelProperties::with_credentials`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpChannelProperties {
    /// Broker host (`host`, required inbound).
    pub host: String,
    /// Broker port (`port`, default `5672` / `5671` when `tls: true`).
    pub port: u16,
    /// Whether to negotiate TLS (`tls`, default `false`). This PLAINTEXT build fails a
    /// TLS connect closed (no TLS compiled) — the value still rides the fingerprint.
    pub tls: bool,
    /// Broker username — as authored a secret REFERENCE (`${ENV}` / `k8s:secret/…#key` /
    /// `env:NAME`); [`Self::with_credentials`] swaps in the resolved value before connect.
    pub username: Option<String>,
    /// Broker password — same secret-reference rule as `username`.
    pub password: Option<String>,
    /// Queue to consume from (`queue`; inbound requires `queue` OR `topic`).
    pub queue: Option<String>,
    /// Topic to consume from (`topic`; inbound requires `queue` OR `topic`).
    pub topic: Option<String>,
    /// AMQP prefetch / receiver link credit (`prefetch-count`, default `10`).
    pub prefetch_count: u32,
    /// Per-poll receive timeout in ms (`receive-timeout-ms`, default `1000`).
    pub receive_timeout_ms: u64,
    /// Engine-level ack semantics (`ack-mode`, default `on-persist`).
    pub ack_mode: AckMode,
    /// Per-channel singleton declaration (`singleton: true` / `consumer: exclusive`).
    pub singleton: bool,
}

impl AmqpChannelProperties {
    /// Read the typed properties off a channel definition. Mirrors
    /// `AmqpChannelProperties.from(ChannelConfig)`: defaults applied, integers validated,
    /// literal credentials refused (`SUTRA.STARTUP.REFUSED.HARDCODED_SECRET`), `host`
    /// required (`SUTRA.INBOUND.AMQP.CONFIG_INVALID`).
    pub fn from_definition(def: &ChannelDefinition) -> Result<AmqpChannelProperties, Diagnostic> {
        let props = &def.properties;
        let channel = &def.binding.channel_name;
        let host = non_blank(props.get("host")).ok_or_else(|| {
            Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!("amqp channel '{channel}' requires property 'host'"),
            )
        })?;
        let tls = parse_bool(props.get("tls"), false, channel)?;
        let default_port = if tls { DEFAULT_TLS_PORT } else { DEFAULT_PORT };
        let port = parse_u16(props.get("port"), default_port, "port", channel)?;
        if port == 0 {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!("amqp channel '{channel}' property 'port' is out of range: 0"),
            ));
        }
        let username = require_secret_ref(props.get("username"), "username", channel)?;
        let password = require_secret_ref(props.get("password"), "password", channel)?;
        let queue = non_blank(props.get("queue"));
        let topic = non_blank(props.get("topic"));
        let prefetch_count = parse_u32(
            props.get("prefetch-count"),
            DEFAULT_PREFETCH,
            "prefetch-count",
            channel,
        )?;
        let receive_timeout_ms = parse_u64(
            props.get("receive-timeout-ms"),
            DEFAULT_RECEIVE_TIMEOUT_MS,
            "receive-timeout-ms",
            channel,
        )?;
        let ack_mode = AckMode::parse(props.get("ack-mode").map(String::as_str));
        Ok(AmqpChannelProperties {
            host,
            port,
            tls,
            username,
            password,
            queue,
            topic,
            prefetch_count,
            receive_timeout_ms,
            ack_mode,
            singleton: def.singleton(),
        })
    }

    /// True when an inbound queue is declared.
    pub fn has_queue(&self) -> bool {
        self.queue.as_deref().is_some_and(|q| !q.trim().is_empty())
    }

    /// True when an inbound topic is declared.
    pub fn has_topic(&self) -> bool {
        self.topic.as_deref().is_some_and(|t| !t.trim().is_empty())
    }

    /// The inbound source address (queue wins over topic) — `None` when neither is declared.
    pub fn source_address(&self) -> Option<&str> {
        if self.has_queue() {
            self.queue.as_deref()
        } else if self.has_topic() {
            self.topic.as_deref()
        } else {
            None
        }
    }

    /// Returns a copy with the broker credentials replaced by their RESOLVED values — the
    /// wiring resolves the validated `${ENV}` / `k8s:secret/…` references into concrete
    /// secrets before a connection is opened.
    pub fn with_credentials(
        &self,
        resolved_user: Option<String>,
        resolved_pass: Option<String>,
    ) -> AmqpChannelProperties {
        AmqpChannelProperties {
            username: resolved_user,
            password: resolved_pass,
            ..self.clone()
        }
    }

    /// The wire connection URI fe2o3 opens — `amqp://[user:pass@]host:port` (or `amqps://`
    /// when `tls: true`; TLS is not compiled in this build, so an `amqps://` connect fails
    /// closed). Credentials, when present, ride the userinfo so fe2o3 negotiates SASL PLAIN.
    pub(crate) fn connection_uri(&self) -> String {
        let scheme = if self.tls { "amqps" } else { "amqp" };
        let mut uri = format!("{scheme}://");
        let user = self.username.as_deref().unwrap_or("").trim();
        if !user.is_empty() {
            uri.push_str(&pct_encode_userinfo(user));
            if let Some(pass) = self.password.as_deref() {
                uri.push(':');
                uri.push_str(&pct_encode_userinfo(pass.trim()));
            }
            uri.push('@');
        }
        uri.push_str(&self.host);
        uri.push(':');
        uri.push_str(&self.port.to_string());
        uri
    }
}

fn non_blank(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_u16(
    raw: Option<&String>,
    fallback: u16,
    key: &str,
    channel: &str,
) -> Result<u16, Diagnostic> {
    parse_int(raw, fallback, key, channel)
}

fn parse_u32(
    raw: Option<&String>,
    fallback: u32,
    key: &str,
    channel: &str,
) -> Result<u32, Diagnostic> {
    parse_int(raw, fallback, key, channel)
}

fn parse_u64(
    raw: Option<&String>,
    fallback: u64,
    key: &str,
    channel: &str,
) -> Result<u64, Diagnostic> {
    parse_int(raw, fallback, key, channel)
}

fn parse_int<T: std::str::FromStr>(
    raw: Option<&String>,
    fallback: T,
    key: &str,
    channel: &str,
) -> Result<T, Diagnostic> {
    match raw.map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(fallback),
        Some(v) => v.parse::<T>().map_err(|_| {
            Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "amqp channel '{channel}' property '{key}' has an invalid numeric value: '{v}'"
                ),
            )
        }),
    }
}

fn parse_bool(raw: Option<&String>, fallback: bool, channel: &str) -> Result<bool, Diagnostic> {
    match raw.map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(fallback),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => Err(Diagnostic::error(
            codes::INBOUND_CONFIG_INVALID,
            format!("amqp channel '{channel}' property 'tls' has an invalid boolean value: '{v}'"),
        )),
    }
}

/// Enforce the 15-factor secret discipline (mirrors the rabbitmq (AMQP 0.9.1) transport): broker
/// credentials must be references — `${ENV_VAR}` (optionally with `:default`),
/// `k8s:secret/<name>#<key>`, or the Rust engine's `env:NAME` form — never literals.
fn require_secret_ref(
    raw: Option<&String>,
    key: &str,
    channel: &str,
) -> Result<Option<String>, Diagnostic> {
    let Some(value) = non_blank(raw) else {
        return Ok(None);
    };
    if is_env_placeholder(&value) || is_k8s_secret_ref(&value) || value.starts_with("env:") {
        return Ok(Some(value));
    }
    Err(Diagnostic::error(
        codes::STARTUP_REFUSED_HARDCODED_SECRET,
        format!(
            "amqp channel '{channel}' property '{key}' must be a secret reference \
             (e.g. ${{ENV_VAR}} or k8s:secret/<name>#<key>); literal credentials are rejected."
        ),
    ))
}

fn is_env_placeholder(value: &str) -> bool {
    value.starts_with("${") && value.ends_with('}')
}

fn is_k8s_secret_ref(value: &str) -> bool {
    value.starts_with("k8s:secret/") && value.contains('#')
}

/// Percent-encode the sub-delims/reserved characters a userinfo segment must escape so the
/// wire URI stays well-formed (`:`, `@`, `/`, `%`, and space).
fn pct_encode_userinfo(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        match b {
            b':' | b'@' | b'/' | b'%' | b' ' | b'?' | b'#' | b'[' | b']' => {
                out.push('%');
                out.push_str(&format!("{b:02X}"));
            }
            _ => out.push(b as char),
        }
    }
    out
}

// ---- destination URIs (sink side) ---------------------------------------------------------

/// A parsed `amqp10://[user:pass@]host[:port]/<destination>[?type=topic]` destination — the
/// URI AUTHORITY names the broker, the PATH names the queue/topic, `?type=topic` forces a
/// topic. Unlike Kafka the broker rides the URI (per-authority connection cache);
/// credentials in the userinfo negotiate SASL PLAIN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpDestination {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    /// The queue or topic name (the link target address).
    pub address: String,
    /// True when `?type=topic` was supplied.
    pub is_topic: bool,
}

impl AmqpDestination {
    /// The wire connection URI fe2o3 opens for this destination.
    pub(crate) fn connection_uri(&self) -> String {
        let scheme = if self.tls { "amqps" } else { "amqp" };
        let mut uri = format!("{scheme}://");
        if let Some(user) = self.username.as_deref().filter(|u| !u.is_empty()) {
            uri.push_str(&pct_encode_userinfo(user));
            if let Some(pass) = self.password.as_deref() {
                uri.push(':');
                uri.push_str(&pct_encode_userinfo(pass));
            }
            uri.push('@');
        }
        uri.push_str(&self.host);
        uri.push(':');
        uri.push_str(&self.port.to_string());
        uri
    }
}

/// Parse an `amqp10://[user:pass@]host[:port]/<destination>[?type=topic]` destination.
/// Failures are PERMANENT (a retry can never fix a malformed URI).
pub fn parse_destination(destination: &str) -> Result<AmqpDestination, Diagnostic> {
    let malformed = |code: &'static str, detail: String| {
        Diagnostic::error(code, format!("amqp destination '{destination}' {detail}"))
    };
    let Some(scheme) = sutra_channels::sink::scheme_of(destination) else {
        return Err(malformed(
            codes::OUTBOUND_SEND_FAILED,
            "has no URI scheme".to_string(),
        ));
    };
    let tls = if scheme.eq_ignore_ascii_case(SCHEME) {
        false
    } else if scheme.eq_ignore_ascii_case(SCHEME_TLS) {
        true
    } else {
        return Err(malformed(
            codes::OUTBOUND_SEND_FAILED,
            format!("has scheme '{scheme}' (expected '{SCHEME}' or '{SCHEME_TLS}')"),
        ));
    };
    let rest = &destination[scheme.len() + "://".len()..];
    // Split off the query (?type=topic) then the authority / path.
    let (authority_path, query) = match rest.split_once('?') {
        Some((ap, q)) => (ap, Some(q)),
        None => (rest, None),
    };
    let (authority, path) = match authority_path.find('/') {
        Some(i) => (&authority_path[..i], &authority_path[i + 1..]),
        None => (authority_path, ""),
    };
    if authority.is_empty() {
        return Err(malformed(
            codes::OUTBOUND_SEND_FAILED,
            format!("has no host — expected {SCHEME}://<host>[:port]/<destination>"),
        ));
    }
    // userinfo@host:port
    let (userinfo, host_port) = match authority.rsplit_once('@') {
        Some((ui, hp)) => (Some(ui), hp),
        None => (None, authority),
    };
    let (username, password) = match userinfo {
        None => (None, None),
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(pct_decode(u)), Some(pct_decode(p))),
            None => (Some(pct_decode(ui)), None),
        },
    };
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => {
            let port = p.parse::<u16>().map_err(|_| {
                malformed(
                    codes::OUTBOUND_SEND_FAILED,
                    format!("has a non-numeric port '{p}'"),
                )
            })?;
            (h.to_string(), port)
        }
        None => (
            host_port.to_string(),
            if tls { DEFAULT_TLS_PORT } else { DEFAULT_PORT },
        ),
    };
    if host.is_empty() {
        return Err(malformed(
            codes::OUTBOUND_SEND_FAILED,
            "has an empty host".to_string(),
        ));
    }
    let address = path.trim_matches('/').to_string();
    if address.is_empty() {
        return Err(malformed(
            codes::OUTBOUND_DESTINATION_MISSING,
            format!("has no queue/topic name — expected {SCHEME}://<host>:<port>/<destination>"),
        ));
    }
    let is_topic = query_is_topic(query);
    Ok(AmqpDestination {
        host,
        port,
        tls,
        username,
        password,
        address,
        is_topic,
    })
}

/// True when the query string carries `type=topic` (ASCII case-insensitive).
fn query_is_topic(query: Option<&str>) -> bool {
    let Some(query) = query else { return false };
    for part in query.split('&') {
        if let Some((k, v)) = part.split_once('=') {
            if k.trim().eq_ignore_ascii_case("type") && v.trim().eq_ignore_ascii_case("topic") {
                return true;
            }
        }
    }
    false
}

/// Minimal percent-decoding for userinfo segments (`%XX` → byte, others verbatim).
fn pct_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "payments", "1.0.0");
        let binding = ChannelBinding::new("payments-in", namespace, DeploymentId::unresolved(), "");
        ChannelDefinition {
            binding,
            transport: Some("amqp".to_string()),
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
    fn properties_apply_documented_defaults() {
        let def = definition(&[("host", "localhost"), ("queue", "in-q")]);
        let props = AmqpChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.host, "localhost");
        assert_eq!(props.port, DEFAULT_PORT);
        assert!(!props.tls);
        assert_eq!(props.queue.as_deref(), Some("in-q"));
        assert_eq!(props.topic, None);
        assert_eq!(props.prefetch_count, DEFAULT_PREFETCH);
        assert_eq!(props.receive_timeout_ms, DEFAULT_RECEIVE_TIMEOUT_MS);
        assert_eq!(props.ack_mode, AckMode::OnPersist);
        assert!(!props.singleton);
        assert!(props.has_queue());
        assert_eq!(props.source_address(), Some("in-q"));
    }

    #[test]
    fn properties_read_overrides_tls_default_port_and_singleton() {
        let def = definition(&[
            ("host", "sb.example"),
            ("tls", "true"),
            ("topic", "orders/Subscriptions/s1"),
            ("prefetch-count", "50"),
            ("receive-timeout-ms", "250"),
            ("ack-mode", "On-Complete"),
            ("singleton", "true"),
        ]);
        let props = AmqpChannelProperties::from_definition(&def).expect("props");
        assert!(props.tls);
        assert_eq!(props.port, DEFAULT_TLS_PORT, "tls default port is 5671");
        assert_eq!(props.topic.as_deref(), Some("orders/Subscriptions/s1"));
        assert_eq!(props.prefetch_count, 50);
        assert_eq!(props.receive_timeout_ms, 250);
        assert_eq!(props.ack_mode, AckMode::OnComplete);
        assert!(props.singleton);
        assert_eq!(props.source_address(), Some("orders/Subscriptions/s1"));

        // `consumer: exclusive` is the other singleton spelling.
        let def = definition(&[("host", "h"), ("queue", "q"), ("consumer", "exclusive")]);
        assert!(
            AmqpChannelProperties::from_definition(&def)
                .expect("props")
                .singleton
        );
    }

    #[test]
    fn missing_host_fails_closed() {
        let def = definition(&[("queue", "in-q")]);
        let err = AmqpChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[test]
    fn literal_credentials_are_refused() {
        let def = definition(&[("host", "h"), ("queue", "q"), ("username", "admin")]);
        let err = AmqpChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::STARTUP_REFUSED_HARDCODED_SECRET);

        // A secret reference is accepted.
        let def = definition(&[("host", "h"), ("queue", "q"), ("username", "${SB_USER}")]);
        assert!(AmqpChannelProperties::from_definition(&def).is_ok());
    }

    #[test]
    fn invalid_bool_and_int_fail_closed() {
        let bad_bool = definition(&[("host", "h"), ("queue", "q"), ("tls", "yes")]);
        assert_eq!(
            AmqpChannelProperties::from_definition(&bad_bool)
                .unwrap_err()
                .code,
            codes::INBOUND_CONFIG_INVALID
        );
        let bad_int = definition(&[("host", "h"), ("queue", "q"), ("prefetch-count", "lots")]);
        assert_eq!(
            AmqpChannelProperties::from_definition(&bad_int)
                .unwrap_err()
                .code,
            codes::INBOUND_CONFIG_INVALID
        );
    }

    #[test]
    fn connection_uri_carries_credentials_as_userinfo() {
        let props = AmqpChannelProperties::from_definition(&definition(&[
            ("host", "broker"),
            ("port", "5672"),
            ("queue", "q"),
        ]))
        .expect("props")
        .with_credentials(Some("artemis".to_string()), Some("artemis".to_string()));
        assert_eq!(props.connection_uri(), "amqp://artemis:artemis@broker:5672");
    }

    #[test]
    fn destination_authority_is_the_broker_path_is_the_address() {
        let d = parse_destination("amqp10://broker:5672/payment-replies").expect("parse");
        assert_eq!(d.host, "broker");
        assert_eq!(d.port, 5672);
        assert!(!d.tls);
        assert_eq!(d.address, "payment-replies");
        assert!(!d.is_topic);
        assert_eq!(d.username, None);
        assert_eq!(d.connection_uri(), "amqp://broker:5672");
    }

    #[test]
    fn destination_userinfo_and_topic_query_and_default_port() {
        let d = parse_destination("amqp10://user:pw@broker/orders?type=topic").expect("parse");
        assert_eq!(d.host, "broker");
        assert_eq!(d.port, DEFAULT_PORT, "no explicit port ⇒ 5672");
        assert_eq!(d.username.as_deref(), Some("user"));
        assert_eq!(d.password.as_deref(), Some("pw"));
        assert_eq!(d.address, "orders");
        assert!(d.is_topic);
        assert_eq!(d.connection_uri(), "amqp://user:pw@broker:5672");

        // amqp10s ⇒ TLS + default 5671.
        let tls = parse_destination("amqp10s://broker/q").expect("parse");
        assert!(tls.tls);
        assert_eq!(tls.port, DEFAULT_TLS_PORT);
    }

    #[test]
    fn malformed_destinations_are_permanent_errors() {
        // no address ⇒ DESTINATION_MISSING
        let err = parse_destination("amqp10://broker:5672/").unwrap_err();
        assert_eq!(err.code, codes::OUTBOUND_DESTINATION_MISSING);
        // wrong scheme / no scheme ⇒ SEND_FAILED
        for bad in [
            "amqp://broker/q",
            "rabbitmq://broker/q",
            "kafka://t",
            "no-scheme",
        ] {
            let err = parse_destination(bad).unwrap_err();
            assert_eq!(err.code, codes::OUTBOUND_SEND_FAILED, "for '{bad}'");
        }
    }
}
