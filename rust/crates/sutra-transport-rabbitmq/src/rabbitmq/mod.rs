//! RabbitMQ transport — the AMQP 0.9.1 broker pair behind the transport seams:
//! [`source::RabbitMqTriggerSource`] implements [`sutra_channels::source::TriggerSource`] (inbound
//! consumer, leader-gated for `singleton: true` channels) and
//! [`sink::RabbitMqMessageSink`] implements [`sutra_channels::sink::MessageSink`] for the
//! `rabbitmq://` / `amqp://` destination schemes.
//!
//! The client crate is `lapin` (raw AMQP 0.9.1). FROZEN wire strings — the cross-broker
//! shape IS the requirement:
//!
//! - the outbox key rides the AMQP **`message-id`** property (RabbitMQ does NOT use the
//!   `sutra-outbox-key` header the other four brokers share);
//! - published messages are persistent (**`delivery-mode = 2`**);
//! - inbound AMQP standard properties project into the header map under the
//!   **`x-amqp-*`** names;
//! - the CloudEvents binary binding for AMQP 0.9.1 uses the **`cloudEvents:`** attribute
//!   prefix — the CE projection happens upstream (dispatcher-side); this sink carries
//!   headers verbatim.

pub mod sink;
pub mod source;

pub use sink::RabbitMqMessageSink;
pub use source::{RabbitMqSourceConfig, RabbitMqTriggerSource};

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact RabbitMQ diagnostic codes
/// this module raises.
pub mod codes {
    pub const INBOUND_CONNECTION_FAILED: &str = "SUTRA.INBOUND.RABBITMQ.CONNECTION_FAILED";
    pub const INBOUND_QUEUE_MISSING: &str = "SUTRA.INBOUND.RABBITMQ.QUEUE_MISSING";
    pub const INBOUND_DELIVER_FAILED: &str = "SUTRA.INBOUND.RABBITMQ.DELIVER_FAILED";
    pub const INBOUND_ACK_FAILED: &str = "SUTRA.INBOUND.RABBITMQ.ACK_FAILED";
    /// Authored inbound-auth config is invalid (bad scheme, missing/unresolvable ref).
    pub const INBOUND_CONFIG_INVALID: &str = "SUTRA.INBOUND.RABBITMQ.CONFIG_INVALID";
    /// An inbound delivery presented a missing/wrong per-message credential; the
    /// delivery is `basic.nack(requeue=false)`-dropped and never dispatched.
    pub const INBOUND_AUTH_REJECTED: &str = "SUTRA.INBOUND.RABBITMQ.AUTH_REJECTED";
    /// `inbound-auth.scheme=mtls` is per-message UNSUPPORTED; a one-time boot WARN,
    /// then allow-through (broker-level TLS still applies).
    pub const INBOUND_MTLS_UNSUPPORTED: &str = "SUTRA.INBOUND.RABBITMQ.MTLS_UNSUPPORTED";

    pub const OUTBOUND_CONNECTION_FAILED: &str = "SUTRA.OUTBOUND.RABBITMQ.CONNECTION_FAILED";
    pub const OUTBOUND_PUBLISH_FAILED: &str = "SUTRA.OUTBOUND.RABBITMQ.PUBLISH_FAILED";

    /// `SUTRA.OUTBOUND.SEND.FAILED` — malformed destination (permanent posture).
    pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.SEND.FAILED";
    /// `SUTRA.STARTUP.REFUSED.HARDCODED_SECRET` — literal broker credentials in
    /// channel YAML are refused (15-factor Factor 3).
    pub const STARTUP_REFUSED_HARDCODED_SECRET: &str = "SUTRA.STARTUP.REFUSED.HARDCODED_SECRET";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "rabbitmq";

/// Effective ack modes of a broker channel — parsed leniently:
/// `on-complete` (ASCII case-insensitive) opts in, anything else is `on-persist`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Ack once the intake made the delivery durable (broker default).
    OnPersist,
    /// Ack only at instance COMPLETED / nack(drop) at FAILED.
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

/// Typed view over the transport-specific channel properties — the RabbitMQ
/// channel-properties record (same keys, same defaults, same secret-reference
/// discipline for `username` / `password`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RabbitMqChannelProperties {
    /// Broker host (`host`, default `localhost`).
    pub host: String,
    /// Broker port (`port`, default `5672`).
    pub port: u16,
    /// AMQP virtual host (`virtual-host`, default `/`).
    pub virtual_host: String,
    /// Broker username — as authored this is a secret REFERENCE (`${ENV}` /
    /// `k8s:secret/…#key` / `env:NAME`); [`Self::with_credentials`] swaps in the
    /// resolved value before a connection is opened.
    pub username: Option<String>,
    /// Broker password — same secret-reference rule as `username`.
    pub password: Option<String>,
    /// Queue to consume from (`queue`, required for inbound).
    pub queue: String,
    /// Exchange for outbound channel declarations (`exchange`, default `""`); the sink
    /// derives its exchange from the destination URI path.
    pub exchange: String,
    /// AMQP `basic.qos` prefetch window per consumer (`prefetch-count`, default `10`).
    pub prefetch_count: u16,
    /// Engine-level ack semantics (`ack-mode`, default `on-persist`).
    pub ack_mode: AckMode,
    /// Per-channel singleton declaration (`singleton: true` / `consumer: exclusive`).
    pub singleton: bool,
}

impl RabbitMqChannelProperties {
    pub const DEFAULT_PORT: u16 = 5672;
    pub const DEFAULT_HOST: &'static str = "localhost";
    pub const DEFAULT_VHOST: &'static str = "/";
    pub const DEFAULT_PREFETCH: u16 = 10;

    /// Read the typed properties off a channel definition. Mirrors
    /// `RabbitMqChannelProperties.from(ChannelConfig)`: defaults applied, integers
    /// validated, literal credentials refused
    /// (`SUTRA.STARTUP.REFUSED.HARDCODED_SECRET`).
    pub fn from_definition(
        def: &ChannelDefinition,
    ) -> Result<RabbitMqChannelProperties, Diagnostic> {
        let props = &def.properties;
        let channel = &def.binding.channel_name;
        let host = non_blank(props.get("host")).unwrap_or_else(|| Self::DEFAULT_HOST.to_string());
        let port = parse_u16(props.get("port"), Self::DEFAULT_PORT, "port", channel)?;
        if port == 0 {
            return Err(Diagnostic::error(
                sutra_channels::codes::PARSE_YAML_PARSE_ERROR,
                format!("rabbitmq channel '{channel}' property 'port' is out of range: 0"),
            ));
        }
        let virtual_host =
            non_blank(props.get("virtual-host")).unwrap_or_else(|| Self::DEFAULT_VHOST.to_string());
        let username = require_secret_ref(props.get("username"), "username", channel)?;
        let password = require_secret_ref(props.get("password"), "password", channel)?;
        let queue = non_blank(props.get("queue")).unwrap_or_default();
        let exchange = non_blank(props.get("exchange")).unwrap_or_default();
        let prefetch_count = parse_u16(
            props.get("prefetch-count"),
            Self::DEFAULT_PREFETCH,
            "prefetch-count",
            channel,
        )?;
        // The startup-orchestrator resolution (`ChannelDefinition::effective_ack_mode`):
        // a declared `ack-mode:` wins, the broker default is `on-persist`.
        let ack_mode = AckMode::parse(Some(def.effective_ack_mode()));
        Ok(RabbitMqChannelProperties {
            host,
            port,
            virtual_host,
            username,
            password,
            queue,
            exchange,
            prefetch_count,
            ack_mode,
            singleton: def.singleton(),
        })
    }

    /// True when an inbound queue is declared.
    pub fn has_queue(&self) -> bool {
        !self.queue.trim().is_empty()
    }

    /// Returns a copy with the broker credentials replaced by their RESOLVED values —
    /// the wiring resolves the validated `${ENV}` / `k8s:secret/…` references into
    /// concrete secrets before a connection is opened.
    pub fn with_credentials(
        &self,
        resolved_user: Option<String>,
        resolved_pass: Option<String>,
    ) -> RabbitMqChannelProperties {
        RabbitMqChannelProperties {
            username: resolved_user,
            password: resolved_pass,
            ..self.clone()
        }
    }

    /// The `amqp://` connection URI for this broker (credentials percent-encoded, the
    /// default `/` vhost elided per the AMQP URI spec).
    pub(crate) fn connection_uri(&self) -> String {
        amqp_connection_uri(
            &self.host,
            self.port,
            &self.virtual_host,
            self.username.as_deref(),
            self.password.as_deref(),
        )
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
    match raw.map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(fallback),
        Some(v) => v.parse::<u16>().map_err(|_| {
            Diagnostic::error(
                sutra_channels::codes::PARSE_YAML_PARSE_ERROR,
                format!(
                    "rabbitmq channel '{channel}' property '{key}' must be an integer in \
                     0..=65535, got '{v}'"
                ),
            )
        }),
    }
}

/// Enforce the 15-factor secret discipline (the secret-reference validation):
/// broker credentials must be references —
/// `${ENV_VAR}` (optionally with `:default`), `k8s:secret/<name>#<key>`, or the Rust
/// engine's `env:NAME` form — never literals.
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
            "rabbitmq channel '{channel}' property '{key}' must be a secret reference \
             (e.g. ${{ENV_VAR}} or k8s:secret/<name>#<key>); literal credentials are rejected."
        ),
    ))
}

/// `^\$\{[A-Z_][A-Z0-9_]*(:.*)?}$` without a regex dependency.
fn is_env_placeholder(value: &str) -> bool {
    let Some(inner) = value.strip_prefix("${").and_then(|v| v.strip_suffix('}')) else {
        return false;
    };
    let name = inner.split_once(':').map_or(inner, |(n, _)| n);
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_uppercase() || first == '_')
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// `^k8s:secret/[^#]+#.+$` without a regex dependency.
fn is_k8s_secret_ref(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("k8s:secret/") else {
        return false;
    };
    match rest.split_once('#') {
        Some((name, k)) => !name.is_empty() && !k.is_empty(),
        None => false,
    }
}

// ---- destination URIs (sink side) ---------------------------------------------------------

/// A parsed `rabbitmq://` / `amqp://` destination — the URI AUTHORITY names the broker,
/// the PATH carries the AMQP routing (the message-sink shape):
/// one path segment publishes to the default exchange with that segment as the routing
/// key; two or more make the first the exchange and the remainder the routing key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RabbitMqDestination {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub exchange: String,
    pub routing_key: String,
}

impl RabbitMqDestination {
    /// Connection cache key — one connection per distinct broker endpoint
    /// (the authority key: `userinfo@host:port`).
    pub(crate) fn authority_key(&self) -> String {
        let user = self.username.as_deref().unwrap_or("");
        format!("{user}@{}:{}", self.host, self.port)
    }

    /// The `amqp://` connection URI for this destination (default `/` vhost).
    pub(crate) fn connection_uri(&self) -> String {
        amqp_connection_uri(
            &self.host,
            self.port,
            RabbitMqChannelProperties::DEFAULT_VHOST,
            self.username.as_deref(),
            self.password.as_deref(),
        )
    }
}

/// Parse a `rabbitmq://[user[:pass]@]host[:port]/[<exchange>/]<routingKey>` destination.
/// Failures are PERMANENT (a retry can never fix a malformed URI).
pub fn parse_destination(destination: &str) -> Result<RabbitMqDestination, Diagnostic> {
    let malformed = |detail: &str| {
        Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("rabbitmq destination '{destination}' {detail}"),
        )
    };
    let Some(scheme) = sutra_channels::sink::scheme_of(destination) else {
        return Err(malformed("has no URI scheme"));
    };
    if !scheme.eq_ignore_ascii_case("rabbitmq") && !scheme.eq_ignore_ascii_case("amqp") {
        return Err(malformed(&format!(
            "has scheme '{scheme}' (expected 'rabbitmq' or 'amqp')"
        )));
    }
    let rest = &destination[scheme.len() + "://".len()..];
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, ""),
    };

    // authority = [userinfo@]host[:port]
    let (userinfo, host_port) = match authority.rfind('@') {
        Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
        None => (None, authority),
    };
    let (username, password) = match userinfo {
        None => (None, None),
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(percent_decode(u)), Some(percent_decode(p))),
            None => (Some(percent_decode(ui)), None),
        },
    };
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => {
            let port: u16 = p
                .parse()
                .map_err(|_| malformed(&format!("has an invalid port '{p}'")))?;
            (h, port)
        }
        None => (host_port, RabbitMqChannelProperties::DEFAULT_PORT),
    };
    let host = if host.is_empty() {
        RabbitMqChannelProperties::DEFAULT_HOST.to_string()
    } else {
        host.to_string()
    };

    // path = [<exchange>/]<routingKey> — one segment ⇒ default exchange.
    let (exchange, routing_key) = match path.find('/') {
        Some(i) => (path[..i].to_string(), path[i + 1..].to_string()),
        None => (String::new(), path.to_string()),
    };
    if exchange.is_empty() && routing_key.is_empty() {
        return Err(malformed(
            "has no exchange or routing key — expected \
             rabbitmq://<broker>[:port]/[<exchange>/]<routingKey>",
        ));
    }
    Ok(RabbitMqDestination {
        host,
        port,
        username,
        password,
        exchange,
        routing_key,
    })
}

// ---- AMQP URI assembly ---------------------------------------------------------------------

/// Build the `amqp://` connection URI lapin consumes. Credentials and non-default
/// vhosts are percent-encoded; the default `/` vhost is elided (empty path = `/`).
fn amqp_connection_uri(
    host: &str,
    port: u16,
    vhost: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> String {
    let mut uri = String::from("amqp://");
    match (username, password) {
        (Some(u), Some(p)) => {
            uri.push_str(&percent_encode(u));
            uri.push(':');
            uri.push_str(&percent_encode(p));
            uri.push('@');
        }
        (Some(u), None) => {
            uri.push_str(&percent_encode(u));
            uri.push('@');
        }
        _ => {}
    }
    uri.push_str(host);
    uri.push(':');
    uri.push_str(&port.to_string());
    if vhost != RabbitMqChannelProperties::DEFAULT_VHOST && !vhost.is_empty() {
        uri.push('/');
        uri.push_str(&percent_encode(vhost));
    }
    uri
}

/// Percent-encode everything outside RFC 3986 `unreserved` — safe for userinfo and
/// single path segments.
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Minimal `%XX` percent-decoding (malformed escapes pass through verbatim).
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if let (Some(hi), Some(lo)) = (
                bytes.get(i + 1).and_then(|b| (*b as char).to_digit(16)),
                bytes.get(i + 2).and_then(|b| (*b as char).to_digit(16)),
            ) {
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

/// Stringify one AMQP field value — UTF-8 for
/// string/byte values, display forms for scalars, debug form for the exotic rest.
pub(crate) fn stringify_field(value: &lapin::types::AMQPValue) -> String {
    use lapin::types::AMQPValue as V;
    match value {
        V::LongString(s) => String::from_utf8_lossy(s.as_bytes()).into_owned(),
        V::ShortString(s) => s.as_str().to_string(),
        V::ByteArray(b) => String::from_utf8_lossy(b.as_slice()).into_owned(),
        V::Boolean(b) => b.to_string(),
        V::ShortShortInt(v) => v.to_string(),
        V::ShortShortUInt(v) => v.to_string(),
        V::ShortInt(v) => v.to_string(),
        V::ShortUInt(v) => v.to_string(),
        V::LongInt(v) => v.to_string(),
        V::LongUInt(v) => v.to_string(),
        V::LongLongInt(v) => v.to_string(),
        V::Float(v) => v.to_string(),
        V::Double(v) => v.to_string(),
        V::Timestamp(v) => v.to_string(),
        other => format!("{other:?}"),
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
            transport: Some("rabbitmq".to_string()),
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
        let def = definition(&[("queue", "transfer-queue-q")]);
        let props = RabbitMqChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.host, "localhost");
        assert_eq!(props.port, 5672);
        assert_eq!(props.virtual_host, "/");
        assert_eq!(props.username, None);
        assert_eq!(props.password, None);
        assert_eq!(props.queue, "transfer-queue-q");
        assert_eq!(props.exchange, "");
        assert_eq!(props.prefetch_count, 10);
        assert_eq!(props.ack_mode, AckMode::OnPersist);
        assert!(!props.singleton);
        assert!(props.has_queue());
    }

    #[test]
    fn properties_read_overrides_and_singleton_flag() {
        let def = definition(&[
            ("queue", "q1"),
            ("host", "broker.internal"),
            ("port", "5673"),
            ("virtual-host", "/payments"),
            ("prefetch-count", "25"),
            ("ack-mode", "On-Complete"),
            ("singleton", "true"),
            ("username", "${RABBITMQ_USERNAME}"),
            ("password", "${RABBITMQ_PASSWORD}"),
        ]);
        let props = RabbitMqChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.host, "broker.internal");
        assert_eq!(props.port, 5673);
        assert_eq!(props.virtual_host, "/payments");
        assert_eq!(props.prefetch_count, 25);
        assert_eq!(props.ack_mode, AckMode::OnComplete);
        assert!(props.singleton);
        assert_eq!(props.username.as_deref(), Some("${RABBITMQ_USERNAME}"));

        // `consumer: exclusive` is the other singleton spelling.
        let def = definition(&[("queue", "q1"), ("consumer", "exclusive")]);
        assert!(
            RabbitMqChannelProperties::from_definition(&def)
                .expect("props")
                .singleton
        );
    }

    #[test]
    fn literal_credentials_are_refused_with_the_hardcoded_secret_code() {
        let def = definition(&[("queue", "q"), ("username", "admin")]);
        let err = RabbitMqChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::STARTUP_REFUSED_HARDCODED_SECRET);

        // Reference forms all pass.
        for reference in [
            "${RABBITMQ_PASSWORD}",
            "${RABBITMQ_PASSWORD:guest}",
            "k8s:secret/broker-creds#password",
            "env:RABBITMQ_PASSWORD",
        ] {
            let def = definition(&[("queue", "q"), ("password", reference)]);
            assert!(
                RabbitMqChannelProperties::from_definition(&def).is_ok(),
                "reference form '{reference}' must be accepted"
            );
        }
        // Lowercase env names are NOT the `${ENV}` form (the regex only matches uppercase).
        let def = definition(&[("queue", "q"), ("password", "${lowercase}")]);
        assert!(RabbitMqChannelProperties::from_definition(&def).is_err());
    }

    #[test]
    fn invalid_integers_fail_closed() {
        for (key, value) in [
            ("port", "not-a-port"),
            ("port", "0"),
            ("prefetch-count", "-1"),
        ] {
            let def = definition(&[("queue", "q"), (key, value)]);
            assert!(
                RabbitMqChannelProperties::from_definition(&def).is_err(),
                "{key}={value} must be rejected"
            );
        }
    }

    #[test]
    fn with_credentials_swaps_only_the_credentials() {
        let def = definition(&[("queue", "q"), ("username", "${U}"), ("password", "${P}")]);
        let props = RabbitMqChannelProperties::from_definition(&def).expect("props");
        let resolved =
            props.with_credentials(Some("real-user".to_string()), Some("real-pass".to_string()));
        assert_eq!(resolved.username.as_deref(), Some("real-user"));
        assert_eq!(resolved.password.as_deref(), Some("real-pass"));
        assert_eq!(resolved.queue, props.queue);
        assert_eq!(resolved.port, props.port);
    }

    #[test]
    fn connection_uri_percent_encodes_credentials_and_vhost() {
        let def = definition(&[("queue", "q"), ("virtual-host", "/pay ments")]);
        let props = RabbitMqChannelProperties::from_definition(&def)
            .expect("props")
            .with_credentials(Some("u@er".to_string()), Some("p:a/ss".to_string()));
        assert_eq!(
            props.connection_uri(),
            "amqp://u%40er:p%3Aa%2Fss@localhost:5672/%2Fpay%20ments"
        );

        // Default vhost is elided.
        let def = definition(&[("queue", "q")]);
        let props = RabbitMqChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.connection_uri(), "amqp://localhost:5672");
    }

    #[test]
    fn destination_single_segment_targets_the_default_exchange() {
        let d = parse_destination("rabbitmq://broker:5672/payments.out").expect("parse");
        assert_eq!(d.host, "broker");
        assert_eq!(d.port, 5672);
        assert_eq!(d.exchange, "");
        assert_eq!(d.routing_key, "payments.out");
        assert_eq!(d.username, None);
    }

    #[test]
    fn destination_two_segments_split_into_exchange_and_routing_key() {
        let d = parse_destination("rabbitmq://broker/payments-ex/route-1").expect("parse");
        assert_eq!(d.port, RabbitMqChannelProperties::DEFAULT_PORT);
        assert_eq!(d.exchange, "payments-ex");
        assert_eq!(d.routing_key, "route-1");

        // Explicit empty first segment = the default exchange (documented example).
        let d = parse_destination("rabbitmq://broker:5672//route-1").expect("parse");
        assert_eq!(d.exchange, "");
        assert_eq!(d.routing_key, "route-1");
    }

    #[test]
    fn destination_credentials_come_from_the_uri() {
        let d = parse_destination("rabbitmq://user:pa%40ss@host:5673/q").expect("parse");
        assert_eq!(d.username.as_deref(), Some("user"));
        assert_eq!(d.password.as_deref(), Some("pa@ss"));
        assert_eq!(d.authority_key(), "user@host:5673");
        assert_eq!(d.connection_uri(), "amqp://user:pa%40ss@host:5673");

        // amqp:// is the sibling scheme the sink also claims.
        assert!(parse_destination("amqp://host/q").is_ok());
    }

    #[test]
    fn malformed_destinations_are_permanent_errors() {
        for bad in [
            "rabbitmq://broker:5672/",
            "rabbitmq://broker:5672",
            "kafka://broker/topic",
            "no-scheme",
            "rabbitmq://broker:not-a-port/q",
        ] {
            let err = parse_destination(bad).unwrap_err();
            assert_eq!(err.code, codes::OUTBOUND_SEND_FAILED, "for '{bad}'");
        }
    }

    #[test]
    fn percent_roundtrip() {
        assert_eq!(percent_encode("a b@c/d:e"), "a%20b%40c%2Fd%3Ae");
        assert_eq!(percent_decode("a%20b%40c%2Fd%3Ae"), "a b@c/d:e");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("bad%zz"), "bad%zz");
    }
}
