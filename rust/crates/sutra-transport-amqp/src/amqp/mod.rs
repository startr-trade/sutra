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
//! (`amqp10s` for TLS) — self-documenting and collision-free. A plaintext connection is opened
//! from a standard `amqp://` URI; a TLS one (`tls: true`, `amqp10s://`) is secured natively by
//! [`tls`] and verified by default.

pub mod sink;
pub mod source;
pub mod tls;

pub use sink::AmqpMessageSink;
pub use source::{AmqpSourceConfig, AmqpTriggerSource};
pub use tls::AmqpTlsProperties;

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
    /// The consumer's TLS trust settings could not be loaded, or the handshake failed (an
    /// untrusted CA or a host-name mismatch, typically).
    pub const INBOUND_BROKER_TLS_FAILED: &str = "SUTRA.INBOUND.AMQP.BROKER_TLS_FAILED";
    /// The broker refused the consumer's SASL credentials.
    pub const INBOUND_BROKER_AUTH_FAILED: &str = "SUTRA.INBOUND.AMQP.BROKER_AUTH_FAILED";

    pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.AMQP.SEND_FAILED";
    pub const OUTBOUND_DESTINATION_MISSING: &str = "SUTRA.OUTBOUND.AMQP.DESTINATION_MISSING";
    pub const OUTBOUND_CONFIG_INVALID: &str = "SUTRA.OUTBOUND.AMQP.CONFIG_INVALID";
    /// The sink's TLS trust settings could not be loaded, or the handshake failed.
    pub const OUTBOUND_BROKER_TLS_FAILED: &str = "SUTRA.OUTBOUND.AMQP.BROKER_TLS_FAILED";
    /// The broker refused the sink's SASL credentials.
    pub const OUTBOUND_BROKER_AUTH_FAILED: &str = "SUTRA.OUTBOUND.AMQP.BROKER_AUTH_FAILED";

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
#[derive(Clone, PartialEq, Eq)]
pub struct AmqpChannelProperties {
    /// Broker host (`host`, required inbound).
    pub host: String,
    /// Broker port (`port`, default `5672` / `5671` when `tls: true`).
    pub port: u16,
    /// Whether to negotiate TLS (`tls`, default `false`).
    pub tls: bool,
    /// The TLS trust settings (`tls-ca-file`, `tls-ca-pem`, `tls-insecure-skip-verify`).
    /// Authoring one with `tls: false` is refused, never ignored.
    pub tls_settings: AmqpTlsProperties,
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

/// The connection properties that may be an environment or secret reference, including the TLS
/// CA path, which differs between environments the way a host does. Credentials, `tls-ca-pem` and
/// `inbound-auth` are secrets: they must be written as a whole reference and resolve separately,
/// later in the wiring.
const REFERENCE_PROPERTIES: [&str; 5] = ["host", "port", "queue", "topic", "tls-ca-file"];

/// Credentials stay out of debug output: after [`AmqpChannelProperties::with_credentials`] these
/// fields hold resolved secrets.
impl std::fmt::Debug for AmqpChannelProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpChannelProperties")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("tls_settings", &self.tls_settings)
            .field("queue", &self.queue)
            .field("topic", &self.topic)
            .field("singleton", &self.singleton)
            .finish_non_exhaustive()
    }
}

impl AmqpChannelProperties {
    /// [`Self::from_definition`] with `${NAME}`, `${NAME:default}`, `env:NAME` and `secret:` style
    /// references in the connection properties resolved first. This is what the wiring runs on: an
    /// unresolved reference is [`codes::INBOUND_CONFIG_INVALID`] and the channel does not start.
    pub fn resolved(
        def: &ChannelDefinition,
        resolver: sutra_transport_spi::EnvRefResolver,
    ) -> Result<AmqpChannelProperties, Diagnostic> {
        let resolved = sutra_transport_spi::resolve_channel_properties(
            def,
            &REFERENCE_PROPERTIES,
            codes::INBOUND_CONFIG_INVALID,
            resolver,
        )?;
        sutra_transport_spi::require_resolved_port(
            def,
            &resolved,
            "port",
            codes::INBOUND_CONFIG_INVALID,
        )?;
        AmqpChannelProperties::from_definition(&resolved)
    }

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
        let tls = parse_bool(props.get("tls"), false, "tls", channel)?;
        let tls_settings = parse_tls_settings(props, tls, channel)?;
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
            tls_settings,
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

    /// Returns a copy with the `tls-ca-pem` reference replaced by the PEM text it resolves to.
    pub fn with_resolved_ca_pem(mut self, resolved_pem: Option<String>) -> AmqpChannelProperties {
        self.tls_settings.ca_pem = resolved_pem;
        self
    }

    /// The plaintext wire connection URI fe2o3 opens — `amqp://[user:pass@]host:port`.
    /// Credentials, when present, ride the userinfo so fe2o3 negotiates SASL PLAIN. A TLS
    /// connection is not opened from a URI: see [`tls::open_tls_connection`].
    pub(crate) fn connection_uri(&self) -> String {
        let mut uri = String::from("amqp://");
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

fn parse_bool(
    raw: Option<&String>,
    fallback: bool,
    key: &str,
    channel: &str,
) -> Result<bool, Diagnostic> {
    match raw.map(|v| v.trim()).filter(|v| !v.is_empty()) {
        None => Ok(fallback),
        Some(v) if v.eq_ignore_ascii_case("true") => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") => Ok(false),
        Some(v) => Err(Diagnostic::error(
            codes::INBOUND_CONFIG_INVALID,
            format!(
                "amqp channel '{channel}' property '{key}' has an invalid boolean value: '{v}'"
            ),
        )),
    }
}

/// Read the TLS trust settings. A CA or a verification opt-out on a plaintext channel is a
/// contradiction that would otherwise silently connect without TLS, so it fails closed, as do two
/// competing CA sources.
fn parse_tls_settings(
    props: &std::collections::BTreeMap<String, String>,
    tls: bool,
    channel: &str,
) -> Result<AmqpTlsProperties, Diagnostic> {
    let settings = AmqpTlsProperties {
        ca_file: non_blank(props.get("tls-ca-file")),
        ca_pem: require_secret_ref(props.get("tls-ca-pem"), "tls-ca-pem", channel)?,
        insecure_skip_verify: parse_bool(
            props.get("tls-insecure-skip-verify"),
            false,
            "tls-insecure-skip-verify",
            channel,
        )?,
    };
    if settings.is_configured() && !tls {
        return Err(Diagnostic::error(
            codes::INBOUND_CONFIG_INVALID,
            format!(
                "amqp channel '{channel}' sets a tls-* property but not 'tls: true'; the \
                 connection would be plaintext"
            ),
        ));
    }
    if settings.ca_file.is_some() && settings.ca_pem.is_some() {
        return Err(Diagnostic::error(
            codes::INBOUND_CONFIG_INVALID,
            format!("amqp channel '{channel}' sets both 'tls-ca-file' and 'tls-ca-pem'"),
        ));
    }
    Ok(settings)
}

/// Enforce the 15-factor secret discipline (mirrors the rabbitmq (AMQP 0.9.1) transport): broker
/// credentials must be references — any form the engine's reference resolver resolves
/// (`env:NAME`, `secret:KEY`, a registered secret-store scheme, `${NAME}`, `${secret:KEY}`, each
/// placeholder optionally with `:default`), or `k8s:secret/<name>#<key>` — never literals.
fn require_secret_ref(
    raw: Option<&String>,
    key: &str,
    channel: &str,
) -> Result<Option<String>, Diagnostic> {
    let Some(value) = non_blank(raw) else {
        return Ok(None);
    };
    if sutra_transport_spi::is_secret_reference(&value) {
        return Ok(Some(value));
    }
    Err(Diagnostic::error(
        codes::STARTUP_REFUSED_HARDCODED_SECRET,
        format!(
            "amqp channel '{channel}' property '{key}' must be a secret reference \
             (e.g. secret:KEY, ${{secret:KEY}} or ${{ENV_VAR}}); literal credentials are rejected."
        ),
    ))
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
#[derive(Clone, PartialEq, Eq)]
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

/// A destination's userinfo may hold a resolved password, so it stays out of debug output.
impl std::fmt::Debug for AmqpDestination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpDestination")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("address", &self.address)
            .field("is_topic", &self.is_topic)
            .finish_non_exhaustive()
    }
}

impl AmqpDestination {
    /// The plaintext wire connection URI fe2o3 opens for this destination.
    pub(crate) fn connection_uri(&self) -> String {
        let mut uri = String::from("amqp://");
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

/// A destination URI with any userinfo removed: the only form of it fit for a diagnostic, because
/// `user:password@` may hold a resolved secret. A string that has no authority is returned as is.
pub(crate) fn redact_destination(destination: &str) -> String {
    let Some((scheme, rest)) = destination.split_once("://") else {
        return destination.to_string();
    };
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((_, host)) => format!("{scheme}://{host}{tail}"),
        None => destination.to_string(),
    }
}

/// Parse an `amqp10://[user:pass@]host[:port]/<destination>[?type=topic]` destination.
/// Failures are PERMANENT (a retry can never fix a malformed URI).
pub fn parse_destination(destination: &str) -> Result<AmqpDestination, Diagnostic> {
    let malformed = |code: &'static str, detail: String| {
        Diagnostic::error(
            code,
            format!(
                "amqp destination '{}' {detail}",
                redact_destination(destination)
            ),
        )
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

        for literal in ["s3cr3t:pass", "secret:", "pre${SB_USER}"] {
            let def = definition(&[("host", "h"), ("queue", "q"), ("password", literal)]);
            let err = AmqpChannelProperties::from_definition(&def).unwrap_err();
            assert_eq!(
                err.code,
                codes::STARTUP_REFUSED_HARDCODED_SECRET,
                "{literal}"
            );
        }

        // Every secret-reference form is accepted.
        for reference in [
            "${SB_USER}",
            "${SB_USER:guest}",
            "env:SB_USER",
            "secret:broker-user",
            "${secret:broker-user}",
            "k8s:secret/broker-creds#user",
        ] {
            let def = definition(&[("host", "h"), ("queue", "q"), ("username", reference)]);
            assert!(
                AmqpChannelProperties::from_definition(&def).is_ok(),
                "{reference}"
            );
        }
    }

    #[test]
    fn tls_settings_default_to_verified_and_read_their_properties() {
        let props =
            AmqpChannelProperties::from_definition(&definition(&[("host", "h"), ("tls", "true")]))
                .expect("props");
        assert_eq!(props.tls_settings, AmqpTlsProperties::default());
        assert!(!props.tls_settings.insecure_skip_verify);

        let props = AmqpChannelProperties::from_definition(&definition(&[
            ("host", "h"),
            ("tls", "true"),
            ("tls-ca-file", "/etc/ca.pem"),
            ("tls-insecure-skip-verify", "TRUE"),
        ]))
        .expect("props");
        assert_eq!(props.tls_settings.ca_file.as_deref(), Some("/etc/ca.pem"));
        assert!(props.tls_settings.insecure_skip_verify);
    }

    #[test]
    fn tls_settings_that_cannot_take_effect_fail_closed() {
        for (props, code) in [
            (
                vec![("host", "h"), ("tls-ca-file", "/etc/ca.pem")],
                codes::INBOUND_CONFIG_INVALID,
            ),
            (
                vec![("host", "h"), ("tls-insecure-skip-verify", "true")],
                codes::INBOUND_CONFIG_INVALID,
            ),
            (
                vec![
                    ("host", "h"),
                    ("tls", "true"),
                    ("tls-ca-file", "/etc/ca.pem"),
                    ("tls-ca-pem", "secret:CA"),
                ],
                codes::INBOUND_CONFIG_INVALID,
            ),
            (
                vec![
                    ("host", "h"),
                    ("tls", "true"),
                    ("tls-insecure-skip-verify", "maybe"),
                ],
                codes::INBOUND_CONFIG_INVALID,
            ),
            (
                vec![("host", "h"), ("tls", "true"), ("tls-ca-pem", "-----BEGIN")],
                codes::STARTUP_REFUSED_HARDCODED_SECRET,
            ),
        ] {
            let err = AmqpChannelProperties::from_definition(&definition(&props)).unwrap_err();
            assert_eq!(err.code, code, "{props:?}");
        }
    }

    #[test]
    fn debug_output_and_diagnostics_never_carry_the_credentials() {
        let props = AmqpChannelProperties::from_definition(&definition(&[("host", "h")]))
            .expect("props")
            .with_credentials(Some("svc-user".into()), Some("hunter2".into()));
        let rendered = format!("{props:?}");
        assert!(!rendered.contains("hunter2") && !rendered.contains("svc-user"));

        let parsed = parse_destination("amqp10s://svc-user:hunter2@broker/q").expect("parse");
        assert!(!format!("{parsed:?}").contains("hunter2"));

        let malformed = parse_destination("amqp10://svc-user:hunter2@broker:5672/").unwrap_err();
        assert!(
            !malformed.message.contains("hunter2"),
            "{}",
            malformed.message
        );
        assert_eq!(
            redact_destination("amqp10s://u:p%40ss@broker:5671/q?type=topic"),
            "amqp10s://broker:5671/q?type=topic"
        );
        assert_eq!(redact_destination("no-scheme"), "no-scheme");
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
