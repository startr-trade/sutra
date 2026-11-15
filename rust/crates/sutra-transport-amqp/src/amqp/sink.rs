//! The AMQP 1.0 outbound sink — [`AmqpMessageSink`] implements [`MessageSink`] for the
//! `amqp10://[user:pass@]host[:port]/<destination>[?type=topic]` destination scheme.
//!
//! The wire projection is FROZEN — the `sutra-*` carriers below are a cross-broker contract,
//! and the native client imposes no identifier restriction on application-property keys:
//!
//! - the row's `outbox_key` rides the **`sutra-outbox-key`** application property (the
//!   shared cross-broker dedup token);
//! - the reply `content-type` lands on the **`content-type`** application property (declared
//!   → caller header → `application/octet-stream`);
//! - every other outbound header rides the message as an application property verbatim (the
//!   CloudEvents `ce-*` binary projection happens upstream, [`sutra_channels::outbox_dispatch`]
//!   `CeBinding::Amqp10` — the `ce-` DASH prefix); a present `traceparent` rides as a
//!   `traceparent` application property (trace-context bridge);
//! - the body is a single AMQP `Data` section (the raw reply bytes).
//!
//! Unlike Kafka the broker host rides the URI (per-authority); the
//! sink opens a fresh connection per send (the outbox delivery loop is a background spine,
//! not a hot path) and closes it after the disposition settles — no shared mutable state,
//! trivially `Send`. Engine-wide default credentials (from `SUTRA_SINK_AMQP_{USERNAME,
//! PASSWORD}`) apply when the URI carries no userinfo (anonymous when both are empty).
//!
//! An `amqp10s://` destination is secured natively (see [`super::tls`]) under the engine-wide
//! trust settings (`SUTRA_SINK_AMQP_TLS_CA_FILE`, `SUTRA_SINK_AMQP_TLS_INSECURE_SKIP_VERIFY`). A
//! destination URI can hold a password, so every diagnostic here names it redacted.
//!
//! **Every broker round trip is bounded.** Opening the connection (TCP, TLS, AMQP open and SASL),
//! beginning the session, attaching the sender and waiting for the disposition each get at most
//! the sink's operation timeout ([`DEFAULT_OPERATION_TIMEOUT`], 30 s, or
//! `SUTRA_SINK_AMQP_OPERATION_TIMEOUT_MS`). A broker that stops answering fails the send with the
//! retryable `SUTRA.OUTBOUND.AMQP.SEND_TIMEOUT`, so the outbox sends again later instead of one
//! delivery worker waiting forever. A send that timed out waiting for its disposition may already
//! have been accepted, so the retry can deliver it twice; the `sutra-outbox-key` property is what a
//! receiver dedupes that on. Closing after the outcome is bounded the same way and never changes it.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fe2o3_amqp::types::messaging::{ApplicationProperties, Message};
use fe2o3_amqp::{Connection, Sender, Session};

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::telemetry::TRACEPARENT_HEADER;

use super::tls::{
    classify_open_error, open_tls_connection, AmqpBrokerTarget, AmqpConnectFailure,
    AmqpTlsProperties,
};
use super::{
    codes, parse_destination, redact_destination, AmqpDestination, PROPERTY_CONTENT_TYPE,
    PROPERTY_OUTBOX_KEY,
};
use super::{SCHEME, SCHEME_TLS};

/// The container-id fe2o3 announces on the AMQP `open` performative.
const CONTAINER_ID: &str = "sutra-amqp-sink";

/// How long one broker round trip of a send may take by default: the same bound the TLS connect
/// and handshake have.
pub const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// The outbound AMQP 1.0 transport. `default_username`/`default_password` are the engine-wide
/// SASL credentials applied when a destination URI carries no userinfo.
pub struct AmqpMessageSink {
    default_username: Option<String>,
    default_password: Option<String>,
    /// The trust settings for `amqp10s://` destinations, or why they could not be read (then
    /// those sends fail closed rather than fall back to a weaker trust decision).
    tls_settings: Result<AmqpTlsProperties, String>,
    /// The loaded client configuration, so the CA bundle is read once rather than per send. A
    /// failed load is not cached.
    tls_config: Mutex<Option<Arc<rustls::ClientConfig>>>,
    /// The bound on each broker round trip of a send.
    operation_timeout: Duration,
}

impl AmqpMessageSink {
    /// A sink with the engine-wide default credentials (either may be empty/`None` ⇒
    /// anonymous / unset, in which case a destination URI's own userinfo — when present — is
    /// used).
    pub fn new(
        default_username: Option<String>,
        default_password: Option<String>,
    ) -> AmqpMessageSink {
        AmqpMessageSink {
            default_username: default_username.filter(|s| !s.is_empty()),
            default_password: default_password.filter(|s| !s.is_empty()),
            tls_settings: Ok(AmqpTlsProperties::default()),
            tls_config: Mutex::new(None),
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
        }
    }

    /// Bound each broker round trip of a send by `timeout` instead of the default 30 s.
    pub fn with_operation_timeout(mut self, timeout: Duration) -> AmqpMessageSink {
        self.operation_timeout = timeout;
        self
    }

    /// Set the trust settings for `amqp10s://` destinations. `Err` holds why the engine-wide
    /// settings are unusable; every TLS send then reports it.
    pub fn with_tls_settings(
        mut self,
        tls_settings: Result<AmqpTlsProperties, String>,
    ) -> AmqpMessageSink {
        self.tls_settings = tls_settings;
        self
    }

    /// The cached-or-fresh TLS client configuration.
    fn tls_client_config(&self) -> Result<Arc<rustls::ClientConfig>, Diagnostic> {
        let settings = self.tls_settings.as_ref().map_err(|e| {
            Diagnostic::error(
                codes::OUTBOUND_CONFIG_INVALID,
                format!("amqp sink TLS settings are invalid: {e}"),
            )
        })?;
        let mut cache = self
            .tls_config
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(config) = cache.as_ref() {
            return Ok(Arc::clone(config));
        }
        let config = settings.client_config().map(Arc::new).map_err(|e| {
            Diagnostic::error(
                codes::OUTBOUND_BROKER_TLS_FAILED,
                format!("amqp sink could not load its TLS trust settings: {e}"),
            )
        })?;
        *cache = Some(Arc::clone(&config));
        Ok(config)
    }

    /// Drain posture: connection-per-send holds no long-lived resources, so drain is a no-op
    /// (present for API symmetry with the other broker sinks).
    pub async fn drain(&self) {}

    /// Fold the engine-wide default credentials into a destination that carries none.
    fn apply_default_credentials(&self, mut dest: AmqpDestination) -> AmqpDestination {
        if dest.username.is_none() {
            dest.username = self.default_username.clone();
            dest.password = self.default_password.clone();
        }
        dest
    }
}

impl MessageSink for AmqpMessageSink {
    fn schemes(&self) -> Vec<String> {
        // Distinct from rabbitmq's {"amqp","rabbitmq"} — AMQP 1.0 lives under `amqp10`.
        vec![SCHEME.to_string(), SCHEME_TLS.to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            // Malformed destination — a retry can never fix it (poison posture).
            let destination = match parse_destination(&message.destination) {
                Ok(d) => self.apply_default_credentials(d),
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            let tls_config = if destination.tls {
                match self.tls_client_config() {
                    Ok(config) => Some(config),
                    Err(diagnostic) => return SendOutcome::RetryableFailure(diagnostic),
                }
            } else {
                None
            };
            deliver(&destination, message, tls_config, self.operation_timeout).await
        })
    }
}

/// Run one broker round trip of a send under `timeout`. `Err` names the step for the diagnostic.
async fn bounded<T>(
    timeout: Duration,
    step: &str,
    redacted: &str,
    fut: impl Future<Output = T>,
) -> Result<T, SendOutcome> {
    tokio::time::timeout(timeout, fut).await.map_err(|_| {
        SendOutcome::RetryableFailure(Diagnostic::error(
            codes::OUTBOUND_SEND_TIMEOUT,
            format!(
                "amqp sink got no answer from {redacted} within {} ms while it {step}; the outbox \
                 sends again",
                timeout.as_millis()
            ),
        ))
    })
}

/// Open a connection → session → sender, send the projected message, settle the outcome,
/// then close. Connection/session/send failures are RETRYABLE (the outbox loop re-attempts);
/// a broker that rejects the disposition is likewise retryable, and so is a broker that does not
/// answer a step within `timeout`.
async fn deliver(
    destination: &AmqpDestination,
    message: &OutboundMessage,
    tls_config: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
) -> SendOutcome {
    let redacted = redact_destination(&message.destination);
    let open = async {
        match tls_config {
            Some(config) => {
                let target = AmqpBrokerTarget {
                    host: &destination.host,
                    port: destination.port,
                    username: destination.username.as_deref(),
                    password: destination.password.as_deref(),
                };
                open_tls_connection(CONTAINER_ID, &target, config).await
            }
            None => Connection::open(CONTAINER_ID, destination.connection_uri().as_str())
                .await
                .map_err(classify_open_error),
        }
    };
    let opened = match bounded(timeout, "opened the connection", &redacted, open).await {
        Ok(opened) => opened,
        Err(timed_out) => return timed_out,
    };
    let mut connection = match opened {
        Ok(c) => c,
        Err(failure) => {
            let (code, detail) = match failure {
                AmqpConnectFailure::Tls(detail) => (codes::OUTBOUND_BROKER_TLS_FAILED, detail),
                AmqpConnectFailure::Auth(detail) => (codes::OUTBOUND_BROKER_AUTH_FAILED, detail),
                AmqpConnectFailure::Connection(detail) => (codes::OUTBOUND_SEND_FAILED, detail),
            };
            return SendOutcome::RetryableFailure(Diagnostic::error(
                code,
                format!("amqp sink could not connect to {redacted}: {detail}"),
            ));
        }
    };
    let outcome =
        deliver_on_connection(&mut connection, destination, message, &redacted, timeout).await;
    // Best-effort close (the outcome is already decided), bounded like every other step.
    let _ = bounded(
        timeout,
        "closed the connection",
        &redacted,
        connection.close(),
    )
    .await;
    outcome
}

async fn deliver_on_connection(
    connection: &mut fe2o3_amqp::connection::ConnectionHandle<()>,
    destination: &AmqpDestination,
    message: &OutboundMessage,
    redacted: &str,
    timeout: Duration,
) -> SendOutcome {
    let begun = bounded(
        timeout,
        "began a session",
        redacted,
        Session::begin(connection),
    )
    .await;
    let mut session = match begun {
        Err(timed_out) => return timed_out,
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            return SendOutcome::RetryableFailure(Diagnostic::error(
                codes::OUTBOUND_SEND_FAILED,
                format!("amqp sink could not begin a session to {redacted}: {e}"),
            ));
        }
    };
    let attached = bounded(
        timeout,
        "attached a sender",
        redacted,
        Sender::attach(
            &mut session,
            format!("{CONTAINER_ID}-{}", destination.address),
            destination.address.as_str(),
        ),
    )
    .await;
    let mut sender = match attached {
        Ok(Ok(s)) => s,
        failed => {
            let _ = bounded(timeout, "ended the session", redacted, session.end()).await;
            return match failed {
                Err(timed_out) => timed_out,
                Ok(Err(e)) => SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!("amqp sink could not attach a sender to {redacted}: {e}"),
                )),
                Ok(Ok(_)) => unreachable!("matched above"),
            };
        }
    };

    let sent = bounded(
        timeout,
        "waited for the broker to settle the message",
        redacted,
        sender.send(build_message(message)),
    )
    .await;
    let outcome = match sent {
        Err(timed_out) => timed_out,
        Ok(Ok(outcome)) => {
            if outcome.is_accepted() {
                SendOutcome::Delivered
            } else {
                SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_SEND_FAILED,
                    format!(
                        "amqp publish to {redacted} was not accepted (disposition: {outcome:?})"
                    ),
                ))
            }
        }
        Ok(Err(e)) => SendOutcome::RetryableFailure(Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("amqp publish to {redacted} failed: {e}"),
        )),
    };

    let _ = bounded(timeout, "closed the sender", redacted, sender.close()).await;
    let _ = bounded(timeout, "ended the session", redacted, session.end()).await;
    outcome
}

/// Build the AMQP 1.0 message (the sink's application-property lift): `sutra-outbox-key` =
/// the outbox key, `content-type` = the resolved content type, every other reply header
/// verbatim (the two owned properties de-duplicated case-insensitively), plus `traceparent`
/// when present. Body = a single `Data` section.
fn build_message(message: &OutboundMessage) -> Message<fe2o3_amqp::types::messaging::Data> {
    Message::builder()
        .application_properties(build_application_properties(message))
        .data(message.body.clone())
        .build()
}

/// The FROZEN application-property table (unit-tested independently of a broker).
fn build_application_properties(message: &OutboundMessage) -> ApplicationProperties {
    let content_type = resolve_content_type(message);
    let mut builder = ApplicationProperties::builder()
        .insert(PROPERTY_OUTBOX_KEY, message.outbox_key.as_str())
        .insert(PROPERTY_CONTENT_TYPE, content_type.as_str());
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case(PROPERTY_OUTBOX_KEY)
            || key.eq_ignore_ascii_case(PROPERTY_CONTENT_TYPE)
        {
            continue;
        }
        builder = builder.insert(key.as_str(), value.as_str());
    }
    if let Some(traceparent) = &message.traceparent {
        if !message.headers.contains_key(TRACEPARENT_HEADER) {
            builder = builder.insert(TRACEPARENT_HEADER, traceparent.as_str());
        }
    }
    builder.build()
}

/// Content-type resolution: declared content type wins, then a caller-supplied
/// `content-type` header (ASCII case-insensitive), then `application/octet-stream`.
fn resolve_content_type(message: &OutboundMessage) -> String {
    if let Some(declared) = &message.content_type {
        if !declared.trim().is_empty() {
            return declared.clone();
        }
    }
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case(PROPERTY_CONTENT_TYPE) {
            return value.clone();
        }
    }
    "application/octet-stream".to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fe2o3_amqp::types::primitives::SimpleValue;

    use super::*;

    fn message(destination: &str) -> OutboundMessage {
        OutboundMessage {
            destination: destination.to_string(),
            headers: BTreeMap::new(),
            body: b"{\"ok\":true}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "outbox-abc-123".to_string(),
            traceparent: None,
        }
    }

    /// Read one application-property value as a string (test helper).
    fn prop(ap: &ApplicationProperties, key: &str) -> Option<String> {
        ap.0.iter().find_map(|(k, v)| {
            if k == key {
                match v {
                    SimpleValue::String(s) => Some(s.clone()),
                    other => Some(format!("{other:?}")),
                }
            } else {
                None
            }
        })
    }

    fn count(ap: &ApplicationProperties, key: &str) -> usize {
        ap.0.iter()
            .filter(|(k, _)| k.eq_ignore_ascii_case(key))
            .count()
    }

    #[test]
    fn outbox_key_rides_the_sutra_outbox_key_property_and_content_type_is_set() {
        // The m9 outbox-key sharing invariant — the AMQP 1.0 sink carries `sutra-outbox-key`.
        let ap = build_application_properties(&message("amqp10://broker:5672/payment-replies"));
        assert_eq!(
            prop(&ap, PROPERTY_OUTBOX_KEY).as_deref(),
            Some("outbox-abc-123")
        );
        assert_eq!(
            prop(&ap, PROPERTY_CONTENT_TYPE).as_deref(),
            Some("application/json")
        );
    }

    #[test]
    fn ce_dash_attributes_and_headers_ride_verbatim_and_traceparent_bridges() {
        let mut m = message("amqp10://broker/replies");
        m.headers.insert("x-tenant".to_string(), "acme".to_string());
        // The AMQP 1.0 CE binding prefix is `ce-` (projected upstream, carried verbatim here).
        m.headers
            .insert("ce-type".to_string(), "payment.reply".to_string());
        m.headers.insert("ce-id".to_string(), "evt-9".to_string());
        m.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        let ap = build_application_properties(&m);
        assert_eq!(prop(&ap, "x-tenant").as_deref(), Some("acme"));
        assert_eq!(prop(&ap, "ce-type").as_deref(), Some("payment.reply"));
        assert_eq!(prop(&ap, "ce-id").as_deref(), Some("evt-9"));
        assert_eq!(
            prop(&ap, "traceparent").as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn explicit_traceparent_header_wins_over_the_field() {
        let mut m = message("amqp10://broker/replies");
        m.headers
            .insert("traceparent".to_string(), "explicit".to_string());
        m.traceparent = Some("from-field".to_string());
        let ap = build_application_properties(&m);
        assert_eq!(prop(&ap, "traceparent").as_deref(), Some("explicit"));
        assert_eq!(count(&ap, "traceparent"), 1);
    }

    #[test]
    fn caller_supplied_owned_properties_do_not_duplicate() {
        let mut m = message("amqp10://broker/replies");
        m.headers
            .insert("Sutra-Outbox-Key".to_string(), "stale".to_string());
        m.headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        let ap = build_application_properties(&m);
        assert_eq!(
            count(&ap, PROPERTY_OUTBOX_KEY),
            1,
            "exactly one outbox-key property"
        );
        assert_eq!(
            prop(&ap, PROPERTY_OUTBOX_KEY).as_deref(),
            Some("outbox-abc-123"),
            "the owned outbox key wins over a stale caller header"
        );
    }

    #[test]
    fn content_type_falls_back_to_header_then_octet_stream() {
        let mut m = message("amqp10://broker/replies");
        m.content_type = None;
        m.headers
            .insert("Content-Type".to_string(), "application/xml".to_string());
        assert_eq!(resolve_content_type(&m), "application/xml");

        m.headers.clear();
        assert_eq!(resolve_content_type(&m), "application/octet-stream");
    }

    #[test]
    fn sink_claims_only_the_amqp10_schemes() {
        let sink = AmqpMessageSink::new(None, None);
        assert_eq!(
            sink.schemes(),
            vec!["amqp10".to_string(), "amqp10s".to_string()]
        );
    }

    #[tokio::test]
    async fn malformed_destination_is_a_permanent_failure() {
        let sink = AmqpMessageSink::new(None, None);
        for bad in ["amqp10://broker:5672/", "amqp://broker/q", "not-a-uri"] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(_) => {}
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn unusable_tls_settings_fail_tls_sends_closed_and_name_the_setting() {
        let sink = AmqpMessageSink::new(None, None)
            .with_tls_settings(Err("SUTRA_SINK_AMQP_TLS_INSECURE_SKIP_VERIFY".to_string()));
        match sink.send(&message("amqp10s://broker:5671/q")).await {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_CONFIG_INVALID);
                assert!(d
                    .message
                    .contains("SUTRA_SINK_AMQP_TLS_INSECURE_SKIP_VERIFY"));
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unloadable_ca_bundle_is_a_retryable_tls_failure() {
        let sink = AmqpMessageSink::new(None, None).with_tls_settings(Ok(AmqpTlsProperties {
            ca_file: Some("/nonexistent/ca.pem".to_string()),
            ..AmqpTlsProperties::default()
        }));
        match sink.send(&message("amqp10s://broker:5671/q")).await {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_BROKER_TLS_FAILED)
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_broker_that_never_answers_is_a_retryable_coded_timeout() {
        // A listener that accepts the TCP connection and then says nothing: the AMQP open waits for
        // a header that never comes.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        let sink =
            AmqpMessageSink::new(None, None).with_operation_timeout(Duration::from_millis(300));
        let started = std::time::Instant::now();
        let outcome = sink
            .send(&message(&format!(
                "amqp10://svc:hunter2@127.0.0.1:{port}/q"
            )))
            .await;
        silent.abort();
        match outcome {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_SEND_TIMEOUT, "{}", d.message);
                assert!(d.message.contains("opened the connection"), "{}", d.message);
                assert!(!d.message.contains("hunter2"), "{}", d.message);
            }
            other => panic!("expected a retryable timeout, got {other:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(5), "bounded");
    }

    #[tokio::test]
    async fn the_operation_timeout_defaults_to_the_connect_bound() {
        assert_eq!(
            AmqpMessageSink::new(None, None).operation_timeout,
            DEFAULT_OPERATION_TIMEOUT
        );
    }

    #[tokio::test]
    async fn a_connect_failure_never_names_the_uri_password() {
        // Port 1 on loopback refuses at once, and the send reports it with the userinfo removed.
        let sink = AmqpMessageSink::new(None, None);
        match sink
            .send(&message("amqp10://svc-user:hunter2@127.0.0.1:1/q"))
            .await
        {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_SEND_FAILED);
                assert!(!d.message.contains("hunter2"), "{}", d.message);
                assert!(!d.message.contains("svc-user"), "{}", d.message);
                assert!(d.message.contains("127.0.0.1:1"), "{}", d.message);
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }
}
