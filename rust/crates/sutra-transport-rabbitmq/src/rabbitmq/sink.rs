//! The RabbitMQ outbound sink — [`RabbitMqMessageSink`] implements
//! [`MessageSink`] for the `rabbitmq://` and `amqp://` destination schemes
//! (`rabbitmq://[user[:pass]@]host[:port]/[<exchange>/]<routingKey>`; one path segment
//! publishes to the DEFAULT exchange with that segment as the routing key).
//!
//! The RabbitMQ message sink, with the m9 broker-contract wire invariants FROZEN:
//!
//! - the row's `outbox_key` rides the AMQP **`message-id`** property (RabbitMQ's
//!   consumer-side dedup identifier — NOT the `sutra-outbox-key` header the other four
//!   brokers use);
//! - published messages are persistent (**`delivery-mode = 2`**);
//! - the reply `content-type` lands on the AMQP `content-type` property, defaulting to
//!   `application/octet-stream` via the header fallback;
//! - every outbound header rides the AMQP application-header table verbatim (the
//!   CloudEvents binary projection — `cloudEvents:*` for AMQP 0.9.1 — happens upstream);
//! - a present `traceparent` rides as a `traceparent` header (trace-context bridge).
//!
//! One [`Connection`] per broker authority is cached for the sink's lifetime (the
//! connection cache); each send opens and closes a fresh AMQP channel.

use std::collections::HashMap;
use std::sync::Arc;

use lapin::options::BasicPublishOptions;
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Connection, ConnectionProperties};
use tracing::warn;

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::telemetry::TRACEPARENT_HEADER;

use super::{codes, parse_destination, RabbitMqDestination};

/// The outbound RabbitMQ transport (default-constructible; connections open lazily on
/// first publish per authority).
#[derive(Default)]
pub struct RabbitMqMessageSink {
    /// Connection pool keyed by `userinfo@host:port` (the authority key).
    connections: tokio::sync::Mutex<HashMap<String, Arc<Connection>>>,
}

impl RabbitMqMessageSink {
    pub fn new() -> RabbitMqMessageSink {
        RabbitMqMessageSink::default()
    }

    /// The cached-or-fresh connection for a destination's broker authority.
    async fn connection_for(
        &self,
        destination: &RabbitMqDestination,
    ) -> Result<Arc<Connection>, Diagnostic> {
        let key = destination.authority_key();
        let mut cache = self.connections.lock().await;
        if let Some(existing) = cache.get(&key) {
            if existing.status().connected() {
                return Ok(Arc::clone(existing));
            }
            cache.remove(&key);
        }
        let options = ConnectionProperties::default()
            .with_connection_name(format!("sutra-sink-rabbitmq-{key}").into());
        let fresh = Connection::connect(&destination.connection_uri(), options)
            .await
            .map_err(|e| {
                Diagnostic::error(
                    codes::OUTBOUND_CONNECTION_FAILED,
                    format!(
                        "rabbitmq sink could not connect to {}:{}: {e}",
                        destination.host, destination.port
                    ),
                )
            })?;
        let fresh = Arc::new(fresh);
        cache.insert(key, Arc::clone(&fresh));
        Ok(fresh)
    }

    /// Drain posture: close every cached connection (best-effort).
    pub async fn drain(&self) {
        let mut cache = self.connections.lock().await;
        for (authority, connection) in cache.drain() {
            if let Err(e) = connection.close(200, "sutra sink drain").await {
                warn!(authority = %authority, error = %e, "rabbitmq sink connection close failed");
            }
        }
    }
}

impl MessageSink for RabbitMqMessageSink {
    fn schemes(&self) -> Vec<String> {
        // The sink claims both spellings for AMQP 0.9.1 — there is no separate
        // AMQP-1.0 sink, so `amqp://` resolves here too.
        vec!["amqp".to_string(), "rabbitmq".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            // Malformed destination — a retry can never fix it (poison posture).
            let destination = match parse_destination(&message.destination) {
                Ok(d) => d,
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            let connection = match self.connection_for(&destination).await {
                Ok(c) => c,
                Err(diagnostic) => return SendOutcome::RetryableFailure(diagnostic),
            };
            // Fresh channel per send, closed afterwards.
            let channel = match connection.create_channel().await {
                Ok(c) => c,
                Err(e) => {
                    return SendOutcome::RetryableFailure(Diagnostic::error(
                        codes::OUTBOUND_CONNECTION_FAILED,
                        format!(
                            "rabbitmq publish to {} could not open a channel: {e}",
                            message.destination
                        ),
                    ))
                }
            };
            let publish = channel
                .basic_publish(
                    &destination.exchange,
                    &destination.routing_key,
                    BasicPublishOptions::default(),
                    &message.body,
                    build_properties(message),
                )
                .await;
            let outcome = match publish {
                Ok(confirm) => match confirm.await {
                    Ok(_) => SendOutcome::Delivered,
                    Err(e) => SendOutcome::RetryableFailure(publish_failed(message, &e)),
                },
                Err(e) => SendOutcome::RetryableFailure(publish_failed(message, &e)),
            };
            if let Err(e) = channel.close(200, "sutra sink send complete").await {
                tracing::debug!(
                    destination = %message.destination,
                    error = %e,
                    "rabbitmq sink channel close failed"
                );
            }
            outcome
        })
    }
}

fn publish_failed(message: &OutboundMessage, error: &lapin::Error) -> Diagnostic {
    Diagnostic::error(
        codes::OUTBOUND_PUBLISH_FAILED,
        format!(
            "rabbitmq publish to {} failed: {error}",
            message.destination
        ),
    )
}

/// The FROZEN wire projection (the sink's property build):
/// `message-id` = the outbox key, `delivery-mode` = 2 (persistent), `content-type`
/// resolved from the message (header fallback, `application/octet-stream` default),
/// headers verbatim as AMQP application headers, plus `traceparent` when present.
pub(crate) fn build_properties(message: &OutboundMessage) -> BasicProperties {
    let mut properties = BasicProperties::default()
        .with_message_id(ShortString::from(message.outbox_key.as_str()))
        .with_delivery_mode(2)
        .with_content_type(ShortString::from(resolve_content_type(message)));
    let mut headers = FieldTable::default();
    for (key, value) in &message.headers {
        headers.insert(
            ShortString::from(key.as_str()),
            AMQPValue::LongString(value.as_str().into()),
        );
    }
    if let Some(traceparent) = &message.traceparent {
        if !message.headers.contains_key(TRACEPARENT_HEADER) {
            headers.insert(
                ShortString::from(TRACEPARENT_HEADER),
                AMQPValue::LongString(traceparent.as_str().into()),
            );
        }
    }
    if !headers.inner().is_empty() {
        properties = properties.with_headers(headers);
    }
    properties
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
        if key.eq_ignore_ascii_case("content-type") {
            return value.clone();
        }
    }
    "application/octet-stream".to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

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

    #[test]
    fn outbox_key_lands_on_the_amqp_message_id_property() {
        // FROZEN — RabbitMQ uses AMQP's native message-id, NOT the sutra-outbox-key header.
        let properties = build_properties(&message("rabbitmq://broker/q"));
        assert_eq!(
            properties.message_id().as_ref().map(|s| s.as_str()),
            Some("outbox-abc-123")
        );
        assert_eq!(*properties.delivery_mode(), Some(2), "persistent delivery");
        assert_eq!(
            properties.content_type().as_ref().map(|s| s.as_str()),
            Some("application/json")
        );
        assert!(properties.headers().is_none(), "no headers, no table");
    }

    #[test]
    fn headers_ride_verbatim_and_traceparent_bridges() {
        let mut m = message("rabbitmq://broker/q");
        m.headers.insert("x-tenant".to_string(), "acme".to_string());
        m.headers
            .insert("cloudEvents:type".to_string(), "payment.reply".to_string());
        m.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        let properties = build_properties(&m);
        let headers = properties.headers().as_ref().expect("headers");
        let get = |k: &str| {
            headers
                .inner()
                .get(&ShortString::from(k))
                .map(super::super::stringify_field)
        };
        assert_eq!(get("x-tenant").as_deref(), Some("acme"));
        // The AMQP 0.9.1 CE binding prefix is `cloudEvents:` — projected upstream,
        // carried verbatim here.
        assert_eq!(get("cloudEvents:type").as_deref(), Some("payment.reply"));
        assert_eq!(
            get("traceparent").as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn explicit_traceparent_header_wins_over_the_field() {
        let mut m = message("rabbitmq://broker/q");
        m.headers
            .insert("traceparent".to_string(), "explicit".to_string());
        m.traceparent = Some("from-field".to_string());
        let properties = build_properties(&m);
        let headers = properties.headers().as_ref().expect("headers");
        assert_eq!(
            headers
                .inner()
                .get(&ShortString::from("traceparent"))
                .map(super::super::stringify_field)
                .as_deref(),
            Some("explicit")
        );
    }

    #[test]
    fn content_type_falls_back_to_header_then_octet_stream() {
        let mut m = message("rabbitmq://broker/q");
        m.content_type = None;
        m.headers
            .insert("Content-Type".to_string(), "application/xml".to_string());
        assert_eq!(resolve_content_type(&m), "application/xml");

        m.headers.clear();
        assert_eq!(resolve_content_type(&m), "application/octet-stream");
    }

    #[tokio::test]
    async fn malformed_destination_is_a_permanent_failure() {
        let sink = RabbitMqMessageSink::new();
        for bad in [
            "rabbitmq://broker:5672/",
            "kafka://broker/topic",
            "not-a-uri",
        ] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(d) => {
                    assert_eq!(d.code, codes::OUTBOUND_SEND_FAILED, "for '{bad}'")
                }
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn unreachable_broker_is_a_retryable_failure() {
        let sink = RabbitMqMessageSink::new();
        // Port 1 is reserved — connect refused immediately.
        match sink.send(&message("rabbitmq://127.0.0.1:1/q")).await {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_CONNECTION_FAILED)
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
        sink.drain().await;
    }

    #[test]
    fn sink_claims_the_amqp_and_rabbitmq_schemes() {
        // The scheme matrix is FROZEN — {"amqp", "rabbitmq"}.
        let mut schemes = RabbitMqMessageSink::new().schemes();
        schemes.sort();
        assert_eq!(schemes, vec!["amqp".to_string(), "rabbitmq".to_string()]);
    }
}
