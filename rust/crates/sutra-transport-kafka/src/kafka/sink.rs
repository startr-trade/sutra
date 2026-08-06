//! The Kafka outbound sink — [`KafkaMessageSink`] implements [`MessageSink`] for the
//! `kafka://<topic>[/<key>]` destination scheme.
//!
//! The Kafka message sink, with the m9 broker-contract wire invariants FROZEN:
//!
//! - the row's `outbox_key` rides the **`sutra-outbox-key`** record header (the shared
//!   dedup token — NOT the AMQP `message-id` property RabbitMQ uses);
//! - the Kafka record **key** (partition pinning) is the OPTIONAL `kafka://<topic>/<key>`
//!   path segment, or NULL when absent — a SEPARATE concern from the outbox key;
//! - the reply `content-type` lands on the **`content-type`** record header (declared →
//!   caller header → `application/octet-stream`);
//! - every other outbound header rides the record verbatim (the CloudEvents `ce_*` binary
//!   projection happens upstream, [`sutra_channels::outbox_dispatch`]); a present `traceparent`
//!   rides as a `traceparent` header (trace-context bridge);
//! - the producer runs `acks=all` + `enable.idempotence=true`.
//!
//! Bootstrap servers do NOT ride the URI — the sink is engine-wide-configured, wired
//! from `SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS`. One [`FutureProducer`] is cached for the
//! sink's lifetime (created lazily on first publish).

use std::sync::Arc;
use std::time::Duration;

use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::telemetry::TRACEPARENT_HEADER;

use super::{codes, parse_destination, HEADER_CONTENT_TYPE, HEADER_OUTBOX_KEY};

/// Producer delivery timeout (the sink's `PT30S` request timeout).
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// The outbound Kafka transport. `bootstrap_servers` is the engine-wide connection target
/// (empty when unconfigured — sends then fail-closed retryable, never poison a row); the
/// producer is created lazily and cached.
pub struct KafkaMessageSink {
    bootstrap_servers: String,
    producer: tokio::sync::Mutex<Option<Arc<FutureProducer>>>,
}

impl KafkaMessageSink {
    /// A sink targeting `bootstrap_servers` (comma-separated `host:port` list; may be empty
    /// when the deployment declares no Kafka sink config).
    pub fn new(bootstrap_servers: impl Into<String>) -> KafkaMessageSink {
        KafkaMessageSink {
            bootstrap_servers: bootstrap_servers.into(),
            producer: tokio::sync::Mutex::new(None),
        }
    }

    /// The cached-or-fresh idempotent producer.
    async fn producer(&self) -> Result<Arc<FutureProducer>, Diagnostic> {
        if self.bootstrap_servers.trim().is_empty() {
            return Err(Diagnostic::error(
                codes::OUTBOUND_CONFIG_INVALID,
                "kafka sink has no bootstrap servers configured \
                 (SUTRA_SINK_KAFKA_BOOTSTRAP_SERVERS) — cannot publish",
            ));
        }
        let mut cache = self.producer.lock().await;
        if let Some(existing) = cache.as_ref() {
            return Ok(Arc::clone(existing));
        }
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", SEND_TIMEOUT.as_millis().to_string())
            .create()
            .map_err(|e| {
                Diagnostic::error(
                    codes::OUTBOUND_CONFIG_INVALID,
                    format!(
                        "kafka sink could not build a producer for '{}': {e}",
                        self.bootstrap_servers
                    ),
                )
            })?;
        let producer = Arc::new(producer);
        *cache = Some(Arc::clone(&producer));
        Ok(producer)
    }

    /// Drain posture: drop the cached producer (best-effort; librdkafka flushes on drop).
    pub async fn drain(&self) {
        let mut cache = self.producer.lock().await;
        *cache = None;
    }
}

impl MessageSink for KafkaMessageSink {
    fn schemes(&self) -> Vec<String> {
        // The scheme matrix is FROZEN — {"kafka"}.
        vec!["kafka".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            // Malformed destination — a retry can never fix it (poison posture).
            let destination = match parse_destination(&message.destination) {
                Ok(d) => d,
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            let producer = match self.producer().await {
                Ok(p) => p,
                Err(diagnostic) => return SendOutcome::RetryableFailure(diagnostic),
            };
            let headers = build_headers(message);
            let result = match &destination.key {
                Some(key) => {
                    let record = FutureRecord::to(&destination.topic)
                        .payload(&message.body)
                        .key(key.as_str())
                        .headers(headers);
                    producer.send(record, Timeout::After(SEND_TIMEOUT)).await
                }
                None => {
                    // No `.key(..)` — the record key stays NULL (round-robin partitioning),
                    // NOT a zero-length key. The unit key type `()` is never written (the
                    // `key` field is None), it only pins inference for the keyless record.
                    let record: FutureRecord<'_, (), Vec<u8>> =
                        FutureRecord::to(&destination.topic)
                            .payload(&message.body)
                            .headers(headers);
                    producer.send(record, Timeout::After(SEND_TIMEOUT)).await
                }
            };
            match result {
                Ok(_) => SendOutcome::Delivered,
                Err((error, _)) => SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_PRODUCE_FAILED,
                    format!("kafka publish to {} failed: {error}", message.destination),
                )),
            }
        })
    }
}

/// The FROZEN record headers (the sink's header lift): `sutra-outbox-key` = the
/// outbox key, `content-type` = the resolved content type, every other reply header
/// verbatim (the two owned headers de-duplicated case-insensitively), plus `traceparent`
/// when present. Returned as an ordered [`OwnedHeaders`] table.
fn build_headers(message: &OutboundMessage) -> OwnedHeaders {
    let content_type = resolve_content_type(message);
    let mut headers = OwnedHeaders::new()
        .insert(Header {
            key: HEADER_OUTBOX_KEY,
            value: Some(message.outbox_key.as_str()),
        })
        .insert(Header {
            key: HEADER_CONTENT_TYPE,
            value: Some(content_type.as_str()),
        });
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case(HEADER_OUTBOX_KEY)
            || key.eq_ignore_ascii_case(HEADER_CONTENT_TYPE)
        {
            continue;
        }
        headers = headers.insert(Header {
            key: key.as_str(),
            value: Some(value.as_str()),
        });
    }
    if let Some(traceparent) = &message.traceparent {
        if !message.headers.contains_key(TRACEPARENT_HEADER) {
            headers = headers.insert(Header {
                key: TRACEPARENT_HEADER,
                value: Some(traceparent.as_str()),
            });
        }
    }
    headers
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
        if key.eq_ignore_ascii_case(HEADER_CONTENT_TYPE) {
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

    /// Read one header value off an OwnedHeaders table (test helper).
    fn header_value(headers: &OwnedHeaders, key: &str) -> Option<String> {
        use rdkafka::message::Headers;
        (0..headers.count()).find_map(|i| {
            let h = headers.get(i);
            if h.key == key {
                Some(String::from_utf8_lossy(h.value.unwrap_or_default()).into_owned())
            } else {
                None
            }
        })
    }

    #[test]
    fn outbox_key_rides_the_sutra_outbox_key_header_and_content_type_is_set() {
        // FROZEN — the outbox key and the reply content type ride their own headers.
        let headers = build_headers(&message("kafka://payment-replies/customer-7"));
        assert_eq!(
            header_value(&headers, "sutra-outbox-key").as_deref(),
            Some("outbox-abc-123")
        );
        assert_eq!(
            header_value(&headers, "content-type").as_deref(),
            Some("application/json")
        );
    }

    #[test]
    fn record_key_comes_from_the_uri_path_not_the_outbox_key() {
        // The record key (partitioning) and the outbox key are SEPARATE concerns.
        let with_key = parse_destination("kafka://payment-replies/customer-7").expect("parse");
        assert_eq!(with_key.key.as_deref(), Some("customer-7"));
        let no_key = parse_destination("kafka://payment-replies").expect("parse");
        assert_eq!(no_key.key, None, "no path segment ⇒ NULL record key");
    }

    #[test]
    fn headers_ride_verbatim_and_traceparent_bridges() {
        let mut m = message("kafka://replies");
        m.headers.insert("x-tenant".to_string(), "acme".to_string());
        m.headers
            .insert("ce_type".to_string(), "payment.reply".to_string());
        m.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        let headers = build_headers(&m);
        assert_eq!(header_value(&headers, "x-tenant").as_deref(), Some("acme"));
        // The Kafka CE binding prefix is `ce_` (projected upstream, carried verbatim here).
        assert_eq!(
            header_value(&headers, "ce_type").as_deref(),
            Some("payment.reply")
        );
        assert_eq!(
            header_value(&headers, "traceparent").as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn explicit_traceparent_header_wins_over_the_field() {
        let mut m = message("kafka://replies");
        m.headers
            .insert("traceparent".to_string(), "explicit".to_string());
        m.traceparent = Some("from-field".to_string());
        let headers = build_headers(&m);
        assert_eq!(
            header_value(&headers, "traceparent").as_deref(),
            Some("explicit")
        );
    }

    #[test]
    fn caller_supplied_outbox_key_and_content_type_headers_do_not_duplicate() {
        let mut m = message("kafka://replies");
        // A caller header that collides with an owned header is skipped in the passthrough
        // (the owned value wins), so no duplicate lands on the wire.
        m.headers
            .insert("Bpm-Outbox-Key".to_string(), "stale".to_string());
        m.headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        let headers = build_headers(&m);
        use rdkafka::message::Headers;
        let outbox_hits = (0..headers.count())
            .filter(|&i| headers.get(i).key.eq_ignore_ascii_case("sutra-outbox-key"))
            .count();
        assert_eq!(outbox_hits, 1, "exactly one sutra-outbox-key header");
        assert_eq!(
            header_value(&headers, "sutra-outbox-key").as_deref(),
            Some("outbox-abc-123"),
            "the owned outbox key wins over a stale caller header"
        );
    }

    #[test]
    fn content_type_falls_back_to_header_then_octet_stream() {
        let mut m = message("kafka://replies");
        m.content_type = None;
        m.headers
            .insert("Content-Type".to_string(), "application/xml".to_string());
        assert_eq!(resolve_content_type(&m), "application/xml");

        m.headers.clear();
        assert_eq!(resolve_content_type(&m), "application/octet-stream");
    }

    #[tokio::test]
    async fn malformed_destination_is_a_permanent_failure() {
        let sink = KafkaMessageSink::new("localhost:9092");
        for bad in ["kafka://", "rabbitmq://broker/q", "not-a-uri"] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(d) => {
                    assert_eq!(d.code, codes::OUTBOUND_SEND_FAILED, "for '{bad}'")
                }
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn unconfigured_bootstrap_is_a_retryable_config_failure() {
        let sink = KafkaMessageSink::new("");
        match sink.send(&message("kafka://replies")).await {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_CONFIG_INVALID)
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }

    #[test]
    fn sink_claims_only_the_kafka_scheme() {
        assert_eq!(KafkaMessageSink::new("h:9092").schemes(), vec!["kafka"]);
    }
}
