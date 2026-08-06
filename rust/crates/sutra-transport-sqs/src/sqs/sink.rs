//! The AWS SQS outbound sink — [`SqsMessageSink`] implements [`MessageSink`] for the
//! `aws-sqs://<queueName>[/…]` destination scheme (and full `https` SQS URLs).
//!
//! The SQS message sink, with the m9 broker-contract wire invariants FROZEN:
//!
//! - the row's `outbox_key` rides the **`sutra-outbox-key`** message attribute (the shared
//!   dedup token — NOT the AMQP `message-id` property RabbitMQ uses);
//! - the reply `content-type` lands on the **`content-type`** message attribute (declared →
//!   caller header → `application/octet-stream`);
//! - every other outbound header rides as a message attribute verbatim (the CloudEvents
//!   `ce-*` binary projection happens upstream, [`sutra_channels::outbox_dispatch`]
//!   `CeBinding::Sqs`); a present `traceparent` rides as a `traceparent` attribute
//!   (trace-context bridge);
//! - a FIFO queue URL (ends `.fifo`) additionally sets `MessageDeduplicationId` to the
//!   outbox key and `MessageGroupId` from a caller `MessageGroupId` header (default
//!   `sutra-default`).
//!
//! Region, default account id, and the optional endpoint override are engine-wide sink
//! config ([`SqsSinkSettings`], wired from `SUTRA_SINK_AWS_SQS_*`), NOT the URI. One
//! [`aws_sdk_sqs::Client`] is cached for the sink's lifetime (built lazily on first
//! publish).

use std::sync::Arc;

use aws_sdk_sqs::types::MessageAttributeValue;
use aws_sdk_sqs::Client;

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::telemetry::TRACEPARENT_HEADER;

use super::{
    is_fifo_queue_url, parse_destination, HEADER_CONTENT_TYPE, HEADER_OUTBOX_KEY, TRANSPORT,
};

/// Engine-wide SQS sink settings — the region, default account id, and optional endpoint
/// override a named `aws-sqs://<queue>` destination resolves through. A full `https` SQS
/// URL destination ignores these (it is used verbatim).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqsSinkSettings {
    pub region: String,
    pub account_id: Option<String>,
    pub endpoint_override: Option<String>,
}

/// The outbound AWS SQS transport. `settings` is the engine-wide connection config (an
/// empty region makes sends fail-closed retryable, never poison a row); the client is
/// created lazily and cached.
pub struct SqsMessageSink {
    settings: SqsSinkSettings,
    client: tokio::sync::Mutex<Option<Arc<Client>>>,
}

impl SqsMessageSink {
    /// A sink resolving named destinations through `settings`.
    pub fn new(settings: SqsSinkSettings) -> SqsMessageSink {
        SqsMessageSink {
            settings,
            client: tokio::sync::Mutex::new(None),
        }
    }

    /// The cached-or-fresh SQS client.
    async fn client(&self) -> Result<Arc<Client>, Diagnostic> {
        if self.settings.region.trim().is_empty() {
            return Err(Diagnostic::error(
                super::codes::OUTBOUND_CONFIG_INVALID,
                "aws-sqs sink has no region configured (SUTRA_SINK_AWS_SQS_REGION) — \
                 cannot publish",
            ));
        }
        let mut cache = self.client.lock().await;
        if let Some(existing) = cache.as_ref() {
            return Ok(Arc::clone(existing));
        }
        let client = Arc::new(super::source::build_client(
            &self.settings.region,
            self.settings.endpoint_override.as_deref(),
        ));
        *cache = Some(Arc::clone(&client));
        Ok(client)
    }

    /// Drain posture: drop the cached client (best-effort).
    pub async fn drain(&self) {
        let mut cache = self.client.lock().await;
        *cache = None;
    }
}

impl MessageSink for SqsMessageSink {
    fn schemes(&self) -> Vec<String> {
        vec![TRANSPORT.to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            // Malformed destination — a retry can never fix it (poison posture).
            let destination = match parse_destination(&message.destination) {
                Ok(d) => d,
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            let Some(queue_url) = destination.resolve(&self.settings) else {
                return SendOutcome::PermanentFailure(Diagnostic::error(
                    super::codes::OUTBOUND_QUEUE_MISSING,
                    format!(
                        "aws-sqs could not derive a queue URL from '{}' with the configured \
                         region/account/endpoint",
                        message.destination
                    ),
                ));
            };
            let client = match self.client().await {
                Ok(c) => c,
                Err(diagnostic) => return SendOutcome::RetryableFailure(diagnostic),
            };

            let mut request = client
                .send_message()
                .queue_url(&queue_url)
                .message_body(String::from_utf8_lossy(&message.body).into_owned());
            for (name, value) in build_attributes(message) {
                let attr = match MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(value)
                    .build()
                {
                    Ok(a) => a,
                    Err(e) => {
                        return SendOutcome::PermanentFailure(Diagnostic::error(
                            super::codes::OUTBOUND_SEND_FAILED,
                            format!("aws-sqs could not build message attribute '{name}': {e}"),
                        ))
                    }
                };
                request = request.message_attributes(name, attr);
            }
            if is_fifo_queue_url(&queue_url) {
                request = request
                    .message_deduplication_id(&message.outbox_key)
                    .message_group_id(resolve_group_id(message));
            }

            match request.send().await {
                Ok(_) => SendOutcome::Delivered,
                Err(error) => SendOutcome::RetryableFailure(Diagnostic::error(
                    super::codes::OUTBOUND_SEND_FAILED,
                    format!(
                        "aws-sqs sendMessage to {queue_url} failed: {}",
                        aws_error_message(&error)
                    ),
                )),
            }
        })
    }
}

/// The FROZEN message attributes (the sink's attribute lift): `sutra-outbox-key` = the
/// outbox key, `content-type` = the resolved content type, every other reply header
/// verbatim (the two owned attributes de-duplicated case-insensitively), plus `traceparent`
/// when present. A pure function — the tier-1 wire-shape assertion target.
pub(crate) fn build_attributes(message: &OutboundMessage) -> Vec<(String, String)> {
    let content_type = resolve_content_type(message);
    let mut out: Vec<(String, String)> = vec![
        (HEADER_OUTBOX_KEY.to_string(), message.outbox_key.clone()),
        (HEADER_CONTENT_TYPE.to_string(), content_type),
    ];
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case(HEADER_OUTBOX_KEY)
            || key.eq_ignore_ascii_case(HEADER_CONTENT_TYPE)
        {
            continue;
        }
        out.push((key.clone(), value.clone()));
    }
    if let Some(traceparent) = &message.traceparent {
        if !message.headers.contains_key(TRACEPARENT_HEADER) {
            out.push((TRACEPARENT_HEADER.to_string(), traceparent.clone()));
        }
    }
    out
}

/// The FIFO message-group id: a caller `MessageGroupId` header (ASCII case-insensitive),
/// else `sutra-default` (SQS requires a group id on every FIFO send).
fn resolve_group_id(message: &OutboundMessage) -> String {
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case("MessageGroupId") && !value.trim().is_empty() {
            return value.clone();
        }
    }
    "sutra-default".to_string()
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

/// Best-effort human message out of an SQS SDK error (service message when present).
fn aws_error_message<E: std::error::Error>(error: &aws_sdk_sqs::error::SdkError<E>) -> String {
    error.to_string()
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

    fn attr<'a>(attrs: &'a [(String, String)], key: &str) -> Option<&'a str> {
        attrs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn outbox_key_and_content_type_are_the_owned_attributes() {
        // FROZEN — the outbox key rides the sutra-outbox-key attribute, content-type its own.
        let attrs = build_attributes(&message("aws-sqs://payment-replies"));
        assert_eq!(attr(&attrs, "sutra-outbox-key"), Some("outbox-abc-123"));
        assert_eq!(attr(&attrs, "content-type"), Some("application/json"));
    }

    #[test]
    fn ce_dash_headers_and_others_ride_verbatim_and_traceparent_bridges() {
        let mut m = message("aws-sqs://replies");
        m.headers.insert("x-tenant".to_string(), "acme".to_string());
        // The SQS CE binding prefix is `ce-` (DASH) — projected upstream, carried verbatim.
        m.headers
            .insert("ce-type".to_string(), "payment.reply".to_string());
        m.headers.insert("ce-id".to_string(), "ce-7".to_string());
        m.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        let attrs = build_attributes(&m);
        assert_eq!(attr(&attrs, "x-tenant"), Some("acme"));
        assert_eq!(attr(&attrs, "ce-type"), Some("payment.reply"));
        assert_eq!(attr(&attrs, "ce-id"), Some("ce-7"));
        assert_eq!(
            attr(&attrs, "traceparent"),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn explicit_traceparent_header_wins_over_the_field() {
        let mut m = message("aws-sqs://replies");
        m.headers
            .insert("traceparent".to_string(), "explicit".to_string());
        m.traceparent = Some("from-field".to_string());
        let attrs = build_attributes(&m);
        // Exactly one traceparent, and the header value wins.
        assert_eq!(attrs.iter().filter(|(k, _)| k == "traceparent").count(), 1);
        assert_eq!(attr(&attrs, "traceparent"), Some("explicit"));
    }

    #[test]
    fn caller_supplied_owned_attributes_do_not_duplicate() {
        let mut m = message("aws-sqs://replies");
        m.headers
            .insert("Sutra-Outbox-Key".to_string(), "stale".to_string());
        m.headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        let attrs = build_attributes(&m);
        assert_eq!(
            attrs
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case("sutra-outbox-key"))
                .count(),
            1,
            "exactly one sutra-outbox-key attribute"
        );
        assert_eq!(
            attr(&attrs, "sutra-outbox-key"),
            Some("outbox-abc-123"),
            "the owned outbox key wins over a stale caller header"
        );
    }

    #[test]
    fn content_type_falls_back_to_header_then_octet_stream() {
        let mut m = message("aws-sqs://replies");
        m.content_type = None;
        m.headers
            .insert("Content-Type".to_string(), "application/xml".to_string());
        assert_eq!(resolve_content_type(&m), "application/xml");
        m.headers.clear();
        assert_eq!(resolve_content_type(&m), "application/octet-stream");
    }

    #[test]
    fn fifo_group_id_prefers_caller_header_then_default() {
        let mut m = message("aws-sqs://orders.fifo");
        assert_eq!(resolve_group_id(&m), "sutra-default");
        m.headers
            .insert("MessageGroupId".to_string(), "tenant-acme".to_string());
        assert_eq!(resolve_group_id(&m), "tenant-acme");
    }

    #[test]
    fn sink_claims_only_the_aws_sqs_scheme() {
        let sink = SqsMessageSink::new(SqsSinkSettings::default());
        assert_eq!(sink.schemes(), vec!["aws-sqs"]);
    }

    #[tokio::test]
    async fn malformed_destination_is_a_permanent_failure() {
        let sink = SqsMessageSink::new(SqsSinkSettings {
            region: "us-east-1".to_string(),
            account_id: Some("000000000000".to_string()),
            endpoint_override: None,
        });
        for bad in ["aws-sqs://", "kafka://topic", "not-a-uri"] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(d) => {
                    assert_eq!(
                        d.code,
                        super::super::codes::OUTBOUND_QUEUE_MISSING,
                        "for '{bad}'"
                    )
                }
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn unconfigured_region_is_a_retryable_config_failure() {
        // A full-URL destination resolves verbatim (no settings needed), so send reaches the
        // client build and the empty-region guard fires there (retryable config error).
        let sink = SqsMessageSink::new(SqsSinkSettings::default());
        match sink
            .send(&message("https://sqs.us-east-1.amazonaws.com/1/replies"))
            .await
        {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, super::super::codes::OUTBOUND_CONFIG_INVALID)
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }
}
