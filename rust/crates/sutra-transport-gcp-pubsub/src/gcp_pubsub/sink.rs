//! The GCP Pub/Sub outbound sink — [`GcpPubSubMessageSink`] implements [`MessageSink`] for
//! the `gcp-pubsub://<topicName>` destination scheme.
//!
//! The message sink, with the m9 broker-contract wire invariants FROZEN:
//!
//! - the row's `outbox_key` rides the **`sutra-outbox-key`** message attribute (the shared
//!   dedup token the non-RabbitMQ brokers carry);
//! - the reply `content-type` lands on the **`content-type`** attribute (declared → caller
//!   header → `application/octet-stream`);
//! - every other outbound header rides as a message attribute verbatim (the CloudEvents
//!   `ce-*` binary projection happens upstream, [`sutra_channels::outbox_dispatch`]
//!   `CeBinding::GcpPubsub`, DASH form); a present `traceparent` rides as a `traceparent`
//!   attribute (trace-context bridge);
//! - the publish is fully awaited (the server message id is read back) so a `Delivered`
//!   outcome means the message is durably accepted — there is no pending un-flushed state.
//!
//! The project id does NOT ride the URI — the sink is engine-wide-configured, wired from
//! `SUTRA_SINK_GCP_PUBSUB_PROJECT_ID` (+ optional `SUTRA_SINK_GCP_PUBSUB_ENDPOINT` for the
//! emulator). One [`Client`] is cached for the sink's lifetime, and one [`Publisher`] per
//! topic (created lazily on first publish).

use std::collections::HashMap;
use std::sync::Arc;

use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::publisher::Publisher;

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome};
use sutra_channels::telemetry::TRACEPARENT_HEADER;

use super::{build_client_config, codes, parse_destination, ATTR_CONTENT_TYPE, ATTR_OUTBOX_KEY};

/// The outbound GCP Pub/Sub transport. `project_id` is the engine-wide target project (empty
/// when unconfigured — sends then fail-closed retryable, never poison a row); the client and
/// per-topic publishers are created lazily and cached.
pub struct GcpPubSubMessageSink {
    project_id: String,
    endpoint_override: Option<String>,
    client: tokio::sync::Mutex<Option<Client>>,
    publishers: tokio::sync::Mutex<HashMap<String, Arc<Publisher>>>,
}

impl GcpPubSubMessageSink {
    /// A sink targeting `project_id` (may be empty when the deployment declares no
    /// Pub/Sub sink config), optionally overriding the SDK endpoint (the emulator host).
    pub fn new(
        project_id: impl Into<String>,
        endpoint_override: Option<String>,
    ) -> GcpPubSubMessageSink {
        GcpPubSubMessageSink {
            project_id: project_id.into(),
            endpoint_override: endpoint_override.filter(|e| !e.trim().is_empty()),
            client: tokio::sync::Mutex::new(None),
            publishers: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The cached-or-fresh client (one gRPC channel pool for the sink's lifetime).
    async fn client(&self) -> Result<Client, Diagnostic> {
        if self.project_id.trim().is_empty() {
            return Err(Diagnostic::error(
                codes::OUTBOUND_CONFIG_INVALID,
                "gcp-pubsub sink has no project-id configured \
                 (SUTRA_SINK_GCP_PUBSUB_PROJECT_ID) — cannot publish",
            ));
        }
        let mut cache = self.client.lock().await;
        if let Some(existing) = cache.as_ref() {
            return Ok(existing.clone());
        }
        let config = build_client_config(&self.project_id, self.endpoint_override.as_deref());
        let client = Client::new(config).await.map_err(|e| {
            Diagnostic::error(
                codes::OUTBOUND_CONFIG_INVALID,
                format!(
                    "gcp-pubsub sink could not build a client for project '{}': {e}",
                    self.project_id
                ),
            )
        })?;
        *cache = Some(client.clone());
        Ok(client)
    }

    /// The cached-or-fresh publisher for `topic`.
    async fn publisher(&self, topic: &str) -> Result<Arc<Publisher>, Diagnostic> {
        {
            let cache = self.publishers.lock().await;
            if let Some(existing) = cache.get(topic) {
                return Ok(Arc::clone(existing));
            }
        }
        let client = self.client().await?;
        let publisher = Arc::new(client.topic(topic).new_publisher(None));
        let mut cache = self.publishers.lock().await;
        // Re-check under the lock (another task may have raced us).
        let entry = cache
            .entry(topic.to_string())
            .or_insert_with(|| Arc::clone(&publisher));
        Ok(Arc::clone(entry))
    }

    /// Drain posture: drop the cached publishers + client (best-effort; every publish is
    /// fully awaited, so there is no pending un-flushed state to lose).
    pub async fn drain(&self) {
        self.publishers.lock().await.clear();
        *self.client.lock().await = None;
    }
}

impl MessageSink for GcpPubSubMessageSink {
    fn schemes(&self) -> Vec<String> {
        vec!["gcp-pubsub".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            // Malformed destination — a retry can never fix it (poison posture).
            let destination = match parse_destination(&message.destination) {
                Ok(d) => d,
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            let publisher = match self.publisher(&destination.topic).await {
                Ok(p) => p,
                Err(diagnostic) => return SendOutcome::RetryableFailure(diagnostic),
            };
            let pubsub_message = PubsubMessage {
                data: message.body.clone(),
                attributes: build_attributes(message),
                ..Default::default()
            };
            let awaiter = publisher.publish(pubsub_message).await;
            match awaiter.get().await {
                Ok(_message_id) => SendOutcome::Delivered,
                Err(status) => SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_PUBLISH_FAILED,
                    format!(
                        "gcp-pubsub publish to {} failed: {status}",
                        message.destination
                    ),
                )),
            }
        })
    }
}

/// The FROZEN message attributes (the sink's attribute lift): `sutra-outbox-key` = the
/// outbox key, `content-type` = the resolved content type, every other reply header
/// verbatim (the two owned attributes de-duplicated case-insensitively), plus `traceparent`
/// when present.
fn build_attributes(message: &OutboundMessage) -> HashMap<String, String> {
    let content_type = resolve_content_type(message);
    let mut attributes = HashMap::new();
    attributes.insert(ATTR_OUTBOX_KEY.to_string(), message.outbox_key.clone());
    attributes.insert(ATTR_CONTENT_TYPE.to_string(), content_type);
    for (key, value) in &message.headers {
        if key.eq_ignore_ascii_case(ATTR_OUTBOX_KEY) || key.eq_ignore_ascii_case(ATTR_CONTENT_TYPE)
        {
            continue;
        }
        attributes.insert(key.clone(), value.clone());
    }
    if let Some(traceparent) = &message.traceparent {
        if !message.headers.contains_key(TRACEPARENT_HEADER) {
            attributes.insert(TRACEPARENT_HEADER.to_string(), traceparent.clone());
        }
    }
    attributes
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
        if key.eq_ignore_ascii_case(ATTR_CONTENT_TYPE) {
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
    fn outbox_key_rides_the_outbox_key_attribute_and_content_type_is_set() {
        let attributes = build_attributes(&message("gcp-pubsub://payment-replies"));
        assert_eq!(
            attributes.get("sutra-outbox-key").map(String::as_str),
            Some("outbox-abc-123")
        );
        assert_eq!(
            attributes.get("content-type").map(String::as_str),
            Some("application/json")
        );
    }

    #[test]
    fn headers_ride_verbatim_and_traceparent_bridges() {
        let mut m = message("gcp-pubsub://replies");
        m.headers.insert("x-tenant".to_string(), "acme".to_string());
        // The GCP CE binding prefix is `ce-` (projected upstream, carried verbatim here).
        m.headers
            .insert("ce-type".to_string(), "payment.reply".to_string());
        m.traceparent = Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        let attributes = build_attributes(&m);
        assert_eq!(attributes.get("x-tenant").map(String::as_str), Some("acme"));
        assert_eq!(
            attributes.get("ce-type").map(String::as_str),
            Some("payment.reply")
        );
        assert_eq!(
            attributes.get("traceparent").map(String::as_str),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn explicit_traceparent_header_wins_over_the_field() {
        let mut m = message("gcp-pubsub://replies");
        m.headers
            .insert("traceparent".to_string(), "explicit".to_string());
        m.traceparent = Some("from-field".to_string());
        let attributes = build_attributes(&m);
        assert_eq!(
            attributes.get("traceparent").map(String::as_str),
            Some("explicit")
        );
    }

    #[test]
    fn caller_supplied_owned_attributes_do_not_duplicate() {
        let mut m = message("gcp-pubsub://replies");
        m.headers
            .insert("Sutra-Outbox-Key".to_string(), "stale".to_string());
        m.headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        let attributes = build_attributes(&m);
        // Attribute keys are unique in the map; the owned value wins.
        assert_eq!(
            attributes.get("sutra-outbox-key").map(String::as_str),
            Some("outbox-abc-123"),
            "the owned outbox key wins over a stale caller header"
        );
        assert!(
            !attributes.contains_key("Sutra-Outbox-Key"),
            "the colliding caller header is dropped, not carried alongside"
        );
    }

    #[test]
    fn content_type_falls_back_to_header_then_octet_stream() {
        let mut m = message("gcp-pubsub://replies");
        m.content_type = None;
        m.headers
            .insert("Content-Type".to_string(), "application/xml".to_string());
        assert_eq!(resolve_content_type(&m), "application/xml");

        m.headers.clear();
        assert_eq!(resolve_content_type(&m), "application/octet-stream");
    }

    #[tokio::test]
    async fn malformed_destination_is_a_permanent_failure() {
        let sink = GcpPubSubMessageSink::new("proj", None);
        for bad in ["gcp-pubsub://", "kafka://broker/t", "not-a-uri"] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(_) => {}
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn unconfigured_project_is_a_retryable_config_failure() {
        let sink = GcpPubSubMessageSink::new("", None);
        match sink.send(&message("gcp-pubsub://replies")).await {
            SendOutcome::RetryableFailure(d) => {
                assert_eq!(d.code, codes::OUTBOUND_CONFIG_INVALID)
            }
            other => panic!("expected RetryableFailure, got {other:?}"),
        }
    }

    #[test]
    fn sink_claims_only_the_gcp_pubsub_scheme() {
        assert_eq!(
            GcpPubSubMessageSink::new("proj", None).schemes(),
            vec!["gcp-pubsub"]
        );
    }
}
