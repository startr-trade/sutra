//! Cross-transport BUNDLE invariants — asserted at the layer where every transport crate is
//! visible (the engine binary bundles them). The vendor outbox-key constants live in the
//! `sutra-transport-<vendor>` crates (domain-neutrality refactor); the neutral channels crate
//! names no broker.
//!
//! Everything here is cargo-feature-aware: the binary selects which transports to bundle
//! (default = HTTP + the five network brokers), so the tests force-link + assert only the
//! ENABLED set. Under `--no-default-features` the completeness test still holds (it compares
//! against the cfg-selected expectation) and the broker-only invariant simply does not compile
//! in unless every broker it names is enabled.

// Force-link every ENABLED transport so its `inventory::submit! TransportFactory` is present in
// THIS test binary (a test binary links only the engine lib; the binary force-links in main.rs).
#[cfg(feature = "amqp")]
use sutra_transport_amqp as _;
#[cfg(feature = "dapr")]
use sutra_transport_dapr as _;
#[cfg(feature = "file")]
use sutra_transport_file as _;
#[cfg(feature = "gcp-pubsub")]
use sutra_transport_gcp_pubsub as _;
#[cfg(feature = "http")]
use sutra_transport_http as _;
#[cfg(feature = "kafka")]
use sutra_transport_kafka as _;
#[cfg(feature = "knative")]
use sutra_transport_knative as _;
#[cfg(feature = "rabbitmq")]
use sutra_transport_rabbitmq as _;
#[cfg(feature = "aws-sqs")]
use sutra_transport_sqs as _;

/// The bundle wires every ENABLED transport GENERICALLY: `transport_factories()` returns one
/// factory per force-linked `sutra-transport-*` crate. A dropped force-link (linker DCE) or a
/// missing `inventory::submit!` shrinks this set — the engine would then silently fail to bind
/// that transport's channels — so this pins it to exactly the cargo-feature-selected set (and
/// proves HTTP rides the SAME SPI as the brokers).
#[test]
#[allow(clippy::vec_init_then_push)] // the pushes are cfg-gated; a vec! literal can't gate rows
fn transport_factories_bundles_every_enabled_transport() {
    let mut expected: Vec<&str> = Vec::new();
    #[cfg(feature = "amqp")]
    expected.push("amqp");
    #[cfg(feature = "aws-sqs")]
    expected.push("aws-sqs");
    #[cfg(feature = "dapr")]
    expected.push("dapr");
    #[cfg(feature = "file")]
    expected.push("file");
    #[cfg(feature = "gcp-pubsub")]
    expected.push("gcp-pubsub");
    #[cfg(feature = "http")]
    expected.push("http");
    #[cfg(feature = "kafka")]
    expected.push("kafka");
    #[cfg(feature = "knative")]
    expected.push("knative");
    #[cfg(feature = "rabbitmq")]
    expected.push("rabbitmq");
    expected.sort_unstable();

    let transports: Vec<&str> = sutra_transport_spi::transport_factories()
        .iter()
        .map(|f| f.transport)
        .collect();
    assert_eq!(
        transports, expected,
        "transport_factories() must bundle exactly the cargo-feature-enabled transports, sorted"
    );
}

/// m9 `BrokerSinkContractTest.kafkaSqsGcpAmqpAllShareTheSameOutboxKeyChannelName` — the
/// non-RabbitMQ brokers carry the dedup / consumer-idempotency token under ONE shared
/// attribute/header string (the frozen wire name `sutra-outbox-key`). RabbitMQ is the deliberate
/// exception (it rides the AMQP 0.9.1 message-id property). Compiled only when the four
/// brokers it names are all bundled (the default image).
#[cfg(all(
    feature = "kafka",
    feature = "aws-sqs",
    feature = "gcp-pubsub",
    feature = "amqp"
))]
#[test]
fn broker_sinks_share_the_outbox_key_carrier_string() {
    assert_eq!(sutra_transport_kafka::HEADER_OUTBOX_KEY, "sutra-outbox-key");
    assert_eq!(
        sutra_transport_sqs::HEADER_OUTBOX_KEY,
        sutra_transport_kafka::HEADER_OUTBOX_KEY
    );
    assert_eq!(
        sutra_transport_gcp_pubsub::ATTR_OUTBOX_KEY,
        sutra_transport_kafka::HEADER_OUTBOX_KEY
    );
    assert_eq!(
        sutra_transport_amqp::PROPERTY_OUTBOX_KEY,
        sutra_transport_kafka::HEADER_OUTBOX_KEY
    );
}
