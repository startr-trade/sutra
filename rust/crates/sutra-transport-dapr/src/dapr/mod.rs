//! Dapr transport — the pub/sub pair behind the transport seams: [`router`] serves the
//! `transport: dapr` inbound channels over the engine's shared HTTP listener (the
//! [`sutra_transport_spi::TransportChannels::inbound_router`] capability, exactly like
//! `sutra-transport-http` — Dapr's sidecar push IS the delivery guarantee, so there is no
//! leader-elected consumer here) and [`sink::DaprMessageSink`] implements
//! [`sutra_channels::sink::MessageSink`] for the `dapr://` destination scheme.

pub mod router;
pub mod sink;

pub use router::{dapr_router_dynamic, dapr_routes_of, DaprRouteSet, DaprRouteTable};
pub use sink::DaprMessageSink;

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact Dapr diagnostic codes this module raises.
/// There is deliberately no Dapr-specific CloudEvents or delivery-failure code: a
/// CE-extraction failure raises the shared
/// `sutra_channels::codes::INBOUND_REJECTED_CLOUDEVENT`, same as the HTTP transport, and an
/// unreachable sidecar surfaces as a retryable `OUTBOUND_PUBLISH_FAILED`.
pub mod codes {
    pub const INBOUND_TOPIC_NOT_BOUND: &str = "SUTRA.INBOUND.DAPR.TOPIC_NOT_BOUND";
    pub const INBOUND_TOPIC_MISMATCH: &str = "SUTRA.INBOUND.DAPR.TOPIC_MISMATCH";
    pub const INBOUND_CONFIG_INVALID: &str = "SUTRA.INBOUND.DAPR.CONFIG_INVALID";

    pub const OUTBOUND_INVALID_DESTINATION: &str = "SUTRA.OUTBOUND.DAPR.INVALID_DESTINATION";
    pub const OUTBOUND_NO_PUBSUB: &str = "SUTRA.OUTBOUND.DAPR.NO_PUBSUB";
    pub const OUTBOUND_NO_TOPIC: &str = "SUTRA.OUTBOUND.DAPR.NO_TOPIC";
    pub const OUTBOUND_PUBLISH_FAILED: &str = "SUTRA.OUTBOUND.DAPR.PUBLISH_FAILED";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "dapr";

/// Typed view over a Dapr channel's transport-specific properties, read off the definition's
/// `properties` map under the dotted key convention every Rust transport uses:
/// `pubsub.name`, `topic`, `sidecar.port`, `source`.
///
/// `sidecar_port` is accepted and validated but never read back: a Dapr sidecar is
/// one-per-process, so [`sink::DaprMessageSink`] takes its port from engine-wide config
/// ([`sink::SINK_SIDECAR_PORT_ENV`], read once at sink registration), not from any single
/// channel. The key stays parseable — and fail-closed on a bad value — so a definition
/// carrying it still loads and is still rejected loudly when it is nonsense.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaprChannelProperties {
    /// `pubsub.name` — the Dapr pub/sub component name; recorded as an `x-dapr-pubsubname`
    /// inbound header for routing context, not required to bind the route.
    pub pubsub_name: Option<String>,
    /// `topic` — REQUIRED for an inbound channel (checked at route-build, not here, so a
    /// definition with no topic still parses; [`Self::has_topic`] reports it).
    pub topic: String,
    /// `sidecar.port` (default [`Self::DEFAULT_SIDECAR_PORT`]) — parsed-but-unused, see above.
    pub sidecar_port: u16,
    /// `source` — CloudEvents `source` override for a synthetic/wrap-mode CE view on this
    /// channel; the router hands it to `cloudevents::extract` as the `WrapDefaults::source`
    /// used when `ce-mode: wrap` synthesises a CloudEvent for a non-CE delivery.
    pub source: Option<String>,
}

impl DaprChannelProperties {
    pub const DEFAULT_SIDECAR_PORT: u16 = 3500;

    /// Read the typed properties off a channel definition. Fails closed
    /// ([`codes::INBOUND_CONFIG_INVALID`]) only when `sidecar.port` is present but not a
    /// valid `1..=65535` integer.
    pub fn from_definition(def: &ChannelDefinition) -> Result<DaprChannelProperties, Diagnostic> {
        let props = &def.properties;
        let channel = &def.binding.channel_name;

        let pubsub_name = non_blank(props.get("pubsub.name"));
        let topic = non_blank(props.get("topic")).unwrap_or_default();
        let sidecar_port = match non_blank(props.get("sidecar.port")) {
            None => Self::DEFAULT_SIDECAR_PORT,
            Some(raw) => raw.parse::<u16>().map_err(|_| {
                Diagnostic::error(
                    codes::INBOUND_CONFIG_INVALID,
                    format!(
                        "dapr channel '{channel}' property 'sidecar.port' must be an integer \
                         in 1..=65535, got '{raw}'"
                    ),
                )
            })?,
        };
        let source = non_blank(props.get("source"));

        Ok(DaprChannelProperties {
            pubsub_name,
            topic,
            sidecar_port,
            source,
        })
    }

    /// True when a topic is declared (the inbound route-build required-ness check).
    pub fn has_topic(&self) -> bool {
        !self.topic.trim().is_empty()
    }
}

fn non_blank(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "orders", "v1");
        let binding = ChannelBinding::new("dapr-in", namespace, DeploymentId::unresolved(), "");
        ChannelDefinition {
            binding,
            transport: Some(TRANSPORT.to_string()),
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
    fn topic_and_defaults() {
        let def = definition(&[("topic", "orders.created")]);
        let props = DaprChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.topic, "orders.created");
        assert_eq!(
            props.sidecar_port,
            DaprChannelProperties::DEFAULT_SIDECAR_PORT
        );
        assert!(props.pubsub_name.is_none());
        assert!(props.has_topic());
    }

    #[test]
    fn pubsub_name_and_sidecar_port_override() {
        let def = definition(&[
            ("topic", "orders.created"),
            ("pubsub.name", "messagebus"),
            ("sidecar.port", "3501"),
            ("source", "/orders/gw"),
        ]);
        let props = DaprChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.pubsub_name.as_deref(), Some("messagebus"));
        assert_eq!(props.sidecar_port, 3501);
        assert_eq!(props.source.as_deref(), Some("/orders/gw"));
    }

    #[test]
    fn missing_topic_parses_but_is_not_bound() {
        let def = definition(&[]);
        let props = DaprChannelProperties::from_definition(&def).expect("props");
        assert!(!props.has_topic());
    }

    #[test]
    fn invalid_sidecar_port_fails_closed() {
        let def = definition(&[("topic", "t"), ("sidecar.port", "not-a-number")]);
        let err = DaprChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[test]
    fn sidecar_port_out_of_range_fails_closed() {
        let def = definition(&[("topic", "t"), ("sidecar.port", "70000")]);
        let err = DaprChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }
}
