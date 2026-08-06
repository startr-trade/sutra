//! Knative Eventing transport — the broker pair behind the transport seams: [`router`] serves
//! the `transport: knative` inbound channels over the engine's shared HTTP listener (the
//! [`sutra_transport_spi::TransportChannels::inbound_router`] capability, exactly like
//! `sutra-transport-http`/`sutra-transport-dapr` — Knative's Broker push IS the delivery
//! guarantee, so there is no leader-elected consumer here) and
//! [`sink::KnativeMessageSink`] implements [`sutra_channels::sink::MessageSink`] for the
//! `knative://` destination scheme.

pub mod router;
pub mod sink;

pub use router::{knative_router_dynamic, knative_routes_of, KnativeRouteSet, KnativeRouteTable};
pub use sink::KnativeMessageSink;

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact Knative diagnostic codes this module raises.
/// There is deliberately no separate broker-unreachable code: an unreachable Broker ingress
/// surfaces as a retryable `OUTBOUND_PUBLISH_FAILED` (see [`sink`]), and a CE-extraction
/// failure raises the shared `sutra_channels::codes::INBOUND_REJECTED_CLOUDEVENT`, same as
/// HTTP/Dapr.
pub mod codes {
    pub const INBOUND_SUBSCRIPTION_NOT_BOUND: &str = "SUTRA.INBOUND.KNATIVE.SUBSCRIPTION_NOT_BOUND";
    /// A delivery presented SOME `ce-*` headers but is missing one or more of `ce-id`,
    /// `ce-source`, `ce-type` — the router hard-rejects it with a 400 before CE extraction
    /// runs, rather than treating the partial event as a plain body.
    pub const INBOUND_MISSING_HEADERS: &str = "SUTRA.INBOUND.KNATIVE.MISSING_HEADERS";
    /// The channel's `on-complete.hold-timeout` property is not a duration — fail closed at
    /// route build (an unparseable bound would silently pick the default and mis-size the
    /// hold against the sender's `DeliverySpec.timeout`).
    pub const INBOUND_HOLD_TIMEOUT_INVALID: &str = "SUTRA.INBOUND.KNATIVE.HOLD_TIMEOUT_INVALID";
    /// An `ack-mode: on-complete` push response was held for the channel's configured bound
    /// without the instance reaching a terminal state — the response is released as an
    /// ACCEPT (the intake is durable and the instance keeps running) and THIS delivery
    /// degrades to `on-persist`. Loud, per delivery, never silent.
    pub const INBOUND_HOLD_TIMEOUT: &str = "SUTRA.INBOUND.KNATIVE.HOLD_TIMEOUT";
    /// The held delivery's settle callbacks were dropped without firing (the engine actor
    /// went away, or the deferred-ack registry refused a duplicate registration) — answered
    /// with a RETRYABLE status so the sender redelivers; inbox dedup absorbs it.
    pub const INBOUND_HOLD_ABANDONED: &str = "SUTRA.INBOUND.KNATIVE.HOLD_ABANDONED";

    pub const OUTBOUND_INVALID_DESTINATION: &str = "SUTRA.OUTBOUND.KNATIVE.INVALID_DESTINATION";
    pub const OUTBOUND_NO_NAMESPACE: &str = "SUTRA.OUTBOUND.KNATIVE.NO_NAMESPACE";
    pub const OUTBOUND_NO_BROKER: &str = "SUTRA.OUTBOUND.KNATIVE.NO_BROKER";
    pub const OUTBOUND_PUBLISH_FAILED: &str = "SUTRA.OUTBOUND.KNATIVE.PUBLISH_FAILED";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "knative";

/// The default Broker ingress base URL — Knative Eventing's in-cluster ingress service, used
/// when neither [`sink::SINK_BROKER_INGRESS_ENV`] nor `K_SINK` is set.
pub const DEFAULT_BROKER_INGRESS: &str = "http://broker-ingress.knative-eventing.svc.cluster.local";

/// The channel property bounding the `ack-mode: on-complete` push-response hold — the
/// engine-side mirror of the sender's `DeliverySpec.timeout` (Knative Eventing, Beta +
/// enabled by default: "the timeout for each sent HTTP request", ISO-8601, settable on
/// Channels / Subscriptions / Brokers / Triggers). Authored as ISO-8601 (`PT30S`) or bare
/// seconds (`30`); see [`parse_hold_timeout`].
pub const HOLD_TIMEOUT_PROPERTY: &str = "on-complete.hold-timeout";

/// Default push-response hold bound ([`HOLD_TIMEOUT_PROPERTY`] absent). Deliberately SHORT:
/// the sender's own `DeliverySpec.timeout` is what actually decides whether a hold survives,
/// and a hold that outlives it converts every held delivery into a redelivery. 30 s sits
/// under the usual dispatcher/proxy defaults and far under Knative Serving's 300 s default
/// revision timeout, so the engine — not the network — is what ends the hold.
pub const DEFAULT_HOLD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The hold bound above which a route-build WARN fires: Knative Serving's
/// `max-revision-timeout-seconds` default (600 s). When the engine is deployed as a Knative
/// Service, the queue-proxy terminates any request that outlives the revision timeout
/// (`timeoutSeconds`, default 300 s, capped by that 600 s maximum), so a longer hold cannot
/// be honoured there — the sender sees a terminated request and retries instead.
pub const HOLD_TIMEOUT_WARN_ABOVE: std::time::Duration = std::time::Duration::from_secs(600);

/// Typed view over a Knative channel's transport-specific properties, read off the
/// definition's `properties` map under the dotted key convention every Rust transport uses:
/// `subscription`, `broker.url`, `source`, [`HOLD_TIMEOUT_PROPERTY`].
///
/// `broker_url` is accepted and defaulted but never read back: the Broker ingress the
/// outbound sink dials is engine-wide config ([`sink::SINK_BROKER_INGRESS_ENV`], with a
/// `K_SINK` fallback per the Knative sink-binding convention, resolved once at sink
/// registration), not a per-channel property. The key stays parseable so a definition
/// carrying it still loads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnativeChannelProperties {
    /// `subscription` — REQUIRED for an inbound channel (checked at route-build, not here,
    /// so a definition with no subscription still parses; [`Self::has_subscription`] reports
    /// it).
    pub subscription: String,
    /// `broker.url` (default [`DEFAULT_BROKER_INGRESS`]) — parsed-but-unused, see above.
    pub broker_url: String,
    /// `source` — CloudEvents `source` override for a synthetic/wrap-mode CE view on this
    /// channel; the router hands it to `cloudevents::extract` as the `WrapDefaults::source`
    /// used when `ce-mode: wrap` synthesises a CloudEvent for a non-CE delivery.
    pub source: Option<String>,
    /// [`HOLD_TIMEOUT_PROPERTY`] — how long an `ack-mode: on-complete` delivery may hold its
    /// push response waiting for the instance's terminal event (default
    /// [`DEFAULT_HOLD_TIMEOUT`]). Ignored under `on-persist`.
    pub hold_timeout: std::time::Duration,
}

impl KnativeChannelProperties {
    pub fn from_definition(
        def: &ChannelDefinition,
    ) -> Result<KnativeChannelProperties, Diagnostic> {
        let props = &def.properties;
        let subscription = non_blank(props.get("subscription")).unwrap_or_default();
        let broker_url = non_blank(props.get("broker.url"))
            .unwrap_or_else(|| DEFAULT_BROKER_INGRESS.to_string());
        let source = non_blank(props.get("source"));
        let hold_timeout = match non_blank(props.get(HOLD_TIMEOUT_PROPERTY)) {
            None => DEFAULT_HOLD_TIMEOUT,
            Some(raw) => parse_hold_timeout(&raw).map_err(|reason| {
                Diagnostic::error(
                    codes::INBOUND_HOLD_TIMEOUT_INVALID,
                    format!(
                        "knative channel '{}' property '{HOLD_TIMEOUT_PROPERTY}' = '{raw}' is \
                         not a duration: {reason} (use ISO-8601, e.g. PT30S, or bare seconds)",
                        def.binding.channel_name
                    ),
                )
            })?,
        };
        Ok(KnativeChannelProperties {
            subscription,
            broker_url,
            source,
            hold_timeout,
        })
    }

    /// True when a subscription is declared (the inbound route-build required-ness check).
    pub fn has_subscription(&self) -> bool {
        !self.subscription.trim().is_empty()
    }
}

fn non_blank(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Parse a [`HOLD_TIMEOUT_PROPERTY`] value: ISO-8601 (`PT30S`, `PT2M`, `P1DT1H`, fractional
/// seconds allowed) or bare seconds (`30`, `0.5`) — the two forms the engine's own duration
/// keys accept, honoured here so a channel author can paste the sender's
/// `DeliverySpec.timeout` verbatim. Zero / negative / non-finite are rejected: an
/// `on-complete` channel that cannot hold at all is a mis-declaration, not a fast path.
///
/// Deliberately a LOCAL parser (the ISO-8601 helpers live in `sutra-bpmn`, which a transport
/// crate has no business depending on); the accepted subset is D/H/M/S — a hold measured in
/// months is meaningless against a push contract whose senders time out in seconds.
pub fn parse_hold_timeout(raw: &str) -> Result<std::time::Duration, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("empty value".to_string());
    }
    let seconds = if trimmed.starts_with(['P', 'p']) {
        iso8601_seconds(trimmed)?
    } else {
        trimmed
            .parse::<f64>()
            .map_err(|_| format!("'{trimmed}' is neither ISO-8601 nor a number of seconds"))?
    };
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(format!("'{trimmed}' must be a positive duration"));
    }
    Ok(std::time::Duration::from_secs_f64(seconds))
}

/// The ISO-8601 subset: `P[nD][T[nH][nM][n(.n)S]]`, ASCII-case-insensitive. Weeks / months /
/// years are rejected on purpose (see [`parse_hold_timeout`]).
fn iso8601_seconds(input: &str) -> Result<f64, String> {
    let upper = input.to_ascii_uppercase();
    let body = &upper[1..]; // strip the leading `P`
    let (date_part, time_part) = match body.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (body, None),
    };
    let mut total = 0.0_f64;
    let mut seen_any = false;
    let mut consume = |part: &str, units: &[(char, f64)]| -> Result<(), String> {
        let mut number = String::new();
        for c in part.chars() {
            if c.is_ascii_digit() || c == '.' || c == ',' {
                number.push(if c == ',' { '.' } else { c });
                continue;
            }
            let Some((_, multiplier)) = units.iter().find(|(unit, _)| *unit == c) else {
                return Err(format!("unsupported unit '{c}' (accepted: D, H, M, S)"));
            };
            let value: f64 = number
                .parse()
                .map_err(|_| format!("'{number}' is not a number before '{c}'"))?;
            total += value * multiplier;
            seen_any = true;
            number.clear();
        }
        if !number.is_empty() {
            return Err(format!("dangling number '{number}' with no unit"));
        }
        Ok(())
    };
    consume(date_part, &[('D', 86_400.0)])?;
    if let Some(time_part) = time_part {
        consume(time_part, &[('H', 3_600.0), ('M', 60.0), ('S', 1.0)])?;
    }
    if !seen_any {
        return Err("no duration components".to_string());
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "orders", "v1");
        let binding = ChannelBinding::new("knative-in", namespace, DeploymentId::unresolved(), "");
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
    fn subscription_and_defaults() {
        let def = definition(&[("subscription", "orders-sub")]);
        let props = KnativeChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.subscription, "orders-sub");
        assert_eq!(props.broker_url, DEFAULT_BROKER_INGRESS);
        assert!(props.has_subscription());
    }

    #[test]
    fn broker_url_and_source_override() {
        let def = definition(&[
            ("subscription", "orders-sub"),
            (
                "broker.url",
                "http://broker-ingress.custom.svc.cluster.local",
            ),
            ("source", "/orders/gw"),
        ]);
        let props = KnativeChannelProperties::from_definition(&def).expect("props");
        assert_eq!(
            props.broker_url,
            "http://broker-ingress.custom.svc.cluster.local"
        );
        assert_eq!(props.source.as_deref(), Some("/orders/gw"));
    }

    #[test]
    fn missing_subscription_parses_but_is_not_bound() {
        let def = definition(&[]);
        let props = KnativeChannelProperties::from_definition(&def).expect("props");
        assert!(!props.has_subscription());
    }

    #[test]
    fn hold_timeout_defaults_and_parses_both_authored_forms() {
        let default = KnativeChannelProperties::from_definition(&definition(&[])).expect("props");
        assert_eq!(default.hold_timeout, DEFAULT_HOLD_TIMEOUT);

        for (raw, expected_secs) in [
            ("PT30S", 30.0),
            ("PT2M", 120.0),
            ("PT1M30S", 90.0),
            ("PT0.25S", 0.25),
            ("P1DT1H", 90_000.0),
            ("pt5s", 5.0),
            ("45", 45.0),
            ("0.5", 0.5),
        ] {
            let def = definition(&[(HOLD_TIMEOUT_PROPERTY, raw)]);
            let props = KnativeChannelProperties::from_definition(&def).expect("props");
            assert_eq!(
                props.hold_timeout,
                std::time::Duration::from_secs_f64(expected_secs),
                "hold-timeout '{raw}'"
            );
        }
    }

    #[test]
    fn a_malformed_or_non_positive_hold_timeout_fails_closed() {
        for raw in [
            "", "  ", "later", "PT", "P", "P2W", "P3M", "PT1X", "0", "-5", "PT-5S",
        ] {
            let def = definition(&[("subscription", "orders-sub"), (HOLD_TIMEOUT_PROPERTY, raw)]);
            match KnativeChannelProperties::from_definition(&def) {
                // A blank value is treated as "unset" (the `non_blank` convention every other
                // property follows) — every other malformed form is a fail-closed diagnostic.
                Ok(props) if raw.trim().is_empty() => {
                    assert_eq!(props.hold_timeout, DEFAULT_HOLD_TIMEOUT)
                }
                Ok(_) => panic!("'{raw}' must not parse as a hold timeout"),
                Err(d) => assert_eq!(d.code, codes::INBOUND_HOLD_TIMEOUT_INVALID, "'{raw}'"),
            }
        }
    }
}
