//! Per-destination send options and the address type, the settings a broker that maps AMQP 1.0
//! onto its own message model needs from a client.
//!
//! Every option is opt-in: with none set the transport sends exactly what it always has (a single
//! `data` body section, application properties named verbatim, no header, no link capability).
//!
//! A sink destination carries them in its query string,
//! `amqp10://host/address?type=queue&durable=true&ttl-ms=60000&body=text&property-names=identifier`:
//!
//! | Key | Values | Effect |
//! |---|---|---|
//! | `type` | `queue`, `topic` | Sent as the target terminus capability. Some brokers treat an address with no capability as a topic. |
//! | `durable` | `true`, `false` | The message header's `durable` flag. Unset sends no flag, which brokers read as not durable. |
//! | `ttl-ms` | milliseconds, `1`..=`4294967295` | The message header's `ttl`. |
//! | `body` | `data` (default), `binary`, `text` | `data` sends the payload as one AMQP `data` section; `binary` as one `amqp-value` holding binary; `text` as one `amqp-value` holding a string (the payload must be UTF-8). Brokers that map AMQP onto their own message format tell these apart: IBM MQ stores `text` as `MQSTR`, `binary` as `MQFMT_NONE`, and a `data` section as an embedded AMQP message (`MQAMQP`). |
//! | `application-properties` | `all` (default), `none` | `none` sends no application properties at all: the outbox key then rides only the `message-id`, the content type and the headers are not sent. For receivers that must see a message with no properties of its own. |
//! | `property-names` | `verbatim` (default), `identifier` | `identifier` rewrites every application-property name to an identifier (`[A-Za-z_$][A-Za-z0-9_$]*`, other characters become `_`), for brokers that refuse other names. `sutra-outbox-key` becomes `sutra_outbox_key`, which the source reads too. |
//!
//! An inbound channel names its address type with the `address-type` property.

use std::fmt;

/// What an address names. Sent as the link terminus capability (`queue` / `topic`), the
/// convention AMQP 1.0 brokers use to tell point-to-point from publish/subscribe addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressType {
    /// A point-to-point queue.
    Queue,
    /// A publish/subscribe topic.
    Topic,
}

impl AddressType {
    /// Parse `queue` / `topic` (ASCII case-insensitive).
    pub fn parse(raw: &str) -> Option<AddressType> {
        let raw = raw.trim();
        if raw.eq_ignore_ascii_case("queue") {
            Some(AddressType::Queue)
        } else if raw.eq_ignore_ascii_case("topic") {
            Some(AddressType::Topic)
        } else {
            None
        }
    }

    /// The terminus capability symbol for this address type.
    pub fn capability(self) -> &'static str {
        match self {
            AddressType::Queue => "queue",
            AddressType::Topic => "topic",
        }
    }
}

/// How the payload rides the message body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BodySection {
    /// One `data` section holding the raw bytes (the default).
    #[default]
    Data,
    /// One `amqp-value` section holding the raw bytes as binary.
    Binary,
    /// One `amqp-value` section holding a string; the payload must be UTF-8.
    Text,
}

/// How application-property names are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PropertyNames {
    /// As the engine names them (`sutra-outbox-key`, `content-type`, headers as authored).
    #[default]
    Verbatim,
    /// Rewritten to identifiers: see [`identifier_name`].
    Identifier,
}

/// The send options of one sink destination, read from its query string.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct AmqpSendOptions {
    /// `type=queue|topic`.
    pub address_type: Option<AddressType>,
    /// `durable=true|false`.
    pub durable: Option<bool>,
    /// `ttl-ms=<n>`.
    pub ttl_ms: Option<u32>,
    /// `body=data|binary|text`.
    pub body: BodySection,
    /// `property-names=verbatim|identifier`.
    pub property_names: PropertyNames,
    /// `application-properties=all|none`: `none` sends no application-properties section.
    pub omit_application_properties: bool,
}

impl fmt::Debug for AmqpSendOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmqpSendOptions")
            .field("address_type", &self.address_type)
            .field("durable", &self.durable)
            .field("ttl_ms", &self.ttl_ms)
            .field("body", &self.body)
            .field("property_names", &self.property_names)
            .field(
                "omit_application_properties",
                &self.omit_application_properties,
            )
            .finish()
    }
}

impl AmqpSendOptions {
    /// Read the options from a destination query (`None` = no query). Keys are ASCII
    /// case-insensitive; an unknown key is ignored, as it always was; a known key with a value it
    /// cannot take is an error naming the key, since sending without it would change the message.
    pub fn from_query(query: Option<&str>) -> Result<AmqpSendOptions, String> {
        let mut options = AmqpSendOptions::default();
        let Some(query) = query else {
            return Ok(options);
        };
        for part in query.split('&').filter(|p| !p.trim().is_empty()) {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            let (key, value) = (key.trim(), value.trim());
            let invalid =
                |expected: &str| format!("has query option '{key}={value}' (expected {expected})");
            if key.eq_ignore_ascii_case("type") {
                options.address_type =
                    Some(AddressType::parse(value).ok_or_else(|| invalid("'queue' or 'topic'"))?);
            } else if key.eq_ignore_ascii_case("durable") {
                options.durable = Some(if value.eq_ignore_ascii_case("true") {
                    true
                } else if value.eq_ignore_ascii_case("false") {
                    false
                } else {
                    return Err(invalid("'true' or 'false'"));
                });
            } else if key.eq_ignore_ascii_case("ttl-ms") {
                let ttl = value
                    .parse::<u32>()
                    .ok()
                    .filter(|ms| *ms > 0)
                    .ok_or_else(|| invalid("a positive number of milliseconds"))?;
                options.ttl_ms = Some(ttl);
            } else if key.eq_ignore_ascii_case("body") {
                options.body = if value.eq_ignore_ascii_case("data") {
                    BodySection::Data
                } else if value.eq_ignore_ascii_case("binary") {
                    BodySection::Binary
                } else if value.eq_ignore_ascii_case("text") {
                    BodySection::Text
                } else {
                    return Err(invalid("'data', 'binary' or 'text'"));
                };
            } else if key.eq_ignore_ascii_case("application-properties") {
                options.omit_application_properties = if value.eq_ignore_ascii_case("all") {
                    false
                } else if value.eq_ignore_ascii_case("none") {
                    true
                } else {
                    return Err(invalid("'all' or 'none'"));
                };
            } else if key.eq_ignore_ascii_case("property-names") {
                options.property_names = if value.eq_ignore_ascii_case("verbatim") {
                    PropertyNames::Verbatim
                } else if value.eq_ignore_ascii_case("identifier") {
                    PropertyNames::Identifier
                } else {
                    return Err(invalid("'verbatim' or 'identifier'"));
                };
            }
        }
        Ok(options)
    }

    /// The application-property name to write for `name` under these options.
    pub fn property_name(&self, name: &str) -> String {
        match self.property_names {
            PropertyNames::Verbatim => name.to_string(),
            PropertyNames::Identifier => identifier_name(name),
        }
    }
}

/// Rewrite `name` as an identifier: ASCII letters, digits, `_` and `$` are kept, every other
/// character becomes `_`, and a leading digit gets a `_` in front. Empty stays `_`.
pub fn identifier_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 1);
    for (i, c) in name.chars().enumerate() {
        if i == 0 && c.is_ascii_digit() {
            out.push('_');
        }
        if c.is_ascii_alphanumeric() || c == '_' || c == '$' {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_query_is_the_unchanged_default() {
        let options = AmqpSendOptions::from_query(None).unwrap();
        assert_eq!(options, AmqpSendOptions::default());
        assert_eq!(options.address_type, None);
        assert_eq!(options.durable, None);
        assert_eq!(options.ttl_ms, None);
        assert_eq!(options.body, BodySection::Data);
        assert_eq!(options.property_names, PropertyNames::Verbatim);
        assert_eq!(
            options.property_name("sutra-outbox-key"),
            "sutra-outbox-key"
        );
    }

    #[test]
    fn every_option_is_read_case_insensitively() {
        let options = AmqpSendOptions::from_query(Some(
            "TYPE=Queue&durable=TRUE&ttl-ms=60000&Body=text&property-names=Identifier&other=x",
        ))
        .unwrap();
        assert_eq!(options.address_type, Some(AddressType::Queue));
        assert_eq!(options.durable, Some(true));
        assert_eq!(options.ttl_ms, Some(60_000));
        assert_eq!(options.body, BodySection::Text);
        assert_eq!(options.property_names, PropertyNames::Identifier);
        assert!(!options.omit_application_properties);
        assert!(
            AmqpSendOptions::from_query(Some("application-properties=None"))
                .unwrap()
                .omit_application_properties
        );
        for (raw, body) in [
            ("body=data", BodySection::Data),
            ("body=BINARY", BodySection::Binary),
        ] {
            assert_eq!(AmqpSendOptions::from_query(Some(raw)).unwrap().body, body);
        }
        let topic = AmqpSendOptions::from_query(Some("type=topic&durable=false")).unwrap();
        assert_eq!(topic.address_type, Some(AddressType::Topic));
        assert_eq!(topic.durable, Some(false));
    }

    #[test]
    fn a_value_an_option_cannot_take_is_an_error_naming_it() {
        for bad in [
            "type=exchange",
            "durable=yes",
            "ttl-ms=0",
            "ttl-ms=-5",
            "ttl-ms=soon",
            "body=xml",
            "property-names=jms",
            "application-properties=some",
        ] {
            let err = AmqpSendOptions::from_query(Some(bad)).unwrap_err();
            let key = bad.split('=').next().unwrap();
            assert!(err.contains(key), "{bad}: {err}");
        }
    }

    #[test]
    fn identifier_names_replace_what_an_identifier_cannot_hold() {
        assert_eq!(identifier_name("sutra-outbox-key"), "sutra_outbox_key");
        assert_eq!(identifier_name("content-type"), "content_type");
        assert_eq!(identifier_name("ce-id"), "ce_id");
        assert_eq!(identifier_name("x.trace.id"), "x_trace_id");
        assert_eq!(identifier_name("9lives"), "_9lives");
        assert_eq!(identifier_name("already_fine$1"), "already_fine$1");
        assert_eq!(identifier_name(""), "_");
        assert_eq!(identifier_name("naïve"), "na_ve");
    }

    #[test]
    fn address_types_map_to_their_capability() {
        assert_eq!(AddressType::parse(" Queue "), Some(AddressType::Queue));
        assert_eq!(AddressType::parse("topic"), Some(AddressType::Topic));
        assert_eq!(AddressType::parse("fanout"), None);
        assert_eq!(AddressType::Queue.capability(), "queue");
        assert_eq!(AddressType::Topic.capability(), "topic");
    }
}
