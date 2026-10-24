use std::sync::Arc;
use std::time::Duration;

use sutra_llm_spi::{codes, RetryableClass};
use sutra_transport_llm::{
    parse_channel, parse_channels, ChannelTable, LlmTransport, DEFAULT_CONCURRENCY,
    DEFAULT_FALLBACK_ON,
};
use sutra_transport_spi::TransportChannels;

use crate::support::{channel_yaml, definitions, resolver, MODULE_KEY};

fn single(yaml: &str) -> Result<sutra_transport_llm::LlmChannel, sutra_channels::Diagnostic> {
    let defs = definitions(yaml);
    assert_eq!(defs.len(), 1);
    parse_channel(&defs[0], resolver).map(|c| c.expect("an outbound llm channel"))
}

#[test]
fn defaults_apply_when_options_are_absent() {
    let channel = single(&channel_yaml(&["ok"], "")).unwrap();
    assert_eq!(channel.name, "ask");
    assert_eq!(channel.module_key, MODULE_KEY);
    assert_eq!(channel.reply_channel, "answers");
    assert_eq!(channel.timeout, Duration::from_secs(60));
    assert_eq!(channel.concurrency, DEFAULT_CONCURRENCY);
    assert_eq!(channel.fallback_on, DEFAULT_FALLBACK_ON.to_vec());
    assert!(!channel.require.tools && !channel.require.json_output);
    assert_eq!(channel.system, None);
    assert_eq!(channel.candidates.len(), 1);
    assert_eq!(channel.permits().available_permits(), DEFAULT_CONCURRENCY);
}

#[test]
fn lists_and_scalars_parse_from_the_flattened_property_bag() {
    let yaml = channel_yaml(
        &["ok", "ok-b"],
        "system: be brief\nmax-output-tokens: 256\ntemperature: 0.2\ntimeout-seconds: 5\n\
         concurrency: 2\nfallback-on: [rate-limited]\nrequire:\n  - tools\n  - json-output",
    );
    let channel = single(&yaml).unwrap();
    assert_eq!(channel.system.as_deref(), Some("be brief"));
    assert_eq!(channel.max_output_tokens, Some(256));
    assert_eq!(channel.temperature, Some(0.2));
    assert_eq!(channel.timeout, Duration::from_secs(5));
    assert_eq!(channel.concurrency, 2);
    assert_eq!(channel.fallback_on, vec![RetryableClass::RateLimited]);
    assert!(channel.require.tools && channel.require.json_output);
    let models: Vec<&str> = channel.candidates.iter().map(|c| c.model()).collect();
    assert_eq!(models, ["ok", "ok-b"]);
}

#[test]
fn candidate_settings_and_resolved_api_key_are_stored() {
    let yaml = "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
                bind: llm://ask\n    llm:\n      reply-channel: answers\n      candidates:\n        \
                - provider: test-fake\n          model: ok\n          base-url: http://stub.invalid\n          \
                api-key: env:LLM_KEY\n";
    let channel = single(yaml).unwrap();
    let candidate = &channel.candidates[0];
    assert_eq!(candidate.provider, "test-fake");
    assert_eq!(
        candidate.config.base_url.as_deref(),
        Some("http://stub.invalid")
    );
    assert_eq!(
        candidate.config.api_key.as_deref(),
        Some("resolved:LLM_KEY")
    );
}

#[test]
fn unresolvable_api_key_is_a_config_error() {
    let yaml = "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
                llm:\n      reply-channel: answers\n      candidates:\n        \
                - provider: test-fake\n          model: ok\n          api-key: plain-value\n";
    let err = single(yaml).err().expect("refused");
    assert_eq!(err.code, codes::CONFIG_INVALID);
    assert!(!err.message.contains("plain-value"));
}

#[test]
fn missing_reply_channel_is_a_config_error() {
    let yaml = "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
                llm:\n      candidates:\n        - provider: test-fake\n          model: ok\n";
    let err = single(yaml).err().expect("refused");
    assert_eq!(err.code, codes::CONFIG_INVALID);
    assert!(err.message.contains("reply-channel"), "{}", err.message);
    assert_eq!(
        err.attributes.get("channel").map(String::as_str),
        Some("ask")
    );
}

#[test]
fn missing_candidates_is_a_config_error() {
    let yaml = "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
                llm:\n      reply-channel: answers\n";
    let err = single(yaml).err().expect("refused");
    assert_eq!(err.code, codes::CONFIG_INVALID);
    assert!(err.message.contains("candidates"), "{}", err.message);
}

#[test]
fn unknown_options_and_values_are_config_errors() {
    for extra in [
        "colour: blue",
        "fallback-on: [refused]",
        "require: [vision]",
        "concurrency: 0",
        "timeout-seconds: soon",
    ] {
        let err = single(&channel_yaml(&["ok"], extra)).err().expect(extra);
        assert_eq!(err.code, codes::CONFIG_INVALID, "{extra}");
    }
}

#[test]
fn require_no_candidate_can_meet_is_a_config_error() {
    let err = single(&channel_yaml(&["no-tools"], "require: [tools]"))
        .err()
        .expect("refused");
    assert_eq!(err.code, codes::CONFIG_INVALID);
}

#[test]
fn unknown_provider_names_channel_provider_and_feature_hint() {
    let yaml = "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
                llm:\n      reply-channel: answers\n      candidates:\n        \
                - provider: not-linked\n          model: m\n";
    let err = single(yaml).err().expect("refused");
    assert_eq!(err.code, codes::PROVIDER_UNKNOWN);
    assert_eq!(
        err.attributes.get("channel").map(String::as_str),
        Some("ask")
    );
    assert_eq!(
        err.attributes.get("provider").map(String::as_str),
        Some("not-linked")
    );
    assert!(err.message.contains("cargo feature"), "{}", err.message);
}

#[test]
fn only_outbound_llm_channels_are_taken() {
    let yaml = "channels:\n  - name: answers\n    transport: http\n";
    let (map, failures) = parse_channels(&definitions(yaml), resolver);
    assert!(map.is_empty());
    assert!(failures.is_empty());
    // An inbound `llm` channel is a job-callback endpoint and is validated as one.
    let inbound = "channels:\n  - name: inbound-llm\n    transport: llm\n    llm:\n      \
                   reply-channel: x\n";
    let (map, failures) = parse_channels(&definitions(inbound), resolver);
    assert!(map.is_empty());
    assert_eq!(failures.len(), 1);
    assert!(failures[0]
        .message
        .contains("unknown option 'llm.reply-channel'"));
}

#[test]
fn spawn_is_strict_and_rewire_skips_bad_channels() {
    let bad = "channels:\n  - name: broken\n    transport: llm\n    direction: outbound\n    \
               llm:\n      reply-channel: answers\n      candidates:\n        \
               - provider: not-linked\n          model: m\n";
    let table = Arc::new(ChannelTable::new());
    let err = LlmTransport::build(table.clone(), &definitions(bad), resolver)
        .err()
        .expect("strict");
    assert_eq!(err.code, codes::PROVIDER_UNKNOWN);

    let transport = LlmTransport::build(
        table.clone(),
        &definitions(&channel_yaml(&["ok"], "")),
        resolver,
    )
    .unwrap();
    assert_eq!(transport.transport(), "llm");
    assert_eq!(transport.consumer_count(), 1);

    let mut mixed = definitions(&channel_yaml(&["ok"], ""));
    mixed.extend(definitions(bad));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(transport.rewire(&mixed));
    assert_eq!(transport.consumer_count(), 1);
    assert!(table.get(MODULE_KEY, "ask").is_some());
    assert!(table.get(MODULE_KEY, "broken").is_none());
}
