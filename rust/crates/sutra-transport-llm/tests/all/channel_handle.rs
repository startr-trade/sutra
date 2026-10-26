//! The channel handles published to the provider SPI's channel registry: another transport's
//! model call and live session go through the channel's candidates, fallback, `require:`
//! filtering and concurrency limit.

use std::time::Duration;

use sutra_llm_spi::{
    AudioFormat, AudioSessionConfig, LlmChannelHandle, LlmChannelRegistry, LlmFailure, LlmMessage,
    LlmRequest, TurnDetection,
};
use sutra_transport_llm::{channel_handles, publish_channels};

use crate::support::{channel_yaml, opened_sessions, table, ANSWER_TEXT, MODULE_KEY};

/// The handle of the `ask` channel `yaml` declares, as the registry hands it out.
fn handle(yaml: &str) -> std::sync::Arc<dyn LlmChannelHandle> {
    let registry = LlmChannelRegistry::new();
    publish_channels(&table(yaml), &registry);
    registry
        .get(MODULE_KEY, "ask")
        .expect("the channel is published")
}

fn request() -> LlmRequest {
    LlmRequest {
        model: "ignored".into(),
        system: None,
        messages: vec![LlmMessage::user("hi")],
        tools: Vec::new(),
        json_output: false,
        max_output_tokens: None,
        temperature: None,
        deadline: Duration::ZERO,
        audio_output: None,
        output_voice: None,
    }
}

fn session_config() -> AudioSessionConfig {
    AudioSessionConfig {
        model: "ignored".into(),
        instructions: None,
        input: AudioFormat::L16 {
            rate: 16000,
            channels: 1,
        },
        output: None,
        voice: None,
        transcription_model: Some("caller-default".into()),
        turn_detection: TurnDetection::Server,
        transcription_only: true,
        language: Some("en".into()),
        connect_deadline: Duration::from_secs(5),
    }
}

#[tokio::test]
async fn a_call_through_the_handle_falls_back_and_names_the_answering_candidate() {
    let handle = handle(&channel_yaml(&["rate-limited", "ok"], ""));
    assert_eq!(handle.channel_name(), "ask");
    let answer = handle.complete(&request()).await.unwrap();
    assert_eq!(answer.provider, "test-fake");
    assert_eq!(answer.model, "ok");
    assert_eq!(answer.response.text, ANSWER_TEXT);
}

#[tokio::test]
async fn a_call_through_the_handle_honours_require_and_stops_on_a_permanent_failure() {
    let handle = handle(&channel_yaml(
        &["no-tools", "ok-required"],
        "require: [tools]",
    ));
    assert_eq!(
        handle.complete(&request()).await.unwrap().model,
        "ok-required"
    );
    assert!(handle.capabilities().tools);

    let handle = self::handle(&channel_yaml(&["permanent", "ok"], ""));
    let Err(LlmFailure::Permanent { detail }) = handle.complete(&request()).await else {
        panic!("a permanent failure does not fall back");
    };
    assert!(detail.contains("SUTRA.LLM.CALL.FAILED"), "{detail}");
    assert!(detail.contains("permanent"), "{detail}");
}

#[tokio::test]
async fn a_video_call_skips_candidates_without_video_understanding() {
    let handle = handle(&channel_yaml(&["ok-text", "video-reader"], ""));
    let mut call = request();
    call.messages[0].video = Some(sutra_llm_spi::LlmVideo::Frames(vec![
        sutra_llm_spi::LlmVideoFrame {
            data: vec![1, 2, 3],
            media_type: "image/jpeg".into(),
            at_ms: Some(0),
        },
    ]));
    let answer = handle.complete(&call).await.unwrap();
    assert_eq!(answer.model, "video-reader");
    assert!(handle.capabilities().video_understanding);
}

#[tokio::test]
async fn a_live_session_opens_on_the_first_realtime_candidate_after_a_fallback() {
    let yaml = channel_yaml(
        &["ok-text", "live-down", "live-two"],
        "require: [json-output]",
    )
    .replace(
        "          model: live-two\n",
        "          model: live-two\n          transcription-model: transcriber-two\n",
    );
    let handle = handle(&yaml);
    let opened = handle.open_audio_session(&session_config()).await.unwrap();
    assert_eq!(opened.provider, "test-fake");
    assert_eq!(opened.model, "live-two");
    let configs = opened_sessions();
    let down = configs.iter().rfind(|c| c.model == "live-down").unwrap();
    assert_eq!(down.transcription_model.as_deref(), Some("caller-default"));
    let two = configs.iter().rfind(|c| c.model == "live-two").unwrap();
    // The candidate's own transcription model wins; everything else is the caller's.
    assert_eq!(two.transcription_model.as_deref(), Some("transcriber-two"));
    assert!(two.transcription_only);
    assert_eq!(two.language.as_deref(), Some("en"));
    // The text-only candidate was never asked for a session.
    assert!(!configs.iter().any(|c| c.model == "ok-text"));
}

#[tokio::test]
async fn a_live_session_without_a_realtime_candidate_or_after_a_permanent_refusal_fails() {
    let handle = handle(&channel_yaml(&["ok-text"], ""));
    let Err(LlmFailure::Permanent { detail }) = handle.open_audio_session(&session_config()).await
    else {
        panic!("no realtime candidate");
    };
    assert!(detail.contains("realtime"), "{detail}");

    let handle = self::handle(&channel_yaml(&["live-refused", "live-ok"], ""));
    let Err(LlmFailure::Permanent { detail }) = handle.open_audio_session(&session_config()).await
    else {
        panic!("a permanent refusal does not fall back");
    };
    assert!(detail.contains("live-refused"), "{detail}");
}

#[tokio::test(start_paused = true)]
async fn an_open_session_holds_one_of_the_channel_permits() {
    let handle = handle(&channel_yaml(
        &["live-ok"],
        "concurrency: 1\ntimeout-seconds: 1",
    ));
    let first = handle.open_audio_session(&session_config()).await.unwrap();
    let Err(LlmFailure::Retryable { detail, .. }) =
        handle.open_audio_session(&session_config()).await
    else {
        panic!("the only permit is held by the open session");
    };
    assert!(detail.contains("concurrency limit 1"), "{detail}");
    drop(first);
    assert!(handle.open_audio_session(&session_config()).await.is_ok());
}

#[test]
fn only_calling_channels_are_published() {
    let yaml = channel_yaml(&["job-a"], "mode: job");
    assert!(channel_handles(&table(&yaml)).is_empty());
    assert_eq!(channel_handles(&table(&channel_yaml(&["ok"], ""))).len(), 1);
}

#[tokio::test]
async fn require_realtime_parses_and_limits_every_use_of_the_channel() {
    let handle = handle(&channel_yaml(
        &["ok-text", "live-only"],
        "require: [realtime]",
    ));
    assert!(handle.capabilities().realtime);
    assert_eq!(
        handle.complete(&request()).await.unwrap().model,
        "live-only"
    );
    assert_eq!(
        handle
            .open_audio_session(&session_config())
            .await
            .unwrap()
            .model,
        "live-only"
    );
}
