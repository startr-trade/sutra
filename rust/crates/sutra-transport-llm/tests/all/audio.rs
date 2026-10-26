//! Audio and video through the `llm` transport: the request and reply shapes, capability
//! filtering (audio is never dropped), format conversion both ways, `voice: pipeline` and its
//! configuration.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::{json, Value};
use sutra_channels::{MessageSink, SendOutcome};
use sutra_llm_spi::audio::{decode, ulaw_encode, wav_unwrap};
use sutra_llm_spi::{codes, AudioFormat, LlmVideo};
use sutra_transport_llm::{parse_channel, parse_request_body, route, route_submit, VoiceMode};

use crate::support::{
    channel_yaml, definitions, last_request, outbound, resolver, sink, CapturePort, ANSWER_TEXT,
    FAKE, FAKE_SPOKEN, FAKE_TRANSCRIPT,
};

/// 20 ms of G.711 µ-law at 8 kHz.
fn ulaw_frame() -> Vec<u8> {
    (0..160).map(|i| ulaw_encode((i * 97) as i16)).collect()
}

fn audio_body(format: &str, data: &[u8], audio_output: Option<&str>) -> Value {
    let mut body = json!({
        "messages": [{"role": "user", "content": "what did they say?",
                      "audio": {"data": BASE64.encode(data), "format": format}}],
    });
    if let Some(out) = audio_output {
        body["audioOutput"] = json!(out);
    }
    body
}

fn channel(yaml: &str) -> sutra_transport_llm::LlmChannel {
    let defs = definitions(yaml);
    parse_channel(&defs[0], resolver)
        .expect("parses")
        .expect("an outbound llm channel")
}

fn config_error(yaml: &str) -> String {
    let defs = definitions(yaml);
    match parse_channel(&defs[0], resolver) {
        Err(d) => {
            assert_eq!(d.code, codes::CONFIG_INVALID, "{d:?}");
            d.message
        }
        Ok(_) => panic!("expected a config error"),
    }
}

const PIPELINE: &str = "voice: pipeline\nspeech:\n  provider: test-fake\n  \
                        transcribe-model: speech-stt\n  speech-model: speech-tts\n  voice: calm";

// ---- request shape ---------------------------------------------------------------------------

#[test]
fn audio_parts_and_audio_output_parse() {
    let body = audio_body("audio/PCMU", &ulaw_frame(), Some("audio/L16;rate=8000"));
    let call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    let audio = call.messages[0].audio.as_ref().expect("audio");
    assert_eq!(audio.format, AudioFormat::Pcmu);
    assert_eq!(audio.data, ulaw_frame());
    assert_eq!(
        call.audio_output,
        Some(AudioFormat::L16 {
            rate: 8000,
            channels: 1
        })
    );
    assert!(call.involves_audio());
    // The echo keeps the shape, not the media.
    assert_eq!(
        call.raw["messages"][0]["audio"],
        json!({"format": "audio/PCMU", "bytes": 160})
    );
}

#[test]
fn video_clips_and_frames_parse() {
    let body = json!({"messages": [{"role": "user", "content": "describe",
        "video": {"frames": [{"data": BASE64.encode(b"jpg1"), "format": "image/jpeg", "atMs": 0},
                             {"data": BASE64.encode(b"jpg2"), "format": "image/jpeg", "atMs": 1000}]}}]});
    let call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    match call.messages[0].video.as_ref().expect("video") {
        LlmVideo::Frames(frames) => {
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[1].at_ms, Some(1000));
            assert_eq!(frames[1].data, b"jpg2");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        call.raw["messages"][0]["video"]["frames"][0],
        json!({"format": "image/jpeg", "atMs": 0, "bytes": 4})
    );
    let clip = json!({"messages": [{"role": "user",
        "video": {"data": BASE64.encode(b"mp4"), "format": "video/mp4"}}]});
    let call = parse_request_body(&serde_json::to_vec(&clip).unwrap()).unwrap();
    assert!(matches!(
        call.messages[0].video,
        Some(LlmVideo::Clip { ref media_type, .. }) if media_type == "video/mp4"
    ));
}

#[test]
fn malformed_media_is_request_invalid() {
    let b64 = BASE64.encode(b"x");
    for body in [
        json!({"messages": [{"role": "user", "audio": {"data": "%%%", "format": "audio/wav"}}]}),
        json!({"messages": [{"role": "user", "audio": {"data": b64}}]}),
        json!({"messages": [{"role": "user", "audio": {"data": b64, "format": "audio/L16"}}]}),
        json!({"messages": [{"role": "user", "audio": {"data": "", "format": "audio/wav"}}]}),
        json!({"messages": [{"role": "assistant", "audio": {"data": b64, "format": "audio/wav"}}]}),
        json!({"messages": [{"role": "user", "audio": "raw"}]}),
        json!({"messages": [{"role": "user", "video": {"frames": []}}]}),
        json!({"messages": [{"role": "user", "video": {"data": b64, "format": "image/png"}}]}),
        json!({"prompt": "x", "audioOutput": "audio/unknown"}),
        json!({"prompt": "x", "audioOutput": 8000}),
        json!({"prompt": "x", "outputVoice": ""}),
    ] {
        let err =
            parse_request_body(&serde_json::to_vec(&body).unwrap()).expect_err(&format!("{body}"));
        assert_eq!(err.code, codes::REQUEST_INVALID, "{body}");
        assert!(
            !err.message.contains(&b64),
            "diagnostics never echo content"
        );
    }
}

// ---- native routing --------------------------------------------------------------------------

#[tokio::test]
async fn audio_skips_candidates_that_cannot_hear_it() {
    let ch = channel(&channel_yaml(&["ok-text", "audio-a"], ""));
    let body = audio_body("audio/PCMU", &ulaw_frame(), None);
    let call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    let answer = route(&ch, &call).await.unwrap_or_else(|_| panic!("routed"));
    assert_eq!(
        answer.candidate.model(),
        "audio-a",
        "the text model is skipped"
    );
    assert_eq!(answer.audio_preserved, Some(true));
    // The G.711 utterance reached the provider as the WAV it takes.
    let sent = last_request("audio-a").unwrap();
    let audio = sent.messages[0].audio.as_ref().unwrap();
    assert_eq!(audio.format, AudioFormat::Wav);
    let pcm = wav_unwrap(&audio.data).unwrap();
    assert_eq!((pcm.rate, pcm.samples.len()), (8000, 160));

    // Only text candidates: nothing can take the call.
    let text_only = channel(&channel_yaml(&["ok-text"], ""));
    assert!(matches!(
        route(&text_only, &call).await,
        Err(sutra_transport_llm::RouteFailure::NoEligibleCandidate)
    ));
}

#[tokio::test]
async fn spoken_answer_is_delivered_in_the_asked_format() {
    let port = Arc::new(CapturePort::default());
    let sink = sink(
        &channel_yaml(&["audio-b"], "output-voice: calm"),
        port.clone(),
    );
    let body = audio_body(
        "audio/wav",
        &crate::support::wav_24k(40),
        Some("audio/PCMU"),
    );
    assert_eq!(sink.send(&outbound(body)).await, SendOutcome::Delivered);
    // The provider was asked for a format it speaks, with the channel's voice.
    let sent = last_request("audio-b").unwrap();
    assert_eq!(sent.audio_output, Some(AudioFormat::Wav));
    assert_eq!(sent.output_voice.as_deref(), Some("calm"));
    let delivered = port.delivered.lock().unwrap();
    let reply: Value = serde_json::from_slice(&delivered[0].body).unwrap();
    assert_eq!(reply["audioPreserved"], true);
    assert_eq!(reply["transcript"], FAKE_SPOKEN);
    assert_eq!(reply["text"], FAKE_SPOKEN);
    assert_eq!(reply["audio"]["format"], "audio/PCMU");
    let ulaw = BASE64
        .decode(reply["audio"]["data"].as_str().unwrap())
        .unwrap();
    assert_eq!(
        ulaw.len(),
        800,
        "100 ms of 24 kHz WAV became 100 ms of 8 kHz G.711"
    );
    assert_eq!(
        reply["request"]["messages"][0]["audio"]["format"],
        "audio/wav"
    );
    assert!(reply["request"]["messages"][0]["audio"]
        .get("data")
        .is_none());
}

#[tokio::test]
async fn text_calls_carry_no_audio_fields() {
    let port = Arc::new(CapturePort::default());
    let sink = sink(&channel_yaml(&["ok"], ""), port.clone());
    assert_eq!(
        sink.send(&outbound(json!({"prompt": "hi"}))).await,
        SendOutcome::Delivered
    );
    let reply: Value = serde_json::from_slice(&port.delivered.lock().unwrap()[0].body).unwrap();
    for key in ["audio", "transcript", "audioPreserved"] {
        assert!(reply.get(key).is_none(), "{key}: {reply}");
    }
}

#[tokio::test]
async fn audio_that_cannot_be_converted_fails_permanently() {
    let port = Arc::new(CapturePort::default());
    let sink = sink(&channel_yaml(&["audio-c"], ""), port);
    // MP3 is not decodable here, and the fake takes WAV only.
    let outcome = sink
        .send(&outbound(audio_body("audio/mpeg", b"ID3", None)))
        .await;
    match outcome {
        SendOutcome::PermanentFailure(d) => {
            assert_eq!(d.code, codes::CALL_FAILED);
            assert!(
                d.message.contains(codes::AUDIO_UNSUPPORTED),
                "{}",
                d.message
            );
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn video_needs_a_video_understanding_candidate() {
    let ch = channel(&channel_yaml(&["ok-text", "video-v"], ""));
    let body = json!({"messages": [{"role": "user", "content": "describe",
        "video": {"frames": [{"data": BASE64.encode(b"jpg"), "format": "image/jpeg"}]}}]});
    let call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    let answer = route(&ch, &call).await.unwrap_or_else(|_| panic!("routed"));
    assert_eq!(answer.candidate.model(), "video-v");
    assert!(last_request("video-v").unwrap().messages[0].video.is_some());
}

#[tokio::test]
async fn require_audio_understanding_skips_the_rest() {
    let yaml = channel_yaml(&["ok-first", "audio-u"], "require: [audio-understanding]");
    let ch = channel(&yaml);
    assert!(ch.require.audio_understanding);
    let call = parse_request_body(br#"{"prompt":"how did the caller sound?"}"#).unwrap();
    let answer = route(&ch, &call).await.unwrap_or_else(|_| panic!("routed"));
    assert_eq!(answer.candidate.model(), "audio-u");
    let msg = config_error(&channel_yaml(&["ok"], "require: [video-understanding]"));
    assert!(msg.contains("no candidate has the capabilities"), "{msg}");
}

#[tokio::test]
async fn job_channels_refuse_audio() {
    let ch = channel(&channel_yaml(&["audio-job"], "mode: job"));
    let body = audio_body("audio/wav", &crate::support::wav_24k(10), None);
    let call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert!(matches!(
        route_submit(&ch, &call).await,
        Err(sutra_transport_llm::RouteFailure::Rejected { .. })
    ));
}

// ---- voice: pipeline -------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_transcribes_answers_in_text_and_speaks() {
    let port = Arc::new(CapturePort::default());
    let sink = sink(&channel_yaml(&["ok-pipe"], PIPELINE), port.clone());
    let body = audio_body("audio/PCMU", &ulaw_frame(), Some("audio/L16;rate=8000"));
    assert_eq!(sink.send(&outbound(body)).await, SendOutcome::Delivered);
    // The text model got the transcript, not the audio.
    let sent = last_request("ok-pipe").unwrap();
    assert!(sent.messages[0].audio.is_none());
    assert_eq!(
        sent.messages[0].content,
        format!("what did they say?\n\n{FAKE_TRANSCRIPT} (speech-stt)")
    );
    assert_eq!(sent.audio_output, None);
    let reply: Value = serde_json::from_slice(&port.delivered.lock().unwrap()[0].body).unwrap();
    assert_eq!(reply["audioPreserved"], false);
    assert_eq!(reply["text"], ANSWER_TEXT);
    assert_eq!(reply["transcript"], ANSWER_TEXT);
    assert_eq!(reply["provider"], FAKE);
    assert_eq!(reply["model"], "ok-pipe");
    assert_eq!(reply["audio"]["format"], "audio/L16;rate=8000");
    let pcm = decode(
        &BASE64
            .decode(reply["audio"]["data"].as_str().unwrap())
            .unwrap(),
        AudioFormat::L16 {
            rate: 8000,
            channels: 1,
        },
    )
    .unwrap();
    assert_eq!(
        pcm.duration_ms(),
        10 * ANSWER_TEXT.len() as u64,
        "the synthesised answer, resampled to 8 kHz"
    );
}

#[tokio::test]
async fn pipeline_leaves_text_calls_alone() {
    let ch = channel(&channel_yaml(&["ok-pipe-text"], PIPELINE));
    assert_eq!(ch.voice, VoiceMode::Pipeline);
    let call = parse_request_body(br#"{"prompt":"hi"}"#).unwrap();
    let answer = route(&ch, &call).await.unwrap_or_else(|_| panic!("routed"));
    assert_eq!(answer.audio_preserved, None);
    assert!(answer.response.audio.is_none());
}

#[test]
fn pipeline_configuration_is_validated() {
    let ch = channel(&channel_yaml(&["ok"], PIPELINE));
    let speech = ch.speech.as_ref().expect("speech stage");
    assert_eq!(speech.provider, FAKE);
    assert_eq!(speech.transcribe_model, "speech-stt");
    assert_eq!(speech.speech_model, "speech-tts");
    assert_eq!(speech.voice.as_deref(), Some("calm"));

    for (extra, expect) in [
        ("voice: pipeline", "needs 'llm.speech.provider'"),
        (
            "voice: pipeline\nspeech:\n  provider: test-fake\n  transcribe-model: speech-stt",
            "needs 'llm.speech.speech-model'",
        ),
        (
            "speech:\n  provider: test-fake",
            "only used with 'voice: pipeline'",
        ),
        ("voice: loud", "is not 'native' or 'pipeline'"),
        (
            "voice: pipeline\nspeech:\n  provider: test-fake\n  transcribe-model: ok\n  \
             speech-model: ok",
            "does not do both transcription and speech synthesis",
        ),
        (
            &format!("{PIPELINE}\nrequire: [audio-understanding]"),
            "cannot 'require' audio",
        ),
        ("require: [telepathy]", "unknown require capability"),
    ] {
        let msg = config_error(&channel_yaml(&["ok", "audio-x"], extra));
        assert!(msg.contains(expect), "{extra}: {msg}");
    }
}
