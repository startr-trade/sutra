//! A live audio session over the OpenAI Realtime WebSocket API.
//!
//! `wss://<base>/realtime?model=<model>` with `Authorization: Bearer <key>`; the session is
//! configured with one `session.update` (`session.type: "realtime"`, `audio.input.format`,
//! `audio.output.format` / `voice`, `output_modalities`, input `transcription`,
//! `turn_detection`) and is ready on `session.updated`. Frames go up as
//! `input_audio_buffer.append`; the answer comes back as `response.output_audio.delta`,
//! `response.output_audio_transcript.*` and `response.output_text.*`, the caller's speech as
//! `conversation.item.input_audio_transcription.*`.
//!
//! FORMATS. The API takes `audio/pcm` at 24 kHz, or G.711 (`audio/pcmu` / `audio/pcma`) at
//! 8 kHz. A G.711 session is passed straight through — the telephony fast path, no conversion
//! either way. Linear PCM at any other rate is resampled to and from 24 kHz here.

use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sutra_llm_spi::audio::transcode;
use sutra_llm_spi::{
    classify_status, redact_url, AudioFormat, AudioSession, AudioSessionConfig, AudioSessionEvent,
    BoxFuture, LlmFailure, RetryableClass, TurnDetection,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_rustls::rustls;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

/// The linear-PCM rate the Realtime API takes and returns.
pub const REALTIME_PCM_RATE: u32 = 24000;
/// Events buffered between the socket reader and [`AudioSession::next_event`].
const EVENT_BUFFER: usize = 256;
/// How much of a refused handshake's response body a failure carries.
const HANDSHAKE_DETAIL_LIMIT: usize = 400;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

type Socket = WebSocketStream<Box<dyn Io>>;

fn permanent(detail: impl Into<String>) -> LlmFailure {
    LlmFailure::Permanent {
        detail: detail.into(),
    }
}

fn unavailable(detail: impl Into<String>) -> LlmFailure {
    LlmFailure::Retryable {
        class: RetryableClass::Unavailable,
        retry_after: None,
        detail: detail.into(),
    }
}

/// The Realtime endpoint for `model` under the provider's `base` URL (`https` → `wss`,
/// `http` → `ws`, for a local stub).
pub fn realtime_url(base: &str, model: &str) -> Result<String, LlmFailure> {
    let base = base.trim_end_matches('/');
    let socket = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if base.starts_with("wss://") || base.starts_with("ws://") {
        base.to_string()
    } else {
        return Err(permanent(format!(
            "'{base}' is not an http(s) or ws(s) URL"
        )));
    };
    if model.is_empty()
        || !model
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_'))
    {
        return Err(permanent(
            "the realtime model id has characters a URL cannot carry",
        ));
    }
    Ok(format!("{socket}/realtime?model={model}"))
}

/// The endpoint of a transcription-only session (`…/realtime?intent=transcription`): the
/// transcription model is named in the session, not the URL.
pub fn transcription_url(base: &str) -> Result<String, LlmFailure> {
    let url = realtime_url(base, "transcription")?;
    Ok(url.replace("?model=transcription", "?intent=transcription"))
}

/// The wire format the API is asked for, given the format the caller has or wants: G.711 as
/// itself, everything else as 24 kHz PCM.
pub fn wire_format(format: AudioFormat) -> (Value, AudioFormat) {
    match format {
        AudioFormat::Pcmu => (json!({"type": "audio/pcmu"}), AudioFormat::Pcmu),
        AudioFormat::Pcma => (json!({"type": "audio/pcma"}), AudioFormat::Pcma),
        _ => (
            json!({"type": "audio/pcm", "rate": REALTIME_PCM_RATE}),
            AudioFormat::L16 {
                rate: REALTIME_PCM_RATE,
                channels: 1,
            },
        ),
    }
}

fn frame_format(format: AudioFormat) -> bool {
    matches!(
        format,
        AudioFormat::L16 { .. } | AudioFormat::Pcmu | AudioFormat::Pcma
    )
}

/// The `session.update` event for `config`.
pub fn session_update(config: &AudioSessionConfig) -> Value {
    let (input_format, _) = wire_format(config.input);
    let mut input = json!({
        "format": input_format,
        "turn_detection": match config.turn_detection {
            TurnDetection::Server => json!({"type": "server_vad"}),
            TurnDetection::Manual => Value::Null,
        },
    });
    if let Some(model) = &config.transcription_model {
        let mut transcription = json!({"model": model});
        if let Some(language) = &config.language {
            transcription["language"] = json!(language);
        }
        input["transcription"] = transcription;
    }
    if config.transcription_only {
        // A transcription session: the caller's audio is transcribed and nothing answers.
        return json!({
            "type": "session.update",
            "session": {"type": "transcription", "audio": {"input": input}},
        });
    }
    let mut audio = json!({"input": input});
    let modalities = match config.output {
        Some(output) => {
            let (output_format, _) = wire_format(output);
            let mut out = json!({"format": output_format});
            if let Some(voice) = &config.voice {
                out["voice"] = json!(voice);
            }
            audio["output"] = out;
            json!(["audio"])
        }
        None => json!(["text"]),
    };
    let mut session = json!({
        "type": "realtime",
        "output_modalities": modalities,
        "audio": audio,
    });
    if let Some(instructions) = &config.instructions {
        session["instructions"] = json!(instructions);
    }
    json!({"type": "session.update", "session": session})
}

/// Map one server event to session events. `output` is (wire format, wanted format) for audio.
pub fn map_server_event(
    value: &Value,
    output: Option<(AudioFormat, AudioFormat)>,
) -> Vec<AudioSessionEvent> {
    let text = |key: &str| value[key].as_str().unwrap_or("").to_string();
    let item = || text("item_id");
    match value["type"].as_str().unwrap_or("") {
        "session.updated" | "transcription_session.updated" => vec![AudioSessionEvent::Ready],
        "input_audio_buffer.speech_started" => vec![AudioSessionEvent::SpeechStarted {
            item_id: item(),
            audio_ms: value["audio_start_ms"].as_u64().unwrap_or(0),
        }],
        "input_audio_buffer.speech_stopped" => vec![AudioSessionEvent::SpeechStopped {
            item_id: item(),
            audio_ms: value["audio_end_ms"].as_u64().unwrap_or(0),
        }],
        "conversation.item.input_audio_transcription.delta" => {
            vec![AudioSessionEvent::InputTranscript {
                item_id: item(),
                text: text("delta"),
                is_final: false,
                speaker: None,
            }]
        }
        "conversation.item.input_audio_transcription.completed" => {
            vec![AudioSessionEvent::InputTranscript {
                item_id: item(),
                text: text("transcript"),
                is_final: true,
                speaker: None,
            }]
        }
        "response.output_audio_transcript.delta" => vec![AudioSessionEvent::OutputTranscript {
            item_id: item(),
            text: text("delta"),
            is_final: false,
        }],
        "response.output_audio_transcript.done" => vec![AudioSessionEvent::OutputTranscript {
            item_id: item(),
            text: text("transcript"),
            is_final: true,
        }],
        "response.output_text.delta" => vec![AudioSessionEvent::OutputText {
            item_id: item(),
            text: text("delta"),
            is_final: false,
        }],
        "response.output_text.done" => vec![AudioSessionEvent::OutputText {
            item_id: item(),
            text: text("text"),
            is_final: true,
        }],
        "response.output_audio.delta" => {
            let Some((wire, wanted)) = output else {
                return Vec::new();
            };
            let decoded = BASE64
                .decode(value["delta"].as_str().unwrap_or(""))
                .map_err(|e| format!("an audio delta is not base64: {e}"))
                .and_then(|raw| transcode(&raw, wire, wanted).map_err(|e| e.to_string()));
            match decoded {
                Ok(data) => vec![AudioSessionEvent::OutputAudio {
                    data,
                    format: wanted,
                }],
                Err(detail) => vec![AudioSessionEvent::Error { detail }],
            }
        }
        "response.done" => vec![AudioSessionEvent::TurnEnded],
        "error" => {
            let message = value["error"]["message"]
                .as_str()
                .unwrap_or("the realtime session reported an error");
            // The error code (`invalid_api_key`, `model_not_found`, …) is what a caller acts on.
            let detail = match value["error"]["code"].as_str() {
                Some(code) if !code.is_empty() => format!("{message} ({code})"),
                _ => message.to_string(),
            };
            vec![AudioSessionEvent::Error { detail }]
        }
        _ => Vec::new(),
    }
}

async fn connect(url: &str, api_key: Option<&str>) -> Result<Socket, LlmFailure> {
    let mut request = url
        .into_client_request()
        .map_err(|e| permanent(format!("'{}' is not a WebSocket URL: {e}", redact_url(url))))?;
    if let Some(key) = api_key {
        let value = HeaderValue::from_str(&format!("Bearer {key}"))
            .map_err(|_| permanent("the api key is not a valid header value"))?;
        request.headers_mut().insert("authorization", value);
    }
    let uri = request.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| permanent(format!("'{}' has no host", redact_url(url))))?
        .trim_matches(['[', ']'])
        .to_string();
    let tls = uri.scheme_str() == Some("wss");
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| unavailable(format!("connect to the realtime endpoint: {e}")))?;
    let io: Box<dyn Io> = if tls {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| permanent(format!("TLS configuration: {e}")))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|_| permanent(format!("'{host}' is not a TLS server name")))?;
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .map_err(|e| unavailable(format!("TLS handshake with the realtime endpoint: {e}")))?;
        Box::new(stream)
    } else {
        Box::new(tcp)
    };
    let (socket, _) = tokio_tungstenite::client_async(request, io)
        .await
        .map_err(|error| match error {
            WsError::Http(response) => {
                let status = response.status().as_u16();
                // The provider's own words (an `invalid_api_key` or `model_not_found` error
                // object), cut short as the HTTP paths cut theirs; the request's credential is
                // a header, never part of this text.
                let body = response
                    .body()
                    .as_deref()
                    .map(|bytes| {
                        String::from_utf8_lossy(bytes)
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join(" ")
                            .chars()
                            .take(HANDSHAKE_DETAIL_LIMIT)
                            .collect::<String>()
                    })
                    .filter(|text| !text.is_empty())
                    .map(|text| format!(": {text}"))
                    .unwrap_or_default();
                classify_status(
                    status,
                    None,
                    format!("the realtime handshake was refused: HTTP {status}{body}"),
                )
            }
            other => unavailable(format!("the realtime handshake failed: {other}")),
        })?;
    Ok(socket)
}

/// An open Realtime session.
pub struct RealtimeSession {
    writer: Mutex<SplitSink<Socket, Message>>,
    events: Mutex<mpsc::Receiver<AudioSessionEvent>>,
    reader: tokio::task::JoinHandle<()>,
    /// (the caller's frame format, what the API is sent).
    input: (AudioFormat, AudioFormat),
    manual_turns: bool,
}

impl RealtimeSession {
    /// Connect, configure and wait for `session.updated`, all within the config's deadline.
    pub async fn open(
        base: &str,
        api_key: Option<&str>,
        config: &AudioSessionConfig,
    ) -> Result<RealtimeSession, LlmFailure> {
        if !frame_format(config.input) {
            return Err(permanent(format!(
                "a live session takes L16 or G.711 frames, not {}",
                config.input
            )));
        }
        if let Some(output) = config.output {
            if !frame_format(output) {
                return Err(permanent(format!(
                    "a live session delivers L16 or G.711 audio, not {output}"
                )));
            }
        }
        let url = if config.transcription_only {
            transcription_url(base)?
        } else {
            realtime_url(base, &config.model)?
        };
        let output = config.output.map(|wanted| (wire_format(wanted).1, wanted));
        let opened = async {
            let socket = connect(&url, api_key).await?;
            let (mut writer, mut stream) = socket.split();
            writer
                .send(Message::text(session_update(config).to_string()))
                .await
                .map_err(|e| unavailable(format!("send session.update: {e}")))?;
            // Configured once the provider echoes the effective session.
            loop {
                match stream.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value = serde_json::from_str(text.as_str()).unwrap_or_default();
                        match map_server_event(&value, output).first() {
                            Some(AudioSessionEvent::Ready) => break,
                            Some(AudioSessionEvent::Error { detail }) => {
                                return Err(permanent(format!(
                                    "the realtime session was refused: {detail}"
                                )))
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        return Err(unavailable(format!("the realtime session dropped: {e}")))
                    }
                    None => return Err(unavailable("the realtime session closed while opening")),
                }
            }
            Ok::<_, LlmFailure>((writer, stream))
        };
        let (writer, stream) = tokio::time::timeout(config.connect_deadline, opened)
            .await
            .map_err(|_| LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                retry_after: None,
                detail: "the realtime session was not ready before the deadline".into(),
            })??;
        let (sender, receiver) = mpsc::channel(EVENT_BUFFER);
        let _ = sender.send(AudioSessionEvent::Ready).await;
        let reader = tokio::spawn(read_events(stream, sender, output));
        Ok(RealtimeSession {
            writer: Mutex::new(writer),
            events: Mutex::new(receiver),
            reader,
            input: (config.input, wire_format(config.input).1),
            manual_turns: config.turn_detection == TurnDetection::Manual,
        })
    }

    async fn send_event(&self, event: Value) -> Result<(), LlmFailure> {
        self.writer
            .lock()
            .await
            .send(Message::text(event.to_string()))
            .await
            .map_err(|e| unavailable(format!("the realtime session is closed: {e}")))
    }
}

async fn read_events(
    mut stream: SplitStream<Socket>,
    sender: mpsc::Sender<AudioSessionEvent>,
    output: Option<(AudioFormat, AudioFormat)>,
) {
    while let Some(message) = stream.next().await {
        match message {
            Ok(Message::Text(text)) => {
                let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else {
                    continue;
                };
                for event in map_server_event(&value, output) {
                    if sender.send(event).await.is_err() {
                        return;
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(_) => {}
            Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => break,
            Err(e) => {
                let _ = sender
                    .send(AudioSessionEvent::Error {
                        detail: format!("the realtime connection failed: {e}"),
                    })
                    .await;
                break;
            }
        }
    }
    let _ = sender.send(AudioSessionEvent::Closed).await;
}

impl AudioSession for RealtimeSession {
    fn send_audio<'a>(&'a self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), LlmFailure>> {
        Box::pin(async move {
            let (from, wire) = self.input;
            let data = transcode(frame, from, wire).map_err(|e| permanent(e.to_string()))?;
            self.send_event(json!({
                "type": "input_audio_buffer.append",
                "audio": BASE64.encode(data),
            }))
            .await
        })
    }

    fn commit(&self) -> BoxFuture<'_, Result<(), LlmFailure>> {
        Box::pin(async move {
            self.send_event(json!({"type": "input_audio_buffer.commit"}))
                .await?;
            if self.manual_turns {
                self.send_event(json!({"type": "response.create"})).await?;
            }
            Ok(())
        })
    }

    fn next_event(&self) -> BoxFuture<'_, Option<AudioSessionEvent>> {
        Box::pin(async move { self.events.lock().await.recv().await })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let mut writer = self.writer.lock().await;
            let _ = writer.send(Message::Close(None)).await;
            let _ = tokio::time::timeout(Duration::from_secs(2), writer.close()).await;
        })
    }
}

impl Drop for RealtimeSession {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_llm_spi::audio::ulaw_encode;
    use tokio::net::TcpListener;

    fn config(input: AudioFormat, output: Option<AudioFormat>) -> AudioSessionConfig {
        AudioSessionConfig {
            model: "gpt-realtime-test".into(),
            instructions: Some("be brief".into()),
            input,
            output,
            voice: Some("marin".into()),
            transcription_model: Some("gpt-transcribe".into()),
            turn_detection: TurnDetection::Server,
            transcription_only: false,
            language: None,
            connect_deadline: Duration::from_secs(5),
        }
    }

    #[test]
    fn a_transcription_session_never_answers() {
        let mut c = config(AudioFormat::Pcmu, None);
        c.transcription_only = true;
        c.language = Some("en".into());
        let update = session_update(&c);
        assert_eq!(update["session"]["type"], "transcription");
        assert!(update["session"].get("output_modalities").is_none());
        assert!(update["session"].get("instructions").is_none());
        assert_eq!(
            update["session"]["audio"]["input"]["transcription"],
            json!({"model": "gpt-transcribe", "language": "en"})
        );
        assert_eq!(
            transcription_url("https://api.openai.com/v1").unwrap(),
            "wss://api.openai.com/v1/realtime?intent=transcription"
        );
        let started = json!({"type": "input_audio_buffer.speech_started", "item_id": "i1",
                             "audio_start_ms": 1500});
        let stopped = json!({"type": "input_audio_buffer.speech_stopped", "item_id": "i1",
                             "audio_end_ms": 4200});
        assert_eq!(
            map_server_event(&started, None),
            vec![AudioSessionEvent::SpeechStarted {
                item_id: "i1".into(),
                audio_ms: 1500
            }]
        );
        assert_eq!(
            map_server_event(&stopped, None),
            vec![AudioSessionEvent::SpeechStopped {
                item_id: "i1".into(),
                audio_ms: 4200
            }]
        );
    }

    #[test]
    fn url_follows_the_base_scheme() {
        assert_eq!(
            realtime_url("https://api.openai.com/v1", "gpt-realtime-2.1").unwrap(),
            "wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1"
        );
        assert_eq!(
            realtime_url("http://127.0.0.1:9/v1/", "m").unwrap(),
            "ws://127.0.0.1:9/v1/realtime?model=m"
        );
        assert!(realtime_url("ftp://x", "m").is_err());
        assert!(realtime_url("https://x", "a&b").is_err());
    }

    #[test]
    fn g711_session_update_passes_the_call_format_through() {
        let update = session_update(&config(AudioFormat::Pcmu, Some(AudioFormat::Pcmu)));
        assert_eq!(update["type"], "session.update");
        let session = &update["session"];
        assert_eq!(session["type"], "realtime");
        assert_eq!(session["output_modalities"], json!(["audio"]));
        assert_eq!(session["instructions"], "be brief");
        assert_eq!(
            session["audio"]["input"]["format"],
            json!({"type": "audio/pcmu"})
        );
        assert_eq!(
            session["audio"]["input"]["turn_detection"],
            json!({"type": "server_vad"})
        );
        assert_eq!(
            session["audio"]["input"]["transcription"],
            json!({"model": "gpt-transcribe"})
        );
        assert_eq!(
            session["audio"]["output"],
            json!({"format": {"type": "audio/pcmu"}, "voice": "marin"})
        );
    }

    #[test]
    fn pcm_session_is_24k_and_text_only_without_output() {
        let mut cfg = config(
            AudioFormat::L16 {
                rate: 8000,
                channels: 1,
            },
            None,
        );
        cfg.turn_detection = TurnDetection::Manual;
        let session = &session_update(&cfg)["session"];
        assert_eq!(
            session["audio"]["input"]["format"],
            json!({"type": "audio/pcm", "rate": 24000})
        );
        assert_eq!(session["audio"]["input"]["turn_detection"], Value::Null);
        assert_eq!(session["output_modalities"], json!(["text"]));
        assert!(session["audio"].get("output").is_none());
    }

    #[test]
    fn server_events_map_to_session_events() {
        let pcm8 = AudioFormat::L16 {
            rate: 8000,
            channels: 1,
        };
        let wire = wire_format(pcm8).1;
        let samples: Vec<u8> = std::iter::repeat_n(1000i16, 240)
            .flat_map(|s| s.to_le_bytes())
            .collect();
        let events = map_server_event(
            &json!({"type": "response.output_audio.delta", "item_id": "i1",
                    "delta": BASE64.encode(&samples)}),
            Some((wire, pcm8)),
        );
        match &events[..] {
            [AudioSessionEvent::OutputAudio { data, format }] => {
                assert_eq!(*format, pcm8);
                assert_eq!(data.len(), 80 * 2, "24 kHz resampled to 8 kHz");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            map_server_event(
                &json!({"type": "conversation.item.input_audio_transcription.completed",
                        "item_id": "u1", "transcript": "hello"}),
                None
            ),
            vec![AudioSessionEvent::InputTranscript {
                item_id: "u1".into(),
                text: "hello".into(),
                is_final: true,
                speaker: None
            }]
        );
        assert_eq!(
            map_server_event(
                &json!({"type": "response.output_audio_transcript.delta",
                        "item_id": "a1", "delta": "hi"}),
                None
            ),
            vec![AudioSessionEvent::OutputTranscript {
                item_id: "a1".into(),
                text: "hi".into(),
                is_final: false
            }]
        );
        assert_eq!(
            map_server_event(&json!({"type": "response.done"}), None),
            vec![AudioSessionEvent::TurnEnded]
        );
        assert_eq!(
            map_server_event(&json!({"type": "error", "error": {"message": "bad"}}), None),
            vec![AudioSessionEvent::Error {
                detail: "bad".into()
            }]
        );
        assert_eq!(
            map_server_event(
                &json!({"type": "error", "error": {"message": "Incorrect API key provided.",
                        "code": "invalid_api_key"}}),
                None
            ),
            vec![AudioSessionEvent::Error {
                detail: "Incorrect API key provided. (invalid_api_key)".into()
            }]
        );
        assert!(map_server_event(&json!({"type": "rate_limits.updated"}), None).is_empty());
    }

    /// An in-process stand-in for the Realtime endpoint: checks the handshake and the
    /// configuration, echoes one appended frame back as the answer's audio, and transcribes.
    #[allow(clippy::result_large_err)] // the handshake callback signature is tungstenite's
    async fn fake_realtime() -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut seen_auth = String::new();
            let mut seen_path = String::new();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                tcp,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    seen_auth = request
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    seen_path = request.uri().to_string();
                    Ok(response)
                },
            )
            .await
            .unwrap();
            assert_eq!(seen_auth, "Bearer sk-test");
            assert_eq!(seen_path, "/v1/realtime?model=gpt-realtime-test");
            let mut received = Vec::new();
            let send = |v: Value| Message::text(v.to_string());
            socket
                .send(send(json!({"type": "session.created"})))
                .await
                .unwrap();
            while let Some(Ok(message)) = socket.next().await {
                let Message::Text(text) = message else {
                    if matches!(message, Message::Close(_)) {
                        break;
                    }
                    continue;
                };
                let value: Value = serde_json::from_str(text.as_str()).unwrap();
                received.push(value.clone());
                match value["type"].as_str().unwrap() {
                    "session.update" => socket
                        .send(send(json!({"type": "session.updated",
                                          "session": value["session"]})))
                        .await
                        .unwrap(),
                    "input_audio_buffer.append" => {
                        let audio = value["audio"].as_str().unwrap();
                        for event in [
                            json!({"type": "conversation.item.input_audio_transcription.completed",
                                   "item_id": "u1", "transcript": "STUB TRANSCRIPT"}),
                            json!({"type": "response.output_audio.delta", "item_id": "a1",
                                   "delta": audio}),
                            json!({"type": "response.output_audio_transcript.done",
                                   "item_id": "a1", "transcript": "STUB ANSWER"}),
                            json!({"type": "response.done"}),
                        ] {
                            socket.send(send(event)).await.unwrap();
                        }
                    }
                    _ => {}
                }
            }
            let _ = socket.close(None).await;
            received
        });
        (base, server)
    }

    #[tokio::test]
    async fn g711_session_round_trips_against_a_fake_endpoint() {
        let (base, server) = fake_realtime().await;
        let cfg = config(AudioFormat::Pcmu, Some(AudioFormat::Pcmu));
        let session = RealtimeSession::open(&base, Some("sk-test"), &cfg)
            .await
            .expect("open");
        assert_eq!(session.next_event().await, Some(AudioSessionEvent::Ready));
        let frame: Vec<u8> = (0..160).map(|i| ulaw_encode(i * 50)).collect();
        session.send_audio(&frame).await.unwrap();
        let mut events = Vec::new();
        while let Some(event) = session.next_event().await {
            let done = event == AudioSessionEvent::TurnEnded;
            events.push(event);
            if done {
                break;
            }
        }
        assert_eq!(
            events,
            vec![
                AudioSessionEvent::InputTranscript {
                    item_id: "u1".into(),
                    text: "STUB TRANSCRIPT".into(),
                    is_final: true,
                    speaker: None,
                },
                AudioSessionEvent::OutputAudio {
                    data: frame.clone(),
                    format: AudioFormat::Pcmu,
                },
                AudioSessionEvent::OutputTranscript {
                    item_id: "a1".into(),
                    text: "STUB ANSWER".into(),
                    is_final: true,
                },
                AudioSessionEvent::TurnEnded,
            ],
            "G.711 passes through unchanged both ways"
        );
        session.close().await;
        let mut last = None;
        while let Some(event) = session.next_event().await {
            last = Some(event);
        }
        assert_eq!(
            last,
            Some(AudioSessionEvent::Closed),
            "Closed is the last event"
        );
        let received = server.await.unwrap();
        assert_eq!(received[0]["type"], "session.update");
        assert_eq!(received[1]["type"], "input_audio_buffer.append");
        assert_eq!(
            BASE64
                .decode(received[1]["audio"].as_str().unwrap())
                .unwrap(),
            frame
        );
    }

    #[tokio::test]
    async fn pcm_frames_are_resampled_to_24k_and_back() {
        let (base, server) = fake_realtime().await;
        let pcm8 = AudioFormat::L16 {
            rate: 8000,
            channels: 1,
        };
        let mut cfg = config(pcm8, Some(pcm8));
        cfg.turn_detection = TurnDetection::Manual;
        let session = RealtimeSession::open(&base, Some("sk-test"), &cfg)
            .await
            .expect("open");
        let frame: Vec<u8> = std::iter::repeat_n(500i16, 160)
            .flat_map(|s| s.to_le_bytes())
            .collect();
        session.send_audio(&frame).await.unwrap();
        session.commit().await.unwrap();
        let mut audio = None;
        while let Some(event) = session.next_event().await {
            match event {
                AudioSessionEvent::OutputAudio { data, .. } => audio = Some(data),
                AudioSessionEvent::TurnEnded => break,
                _ => {}
            }
        }
        assert_eq!(audio.expect("audio"), frame, "8 kHz in, 8 kHz out");
        session.close().await;
        let received = server.await.unwrap();
        let sent = BASE64
            .decode(received[1]["audio"].as_str().unwrap())
            .unwrap();
        assert_eq!(sent.len(), 480 * 2, "sent upstream at 24 kHz");
        assert_eq!(received[2]["type"], "input_audio_buffer.commit");
        assert_eq!(received[3]["type"], "response.create", "manual turns ask");
    }

    #[tokio::test]
    async fn a_refused_handshake_carries_the_providers_reason() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut tcp, &mut buf).await;
            let body = r#"{"error": {"message": "Incorrect API key provided.",
                "type": "invalid_request_error", "code": "invalid_api_key"}}"#;
            let response = format!(
                "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\n\r\n{body}",
                body.len()
            );
            tokio::io::AsyncWriteExt::write_all(&mut tcp, response.as_bytes())
                .await
                .unwrap();
        });
        let cfg = config(AudioFormat::Pcmu, None);
        let failure = RealtimeSession::open(&format!("http://{addr}/v1"), Some("sk-bad"), &cfg)
            .await
            .err()
            .expect("refused");
        server.await.unwrap();
        match failure {
            LlmFailure::Permanent { detail } => {
                assert!(detail.contains("HTTP 401"), "{detail}");
                assert!(detail.contains("invalid_api_key"), "{detail}");
                assert!(!detail.contains("sk-bad"), "{detail}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn wav_frames_and_refused_handshakes_fail_cleanly() {
        let wav = config(AudioFormat::Wav, None);
        assert!(matches!(
            RealtimeSession::open("http://127.0.0.1:9/v1", None, &wav).await,
            Err(LlmFailure::Permanent { .. })
        ));
        // Nothing listens on the port: retryable.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let cfg = config(AudioFormat::Pcmu, None);
        assert!(matches!(
            RealtimeSession::open(&format!("http://{addr}/v1"), None, &cfg).await,
            Err(LlmFailure::Retryable {
                class: RetryableClass::Unavailable,
                ..
            })
        ));
    }
}
