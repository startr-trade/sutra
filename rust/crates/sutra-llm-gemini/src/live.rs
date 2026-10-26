//! A live audio session over the Gemini Live API (`BidiGenerateContent` WebSocket).
//!
//! AUTHENTICATION. The API key never travels in the URL. With a key, the session first mints a
//! single-use ephemeral token (`POST {base}/v1beta/auth_tokens`, the key in the
//! `x-goog-api-key` header, `{"uses": 1}`; the token is the answer's `name`) and then opens
//! `wss://<host>/ws/google.ai.generativelanguage.v1beta.GenerativeService.
//! BidiGenerateContentConstrained` with the token in the handshake's `Authorization: Token
//! <token>` header. The Live API documents the long-lived key only as a `?key=` query parameter
//! (and no `x-goog-api-key` header on the WebSocket handshake), which would put it in every proxy
//! and access log on the way; the ephemeral token is short-lived, good for one session, and goes
//! in a header too. Without a key (a local stub or an authenticating proxy) the plain
//! `BidiGenerateContent` endpoint is opened without credentials.
//!
//! The first client message is `{"setup": {...}}` (model, `generationConfig` with
//! `responseModalities` and `speechConfig`, `systemInstruction`, `realtimeInputConfig`, and the
//! input / output audio transcription switches); the server answers `setupComplete`. Audio goes
//! up as `realtimeInput.audio {data, mimeType: "audio/pcm;rate=16000"}` and comes back in
//! `serverContent.modelTurn.parts[].inlineData` as 24 kHz 16-bit little-endian PCM, with the
//! caller's and the model's speech transcribed in `serverContent.inputTranscription` /
//! `outputTranscription` and the end of an answer marked by `serverContent.turnComplete`.
//!
//! The server may deliver its JSON in binary frames, so both text and binary frames are read.
//!
//! The answer is always requested as audio (the native-audio models take no other modality); a
//! text-only session (`output: None`) drops the audio and reports the output transcription as
//! [`AudioSessionEvent::OutputText`].
//!
//! TURNS. With server turn detection the API's own voice-activity detection ends the caller's
//! turn (at a low end-of-speech sensitivity, after [`END_OF_SPEECH_SILENCE_MS`] of silence), and [`AudioSession::commit`] only flushes buffered audio (`audioStreamEnd`). With
//! manual turns automatic detection is switched off, the first frame of a turn is preceded by
//! `activityStart`, and `commit` sends `activityEnd`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sutra_llm_spi::audio::transcode;
use sutra_llm_spi::{
    classify_status, AudioFormat, AudioSession, AudioSessionConfig, AudioSessionEvent, BoxFuture,
    JsonHttpClient, LlmFailure, RetryableClass, TurnDetection,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_rustls::rustls;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

/// The linear-PCM rate the Live API takes.
pub const LIVE_INPUT_RATE: u32 = 16000;
/// The linear-PCM rate the Live API returns.
pub const LIVE_OUTPUT_RATE: u32 = 24000;
const SERVICE_PATH: &str =
    "ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";
/// The endpoint an ephemeral token opens.
const CONSTRAINED_PATH: &str =
    "ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContentConstrained";
/// Where an ephemeral token is minted, under the provider's base URL.
const TOKEN_PATH: &str = "v1beta/auth_tokens";
/// Events buffered between the socket reader and [`AudioSession::next_event`].
const EVENT_BUFFER: usize = 256;

const WIRE_INPUT: AudioFormat = AudioFormat::L16 {
    rate: LIVE_INPUT_RATE,
    channels: 1,
};
const WIRE_OUTPUT: AudioFormat = AudioFormat::L16 {
    rate: LIVE_OUTPUT_RATE,
    channels: 1,
};

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

/// The Live endpoint under the provider's `base` URL (`https` → `wss`, `http` → `ws`, for a
/// local stub): the constrained endpoint an ephemeral token opens when `constrained`, the plain
/// one otherwise. The URL never carries a credential.
pub fn live_url(base: &str, constrained: bool) -> Result<String, LlmFailure> {
    let base = base.trim_end_matches('/');
    let socket = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if base.starts_with("wss://") || base.starts_with("ws://") {
        base.to_string()
    } else {
        return Err(permanent("the base URL is not an http(s) or ws(s) URL"));
    };
    let path = if constrained {
        CONSTRAINED_PATH
    } else {
        SERVICE_PATH
    };
    Ok(format!("{socket}/{path}"))
}

/// Mint a single-use ephemeral token for one Live session: `POST {base}/v1beta/auth_tokens`
/// with the key in the `x-goog-api-key` header. The token is the answer's `name`.
pub async fn ephemeral_token(
    http: &JsonHttpClient,
    base: &str,
    api_key: &str,
    deadline: Duration,
) -> Result<String, LlmFailure> {
    let base = base.trim_end_matches('/');
    let http_base = if let Some(rest) = base.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = base.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        base.to_string()
    };
    let answer = http
        .post_json(
            &format!("{http_base}/{TOKEN_PATH}"),
            &[(crate::API_KEY_HEADER, api_key)],
            &json!({"uses": 1}),
            deadline,
        )
        .await
        .map_err(LlmFailure::from)?;
    answer["name"]
        .as_str()
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .ok_or_else(|| permanent("the token service answered without a token"))
}

fn frame_format(format: AudioFormat) -> bool {
    matches!(
        format,
        AudioFormat::L16 { .. } | AudioFormat::Pcmu | AudioFormat::Pcma
    )
}

/// The instruction a transcription-only session is opened with when the caller gives none.
pub const TRANSCRIPTION_ONLY_INSTRUCTION: &str =
    "You are listening to a conversation in order to transcribe it. Never answer, never speak; stay silent.";

/// With server turn detection, how long the caller must be silent before the turn ends.
pub const END_OF_SPEECH_SILENCE_MS: u32 = 800;

/// The first client message for `config`.
pub fn live_setup(config: &AudioSessionConfig) -> Value {
    let model = config
        .model
        .strip_prefix("models/")
        .unwrap_or(&config.model);
    let mut generation = json!({"responseModalities": ["AUDIO"]});
    if let Some(voice) = &config.voice {
        generation["speechConfig"] =
            json!({"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": voice}}});
    }
    let mut setup = json!({
        "model": format!("models/{model}"),
        "generationConfig": generation,
        // Needed for the text-only mode, and cheap otherwise.
        "outputAudioTranscription": {},
    });
    if let Some(instructions) = &config.instructions {
        setup["systemInstruction"] = json!({"parts": [{"text": instructions}]});
    }
    if config.transcription_model.is_some() || config.transcription_only {
        setup["inputAudioTranscription"] = json!({});
    }
    if config.transcription_only && config.instructions.is_none() {
        // The Live API has no listen-only session: the model is asked to stay silent, and
        // whatever it says anyway is the caller's to ignore.
        setup["systemInstruction"] = json!({"parts": [{"text": TRANSCRIPTION_ONLY_INSTRUCTION}]});
    }
    setup["realtimeInputConfig"] = if config.turn_detection == TurnDetection::Manual {
        json!({"automaticActivityDetection": {"disabled": true}})
    } else {
        // People pause inside a sentence, on a phone line especially. With the API's default
        // end-of-speech sensitivity a short dip after the first word ended the caller's turn
        // and the model answered the fragment; a low sensitivity and the silence the API
        // recommends (500-800 ms) wait for the end of the sentence.
        json!({"automaticActivityDetection": {
            "endOfSpeechSensitivity": "END_SENSITIVITY_LOW",
            "silenceDurationMs": END_OF_SPEECH_SILENCE_MS,
        }})
    };
    json!({"setup": setup})
}

/// Per-session mapping state: transcript text accumulated for the current turn.
pub(crate) struct LiveState {
    wanted: Option<AudioFormat>,
    turn: u64,
    input: String,
    output: String,
}

impl LiveState {
    pub(crate) fn new(wanted: Option<AudioFormat>) -> LiveState {
        LiveState {
            wanted,
            turn: 0,
            input: String::new(),
            output: String::new(),
        }
    }

    fn flush(&mut self, events: &mut Vec<AudioSessionEvent>) {
        let input = std::mem::take(&mut self.input);
        if !input.is_empty() {
            events.push(AudioSessionEvent::InputTranscript {
                item_id: format!("in-{}", self.turn),
                text: input,
                is_final: true,
                speaker: None,
            });
        }
        let output = std::mem::take(&mut self.output);
        if !output.is_empty() {
            events.push(self.output_event(output, true));
        }
        self.turn += 1;
    }

    fn output_event(&self, text: String, is_final: bool) -> AudioSessionEvent {
        let item_id = format!("out-{}", self.turn);
        if self.wanted.is_some() {
            AudioSessionEvent::OutputTranscript {
                item_id,
                text,
                is_final,
            }
        } else {
            AudioSessionEvent::OutputText {
                item_id,
                text,
                is_final,
            }
        }
    }

    /// Map one server message to session events.
    pub(crate) fn map(&mut self, value: &Value) -> Vec<AudioSessionEvent> {
        let mut events = Vec::new();
        if value.get("setupComplete").is_some() {
            events.push(AudioSessionEvent::Ready);
        }
        if value.get("goAway").is_some() {
            events.push(AudioSessionEvent::Error {
                detail: "the server will close the session soon".into(),
            });
        }
        let content = &value["serverContent"];
        if !content.is_object() {
            return events;
        }
        for part in content["modelTurn"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let (Some(data), Some(wanted)) = (part["inlineData"]["data"].as_str(), self.wanted)
            else {
                continue;
            };
            let decoded = BASE64
                .decode(data)
                .map_err(|e| format!("an audio chunk is not base64: {e}"))
                .and_then(|raw| transcode(&raw, WIRE_OUTPUT, wanted).map_err(|e| e.to_string()));
            events.push(match decoded {
                Ok(data) => AudioSessionEvent::OutputAudio {
                    data,
                    format: wanted,
                },
                Err(detail) => AudioSessionEvent::Error { detail },
            });
        }
        if let Some(text) = content["inputTranscription"]["text"]
            .as_str()
            .filter(|t| !t.is_empty())
        {
            self.input.push_str(text);
            events.push(AudioSessionEvent::InputTranscript {
                item_id: format!("in-{}", self.turn),
                text: text.to_string(),
                is_final: false,
                speaker: None,
            });
        }
        if let Some(text) = content["outputTranscription"]["text"]
            .as_str()
            .filter(|t| !t.is_empty())
        {
            self.output.push_str(text);
            events.push(self.output_event(text.to_string(), false));
        }
        if content["interrupted"] == json!(true) {
            self.flush(&mut events);
        }
        if content["turnComplete"] == json!(true) {
            self.flush(&mut events);
            events.push(AudioSessionEvent::TurnEnded);
        }
        events
    }
}

fn frame_json(message: &Message) -> Option<Value> {
    match message {
        Message::Text(text) => serde_json::from_str(text.as_str()).ok(),
        Message::Binary(bytes) => serde_json::from_slice(bytes).ok(),
        _ => None,
    }
}

async fn connect(url: &str, token: Option<&str>) -> Result<Socket, LlmFailure> {
    // Errors below never repeat the token; the URL carries none.
    let mut request = url
        .into_client_request()
        .map_err(|e| permanent(format!("the live endpoint is not a WebSocket URL: {e}")))?;
    if let Some(token) = token {
        let value = HeaderValue::from_str(&format!("Token {token}"))
            .map_err(|_| permanent("the ephemeral token is not a valid header value"))?;
        request.headers_mut().insert("authorization", value);
    }
    let uri = request.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| permanent("the live endpoint has no host"))?
        .trim_matches(['[', ']'])
        .to_string();
    let tls = uri.scheme_str() == Some("wss");
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|e| unavailable(format!("connect to the live endpoint: {e}")))?;
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
            .map_err(|e| unavailable(format!("TLS handshake with the live endpoint: {e}")))?;
        Box::new(stream)
    } else {
        Box::new(tcp)
    };
    let (socket, _) = tokio_tungstenite::client_async(request, io)
        .await
        .map_err(|error| match error {
            WsError::Http(response) => {
                let status = response.status().as_u16();
                classify_status(
                    status,
                    None,
                    format!("the live handshake was refused: HTTP {status}"),
                )
            }
            other => unavailable(format!("the live handshake failed: {other}")),
        })?;
    Ok(socket)
}

/// An open Live session.
pub struct GeminiLiveSession {
    writer: Mutex<SplitSink<Socket, Message>>,
    events: Mutex<mpsc::Receiver<AudioSessionEvent>>,
    reader: tokio::task::JoinHandle<()>,
    input: AudioFormat,
    manual_turns: bool,
    /// Manual turns: `activityStart` has been sent for the turn in progress.
    activity_open: AtomicBool,
}

impl GeminiLiveSession {
    /// Mint the session's ephemeral token (with a key), connect, send the setup and wait for
    /// `setupComplete`, all within the config's deadline.
    pub async fn open(
        http: &JsonHttpClient,
        base: &str,
        api_key: Option<&str>,
        config: &AudioSessionConfig,
    ) -> Result<GeminiLiveSession, LlmFailure> {
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
        let url = live_url(base, api_key.is_some())?;
        let opened = async {
            let token = match api_key {
                Some(key) => Some(ephemeral_token(http, base, key, config.connect_deadline).await?),
                None => None,
            };
            let socket = connect(&url, token.as_deref()).await?;
            let (mut writer, mut stream) = socket.split();
            writer
                .send(Message::text(live_setup(config).to_string()))
                .await
                .map_err(|e| unavailable(format!("send the setup message: {e}")))?;
            loop {
                match stream.next().await {
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                        return Err(permanent(format!("the live session was refused: {reason}")));
                    }
                    Some(Ok(message)) => {
                        if frame_json(&message).is_some_and(|v| v.get("setupComplete").is_some()) {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        return Err(unavailable(format!("the live session dropped: {e}")))
                    }
                    None => return Err(unavailable("the live session closed while opening")),
                }
            }
            Ok::<_, LlmFailure>((writer, stream))
        };
        let (writer, stream) = tokio::time::timeout(config.connect_deadline, opened)
            .await
            .map_err(|_| LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                retry_after: None,
                detail: "the live session was not ready before the deadline".into(),
            })??;
        let (sender, receiver) = mpsc::channel(EVENT_BUFFER);
        let _ = sender.send(AudioSessionEvent::Ready).await;
        let reader = tokio::spawn(read_events(stream, sender, LiveState::new(config.output)));
        Ok(GeminiLiveSession {
            writer: Mutex::new(writer),
            events: Mutex::new(receiver),
            reader,
            input: config.input,
            manual_turns: config.turn_detection == TurnDetection::Manual,
            activity_open: AtomicBool::new(false),
        })
    }

    async fn send_json(&self, value: Value) -> Result<(), LlmFailure> {
        self.writer
            .lock()
            .await
            .send(Message::text(value.to_string()))
            .await
            .map_err(|e| unavailable(format!("the live session is closed: {e}")))
    }
}

async fn read_events(
    mut stream: SplitStream<Socket>,
    sender: mpsc::Sender<AudioSessionEvent>,
    mut state: LiveState,
) {
    while let Some(message) = stream.next().await {
        match message {
            Ok(Message::Close(_)) => break,
            Ok(message) => {
                let Some(value) = frame_json(&message) else {
                    continue;
                };
                for event in state.map(&value) {
                    if sender.send(event).await.is_err() {
                        return;
                    }
                }
            }
            Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => break,
            Err(e) => {
                let _ = sender
                    .send(AudioSessionEvent::Error {
                        detail: format!("the live connection failed: {e}"),
                    })
                    .await;
                break;
            }
        }
    }
    let _ = sender.send(AudioSessionEvent::Closed).await;
}

impl AudioSession for GeminiLiveSession {
    fn send_audio<'a>(&'a self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), LlmFailure>> {
        Box::pin(async move {
            let data =
                transcode(frame, self.input, WIRE_INPUT).map_err(|e| permanent(e.to_string()))?;
            if self.manual_turns && !self.activity_open.swap(true, Ordering::SeqCst) {
                self.send_json(json!({"realtimeInput": {"activityStart": {}}}))
                    .await?;
            }
            self.send_json(json!({"realtimeInput": {"audio": {
                "data": BASE64.encode(data),
                "mimeType": format!("audio/pcm;rate={LIVE_INPUT_RATE}"),
            }}}))
            .await
        })
    }

    fn commit(&self) -> BoxFuture<'_, Result<(), LlmFailure>> {
        Box::pin(async move {
            if self.manual_turns {
                self.activity_open.store(false, Ordering::SeqCst);
                self.send_json(json!({"realtimeInput": {"activityEnd": {}}}))
                    .await
            } else {
                self.send_json(json!({"realtimeInput": {"audioStreamEnd": true}}))
                    .await
            }
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

impl Drop for GeminiLiveSession {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn pcm(rate: u32) -> AudioFormat {
        AudioFormat::L16 { rate, channels: 1 }
    }

    fn config(output: Option<AudioFormat>) -> AudioSessionConfig {
        AudioSessionConfig {
            model: "gemini-live-test".into(),
            instructions: Some("be brief".into()),
            input: pcm(8000),
            output,
            voice: Some("Puck".into()),
            transcription_model: Some("any".into()),
            turn_detection: TurnDetection::Server,
            transcription_only: false,
            language: None,
            connect_deadline: Duration::from_secs(5),
        }
    }

    fn samples(value: i16, count: usize) -> Vec<u8> {
        std::iter::repeat_n(value, count)
            .flat_map(|s| s.to_le_bytes())
            .collect()
    }

    #[test]
    fn url_follows_the_base_scheme_and_never_carries_a_credential() {
        assert_eq!(
            live_url("https://generativelanguage.googleapis.com", true).unwrap(),
            format!("wss://generativelanguage.googleapis.com/{CONSTRAINED_PATH}")
        );
        assert_eq!(
            live_url("http://127.0.0.1:9/", false).unwrap(),
            format!("ws://127.0.0.1:9/{SERVICE_PATH}")
        );
        assert!(live_url("ftp://x", false).is_err());
        assert!(!live_url("https://x", true).unwrap().contains('?'));
    }

    #[test]
    fn setup_message_maps_the_config() {
        let setup = live_setup(&config(Some(pcm(8000))));
        assert_eq!(
            setup,
            json!({"setup": {
                "model": "models/gemini-live-test",
                "generationConfig": {
                    "responseModalities": ["AUDIO"],
                    "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig": {"voiceName": "Puck"}}},
                },
                "outputAudioTranscription": {},
                "systemInstruction": {"parts": [{"text": "be brief"}]},
                "inputAudioTranscription": {},
                "realtimeInputConfig": {"automaticActivityDetection": {
                    "endOfSpeechSensitivity": "END_SENSITIVITY_LOW",
                    "silenceDurationMs": END_OF_SPEECH_SILENCE_MS,
                }},
            }})
        );
        let mut manual = config(None);
        manual.turn_detection = TurnDetection::Manual;
        manual.transcription_model = None;
        manual.voice = None;
        manual.model = "models/m".into();
        let setup = &live_setup(&manual)["setup"];
        assert_eq!(setup["model"], "models/m");
        assert_eq!(
            setup["realtimeInputConfig"],
            json!({"automaticActivityDetection": {"disabled": true}})
        );
        assert!(setup.get("inputAudioTranscription").is_none());
        assert!(setup["generationConfig"].get("speechConfig").is_none());
        let mut listen = config(None);
        listen.transcription_only = true;
        listen.transcription_model = None;
        listen.instructions = None;
        let setup = &live_setup(&listen)["setup"];
        assert_eq!(setup["inputAudioTranscription"], json!({}));
        assert_eq!(
            setup["systemInstruction"]["parts"][0]["text"],
            TRANSCRIPTION_ONLY_INSTRUCTION
        );
    }

    #[test]
    fn server_messages_map_to_session_events() {
        let mut state = LiveState::new(Some(pcm(8000)));
        assert_eq!(
            state.map(&json!({"setupComplete": {}})),
            vec![AudioSessionEvent::Ready]
        );
        let events = state.map(&json!({"serverContent": {"modelTurn": {"parts": [
            {"inlineData": {"mimeType": "audio/pcm;rate=24000",
                            "data": BASE64.encode(samples(1000, 240))}}]}}}));
        match &events[..] {
            [AudioSessionEvent::OutputAudio { data, format }] => {
                assert_eq!(*format, pcm(8000));
                assert_eq!(data.len(), 80 * 2, "24 kHz resampled to 8 kHz");
            }
            other => panic!("{other:?}"),
        }
        state.map(&json!({"serverContent": {"inputTranscription": {"text": "hel"}}}));
        state.map(&json!({"serverContent": {"inputTranscription": {"text": "lo"}}}));
        state.map(&json!({"serverContent": {"outputTranscription": {"text": "hi"}}}));
        let end = state.map(&json!({"serverContent": {"turnComplete": true}}));
        assert_eq!(
            end,
            vec![
                AudioSessionEvent::InputTranscript {
                    item_id: "in-0".into(),
                    text: "hello".into(),
                    is_final: true,
                    speaker: None
                },
                AudioSessionEvent::OutputTranscript {
                    item_id: "out-0".into(),
                    text: "hi".into(),
                    is_final: true
                },
                AudioSessionEvent::TurnEnded,
            ]
        );
        // The next turn gets fresh ids.
        let next = state.map(&json!({"serverContent": {"outputTranscription": {"text": "x"}}}));
        assert_eq!(
            next,
            vec![AudioSessionEvent::OutputTranscript {
                item_id: "out-1".into(),
                text: "x".into(),
                is_final: false
            }]
        );
        assert!(state.map(&json!({"usageMetadata": {}})).is_empty());
    }

    #[test]
    fn text_only_sessions_report_output_text_and_drop_audio() {
        let mut state = LiveState::new(None);
        let events = state.map(&json!({"serverContent": {
            "modelTurn": {"parts": [{"inlineData": {"data": BASE64.encode(samples(1, 4))}}]},
            "outputTranscription": {"text": "yo"}}}));
        assert_eq!(
            events,
            vec![AudioSessionEvent::OutputText {
                item_id: "out-0".into(),
                text: "yo".into(),
                is_final: false
            }]
        );
    }

    /// The token the fake token service mints.
    const EPHEMERAL: &str = "auth_tokens/eph-1";

    /// Answer one HTTP request on `tcp` as the token service does, checking it carries the key
    /// in the `x-goog-api-key` header and nowhere in the request line.
    async fn serve_token(mut tcp: tokio::net::TcpStream) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut raw = Vec::new();
        let mut buf = [0u8; 1024];
        let (head_end, length) = loop {
            let n = tcp.read(&mut buf).await.unwrap();
            assert!(n > 0, "the token request ended early");
            raw.extend_from_slice(&buf[..n]);
            if let Some(i) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&raw[..i]).to_ascii_lowercase();
                let length = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .map(|v| v.trim().parse::<usize>().unwrap())
                    .unwrap_or(0);
                break (i + 4, length);
            }
        };
        while raw.len() < head_end + length {
            let n = tcp.read(&mut buf).await.unwrap();
            raw.extend_from_slice(&buf[..n]);
        }
        let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
        let request_line = head.lines().next().unwrap();
        assert_eq!(request_line, format!("POST /{TOKEN_PATH} HTTP/1.1"));
        assert!(
            !request_line.contains("test-key"),
            "the key is not in the URL"
        );
        assert!(
            head.lines()
                .any(|l| l.eq_ignore_ascii_case("x-goog-api-key: test-key")),
            "the key travels in the header: {head}"
        );
        let body: Value = serde_json::from_slice(&raw[head_end..head_end + length]).unwrap();
        assert_eq!(body, json!({"uses": 1}));
        let answer = json!({"name": EPHEMERAL}).to_string();
        tcp.write_all(
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
                 connection: close\r\n\r\n{answer}",
                answer.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let _ = tcp.shutdown().await;
    }

    /// An in-process stand-in for the Live endpoint (and, for a keyed session, the token
    /// service). It checks the handshake — the constrained endpoint with `Authorization: Token
    /// <ephemeral>` and no query string when keyed, the plain endpoint without credentials
    /// otherwise — answers the setup with `setupComplete` in a BINARY frame (as the real service
    /// does), and answers each audio frame with a transcription, 24 kHz audio, an output
    /// transcription and `turnComplete`.
    #[allow(clippy::result_large_err)] // the handshake callback signature is tungstenite's
    async fn fake_live(keyed: bool) -> (String, tokio::task::JoinHandle<Vec<Value>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            if keyed {
                let (tcp, _) = listener.accept().await.unwrap();
                serve_token(tcp).await;
            }
            let (tcp, _) = listener.accept().await.unwrap();
            let mut seen_path = String::new();
            let mut seen_auth = None;
            let mut socket = tokio_tungstenite::accept_hdr_async(
                tcp,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    seen_path = request.uri().to_string();
                    seen_auth = request
                        .headers()
                        .get("authorization")
                        .map(|v| v.to_str().unwrap().to_string());
                    Ok(response)
                },
            )
            .await
            .unwrap();
            assert!(!seen_path.contains('?'), "no query string: {seen_path}");
            assert!(!seen_path.contains("key="), "no key in the URL");
            if keyed {
                assert_eq!(seen_path, format!("/{CONSTRAINED_PATH}"));
                assert_eq!(seen_auth, Some(format!("Token {EPHEMERAL}")));
            } else {
                assert_eq!(seen_path, format!("/{SERVICE_PATH}"));
                assert_eq!(seen_auth, None);
            }
            let mut received = Vec::new();
            while let Some(Ok(message)) = socket.next().await {
                let Message::Text(text) = message else {
                    if matches!(message, Message::Close(_)) {
                        break;
                    }
                    continue;
                };
                let value: Value = serde_json::from_str(text.as_str()).unwrap();
                received.push(value.clone());
                if value.get("setup").is_some() {
                    socket
                        .send(Message::binary(json!({"setupComplete": {}}).to_string()))
                        .await
                        .unwrap();
                } else if value["realtimeInput"].get("audio").is_some() {
                    for event in [
                        json!({"serverContent": {"inputTranscription": {"text": "STUB IN"}}}),
                        json!({"serverContent": {"modelTurn": {"parts": [{"inlineData": {
                            "mimeType": "audio/pcm;rate=24000",
                            "data": BASE64.encode(samples(500, 720))}}]}}}),
                        json!({"serverContent": {"outputTranscription": {"text": "STUB OUT"}}}),
                        json!({"serverContent": {"turnComplete": true}}),
                    ] {
                        socket
                            .send(Message::binary(event.to_string()))
                            .await
                            .unwrap();
                    }
                }
            }
            let _ = socket.close(None).await;
            received
        });
        (base, server)
    }

    fn http() -> JsonHttpClient {
        JsonHttpClient::new(Duration::from_secs(5))
    }

    #[tokio::test]
    async fn session_round_trips_against_a_fake_endpoint() {
        let (base, server) = fake_live(true).await;
        let session =
            GeminiLiveSession::open(&http(), &base, Some("test-key"), &config(Some(pcm(8000))))
                .await
                .expect("open");
        assert_eq!(session.next_event().await, Some(AudioSessionEvent::Ready));
        session.send_audio(&samples(300, 160)).await.unwrap();
        session.commit().await.unwrap();
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
                    item_id: "in-0".into(),
                    text: "STUB IN".into(),
                    is_final: false,
                    speaker: None,
                },
                AudioSessionEvent::OutputAudio {
                    data: samples(500, 240),
                    format: pcm(8000),
                },
                AudioSessionEvent::OutputTranscript {
                    item_id: "out-0".into(),
                    text: "STUB OUT".into(),
                    is_final: false,
                },
                AudioSessionEvent::InputTranscript {
                    item_id: "in-0".into(),
                    text: "STUB IN".into(),
                    is_final: true,
                    speaker: None,
                },
                AudioSessionEvent::OutputTranscript {
                    item_id: "out-0".into(),
                    text: "STUB OUT".into(),
                    is_final: true,
                },
                AudioSessionEvent::TurnEnded,
            ]
        );
        session.close().await;
        let mut last = None;
        while let Some(event) = session.next_event().await {
            last = Some(event);
        }
        assert_eq!(last, Some(AudioSessionEvent::Closed));
        let received = server.await.unwrap();
        assert!(received[0].get("setup").is_some());
        let audio = &received[1]["realtimeInput"]["audio"];
        assert_eq!(audio["mimeType"], "audio/pcm;rate=16000");
        assert_eq!(
            BASE64
                .decode(audio["data"].as_str().unwrap())
                .unwrap()
                .len(),
            320 * 2,
            "8 kHz frames go up at 16 kHz"
        );
        assert_eq!(
            received[2],
            json!({"realtimeInput": {"audioStreamEnd": true}})
        );
    }

    #[tokio::test]
    async fn manual_turns_bracket_audio_with_activity_markers() {
        // Keyless: the plain endpoint, no credentials.
        let (base, server) = fake_live(false).await;
        let mut cfg = config(Some(pcm(8000)));
        cfg.turn_detection = TurnDetection::Manual;
        let session = GeminiLiveSession::open(&http(), &base, None, &cfg)
            .await
            .expect("open");
        session.send_audio(&samples(1, 160)).await.unwrap();
        session.send_audio(&samples(1, 160)).await.unwrap();
        session.commit().await.unwrap();
        session.close().await;
        let received = server.await.unwrap();
        let kinds: Vec<String> = received[1..]
            .iter()
            .map(|v| {
                v["realtimeInput"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .next()
                    .unwrap()
                    .clone()
            })
            .collect();
        assert_eq!(kinds, ["activityStart", "audio", "audio", "activityEnd"]);
        assert_eq!(
            received[0]["setup"]["realtimeInputConfig"]["automaticActivityDetection"]["disabled"],
            true
        );
    }

    #[tokio::test]
    async fn wav_frames_and_unreachable_endpoints_fail_cleanly() {
        let mut wav = config(None);
        wav.input = AudioFormat::Wav;
        assert!(matches!(
            GeminiLiveSession::open(&http(), "http://127.0.0.1:9", None, &wav).await,
            Err(LlmFailure::Permanent { .. })
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        assert!(matches!(
            GeminiLiveSession::open(&http(), &format!("http://{addr}"), None, &config(None)).await,
            Err(LlmFailure::Retryable {
                class: RetryableClass::Unavailable,
                ..
            })
        ));
    }
}
