//! Shared fixtures: a fake provider keyed by model name, a test resolver, a capturing
//! reply port and channel-YAML helpers.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sutra_channels::dispatch::InboundMessage;
use sutra_channels::{BoxFuture, ChannelDefinition, Diagnostic, OutboundMessage};
use sutra_llm_spi::{
    AudioFormat, LlmAudio, LlmSpeechRequest, LlmTranscript, LlmTranscriptionRequest,
};
use sutra_llm_spi::{
    FinishReason, LlmCallbackEvent, LlmCapabilities, LlmFailure, LlmJobRef, LlmJobState,
    LlmProvider, LlmProviderConfig, LlmProviderFactory, LlmRequest, LlmResponse, LlmToolCall,
    LlmUsage, RetryableClass,
};
use sutra_transport_llm::{parse_channels, ChannelTable, LlmSink, ReplyPort};

pub const FAKE: &str = "test-fake";
pub const TENANT: &str = "acme";
pub const MODULE_KEY: &str = "acme/orders/1.0.0";
pub const ANSWER_TEXT: &str = "THE-ANSWER-TEXT";

/// Behaviour is chosen by the model name:
/// `ok*` answers; `rate-limited` / `unavailable` / `timeout` fail retryably; `permanent`
/// fails permanently; `no-tools*` answers but lacks tool support; `slow-<tag>` answers after
/// a pause, tracking peak concurrency under `<tag>`; `hang` never answers in time;
/// `rate-limited-after-part` streams a piece before failing.
/// `job*` models support async jobs (`job-fail*` refuses the submit retryably) and, unless
/// `job-poll*`, job callbacks; a job's state is set with [`set_job_state`].
struct FakeProvider {
    model: String,
}

#[derive(Default)]
pub struct Peak {
    pub current: AtomicUsize,
    pub max: AtomicUsize,
}

pub fn peak(tag: &str) -> Arc<Peak> {
    static PEAKS: OnceLock<Mutex<HashMap<String, Arc<Peak>>>> = OnceLock::new();
    PEAKS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .entry(tag.to_string())
        .or_default()
        .clone()
}

fn answer(model: &str) -> LlmResponse {
    LlmResponse {
        text: ANSWER_TEXT.to_string(),
        tool_calls: vec![LlmToolCall {
            id: "call-1".into(),
            name: "lookup".into(),
            arguments: serde_json::json!({"id": 7}),
        }],
        finish: FinishReason::Stop,
        usage: LlmUsage {
            input_tokens: 11,
            output_tokens: 22,
        },
        provider_request_id: Some(format!("req-{model}")),
        audio: None,
        transcript: None,
    }
}

/// The header the fake verifies callbacks by: it must equal the secret.
pub const SIGNATURE_HEADER: &str = "x-test-signature";

fn jobs() -> &'static Mutex<HashMap<String, (LlmJobState, usize)>> {
    static JOBS: OnceLock<Mutex<HashMap<String, (LlmJobState, usize)>>> = OnceLock::new();
    JOBS.get_or_init(Default::default)
}

/// Set a submitted job's state as the fake provider will report it.
pub fn set_job_state(id: &str, state: LlmJobState) {
    jobs()
        .lock()
        .unwrap()
        .entry(id.to_string())
        .or_insert((LlmJobState::Pending, 0))
        .0 = state;
}

/// A finished job's response.
pub fn done() -> LlmJobState {
    LlmJobState::Done(answer("job"))
}

/// How often a job was polled.
pub fn polls(id: &str) -> usize {
    jobs().lock().unwrap().get(id).map(|(_, n)| *n).unwrap_or(0)
}

fn retryable(class: RetryableClass) -> LlmFailure {
    LlmFailure::Retryable {
        class,
        retry_after: None,
        detail: format!("fake {}", class.as_str()),
    }
}

/// Every request a fake model was sent, by model.
fn sent() -> &'static Mutex<HashMap<String, Vec<LlmRequest>>> {
    static SENT: OnceLock<Mutex<HashMap<String, Vec<LlmRequest>>>> = OnceLock::new();
    SENT.get_or_init(Default::default)
}

/// The last request `model` was sent.
pub fn last_request(model: &str) -> Option<LlmRequest> {
    sent()
        .lock()
        .unwrap()
        .get(model)
        .and_then(|r| r.last().cloned())
}

/// What the `speech*` fake transcribes every clip as.
pub const FAKE_TRANSCRIPT: &str = "FAKE TRANSCRIPT";
/// The transcript of an `audio*` fake's spoken answer.
pub const FAKE_SPOKEN: &str = "FAKE SPOKEN ANSWER";

/// `ms` of a 24 kHz mono tone as a WAV.
pub fn wav_24k(ms: u32) -> Vec<u8> {
    let n = (24 * ms) as usize;
    sutra_llm_spi::audio::wav_wrap(&sutra_llm_spi::audio::Pcm {
        rate: 24000,
        channels: 1,
        samples: (0..n).map(|i| ((i % 48) as i16 - 24) * 400).collect(),
    })
}

impl LlmProvider for FakeProvider {
    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        sent()
            .lock()
            .unwrap()
            .entry(self.model.clone())
            .or_default()
            .push(request.clone());
        Box::pin(async move {
            let model = self.model.as_str();
            if let (true, Some(format)) = (model.starts_with("audio"), request.audio_output) {
                let mut response = answer(model);
                response.text = FAKE_SPOKEN.into();
                response.tool_calls.clear();
                response.audio = Some(LlmAudio {
                    data: wav_24k(100),
                    format,
                });
                response.transcript = Some(FAKE_SPOKEN.into());
                return Ok(response);
            }
            match model {
                "rate-limited" => Err(retryable(RetryableClass::RateLimited)),
                "unavailable" => Err(retryable(RetryableClass::Unavailable)),
                "timeout" => Err(retryable(RetryableClass::Timeout)),
                "permanent" => Err(LlmFailure::Permanent {
                    detail: "fake contract reject".into(),
                }),
                "hang" => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Ok(answer(model))
                }
                _ if model.starts_with("slow-") => {
                    let peak = peak(&model["slow-".len()..]);
                    let now = peak.current.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.max.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    peak.current.fetch_sub(1, Ordering::SeqCst);
                    Ok(answer(model))
                }
                _ => Ok(answer(model)),
            }
        })
    }

    /// `rate-limited-after-part` streams one piece, then fails retryably (a mid-stream
    /// failure the router falls back from); every other model streams its whole answer.
    fn complete_streaming<'a>(
        &'a self,
        request: &'a LlmRequest,
        on_chunk: sutra_llm_spi::ChunkSink<'a>,
    ) -> BoxFuture<'a, Result<LlmResponse, LlmFailure>> {
        Box::pin(async move {
            if self.model == "rate-limited-after-part" {
                on_chunk("partial-");
                return Err(retryable(RetryableClass::RateLimited));
            }
            let response = self.complete(request).await?;
            on_chunk(&response.text);
            Ok(response)
        })
    }

    /// `audio*` models take and speak WAV and hear the voice; `video*` models read video;
    /// `speech*` models transcribe and synthesise (WAV).
    fn capabilities(&self) -> LlmCapabilities {
        let job = self.model.starts_with("job");
        let audio = self.model.starts_with("audio");
        let speech = self.model.starts_with("speech");
        LlmCapabilities {
            tools: !self.model.starts_with("no-tools"),
            json_output: true,
            async_jobs: job || self.model.starts_with("audio-job"),
            job_callbacks: job && !self.model.starts_with("job-poll"),
            audio_input: audio,
            audio_output: audio,
            audio_understanding: audio,
            video_understanding: self.model.starts_with("video"),
            transcription: speech,
            speech_synthesis: speech,
            realtime: self.model.starts_with("live"),
            ..LlmCapabilities::default()
        }
    }

    fn audio_input_formats(&self) -> Vec<AudioFormat> {
        if self.model.starts_with("audio") {
            vec![AudioFormat::Wav]
        } else {
            Vec::new()
        }
    }

    fn audio_output_formats(&self) -> Vec<AudioFormat> {
        self.audio_input_formats()
    }

    fn transcription_formats(&self) -> Vec<AudioFormat> {
        vec![AudioFormat::Wav]
    }

    fn speech_formats(&self) -> Vec<AudioFormat> {
        vec![AudioFormat::Wav]
    }

    fn transcribe<'a>(
        &'a self,
        request: &'a LlmTranscriptionRequest,
    ) -> BoxFuture<'a, Result<LlmTranscript, LlmFailure>> {
        Box::pin(async move {
            if request.audio.format != AudioFormat::Wav {
                return Err(LlmFailure::Permanent {
                    detail: "fake transcribes WAV only".into(),
                });
            }
            Ok(LlmTranscript {
                text: format!("{FAKE_TRANSCRIPT} ({})", request.model),
            })
        })
    }

    fn synthesize<'a>(
        &'a self,
        request: &'a LlmSpeechRequest,
    ) -> BoxFuture<'a, Result<LlmAudio, LlmFailure>> {
        Box::pin(async move {
            Ok(LlmAudio {
                data: wav_24k(10 * request.text.len() as u32),
                format: request.format,
            })
        })
    }

    /// `live-down` is unavailable, `live-refused` refuses permanently; any other `live*` model
    /// opens a session that closes when asked. Every attempt is recorded ([`opened_sessions`]).
    fn open_audio_session<'a>(
        &'a self,
        config: &'a sutra_llm_spi::AudioSessionConfig,
    ) -> BoxFuture<'a, Result<Box<dyn sutra_llm_spi::AudioSession>, LlmFailure>> {
        Box::pin(async move {
            sessions().lock().unwrap().push(config.clone());
            match self.model.as_str() {
                "live-down" => Err(retryable(RetryableClass::Unavailable)),
                "live-refused" => Err(LlmFailure::Permanent {
                    detail: "fake session reject".into(),
                }),
                _ => Ok(Box::new(IdleSession) as Box<dyn sutra_llm_spi::AudioSession>),
            }
        })
    }

    fn submit<'a>(
        &'a self,
        _request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmJobRef, LlmFailure>> {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        Box::pin(async move {
            if self.model.starts_with("job-fail") {
                return Err(retryable(RetryableClass::Unavailable));
            }
            let id = format!("{}-{}", self.model, SEQ.fetch_add(1, Ordering::SeqCst));
            jobs()
                .lock()
                .unwrap()
                .insert(id.clone(), (LlmJobState::Pending, 0));
            Ok(LlmJobRef { id })
        })
    }

    fn poll<'a>(&'a self, job: &'a LlmJobRef) -> BoxFuture<'a, Result<LlmJobState, LlmFailure>> {
        Box::pin(async move {
            let mut jobs = jobs().lock().unwrap();
            match jobs.get_mut(&job.id) {
                Some((state, polls)) => {
                    *polls += 1;
                    Ok(state.clone())
                }
                None => Err(LlmFailure::Permanent {
                    detail: "unknown job".into(),
                }),
            }
        })
    }

    /// Body `{"job": "<id>", "done": true|false}`; no `done` means "fetch it".
    fn parse_callback(
        &self,
        _headers: &BTreeMap<String, String>,
        body: &[u8],
    ) -> Result<LlmCallbackEvent, LlmFailure> {
        let value: serde_json::Value =
            serde_json::from_slice(body).map_err(|_| LlmFailure::Permanent {
                detail: "not json".into(),
            })?;
        let id = value["job"].as_str().ok_or(LlmFailure::Permanent {
            detail: "no job".into(),
        })?;
        let state = value["done"].as_bool().map(|done| {
            if done {
                LlmJobState::Done(answer(&self.model))
            } else {
                LlmJobState::Pending
            }
        });
        Ok(LlmCallbackEvent {
            job: LlmJobRef { id: id.to_string() },
            state,
        })
    }

    fn verify_callback(
        &self,
        headers: &BTreeMap<String, String>,
        _body: &[u8],
        secret: &str,
    ) -> bool {
        headers.get(SIGNATURE_HEADER).map(String::as_str) == Some(secret)
    }
}

fn sessions() -> &'static Mutex<Vec<sutra_llm_spi::AudioSessionConfig>> {
    static SESSIONS: OnceLock<Mutex<Vec<sutra_llm_spi::AudioSessionConfig>>> = OnceLock::new();
    SESSIONS.get_or_init(Default::default)
}

/// Every session configuration a `live*` fake was asked to open, as passed to it.
pub fn opened_sessions() -> Vec<sutra_llm_spi::AudioSessionConfig> {
    sessions().lock().unwrap().clone()
}

/// A live session that never reports anything until closed.
struct IdleSession;

impl sutra_llm_spi::AudioSession for IdleSession {
    fn send_audio<'a>(&'a self, _frame: &'a [u8]) -> BoxFuture<'a, Result<(), LlmFailure>> {
        Box::pin(async { Ok(()) })
    }
    fn commit(&self) -> BoxFuture<'_, Result<(), LlmFailure>> {
        Box::pin(async { Ok(()) })
    }
    fn next_event(&self) -> BoxFuture<'_, Option<sutra_llm_spi::AudioSessionEvent>> {
        Box::pin(async { Some(sutra_llm_spi::AudioSessionEvent::Closed) })
    }
    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

fn build_fake(config: &LlmProviderConfig) -> Result<Arc<dyn LlmProvider>, Diagnostic> {
    Ok(Arc::new(FakeProvider {
        model: config.model.clone(),
    }))
}

inventory::submit! {
    LlmProviderFactory { provider: FAKE, build: build_fake }
}

/// `env:<name>` resolves to `resolved:<name>`; anything else is refused.
pub fn resolver(reference: &str) -> Result<String, String> {
    reference
        .strip_prefix("env:")
        .map(|name| format!("resolved:{name}"))
        .ok_or_else(|| "unsupported reference".to_string())
}

pub fn definitions(yaml: &str) -> Vec<ChannelDefinition> {
    sutra_channels::load_channel_definitions(yaml.as_bytes(), "acme", "orders", "1.0.0", "t.yaml")
        .expect("channel yaml loads")
}

/// One outbound `ask` channel replying on `answers`, with `extra` appended under `llm:`
/// (indented four spaces) and the given candidate models (all on the fake provider).
pub fn channel_yaml(models: &[&str], extra: &str) -> String {
    let mut yaml = String::from(
        "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
         bind: \"llm://ask\"\n    llm:\n      reply-channel: answers\n      candidates:\n",
    );
    for model in models {
        yaml.push_str(&format!(
            "        - provider: {FAKE}\n          model: {model}\n"
        ));
    }
    for line in extra.lines() {
        yaml.push_str("      ");
        yaml.push_str(line);
        yaml.push('\n');
    }
    yaml
}

pub fn table(yaml: &str) -> Arc<ChannelTable> {
    let (map, failures) = parse_channels(&definitions(yaml), resolver);
    assert!(
        failures.is_empty(),
        "unexpected config failures: {failures:?}"
    );
    Arc::new(ChannelTable::from_map(map))
}

#[derive(Default)]
pub struct CapturePort {
    pub delivered: Mutex<Vec<InboundMessage>>,
    pub fail_with: Option<Diagnostic>,
}

impl ReplyPort for CapturePort {
    fn deliver<'a>(&'a self, message: InboundMessage) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            if let Some(d) = &self.fail_with {
                return Err(d.clone());
            }
            self.delivered.lock().unwrap().push(message);
            Ok(())
        })
    }
}

pub fn sink(yaml: &str, port: Arc<CapturePort>) -> LlmSink {
    LlmSink::new(table(yaml), port)
}

pub fn outbound(body: serde_json::Value) -> OutboundMessage {
    let mut headers = BTreeMap::new();
    headers.insert("x-order-id".to_string(), "42".to_string());
    OutboundMessage {
        destination: format!("llm://{MODULE_KEY}/ask"),
        headers,
        body: serde_json::to_vec(&body).unwrap(),
        content_type: Some("application/json".into()),
        outbox_key: "ob-1".into(),
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
    }
}
