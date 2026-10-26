//! Live audio sessions: a streaming, two-way audio conversation with a model (a provider's
//! realtime / live API). Frames go in as they are captured; transcripts, the model's audio and
//! its text come back as events. Opened through [`crate::LlmProvider::open_audio_session`],
//! which a provider without a live API leaves at its "unsupported" default.

use std::time::Duration;

use crate::{AudioFormat, BoxFuture, LlmFailure};

/// Who decides that the caller's turn has ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TurnDetection {
    /// The provider detects the end of speech and answers on its own.
    #[default]
    Server,
    /// The caller ends each turn with [`AudioSession::commit`].
    Manual,
}

/// How to open a session.
#[derive(Debug, Clone, PartialEq)]
pub struct AudioSessionConfig {
    /// The provider's live model id.
    pub model: String,
    pub instructions: Option<String>,
    /// The format of every frame passed to [`AudioSession::send_audio`]. The adapter converts it
    /// to what the provider takes.
    pub input: AudioFormat,
    /// The format [`AudioSessionEvent::OutputAudio`] is delivered in; `None` asks for a text-only
    /// answer.
    pub output: Option<AudioFormat>,
    pub voice: Option<String>,
    /// Transcribe the caller's audio with this model (the provider's transcription model id);
    /// `None` leaves input transcription off.
    pub transcription_model: Option<String>,
    pub turn_detection: TurnDetection,
    /// Listen and transcribe only: the model never answers (a provider with a transcription
    /// session type opens one; one without it answers in text, which the caller ignores).
    pub transcription_only: bool,
    /// The spoken language, as a hint (an ISO-639-1 code).
    pub language: Option<String>,
    /// Bound on connecting and configuring the session.
    pub connect_deadline: Duration,
}

/// What a live session reports.
#[derive(Debug, Clone, PartialEq)]
pub enum AudioSessionEvent {
    /// The provider accepted the session configuration.
    Ready,
    /// Text of the caller's speech. Deltas arrive with `is_final: false`, the completed segment
    /// with `is_final: true`; segments are keyed by `item_id` and may complete out of order.
    /// `speaker` is set only by a provider that labels speakers.
    InputTranscript {
        item_id: String,
        text: String,
        is_final: bool,
        speaker: Option<String>,
    },
    /// The provider detected the start of speech, `audio_ms` into the audio sent so far.
    SpeechStarted { item_id: String, audio_ms: u64 },
    /// The provider detected the end of that speech.
    SpeechStopped { item_id: String, audio_ms: u64 },
    /// Text of what the model is saying.
    OutputTranscript {
        item_id: String,
        text: String,
        is_final: bool,
    },
    /// Text the model answered with (a text-modality answer).
    OutputText {
        item_id: String,
        text: String,
        is_final: bool,
    },
    /// The model's audio, in the session's configured output format.
    OutputAudio { data: Vec<u8>, format: AudioFormat },
    /// The model finished one answer.
    TurnEnded,
    /// The provider reported an error; the session may still be open. Never carries audio or
    /// transcript content.
    Error { detail: String },
    /// The session is over; no event follows.
    Closed,
}

/// An open live session. Methods take `&self`, so one task can send frames while another reads
/// events.
pub trait AudioSession: Send + Sync {
    /// Send one captured frame, in the configured input format.
    fn send_audio<'a>(&'a self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), LlmFailure>>;

    /// End the caller's turn (with [`TurnDetection::Manual`], also asks for the answer).
    fn commit(&self) -> BoxFuture<'_, Result<(), LlmFailure>>;

    /// The next event; `None` once [`AudioSessionEvent::Closed`] has been delivered.
    fn next_event(&self) -> BoxFuture<'_, Option<AudioSessionEvent>>;

    /// Close the session.
    fn close(&self) -> BoxFuture<'_, ()>;
}
