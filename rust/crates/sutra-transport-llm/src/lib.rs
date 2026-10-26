//! The `llm` request/reply transport.
//!
//! A process calls a language model through an ordinary channel call on an OUTBOUND
//! channel (`transport: llm`, `bind: llm://<name>`). The outbox dispatcher hands the call
//! to [`LlmSink`], which:
//!
//! 1. resolves the qualified `llm://<tenant>/<module>/<version>/<channel>` destination to
//!    the channel's [`LlmChannel`] in the shared [`ChannelTable`];
//! 2. parses the JSON request body ([`parse_request_body`]) and reads any media references
//!    from the channel's object store ([`resolve_media`]);
//! 3. waits for one of the channel's `llm.concurrency` permits, then [`route`]s the call
//!    across the ordered `llm.candidates`, skipping candidates that lack a needed
//!    capability and falling back on the `llm.fallback-on` failure classes;
//! 4. delivers the answer into the engine as an inbound message on `llm.reply-channel`
//!    (same deployment), deduplicated on the outbox key, which the process correlates
//!    like any other inbound message.
//!
//! AUDIO AND VIDEO. A user message may carry `audio` / `video`, and `audioOutput` asks for a
//! spoken answer. Audio stays audio: a candidate that cannot take what the call carries is
//! skipped, never sent a text-only version of it; the transport converts between the formats
//! the call states and the formats the candidate takes or speaks (WAV, L16, G.711). With
//! `llm.voice: pipeline` the `llm.speech` provider transcribes the audio, the text model
//! answers, and the speech provider speaks the answer — the reply then says
//! `audioPreserved: false`.
//!
//! With `llm.mode: job` the sink instead submits a provider job, records it in the
//! `llm_job` table and returns; the [`jobs`] poller (and, for `completion: callback`
//! candidates, the [`callback`] endpoint served for inbound `transport: llm` channels)
//! completes it later and delivers the answer the same way.
//!
//! CHANNELS FOR OTHER TRANSPORTS. Every live `mode: call` channel is also published to the
//! provider SPI's channel registry ([`sutra_llm_spi::llm_channels`], see [`handle`]), so a
//! transport that needs a model for its own media plane (live transcription, per-segment video
//! analysis) names an `llm` channel of its deployment and is served by the same routing.
//!
//! Providers come from the provider SPI (`sutra-llm-spi`); this crate names none. No
//! prompt or answer text is written to spans, logs or diagnostics.
#![forbid(unsafe_code)]

use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use sutra_channels::http::EngineHandle;
use sutra_channels::{ChannelDefinition, Diagnostic, MessageSink, SinkRegistry};
use sutra_transport_spi::{
    EnvRefResolver, InlineCallerFactory, ReplySinkFactory, TransportChannels, TransportFactory,
};

pub mod callback;
pub mod config;
pub mod handle;
pub mod inline;
pub mod job_store;
pub mod jobs;
pub mod media;
pub mod request;
pub mod sink;

pub use callback::callback_router;
pub use config::{
    parse_all, parse_callback_channel, parse_channel, parse_channels, CallbackMap, CallbackTable,
    CandidateJob, ChannelMap, ChannelMode, ChannelTable, Completion, LlmCallbackChannel,
    LlmCandidate, LlmChannel, ParsedChannels, SpeechStage, VoiceMode, CALLBACK_PATH_PREFIX,
    DEFAULT_CALLBACK_POLL_INTERVAL_SECONDS, DEFAULT_CONCURRENCY, DEFAULT_FALLBACK_ON,
    DEFAULT_JOB_TIMEOUT_SECONDS, DEFAULT_POLL_INTERVAL_SECONDS, DEFAULT_TIMEOUT_SECONDS,
};
pub use handle::{channel_handles, publish_channels, RoutedChannelHandle};
pub use inline::LlmInlineCaller;
pub use job_store::{JobClaim, JobStoreSlot, LlmJobRow, LlmJobStore, NewLlmJob};
pub use jobs::{JobOutcome, JobRunner, PollerHandle, DEFAULT_POLLER_TICK};
pub use media::{resolve_media, MediaFailure};
pub use request::{
    echo_without_media, parse_destination, parse_request_body, CallRequest, MediaReference,
    MediaSlot,
};
pub use sink::{
    build_reply, failure_outcome, reply_body, route, route_pipeline, route_streaming, route_submit,
    structured_answer, structured_answer_error, LlmSink, ReplyPort, RouteFailure, RoutedAnswer,
    SubmittedJob, MODEL_HEADER, PROVIDER_HEADER,
};

/// The `transport:` value and URI scheme.
pub const TRANSPORT: &str = "llm";

/// The process-wide table: written by [`spawn`] and [`LlmTransport::rewire`], read by the
/// sink the engine builds through the [`ReplySinkFactory`].
static TABLE: LazyLock<Arc<ChannelTable>> = LazyLock::new(|| Arc::new(ChannelTable::new()));
/// The process-wide callback channels, served by [`LlmTransport::inbound_router`].
static CALLBACKS: LazyLock<Arc<CallbackTable>> = LazyLock::new(|| Arc::new(CallbackTable::new()));
/// The job store the sink records submissions in; set by [`spawn`] when a pool is given.
static JOBS: LazyLock<Arc<JobStoreSlot>> = LazyLock::new(|| Arc::new(JobStoreSlot::new()));

/// Persistence for `llm.mode: job`: the runner settling jobs and, once started, its poller.
pub struct JobSupport {
    pub runner: Arc<JobRunner>,
    poller: Mutex<Option<PollerHandle>>,
}

impl JobSupport {
    pub fn new(runner: Arc<JobRunner>) -> JobSupport {
        JobSupport {
            runner,
            poller: Mutex::new(None),
        }
    }

    /// Start the poller on `runtime`, ticking every `tick`.
    pub fn start(&self, runtime: &tokio::runtime::Handle, tick: Duration) {
        let handle = PollerHandle::spawn(self.runner.clone(), runtime, tick);
        if let Some(previous) = self
            .poller
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(handle)
        {
            previous.stop_detached();
        }
    }

    fn take(&self) -> Option<PollerHandle> {
        self.poller.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

/// The `llm` transport's lifecycle handle. Holds no connection: calls are made per send.
pub struct LlmTransport {
    table: Arc<ChannelTable>,
    callbacks: Arc<CallbackTable>,
    resolver: EnvRefResolver,
    jobs: Option<JobSupport>,
    /// Whether each installed set is published to the process-wide channel registry.
    publish: bool,
}

/// `mode: job` channels are refused without persistence.
fn without_jobs(map: &mut ChannelMap, failures: &mut Vec<Diagnostic>) {
    let refused: Vec<(String, String)> = map
        .iter()
        .filter(|(_, c)| c.mode == ChannelMode::Job)
        .map(|(k, _)| k.clone())
        .collect();
    for key in refused {
        failures.push(
            Diagnostic::error(
                sutra_llm_spi::codes::CONFIG_INVALID,
                format!(
                    "llm channel '{}': 'llm.mode: job' needs the engine database, which is not \
                     configured",
                    key.1
                ),
            )
            .with_attribute("channel", key.1.clone()),
        );
        map.remove(&key);
    }
}

impl LlmTransport {
    /// Parse `definitions` strictly (the first bad channel fails the whole set) and install
    /// them into `table`. Without job support, `mode: job` channels are refused.
    pub fn build(
        table: Arc<ChannelTable>,
        definitions: &[ChannelDefinition],
        resolver: EnvRefResolver,
    ) -> Result<LlmTransport, Diagnostic> {
        LlmTransport::build_with(
            table,
            Arc::new(CallbackTable::new()),
            definitions,
            resolver,
            None,
        )
    }

    /// [`LlmTransport::build`] with a callback table and optional job support.
    pub fn build_with(
        table: Arc<ChannelTable>,
        callbacks: Arc<CallbackTable>,
        definitions: &[ChannelDefinition],
        resolver: EnvRefResolver,
        jobs: Option<JobSupport>,
    ) -> Result<LlmTransport, Diagnostic> {
        let transport = LlmTransport {
            table,
            callbacks,
            resolver,
            jobs,
            publish: false,
        };
        let mut failures = transport.install(definitions);
        if !failures.is_empty() {
            return Err(failures.swap_remove(0));
        }
        Ok(transport)
    }

    /// Parse and swap in the set; returns the per-channel failures (those channels are out).
    fn install(&self, definitions: &[ChannelDefinition]) -> Vec<Diagnostic> {
        let mut parsed = parse_all(definitions, self.resolver);
        if self.jobs.is_none() {
            without_jobs(&mut parsed.channels, &mut parsed.failures);
        }
        self.table.swap(parsed.channels);
        self.callbacks.swap(parsed.callbacks);
        if self.publish {
            sutra_llm_spi::llm_channels().swap(channel_handles(&self.table));
        }
        parsed.failures
    }

    /// Publish the installed set, and every set a rewire installs, to the process-wide channel
    /// registry ([`sutra_llm_spi::llm_channels`]).
    pub fn publishing(mut self) -> LlmTransport {
        self.publish = true;
        sutra_llm_spi::llm_channels().swap(channel_handles(&self.table));
        self
    }

    /// The job runner, when persistence is configured.
    pub fn job_runner(&self) -> Option<Arc<JobRunner>> {
        self.jobs.as_ref().map(|j| j.runner.clone())
    }
}

#[async_trait::async_trait]
impl TransportChannels for LlmTransport {
    fn transport(&self) -> &str {
        TRANSPORT
    }

    fn consumer_count(&self) -> usize {
        self.table.len() + self.callbacks.len()
    }

    /// Rebuild from the newly active set; a bad channel is logged and left out, the rest
    /// go live.
    async fn rewire(&self, active: &[ChannelDefinition]) {
        for diagnostic in self.install(active) {
            tracing::warn!(
                code = %diagnostic.code,
                channel = diagnostic.attributes.get("channel").map(String::as_str).unwrap_or(""),
                "llm channel skipped on activation flip: {}",
                diagnostic.message
            );
        }
    }

    async fn drain(&self) {
        if let Some(poller) = self.jobs.as_ref().and_then(JobSupport::take) {
            poller.stop().await;
        }
    }

    fn stop_all_detached(&self, _runtime: &tokio::runtime::Handle) {
        if let Some(poller) = self.jobs.as_ref().and_then(JobSupport::take) {
            poller.stop_detached();
        }
    }

    fn inbound_router(&self) -> Option<axum::Router> {
        Some(callback_router(self.callbacks.clone(), self.job_runner()))
    }
}

/// A claim owner unique to this process and spawn.
fn replica_id() -> String {
    let nanos = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    format!("llm-{}-{nanos}", std::process::id())
}

fn spawn(
    definitions: &[ChannelDefinition],
    engine: EngineHandle,
    pool: Option<sqlx::PgPool>,
    resolver: EnvRefResolver,
    handle: tokio::runtime::Handle,
) -> Result<Arc<dyn TransportChannels>, Diagnostic> {
    let store = pool.map(|pool| LlmJobStore::new(pool, replica_id()));
    JOBS.set(store.clone());
    let jobs = store.map(|store| {
        JobSupport::new(Arc::new(JobRunner::new(
            TABLE.clone(),
            store,
            Arc::new(engine),
        )))
    });
    let transport = LlmTransport::build_with(
        TABLE.clone(),
        CALLBACKS.clone(),
        definitions,
        resolver,
        jobs,
    )?
    .publishing();
    if let Some(jobs) = &transport.jobs {
        jobs.start(&handle, DEFAULT_POLLER_TICK);
    }
    Ok(Arc::new(transport))
}

/// The reply-capable sink comes through [`ReplySinkFactory`]; nothing to register here.
fn register_sink(_registry: &mut SinkRegistry) {}

fn build_sink(engine: EngineHandle) -> Arc<dyn MessageSink> {
    Arc::new(LlmSink::new(TABLE.clone(), Arc::new(engine)).with_jobs(JOBS.clone()))
}

inventory::submit! {
    TransportFactory {
        transport: TRANSPORT,
        spawn,
        register_sink,
        handles_on_complete: false,
    }
}

inventory::submit! {
    ReplySinkFactory {
        transport: TRANSPORT,
        build: build_sink,
    }
}

fn build_inline() -> Arc<dyn sutra_channels::InlineCaller> {
    Arc::new(LlmInlineCaller::new(TABLE.clone()))
}

inventory::submit! {
    InlineCallerFactory {
        transport: TRANSPORT,
        build: build_inline,
    }
}
