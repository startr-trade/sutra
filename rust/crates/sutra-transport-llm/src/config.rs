//! Channel configuration: the `llm.*` property block of an outbound `transport: llm`
//! channel, parsed into an [`LlmChannel`] with its candidate providers already built, and
//! the [`ChannelTable`] the sink reads at send time. An INBOUND `transport: llm` channel is a
//! job-callback endpoint ([`LlmCallbackChannel`]), kept in a [`CallbackTable`].

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use sutra_channels::{ChannelDefinition, Diagnostic};
use sutra_llm_spi::{
    codes, find_provider, LlmCapabilities, LlmProvider, LlmProviderConfig, RetryableClass,
};
use sutra_transport_spi::EnvRefResolver;
use tokio::sync::Semaphore;

use crate::TRANSPORT;

/// Default whole-call deadline, per candidate attempt.
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
/// Default number of calls in flight per channel.
pub const DEFAULT_CONCURRENCY: usize = 4;
/// Default failure classes that move on to the next candidate.
pub const DEFAULT_FALLBACK_ON: [RetryableClass; 3] = [
    RetryableClass::RateLimited,
    RetryableClass::Unavailable,
    RetryableClass::Timeout,
];

/// Default job poll cadence with `completion: poll`.
pub const DEFAULT_POLL_INTERVAL_SECONDS: u64 = 30;
/// Default safety-net sweep cadence with `completion: callback`.
pub const DEFAULT_CALLBACK_POLL_INTERVAL_SECONDS: u64 = 300;
/// Default job deadline.
pub const DEFAULT_JOB_TIMEOUT_SECONDS: u64 = 86_400;
/// Callback endpoints are served under this path prefix on the shared listener.
pub const CALLBACK_PATH_PREFIX: &str = "/llm/";

const KNOWN_KEYS: [&str; 10] = [
    "mode",
    "candidates",
    "reply-channel",
    "system",
    "max-output-tokens",
    "temperature",
    "timeout-seconds",
    "concurrency",
    "fallback-on",
    "require",
];

const CALLBACK_KEYS: [&str; 2] = ["callback-for", "callback-secret"];

const CANDIDATE_KEYS: [&str; 9] = [
    "provider",
    "model",
    "base-url",
    "api-key",
    "completion",
    "callback-channel",
    "poll-interval-seconds",
    "job-timeout-seconds",
    "callback-secret",
];

/// Candidate keys only meaningful on a `mode: job` channel.
const JOB_CANDIDATE_KEYS: [&str; 5] = [
    "completion",
    "callback-channel",
    "poll-interval-seconds",
    "job-timeout-seconds",
    "callback-secret",
];

/// How an outbound `llm` channel calls (`llm.mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChannelMode {
    /// Hold the request until the provider answers.
    #[default]
    Call,
    /// Submit a provider job and complete it later.
    Job,
}

/// How a submitted job's completion is learned (`completion`, per candidate).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Completion {
    #[default]
    Poll,
    /// The provider notifies a callback channel; polling remains as a safety net.
    Callback,
}

/// A candidate's job settings. Defaults apply on a `mode: call` channel too, unused.
pub struct CandidateJob {
    pub completion: Completion,
    /// The inbound `llm` callback channel (same deployment), with `completion: callback`.
    pub callback_channel: Option<String>,
    pub poll_interval: Duration,
    pub job_timeout: Duration,
    /// The resolved callback-verification secret.
    pub callback_secret: Option<String>,
}

impl Default for CandidateJob {
    fn default() -> CandidateJob {
        CandidateJob {
            completion: Completion::Poll,
            callback_channel: None,
            poll_interval: Duration::from_secs(DEFAULT_POLL_INTERVAL_SECONDS),
            job_timeout: Duration::from_secs(DEFAULT_JOB_TIMEOUT_SECONDS),
            callback_secret: None,
        }
    }
}

/// One configured candidate: the provider id, its settings (secret resolved) and the built
/// provider. Deliberately not `Debug`: the settings carry the resolved credential.
pub struct LlmCandidate {
    pub provider: String,
    pub config: LlmProviderConfig,
    pub client: Arc<dyn LlmProvider>,
    pub job: CandidateJob,
}

impl LlmCandidate {
    pub fn model(&self) -> &str {
        &self.config.model
    }

    /// Whether this candidate has every capability in `need`.
    pub fn satisfies(&self, need: LlmCapabilities) -> bool {
        let have = self.client.capabilities();
        (!need.tools || have.tools)
            && (!need.json_output || have.json_output)
            && (!need.async_jobs || have.async_jobs)
            && (!need.job_callbacks || have.job_callbacks)
    }
}

/// One outbound `llm` channel, ready to serve calls.
pub struct LlmChannel {
    pub name: String,
    pub module_key: String,
    /// The deployment the channel is bound to; job rows are scoped by it.
    pub deployment: String,
    pub mode: ChannelMode,
    /// The inbound channel (same deployment) the answer is delivered to.
    pub reply_channel: String,
    pub system: Option<String>,
    pub max_output_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub timeout: Duration,
    pub concurrency: usize,
    pub fallback_on: Vec<RetryableClass>,
    /// Capabilities every serving candidate must have (`llm.require`).
    pub require: LlmCapabilities,
    /// In declaration order; the router tries them first to last.
    pub candidates: Vec<LlmCandidate>,
    pub(crate) permits: Arc<Semaphore>,
}

impl LlmChannel {
    /// The channel's concurrency gate.
    pub fn permits(&self) -> &Arc<Semaphore> {
        &self.permits
    }
}

fn config_error(channel: &str, message: impl Into<String>) -> Diagnostic {
    Diagnostic::error(codes::CONFIG_INVALID, message).with_attribute("channel", channel)
}

/// Parse a YAML-text list property (a list flattens to its YAML text) into strings. A plain
/// scalar is accepted as a comma-separated list.
fn string_list(channel: &str, key: &str, raw: &str) -> Result<Vec<String>, Diagnostic> {
    match serde_yaml_ng::from_str::<serde_yaml_ng::Value>(raw) {
        Ok(serde_yaml_ng::Value::Sequence(items)) => items
            .into_iter()
            .map(|item| match item {
                serde_yaml_ng::Value::String(s) => Ok(s.trim().to_string()),
                _ => Err(config_error(
                    channel,
                    format!("llm channel '{channel}': '{key}' entries must be strings"),
                )),
            })
            .collect(),
        Ok(serde_yaml_ng::Value::String(s)) => Ok(s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()),
        _ => Err(config_error(
            channel,
            format!("llm channel '{channel}': '{key}' must be a list"),
        )),
    }
}

fn parse_number<T: std::str::FromStr>(
    channel: &str,
    key: &str,
    raw: Option<&String>,
) -> Result<Option<T>, Diagnostic> {
    raw.map(|v| {
        v.trim().parse::<T>().map_err(|_| {
            config_error(
                channel,
                format!("llm channel '{channel}': '{key}' has an invalid value"),
            )
        })
    })
    .transpose()
}

fn candidate_string(
    channel: &str,
    index: usize,
    entry: &serde_yaml_ng::Mapping,
    key: &str,
) -> Result<Option<String>, Diagnostic> {
    match entry.get(serde_yaml_ng::Value::String(key.to_string())) {
        None | Some(serde_yaml_ng::Value::Null) => Ok(None),
        Some(serde_yaml_ng::Value::String(s)) if !s.trim().is_empty() => {
            Ok(Some(s.trim().to_string()))
        }
        Some(_) => Err(config_error(
            channel,
            format!(
                "llm channel '{channel}': candidate {index} '{key}' must be a non-empty string"
            ),
        )),
    }
}

fn candidate_seconds(
    channel: &str,
    index: usize,
    entry: &serde_yaml_ng::Mapping,
    key: &str,
    default: u64,
) -> Result<Duration, Diagnostic> {
    let invalid = || {
        config_error(
            channel,
            format!(
                "llm channel '{channel}': candidate {index} '{key}' must be a positive integer"
            ),
        )
    };
    let seconds = match entry.get(serde_yaml_ng::Value::String(key.to_string())) {
        None | Some(serde_yaml_ng::Value::Null) => default,
        Some(serde_yaml_ng::Value::Number(n)) => n.as_u64().ok_or_else(invalid)?,
        Some(serde_yaml_ng::Value::String(s)) => s.trim().parse().map_err(|_| invalid())?,
        Some(_) => return Err(invalid()),
    };
    if seconds == 0 {
        return Err(invalid());
    }
    Ok(Duration::from_secs(seconds))
}

fn resolve_secret(
    channel: &str,
    what: &str,
    reference: Option<String>,
    resolver: EnvRefResolver,
) -> Result<Option<String>, Diagnostic> {
    // Always through the resolver: a literal secret is rejected by package validation, never
    // echoed here.
    reference
        .map(|reference| {
            resolver(&reference).map_err(|reason| {
                config_error(
                    channel,
                    format!(
                        "llm channel '{channel}': {what} reference could not be resolved: {reason}"
                    ),
                )
            })
        })
        .transpose()
}

fn parse_candidate_job(
    channel: &str,
    index: usize,
    entry: &serde_yaml_ng::Mapping,
    mode: ChannelMode,
    resolver: EnvRefResolver,
) -> Result<CandidateJob, Diagnostic> {
    if mode == ChannelMode::Call {
        if let Some(key) = JOB_CANDIDATE_KEYS
            .iter()
            .find(|k| entry.contains_key(serde_yaml_ng::Value::String(k.to_string())))
        {
            return Err(config_error(
                channel,
                format!(
                    "llm channel '{channel}': candidate {index} '{key}' is only valid with \
                     'llm.mode: job'"
                ),
            ));
        }
        return Ok(CandidateJob::default());
    }
    let completion = match candidate_string(channel, index, entry, "completion")?.as_deref() {
        None | Some("poll") => Completion::Poll,
        Some("callback") => Completion::Callback,
        Some(other) => {
            return Err(config_error(
                channel,
                format!(
                    "llm channel '{channel}': candidate {index} completion '{other}' is not \
                     'poll' or 'callback'"
                ),
            ))
        }
    };
    let callback_channel = candidate_string(channel, index, entry, "callback-channel")?;
    match (completion, &callback_channel) {
        (Completion::Callback, None) => {
            return Err(config_error(
                channel,
                format!(
                    "llm channel '{channel}': candidate {index} 'completion: callback' needs \
                     'callback-channel'"
                ),
            ))
        }
        (Completion::Poll, Some(_)) => {
            return Err(config_error(
                channel,
                format!(
                    "llm channel '{channel}': candidate {index} 'callback-channel' needs \
                     'completion: callback'"
                ),
            ))
        }
        _ => {}
    }
    let default_interval = match completion {
        Completion::Poll => DEFAULT_POLL_INTERVAL_SECONDS,
        Completion::Callback => DEFAULT_CALLBACK_POLL_INTERVAL_SECONDS,
    };
    Ok(CandidateJob {
        completion,
        callback_channel,
        poll_interval: candidate_seconds(
            channel,
            index,
            entry,
            "poll-interval-seconds",
            default_interval,
        )?,
        job_timeout: candidate_seconds(
            channel,
            index,
            entry,
            "job-timeout-seconds",
            DEFAULT_JOB_TIMEOUT_SECONDS,
        )?,
        callback_secret: resolve_secret(
            channel,
            &format!("candidate {index} callback-secret"),
            candidate_string(channel, index, entry, "callback-secret")?,
            resolver,
        )?,
    })
}

fn parse_candidates(
    channel: &str,
    raw: &str,
    mode: ChannelMode,
    resolver: EnvRefResolver,
) -> Result<Vec<LlmCandidate>, Diagnostic> {
    let parsed: serde_yaml_ng::Value = serde_yaml_ng::from_str(raw).map_err(|_| {
        config_error(
            channel,
            format!("llm channel '{channel}': 'candidates' is not a valid list"),
        )
    })?;
    let serde_yaml_ng::Value::Sequence(items) = parsed else {
        return Err(config_error(
            channel,
            format!("llm channel '{channel}': 'candidates' must be a list"),
        ));
    };
    if items.is_empty() {
        return Err(config_error(
            channel,
            format!("llm channel '{channel}': 'candidates' must name at least one candidate"),
        ));
    }
    let mut out = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let serde_yaml_ng::Value::Mapping(entry) = item else {
            return Err(config_error(
                channel,
                format!("llm channel '{channel}': candidate {index} must be a mapping"),
            ));
        };
        for key in entry.keys() {
            let known = key.as_str().is_some_and(|k| CANDIDATE_KEYS.contains(&k));
            if !known {
                return Err(config_error(
                    channel,
                    format!(
                        "llm channel '{channel}': candidate {index} has an unknown option '{}'",
                        key.as_str().unwrap_or("<non-string>")
                    ),
                ));
            }
        }
        let provider = candidate_string(channel, index, entry, "provider")?.ok_or_else(|| {
            config_error(
                channel,
                format!("llm channel '{channel}': candidate {index} is missing 'provider'"),
            )
        })?;
        let model = candidate_string(channel, index, entry, "model")?.ok_or_else(|| {
            config_error(
                channel,
                format!("llm channel '{channel}': candidate {index} is missing 'model'"),
            )
        })?;
        let base_url = candidate_string(channel, index, entry, "base-url")?;
        // Always through the resolver: a literal secret is rejected by package validation,
        // never echoed here.
        let api_key = candidate_string(channel, index, entry, "api-key")?
            .map(|reference| {
                resolver(&reference).map_err(|reason| {
                    config_error(
                        channel,
                        format!(
                            "llm channel '{channel}': candidate {index} api-key reference \
                             could not be resolved: {reason}"
                        ),
                    )
                    .with_attribute("provider", provider.clone())
                })
            })
            .transpose()?;
        let factory = find_provider(&provider).ok_or_else(|| {
            Diagnostic::error(
                codes::PROVIDER_UNKNOWN,
                format!(
                    "llm channel '{channel}': candidate {index} names provider '{provider}', \
                     which no linked provider registered (is the provider's cargo feature \
                     enabled?)"
                ),
            )
            .with_attribute("channel", channel)
            .with_attribute("provider", provider.clone())
        })?;
        let config = LlmProviderConfig {
            model,
            base_url,
            api_key,
        };
        let client = (factory.build)(&config).map_err(|d| {
            d.with_attribute("channel", channel)
                .with_attribute("provider", provider.clone())
        })?;
        let job = parse_candidate_job(channel, index, entry, mode, resolver)?;
        if job.completion == Completion::Callback && !client.capabilities().job_callbacks {
            return Err(config_error(
                channel,
                format!(
                    "llm channel '{channel}': candidate {index} uses 'completion: callback' but \
                     provider '{provider}' has no job callbacks"
                ),
            )
            .with_attribute("provider", provider));
        }
        out.push(LlmCandidate {
            provider,
            config,
            client,
            job,
        });
    }
    Ok(out)
}

/// Parse one channel definition. `Ok(None)` when it is not an outbound `llm` channel.
pub fn parse_channel(
    definition: &ChannelDefinition,
    resolver: EnvRefResolver,
) -> Result<Option<LlmChannel>, Diagnostic> {
    if definition.transport.as_deref().map(str::trim) != Some(TRANSPORT)
        || !definition.is_outbound()
    {
        return Ok(None);
    }
    let channel = definition.binding.channel_name.as_str();
    let block: BTreeMap<&str, &String> = definition
        .properties
        .iter()
        .filter_map(|(k, v)| k.strip_prefix("llm.").map(|k| (k, v)))
        .collect();
    if let Some(unknown) = block.keys().find(|k| !KNOWN_KEYS.contains(k)) {
        return Err(config_error(
            channel,
            format!("llm channel '{channel}': unknown option 'llm.{unknown}'"),
        ));
    }

    // A sync channel (`ack-mode: on-complete`) answers inline and has no reply channel.
    let sync = definition
        .properties
        .get("ack-mode")
        .is_some_and(|m| m.trim().eq_ignore_ascii_case("on-complete"));
    let reply_channel = block
        .get("reply-channel")
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let reply_channel = match (reply_channel, sync) {
        (Some(reply), _) => reply,
        (None, true) => String::new(),
        (None, false) => {
            return Err(config_error(
                channel,
                format!("llm channel '{channel}': 'llm.reply-channel' is required"),
            ))
        }
    };
    let candidates_raw = block.get("candidates").ok_or_else(|| {
        config_error(
            channel,
            format!("llm channel '{channel}': 'llm.candidates' is required"),
        )
    })?;

    let timeout_seconds = parse_number::<u64>(
        channel,
        "timeout-seconds",
        block.get("timeout-seconds").copied(),
    )?
    .unwrap_or(DEFAULT_TIMEOUT_SECONDS);
    let concurrency =
        parse_number::<usize>(channel, "concurrency", block.get("concurrency").copied())?
            .unwrap_or(DEFAULT_CONCURRENCY);
    if timeout_seconds == 0 || concurrency == 0 {
        return Err(config_error(
            channel,
            format!("llm channel '{channel}': timeout-seconds and concurrency must be positive"),
        ));
    }
    let max_output_tokens = parse_number::<u32>(
        channel,
        "max-output-tokens",
        block.get("max-output-tokens").copied(),
    )?;
    let temperature =
        parse_number::<f32>(channel, "temperature", block.get("temperature").copied())?;

    let fallback_on = match block.get("fallback-on") {
        None => DEFAULT_FALLBACK_ON.to_vec(),
        Some(raw) => string_list(channel, "fallback-on", raw)?
            .iter()
            .map(|class| {
                RetryableClass::parse(class).ok_or_else(|| {
                    config_error(
                        channel,
                        format!("llm channel '{channel}': unknown fallback-on class '{class}'"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    let mut require = LlmCapabilities::default();
    if let Some(raw) = block.get("require") {
        for need in string_list(channel, "require", raw)? {
            match need.as_str() {
                "tools" => require.tools = true,
                "json-output" => require.json_output = true,
                other => {
                    return Err(config_error(
                        channel,
                        format!("llm channel '{channel}': unknown require capability '{other}'"),
                    ))
                }
            }
        }
    }

    let mode = match block.get("mode").map(|m| m.trim()) {
        None | Some("call") => ChannelMode::Call,
        Some("job") => ChannelMode::Job,
        Some(other) => {
            return Err(config_error(
                channel,
                format!("llm channel '{channel}': mode '{other}' is not 'call' or 'job'"),
            ))
        }
    };
    if mode == ChannelMode::Job && sync {
        return Err(config_error(
            channel,
            format!(
                "llm channel '{channel}': 'llm.mode: job' completes later and cannot serve an \
                 'ack-mode: on-complete' channel"
            ),
        ));
    }

    let candidates = parse_candidates(channel, candidates_raw, mode, resolver)?;
    if !candidates.iter().any(|c| c.satisfies(require)) {
        return Err(config_error(
            channel,
            format!("llm channel '{channel}': no candidate has the capabilities 'require' names"),
        ));
    }
    if mode == ChannelMode::Job {
        let need = LlmCapabilities {
            async_jobs: true,
            ..require
        };
        if !candidates.iter().any(|c| c.satisfies(need)) {
            return Err(config_error(
                channel,
                format!(
                    "llm channel '{channel}': 'llm.mode: job' needs a candidate whose provider \
                     supports async jobs"
                ),
            ));
        }
    }

    Ok(Some(LlmChannel {
        name: channel.to_string(),
        module_key: definition.binding.namespace.module_key(),
        deployment: definition.binding.deployment_id().value().to_string(),
        mode,
        reply_channel,
        system: block.get("system").map(|s| s.to_string()),
        max_output_tokens,
        temperature,
        timeout: Duration::from_secs(timeout_seconds),
        concurrency,
        fallback_on,
        require,
        candidates,
        permits: Arc::new(Semaphore::new(concurrency)),
    }))
}

/// Keyed by `(module_key, channel_name)`.
pub type ChannelMap = HashMap<(String, String), Arc<LlmChannel>>;

/// Parse every outbound `llm` channel in `definitions`, collecting per-channel failures
/// instead of stopping at the first. Callback channels are parsed and cross-checked too, but
/// only the outbound map is returned; [`parse_all`] returns both.
pub fn parse_channels(
    definitions: &[ChannelDefinition],
    resolver: EnvRefResolver,
) -> (ChannelMap, Vec<Diagnostic>) {
    let parsed = parse_all(definitions, resolver);
    (parsed.channels, parsed.failures)
}

/// Everything [`parse_all`] produced.
#[derive(Default)]
pub struct ParsedChannels {
    pub channels: ChannelMap,
    pub callbacks: CallbackMap,
    pub failures: Vec<Diagnostic>,
}

/// Parse every `llm` channel in `definitions`: outbound channels and inbound callback
/// channels, then check each `callback-channel` reference. A failing channel is left out and
/// its diagnostic collected.
pub fn parse_all(definitions: &[ChannelDefinition], resolver: EnvRefResolver) -> ParsedChannels {
    let mut out = ParsedChannels::default();
    for definition in definitions {
        let parsed = parse_channel(definition, resolver).and_then(|channel| match channel {
            Some(channel) => Ok(Some(Either::Outbound(channel))),
            None => parse_callback_channel(definition, resolver)
                .map(|callback| callback.map(Either::Callback)),
        });
        match parsed {
            Ok(Some(Either::Outbound(channel))) => {
                out.channels.insert(
                    (channel.module_key.clone(), channel.name.clone()),
                    Arc::new(channel),
                );
            }
            Ok(Some(Either::Callback(callback))) => {
                if let Some(existing) = out.callbacks.get(&callback.path) {
                    out.failures.push(config_error(
                        &callback.name,
                        format!(
                            "llm callback channel '{}': path '{}' is already bound by '{}'",
                            callback.name, callback.path, existing.name
                        ),
                    ));
                    continue;
                }
                out.callbacks
                    .insert(callback.path.clone(), Arc::new(callback));
            }
            Ok(None) => {}
            Err(diagnostic) => out.failures.push(diagnostic),
        }
    }
    let mut rejected = Vec::new();
    for (key, channel) in &out.channels {
        if let Err(diagnostic) = check_callback_refs(channel, &out.callbacks, definitions) {
            out.failures.push(diagnostic);
            rejected.push(key.clone());
        }
    }
    for key in rejected {
        out.channels.remove(&key);
    }
    out
}

enum Either {
    Outbound(LlmChannel),
    Callback(LlmCallbackChannel),
}

fn check_callback_refs(
    channel: &LlmChannel,
    callbacks: &CallbackMap,
    definitions: &[ChannelDefinition],
) -> Result<(), Diagnostic> {
    let name = channel.name.as_str();
    for (index, candidate) in channel.candidates.iter().enumerate() {
        let Some(target) = candidate.job.callback_channel.as_deref() else {
            continue;
        };
        let callback = callbacks
            .values()
            .find(|c| c.module_key == channel.module_key && c.name == target);
        let Some(callback) = callback else {
            let declared = definitions.iter().any(|d| {
                d.binding.channel_name == target
                    && d.binding.namespace.module_key() == channel.module_key
            });
            let why = if declared {
                "is not an inbound 'transport: llm' channel; only webhook callbacks served by \
                 the llm transport are supported"
            } else {
                "names no channel in this deployment"
            };
            return Err(config_error(
                name,
                format!(
                    "llm channel '{name}': candidate {index} callback-channel '{target}' {why}"
                ),
            ));
        };
        if callback.provider != candidate.provider {
            return Err(config_error(
                name,
                format!(
                    "llm channel '{name}': candidate {index} callback-channel '{target}' takes \
                     callbacks for provider '{}', not '{}'",
                    callback.provider, candidate.provider
                ),
            ));
        }
        if callback.secret.is_none() && candidate.job.callback_secret.is_none() {
            return Err(config_error(
                name,
                format!(
                    "llm channel '{name}': candidate {index} needs a 'callback-secret', on the \
                     candidate or on callback channel '{target}'"
                ),
            ));
        }
    }
    Ok(())
}

/// An inbound `transport: llm` channel: the webhook endpoint a provider posts job
/// notifications to. Deliberately not `Debug`: it carries the resolved secret.
pub struct LlmCallbackChannel {
    pub name: String,
    pub module_key: String,
    pub deployment: String,
    /// The served path, under [`CALLBACK_PATH_PREFIX`].
    pub path: String,
    /// The provider whose callbacks this endpoint takes (`llm.callback-for`).
    pub provider: String,
    /// The resolved `llm.callback-secret`, if declared here rather than per candidate.
    pub secret: Option<String>,
}

/// Parse one inbound `llm` callback channel. `Ok(None)` when it is not one.
pub fn parse_callback_channel(
    definition: &ChannelDefinition,
    resolver: EnvRefResolver,
) -> Result<Option<LlmCallbackChannel>, Diagnostic> {
    if definition.transport.as_deref().map(str::trim) != Some(TRANSPORT) || definition.is_outbound()
    {
        return Ok(None);
    }
    let channel = definition.binding.channel_name.as_str();
    let block: BTreeMap<&str, &String> = definition
        .properties
        .iter()
        .filter_map(|(k, v)| k.strip_prefix("llm.").map(|k| (k, v)))
        .collect();
    if let Some(unknown) = block.keys().find(|k| !CALLBACK_KEYS.contains(k)) {
        return Err(config_error(
            channel,
            format!("llm callback channel '{channel}': unknown option 'llm.{unknown}'"),
        ));
    }
    let provider = block
        .get("callback-for")
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .ok_or_else(|| {
            config_error(
                channel,
                format!("llm callback channel '{channel}': 'llm.callback-for' is required"),
            )
        })?;
    if find_provider(&provider).is_none() {
        return Err(Diagnostic::error(
            codes::PROVIDER_UNKNOWN,
            format!("llm callback channel '{channel}': no linked provider registered '{provider}'"),
        )
        .with_attribute("channel", channel)
        .with_attribute("provider", provider));
    }
    let bind = definition.bind_spec.as_deref().unwrap_or("").trim();
    let path = bind
        .split_once(char::is_whitespace)
        .filter(|(method, _)| method.eq_ignore_ascii_case("POST"))
        .map(|(_, path)| path.trim())
        .filter(|path| {
            path.len() > CALLBACK_PATH_PREFIX.len() && path.starts_with(CALLBACK_PATH_PREFIX)
        })
        .ok_or_else(|| {
            config_error(
                channel,
                format!(
                    "llm callback channel '{channel}': bind must be 'POST \
                     {CALLBACK_PATH_PREFIX}<path>'"
                ),
            )
        })?;
    let secret = resolve_secret(
        channel,
        "callback-secret",
        block
            .get("callback-secret")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        resolver,
    )?;
    Ok(Some(LlmCallbackChannel {
        name: channel.to_string(),
        module_key: definition.binding.namespace.module_key(),
        deployment: definition.binding.deployment_id().value().to_string(),
        path: path.to_string(),
        provider,
        secret,
    }))
}

/// Callback channels keyed by served path.
pub type CallbackMap = HashMap<String, Arc<LlmCallbackChannel>>;

/// The live callback-channel set, swapped whole on a rewire.
#[derive(Default)]
pub struct CallbackTable {
    callbacks: RwLock<CallbackMap>,
}

impl CallbackTable {
    pub fn new() -> CallbackTable {
        CallbackTable::default()
    }

    pub fn swap(&self, map: CallbackMap) {
        *self.callbacks.write().unwrap_or_else(|e| e.into_inner()) = map;
    }

    pub fn get(&self, path: &str) -> Option<Arc<LlmCallbackChannel>> {
        self.callbacks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(path)
            .cloned()
    }

    pub fn len(&self) -> usize {
        self.callbacks
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The live `llm` channel set, swapped whole on a rewire and read per call.
#[derive(Default)]
pub struct ChannelTable {
    channels: RwLock<ChannelMap>,
}

impl ChannelTable {
    pub fn new() -> ChannelTable {
        ChannelTable::default()
    }

    pub fn from_map(map: ChannelMap) -> ChannelTable {
        ChannelTable {
            channels: RwLock::new(map),
        }
    }

    pub fn swap(&self, map: ChannelMap) {
        *self.channels.write().unwrap_or_else(|e| e.into_inner()) = map;
    }

    pub fn get(&self, module_key: &str, channel: &str) -> Option<Arc<LlmChannel>> {
        self.channels
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(module_key.to_string(), channel.to_string()))
            .cloned()
    }

    /// Every live channel.
    pub fn all(&self) -> Vec<Arc<LlmChannel>> {
        self.channels
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.channels
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
