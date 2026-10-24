//! `llm.mode: job` without a database: configuration validation, submit routing, the
//! no-persistence refusals and the callback endpoint's pre-intake checks.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use sutra_channels::{MessageSink, SendOutcome};
use sutra_llm_spi::codes;
use sutra_transport_llm::{
    callback_router, parse_all, parse_request_body, route_submit, CallbackTable, ChannelMode,
    ChannelTable, Completion, JobRunner, LlmJobStore, LlmTransport,
    DEFAULT_CALLBACK_POLL_INTERVAL_SECONDS, DEFAULT_JOB_TIMEOUT_SECONDS,
    DEFAULT_POLL_INTERVAL_SECONDS,
};
use tower::ServiceExt;

use crate::support::{definitions, outbound, resolver, CapturePort, FAKE, SIGNATURE_HEADER};

/// A job channel `ask` with one candidate per `(model, candidate-extra)`, plus `extra_channels`
/// appended verbatim under `channels:`.
pub fn job_yaml(candidates: &[(&str, &str)], channel_extra: &str, extra_channels: &str) -> String {
    let mut yaml = String::from(
        "channels:\n  - name: ask\n    transport: llm\n    direction: outbound\n    \
         bind: \"llm://ask\"\n    llm:\n      mode: job\n      reply-channel: answers\n      \
         candidates:\n",
    );
    for (model, extra) in candidates {
        yaml.push_str(&format!(
            "        - provider: {FAKE}\n          model: {model}\n"
        ));
        for line in extra.lines() {
            yaml.push_str("          ");
            yaml.push_str(line);
            yaml.push('\n');
        }
    }
    for line in channel_extra.lines() {
        yaml.push_str("      ");
        yaml.push_str(line);
        yaml.push('\n');
    }
    yaml.push_str(extra_channels);
    yaml
}

/// The inbound callback channel `hooks` for the fake provider.
pub fn hooks_channel(secret_line: &str) -> String {
    format!(
        "  - name: hooks\n    transport: llm\n    bind: \"POST /llm/hooks/fake\"\n    llm:\n      \
         callback-for: {FAKE}\n{secret_line}"
    )
}

pub const CALLBACK_CANDIDATE: &str = "completion: callback\ncallback-channel: hooks";

fn failure(yaml: &str) -> sutra_channels::Diagnostic {
    let parsed = parse_all(&definitions(yaml), resolver);
    assert!(
        !parsed
            .channels
            .contains_key(&(crate::support::MODULE_KEY.into(), "ask".into())),
        "the channel should have been refused"
    );
    parsed
        .failures
        .into_iter()
        .next()
        .expect("a config failure")
}

#[test]
fn job_defaults_per_completion() {
    let yaml = job_yaml(
        &[("job-poll", ""), ("job", CALLBACK_CANDIDATE)],
        "",
        &hooks_channel("      callback-secret: env:hook\n"),
    );
    let parsed = parse_all(&definitions(&yaml), resolver);
    assert!(parsed.failures.is_empty(), "{:?}", parsed.failures);
    let channel = &parsed.channels[&(crate::support::MODULE_KEY.into(), "ask".into())];
    assert_eq!(channel.mode, ChannelMode::Job);
    let poll = &channel.candidates[0].job;
    assert_eq!(poll.completion, Completion::Poll);
    assert_eq!(
        poll.poll_interval,
        Duration::from_secs(DEFAULT_POLL_INTERVAL_SECONDS)
    );
    assert_eq!(
        poll.job_timeout,
        Duration::from_secs(DEFAULT_JOB_TIMEOUT_SECONDS)
    );
    let callback = &channel.candidates[1].job;
    assert_eq!(callback.completion, Completion::Callback);
    assert_eq!(callback.callback_channel.as_deref(), Some("hooks"));
    assert_eq!(
        callback.poll_interval,
        Duration::from_secs(DEFAULT_CALLBACK_POLL_INTERVAL_SECONDS)
    );
    let hooks = &parsed.callbacks["/llm/hooks/fake"];
    assert_eq!(hooks.provider, FAKE);
    assert_eq!(hooks.secret.as_deref(), Some("resolved:hook"));
}

#[test]
fn job_settings_parse_and_resolve() {
    let yaml = job_yaml(
        &[(
            "job",
            "completion: callback\ncallback-channel: hooks\npoll-interval-seconds: 7\n\
             job-timeout-seconds: 90\ncallback-secret: env:cand",
        )],
        "",
        &hooks_channel(""),
    );
    let parsed = parse_all(&definitions(&yaml), resolver);
    assert!(parsed.failures.is_empty(), "{:?}", parsed.failures);
    let job =
        &parsed.channels[&(crate::support::MODULE_KEY.into(), "ask".into())].candidates[0].job;
    assert_eq!(job.poll_interval, Duration::from_secs(7));
    assert_eq!(job.job_timeout, Duration::from_secs(90));
    assert_eq!(job.callback_secret.as_deref(), Some("resolved:cand"));
}

#[test]
fn job_mode_is_refused_on_an_on_complete_channel() {
    let yaml = job_yaml(&[("job", "")], "", "").replace(
        "    direction: outbound\n",
        "    direction: outbound\n    ack-mode: on-complete\n",
    );
    let d = failure(&yaml);
    assert_eq!(d.code, codes::CONFIG_INVALID);
    assert!(d.message.contains("on-complete"), "{}", d.message);
}

#[test]
fn job_mode_needs_a_candidate_with_async_jobs() {
    let d = failure(&job_yaml(&[("ok", "")], "", ""));
    assert_eq!(d.code, codes::CONFIG_INVALID);
    assert!(d.message.contains("async jobs"), "{}", d.message);
}

#[test]
fn callback_completion_needs_provider_callbacks() {
    let d = failure(&job_yaml(
        &[("job-poll", CALLBACK_CANDIDATE)],
        "",
        &hooks_channel("      callback-secret: env:hook\n"),
    ));
    assert_eq!(d.code, codes::CONFIG_INVALID);
    assert!(d.message.contains("no job callbacks"), "{}", d.message);
}

#[test]
fn callback_completion_needs_a_callback_channel_and_a_secret() {
    let d = failure(&job_yaml(&[("job", "completion: callback")], "", ""));
    assert!(
        d.message.contains("needs 'callback-channel'"),
        "{}",
        d.message
    );
    let d = failure(&job_yaml(
        &[("job", CALLBACK_CANDIDATE)],
        "",
        &hooks_channel(""),
    ));
    assert!(d.message.contains("callback-secret"), "{}", d.message);
    let d = failure(&job_yaml(&[("job", CALLBACK_CANDIDATE)], "", ""));
    assert!(d.message.contains("names no channel"), "{}", d.message);
}

#[test]
fn a_broker_callback_channel_is_refused_with_a_clear_diagnostic() {
    let queue = "  - name: hooks\n    transport: aws-sqs\n    bind: \"job-events\"\n";
    let d = failure(&job_yaml(&[("job", CALLBACK_CANDIDATE)], "", queue));
    assert_eq!(d.code, codes::CONFIG_INVALID);
    assert!(
        d.message
            .contains("is not an inbound 'transport: llm' channel"),
        "{}",
        d.message
    );
}

#[test]
fn job_only_keys_are_refused_in_call_mode_and_bad_values_rejected() {
    let call =
        job_yaml(&[("job", "poll-interval-seconds: 5")], "", "").replace("      mode: job\n", "");
    assert!(failure(&call)
        .message
        .contains("only valid with 'llm.mode: job'"));
    let zero = job_yaml(&[("job", "job-timeout-seconds: 0")], "", "");
    assert!(failure(&zero).message.contains("positive integer"));
    let mode = job_yaml(&[("job", "")], "", "").replace("mode: job", "mode: later");
    assert!(failure(&mode).message.contains("mode 'later'"));
    let completion = job_yaml(&[("job", "completion: push")], "", "");
    assert!(failure(&completion).message.contains("completion 'push'"));
}

#[test]
fn a_callback_channel_bind_must_be_a_post_under_the_prefix() {
    let yaml = job_yaml(
        &[("job", CALLBACK_CANDIDATE)],
        "",
        &hooks_channel("      callback-secret: env:hook\n")
            .replace("POST /llm/hooks/fake", "POST /hooks/fake"),
    );
    let parsed = parse_all(&definitions(&yaml), resolver);
    assert!(parsed
        .failures
        .iter()
        .any(|d| d.message.contains("bind must be 'POST /llm/<path>'")));
}

#[test]
fn job_mode_without_persistence_is_refused_at_load() {
    let defs = definitions(&job_yaml(&[("job-poll", "")], "", ""));
    let err = LlmTransport::build(Arc::new(ChannelTable::new()), &defs, resolver)
        .err()
        .expect("refused without a database");
    assert_eq!(err.code, codes::CONFIG_INVALID);
    assert!(err.message.contains("engine database"), "{}", err.message);
}

#[tokio::test]
async fn a_sink_without_a_job_store_defers_the_row() {
    let yaml = job_yaml(&[("job-poll", "")], "", "");
    let sink = crate::support::sink(&yaml, Arc::new(CapturePort::default()));
    let outcome = sink.send(&outbound(json!({"prompt": "hi"}))).await;
    assert!(
        matches!(outcome, SendOutcome::RetryableFailure(_)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn submit_falls_back_like_a_call_and_skips_candidates_without_jobs() {
    let table = crate::support::table(&job_yaml(
        &[("ok", ""), ("job-fail", ""), ("job-poll", "")],
        "",
        "",
    ));
    let channel = table.get(crate::support::MODULE_KEY, "ask").unwrap();
    let call = parse_request_body(br#"{"prompt":"hi"}"#).unwrap();
    let submitted = route_submit(&channel, &call).await.expect("submitted");
    assert_eq!(submitted.index, 2);
    assert!(submitted.job.id.starts_with("job-poll-"));
}

fn lazy_runner(table: Arc<ChannelTable>) -> Arc<JobRunner> {
    // Never connects: these requests are answered before any database access.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/none")
        .unwrap();
    Arc::new(JobRunner::new(
        table,
        LlmJobStore::new(pool, "test"),
        Arc::new(CapturePort::default()),
    ))
}

#[tokio::test]
async fn a_callback_with_a_bad_signature_is_rejected_before_intake() {
    let yaml = job_yaml(
        &[("job", CALLBACK_CANDIDATE)],
        "",
        &hooks_channel("      callback-secret: env:hook\n"),
    );
    let parsed = parse_all(&definitions(&yaml), resolver);
    let callbacks = Arc::new(CallbackTable::new());
    callbacks.swap(parsed.callbacks);
    let runner = lazy_runner(Arc::new(ChannelTable::from_map(parsed.channels)));
    let router = callback_router(callbacks, Some(runner));

    let post = |path: &str, signature: &str| {
        Request::post(path)
            .header(SIGNATURE_HEADER, signature)
            .body(Body::from(r#"{"job":"j","done":true}"#))
            .unwrap()
    };
    let status = |r: axum::response::Response| r.status();
    let bad = router
        .clone()
        .oneshot(post("/llm/hooks/fake", "wrong"))
        .await
        .unwrap();
    assert_eq!(status(bad), StatusCode::UNAUTHORIZED);
    let unknown = router
        .oneshot(post("/llm/hooks/other", "resolved:hook"))
        .await
        .unwrap();
    assert_eq!(status(unknown), StatusCode::NOT_FOUND);
}
