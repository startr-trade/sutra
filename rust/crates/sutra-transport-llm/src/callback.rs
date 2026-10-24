//! The job-callback endpoint: `POST /llm/<path>` on the shared HTTP listener, served for every
//! inbound `transport: llm` channel. A callback is verified before anything else (`401`
//! otherwise), parsed by the provider adapter, then settles its job row through the
//! [`JobRunner`]. A callback that finds no claimable row (delivered already, or the poller
//! holds it) is acknowledged without delivering, so duplicates deliver once.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;

use crate::config::{CallbackTable, ChannelMode, LlmCallbackChannel, CALLBACK_PATH_PREFIX};
use crate::jobs::JobRunner;
use sutra_llm_spi::LlmProvider;

struct CallbackState {
    callbacks: Arc<CallbackTable>,
    runner: Option<Arc<JobRunner>>,
}

/// The router serving every callback channel in `callbacks`, live across rewires. Without a
/// runner (no persistence) every callback answers `503`.
pub fn callback_router(callbacks: Arc<CallbackTable>, runner: Option<Arc<JobRunner>>) -> Router {
    let state = Arc::new(CallbackState { callbacks, runner });
    let route = format!("{CALLBACK_PATH_PREFIX}{{*path}}");
    Router::new()
        .route(&route, post(handle_callback))
        .with_state(state)
}

fn copy_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_ascii_lowercase(), v.to_string()))
        })
        .collect()
}

/// The `(provider client, secret)` pairs a callback on `callback` may be verified with: the
/// channel's own secret and each referencing candidate's secret, each with that candidate's
/// adapter.
fn verifiers(
    runner: &JobRunner,
    callback: &LlmCallbackChannel,
) -> Vec<(Arc<dyn LlmProvider>, String)> {
    let mut out = Vec::new();
    for channel in runner.table().all() {
        if channel.mode != ChannelMode::Job || channel.module_key != callback.module_key {
            continue;
        }
        for candidate in &channel.candidates {
            if candidate.job.callback_channel.as_deref() != Some(callback.name.as_str())
                || candidate.provider != callback.provider
            {
                continue;
            }
            for secret in [&callback.secret, &candidate.job.callback_secret]
                .into_iter()
                .flatten()
            {
                out.push((candidate.client.clone(), secret.clone()));
            }
        }
    }
    out
}

async fn handle_callback(
    State(state): State<Arc<CallbackState>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let Some(callback) = state
        .callbacks
        .get(&format!("{CALLBACK_PATH_PREFIX}{path}"))
    else {
        return StatusCode::NOT_FOUND;
    };
    let Some(runner) = state.runner.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    let headers = copy_headers(&headers);
    let Some(client) = verifiers(runner, &callback)
        .into_iter()
        .find(|(client, secret)| client.verify_callback(&headers, &body, secret))
        .map(|(client, _)| client)
    else {
        tracing::warn!(channel = %callback.name, "llm job callback rejected: bad signature");
        return StatusCode::UNAUTHORIZED;
    };
    let event = match client.parse_callback(&headers, &body) {
        Ok(event) => event,
        Err(failure) => {
            tracing::warn!(
                channel = %callback.name,
                "llm job callback not understood: {}",
                failure.detail()
            );
            return StatusCode::BAD_REQUEST;
        }
    };
    match runner
        .on_callback(
            &callback.deployment,
            &callback.provider,
            &event.job.id,
            event.state,
        )
        .await
    {
        Ok(_) => StatusCode::OK,
        Err(error) => {
            // The provider retries; the poller is the safety net either way.
            tracing::warn!(channel = %callback.name, %error, "llm job callback claim failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}
