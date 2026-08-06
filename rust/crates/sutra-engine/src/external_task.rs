//! The external-task (pull) bridge + the worker-facing HTTP surface.
//!
//! Two halves, mirroring [`crate::outbox`]'s split:
//! * [`PgExternalTaskRows`] — `sutra_channels::ExternalTaskRows` implemented over
//!   `sutra-persistence`'s `PgExternalTaskStore`, keeping `sutra-channels` persistence-free;
//! * [`external_task_routes`] — the three worker operations
//!   (`fetch-and-lock` / `{id}/complete` / `{id}/failure`) as axum handlers over
//!   `sutra_channels::ExternalTaskService`.
//!
//! Posture: these are OPERATE-surface routes (`/sutra/*`), not admin ones. They carry the same
//! unauthenticated cluster-internal posture the rest of `/sutra/*` does — the workers that drive
//! them are engine-adjacent processes, and the gated twins of the operate surface live under
//! `/admin/*`. A deployment that needs authenticated workers puts them behind the same ingress
//! policy the rest of the operate surface already needs.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::json;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use sutra_channels::codes as channel_codes;
use sutra_channels::diag::Diagnostic;
use sutra_channels::external_task::{
    ExternalTask, ExternalTaskRows, ExternalTaskService, ParkRequest, TaskLockView,
};
use sutra_channels::sink::BoxFuture;
use sutra_persistence::stores::{ExternalTaskRow, ExternalTaskStore, PgExternalTaskStore};
use sutra_persistence::DeploymentId as PersistDeploymentId;

// ---- the persistence bridge ----------------------------------------------------------------

/// PG-backed [`ExternalTaskRows`].
pub struct PgExternalTaskRows {
    store: PgExternalTaskStore,
}

impl PgExternalTaskRows {
    pub fn new(pool: PgPool) -> PgExternalTaskRows {
        PgExternalTaskRows {
            store: PgExternalTaskStore::new(pool),
        }
    }
}

fn persist_dep(
    deployment: &sutra_executor::DeploymentId,
) -> Result<PersistDeploymentId, Diagnostic> {
    PersistDeploymentId::new(deployment.value()).map_err(|e| {
        Diagnostic::error(
            channel_codes::RUNTIME_UNEXPECTED,
            format!("deployment id failed persistence-form validation: {e}"),
        )
    })
}

fn parse_task_id(task_id: &str) -> Result<Uuid, Diagnostic> {
    Uuid::parse_str(task_id).map_err(|e| {
        Diagnostic::error(
            channel_codes::EXTERNAL_TASK_REQUEST_INVALID,
            format!("external task id '{task_id}' is not a UUID: {e}"),
        )
    })
}

fn store_diag(context: &str, e: sutra_persistence::PersistenceError) -> Diagnostic {
    Diagnostic::error(channel_codes::RUNTIME_UNEXPECTED, format!("{context}: {e}"))
}

/// Persistence row → the pull surface's transport-neutral shape.
fn to_task(row: ExternalTaskRow) -> ExternalTask {
    ExternalTask {
        task_id: row.task_id.to_string(),
        deployment: sutra_executor::DeploymentId::of(row.deployment.as_str())
            .unwrap_or_else(|_| sutra_executor::DeploymentId::unresolved()),
        instance_id: row.instance_id.to_string(),
        channel: row.channel,
        tenant: row.tenant,
        module_key: row.module_key,
        headers: row.headers,
        // Boundary exit: the claimed task carries raw bytes to the worker. `into_inner()` marks
        // the deliberate unwrap of the persisted `Sensitive` body at the persistence→engine
        // hand-off, exactly as the outbox bridge does.
        body: row.body.into_inner(),
        content_type: row.content_type,
        outbox_key: row.outbox_key,
        traceparent: row.traceparent,
        created_at: row.created_at,
        lock_expires_at: row.lock_expires_at,
        attempt_count: row.attempt_count,
        retries_left: row.retries_left,
    }
}

impl ExternalTaskRows for PgExternalTaskRows {
    fn park<'a>(&'a self, task: &'a ParkRequest) -> BoxFuture<'a, Result<bool, Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(&task.deployment)?;
            let instance_id = Uuid::parse_str(&task.instance_id).unwrap_or_else(|_| Uuid::nil());
            let now = OffsetDateTime::now_utc();
            let row = ExternalTaskRow {
                deployment: dep,
                task_id: Uuid::new_v4(),
                instance_id,
                channel: task.channel.clone(),
                tenant: task.tenant.clone(),
                module_key: task.module_key.clone(),
                body: task.body.clone().into(),
                content_type: task.content_type.clone(),
                headers: task.headers.clone(),
                outbox_key: task.outbox_key.clone(),
                traceparent: task.traceparent.clone(),
                created_at: now,
                fetchable_at: now,
                lock_owner: None,
                lock_expires_at: None,
                attempt_count: 0,
                retries_left: task.retries,
                failed: false,
                last_error: None,
            };
            self.store
                .park(&row)
                .await
                .map_err(|e| store_diag("external task park failed", e))
        })
    }

    fn fetch_and_lock<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        channels: &'a [String],
        worker: &'a str,
        now: OffsetDateTime,
        lock_expires_at: OffsetDateTime,
        max_tasks: i64,
    ) -> BoxFuture<'a, Result<Vec<ExternalTask>, Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let rows = self
                .store
                .fetch_and_lock(&dep, channels, worker, now, lock_expires_at, max_tasks)
                .await
                .map_err(|e| store_diag("external task fetchAndLock failed", e))?;
            Ok(rows.into_iter().map(to_task).collect())
        })
    }

    fn peek<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<TaskLockView>, Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let id = parse_task_id(task_id)?;
            let row = self
                .store
                .peek(&dep, id)
                .await
                .map_err(|e| store_diag("external task peek failed", e))?;
            Ok(row.map(|row| TaskLockView {
                lock_owner: row.lock_owner,
                lock_expires_at: row.lock_expires_at,
                failed: row.failed,
            }))
        })
    }

    fn hold<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        task_id: &'a str,
        worker: &'a str,
        now: OffsetDateTime,
        new_expiry: OffsetDateTime,
    ) -> BoxFuture<'a, Result<Option<ExternalTask>, Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let id = parse_task_id(task_id)?;
            let row = self
                .store
                .hold(&dep, id, worker, now, new_expiry)
                .await
                .map_err(|e| store_diag("external task hold failed", e))?;
            Ok(row.map(to_task))
        })
    }

    fn delete<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        task_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let id = parse_task_id(task_id)?;
            self.store
                .delete(&dep, id)
                .await
                .map_err(|e| store_diag("external task delete failed", e))
        })
    }

    fn fail<'a>(
        &'a self,
        deployment: &'a sutra_executor::DeploymentId,
        task_id: &'a str,
        worker: &'a str,
        now: OffsetDateTime,
        retries_left_after: i32,
        fetchable_at: OffsetDateTime,
        error: &'a str,
    ) -> BoxFuture<'a, Result<bool, Diagnostic>> {
        Box::pin(async move {
            let dep = persist_dep(deployment)?;
            let id = parse_task_id(task_id)?;
            self.store
                .fail(
                    &dep,
                    id,
                    worker,
                    now,
                    retries_left_after,
                    fetchable_at,
                    error,
                )
                .await
                .map_err(|e| store_diag("external task fail failed", e))
        })
    }
}

// ---- the worker-facing HTTP surface ---------------------------------------------------------

/// The pull routes, mounted on the platform router. `None` service (a persistence-less engine)
/// still mounts them — they answer `503`, which is the honest signal that pull needs a database,
/// rather than a `404` that reads as "this engine has no such feature".
pub(crate) fn external_task_routes(service: Option<Arc<ExternalTaskService>>) -> Router {
    Router::new()
        .route("/sutra/external-tasks/fetch-and-lock", post(fetch_and_lock))
        .route("/sutra/external-tasks/{id}/complete", post(complete))
        .route("/sutra/external-tasks/{id}/failure", post(failure))
        .with_state(service)
}

type ServiceState = Option<Arc<ExternalTaskService>>;

/// `POST /sutra/external-tasks/fetch-and-lock` — the long-poll claim.
async fn fetch_and_lock(
    State(service): State<ServiceState>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let Some(service) = service else {
        return unavailable();
    };
    let channels = string_list(&body, "channels");
    let request = match service.validate(
        body.get("workerId").and_then(|v| v.as_str()).unwrap_or(""),
        channels,
        body.get("lockDuration").and_then(|v| v.as_str()),
        body.get("maxTasks")
            .and_then(|v| v.as_u64())
            .map(|n| n as u32),
        body.get("asyncResponseTimeout").and_then(|v| v.as_str()),
    ) {
        Ok(request) => request,
        Err(diagnostic) => return problem(&diagnostic),
    };
    match service.fetch_and_lock(&request).await {
        Ok(tasks) => {
            let rendered: Vec<serde_json::Value> = tasks.iter().map(task_json).collect();
            (StatusCode::OK, Json(json!({ "tasks": rendered }))).into_response()
        }
        Err(diagnostic) => problem(&diagnostic),
    }
}

/// `POST /sutra/external-tasks/{id}/complete` — feed the worker's result back through the
/// engine's ordinary inbound path.
async fn complete(
    State(service): State<ServiceState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let Some(service) = service else {
        return unavailable();
    };
    let worker_id = body.get("workerId").and_then(|v| v.as_str()).unwrap_or("");
    let (result, content_type) = match result_payload(&body) {
        Ok(payload) => payload,
        Err(diagnostic) => return problem(&diagnostic),
    };
    match service.complete(&id, worker_id, result, content_type).await {
        Ok(done) => (
            StatusCode::OK,
            Json(json!({
                "taskId": done.task_id,
                "channel": done.channel,
                "instanceId": done.instance_id,
                "status": "COMPLETED",
            })),
        )
            .into_response(),
        Err(diagnostic) => problem(&diagnostic),
    }
}

/// `POST /sutra/external-tasks/{id}/failure` — release the lock with a spent (or explicitly
/// set) retry budget, or exhaust it into the terminal posture.
async fn failure(
    State(service): State<ServiceState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> axum::response::Response {
    let Some(service) = service else {
        return unavailable();
    };
    let worker_id = body.get("workerId").and_then(|v| v.as_str()).unwrap_or("");
    let error_message = body
        .get("errorMessage")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let retries = body
        .get("retries")
        .and_then(|v| v.as_i64())
        .map(|n| n.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32);
    let retry_timeout = match body.get("retryTimeout").and_then(|v| v.as_str()) {
        None => None,
        Some(raw) => match sutra_bpmn::duration::parse_iso8601_duration(raw.trim()) {
            Ok(duration) => Some(duration),
            Err(reason) => {
                return problem(&Diagnostic::error(
                    channel_codes::EXTERNAL_TASK_REQUEST_INVALID,
                    format!("retryTimeout: '{raw}' is not an ISO-8601 duration — {reason}"),
                ))
            }
        },
    };
    match service
        .fail(&id, worker_id, &error_message, retries, retry_timeout)
        .await
    {
        Ok(failed) => (
            StatusCode::OK,
            Json(json!({
                "taskId": failed.task_id,
                "channel": failed.channel,
                "instanceId": failed.instance_id,
                "retries": failed.retries_left,
                "status": if failed.terminal { "TERMINAL" } else { "PENDING" },
            })),
        )
            .into_response(),
        Err(diagnostic) => problem(&diagnostic),
    }
}

use axum::response::IntoResponse;

/// The worker-visible projection of a locked task. The payload is base64 only when it is not
/// valid UTF-8 — a JSON/XML worker reads `payload` directly, a binary one reads `payloadBase64`,
/// and neither has to guess which it got.
fn task_json(task: &ExternalTask) -> serde_json::Value {
    let mut out = json!({
        "id": task.task_id,
        "channel": task.channel,
        "tenant": task.tenant,
        "moduleKey": task.module_key,
        "deploymentId": task.deployment.value(),
        "instanceId": task.instance_id,
        "idempotencyKey": task.outbox_key,
        "headers": task.headers,
        "retries": task.retries_left,
        "attempts": task.attempt_count,
    });
    if let Some(content_type) = &task.content_type {
        out["contentType"] = json!(content_type);
    }
    if let Some(expiry) = task.lock_expires_at {
        out["lockExpiresAt"] = json!(rfc3339(expiry));
    }
    match std::str::from_utf8(&task.body) {
        Ok(text) => out["payload"] = json!(text),
        Err(_) => out["payloadBase64"] = json!(base64(&task.body)),
    }
    out
}

/// A completion's result payload: `result` (text) or `resultBase64` (bytes), never both.
fn result_payload(
    body: &serde_json::Value,
) -> Result<(Option<Vec<u8>>, Option<String>), Diagnostic> {
    let text = body.get("result").and_then(|v| v.as_str());
    let encoded = body.get("resultBase64").and_then(|v| v.as_str());
    if text.is_some() && encoded.is_some() {
        return Err(Diagnostic::error(
            channel_codes::EXTERNAL_TASK_REQUEST_INVALID,
            "a completion carries 'result' OR 'resultBase64', never both",
        ));
    }
    let content_type = body
        .get("contentType")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let bytes = match (text, encoded) {
        (Some(text), _) => Some(text.as_bytes().to_vec()),
        (None, Some(encoded)) => Some(decode_base64(encoded).ok_or_else(|| {
            Diagnostic::error(
                channel_codes::EXTERNAL_TASK_REQUEST_INVALID,
                "resultBase64 is not valid base64",
            )
        })?),
        (None, None) => None,
    };
    Ok((bytes, content_type))
}

fn string_list(body: &serde_json::Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Structured-code → HTTP status. Every arm fails CLOSED: a lock problem is a `409` the worker
/// must react to, never a `200` it can mistake for success.
fn status_for(code: &str) -> StatusCode {
    match code {
        channel_codes::EXTERNAL_TASK_REQUEST_INVALID => StatusCode::BAD_REQUEST,
        channel_codes::EXTERNAL_TASK_NOT_FOUND => StatusCode::NOT_FOUND,
        channel_codes::EXTERNAL_TASK_LOCK_HELD
        | channel_codes::EXTERNAL_TASK_LOCK_LOST
        | channel_codes::EXTERNAL_TASK_TERMINAL => StatusCode::CONFLICT,
        channel_codes::EXTERNAL_TASK_COMPLETION_REJECTED => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// RFC 7807 problem document carrying the stable `SUTRA.*` code — the same shape the channel
/// HTTP transport answers rejects with, so a worker parses one error format.
fn problem(diagnostic: &Diagnostic) -> axum::response::Response {
    let status = status_for(&diagnostic.code);
    let mut body = json!({
        "type": "about:blank",
        "title": diagnostic.code,
        "status": status.as_u16(),
        "detail": diagnostic.message,
        "code": diagnostic.code,
    });
    if !diagnostic.attributes.is_empty() {
        let attributes: BTreeMap<&str, &str> = diagnostic
            .attributes
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        body["attributes"] = json!(attributes);
    }
    (
        status,
        [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
        serde_json::to_string(&body).unwrap_or_default(),
    )
        .into_response()
}

fn unavailable() -> axum::response::Response {
    problem(&Diagnostic::error(
        channel_codes::INBOUND_PERSISTENCE_REQUIRED,
        "the external-task pull surface needs the engine datasource (sutra.datasource.url); this \
         engine is running without persistence",
    ))
}

fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(BASE64_ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(BASE64_ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            BASE64_ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64_ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let mut bits = 0u32;
    let mut held = 0u8;
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    for byte in text.bytes().filter(|b| !b.is_ascii_whitespace()) {
        if byte == b'=' {
            break;
        }
        let value = BASE64_ALPHABET.iter().position(|c| *c == byte)? as u32;
        bits = (bits << 6) | value;
        held += 6;
        if held >= 8 {
            held -= 8;
            out.push((bits >> held) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structured_codes_map_to_fail_closed_statuses() {
        assert_eq!(
            status_for(channel_codes::EXTERNAL_TASK_REQUEST_INVALID),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for(channel_codes::EXTERNAL_TASK_NOT_FOUND),
            StatusCode::NOT_FOUND
        );
        // A stale worker never gets a 2xx it could read as success.
        assert_eq!(
            status_for(channel_codes::EXTERNAL_TASK_LOCK_LOST),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_for(channel_codes::EXTERNAL_TASK_LOCK_HELD),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_for(channel_codes::EXTERNAL_TASK_TERMINAL),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_for(channel_codes::EXTERNAL_TASK_COMPLETION_REJECTED),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            status_for("SUTRA.RUNTIME.UNEXPECTED"),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn base64_round_trips_and_rejects_garbage() {
        for sample in [&b""[..], b"f", b"fo", b"foo", b"foob", b"\x00\xff\xfe"] {
            let encoded = base64(sample);
            assert_eq!(
                decode_base64(&encoded).as_deref(),
                Some(sample),
                "round-trip {encoded}"
            );
        }
        assert_eq!(decode_base64("not base64!"), None);
    }

    #[test]
    fn a_completion_carries_result_or_result_base64_never_both() {
        let both = json!({ "result": "ok", "resultBase64": "b2s=" });
        let err = result_payload(&both).unwrap_err();
        assert_eq!(err.code, channel_codes::EXTERNAL_TASK_REQUEST_INVALID);

        let text = json!({ "result": "ok", "contentType": "text/plain" });
        let (bytes, content_type) = result_payload(&text).unwrap();
        assert_eq!(bytes.as_deref(), Some(&b"ok"[..]));
        assert_eq!(content_type.as_deref(), Some("text/plain"));

        // Neither is legal — the worker did the work and has nothing to hand back.
        let (bytes, _) = result_payload(&json!({})).unwrap();
        assert!(bytes.is_none());
    }

    #[test]
    fn a_text_payload_is_served_inline_and_binary_as_base64() {
        let mut task = ExternalTask {
            task_id: "t-1".to_string(),
            deployment: sutra_executor::DeploymentId::unresolved(),
            instance_id: "i-1".to_string(),
            channel: "score-in".to_string(),
            tenant: "acme".to_string(),
            module_key: "acme/demoflow/1.0.0".to_string(),
            headers: BTreeMap::new(),
            body: b"{\"ask\":1}".to_vec(),
            content_type: Some("application/json".to_string()),
            outbox_key: "ob-1".to_string(),
            traceparent: None,
            created_at: OffsetDateTime::UNIX_EPOCH,
            lock_expires_at: None,
            attempt_count: 1,
            retries_left: 3,
        };
        let rendered = task_json(&task);
        assert_eq!(rendered["payload"], json!("{\"ask\":1}"));
        assert!(rendered.get("payloadBase64").is_none());
        assert_eq!(rendered["retries"], json!(3));

        task.body = vec![0x00, 0xff];
        let rendered = task_json(&task);
        assert!(rendered.get("payload").is_none());
        assert_eq!(rendered["payloadBase64"], json!("AP8="));
    }
}
