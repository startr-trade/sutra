//! The OIDC-gated administrative REST surface (`/admin/*`).
//!
//! The admin surface is the **authenticated twin** of the `/sutra/*` operate routes: the same
//! deployment-status + instance read/control operations, but behind a bearer-JWT gate. Every
//! handler here is a thin wrapper that delegates to the shared `pub(crate)` helpers in
//! [`crate::server`] — the engine services (deployment-status snapshot, `InstanceStore`,
//! `AliasStore`) are reused verbatim, never reimplemented.
//!
//! ## Endpoints (see [`crate::server::PLATFORM_ROUTES`], drift-gated against `openapi/platform.yaml`)
//!
//! | Method + path                                | Operation                                  |
//! |----------------------------------------------|--------------------------------------------|
//! | `GET  /admin/deployments`                    | activation-status snapshot (all)           |
//! | `GET  /admin/deployments/{id}`               | one deployment's activation status         |
//! | `GET  /admin/instances`                      | list instances (`?deployment=`, `?status=`)|
//! | `GET  /admin/instances/{id}`                 | inspect one instance (`@sensitive` redacted)|
//! | `GET  /admin/instances/by-alias/{key}/{value}`| resolve a live instance by business key    |
//! | `GET  /admin/instances/{id}/history`         | the instance's audit journal, seq-paged    |
//! | `POST /admin/instances/{id}/cancel`          | cancel/terminate an instance               |
//! | `POST /admin/instances/{id}/migrate`         | re-pin one instance onto another deployment|
//! | `POST /admin/instances/migrate`              | batch re-pin (per-instance outcome report)  |
//! | `GET  /admin/dead-letters`                   | list dead letters (`?deploymentId=`, paged)|
//! | `GET  /admin/dead-letters/{id}`              | inspect one dead letter (metadata only)    |
//! | `POST /admin/dead-letters/{id}/replay`       | redrive one through the normal intake path |
//!
//! The dead-letter routes are admin-ONLY by construction, never mirrored onto `/sutra/*`: a dead
//! letter carries the raw business payload that failed. Even here the bytes never render — the
//! read projections report the payload's length, and the bytes leave the store only by being
//! re-dispatched into intake by the replay route.
//!
//! The instance-HISTORY route is admin-only for the same reason: an audit row can carry the
//! payload a process captured at a node. It reads the `audit_event` journal the engine has always
//! written and never exposed; the journal itself stays opt-in (`sutra.audit.sql` + `<q:audit>`),
//! and an empty page names which switch left it empty rather than implying history was lost.
//!
//! **Descoped (documented):** deployment *retire/undeploy* is intentionally NOT an admin control.
//! The Rust engine's deployment lifecycle is filesystem-driven — the sealed-`.sutra` directory is
//! the sole source, watched with two-phase activation — so the honest "undeploy" primitive is
//! removing the archive from that directory (any in-memory retire would be re-activated on the next
//! watch tick). The admin surface therefore exposes the deployment *read* status plus the instance
//! read/control operations; archive lifecycle stays a deploy-plane (filesystem / ConfigMap) concern.
//!
//! ## Gating ([`AdminGate`])
//!
//! Resolved once at boot from [`AdminAuthConfig`] (`sutra.admin.oidc.*`), fail-closed:
//!
//! * **`DevOpen`** — `sutra.admin.oidc.dev-disabled=true`: serve `/admin/*` with NO auth. The ONLY
//!   way gating is disabled; dev/compose only, logged loudly at boot.
//! * **`Unconfigured`** — gating required (dev flag off) but issuer/audience/jwks absent: every admin
//!   request fails closed with `503`. This is the DEFAULT posture — the surface is never open.
//! * **`Oidc`** — full validation: a bearer JWT verified against the JWKS (signature) with matching
//!   `iss`, `aud`, and a non-expired `exp`, then an authorization check for the required admin
//!   scope/claim. Missing/invalid token → `401`; valid token without the scope → `403`.
//!
//! Alg-confusion is closed by restricting accepted algorithms to the asymmetric family
//! ([`ASYMMETRIC_ALGS`]) — a token forged with `HS256` over the public key is rejected. The JWKS is
//! a URL (fetched + cached, refetched on a `kid` miss to ride key rotation) or an inline JWKS
//! document delivered via `secret:`/`${secret:…}` (a mounted Secret volume — no network).

use std::sync::Arc;

use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use jsonwebtoken::{decode, decode_header, jwk, Algorithm, DecodingKey, Validation};
use sutra_channels::auth::{
    constant_time_equals, InboundScheme, DEFAULT_APIKEY_HEADER, DEFAULT_BEARER_HEADER,
};
use tracing::{info, warn};

use crate::config::AdminAuthConfig;
use crate::server::AppState;

/// The asymmetric-only algorithm allowlist for admin JWT verification. Restricting the accepted
/// algorithms to the public-key family (RSA / ECDSA / EdDSA) closes the classic alg-confusion
/// attack: a token forged with a symmetric `HS*` algorithm using the public JWKS key as the HMAC
/// secret is rejected because `HS*` is not in this set. `none` is impossible — it is not an
/// [`Algorithm`] variant.
pub const ASYMMETRIC_ALGS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];

/// The resolved `/admin/*` gate — built once at boot from [`AdminAuthConfig`]. `Clone` (the OIDC
/// verifier is behind an `Arc`) so it can be baked into the tower middleware state.
#[derive(Clone)]
pub(crate) enum AdminGate {
    /// `sutra.admin.oidc.dev-disabled=true` — serve `/admin/*` with NO auth (dev only).
    DevOpen,
    /// Gating required but no auth params configured — every admin request fails closed (503).
    Unconfigured,
    /// The static-secret gate (`sutra.admin.auth.*`) — an auth key/secret checked in a header, the
    /// same model the channels use. Possession authorizes; no per-caller identity or scopes.
    ApiKey(Arc<AdminKeyGate>),
    /// Full bearer-JWT gating against a configured issuer / audience / JWKS.
    Oidc(Arc<OidcVerifier>),
}

/// The resolved static-secret admin gate (`apikey`/`bearer`) — mirrors the channels' `inbound-auth`:
/// the expected key/secret is resolved ONCE at boot, and each request's configured header is
/// constant-time-compared against it. Fail-closed (missing/mismatched header → 401).
pub(crate) struct AdminKeyGate {
    scheme: InboundScheme,
    header: String,
    /// The resolved expected key/secret.
    expected: String,
}

impl AdminKeyGate {
    /// `true` when the request carries the expected key/secret in the configured header. For the
    /// `bearer` scheme a leading (case-insensitive) `Bearer ` is stripped before comparison.
    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(value) = headers
            .get(self.header.as_str())
            .and_then(|v| v.to_str().ok())
        else {
            return false;
        };
        let presented = match self.scheme {
            InboundScheme::Bearer => strip_bearer_prefix(value.trim()),
            _ => value.trim(),
        };
        constant_time_equals(self.expected.as_bytes(), presented.as_bytes())
    }
}

/// Strip a leading case-insensitive `Bearer ` scheme (RFC 6750), returning the token.
fn strip_bearer_prefix(value: &str) -> &str {
    let prefix = "Bearer ";
    if value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix) {
        value[prefix.len()..].trim()
    } else {
        value
    }
}

impl AdminGate {
    /// Resolve the gate from config (fail-closed). Returns `Err` only on an actively-broken OIDC
    /// config (an inline JWKS that is neither a URL nor a valid JWKS document) — that refuses boot
    /// rather than silently degrading. `DevOpen` / `Unconfigured` are valid, boot-clean postures.
    pub(crate) fn from_config(cfg: &AdminAuthConfig) -> Result<AdminGate, String> {
        if cfg.dev_disabled {
            return Ok(AdminGate::DevOpen);
        }
        // The static-secret gate (the channels' auth-key+secret model) takes precedence when set.
        if let Some(scheme_raw) = cfg.auth_scheme.as_deref() {
            let scheme = match scheme_raw.to_ascii_lowercase().as_str() {
                "apikey" => InboundScheme::ApiKey,
                "bearer" => InboundScheme::Bearer,
                other => {
                    return Err(format!(
                        "sutra.admin.auth.scheme is '{other}'; must be apikey or bearer"
                    ));
                }
            };
            let key_ref = cfg.auth_key_ref.as_deref().ok_or_else(|| {
                "sutra.admin.auth.scheme is set but sutra.admin.auth.key-ref is missing".to_string()
            })?;
            let expected = crate::envref::resolve_value(key_ref)
                .map_err(|e| format!("sutra.admin.auth.key-ref '{key_ref}' did not resolve: {e}"))?
                .trim()
                .to_string();
            if expected.is_empty() {
                return Err("sutra.admin.auth.key-ref resolved to an empty secret".to_string());
            }
            let header = cfg.auth_header.clone().unwrap_or_else(|| match scheme {
                InboundScheme::ApiKey => DEFAULT_APIKEY_HEADER.to_string(),
                _ => DEFAULT_BEARER_HEADER.to_string(),
            });
            return Ok(AdminGate::ApiKey(Arc::new(AdminKeyGate {
                scheme,
                header,
                expected,
            })));
        }
        match (
            cfg.issuer.as_deref(),
            cfg.audience.as_deref(),
            cfg.jwks.as_deref(),
        ) {
            (Some(issuer), Some(audience), Some(jwks)) => {
                let verifier = OidcVerifier::build(
                    issuer,
                    audience,
                    jwks,
                    &cfg.role_claim,
                    &cfg.required_role,
                )?;
                Ok(AdminGate::Oidc(Arc::new(verifier)))
            }
            _ => Ok(AdminGate::Unconfigured),
        }
    }

    /// Emit the boot-time posture record (audit-visible in the structured log). `DevOpen` is a
    /// WARNING — a reachable-without-auth admin surface is a production hazard.
    pub(crate) fn log_posture(&self) {
        match self {
            AdminGate::DevOpen => warn!(
                "SUTRA.ADMIN.OIDC_DISABLED — /admin/* is served WITHOUT authentication \
                 (sutra.admin.oidc.dev-disabled=true). Dev / docker-compose only; NEVER production."
            ),
            AdminGate::Unconfigured => info!(
                "/admin/* is UNCONFIGURED (no sutra.admin.auth.* and no sutra.admin.oidc.*) — the \
                 surface is closed (503) until an auth key/secret or OIDC is configured"
            ),
            AdminGate::ApiKey(gate) => info!(
                header = %gate.header,
                "/admin/* auth-key gating active (static key/secret in a header — the channels' \
                 inbound-auth model)"
            ),
            AdminGate::Oidc(_) => info!(
                "/admin/* OIDC gating active (bearer JWT: issuer + audience + JWKS signature + \
                 required admin scope)"
            ),
        }
    }
}

/// The bearer-JWT verifier for `/admin/*` — issuer/audience/expiry + JWKS signature + admin scope.
pub(crate) struct OidcVerifier {
    issuer: String,
    audience: String,
    /// The JWT claim carrying the caller's roles/scopes (default `roles`).
    role_claim: String,
    /// The scope/role the caller must hold (default `sutra-admin`).
    required_role: String,
    jwks: JwksSource,
}

/// Where the verifier gets its signing keys: an inline JWKS document (static) or a remote JWKS
/// endpoint (fetched + cached, refetched on a `kid` miss to ride rotation).
enum JwksSource {
    Static(jwk::JwkSet),
    Remote {
        url: String,
        cache: tokio::sync::RwLock<Option<jwk::JwkSet>>,
    },
}

/// The outcome of authorizing one request — mapped to the HTTP response by the middleware.
enum AdminDecision {
    Authorized,
    /// No usable identity — `401` (missing/malformed token, unknown key, bad signature/iss/aud/exp).
    Unauthenticated(&'static str),
    /// Authenticated but missing the required admin scope/claim — `403`.
    Forbidden,
}

impl OidcVerifier {
    /// Build the verifier, resolving the JWKS reference through the envref SPI (`env:`/`secret:`/
    /// `${…}`) and classifying it as a remote URL or an inline JWKS document.
    fn build(
        issuer: &str,
        audience: &str,
        jwks_ref: &str,
        role_claim: &str,
        required_role: &str,
    ) -> Result<OidcVerifier, String> {
        let resolved = crate::envref::resolve_value(jwks_ref)
            .map_err(|e| format!("jwks reference '{jwks_ref}' did not resolve: {e}"))?;
        let trimmed = resolved.trim();
        let jwks = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            JwksSource::Remote {
                url: trimmed.to_string(),
                cache: tokio::sync::RwLock::new(None),
            }
        } else {
            let set: jwk::JwkSet = serde_json::from_str(trimmed).map_err(|e| {
                format!("jwks is neither an http(s) URL nor a valid JWKS JSON document: {e}")
            })?;
            if set.keys.is_empty() {
                return Err("inline JWKS document has no keys".to_string());
            }
            JwksSource::Static(set)
        };
        Ok(OidcVerifier {
            issuer: issuer.to_string(),
            audience: audience.to_string(),
            role_claim: role_claim.to_string(),
            required_role: required_role.to_string(),
            jwks,
        })
    }

    /// Verify one bearer token and decide the request's fate.
    async fn authorize(&self, token: &str) -> AdminDecision {
        let header = match decode_header(token) {
            Ok(h) => h,
            Err(_) => return AdminDecision::Unauthenticated("malformed JWT header"),
        };
        // Reject the attacker-controlled header alg unless it is asymmetric — this closes the
        // classic alg-confusion attack (an `HS*` forgery using the public JWKS key as the HMAC
        // secret) BEFORE any key material or signature is touched. `none` is impossible (not an
        // `Algorithm` variant). The surviving alg is then the ONLY one accepted by `Validation`.
        if !ASYMMETRIC_ALGS.contains(&header.alg) {
            return AdminDecision::Unauthenticated("unsupported (non-asymmetric) token algorithm");
        }
        let jwk = match self.jwks.find_key(header.kid.as_deref()).await {
            Ok(Some(jwk)) => jwk,
            Ok(None) => {
                return AdminDecision::Unauthenticated("no JWKS key matches the token 'kid'")
            }
            Err(e) => {
                warn!(error = %e, "admin JWKS resolution failed");
                return AdminDecision::Unauthenticated("signing keys (JWKS) are unavailable");
            }
        };
        let key = match DecodingKey::from_jwk(&jwk) {
            Ok(k) => k,
            Err(_) => return AdminDecision::Unauthenticated("unusable JWKS key"),
        };
        // Verify with the token's (now allowlist-confirmed) algorithm — a single, key-compatible
        // alg. iss / aud / exp are validated by jsonwebtoken.
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        let data = match decode::<serde_json::Value>(token, &key, &validation) {
            Ok(d) => d,
            Err(_) => {
                return AdminDecision::Unauthenticated(
                    "token rejected (signature / issuer / audience / expiry)",
                )
            }
        };
        if self.has_required_role(&data.claims) {
            AdminDecision::Authorized
        } else {
            AdminDecision::Forbidden
        }
    }

    /// Whether the token's role/scope claim carries the required admin scope. Accepts both the
    /// OAuth2 space-delimited `scope` string and a JSON array of role strings.
    fn has_required_role(&self, claims: &serde_json::Value) -> bool {
        match claims.get(&self.role_claim) {
            Some(serde_json::Value::String(scopes)) => {
                scopes.split_whitespace().any(|r| r == self.required_role)
            }
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str())
                .any(|r| r == self.required_role),
            _ => false,
        }
    }
}

impl JwksSource {
    /// Find the signing key for a token `kid`. Static sets look up directly; remote sets serve the
    /// cache first, then fetch once (cold cache OR a `kid` miss — the rotation path). A `None` `kid`
    /// resolves only when the set has exactly one key.
    async fn find_key(&self, kid: Option<&str>) -> Result<Option<jwk::Jwk>, String> {
        match self {
            JwksSource::Static(set) => Ok(lookup(set, kid)),
            JwksSource::Remote { url, cache } => {
                {
                    let guard = cache.read().await;
                    if let Some(set) = guard.as_ref() {
                        if let Some(found) = lookup(set, kid) {
                            return Ok(Some(found));
                        }
                    }
                }
                // Cold cache or an unknown kid (key rotation) — fetch once and re-cache.
                let fetched = fetch_jwks(url).await?;
                let found = lookup(&fetched, kid);
                *cache.write().await = Some(fetched);
                Ok(found)
            }
        }
    }
}

/// Resolve a key from a set by `kid`, or the sole key when no `kid` is given.
fn lookup(set: &jwk::JwkSet, kid: Option<&str>) -> Option<jwk::Jwk> {
    match kid {
        Some(kid) => set.find(kid).cloned(),
        None if set.keys.len() == 1 => set.keys.first().cloned(),
        None => None,
    }
}

/// Fetch + parse a remote JWKS document. Network + parse errors surface as `Err` (→ 401 "unavailable"
/// at the call site — fail-closed, never fail-open).
async fn fetch_jwks(url: &str) -> Result<jwk::JwkSet, String> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| format!("JWKS fetch from '{url}' failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "JWKS endpoint '{url}' returned HTTP {}",
            resp.status()
        ));
    }
    resp.json::<jwk::JwkSet>()
        .await
        .map_err(|e| format!("JWKS from '{url}' is not a valid JWKS document: {e}"))
}

/// Build the `/admin/*` sub-router: the thin admin handlers (delegating to [`crate::server`]) with
/// the [`AdminGate`] bearer-JWT layer applied over them and the shared [`AppState`] wired in.
pub(crate) fn admin_router(state: AppState, gate: AdminGate) -> Router {
    Router::new()
        .route(
            "/admin/deployments",
            get(admin_deployments).post(admin_deploy),
        )
        .route(
            "/admin/deployments/{id}",
            get(admin_deployment_by_id).delete(admin_undeploy),
        )
        .route("/admin/instances", get(admin_instances))
        .route("/admin/instances/{id}", get(admin_instance_by_id))
        .route(
            "/admin/instances/by-alias/{key}/{value}",
            get(admin_instance_by_alias),
        )
        .route("/admin/instances/{id}/history", get(admin_instance_history))
        .route("/admin/instances/{id}/cancel", post(admin_instance_cancel))
        .route(
            "/admin/instances/{id}/migrate",
            post(admin_instance_migrate),
        )
        // The BATCH form. A STATIC segment where `/admin/instances/{id}` takes a parameter: the
        // router prefers the literal, and `migrate` could never have been a valid instance id
        // anyway, so the two never compete for a request.
        .route("/admin/instances/migrate", post(admin_instances_migrate))
        .route("/admin/subjects/erase", post(admin_subject_erase))
        .route("/admin/dead-letters", get(admin_dead_letters))
        .route("/admin/dead-letters/{id}", get(admin_dead_letter_by_id))
        .route(
            "/admin/dead-letters/{id}/replay",
            post(admin_dead_letter_replay),
        )
        .route_layer(axum::middleware::from_fn_with_state(gate, admin_gate))
        // The sync deploy (POST /admin/deployments) uploads the sealed archive as the body — a
        // large, multi-flow deployment can exceed axum's 2 MiB default. Raise the cap so real
        // deployments upload (design: db-backed-deployment-store.md "k8s deploy robustness").
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(state)
}

/// The bearer-JWT gate middleware. Applied only to `/admin/*` so it never loosens the platform
/// routes. Fail-closed on every branch.
async fn admin_gate(State(gate): State<AdminGate>, req: Request, next: Next) -> Response {
    match &gate {
        AdminGate::DevOpen => next.run(req).await,
        AdminGate::Unconfigured => unconfigured_response(),
        AdminGate::ApiKey(key_gate) => {
            if key_gate.authorized(req.headers()) {
                next.run(req).await
            } else {
                unauthenticated("missing or invalid admin auth key/secret")
            }
        }
        AdminGate::Oidc(verifier) => {
            let Some(token) = bearer_token(req.headers()) else {
                return unauthenticated("missing or malformed 'Authorization: Bearer' header");
            };
            match verifier.authorize(&token).await {
                AdminDecision::Authorized => next.run(req).await,
                AdminDecision::Unauthenticated(reason) => unauthenticated(reason),
                AdminDecision::Forbidden => forbidden(),
            }
        }
    }
}

/// Extract the bearer token from an `Authorization: Bearer <token>` header (scheme case-insensitive).
fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer") && !token.trim().is_empty())
        .then(|| token.trim().to_string())
}

fn unauthenticated(reason: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(serde_json::json!({ "error": "unauthorized", "reason": reason })),
    )
        .into_response()
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "error": "forbidden",
            "reason": "the presented token lacks the required admin scope",
        })),
    )
        .into_response()
}

fn unconfigured_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": "admin surface unavailable",
            "reason": "admin gating is required but not configured — set an auth key/secret \
                       (sutra.admin.auth.{scheme,key-ref}) or OIDC (sutra.admin.oidc.{issuer,audience,\
                       jwks}), or sutra.admin.oidc.dev-disabled=true for a dev deployment",
        })),
    )
        .into_response()
}

// ---- handlers — each a thin delegate to the shared read/control helper in `crate::server` -------

async fn admin_deployments(State(state): State<AppState>) -> impl IntoResponse {
    Json(crate::server::deployments_snapshot_json(&state))
}

async fn admin_deployment_by_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    crate::server::deployment_status_response(&state, &id)
}

/// `POST /admin/deployments` (db source) — deploy the sealed `.sutra` archive in the request body.
/// Default (`?mode=sync`, or omitted): validate → store ACTIVE → activate (flip) → return `Active`
/// synchronously. `?mode=async`: validate + store synchronously, defer the flip, return `202
/// Accepted` `{deploymentId, Pending}` (poll `GET /sutra/deployments/{id}` or await the completion
/// CloudEvent). Async completion sinks are opt-in via `?callback=<http(s)-url>` (a webhook) and/or
/// `?notify=<broker-uri>` (a topic) — the engine emits `com.sutra.deployment.activated/.failed`
/// there on flip. OIDC-gated by the surface.
async fn admin_deploy(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    match params.get("mode").map(String::as_str) {
        Some("async") => {
            // Collect the opt-in completion sinks: a callback webhook and/or a broker topic.
            let sinks: Vec<String> = ["callback", "notify"]
                .iter()
                .filter_map(|k| params.get(*k))
                .filter(|v| !v.is_empty())
                .cloned()
                .collect();
            crate::server::deploy_async_response(&state, sinks, body).await
        }
        _ => crate::server::deploy_response(&state, body).await,
    }
}

/// `DELETE /admin/deployments/{slot}` (db source) — retire the slot's active archive + re-flip.
async fn admin_undeploy(
    State(state): State<AppState>,
    Path(slot): Path<String>,
) -> impl IntoResponse {
    crate::server::undeploy_response(&state, &slot).await
}

async fn admin_instances(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    crate::server::instances_list_response(&state, &params).await
}

async fn admin_instance_by_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    crate::server::instance_inspect_response(&state, &id).await
}

async fn admin_instance_by_alias(
    State(state): State<AppState>,
    Path((key, value)): Path<(String, String)>,
) -> impl IntoResponse {
    crate::server::instance_by_alias_response(&state, &key, &value).await
}

/// `GET /admin/instances/{id}/history` — one seq-ordered page of the instance's `audit_event`
/// journal (`?afterSeq=` cursor, `?limit=`). Admin-only: audit rows carry captured business
/// payloads. The journal is OPT-IN (`sutra.audit.sql` + `<q:audit>`); an empty page says which
/// switch left it empty rather than implying the history was lost.
async fn admin_instance_history(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    crate::server::instance_history_response(&state, &id, &params).await
}

async fn admin_instance_cancel(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    crate::server::instance_cancel_response(&state, &id).await
}

/// `POST /admin/instances/{id}/migrate` — re-pin one live instance onto another ACTIVE deployment,
/// rewriting every node id its durable state names. Body (JSON):
/// `{targetDeploymentId, nodeMapping?, dryRun?}`.
///
/// Admin-ONLY by construction, with no `/sutra/*` twin: this is the single operation that can move
/// an in-flight instance between process versions, and an unauthenticated caller must never be able
/// to re-point somebody's parked work at a different model. The response is always the full
/// validation report — dry run, refusal and success alike.
async fn admin_instance_migrate(
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<axum::Json<serde_json::Value>>,
) -> impl IntoResponse {
    let body = body
        .map(|axum::Json(v)| v)
        .unwrap_or(serde_json::Value::Null);
    crate::server::instance_migrate_response(&state, &id, body).await
}

/// `POST /admin/instances/migrate` — the BATCH form: migrate a filtered population off ONE
/// deployment pin. Body (JSON): `{targetDeploymentId, filter{sourceDeploymentId, processId?,
/// status?, includeTerminal?, limit?}, targetProcessId?, nodeMapping?, dryRun?, resume?}`.
///
/// Each selected instance validates, claims and commits INDEPENDENTLY — one transaction each — and
/// gets its own entry in the report; a refusal or a claim bounce moves nothing and stops nothing.
/// The `200` therefore describes the batch (accepted, executed, reported in full), never the
/// instances: callers key on `totals` and on each entry's `outcome`.
async fn admin_instances_migrate(
    State(state): State<AppState>,
    body: Option<axum::Json<serde_json::Value>>,
) -> impl IntoResponse {
    let body = body
        .map(|axum::Json(v)| v)
        .unwrap_or(serde_json::Value::Null);
    crate::server::instances_migrate_batch_response(&state, body).await
}

/// `GET /admin/dead-letters` — one newest-first page of dead letters (`?deploymentId=` narrows +
/// enables paging, `?limit=`/`?offset=`). Metadata only: the captured payload's LENGTH is
/// reported, never its bytes.
async fn admin_dead_letters(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    crate::server::dead_letters_list_response(&state, &params).await
}

/// `GET /admin/dead-letters/{id}` — one dead letter's failure metadata.
async fn admin_dead_letter_by_id(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    crate::server::dead_letter_get_response(&state, &id, &params).await
}

/// `POST /admin/dead-letters/{id}/replay` — redrive the captured payload through the NORMAL
/// intake path as a fresh delivery (new event id, so inbox dedup does not swallow it). `422` when
/// the row captured no payload/routing keys — a replay is never fabricated.
async fn admin_dead_letter_replay(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    crate::server::dead_letter_replay_response(&state, &id, &params).await
}

/// GDPR erase/disclose a data subject's instances via the HMAC blind index. Body (JSON):
/// `{keyId, deploymentId, subjectName, value, dryRun?}`.
async fn admin_subject_erase(
    State(state): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> impl IntoResponse {
    crate::server::subject_erase_response(&state, body).await
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt; // `oneshot`

    use super::*;

    // A throwaway RSA-2048 keypair GENERATED at first use, once per test binary — never
    // committed: a public repository must carry no private-key material, not even a test
    // key (every downstream secret scanner flags it, and "it's only a test key" is a
    // conversation better never had). Returns (PKCS#8 PEM, JWKS `n`, JWKS `e`) with the
    // JWKS halves base64url-unpadded per RFC 7518. Keygen cost is one-time and kept sane
    // in debug builds by the workspace's `num-bigint-dig` opt-level override.
    fn test_key() -> &'static (String, String, String) {
        use base64::Engine as _;
        use rsa::pkcs8::EncodePrivateKey as _;
        use rsa::traits::PublicKeyParts as _;
        static KEY: std::sync::OnceLock<(String, String, String)> = std::sync::OnceLock::new();
        KEY.get_or_init(|| {
            let key =
                rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).expect("test RSA keygen");
            let pem = key
                .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
                .expect("PKCS#8 PEM")
                .to_string();
            let b64url =
                |bytes: Vec<u8>| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
            (
                pem,
                b64url(key.n().to_bytes_be()),
                b64url(key.e().to_bytes_be()),
            )
        })
    }

    const TEST_KID: &str = "tp33-test-key-1";
    const TEST_ISSUER: &str = "https://idp.test.local/";
    const TEST_AUDIENCE: &str = "sutra-admin";

    /// The public JWKS matching [`test_key`] — a genuine test JWKS, no IdP call.
    fn test_jwks_json() -> String {
        let (_, n, e) = test_key();
        serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": TEST_KID,
                "n": n,
                "e": e
            }]
        })
        .to_string()
    }

    fn oidc_config() -> AdminAuthConfig {
        AdminAuthConfig {
            issuer: Some(TEST_ISSUER.to_string()),
            audience: Some(TEST_AUDIENCE.to_string()),
            jwks: Some(test_jwks_json()),
            role_claim: "roles".to_string(),
            required_role: "sutra-admin".to_string(),
            dev_disabled: false,
            auth_scheme: None,
            auth_key_ref: None,
            auth_header: None,
        }
    }

    /// Mint a locally-signed RS256 JWT with the given roles and an `exp` offset (seconds from now;
    /// negative = already expired).
    fn mint_token(roles: &[&str], exp_offset_secs: i64) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = serde_json::json!({
            "iss": TEST_ISSUER,
            "aud": TEST_AUDIENCE,
            "sub": "alice@test.local",
            "exp": now + exp_offset_secs,
            "roles": roles,
        });
        let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(test_key().0.as_bytes())
            .expect("test RSA PEM parses");
        jsonwebtoken::encode(&header, &claims, &key).expect("token signs")
    }

    async fn get_admin(router: Router, auth: Option<&str>) -> StatusCode {
        let mut builder = Request::builder().method("GET").uri("/admin/deployments");
        if let Some(auth) = auth {
            builder = builder.header(header::AUTHORIZATION, auth);
        }
        let req = builder.body(Body::empty()).unwrap();
        router.oneshot(req).await.unwrap().status()
    }

    /// The activation-status route needs no persistence, so `AppState` is not required for the
    /// gate tests — the empty test state answers `200` on `/admin/deployments`.
    fn router_with(gate: AdminGate) -> Router {
        admin_router(crate::server::app_state_for_test(), gate)
    }

    #[tokio::test]
    async fn oidc_rejects_missing_token_with_401() {
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        assert_eq!(
            get_admin(router_with(gate), None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn oidc_rejects_garbage_and_unsigned_tokens_with_401() {
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        assert_eq!(
            get_admin(router_with(gate.clone()), Some("Bearer not-a-jwt")).await,
            StatusCode::UNAUTHORIZED
        );
        // A well-formed but expired token is fail-closed too.
        let expired = mint_token(&["sutra-admin"], -3600);
        assert_eq!(
            get_admin(router_with(gate), Some(&format!("Bearer {expired}"))).await,
            StatusCode::UNAUTHORIZED,
            "an expired admin token is rejected"
        );
    }

    #[tokio::test]
    async fn oidc_rejects_valid_token_without_admin_scope_with_403() {
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        let token = mint_token(&["tenant-read:acme"], 3600);
        assert_eq!(
            get_admin(router_with(gate), Some(&format!("Bearer {token}"))).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn oidc_accepts_valid_admin_token_with_200() {
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        let token = mint_token(&["ops", "sutra-admin"], 3600);
        assert_eq!(
            get_admin(router_with(gate), Some(&format!("Bearer {token}"))).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn oidc_rejects_wrong_audience_with_401() {
        // A token issued for a different audience must not open this surface.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = serde_json::json!({
            "iss": TEST_ISSUER, "aud": "some-other-api",
            "sub": "mallory", "exp": now + 3600, "roles": ["sutra-admin"],
        });
        let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(test_key().0.as_bytes()).unwrap();
        let token = jsonwebtoken::encode(&header, &claims, &key).unwrap();
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        assert_eq!(
            get_admin(router_with(gate), Some(&format!("Bearer {token}"))).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn dev_disabled_serves_admin_without_auth() {
        let cfg = AdminAuthConfig {
            dev_disabled: true,
            ..AdminAuthConfig::default()
        };
        let gate = AdminGate::from_config(&cfg).unwrap();
        assert!(matches!(gate, AdminGate::DevOpen));
        assert_eq!(get_admin(router_with(gate), None).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn unconfigured_surface_is_closed_with_503() {
        // Gating required (dev flag off) but no OIDC params → closed, never open.
        let gate = AdminGate::from_config(&AdminAuthConfig::default()).unwrap();
        assert!(matches!(gate, AdminGate::Unconfigured));
        assert_eq!(
            get_admin(router_with(gate), None).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    // ---- the auth-key + secret gate (channels' inbound-auth model) --------------------------

    const TEST_ADMIN_KEY: &str = "tier3-admin-secret-9f3a";

    /// An apikey gate whose expected key is a plain value (resolves to itself via the envref SPI).
    fn apikey_config() -> AdminAuthConfig {
        AdminAuthConfig {
            auth_scheme: Some("apikey".to_string()),
            auth_key_ref: Some(TEST_ADMIN_KEY.to_string()),
            ..AdminAuthConfig::default()
        }
    }

    async fn get_admin_with(router: Router, header: &str, value: &str) -> StatusCode {
        let req = Request::builder()
            .method("GET")
            .uri("/admin/deployments")
            .header(header, value)
            .body(Body::empty())
            .unwrap();
        router.oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn apikey_gate_accepts_the_matching_key_with_200() {
        let gate = AdminGate::from_config(&apikey_config()).unwrap();
        assert!(matches!(gate, AdminGate::ApiKey(_)));
        assert_eq!(
            get_admin_with(router_with(gate), "X-API-Key", TEST_ADMIN_KEY).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn apikey_gate_rejects_missing_and_wrong_key_with_401() {
        let gate = AdminGate::from_config(&apikey_config()).unwrap();
        assert_eq!(
            get_admin(router_with(gate.clone()), None).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            get_admin_with(router_with(gate), "X-API-Key", "wrong-key").await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn apikey_bearer_scheme_strips_prefix_and_wins_over_oidc() {
        // scheme=bearer (default header authorization); a "Bearer <key>" is accepted. The
        // static-secret gate takes precedence even when OIDC params are also present.
        let cfg = AdminAuthConfig {
            auth_scheme: Some("bearer".to_string()),
            auth_key_ref: Some(TEST_ADMIN_KEY.to_string()),
            issuer: Some(TEST_ISSUER.to_string()),
            audience: Some(TEST_AUDIENCE.to_string()),
            jwks: Some(test_jwks_json()),
            ..AdminAuthConfig::default()
        };
        let gate = AdminGate::from_config(&cfg).unwrap();
        assert!(
            matches!(gate, AdminGate::ApiKey(_)),
            "the auth-key gate wins over OIDC when both are configured"
        );
        assert_eq!(
            get_admin(router_with(gate), Some(&format!("Bearer {TEST_ADMIN_KEY}"))).await,
            StatusCode::OK
        );
    }

    #[test]
    fn apikey_scheme_without_key_ref_refuses_boot() {
        let cfg = AdminAuthConfig {
            auth_scheme: Some("apikey".to_string()),
            auth_key_ref: None,
            ..AdminAuthConfig::default()
        };
        assert!(AdminGate::from_config(&cfg).is_err());
    }

    #[test]
    fn a_broken_inline_jwks_refuses_boot() {
        let cfg = AdminAuthConfig {
            issuer: Some(TEST_ISSUER.to_string()),
            audience: Some(TEST_AUDIENCE.to_string()),
            jwks: Some("{not-json".to_string()),
            ..AdminAuthConfig::default()
        };
        assert!(AdminGate::from_config(&cfg).is_err());
    }

    // ---- the dead-letter surface (P0-4) ----------------------------------------------------
    //
    // The rows themselves need a database (covered by the persistence pg suite + the engine's
    // docker-gated IT). What must hold with NO database at all is the surface's posture: it is
    // GATED like every other admin route, it degrades honestly rather than 500ing, and it never
    // invents a replay.

    async fn request(router: Router, method: &str, uri: &str, auth: Option<&str>) -> Response {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(auth) = auth {
            builder = builder.header(header::AUTHORIZATION, auth);
        }
        router
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    }

    #[tokio::test]
    async fn the_dead_letter_routes_are_gated_like_every_other_admin_route() {
        // A dead letter holds the raw payload that failed — an ungated read would be a data leak.
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        for (method, uri) in [
            ("GET", "/admin/dead-letters"),
            ("GET", "/admin/dead-letters/1"),
            ("POST", "/admin/dead-letters/1/replay"),
        ] {
            let status = request(router_with(gate.clone()), method, uri, None)
                .await
                .status();
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must require a token"
            );
        }
    }

    #[tokio::test]
    async fn an_unconfigured_admin_surface_closes_the_dead_letter_routes_too() {
        let gate = AdminGate::from_config(&AdminAuthConfig::default()).unwrap();
        assert!(matches!(gate, AdminGate::Unconfigured));
        assert_eq!(
            request(router_with(gate), "GET", "/admin/dead-letters", None)
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn listing_on_a_persistence_less_engine_is_an_empty_set_not_an_error() {
        let response = request(
            router_with(AdminGate::DevOpen),
            "GET",
            "/admin/dead-letters",
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            body_json(response).await,
            serde_json::json!({ "deadLetters": [] })
        );
    }

    #[tokio::test]
    async fn a_malformed_deployment_id_is_a_400_not_a_silent_full_scan() {
        let response = request(
            router_with(AdminGate::DevOpen),
            "GET",
            "/admin/dead-letters/1?deploymentId=not-a-deployment",
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_non_numeric_dead_letter_id_is_a_400() {
        for uri in [
            "/admin/dead-letters/not-a-number",
            "/admin/dead-letters/0",
            "/admin/dead-letters/-3/replay",
        ] {
            let status = request(router_with(AdminGate::DevOpen), "GET", uri, None)
                .await
                .status();
            assert!(
                status == StatusCode::BAD_REQUEST || status == StatusCode::METHOD_NOT_ALLOWED,
                "{uri} answered {status}"
            );
        }
    }

    // ---- instance migration (P1-8) ---------------------------------------------------------
    //
    // The compatibility matrix is proved in `crate::migrate`'s unit tests and the durable move in
    // the docker-gated IT. What must hold with NO database at all is the surface's posture: the
    // route is GATED like every other admin route, it validates its body before it touches
    // anything, and it degrades honestly rather than 500ing.

    async fn post_body(router: Router, uri: &str, body: &str) -> Response {
        router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    const A_UUID: &str = "11111111-2222-4333-8444-555555555555";

    #[tokio::test]
    async fn the_migrate_route_is_gated_like_every_other_admin_route() {
        // Re-pointing somebody's parked work at a different model is the last thing that may be
        // reachable unauthenticated — there is deliberately no `/sutra/*` twin either.
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        let status = request(
            router_with(gate),
            "POST",
            &format!("/admin/instances/{A_UUID}/migrate"),
            None,
        )
        .await
        .status();
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let closed = AdminGate::from_config(&AdminAuthConfig::default()).unwrap();
        assert_eq!(
            request(
                router_with(closed),
                "POST",
                &format!("/admin/instances/{A_UUID}/migrate"),
                None
            )
            .await
            .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn migrate_validates_its_body_before_it_resolves_anything() {
        // A malformed id, a missing target and a non-object mapping are all 400s — they are
        // decided from the request alone, so they must not depend on a database being present.
        for (uri, body) in [
            (
                "/admin/instances/not-a-uuid/migrate".to_string(),
                r#"{"targetDeploymentId":"dep-000000000000000000000001"}"#,
            ),
            (format!("/admin/instances/{A_UUID}/migrate"), "{}"),
            (
                format!("/admin/instances/{A_UUID}/migrate"),
                r#"{"targetDeploymentId":"dep-000000000000000000000001","nodeMapping":["A"]}"#,
            ),
            (
                format!("/admin/instances/{A_UUID}/migrate"),
                r#"{"targetDeploymentId":"not-a-deployment-id"}"#,
            ),
        ] {
            let response = post_body(router_with(AdminGate::DevOpen), &uri, body).await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{uri} / {body} answered {}",
                response.status()
            );
        }
    }

    #[tokio::test]
    async fn migrate_on_a_persistence_less_engine_refuses_rather_than_pretending() {
        // No pool ⇒ no instance state to move. The target check runs first and this engine has no
        // ACTIVE deployments either, so the refusal is structured and names the reason.
        let response = post_body(
            router_with(AdminGate::DevOpen),
            &format!("/admin/instances/{A_UUID}/migrate"),
            r#"{"targetDeploymentId":"dep-000000000000000000000001"}"#,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(response).await;
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("persistence"),
            "the refusal says WHY: {body}"
        );
    }

    // ---- the BATCH migrate surface (F2) ----------------------------------------------------
    //
    // The partial-failure contract needs a database (the docker IT proves it). What must hold with
    // no database at all is the surface: the batch route is REACHABLE beside the parameterised one,
    // it is gated identically, and every refusal it can decide from the request alone is decided
    // there — before anything is resolved.

    #[tokio::test]
    async fn the_batch_route_resolves_beside_the_parameterised_one_and_is_gated_the_same() {
        // `/admin/instances/migrate` is a STATIC segment where `/admin/instances/{id}` takes a
        // parameter. The router must prefer the literal — if it did not, this would 400 as "not a
        // UUID" instead of reaching the batch handler.
        let gate = AdminGate::from_config(&oidc_config()).unwrap();
        assert_eq!(
            request(router_with(gate), "POST", "/admin/instances/migrate", None)
                .await
                .status(),
            StatusCode::UNAUTHORIZED,
            "re-pointing a whole population is the last thing that may be reachable unauthenticated"
        );

        let response = post_body(
            router_with(AdminGate::DevOpen),
            "/admin/instances/migrate",
            r#"{"targetDeploymentId":"dep-000000000000000000000001",
                "filter":{"sourceDeploymentId":"dep-000000000000000000000002"}}"#,
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "the batch handler ran (no persistence on this engine), so the route resolved"
        );
        assert!(
            body_json(response).await["error"]
                .as_str()
                .unwrap_or_default()
                .contains("persistence"),
            "the refusal says WHY"
        );
    }

    #[tokio::test]
    async fn the_batch_validates_everything_it_can_decide_from_the_request_alone() {
        let target = "dep-000000000000000000000001";
        let source = "dep-000000000000000000000002";
        for (body, why) in [
            (
                format!(r#"{{"filter":{{"sourceDeploymentId":"{source}"}}}}"#),
                "a missing target",
            ),
            (
                format!(r#"{{"targetDeploymentId":"{target}"}}"#),
                "a missing filter — a batch moves instances off ONE named pin",
            ),
            (
                format!(r#"{{"targetDeploymentId":"{target}","filter":{{}}}}"#),
                "a filter with no sourceDeploymentId",
            ),
            (
                format!(
                    r#"{{"targetDeploymentId":"{target}","filter":{{"sourceDeploymentId":"{source}","status":"COMPLETED"}}}}"#
                ),
                "a terminal status — no instance migrates in it",
            ),
            (
                format!(
                    r#"{{"targetDeploymentId":"{target}","resume":true,"filter":{{"sourceDeploymentId":"{source}","status":"SUSPENDED"}}}}"#
                ),
                "resume over exactly the instances resume refuses",
            ),
            (
                format!(
                    r#"{{"targetDeploymentId":"{target}","targetProcessId":"p2","filter":{{"sourceDeploymentId":"{source}"}}}}"#
                ),
                "a cross-process re-home of a MIXED population",
            ),
            (
                format!(
                    r#"{{"targetDeploymentId":"{target}","nodeMapping":["A"],"filter":{{"sourceDeploymentId":"{source}"}}}}"#
                ),
                "a nodeMapping that is not an object",
            ),
            (
                format!(
                    r#"{{"targetDeploymentId":"{target}","filter":{{"sourceDeploymentId":"{source}","limit":0}}}}"#
                ),
                "a non-positive limit",
            ),
        ] {
            let response = post_body(
                router_with(AdminGate::DevOpen),
                "/admin/instances/migrate",
                &body,
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{why}: {body} answered {}",
                response.status()
            );
        }
    }

    #[tokio::test]
    async fn replay_without_persistence_refuses_rather_than_pretending() {
        let response = request(
            router_with(AdminGate::DevOpen),
            "POST",
            "/admin/dead-letters/1/replay",
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(response).await;
        assert_eq!(body["deadLetterId"], "1");
        assert!(
            body["reason"]
                .as_str()
                .unwrap_or_default()
                .contains("persistence"),
            "the refusal says WHY: {body}"
        );
    }
}
