//! Engine configuration — canonical `sutra.*` keys ONLY (no framework prefixes in
//! contracts, and no env-alias compatibility layer — harnesses set the canonical
//! names), loadable from a properties-style file and the environment:
//!
//! | key (file)                        | canonical env                     |
//! |-----------------------------------|-----------------------------------|
//! | `sutra.deployments.dir`           | `SUTRA_DEPLOYMENTS_DIR`           |
//! | `sutra.deployments.poll-interval` | `SUTRA_DEPLOYMENTS_POLL_INTERVAL` |
//! | `sutra.http.port`                 | `SUTRA_HTTP_PORT`                 |
//! | `sutra.datasource.url`            | `SUTRA_DATASOURCE_URL`            |
//! | `sutra.datasource.username`       | `SUTRA_DATASOURCE_USERNAME`       |
//! | `sutra.datasource.password`       | `SUTRA_DATASOURCE_PASSWORD`       |
//! | `sutra.outbox.tick-interval`      | `SUTRA_OUTBOX_TICK_INTERVAL`      |
//! | `sutra.outbox.retry.base-delay`   | `SUTRA_OUTBOX_RETRY_BASE_DELAY`   |
//! | `sutra.outbox.retry.max-delay`    | `SUTRA_OUTBOX_RETRY_MAX_DELAY`    |
//! | `sutra.outbox.retry.jitter`       | `SUTRA_OUTBOX_RETRY_JITTER`       |
//! | `sutra.outbox.retry.max-attempts` | `SUTRA_OUTBOX_RETRY_MAX_ATTEMPTS` |
//! | `sutra.ack.deferred.capacity`     | `SUTRA_ACK_DEFERRED_CAPACITY`     |
//! | `sutra.ack.deferred.timeout`      | `SUTRA_ACK_DEFERRED_TIMEOUT`      |
//! | `sutra.ack.deferred.sweep-interval` | `SUTRA_ACK_DEFERRED_SWEEP_INTERVAL` |
//! | `sutra.instance.sweep-interval`    | `SUTRA_INSTANCE_SWEEP_INTERVAL`   |
//! | `sutra.instance.claim-timeout`     | `SUTRA_INSTANCE_CLAIM_TIMEOUT`    |
//! | `sutra.instance.retention`         | `SUTRA_INSTANCE_RETENTION`        |
//! | `sutra.instance.retention-sweep-interval` | `SUTRA_INSTANCE_RETENTION_SWEEP_INTERVAL` |
//! | `sutra.engine.shards`             | `SUTRA_ENGINE_SHARDS`             |
//! | `sutra.engine.shard-queue-capacity` | `SUTRA_ENGINE_SHARD_QUEUE_CAPACITY` |
//! | `sutra.external-task.default-lock-duration` | `SUTRA_EXTERNAL_TASK_DEFAULT_LOCK_DURATION` |
//! | `sutra.external-task.max-lock-duration` | `SUTRA_EXTERNAL_TASK_MAX_LOCK_DURATION` |
//! | `sutra.external-task.max-async-response-timeout` | `SUTRA_EXTERNAL_TASK_MAX_ASYNC_RESPONSE_TIMEOUT` |
//! | `sutra.external-task.max-tasks`   | `SUTRA_EXTERNAL_TASK_MAX_TASKS`   |
//! | `sutra.external-task.retries`     | `SUTRA_EXTERNAL_TASK_RETRIES`     |
//! | `sutra.external-task.retry-timeout` | `SUTRA_EXTERNAL_TASK_RETRY_TIMEOUT` |
//! | `sutra.codec.max-payload-bytes`   | `SUTRA_CODEC_MAX_PAYLOAD_BYTES`   |
//! | `sutra.audit.jsonl.path`          | `SUTRA_AUDIT_JSONL`               |
//! | `sutra.audit.otel.endpoint`       | `SUTRA_AUDIT_OTEL_ENDPOINT`       |
//! | `sutra.audit.sql`                 | `SUTRA_AUDIT_SQL`                 |
//! | `sutra.admin.oidc.issuer`         | `SUTRA_ADMIN_OIDC_ISSUER`         |
//! | `sutra.admin.oidc.audience`       | `SUTRA_ADMIN_OIDC_AUDIENCE`       |
//! | `sutra.admin.oidc.jwks`           | `SUTRA_ADMIN_OIDC_JWKS`           |
//! | `sutra.admin.oidc.role-claim`     | `SUTRA_ADMIN_OIDC_ROLE_CLAIM`     |
//! | `sutra.admin.oidc.required-role`  | `SUTRA_ADMIN_OIDC_REQUIRED_ROLE`  |
//! | `sutra.admin.oidc.dev-disabled`   | `SUTRA_ADMIN_OIDC_DEV_DISABLED`   |
//! | `sutra.admin.auth.scheme`         | `SUTRA_ADMIN_AUTH_SCHEME`         |
//! | `sutra.admin.auth.key-ref`        | `SUTRA_ADMIN_AUTH_KEY_REF`        |
//! | `sutra.admin.auth.header`         | `SUTRA_ADMIN_AUTH_HEADER`         |
//!
//! Admin auth: the `/admin/*` surface is gated. Two models, fail-closed:
//! * **Auth key + secret** (`sutra.admin.auth.*`) — the SAME static-secret model the channels use
//!   (`inbound-auth`): `scheme = apikey|bearer`, an expected key/secret resolved from `key-ref`
//!   (`secret:`/`env:`/`${…}`), checked in `header` (default `X-API-Key` / `authorization`). This is
//!   the platform-wide convention and takes precedence when set. (A dedicated multi-tenant IdP
//!   integration layer is a planned future feature — the OIDC gate below is its seed.)
//! * **OIDC bearer-JWT** — issuer + audience + JWKS signature + a required admin scope/claim.
//!   `sutra.admin.oidc.jwks` is a JWKS **URL** (fetched + cached) or an **inline JWKS document**
//!   (via `secret:`/`${secret:…}`, a mounted Secret volume — no network).
//!
//! Gating is disabled ONLY via the explicit dev flag `sutra.admin.oidc.dev-disabled=true`; otherwise
//! a missing/invalid credential → 401, a valid OIDC token without the required role → 403, and a
//! fully-unconfigured surface → 503 (never open).
//!
//! Deployment source: `sutra.deployments.dir` names a directory of sealed
//! `.sutra` archives — the sole deployment source, watched for add/remove/change. It is
//! REQUIRED: the engine has nothing to load without it and refuses to boot.
//!
//! The datasource URL is the native `postgres://` / `postgresql://` form. Telemetry
//! keys (`sutra.telemetry.*`, with the vendor-neutral `OTEL_*` env names
//! accepted) resolve through the same sources — table in
//! [`crate::otel::TelemetryConfig`].
//!
//! Precedence: canonical env > config file > default. The config file path comes from
//! `SUTRA_CONFIG` (default `sutra.properties` in the working directory, loaded only
//! when present). File values may use `${ENV}` / `env:NAME` indirection.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use crate::envref;

/// Where the engine sources its deployment archives (`sutra.deployment.source`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeploymentSourceKind {
    /// The dir/ConfigMap folder-watch source (the legacy default).
    #[default]
    Dir,
    /// The DB-backed `deployment_archive` store — deploy is API-only. Requires a configured
    /// datasource.
    Db,
}

/// Resolved engine configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineConfig {
    /// Which deployment source the engine boots from (`sutra.deployment.source` =
    /// `dir` | `db`, default `dir`). `db` boots its active set from the `deployment_archive`
    /// store and takes deploys only via the API; `dir` watches the archives directory.
    pub deployment_source: DeploymentSourceKind,
    /// Directory of sealed `.sutra` deployment archives (`sutra.deployments.dir`) — the
    /// deployments source (a directory only, in v1), verified fail-closed per
    /// archive and watched for add/remove/change. REQUIRED when `deployment_source = dir`
    /// (`EngineConfig::load` refuses to boot without it); `Option` so `db` mode and
    /// [`EngineConfig::default`] can leave it unset.
    pub deployments_dir: Option<PathBuf>,
    /// Deployments-dir poll cadence (`sutra.deployments.poll-interval`, ISO-8601 or bare
    /// seconds). Default `PT2S`.
    pub deployments_poll_interval: std::time::Duration,
    /// HTTP listen port (`sutra.http.port`); `0` binds a dynamic port. Default `8080`
    /// (container-internal — tests always pass `0`).
    pub http_port: u16,
    /// The ENGINE-INTERNAL datasource (the engine tables: instance/outbox/lease/audit). Never a
    /// module's business store — those own their connections in `datastores.yaml`.
    pub datasource_url: Option<String>,
    pub datasource_username: Option<String>,
    pub datasource_password: Option<String>,
    /// Encryption at rest — the master secret (`sutra.crypto.master-key`, via `secret:`/`env:`)
    /// from which per-tenant DEKs are HKDF-derived. `None` ⇒ encryption disabled:
    /// sensitive instance variables persist as plaintext v2 (backward-compatible). Set it to turn on
    /// at-rest encryption for `@sensitive` + redactor-controlled variables.
    pub crypto_master_key: Option<String>,
    /// KEK-wrap envelope key source (`sutra.crypto.envelope.*`) — the
    /// production alternative to `crypto_master_key`. When enabled, per-tenant DEKs are read
    /// (sealed) from the `data_key` store and unwrapped under a KEK resolved from the envref/KMS
    /// registry, instead of HKDF-derived from a master secret. Mutually exclusive with
    /// `crypto_master_key` (fail-closed if both set); disabled keeps the master-key/plaintext path.
    pub crypto_envelope: CryptoEnvelopeConfig,
    /// When true AND a datasource pool is configured, non-idempotent inbound
    /// failures are durably recorded to the `dead_letter` table (`sutra.incident.sql`), beneath the
    /// always-on `tracing::error!` floor. Default false — opt-in like `sutra.audit.sql`.
    pub incident_sql: bool,
    /// Outbox dispatcher cadence (`sutra.outbox.tick-interval`, ISO-8601 duration or
    /// bare seconds). Default `PT5S` — the tick-interval default.
    pub outbox_tick_interval: std::time::Duration,
    /// Outbox delivery retry curve (`sutra.outbox.retry.*`) — the FIRST instance of the
    /// generalized `sutra.<service>.retry.*` shape. Defaults reproduce the
    /// shipped hard-coded curve (base `PT1S`, max `PT5M`, jitter on), so an unconfigured
    /// engine's retry behaviour is byte-identical. Any future `sutra.<service>.retry.*` block
    /// reuses [`parse_retry`] verbatim.
    pub outbox_retry: RetryConfig,
    /// The deferred-ack registry knobs (`sutra.ack.deferred.*`) for broker
    /// `ack-mode: on-complete` channels: bounded capacity (default 10 000), per-entry
    /// timeout (default `PT1H`), and the `sweep_timeouts()` cadence (default `PT1M`).
    pub deferred_ack: DeferredAckConfig,
    /// The worker-facing pull surface's bounds (`sutra.external-task.*`) — lock durations,
    /// long-poll ceiling, batch ceiling, and the retry budget a parked task starts with. Every
    /// one is a CEILING: a worker asks for less or is rejected, never silently clamped.
    pub external_task: sutra_channels::ExternalTaskLimits,
    /// The stuck-instance scanner knobs (`sutra.instance.*`): the sweep cadence
    /// (`sweep-interval`, default `PT1M`) and how long a per-instance ownership claim
    /// survives an owner's silence before the sweep clears it (`claim-timeout`, default
    /// `PT5M`). See [`crate::sweeper::StuckInstanceScanner`].
    pub instance_sweep: crate::sweeper::StuckInstanceScannerConfig,
    /// The execution shard-router knobs (`sutra.engine.shards` /
    /// `sutra.engine.shard-queue-capacity`): the actor-lane count (default 1 —
    /// byte-identical to the single-lane engine) and the opt-in per-lane mailbox bound.
    /// See [`EngineShardConfig`] for the N>1 semantics.
    pub engine_shards: EngineShardConfig,
    /// Terminal-instance retention (`sutra.instance.retention`, default `P7D`) and the purge
    /// cadence that enforces it (`sutra.instance.retention-sweep-interval`, default `PT1H`).
    /// A finished instance's row is RETAINED (snapshot re-stamped COMPLETED/TERMINATED) so
    /// `GET /sutra/instances/{id}` keeps answering, and purged once the window elapses. The
    /// explicit value `PT0S` restores the pre-P1-2 behaviour — deleted in the terminal
    /// transaction, no history. See [`crate::sweeper::TerminalRetentionSweeper`].
    pub instance_retention: crate::sweeper::TerminalRetentionConfig,
    /// The global inbound payload byte-cap (`sutra.codec.max-payload-bytes`) seeded
    /// into the dispatcher's [`sutra_channels::PayloadCapPolicy`] as the default ceiling;
    /// each channel's `payload-cap-bytes` overrides it per channel. Default 10 MiB;
    /// `0` disables the global cap.
    pub payload_cap_bytes: u64,
    /// Whether the boot RLS-bypass posture check REFUSES startup when the engine's
    /// PostgreSQL role can bypass RLS (superuser / BYPASSRLS)
    /// (`sutra.persistence.rls-bypass-check.enabled`). Default `true` (fail-closed). Setting
    /// it `false` acknowledges the risk and downgrades the refusal to a WARNING (dev only).
    pub rls_bypass_check_enabled: bool,
    /// The engine-global audit sinks (JSONL file + OTel-log). Both compose — each
    /// enabled sink is registered on the per-activation [`sutra_channels::AuditSinkRegistry`], and
    /// the `AuditListener` is wired only when at least one is active. A best-effort trail — it
    /// never blocks or fails execution; the per-deployment DB sink is the follow-on.
    pub audit: AuditConfig,
    /// OTLP telemetry keys (endpoint / service-name / temporality / label
    /// allowlist — table in [`crate::otel::TelemetryConfig`]). Resolved from the SAME
    /// sources, but fail-OPEN: bad telemetry values degrade to defaults with warnings,
    /// they never refuse boot.
    pub telemetry: crate::otel::TelemetryConfig,
    /// The `/admin/*` OIDC gating keys (`sutra.admin.oidc.*`). Fail-CLOSED — the admin
    /// surface is never open by default; see [`AdminAuthConfig`].
    pub admin_auth: AdminAuthConfig,
    /// TEST-ONLY seam (P1-7 time-skipping test runtime): installs a [`sutra_executor::TestClock`]
    /// as the "now" every temporal read in this boot uses — the executor's `now_supplier` (park
    /// due-ats, `<q:retry>` backoff), the timer poller's per-tick claim instant, and the
    /// timer-`<startEvent>` schedule arming instant. `None` (the only value [`EngineConfig::load`]
    /// / [`EngineConfig::default`] ever produce — there is no `sutra.*` key or `SUTRA_*` env var
    /// for this field) reads the real wall clock exactly as before this field existed: every
    /// existing boot path is byte-identical. Set it only by constructing an `EngineConfig` from
    /// Rust code that holds a `TestClock` — see `sutra_engine::fast_forward_until` for the paired
    /// test helper.
    pub now_override: Option<sutra_executor::TestClock>,
}

impl Default for EngineConfig {
    /// The documented per-key defaults with NO deployments source — callers (tests) fill
    /// in `deployments_dir`; `EngineConfig::load` enforces that it is set.
    fn default() -> EngineConfig {
        EngineConfig {
            deployment_source: DeploymentSourceKind::Dir,
            deployments_dir: None,
            deployments_poll_interval: std::time::Duration::from_secs(2),
            http_port: 8080,
            datasource_url: None,
            crypto_master_key: None,
            crypto_envelope: CryptoEnvelopeConfig::default(),
            incident_sql: false,
            datasource_username: None,
            datasource_password: None,
            outbox_tick_interval: std::time::Duration::from_secs(5),
            outbox_retry: RetryConfig::default(),
            deferred_ack: DeferredAckConfig::default(),
            external_task: sutra_channels::ExternalTaskLimits::default(),
            instance_sweep: crate::sweeper::StuckInstanceScannerConfig::default(),
            engine_shards: EngineShardConfig::default(),
            instance_retention: crate::sweeper::TerminalRetentionConfig::default(),
            payload_cap_bytes: sutra_channels::PayloadCapPolicy::DEFAULT_MAX_PAYLOAD_BYTES,
            rls_bypass_check_enabled: true,
            audit: AuditConfig::default(),
            telemetry: crate::otel::TelemetryConfig::default(),
            admin_auth: AdminAuthConfig::default(),
            now_override: None,
        }
    }
}

/// The `/admin/*` OIDC gating config (`sutra.admin.oidc.*`). The engine validates a
/// bearer JWT on every admin request: signature (against the JWKS), `iss`, `aud`, expiry, and a
/// required admin scope/claim. Fail-CLOSED:
///
/// * `dev_disabled == true` → the `/admin/*` surface is served WITHOUT auth (dev/compose only; a
///   loud startup warning records the posture). This is the ONLY way to disable gating.
/// * `dev_disabled == false` (default) AND `issuer`/`audience`/`jwks` all set → real JWT gating.
/// * `dev_disabled == false` AND the OIDC params are absent/incomplete → the surface is
///   **unconfigured** and every admin request fails closed with `503` (never open by default).
///
/// The values resolve through the envref SPI (`env:`/`secret:`/`${…}`) like every other engine
/// key; `jwks` additionally accepts a whole-value `secret:`/`env:` ref carrying an inline JWKS
/// document, so a mounted Secret volume feeds the keys with no network fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminAuthConfig {
    /// `sutra.admin.oidc.issuer` — the expected token `iss` (exact match). Required for gating.
    pub issuer: Option<String>,
    /// `sutra.admin.oidc.audience` — the expected token `aud` (membership). Required for gating.
    pub audience: Option<String>,
    /// `sutra.admin.oidc.jwks` — a JWKS **URL** (`https://…`, fetched + cached) or an inline JWKS
    /// JSON document (delivered via `secret:`/`${secret:…}`). Required for gating.
    pub jwks: Option<String>,
    /// `sutra.admin.oidc.role-claim` — the JWT claim carrying the caller's roles/scopes. A
    /// space-delimited string (OAuth2 `scope`) or a JSON array are both accepted. Default `roles`.
    pub role_claim: String,
    /// `sutra.admin.oidc.required-role` — the scope/role a caller must hold to reach `/admin/*`.
    /// A validated token that lacks it is authenticated-but-unauthorized → `403`. Default
    /// `sutra-admin`.
    pub required_role: String,
    /// `sutra.admin.oidc.dev-disabled` — the ONE explicit escape hatch: `true` serves `/admin/*`
    /// with NO auth (dev only). Default `false` (gating required). Never disabled implicitly.
    pub dev_disabled: bool,
    /// `sutra.admin.auth.scheme` — the static-secret admin gate (`apikey` | `bearer`), the SAME
    /// message-auth model the channels use (`inbound-auth.*`). When set (with `auth_key_ref`) it
    /// gates `/admin/*` in place of OIDC — the platform-wide auth-key+secret convention: possession
    /// of the expected key/secret in the configured header authorizes the request (no per-caller
    /// identity or scopes; that is the OIDC gate's role). Takes precedence over OIDC when both are
    /// set. Fail-closed on a missing/unresolvable key.
    pub auth_scheme: Option<String>,
    /// `sutra.admin.auth.key-ref` — the resolver ref (`secret:`/`env:`/`${…}`) whose resolved value
    /// is the expected key/secret. Required when `auth_scheme` is set.
    pub auth_key_ref: Option<String>,
    /// `sutra.admin.auth.header` — the header carrying the presented credential (default: `X-API-Key`
    /// for apikey, `authorization` for bearer).
    pub auth_header: Option<String>,
}

impl Default for AdminAuthConfig {
    /// Fail-closed defaults: no OIDC params, `roles`/`sutra-admin` claim wiring, gating NOT
    /// dev-disabled. With no issuer/audience/jwks set this yields an UNCONFIGURED surface that
    /// answers `503` on every admin request — closed until an operator either configures OIDC or
    /// sets the explicit dev flag.
    fn default() -> AdminAuthConfig {
        AdminAuthConfig {
            issuer: None,
            audience: None,
            jwks: None,
            role_claim: "roles".to_string(),
            required_role: "sutra-admin".to_string(),
            dev_disabled: false,
            auth_scheme: None,
            auth_key_ref: None,
            auth_header: None,
        }
    }
}

/// Retry-curve config (`sutra.<service>.retry.*`) — the generalized shape. Carries
/// plain values because the runtime [`sutra_channels::RetryPolicy`] holds a non-`Eq` jitter
/// sampler; it is converted to a policy at the wiring point. Defaults reproduce
/// `RetryPolicy::default()` (base `PT1S`, max `PT5M`, jitter on).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryConfig {
    pub base_delay: std::time::Duration,
    pub max_delay: std::time::Duration,
    pub jitter: bool,
    /// `sutra.<service>.retry.max-attempts` — the total attempts before an entry is abandoned.
    ///
    /// **`None` is the default and it means RETRY FOREVER**, which is deliberate and documented
    /// rather than an oversight: the outbox's whole contract is at-least-once delivery, and a
    /// default ceiling would silently convert "your broker was down for an hour" into "your
    /// message was dropped". Operators who prefer a bounded queue opt in; when they do, an
    /// exhausted entry is flagged terminal (V604 `poisoned`) rather than deleted, and raises one
    /// durable incident (`SUTRA.OUTBOUND.DELIVERY_ATTEMPTS_EXHAUSTED`).
    ///
    /// Values below 1 are rejected at load — "give up before attempting anything" is never what
    /// was meant.
    pub max_attempts: Option<i32>,
}

impl Default for RetryConfig {
    fn default() -> RetryConfig {
        RetryConfig {
            base_delay: std::time::Duration::from_secs(1),
            max_delay: std::time::Duration::from_secs(300),
            jitter: true,
            max_attempts: None,
        }
    }
}

/// The composable engine-global audit-sink config. Each field enables one built-in
/// [`sutra_channels::AuditSink`]; the assembly registers every enabled sink on one
/// [`sutra_channels::AuditSinkRegistry`], so JSONL and OTel compose (the DB sink is the follow-on).
/// `Default` = every sink off (`<q:audit>` has no engine-global target — audit emission is a no-op).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AuditConfig {
    /// `sutra.audit.jsonl.path` — when set, the JSONL sink appends per-instance/per-node events to
    /// this file (the shape `sutra audit-replay --from-jsonl` reads).
    pub jsonl_path: Option<PathBuf>,
    /// `sutra.audit.otel.endpoint` — the OTLP endpoint of a DEDICATED audit observability stack. When
    /// set, the OTel-log sink exports each audit event as an OTLP log record (on the `sutra.audit`
    /// instrumentation scope) through its OWN logs pipeline pointed here — NEVER the engine's
    /// telemetry stream (audit stays a distinct, precisely-cullable sidecar). `None` = the OTel sink
    /// is unavailable.
    pub otel_endpoint: Option<String>,
    /// `sutra.audit.sql` — when true AND a datasource pool is configured, the SQL sink writes each
    /// event to the shipped `audit_event` table (the `<q:audit>` default sink). Idempotent per
    /// `(deployment_id, instance_id, seq)`; the durable audit of record.
    pub sql: bool,
}

/// KEK-wrap envelope key source (`sutra.crypto.envelope.*`). When
/// `enabled`, the assembly builds a `sutra_crypto::EnvelopeKeyProvider`: it loads the sealed
/// `key_id → WrappedDataKey` map from the `data_key` store and unwraps DEKs under a KEK resolved
/// (whole-value `secret:`/`env:`/`vault:`/`aws-secrets:`) from `kek_ref`. `Default` = disabled.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CryptoEnvelopeConfig {
    /// `sutra.crypto.envelope.enabled` — selects the envelope KeyProvider over `crypto_master_key`
    /// (mutually exclusive; the two are validated fail-closed at load).
    pub enabled: bool,
    /// `sutra.crypto.envelope.kek` — the key-encryption-key reference, resolved whole-value at
    /// build time (NOT trimmed / not scheme-resolved here). Required when `enabled`.
    pub kek_ref: Option<String>,
}

/// The deferred-ack registry knobs (`sutra.ack.deferred.*`). The registry backs
/// broker `ack-mode: on-complete` channels: a parked instance's broker settle is held
/// here until its terminal event; `capacity` bounds the in-memory registry (overflow
/// nacks the OLDEST entry — `SUTRA.ACK.DEFERRED_OVERFLOW`), `timeout` is the per-entry
/// max age the periodic sweep enforces (`SUTRA.ACK.DEFERRED_TIMEOUT`), and
/// `sweep_interval` is the sweep task's cadence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredAckConfig {
    /// `sutra.ack.deferred.capacity` (env `SUTRA_ACK_DEFERRED_CAPACITY`) — bounded
    /// registry size. Default 10 000.
    pub capacity: usize,
    /// `sutra.ack.deferred.timeout` (env `SUTRA_ACK_DEFERRED_TIMEOUT`, ISO-8601 or bare
    /// seconds) — per-entry max age before the sweep nacks it. Default `PT1H`.
    pub timeout: std::time::Duration,
    /// `sutra.ack.deferred.sweep-interval` (env `SUTRA_ACK_DEFERRED_SWEEP_INTERVAL`,
    /// ISO-8601 or bare seconds) — the `sweep_timeouts()` cadence. Default `PT1M`.
    pub sweep_interval: std::time::Duration,
}

impl Default for DeferredAckConfig {
    fn default() -> DeferredAckConfig {
        DeferredAckConfig {
            capacity: 10_000,
            timeout: std::time::Duration::from_secs(3600),
            sweep_interval: std::time::Duration::from_secs(60),
        }
    }
}

/// The execution shard-router knobs (`sutra.engine.*`) — the engine-actor lane count and
/// the per-lane mailbox bound consumed by `sutra_channels::spawn_engine_sharded`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineShardConfig {
    /// `sutra.engine.shards` (env `SUTRA_ENGINE_SHARDS`) — integer ≥ 1, default `1`
    /// (one actor lane, byte-identical to the pre-router engine). At N > 1 the router
    /// spawns N identical actor lanes and routes every piece of instance-addressed work
    /// by the stable instance-id hash; per-INSTANCE serialization is preserved at every
    /// N, while the incidental cross-INSTANCE serialization the single lane provided as
    /// a side effect disappears (execution scale-out §6.1) — which is why the default
    /// stays 1 and turning it up is an explicit operator action.
    pub shards: u32,
    /// `sutra.engine.shard-queue-capacity` (env `SUTRA_ENGINE_SHARD_QUEUE_CAPACITY`) —
    /// per-shard mailbox bound, integer ≥ 1. `None` (the default, and the unset value) is
    /// UNBOUNDED — parity with the engine queue as it has always been. When bounded, a
    /// full mailbox makes the enqueue await on the caller's async task, so backpressure
    /// propagates to the transport rather than growing the queue.
    pub queue_capacity: Option<usize>,
}

impl Default for EngineShardConfig {
    fn default() -> EngineShardConfig {
        EngineShardConfig {
            shards: 1,
            queue_capacity: None,
        }
    }
}

/// A configuration error — missing required key or unresolvable value (fail-closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl EngineConfig {
    /// Load from the process environment + the optional config file (see module docs).
    pub fn load() -> Result<EngineConfig, ConfigError> {
        let file = std::env::var("SUTRA_CONFIG").unwrap_or_else(|_| "sutra.properties".into());
        let file_values = read_properties(&PathBuf::from(file))?;
        EngineConfig::from_sources(&file_values, &|name| std::env::var(name).ok())
    }

    /// Pure resolution over explicit sources (unit-testable without process-global env).
    fn from_sources(
        file: &BTreeMap<String, String>,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<EngineConfig, ConfigError> {
        let value = |file_key: &str, canonical: &str| -> Result<Option<String>, ConfigError> {
            let raw = env(canonical).or_else(|| file.get(file_key).cloned());
            match raw {
                None => Ok(None),
                Some(raw) => envref::resolve_placeholders(&raw)
                    .map(Some)
                    .map_err(|e| ConfigError(format!("config key '{file_key}': {e}"))),
            }
        };

        let deployment_source = match value("sutra.deployment.source", "SUTRA_DEPLOYMENT_SOURCE")? {
            None => DeploymentSourceKind::Dir,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "dir" => DeploymentSourceKind::Dir,
                "db" => DeploymentSourceKind::Db,
                other => {
                    return Err(ConfigError(format!(
                        "sutra.deployment.source: '{other}' is not 'dir' or 'db'"
                    )))
                }
            },
        };

        let deployments_dir = value("sutra.deployments.dir", "SUTRA_DEPLOYMENTS_DIR")?;
        if deployment_source == DeploymentSourceKind::Dir && deployments_dir.is_none() {
            return Err(ConfigError(
                "sutra.deployments.dir (env SUTRA_DEPLOYMENTS_DIR — a directory of .sutra \
                 archives) is required for the 'dir' deployment source; the engine has nothing \
                 to load without one (use sutra.deployment.source=db for the DB-backed store)"
                    .to_string(),
            ));
        }

        let deployments_poll_interval = match value(
            "sutra.deployments.poll-interval",
            "SUTRA_DEPLOYMENTS_POLL_INTERVAL",
        )? {
            None => std::time::Duration::from_secs(2),
            Some(raw) => parse_duration(&raw).map_err(|e| {
                ConfigError(format!("sutra.deployments.poll-interval: '{raw}' — {e}"))
            })?,
        };

        let http_port = match value("sutra.http.port", "SUTRA_HTTP_PORT")? {
            None => 8080,
            Some(p) => p
                .trim()
                .parse::<u16>()
                .map_err(|_| ConfigError(format!("sutra.http.port: '{p}' is not a valid port")))?,
        };

        let outbox_tick_interval =
            match value("sutra.outbox.tick-interval", "SUTRA_OUTBOX_TICK_INTERVAL")? {
                None => std::time::Duration::from_secs(5),
                Some(raw) => parse_duration(&raw).map_err(|e| {
                    ConfigError(format!("sutra.outbox.tick-interval: '{raw}' — {e}"))
                })?,
            };

        let outbox_retry = parse_retry(&value, "outbox", "OUTBOX", RetryConfig::default())?;

        // The deferred-ack registry knobs (`sutra.ack.deferred.*`).
        let deferred_ack_defaults = DeferredAckConfig::default();
        let deferred_ack = DeferredAckConfig {
            capacity: match value("sutra.ack.deferred.capacity", "SUTRA_ACK_DEFERRED_CAPACITY")? {
                None => deferred_ack_defaults.capacity,
                Some(raw) => raw
                    .trim()
                    .parse::<usize>()
                    .ok()
                    .filter(|c| *c > 0)
                    .ok_or_else(|| {
                        ConfigError(format!(
                            "sutra.ack.deferred.capacity: '{raw}' is not a positive entry count"
                        ))
                    })?,
            },
            timeout: match value("sutra.ack.deferred.timeout", "SUTRA_ACK_DEFERRED_TIMEOUT")? {
                None => deferred_ack_defaults.timeout,
                Some(raw) => parse_duration(&raw).map_err(|e| {
                    ConfigError(format!("sutra.ack.deferred.timeout: '{raw}' — {e}"))
                })?,
            },
            sweep_interval: match value(
                "sutra.ack.deferred.sweep-interval",
                "SUTRA_ACK_DEFERRED_SWEEP_INTERVAL",
            )? {
                None => deferred_ack_defaults.sweep_interval,
                Some(raw) => parse_duration(&raw).map_err(|e| {
                    ConfigError(format!("sutra.ack.deferred.sweep-interval: '{raw}' — {e}"))
                })?,
            },
        };
        if deferred_ack.sweep_interval.is_zero() {
            return Err(ConfigError(
                "sutra.ack.deferred.sweep-interval must be positive (a zero interval would \
                 hot-loop the sweep task)"
                    .to_string(),
            ));
        }

        // The external-task (pull) surface's bounds (`sutra.external-task.*`).
        let external_task_defaults = sutra_channels::ExternalTaskLimits::default();
        let external_task = sutra_channels::ExternalTaskLimits {
            default_lock_duration: duration_key(
                &value,
                "sutra.external-task.default-lock-duration",
                "SUTRA_EXTERNAL_TASK_DEFAULT_LOCK_DURATION",
                external_task_defaults.default_lock_duration,
            )?,
            max_lock_duration: duration_key(
                &value,
                "sutra.external-task.max-lock-duration",
                "SUTRA_EXTERNAL_TASK_MAX_LOCK_DURATION",
                external_task_defaults.max_lock_duration,
            )?,
            max_async_response_timeout: duration_key(
                &value,
                "sutra.external-task.max-async-response-timeout",
                "SUTRA_EXTERNAL_TASK_MAX_ASYNC_RESPONSE_TIMEOUT",
                external_task_defaults.max_async_response_timeout,
            )?,
            max_tasks: match value(
                "sutra.external-task.max-tasks",
                "SUTRA_EXTERNAL_TASK_MAX_TASKS",
            )? {
                None => external_task_defaults.max_tasks,
                Some(raw) => raw
                    .trim()
                    .parse::<u32>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        ConfigError(format!(
                            "sutra.external-task.max-tasks: '{raw}' is not a positive task count"
                        ))
                    })?,
            },
            retries: match value("sutra.external-task.retries", "SUTRA_EXTERNAL_TASK_RETRIES")? {
                None => external_task_defaults.retries,
                Some(raw) => raw
                    .trim()
                    .parse::<i32>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .ok_or_else(|| {
                        ConfigError(format!(
                            "sutra.external-task.retries: '{raw}' is not a non-negative retry \
                             budget"
                        ))
                    })?,
            },
            retry_timeout: duration_key(
                &value,
                "sutra.external-task.retry-timeout",
                "SUTRA_EXTERNAL_TASK_RETRY_TIMEOUT",
                external_task_defaults.retry_timeout,
            )?,
        };
        if external_task.default_lock_duration > external_task.max_lock_duration {
            return Err(ConfigError(
                "sutra.external-task.default-lock-duration must not exceed \
                 sutra.external-task.max-lock-duration (the default would be rejected by its \
                 own ceiling on every request)"
                    .to_string(),
            ));
        }
        if external_task.default_lock_duration.is_zero() {
            return Err(ConfigError(
                "sutra.external-task.default-lock-duration must be positive (a zero lock is \
                 expired the instant it is handed out)"
                    .to_string(),
            ));
        }

        // The stuck-instance scanner knobs (`sutra.instance.*`).
        let instance_sweep_defaults = crate::sweeper::StuckInstanceScannerConfig::default();
        let instance_sweep = crate::sweeper::StuckInstanceScannerConfig {
            interval: match value(
                "sutra.instance.sweep-interval",
                "SUTRA_INSTANCE_SWEEP_INTERVAL",
            )? {
                None => instance_sweep_defaults.interval,
                Some(raw) => parse_duration(&raw).map_err(|e| {
                    ConfigError(format!("sutra.instance.sweep-interval: '{raw}' — {e}"))
                })?,
            },
            claim_timeout: match value(
                "sutra.instance.claim-timeout",
                "SUTRA_INSTANCE_CLAIM_TIMEOUT",
            )? {
                None => instance_sweep_defaults.claim_timeout,
                Some(raw) => parse_duration(&raw).map_err(|e| {
                    ConfigError(format!("sutra.instance.claim-timeout: '{raw}' — {e}"))
                })?,
            },
        };
        if instance_sweep.interval.is_zero() {
            return Err(ConfigError(
                "sutra.instance.sweep-interval must be positive (a zero interval would \
                 hot-loop the stuck-instance scanner)"
                    .to_string(),
            ));
        }
        if instance_sweep.claim_timeout.is_zero() {
            return Err(ConfigError(
                "sutra.instance.claim-timeout must be positive (a zero timeout would reclaim \
                 every instance the instant a replica claims it, re-opening the \
                 double-resume window the claim exists to close)"
                    .to_string(),
            ));
        }
        if instance_sweep.claim_timeout <= instance_sweep.interval {
            return Err(ConfigError(format!(
                "sutra.instance.claim-timeout ({:?}) must be strictly greater than \
                 sutra.instance.sweep-interval ({:?}) — a claim that expires within one sweep \
                 tick can be cleared while its owner is still mid-step",
                instance_sweep.claim_timeout, instance_sweep.interval
            )));
        }

        // The execution shard-router knobs (`sutra.engine.*`).
        let engine_shard_defaults = EngineShardConfig::default();
        let engine_shards = EngineShardConfig {
            shards: match value("sutra.engine.shards", "SUTRA_ENGINE_SHARDS")? {
                None => engine_shard_defaults.shards,
                Some(raw) => raw
                    .trim()
                    .parse::<u32>()
                    .ok()
                    .filter(|n| *n >= 1)
                    .ok_or_else(|| {
                        ConfigError(format!(
                            "sutra.engine.shards: '{raw}' is not an integer >= 1 (1 = the \
                             single-lane default)"
                        ))
                    })?,
            },
            queue_capacity: match value(
                "sutra.engine.shard-queue-capacity",
                "SUTRA_ENGINE_SHARD_QUEUE_CAPACITY",
            )? {
                None => engine_shard_defaults.queue_capacity,
                Some(raw) => Some(
                    raw.trim()
                        .parse::<usize>()
                        .ok()
                        .filter(|n| *n >= 1)
                        .ok_or_else(|| {
                            ConfigError(format!(
                                "sutra.engine.shard-queue-capacity: '{raw}' is not an integer \
                                 >= 1 (leave the key unset for an unbounded queue)"
                            ))
                        })?,
                ),
            },
        };
        // N > 1 is LIVE (execution scale-out Phase 2): the router spawns one actor lane
        // per shard. The 0-refusal above (the `>= 1` filter) is the only validation left.

        // Terminal-instance retention (`sutra.instance.retention*`). Note the ASYMMETRY with the
        // knobs above: a zero RETENTION is legal and meaningful (it restores delete-at-terminal),
        // while a zero purge INTERVAL is not (it would hot-loop the sweep task) — so only the
        // latter is rejected.
        let retention_defaults = crate::sweeper::TerminalRetentionConfig::default();
        let instance_retention = crate::sweeper::TerminalRetentionConfig {
            retention: match value("sutra.instance.retention", "SUTRA_INSTANCE_RETENTION")? {
                None => retention_defaults.retention,
                Some(raw) => parse_duration(&raw)
                    .map_err(|e| ConfigError(format!("sutra.instance.retention: '{raw}' — {e}")))?,
            },
            interval: match value(
                "sutra.instance.retention-sweep-interval",
                "SUTRA_INSTANCE_RETENTION_SWEEP_INTERVAL",
            )? {
                None => retention_defaults.interval,
                Some(raw) => parse_duration(&raw).map_err(|e| {
                    ConfigError(format!(
                        "sutra.instance.retention-sweep-interval: '{raw}' — {e}"
                    ))
                })?,
            },
        };
        if instance_retention.interval.is_zero() {
            return Err(ConfigError(
                "sutra.instance.retention-sweep-interval must be positive (a zero interval \
                 would hot-loop the terminal-retention purge). To keep NO history at all, set \
                 sutra.instance.retention=PT0S instead — that deletes at the terminal step and \
                 leaves the sweep with nothing to do"
                    .to_string(),
            ));
        }

        let payload_cap_bytes = match value(
            "sutra.codec.max-payload-bytes",
            "SUTRA_CODEC_MAX_PAYLOAD_BYTES",
        )? {
            None => sutra_channels::PayloadCapPolicy::DEFAULT_MAX_PAYLOAD_BYTES,
            Some(raw) => raw.trim().parse::<u64>().map_err(|_| {
                ConfigError(format!(
                    "sutra.codec.max-payload-bytes: '{raw}' is not a non-negative byte \
                         count (use 0 to disable the global cap)"
                ))
            })?,
        };

        let rls_bypass_check_enabled = match value(
            "sutra.persistence.rls-bypass-check.enabled",
            "SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED",
        )? {
            None => true,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" => false,
                other => {
                    return Err(ConfigError(format!(
                        "sutra.persistence.rls-bypass-check.enabled: '{other}' is not a boolean \
                         (true/false)"
                    )))
                }
            },
        };

        // Crypto key source: the master key (HKDF DEKs) and the envelope KEK (unwraps DEKs from the
        // data_key store) are mutually exclusive — computed before the struct so the XOR can fail closed.
        let crypto_master_key = value("sutra.crypto.master-key", "SUTRA_CRYPTO_MASTER_KEY")?;
        let crypto_envelope = CryptoEnvelopeConfig {
            enabled: match value(
                "sutra.crypto.envelope.enabled",
                "SUTRA_CRYPTO_ENVELOPE_ENABLED",
            )? {
                None => false,
                Some(raw) => parse_bool(&raw, "sutra.crypto.envelope.enabled")?,
            },
            // NOT trimmed — resolved whole-value (`secret:`/`env:`/`vault:`/…) at build time.
            kek_ref: value("sutra.crypto.envelope.kek", "SUTRA_CRYPTO_ENVELOPE_KEK")?
                .filter(|s| !s.trim().is_empty()),
        };
        if crypto_envelope.enabled {
            if crypto_master_key.is_some() {
                return Err(ConfigError(
                    "sutra.crypto.envelope.enabled=true is mutually exclusive with \
                     sutra.crypto.master-key — set exactly one key source (the envelope KEK unwraps \
                     DEKs from the data_key store; the master key HKDF-derives them). Refusing to boot."
                        .to_string(),
                ));
            }
            if crypto_envelope.kek_ref.is_none() {
                return Err(ConfigError(
                    "sutra.crypto.envelope.enabled=true requires sutra.crypto.envelope.kek (a \
                     secret:/env:/vault:/aws-secrets: reference to the key-encryption key). \
                     Refusing to boot."
                        .to_string(),
                ));
            }
        }
        let incident_sql = match value("sutra.incident.sql", "SUTRA_INCIDENT_SQL")? {
            None => false,
            Some(raw) => parse_bool(&raw, "sutra.incident.sql")?,
        };

        Ok(EngineConfig {
            deployment_source,
            deployments_dir: deployments_dir.map(PathBuf::from),
            deployments_poll_interval,
            http_port,
            datasource_url: value("sutra.datasource.url", "SUTRA_DATASOURCE_URL")?,
            datasource_username: value("sutra.datasource.username", "SUTRA_DATASOURCE_USERNAME")?,
            datasource_password: value("sutra.datasource.password", "SUTRA_DATASOURCE_PASSWORD")?,
            crypto_master_key,
            crypto_envelope,
            incident_sql,
            outbox_tick_interval,
            outbox_retry,
            deferred_ack,
            external_task,
            instance_sweep,
            engine_shards,
            instance_retention,
            payload_cap_bytes,
            rls_bypass_check_enabled,
            audit: AuditConfig {
                jsonl_path: value("sutra.audit.jsonl.path", "SUTRA_AUDIT_JSONL")?
                    .map(PathBuf::from),
                otel_endpoint: value("sutra.audit.otel.endpoint", "SUTRA_AUDIT_OTEL_ENDPOINT")?
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
                sql: match value("sutra.audit.sql", "SUTRA_AUDIT_SQL")? {
                    None => false,
                    Some(raw) => parse_bool(&raw, "sutra.audit.sql")?,
                },
            },
            telemetry: crate::otel::TelemetryConfig::from_sources(file, env),
            admin_auth: AdminAuthConfig {
                issuer: value("sutra.admin.oidc.issuer", "SUTRA_ADMIN_OIDC_ISSUER")?
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
                audience: value("sutra.admin.oidc.audience", "SUTRA_ADMIN_OIDC_AUDIENCE")?
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
                // NB: NOT trimmed — an inline JWKS document has surrounding braces, not padding;
                // resolved further (whole-value `secret:`/`env:`) at auth-runtime build time.
                jwks: value("sutra.admin.oidc.jwks", "SUTRA_ADMIN_OIDC_JWKS")?
                    .filter(|s| !s.trim().is_empty()),
                role_claim: value("sutra.admin.oidc.role-claim", "SUTRA_ADMIN_OIDC_ROLE_CLAIM")?
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "roles".to_string()),
                required_role: value(
                    "sutra.admin.oidc.required-role",
                    "SUTRA_ADMIN_OIDC_REQUIRED_ROLE",
                )?
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "sutra-admin".to_string()),
                dev_disabled: match value(
                    "sutra.admin.oidc.dev-disabled",
                    "SUTRA_ADMIN_OIDC_DEV_DISABLED",
                )? {
                    None => false,
                    Some(raw) => parse_bool(&raw, "sutra.admin.oidc.dev-disabled")?,
                },
                auth_scheme: value("sutra.admin.auth.scheme", "SUTRA_ADMIN_AUTH_SCHEME")?
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
                // NOT trimmed here — resolved (whole-value `secret:`/`env:`/`${…}`) at gate-build time.
                auth_key_ref: value("sutra.admin.auth.key-ref", "SUTRA_ADMIN_AUTH_KEY_REF")?
                    .filter(|s| !s.trim().is_empty()),
                auth_header: value("sutra.admin.auth.header", "SUTRA_ADMIN_AUTH_HEADER")?
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
            },
            // Deliberately not read from `file`/`env` — see the field doc: the ONLY way to set
            // this is constructing an `EngineConfig` from Rust code, never config-driven.
            now_override: None,
        })
    }
}

/// Parse a duration value: the ISO-8601 `P…DT…H…M…S` subset the configs use
/// (`PT5S`, `PT0.2S`, `PT1M30S`, `PT1H`, `P7D`, `P1DT12H`; case-insensitive, fractional values
/// allowed) or a bare non-negative integer meaning seconds.
///
/// The DAY component was added for `sutra.instance.retention`, whose default (`P7D`) is a window an
/// operator naturally states in days; every existing `PT…` value parses exactly as before. Days are
/// treated as 24 h — these are engine cadences and retention windows, not calendar arithmetic, so
/// there is no zone or DST to honour. Weeks/months/years are deliberately NOT accepted: a month is
/// not a duration.
fn parse_duration(raw: &str) -> Result<std::time::Duration, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("empty duration".to_string());
    }
    if let Ok(seconds) = value.parse::<u64>() {
        return Ok(std::time::Duration::from_secs(seconds));
    }
    let upper = value.to_ascii_uppercase();
    let Some(body) = upper.strip_prefix('P') else {
        return Err("expected an ISO-8601 duration (P…/PT…) or bare seconds".to_string());
    };
    // Split the date part (days) from the time part; either half may be absent, but at least one
    // component must be present overall.
    let (date_part, time_part) = match body.split_once('T') {
        Some((date, time)) => (date, Some(time)),
        None => (body, None),
    };
    if date_part.is_empty() && time_part.map(str::is_empty).unwrap_or(true) {
        return Err("ISO-8601 duration has no components".to_string());
    }
    let mut total_ms: u64 = 0;
    // `accumulate` folds one half of the duration; `units` is the set of unit letters legal there,
    // so an `S` in the date half (or a `D` in the time half) is rejected rather than silently
    // accepted in the wrong position.
    let mut accumulate = |components: &str, units: &[(char, f64)]| -> Result<(), String> {
        let mut number = String::new();
        for c in components.chars() {
            if c.is_ascii_digit() || c == '.' {
                number.push(c);
                continue;
            }
            let Some((_, unit_ms)) = units.iter().find(|(unit, _)| *unit == c) else {
                return Err(format!("unexpected character '{c}'"));
            };
            let parsed = number
                .parse::<f64>()
                .map_err(|_| format!("invalid number '{number}'"))?;
            if !parsed.is_finite() || parsed < 0.0 {
                return Err(format!("invalid number '{number}'"));
            }
            total_ms += (parsed * unit_ms).round() as u64;
            number.clear();
        }
        if !number.is_empty() {
            return Err(format!("dangling number '{number}' (missing unit)"));
        }
        Ok(())
    };
    accumulate(date_part, &[('D', 86_400_000.0)])?;
    if let Some(time_part) = time_part {
        if time_part.is_empty() {
            return Err("ISO-8601 duration has a 'T' with no time components".to_string());
        }
        accumulate(
            time_part,
            &[('H', 3_600_000.0), ('M', 60_000.0), ('S', 1_000.0)],
        )?;
    }
    Ok(std::time::Duration::from_millis(total_ms))
}

/// Parse a lenient boolean (`true`/`false`/`1`/`0`/`yes`/`no`/`on`/`off`, case-insensitive).
fn parse_bool(raw: &str, key: &str) -> Result<bool, ConfigError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(ConfigError(format!(
            "{key}: '{other}' is not a boolean (true/false)"
        ))),
    }
}

/// Resolve one duration-valued key (ISO-8601 or bare seconds), falling back to `default` when
/// unset and failing CLOSED on a malformed value. The shape [`parse_retry`] uses, lifted for
/// the flat key blocks that have no `sutra.<service>.<block>.*` prefix in common.
#[allow(clippy::type_complexity)]
fn duration_key(
    value: &dyn Fn(&str, &str) -> Result<Option<String>, ConfigError>,
    file_key: &str,
    canonical: &str,
    default: std::time::Duration,
) -> Result<std::time::Duration, ConfigError> {
    match value(file_key, canonical)? {
        None => Ok(default),
        Some(raw) => {
            parse_duration(&raw).map_err(|e| ConfigError(format!("{file_key}: '{raw}' — {e}")))
        }
    }
}

/// Parse a `sutra.<service>.retry.*` block into a [`RetryConfig`] (the generalized retry
/// shape). `outbox` is the first consumer (`parse_retry(value, "outbox", "OUTBOX", …)`); any
/// later service reuses this verbatim by passing its own `service`/`env_service` pair. Missing
/// keys fall back to `default`. Fails closed on a non-positive base, a max below the base, or a
/// malformed duration/boolean — so an invalid curve refuses boot rather than panicking at
/// `RetryPolicy::new`.
#[allow(clippy::type_complexity)]
fn parse_retry(
    value: &dyn Fn(&str, &str) -> Result<Option<String>, ConfigError>,
    service: &str,
    env_service: &str,
    default: RetryConfig,
) -> Result<RetryConfig, ConfigError> {
    let base_key = format!("sutra.{service}.retry.base-delay");
    let base_delay = match value(&base_key, &format!("SUTRA_{env_service}_RETRY_BASE_DELAY"))? {
        None => default.base_delay,
        Some(raw) => {
            parse_duration(&raw).map_err(|e| ConfigError(format!("{base_key}: '{raw}' — {e}")))?
        }
    };
    let max_key = format!("sutra.{service}.retry.max-delay");
    let max_delay = match value(&max_key, &format!("SUTRA_{env_service}_RETRY_MAX_DELAY"))? {
        None => default.max_delay,
        Some(raw) => {
            parse_duration(&raw).map_err(|e| ConfigError(format!("{max_key}: '{raw}' — {e}")))?
        }
    };
    let jitter_key = format!("sutra.{service}.retry.jitter");
    let jitter = match value(&jitter_key, &format!("SUTRA_{env_service}_RETRY_JITTER"))? {
        None => default.jitter,
        Some(raw) => parse_bool(&raw, &jitter_key)?,
    };
    let attempts_key = format!("sutra.{service}.retry.max-attempts");
    let max_attempts = match value(
        &attempts_key,
        &format!("SUTRA_{env_service}_RETRY_MAX_ATTEMPTS"),
    )? {
        None => default.max_attempts,
        Some(raw) => match raw.trim().parse::<i32>() {
            Ok(n) if n >= 1 => Some(n),
            _ => {
                return Err(ConfigError(format!(
                    "{attempts_key}: '{raw}' must be an integer >= 1 (omit the key entirely for \
                     the default: retry forever)"
                )));
            }
        },
    };
    if base_delay.is_zero() {
        return Err(ConfigError(format!(
            "{base_key}: base-delay must be greater than zero"
        )));
    }
    if max_delay < base_delay {
        return Err(ConfigError(format!(
            "{max_key}: max-delay ({max_delay:?}) must be >= base-delay ({base_delay:?})"
        )));
    }
    Ok(RetryConfig {
        base_delay,
        max_delay,
        jitter,
        max_attempts,
    })
}

/// Read a properties-style config file (`key=value` lines, `#`/`!` comments). A missing
/// file yields empty values — the file is optional; a present-but-unreadable one fails.
fn read_properties(path: &PathBuf) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut out = BTreeMap::new();
    if !path.is_file() {
        return Ok(out);
    }
    let text = std::fs::read_to_string(path).map_err(|e| {
        ConfigError(format!(
            "failed to read config file {}: {e}",
            path.display()
        ))
    })?;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precedence_canonical_env_over_file() {
        let mut file = BTreeMap::new();
        file.insert(
            "sutra.deployments.dir".to_string(),
            "/from-file".to_string(),
        );
        file.insert("sutra.http.port".to_string(), "1111".to_string());

        // file only
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.deployments_dir, Some(PathBuf::from("/from-file")));
        assert_eq!(c.http_port, 1111);

        // canonical env beats the file
        let c = EngineConfig::from_sources(&file, &|name| match name {
            "SUTRA_DEPLOYMENTS_DIR" => Some("/canonical".into()),
            "SUTRA_HTTP_PORT" => Some("0".into()),
            "SUTRA_DATASOURCE_URL" => Some("postgresql://db/engine".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(c.deployments_dir, Some(PathBuf::from("/canonical")));
        assert_eq!(c.http_port, 0);
        assert_eq!(c.datasource_url.as_deref(), Some("postgresql://db/engine"));

        // unset env falls back to the file
        let c = EngineConfig::from_sources(&file, &|name| match name {
            "SUTRA_DEPLOYMENTS_DIR" => Some("/canonical".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(c.deployments_dir, Some(PathBuf::from("/canonical")));
        assert_eq!(c.http_port, 1111, "unset env falls back to the file");

        // Only the canonical `SUTRA_*` env names are read — any other prefix is ignored,
        // so the file values stand and an unset canonical key stays `None`.
        let c = EngineConfig::from_sources(&file, &|name| match name {
            "APP_HTTP_PORT" => Some("0".into()),
            "APP_DATASOURCE_URL" => Some("postgresql://db/engine".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(c.deployments_dir, Some(PathBuf::from("/from-file")));
        assert_eq!(c.http_port, 1111);
        assert_eq!(c.datasource_url, None);
    }

    #[test]
    fn deferred_ack_knobs_default_and_parse_from_env_or_file() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Documented defaults: capacity 10 000, timeout PT1H, sweep PT1M.
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.deferred_ack, DeferredAckConfig::default());
        assert_eq!(c.deferred_ack.capacity, 10_000);
        assert_eq!(c.deferred_ack.timeout, std::time::Duration::from_secs(3600));
        assert_eq!(
            c.deferred_ack.sweep_interval,
            std::time::Duration::from_secs(60)
        );

        // File values parse (ISO-8601 + bare seconds); a canonical env var beats the file.
        file.insert("sutra.ack.deferred.capacity".to_string(), "500".to_string());
        file.insert("sutra.ack.deferred.timeout".to_string(), "PT2H".to_string());
        file.insert(
            "sutra.ack.deferred.sweep-interval".to_string(),
            "30".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_ACK_DEFERRED_CAPACITY").then(|| "2000".to_string())
        })
        .unwrap();
        assert_eq!(c.deferred_ack.capacity, 2000, "env beats the file");
        assert_eq!(c.deferred_ack.timeout, std::time::Duration::from_secs(7200));
        assert_eq!(
            c.deferred_ack.sweep_interval,
            std::time::Duration::from_secs(30)
        );

        // Invalid values fail closed: zero/garbage capacity, zero sweep interval.
        file.insert("sutra.ack.deferred.capacity".to_string(), "0".to_string());
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
        file.insert(
            "sutra.ack.deferred.capacity".to_string(),
            "many".to_string(),
        );
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
        file.insert("sutra.ack.deferred.capacity".to_string(), "500".to_string());
        file.insert(
            "sutra.ack.deferred.sweep-interval".to_string(),
            "PT0S".to_string(),
        );
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
    }

    #[test]
    fn instance_sweep_knobs_default_and_parse_from_env_or_file() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Documented defaults: sweep PT1M, claim timeout PT5M.
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(
            c.instance_sweep,
            crate::sweeper::StuckInstanceScannerConfig::default()
        );
        assert_eq!(
            c.instance_sweep.interval,
            std::time::Duration::from_secs(60)
        );
        assert_eq!(
            c.instance_sweep.claim_timeout,
            std::time::Duration::from_secs(300)
        );

        // File values parse (ISO-8601 + bare seconds); a canonical env var beats the file.
        file.insert(
            "sutra.instance.sweep-interval".to_string(),
            "PT10S".to_string(),
        );
        file.insert(
            "sutra.instance.claim-timeout".to_string(),
            "120".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_INSTANCE_CLAIM_TIMEOUT").then(|| "PT30M".to_string())
        })
        .unwrap();
        assert_eq!(
            c.instance_sweep.interval,
            std::time::Duration::from_secs(10)
        );
        assert_eq!(
            c.instance_sweep.claim_timeout,
            std::time::Duration::from_secs(1800),
            "env beats the file"
        );

        // Fail closed: a zero interval hot-loops the scanner; a zero timeout (or one inside a
        // single sweep tick) would reclaim instances out from under a live owner.
        file.insert(
            "sutra.instance.sweep-interval".to_string(),
            "PT0S".to_string(),
        );
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
        file.insert(
            "sutra.instance.sweep-interval".to_string(),
            "PT10S".to_string(),
        );
        file.insert(
            "sutra.instance.claim-timeout".to_string(),
            "PT0S".to_string(),
        );
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
        file.insert(
            "sutra.instance.claim-timeout".to_string(),
            "PT10S".to_string(),
        );
        assert!(
            EngineConfig::from_sources(&file, &|_| None).is_err(),
            "claim-timeout must be strictly greater than the sweep interval"
        );
    }

    // Phase-2 note (execution scale-out §8): the Phase-1 refusal of `shards > 1`
    // (`SUTRA.CONFIG.ENGINE.SHARDS_UNSUPPORTED`) was a DESIGNED interim posture — this
    // test's refusal leg is the one planned expectation change of the N-lane landing;
    // everything else in it is unchanged.
    #[test]
    fn engine_shard_knobs_default_parse_and_accept_n_lanes() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Documented defaults: one lane, unbounded queue — byte-identical to the
        // pre-router engine.
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.engine_shards, EngineShardConfig::default());
        assert_eq!(c.engine_shards.shards, 1);
        assert_eq!(c.engine_shards.queue_capacity, None);

        // An explicit 1 and a bounded queue both parse; a canonical env var beats the file.
        file.insert("sutra.engine.shards".to_string(), "1".to_string());
        file.insert(
            "sutra.engine.shard-queue-capacity".to_string(),
            "64".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_ENGINE_SHARD_QUEUE_CAPACITY").then(|| "128".to_string())
        })
        .unwrap();
        assert_eq!(c.engine_shards.shards, 1);
        assert_eq!(
            c.engine_shards.queue_capacity,
            Some(128),
            "env beats the file"
        );

        // Fail closed: zero lanes is meaningless, and a zero capacity is "no queue at
        // all" (UNSET is how an unbounded queue is asked for).
        file.insert("sutra.engine.shards".to_string(), "0".to_string());
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
        file.insert("sutra.engine.shards".to_string(), "1".to_string());
        file.insert(
            "sutra.engine.shard-queue-capacity".to_string(),
            "0".to_string(),
        );
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
        file.insert(
            "sutra.engine.shard-queue-capacity".to_string(),
            "64".to_string(),
        );

        // shards > 1 is ACCEPTED (Phase 2 of the execution scale-out: N lanes are live;
        // the Phase-1 SHARDS_UNSUPPORTED refusal is lifted, per the design's plan).
        file.insert("sutra.engine.shards".to_string(), "4".to_string());
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.engine_shards.shards, 4);
        assert_eq!(
            c.engine_shards.queue_capacity,
            Some(64),
            "the per-lane queue bound rides along unchanged"
        );
    }

    #[test]
    fn instance_retention_defaults_to_a_week_and_accepts_pt0s_as_delete_at_terminal() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Documented defaults: retain P7D, purge hourly.
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(
            c.instance_retention,
            crate::sweeper::TerminalRetentionConfig::default()
        );
        assert_eq!(
            c.instance_retention.retention,
            std::time::Duration::from_secs(604_800)
        );
        assert_eq!(
            c.instance_retention.interval,
            std::time::Duration::from_secs(3600)
        );

        // File values parse (day component included); a canonical env var beats the file.
        file.insert("sutra.instance.retention".to_string(), "P30D".to_string());
        file.insert(
            "sutra.instance.retention-sweep-interval".to_string(),
            "PT15M".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_INSTANCE_RETENTION").then(|| "PT36H".to_string())
        })
        .unwrap();
        assert_eq!(
            c.instance_retention.retention,
            std::time::Duration::from_secs(129_600),
            "env beats the file"
        );
        assert_eq!(
            c.instance_retention.interval,
            std::time::Duration::from_secs(900)
        );

        // PT0S is LEGAL for the retention itself — it is the explicit "keep no history, delete at
        // the terminal step" posture, i.e. the engine's behaviour before P1-2.
        file.insert("sutra.instance.retention".to_string(), "PT0S".to_string());
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.instance_retention.retention, std::time::Duration::ZERO);

        // …but a zero purge cadence is not: it would hot-loop the sweep task.
        file.insert(
            "sutra.instance.retention-sweep-interval".to_string(),
            "PT0S".to_string(),
        );
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
    }

    #[test]
    fn outbox_tick_interval_parses_iso8601_and_defaults_to_pt5s() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.outbox_tick_interval, std::time::Duration::from_secs(5));

        file.insert(
            "sutra.outbox.tick-interval".to_string(),
            "PT0.2S".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(
            c.outbox_tick_interval,
            std::time::Duration::from_millis(200)
        );

        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_OUTBOX_TICK_INTERVAL").then(|| "PT1M30S".to_string())
        })
        .unwrap();
        assert_eq!(c.outbox_tick_interval, std::time::Duration::from_secs(90));

        file.insert("sutra.outbox.tick-interval".to_string(), "7".to_string());
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.outbox_tick_interval, std::time::Duration::from_secs(7));

        file.insert(
            "sutra.outbox.tick-interval".to_string(),
            "five seconds".to_string(),
        );
        assert!(EngineConfig::from_sources(&file, &|_| None).is_err());
    }

    #[test]
    fn outbox_retry_defaults_match_the_shipped_curve_and_parse_overrides() {
        use std::time::Duration;
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Defaults reproduce RetryPolicy::default() (PT1S / PT5M / jitter on).
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.outbox_retry, RetryConfig::default());
        assert_eq!(c.outbox_retry.base_delay, Duration::from_secs(1));
        assert_eq!(c.outbox_retry.max_delay, Duration::from_secs(300));
        assert!(c.outbox_retry.jitter);

        // File overrides (generalized shape).
        file.insert(
            "sutra.outbox.retry.base-delay".to_string(),
            "PT2S".to_string(),
        );
        file.insert(
            "sutra.outbox.retry.max-delay".to_string(),
            "PT30S".to_string(),
        );
        file.insert("sutra.outbox.retry.jitter".to_string(), "false".to_string());
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.outbox_retry.base_delay, Duration::from_secs(2));
        assert_eq!(c.outbox_retry.max_delay, Duration::from_secs(30));
        assert!(!c.outbox_retry.jitter);

        // Canonical env beats the file.
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_OUTBOX_RETRY_BASE_DELAY").then(|| "PT5S".to_string())
        })
        .unwrap();
        assert_eq!(c.outbox_retry.base_delay, Duration::from_secs(5));

        // Fail-closed: max < base.
        let mut bad = BTreeMap::new();
        bad.insert("sutra.deployments.dir".to_string(), "/r".to_string());
        bad.insert(
            "sutra.outbox.retry.base-delay".to_string(),
            "PT10S".to_string(),
        );
        bad.insert(
            "sutra.outbox.retry.max-delay".to_string(),
            "PT1S".to_string(),
        );
        let err = EngineConfig::from_sources(&bad, &|_| None).unwrap_err();
        assert!(err.0.contains("max-delay"), "{}", err.0);

        // Fail-closed: non-boolean jitter.
        bad.insert(
            "sutra.outbox.retry.max-delay".to_string(),
            "PT30S".to_string(),
        );
        bad.insert("sutra.outbox.retry.jitter".to_string(), "maybe".to_string());
        let err = EngineConfig::from_sources(&bad, &|_| None).unwrap_err();
        assert!(err.0.contains("jitter"), "{}", err.0);
    }

    #[test]
    fn outbox_max_attempts_is_absent_by_default_and_parses_a_configured_ceiling() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // ABSENT = retry forever. This is the documented default, not an oversight: a default
        // ceiling would silently convert "the broker was down for an hour" into "the message was
        // dropped", and at-least-once is the outbox's whole contract.
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.outbox_retry.max_attempts, None);

        file.insert(
            "sutra.outbox.retry.max-attempts".to_string(),
            "7".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.outbox_retry.max_attempts, Some(7));

        // The canonical env twin beats the file, like every other key.
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_OUTBOX_RETRY_MAX_ATTEMPTS").then(|| "3".to_string())
        })
        .unwrap();
        assert_eq!(c.outbox_retry.max_attempts, Some(3));

        // Fail-closed on a value that cannot mean anything: "give up before attempting" is never
        // what was intended, and silently coercing it would stop the outbox delivering at all.
        for raw in ["0", "-1", "lots", "3.5"] {
            let mut bad = file.clone();
            bad.insert(
                "sutra.outbox.retry.max-attempts".to_string(),
                raw.to_string(),
            );
            let err = EngineConfig::from_sources(&bad, &|_| None).unwrap_err();
            assert!(
                err.0.contains("max-attempts"),
                "max-attempts='{raw}' must refuse boot: {}",
                err.0
            );
        }
    }

    #[test]
    fn payload_cap_defaults_to_ten_mebibytes_and_parses_overrides() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Default = the shipped 10 MiB global ceiling.
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(
            c.payload_cap_bytes,
            sutra_channels::PayloadCapPolicy::DEFAULT_MAX_PAYLOAD_BYTES
        );
        assert_eq!(c.payload_cap_bytes, 10 * 1024 * 1024);

        // `0` disables the global cap.
        file.insert("sutra.codec.max-payload-bytes".to_string(), "0".to_string());
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.payload_cap_bytes, 0);

        // Canonical env overrides the file.
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_CODEC_MAX_PAYLOAD_BYTES").then(|| "1048576".to_string())
        })
        .unwrap();
        assert_eq!(c.payload_cap_bytes, 1024 * 1024);

        // A non-numeric / negative value is a fail-closed config error.
        file.insert(
            "sutra.codec.max-payload-bytes".to_string(),
            "-1".to_string(),
        );
        let err = EngineConfig::from_sources(&file, &|_| None).unwrap_err();
        assert!(err.0.contains("max-payload-bytes"), "{}", err.0);
    }

    #[test]
    fn audit_jsonl_path_is_off_by_default_and_parses_from_env_or_file() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Off by default (no engine-global audit sink — neither JSONL, OTel, nor SQL).
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.audit.jsonl_path, None);
        assert_eq!(c.audit.otel_endpoint, None);
        assert!(!c.audit.sql);

        // Canonical env activates the JSONL sink.
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_AUDIT_JSONL").then(|| "/var/log/sutra/audit.jsonl".to_string())
        })
        .unwrap();
        assert_eq!(
            c.audit.jsonl_path,
            Some(PathBuf::from("/var/log/sutra/audit.jsonl"))
        );

        // File form.
        file.insert(
            "sutra.audit.jsonl.path".to_string(),
            "/data/audit.jsonl".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.audit.jsonl_path, Some(PathBuf::from("/data/audit.jsonl")));

        // The OTel-log sink activates on a DEDICATED endpoint (`sutra.audit.otel.endpoint`) — its own
        // audit OTLP pipeline, never the engine telemetry stream. `None` when unset/blank.
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_AUDIT_OTEL_ENDPOINT").then(|| "http://audit-otel:4317".to_string())
        })
        .unwrap();
        assert_eq!(
            c.audit.otel_endpoint.as_deref(),
            Some("http://audit-otel:4317")
        );
        // JSONL + OTel compose (both set here).
        assert_eq!(c.audit.jsonl_path, Some(PathBuf::from("/data/audit.jsonl")));

        file.insert(
            "sutra.audit.otel.endpoint".to_string(),
            "http://a:4317".to_string(),
        );
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.audit.otel_endpoint.as_deref(), Some("http://a:4317"));
        // A blank endpoint is treated as unset.
        file.insert("sutra.audit.otel.endpoint".to_string(), "  ".to_string());
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.audit.otel_endpoint, None);

        // The SQL sink is the third composable toggle (`sutra.audit.sql`) — default false,
        // activated by env/file, lenient-boolean parsed. Registration is additionally gated on
        // a datasource pool being present (see the assembly).
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_AUDIT_SQL").then(|| "true".to_string())
        })
        .unwrap();
        assert!(c.audit.sql);
        file.insert("sutra.audit.sql".to_string(), "1".to_string());
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert!(c.audit.sql);
    }

    #[test]
    fn parse_duration_rejects_malformed_forms() {
        assert!(parse_duration("PT").is_err());
        assert!(parse_duration("PT5").is_err());
        assert!(parse_duration("").is_err());
        assert!(parse_duration("P").is_err());
        // A month is not a duration, and a week is not a unit this engine takes.
        assert!(parse_duration("P1M").is_err());
        assert!(parse_duration("P2W").is_err());
        // Units in the wrong half are rejected rather than silently reinterpreted.
        assert!(parse_duration("P5S").is_err());
        assert!(parse_duration("PT1D").is_err());
        assert_eq!(
            parse_duration("pt2h").unwrap(),
            std::time::Duration::from_secs(7200)
        );
    }

    #[test]
    fn parse_duration_accepts_the_day_component_the_retention_window_is_stated_in() {
        assert_eq!(
            parse_duration("P7D").unwrap(),
            std::time::Duration::from_secs(604_800)
        );
        assert_eq!(
            parse_duration("P1DT12H").unwrap(),
            std::time::Duration::from_secs(129_600)
        );
        assert_eq!(parse_duration("PT0S").unwrap(), std::time::Duration::ZERO);
        // Every pre-existing PT… form is untouched.
        assert_eq!(
            parse_duration("PT1M30S").unwrap(),
            std::time::Duration::from_secs(90)
        );
        assert_eq!(
            parse_duration("PT0.2S").unwrap(),
            std::time::Duration::from_millis(200)
        );
    }

    #[test]
    fn telemetry_keys_resolve_through_the_engine_config() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Defaults: telemetry off, fail-open (no error despite no telemetry keys).
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert!(!c.telemetry.is_active());

        // The k8s IT harness env set (canonical SUTRA_TELEMETRY_* + the standard
        // vendor-neutral temporality env) activates export.
        let c = EngineConfig::from_sources(&file, &|name| match name {
            "SUTRA_TELEMETRY_OTLP_ENDPOINT" => Some("http://otel-collector:4317".into()),
            "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE" => Some("delta".into()),
            "SUTRA_TELEMETRY_SERVICE_NAME" => Some("demo-svc".into()),
            _ => None,
        })
        .unwrap();
        assert!(c.telemetry.is_active());
        assert_eq!(
            c.telemetry.otlp_endpoint.as_deref(),
            Some("http://otel-collector:4317")
        );
        assert_eq!(c.telemetry.service_name, "demo-svc");
        assert_eq!(
            c.telemetry.metrics_temporality,
            crate::otel::TemporalityPreference::Delta
        );
        assert_eq!(
            c.telemetry.metrics_wiring(),
            Some(vec![
                "tenant".to_string(),
                "module".to_string(),
                "version".to_string()
            ])
        );
    }

    #[test]
    fn deployments_dir_is_required_and_port_validates() {
        let empty = BTreeMap::new();
        let err = EngineConfig::from_sources(&empty, &|_| None).unwrap_err();
        assert!(
            err.0.contains("sutra.deployments.dir"),
            "the deployments source is required: {}",
            err.0
        );

        let err = EngineConfig::from_sources(&empty, &|name| match name {
            "SUTRA_DEPLOYMENTS_DIR" => Some("/r".into()),
            "SUTRA_HTTP_PORT" => Some("not-a-port".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(err.0.contains("not a valid port"));
    }

    #[test]
    fn deployment_source_db_needs_no_dir_and_parses() {
        // `db` source boots from the deployment_archive store — no deployments_dir required.
        let cfg = EngineConfig::from_sources(&BTreeMap::new(), &|name| match name {
            "SUTRA_DEPLOYMENT_SOURCE" => Some("db".into()),
            _ => None,
        })
        .expect("db source needs no deployments_dir");
        assert_eq!(cfg.deployment_source, DeploymentSourceKind::Db);
        assert!(cfg.deployments_dir.is_none());

        // The default stays `dir` (and still requires a dir).
        let cfg = EngineConfig::from_sources(&BTreeMap::new(), &|name| match name {
            "SUTRA_DEPLOYMENTS_DIR" => Some("/r".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.deployment_source, DeploymentSourceKind::Dir);

        // A bogus source is rejected.
        let err = EngineConfig::from_sources(&BTreeMap::new(), &|name| match name {
            "SUTRA_DEPLOYMENT_SOURCE" => Some("s3".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(err.0.contains("sutra.deployment.source"), "{}", err.0);
    }

    #[test]
    fn crypto_envelope_and_incident_keys_parse_and_fail_closed() {
        let dir = || {
            let mut f = BTreeMap::new();
            f.insert("sutra.deployments.dir".to_string(), "/r".to_string());
            f
        };

        // Default: envelope disabled, incident_sql off (opt-in like audit.sql).
        let c = EngineConfig::from_sources(&dir(), &|_| None).unwrap();
        assert!(!c.crypto_envelope.enabled);
        assert!(c.crypto_envelope.kek_ref.is_none());
        assert!(!c.incident_sql);

        // Enabled + a KEK ref parses; the kek is captured UNRESOLVED for build-time whole-value
        // resolution (not scheme-resolved at load, like admin.auth.key-ref).
        let mut f = dir();
        f.insert(
            "sutra.crypto.envelope.enabled".to_string(),
            "true".to_string(),
        );
        f.insert(
            "sutra.crypto.envelope.kek".to_string(),
            "secret:kek".to_string(),
        );
        f.insert("sutra.incident.sql".to_string(), "true".to_string());
        let c = EngineConfig::from_sources(&f, &|_| None).unwrap();
        assert!(c.crypto_envelope.enabled);
        assert_eq!(c.crypto_envelope.kek_ref.as_deref(), Some("secret:kek"));
        assert!(c.incident_sql);

        // Fail-closed: envelope enabled without a KEK ref is refused.
        let mut f = dir();
        f.insert(
            "sutra.crypto.envelope.enabled".to_string(),
            "true".to_string(),
        );
        let err = EngineConfig::from_sources(&f, &|_| None).unwrap_err();
        assert!(err.0.contains("sutra.crypto.envelope.kek"), "{}", err.0);

        // Fail-closed: envelope + master-key are mutually exclusive (ambiguous key source).
        let mut f = dir();
        f.insert(
            "sutra.crypto.envelope.enabled".to_string(),
            "true".to_string(),
        );
        f.insert(
            "sutra.crypto.envelope.kek".to_string(),
            "secret:kek".to_string(),
        );
        f.insert("sutra.crypto.master-key".to_string(), "s3cr3t".to_string());
        let err = EngineConfig::from_sources(&f, &|_| None).unwrap_err();
        assert!(err.0.contains("mutually exclusive"), "{}", err.0);
    }

    #[test]
    fn admin_oidc_defaults_are_fail_closed_and_keys_parse() {
        let mut file = BTreeMap::new();
        file.insert("sutra.deployments.dir".to_string(), "/r".to_string());

        // Defaults: no OIDC params, NOT dev-disabled → an UNCONFIGURED (503, closed) surface.
        let c = EngineConfig::from_sources(&file, &|_| None).unwrap();
        assert_eq!(c.admin_auth, AdminAuthConfig::default());
        assert!(c.admin_auth.issuer.is_none());
        assert!(c.admin_auth.audience.is_none());
        assert!(c.admin_auth.jwks.is_none());
        assert_eq!(c.admin_auth.role_claim, "roles");
        assert_eq!(c.admin_auth.required_role, "sutra-admin");
        assert!(
            !c.admin_auth.dev_disabled,
            "gating is never dev-disabled by default"
        );

        // Canonical env configures the OIDC gating (issuer/audience/jwks + claim wiring).
        let c = EngineConfig::from_sources(&file, &|name| match name {
            "SUTRA_ADMIN_OIDC_ISSUER" => Some("https://idp.example.com/".into()),
            "SUTRA_ADMIN_OIDC_AUDIENCE" => Some("sutra-admin".into()),
            "SUTRA_ADMIN_OIDC_JWKS" => Some("https://idp.example.com/jwks.json".into()),
            "SUTRA_ADMIN_OIDC_ROLE_CLAIM" => Some("scope".into()),
            "SUTRA_ADMIN_OIDC_REQUIRED_ROLE" => Some("admin".into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            c.admin_auth.issuer.as_deref(),
            Some("https://idp.example.com/")
        );
        assert_eq!(c.admin_auth.audience.as_deref(), Some("sutra-admin"));
        assert_eq!(
            c.admin_auth.jwks.as_deref(),
            Some("https://idp.example.com/jwks.json")
        );
        assert_eq!(c.admin_auth.role_claim, "scope");
        assert_eq!(c.admin_auth.required_role, "admin");

        // The dev escape hatch is an explicit, lenient-boolean opt-in.
        let c = EngineConfig::from_sources(&file, &|name| {
            (name == "SUTRA_ADMIN_OIDC_DEV_DISABLED").then(|| "true".to_string())
        })
        .unwrap();
        assert!(c.admin_auth.dev_disabled);
        file.insert(
            "sutra.admin.oidc.dev-disabled".to_string(),
            "nope".to_string(),
        );
        let err = EngineConfig::from_sources(&file, &|_| None).unwrap_err();
        assert!(err.0.contains("dev-disabled"), "{}", err.0);
    }
}
