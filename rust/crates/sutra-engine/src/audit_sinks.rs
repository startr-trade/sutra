//! Engine-side audit sinks (B1) — the [`sutra_channels::AuditSink`] implementations that
//! depend on the engine's own facades, kept out of the dependency-free `sutra-channels`
//! seam (exactly like the outbox `MessageSink` trait there vs. its engine-side impls).
//!
//! # OTel-log sink (dedicated pipeline)
//!
//! [`OtelAuditSink`] exports each [`sutra_channels::AuditEvent`] as an OTLP **log record** on the
//! `sutra.audit` instrumentation scope through a DEDICATED logs pipeline pointed at
//! `SUTRA_AUDIT_OTEL_ENDPOINT` — a separate audit observability stack, NEVER the engine's telemetry
//! stream. The `sutra.audit` scope plus the audit-context attributes (deployment / instance / seq /
//! event) are the "baggage" a collector routes on, so audit is precisely cullable and never buried
//! in the telemetry firehose.
//!
//! The dedicated `SdkLoggerProvider` is built ONCE (module-static, mirroring the engine telemetry's
//! own `ACTIVE_TELEMETRY`) via [`init_audit_otel`], and force-flushed on shutdown via [`flush_audit`]
//! so buffered audit records leave before the process does. Best-effort: emit is infallible.

use std::sync::OnceLock;

use opentelemetry::logs::{LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::Resource;

/// The dedicated audit OTLP logs pipeline — built ONCE from `SUTRA_AUDIT_OTEL_ENDPOINT`, reused
/// across activation flips (the endpoint is engine-static). `Some(None)` = init was attempted but
/// the exporter was unavailable.
static AUDIT_LOGGER: OnceLock<Option<SdkLoggerProvider>> = OnceLock::new();

/// Initialise (once) the dedicated audit OTLP logs pipeline to `endpoint`. Idempotent — the first
/// call wins (the endpoint is engine config, constant for the process). Returns whether a live
/// provider is available, so the assembly registers the `otel` sink only when export can happen.
pub fn init_audit_otel(endpoint: &str) -> bool {
    AUDIT_LOGGER
        .get_or_init(|| build_audit_provider(endpoint))
        .is_some()
}

fn build_audit_provider(endpoint: &str) -> Option<SdkLoggerProvider> {
    let resource = Resource::builder().with_service_name("sutra-audit").build();
    match opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
    {
        Ok(exporter) => Some(
            SdkLoggerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                .build(),
        ),
        Err(e) => {
            tracing::warn!("dedicated audit OTLP log exporter unavailable (OTel audit off): {e}");
            None
        }
    }
}

/// Force-flush the dedicated audit logs pipeline — the shutdown drain, so buffered audit records
/// export before the process exits. No-op when the OTel audit sink was never initialised.
pub fn flush_audit() {
    if let Some(Some(provider)) = AUDIT_LOGGER.get() {
        let _ = provider.force_flush();
    }
}

/// The OTel-log audit sink — exports one OTLP log record per [`sutra_channels::AuditEvent`] on the
/// `sutra.audit` scope through the dedicated pipeline. Registered by the assembly when
/// `sutra.audit.otel.endpoint` is set AND [`init_audit_otel`] reported a live provider.
pub struct OtelAuditSink;

impl sutra_channels::AuditSink for OtelAuditSink {
    fn name(&self) -> &str {
        "otel"
    }

    fn emit<'a>(
        &'a self,
        e: &'a sutra_channels::AuditEvent,
    ) -> sutra_channels::BoxFuture<'a, Result<(), sutra_channels::Diagnostic>> {
        Box::pin(async move {
            if let Some(Some(provider)) = AUDIT_LOGGER.get() {
                emit_audit_log(provider, e);
            }
            Ok(())
        })
    }
}

/// Emit one audit event as an OTLP log record on the `sutra.audit` scope, with the audit context as
/// attributes (the cull-out "baggage"). A free function so it is testable against an in-memory
/// provider — no module static, no live collector.
fn emit_audit_log(provider: &SdkLoggerProvider, e: &sutra_channels::AuditEvent) {
    let logger = provider.logger("sutra.audit");
    let mut record = logger.create_log_record();
    record.set_severity_number(Severity::Info);
    record.set_severity_text("INFO");
    record.set_body("audit event".into());
    record.add_attribute("deployment_id", e.deployment_id.clone());
    record.add_attribute("tenant", e.tenant.clone());
    record.add_attribute("instance_id", e.instance_id.clone());
    record.add_attribute("seq", e.seq as i64);
    record.add_attribute("event_type", e.event_type.clone());
    record.add_attribute(
        "node_id",
        e.node_id.clone().unwrap_or_else(|| "-".to_string()),
    );
    logger.emit(record);
}

// ---------------------------------------------------------------------------------------------
// SQL sink — the durable audit of record (the `<q:audit>` default sink, `"sql"`).
// ---------------------------------------------------------------------------------------------

/// The SQL audit sink — persists each [`sutra_channels::AuditEvent`] into the shipped
/// `audit_event` table via [`sutra_persistence::stores::PgAuditEventStore`]. Registered by the
/// assembly when `sutra.audit.sql` is set AND a datasource pool is configured (pg-only, like the
/// engine's other system stores). The write is deployment-scoped (RLS) and idempotent per
/// `(deployment_id, instance_id, seq)` — a mid-flight-restart / concurrent-replica re-emit is a
/// no-op, which is why the per-instance seq must stay monotonic across suspend/resume (see
/// [`sutra_channels::AuditListener::seed`]). Best-effort: a write error is a [`Diagnostic`] the
/// dispatcher logs, never an execution failure.
pub struct SqlAuditSink {
    store: sutra_persistence::stores::PgAuditEventStore,
}

impl SqlAuditSink {
    /// Wrap a datasource pool as the audit store.
    pub fn new(pool: sqlx::PgPool) -> SqlAuditSink {
        SqlAuditSink {
            store: sutra_persistence::stores::PgAuditEventStore::new(pool),
        }
    }

    /// Map a channel [`AuditEvent`](sutra_channels::AuditEvent) onto a persistence row: parse the
    /// instance id to a UUID (`None` when it is not one — still recorded, best-effort), the
    /// RFC 3339 `at` to an `OffsetDateTime` (falling back to now on a malformed value), and
    /// saturate the `u32` seq into the table's `INTEGER`. The deployment id parse is the only
    /// hard failure — a non-`dep-…` id cannot be RLS-scoped, so it is a logged `Err`.
    fn to_row(
        e: &sutra_channels::AuditEvent,
    ) -> Result<sutra_persistence::stores::AuditEventRow, sutra_channels::Diagnostic> {
        let deployment =
            sutra_persistence::DeploymentId::new(e.deployment_id.clone()).map_err(|err| {
                sutra_channels::Diagnostic::error(
                    sutra_channels::AUDIT_SINK_WRITE_FAILED,
                    format!(
                        "audit event has an unusable deployment id '{}': {err}",
                        e.deployment_id
                    ),
                )
            })?;
        let at = time::OffsetDateTime::parse(&e.at, &time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| time::OffsetDateTime::now_utc());
        Ok(sutra_persistence::stores::AuditEventRow {
            deployment,
            instance_id: uuid::Uuid::parse_str(&e.instance_id).ok(),
            seq: e.seq.min(i32::MAX as u32) as i32,
            at,
            event_type: e.event_type.clone(),
            node_id: e.node_id.clone(),
            diagnostic_code: e.diagnostic_code.clone(),
            diagnostic_json: e.diagnostic_json.clone(),
            payload_json: e.payload_json.clone(),
        })
    }
}

impl sutra_channels::AuditSink for SqlAuditSink {
    fn name(&self) -> &str {
        "sql"
    }

    fn emit<'a>(
        &'a self,
        e: &'a sutra_channels::AuditEvent,
    ) -> sutra_channels::BoxFuture<'a, Result<(), sutra_channels::Diagnostic>> {
        Box::pin(async move {
            let row = Self::to_row(e)?;
            // `false` = the (deployment, instance, seq) row already exists — an idempotent
            // replay, not an error. A DB error IS surfaced (logged best-effort by the dispatcher).
            self.store
                .insert(&row)
                .await
                .map(|_inserted| ())
                .map_err(|err| {
                    sutra_channels::Diagnostic::error(
                        sutra_channels::AUDIT_SINK_WRITE_FAILED,
                        format!("audit_event insert failed: {err}"),
                    )
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::{AuditEvent, AuditSink};

    fn sample() -> AuditEvent {
        AuditEvent {
            sink: "otel".into(),
            deployment_id: "dep-000000000000000000000001".into(),
            tenant: "acme".into(),
            instance_id: "11111111-1111-1111-1111-111111111111".into(),
            seq: 3,
            at: "2026-07-19T12:00:00Z".into(),
            event_type: "NODE_ENTERED".into(),
            node_id: Some("Start".into()),
            diagnostic_code: None,
            diagnostic_json: None,
            payload_json: "{}".into(),
        }
    }

    /// The sink exports exactly one OTLP log record on the `sutra.audit` scope, carrying the audit
    /// context as attributes — proven against an in-memory logs exporter (the dedicated-pipeline
    /// wiring to a real endpoint is a tier-3/collector concern). This is what a downstream collector
    /// culls audit on: scope `sutra.audit` + the deployment/instance/seq/event attributes.
    #[test]
    fn otel_sink_exports_one_sutra_audit_log_record_with_the_audit_context() {
        use opentelemetry_sdk::logs::in_memory_exporter::InMemoryLogExporter;

        let exporter = InMemoryLogExporter::default();
        let provider = SdkLoggerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();

        emit_audit_log(&provider, &sample());
        let _ = provider.force_flush();

        let logs = exporter.get_emitted_logs().expect("emitted logs");
        assert_eq!(logs.len(), 1, "exactly one sutra.audit log record");
        let emitted = &logs[0];
        assert_eq!(
            emitted.instrumentation.name(),
            "sutra.audit",
            "scope is the cull-out selector"
        );
        let attrs: std::collections::HashMap<String, String> = emitted
            .record
            .attributes_iter()
            .map(|(k, v)| (k.to_string(), format!("{v:?}")))
            .collect();
        let has = |k: &str, needle: &str| {
            attrs
                .get(k)
                .unwrap_or_else(|| panic!("attribute {k} present; got {attrs:?}"))
                .contains(needle)
        };
        assert!(has("event_type", "NODE_ENTERED"));
        assert!(has("instance_id", "11111111-1111-1111-1111-111111111111"));
        assert!(has("seq", "3"));
        assert!(has("deployment_id", "dep-000000000000000000000001"));
        assert!(has("node_id", "Start"));
    }

    #[test]
    fn sink_name_is_otel() {
        assert_eq!(OtelAuditSink.name(), "otel");
    }
}
