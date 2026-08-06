//! Boot-time RLS posture checks — the role's bypass ATTRIBUTES, and whether the policies
//! actually bind for that role.
//!
//! The persistence contract pins a two-layer isolation model: every store operation binds
//! the deployment id EXPLICITLY *and* row-level-security policies retain it. If the
//! engine's PostgreSQL role can bypass RLS — a superuser, or any role minted `BYPASSRLS` —
//! the second layer silently evaporates: a bug in the explicit bind would leak
//! cross-deployment rows with nothing to catch it. This check reads `pg_roles` for the
//! connecting role at boot and, by default, REFUSES startup (fail-closed) when the role is
//! risky. The refusal downgrades to a WARNING
//! when the operator explicitly acknowledges the risk
//! (`sutra.persistence.rls-bypass-check.enabled=false`) — dev/test only.
//!
//! `NOBYPASSRLS` is necessary but NOT sufficient. PostgreSQL also exempts a table's OWNER
//! from that table's own policies unless the table carries `FORCE ROW LEVEL SECURITY`
//! (`check_enable_rls()` in `rls.c`: the exemption is granted when the caller has the
//! PRIVILEGES OF the owning role, so inherited membership counts too — hence the
//! `pg_has_role(current_user, relowner, 'USAGE')` probe rather than a plain oid compare).
//! The engine migrations CREATE these tables, so in the shipped single-role posture the
//! engine role owns them and every policy is inert: `relrowsecurity` is on, the policies are
//! listed, and not one of them is ever evaluated. The migrations' own comments
//! (`V403__instance_rls.sql`, `V702__channel_instance_rls.sql`) name the production
//! hardening — a dedicated non-owning `NOBYPASSRLS` application role plus `FORCE ROW LEVEL
//! SECURITY` per table — as operator responsibility. [`check_rls_enforcement_posture`] makes
//! that gap LOUD instead of silent: a WARNING naming the inert tables and the remediation.
//! It is deliberately NON-FATAL — the shipped dev/test posture (one owning role, no FORCE)
//! must keep booting, and the explicit `deployment_id` bind on every statement is still doing
//! the isolation work. Only the bypass-attribute check refuses boot.
//!
//! Dialect note: `pg_roles`, `pg_class.relforcerowsecurity` and `BYPASSRLS` are PostgreSQL
//! concepts, and the engine-internal datasource is PostgreSQL (`PgPool`), so the checks always
//! run here. A future non-PostgreSQL engine store would soft-skip (RLS is a PG feature) — the
//! posture mirrors the persistence dialect suites' "enforced-bind-only" posture for
//! MySQL/MSSQL. A role absent from `pg_roles`, a catalog query error, or a database with no
//! RLS-bearing tables yet (pre-migration) also soft-skips rather than refusing boot — or
//! shouting — on an uninterpretable result.

use sqlx::{PgPool, Row};

/// Refuses startup when the engine role can bypass RLS (ERROR severity).
pub const STARTUP_RLS_BYPASS_RISK: &str = "SUTRA.STARTUP.RLS_BYPASS_RISK";
/// The acknowledged (opt-out) posture — logged, startup continues (WARNING severity).
pub const STARTUP_RLS_BYPASS_ACKNOWLEDGED: &str = "SUTRA.STARTUP.RLS_BYPASS_ACKNOWLEDGED";
/// The policies are declared but inert for this role: it owns the RLS-bearing tables and they
/// carry no `FORCE ROW LEVEL SECURITY` (WARNING severity — logged, startup continues).
pub const STARTUP_RLS_INERT_POSTURE: &str = "SUTRA.STARTUP.RLS_INERT_POSTURE";

/// How many inert table names the diagnostic message spells out before eliding the rest.
const NAMED_TABLE_LIMIT: usize = 12;

/// Diagnostic severity — an ERROR refuses boot, a WARNING is logged and boot continues.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Warning,
    Error,
}

/// One posture diagnostic — code, severity, the role under examination and its
/// `rolbypassrls` / `rolsuper` attributes, plus (for
/// [`STARTUP_RLS_INERT_POSTURE`]) the tables whose policies never bind for it.
#[derive(Debug, Clone)]
pub struct RlsPostureDiagnostic {
    pub code: &'static str,
    pub severity: Severity,
    pub message: String,
    pub role: String,
    pub bypass_rls: bool,
    pub super_user: bool,
    /// Schema-qualified tables with RLS enabled, owned by this role, and NOT forced — empty
    /// for the bypass-attribute diagnostics.
    pub inert_tables: Vec<String>,
}

/// The result of the posture check — empty when the role is safe or the check soft-skips.
#[derive(Debug, Clone, Default)]
pub struct RlsPostureReport {
    pub diagnostics: Vec<RlsPostureDiagnostic>,
}

impl RlsPostureReport {
    /// True when any diagnostic is ERROR severity (startup must refuse).
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity == Severity::Error)
    }
}

/// Run the posture check against the engine's PostgreSQL pool. `enabled` (default `true`)
/// governs whether a risky role is an ERROR (refuse) or a WARNING (acknowledged opt-out).
/// A safe role (NOBYPASSRLS, not superuser), a role absent from `pg_roles`, or a query error
/// all return an empty report (clean / soft-skip).
pub async fn check_rls_bypass_posture(pool: &PgPool, enabled: bool) -> RlsPostureReport {
    let row = match sqlx::query(
        "SELECT rolname, rolbypassrls, rolsuper FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_optional(pool)
    .await
    {
        Ok(Some(row)) => row,
        // current_user not in pg_roles (e.g. SET ROLE to a role removed mid-session) — soft skip.
        Ok(None) => return RlsPostureReport::default(),
        Err(e) => {
            // The pg_roles catalog is PG-specific; a non-PG backend errors here. Soft-skip
            // rather than refuse boot on a query we cannot interpret.
            tracing::warn!(error = %e, "RLS-bypass posture query failed — check soft-skipped");
            return RlsPostureReport::default();
        }
    };
    let role: String = row.get("rolname");
    let bypass_rls: bool = row.get("rolbypassrls");
    let super_user: bool = row.get("rolsuper");
    if !(bypass_rls || super_user) {
        return RlsPostureReport::default(); // NOBYPASSRLS, not superuser — the safe posture
    }

    let diagnostic = if enabled {
        RlsPostureDiagnostic {
            code: STARTUP_RLS_BYPASS_RISK,
            severity: Severity::Error,
            message: format!(
                "the engine PostgreSQL role '{role}' can bypass row-level security \
                 (rolbypassrls={bypass_rls}, rolsuper={super_user}); the RLS policies on the \
                 engine tables are silently ineffective. Use a dedicated NOBYPASSRLS, \
                 non-superuser role, or acknowledge the risk with \
                 sutra.persistence.rls-bypass-check.enabled=false (downgrades to a WARNING)"
            ),
            role,
            bypass_rls,
            super_user,
            inert_tables: Vec::new(),
        }
    } else {
        RlsPostureDiagnostic {
            code: STARTUP_RLS_BYPASS_ACKNOWLEDGED,
            severity: Severity::Warning,
            message: format!(
                "the engine PostgreSQL role '{role}' can bypass row-level security \
                 (rolbypassrls={bypass_rls}, rolsuper={super_user}); the RLS-bypass check is \
                 disabled, so startup continues. Production should use a NOBYPASSRLS role"
            ),
            role,
            bypass_rls,
            super_user,
            inert_tables: Vec::new(),
        }
    };
    RlsPostureReport {
        diagnostics: vec![diagnostic],
    }
}

/// One RLS-bearing table as the catalog describes it, relative to the CONNECTED role. Only
/// tables with `relrowsecurity` set are ever built into this shape — a table without RLS
/// enabled is not part of the isolation contract and says nothing about the posture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsTablePosture {
    /// Schema-qualified name (`public.instance_state`).
    pub qualified_name: String,
    /// `pg_class.relforcerowsecurity` — when true, the owner is subject to the policies too.
    pub force_row_security: bool,
    /// True when the connected role has the privileges of the table's owning role, which is
    /// exactly the condition under which PostgreSQL waives the policies (absent FORCE).
    pub owned_by_connected_role: bool,
}

/// The pure classification behind [`STARTUP_RLS_INERT_POSTURE`]: an RLS-enabled table is inert
/// for the connected role when that role owns it AND the table is not forced. Owned + forced
/// enforces; not-owned enforces regardless of FORCE. Returns the qualified names in input
/// order, so the caller's `ORDER BY` decides the message ordering. Kept free of any database
/// handle so the truth table is unit-testable without a container.
pub fn classify_inert_rls_tables(tables: &[RlsTablePosture]) -> Vec<String> {
    tables
        .iter()
        .filter(|t| t.owned_by_connected_role && !t.force_row_security)
        .map(|t| t.qualified_name.clone())
        .collect()
}

/// Renders the inert table list for the diagnostic message: the first
/// [`NAMED_TABLE_LIMIT`] names, then `+N more`.
fn render_table_list(tables: &[String]) -> String {
    let named = tables
        .iter()
        .take(NAMED_TABLE_LIMIT)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    match tables.len().checked_sub(NAMED_TABLE_LIMIT) {
        Some(rest) if rest > 0 => format!("{named}, +{rest} more"),
        _ => named,
    }
}

/// Reads the RLS-bearing tables on the search path and reports the ones whose policies are
/// INERT for the connected role (owner without FORCE). WARNING severity only — the shipped
/// single-role posture is a supported dev/test posture, and the explicit `deployment_id` bind
/// still isolates; this exists so the gap is visible in logs instead of silently assumed away.
/// A catalog error, or a database with no RLS-bearing tables, returns an empty report.
pub async fn check_rls_enforcement_posture(pool: &PgPool) -> RlsPostureReport {
    // `pg_has_role(..., 'USAGE')` mirrors PostgreSQL's own owner-exemption test
    // (`has_privs_of_role`), so inherited membership in the owning role counts as ownership —
    // a plain `relowner = current_user::regrole` compare would under-report.
    let rows = match sqlx::query(
        "SELECT n.nspname || '.' || c.relname AS qualified_name, \
                c.relforcerowsecurity AS force_row_security, \
                pg_catalog.pg_has_role(current_user, c.relowner, 'USAGE') \
                    AS owned_by_connected_role, \
                current_user AS connected_role \
           FROM pg_catalog.pg_class c \
           JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
          WHERE c.relkind IN ('r', 'p') AND c.relrowsecurity \
            AND n.nspname = ANY (current_schemas(false)) \
          ORDER BY 1",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            // PG-specific catalog; a non-PG backend or a permission-starved role errors here.
            tracing::warn!(error = %e, "RLS enforcement posture query failed — check soft-skipped");
            return RlsPostureReport::default();
        }
    };
    let Some(first) = rows.first() else {
        return RlsPostureReport::default(); // no RLS-bearing tables (pre-migration) — nothing to say
    };
    let role: String = first.get("connected_role");
    let tables: Vec<RlsTablePosture> = rows
        .iter()
        .map(|row| RlsTablePosture {
            qualified_name: row.get("qualified_name"),
            force_row_security: row.get("force_row_security"),
            owned_by_connected_role: row.get("owned_by_connected_role"),
        })
        .collect();

    let inert_tables = classify_inert_rls_tables(&tables);
    if inert_tables.is_empty() {
        return RlsPostureReport::default(); // every RLS table binds for this role
    }
    let count = inert_tables.len();
    let listed = render_table_list(&inert_tables);
    RlsPostureReport {
        diagnostics: vec![RlsPostureDiagnostic {
            code: STARTUP_RLS_INERT_POSTURE,
            severity: Severity::Warning,
            message: format!(
                "row-level security is ENABLED but INERT for the engine PostgreSQL role \
                 '{role}' on {count} table(s) [{listed}]: the role owns them and they carry no \
                 FORCE ROW LEVEL SECURITY, so PostgreSQL exempts it from their policies and \
                 the deployment_id policies are never evaluated. Deployment isolation rests on \
                 the explicit deployment_id bind alone. To harden: connect the engine as a \
                 dedicated NON-OWNING, NOBYPASSRLS application role granted only \
                 SELECT/INSERT/UPDATE/DELETE on these tables, and/or issue ALTER TABLE \
                 <table> FORCE ROW LEVEL SECURITY for each"
            ),
            role,
            bypass_rls: false,
            super_user: false,
            inert_tables,
        }],
    }
}

/// Boot wiring: run both checks, log every diagnostic at its severity, and REFUSE startup
/// (`Err`) when any diagnostic is an ERROR. The bypass-attribute gate runs first and is
/// fail-closed; the enforcement-posture check runs after it, and only when the bypass gate
/// did not already refuse (under a bypassing role the ownership question is moot — RLS is
/// off for that role either way). A clean or soft-skipped result returns `Ok`.
pub async fn enforce_rls_bypass_posture(pool: &PgPool, enabled: bool) -> Result<(), String> {
    let report = check_rls_bypass_posture(pool, enabled).await;
    log_diagnostics(&report);
    if let Some(err) = report
        .diagnostics
        .iter()
        .find(|d| d.severity == Severity::Error)
    {
        return Err(format!("[{}] {}", err.code, err.message));
    }
    log_diagnostics(&check_rls_enforcement_posture(pool).await);
    Ok(())
}

/// Logs each diagnostic at its own severity, carrying the stable code and role as fields.
fn log_diagnostics(report: &RlsPostureReport) {
    for d in &report.diagnostics {
        match d.severity {
            Severity::Error => {
                tracing::error!(code = d.code, role = %d.role, "{}", d.message)
            }
            Severity::Warning => {
                tracing::warn!(code = d.code, role = %d.role, "{}", d.message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, force: bool, owned: bool) -> RlsTablePosture {
        RlsTablePosture {
            qualified_name: name.to_owned(),
            force_row_security: force,
            owned_by_connected_role: owned,
        }
    }

    #[test]
    fn owner_without_force_is_the_inert_posture() {
        let tables = [
            table("public.instance_state", false, true),
            table("public.outbox_entry", false, true),
        ];
        assert_eq!(
            classify_inert_rls_tables(&tables),
            vec![
                "public.instance_state".to_owned(),
                "public.outbox_entry".to_owned()
            ]
        );
    }

    #[test]
    fn force_or_non_ownership_each_make_the_policies_bind() {
        let tables = [
            // Owner, but FORCEd — the owner is subject to its own policies.
            table("public.instance_state", true, true),
            // Not the owner — policies apply with or without FORCE.
            table("public.outbox_entry", false, false),
            table("public.waiting_event", true, false),
        ];
        assert!(classify_inert_rls_tables(&tables).is_empty());
    }

    #[test]
    fn classification_is_per_table_not_all_or_nothing() {
        let tables = [
            table("public.instance_state", true, true),
            table("public.outbox_entry", false, true),
            table("public.audit_event", false, false),
        ];
        assert_eq!(
            classify_inert_rls_tables(&tables),
            vec!["public.outbox_entry".to_owned()]
        );
    }

    #[test]
    fn no_rls_bearing_tables_classifies_clean() {
        assert!(classify_inert_rls_tables(&[]).is_empty());
    }

    #[test]
    fn long_table_lists_are_elided_in_the_message() {
        let many: Vec<String> = (0..NAMED_TABLE_LIMIT + 3)
            .map(|i| format!("public.t{i}"))
            .collect();
        let rendered = render_table_list(&many);
        assert!(rendered.starts_with("public.t0, public.t1,"));
        assert!(rendered.ends_with("+3 more"), "rendered: {rendered}");
        assert!(!rendered.contains("public.t12,"));

        let few = vec!["public.a".to_owned(), "public.b".to_owned()];
        assert_eq!(render_table_list(&few), "public.a, public.b");
    }
}
