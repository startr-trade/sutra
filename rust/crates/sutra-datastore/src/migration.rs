//! Module-resident migrations — versioning, placeholder substitution, checksums and the
//! per-database migration LEDGER every SQL dialect records applied scripts in.
//!
//! Pure (strings and hashing, no connection), so it rides the `--no-default-features` build the
//! deploy-time lint uses; the three dialect modules execute what it prepares.
//!
//! # Versions and order
//!
//! A script named `V<n>__<description>.sql` has version `<n>` with leading zeros stripped, so
//! `V001__init.sql` and `V1__init.sql` are the same version — and two scripts of one store with
//! the same version are refused. Any other file name keeps working: its version is the file stem.
//! When EVERY script of a store is `V<n>__`-named, scripts run in numeric version order
//! (`V2` before `V10`); otherwise they run in plain file-name order, exactly as before versioned
//! names existed.
//!
//! # Placeholders
//!
//! Before a script runs (and before its checksum is taken) five placeholders are substituted:
//! `${package.tenant}`, `${package.module}`, `${package.version}`, `${store.name}` and
//! `${store.table}`. Any other `${…}` is refused, naming the file, line and placeholder; so is a
//! `${` with no closing `}`. `$${` writes a literal `${`. A lone `$` or `$$` (PostgreSQL
//! dollar quoting) is left untouched.
//!
//! # The ledger
//!
//! Each database a store migrates holds one history table, [`LEDGER_TABLE`], keyed on
//! `(store_name, version)`. Under the cross-replica migration lock, a script whose version is
//! recorded with the same checksum is skipped; one recorded with a DIFFERENT checksum refuses the
//! store ([`codes::MIGRATION_CHECKSUM`]) — an applied script must never be edited, a change is a
//! new `V<n+1>__` script. An unrecorded script runs and is recorded (in the same transaction on
//! dialects with transactional DDL). Rows for scripts no longer shipped are ignored.
//!
//! Adoption needs no special case: a database migrated before the ledger existed simply has no
//! rows for the store, so its (idempotent) scripts run once more and are recorded.

use std::collections::BTreeMap;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::config::StoreDefinition;
use crate::error::DataStoreError;
use crate::projected::SqlDialect;

/// The stable diagnostic codes a migration failure carries.
pub mod codes {
    /// A migration script uses a `${…}` placeholder that is not one of the five supported
    /// ones, or leaves a `${` unterminated. Raised by `sutra lint` and when the engine loads the
    /// package.
    pub const MIGRATION_PLACEHOLDER: &str = "SUTRA.CONFIG.DATASTORE.MIGRATION_PLACEHOLDER";
    /// Two migration scripts of one store resolve to the same version (`V1__a.sql` and
    /// `V001__b.sql`).
    pub const MIGRATION_VERSION_DUPLICATE: &str =
        "SUTRA.CONFIG.DATASTORE.MIGRATION_VERSION_DUPLICATE";
    /// A script already recorded in the migration ledger has changed since it was applied. The
    /// store refuses first use; the fix is a new, higher-versioned script.
    pub const MIGRATION_CHECKSUM: &str = "SUTRA.RUNTIME.DATASTORE.MIGRATION_CHECKSUM";
}

/// The per-database migration history table.
pub const LEDGER_TABLE: &str = "sutra_store_migrations";

/// The table a key/value store (one without a declared `structure`) writes to.
pub const KEY_VALUE_TABLE: &str = "data_store";

/// The values the five migration placeholders resolve to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationPlaceholders {
    /// `${package.tenant}` — the package's `tenant` label.
    pub tenant: String,
    /// `${package.module}` — the package's `module` label.
    pub module: String,
    /// `${package.version}` — the package's `version` label.
    pub version: String,
    /// `${store.name}` — the store's declared name.
    pub store_name: String,
    /// `${store.table}` — the table the store writes to: the projected table of a typed store,
    /// [`KEY_VALUE_TABLE`] for a key/value store.
    pub store_table: String,
}

impl MigrationPlaceholders {
    /// The placeholders for `def` inside the package labelled `tenant`/`module`/`version`.
    pub fn for_store(
        def: &StoreDefinition,
        tenant: &str,
        module: &str,
        version: &str,
    ) -> MigrationPlaceholders {
        let store_table = if def.structure.is_some() {
            crate::projected::table_for(def)
        } else {
            KEY_VALUE_TABLE.to_string()
        };
        MigrationPlaceholders {
            tenant: tenant.to_string(),
            module: module.to_string(),
            version: version.to_string(),
            store_name: def.name.clone(),
            store_table,
        }
    }

    fn resolve(&self, name: &str) -> Option<&str> {
        Some(match name {
            "package.tenant" => &self.tenant,
            "package.module" => &self.module,
            "package.version" => &self.version,
            "store.name" => &self.store_name,
            "store.table" => &self.store_table,
            _ => return None,
        })
    }
}

/// One prepared migration script: placeholders substituted, version derived, checksum taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationScript {
    /// The script's file name (`V2__add_index.sql`).
    pub file: String,
    /// The ledger version — `<n>` without leading zeros for `V<n>__` names, else the file stem.
    pub version: String,
    /// The SQL after placeholder substitution — what runs.
    pub sql: String,
    /// Lowercase hex SHA-256 of [`Self::sql`].
    pub checksum: String,
}

/// What a store applies on first use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreMigrations {
    /// Idempotent scripts re-run on every first use, with no ledger — the engine-owned coverage
    /// DDL, and any caller that hands over plain strings.
    Idempotent(Vec<String>),
    /// Package scripts, recorded in [`LEDGER_TABLE`] and run once per database.
    Recorded(Vec<MigrationScript>),
}

impl StoreMigrations {
    /// `true` when there is nothing to apply.
    pub fn is_empty(&self) -> bool {
        match self {
            StoreMigrations::Idempotent(s) => s.is_empty(),
            StoreMigrations::Recorded(s) => s.is_empty(),
        }
    }
}

impl From<Vec<String>> for StoreMigrations {
    fn from(scripts: Vec<String>) -> StoreMigrations {
        StoreMigrations::Idempotent(scripts)
    }
}

impl From<Vec<MigrationScript>> for StoreMigrations {
    fn from(scripts: Vec<MigrationScript>) -> StoreMigrations {
        StoreMigrations::Recorded(scripts)
    }
}

/// The version a `V<n>__<desc>.sql` file name declares, leading zeros stripped; `None` for any
/// other name.
pub fn versioned_file_number(file: &str) -> Option<String> {
    let rest = file.strip_prefix('V')?;
    let (digits, tail) = rest.split_once("__")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    tail.strip_suffix(".sql")?;
    let stripped = digits.trim_start_matches('0');
    Some(if stripped.is_empty() { "0" } else { stripped }.to_string())
}

/// The ledger version of a script file: [`versioned_file_number`], else the file stem.
pub fn migration_version(file: &str) -> String {
    versioned_file_number(file)
        .unwrap_or_else(|| file.strip_suffix(".sql").unwrap_or(file).to_string())
}

/// Lowercase hex SHA-256 of `text`.
pub fn migration_checksum(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Substitute the five placeholders in `sql` (see the module docs). `file` names the script in
/// the error.
pub fn substitute_placeholders(
    file: &str,
    sql: &str,
    placeholders: &MigrationPlaceholders,
) -> Result<String, DataStoreError> {
    let mut out = String::with_capacity(sql.len());
    let mut rest = sql;
    let line_of = |remaining: &str| sql[..sql.len() - remaining.len()].matches('\n').count() + 1;
    while let Some(pos) = rest.find('$') {
        out.push_str(&rest[..pos]);
        let at = &rest[pos..];
        if let Some(after) = at.strip_prefix("$${") {
            out.push_str("${");
            rest = after;
        } else if let Some(after) = at.strip_prefix("${") {
            let Some(end) = after
                .find(['}', '\n'])
                .filter(|&i| after.as_bytes()[i] == b'}')
            else {
                return Err(DataStoreError::with_code(
                    codes::MIGRATION_PLACEHOLDER,
                    format!(
                        "migration '{file}' line {}: '${{' is not closed by '}}' on the same \
                         line (write '$${{' for a literal '${{')",
                        line_of(at)
                    ),
                ));
            };
            let name = &after[..end];
            let Some(value) = placeholders.resolve(name) else {
                return Err(DataStoreError::with_code(
                    codes::MIGRATION_PLACEHOLDER,
                    format!(
                        "migration '{file}' line {}: unknown placeholder '${{{name}}}' — the \
                         supported placeholders are ${{package.tenant}}, ${{package.module}}, \
                         ${{package.version}}, ${{store.name}} and ${{store.table}} (write '$${{' \
                         for a literal '${{')",
                        line_of(at)
                    ),
                ));
            };
            out.push_str(value);
            rest = &after[end + 1..];
        } else {
            out.push('$');
            rest = &at[1..];
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// Prepare a store's raw scripts — `(file name, SQL)` pairs in any order — into the ordered,
/// substituted, checksummed list its provider applies. Refuses duplicate versions and bad
/// placeholders.
pub fn build_migration_scripts(
    store: &str,
    mut files: Vec<(String, String)>,
    placeholders: &MigrationPlaceholders,
) -> Result<Vec<MigrationScript>, DataStoreError> {
    let all_versioned = files
        .iter()
        .all(|(f, _)| versioned_file_number(f).is_some());
    if all_versioned {
        files.sort_by(|(a, _), (b, _)| {
            let (na, nb) = (migration_version(a), migration_version(b));
            (na.len(), na, a).cmp(&(nb.len(), nb, b))
        });
    } else {
        files.sort_by(|(a, _), (b, _)| a.cmp(b));
    }
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    let mut scripts = Vec::with_capacity(files.len());
    for (file, raw) in files {
        let version = migration_version(&file);
        if let Some(previous) = seen.insert(version.clone(), file.clone()) {
            return Err(DataStoreError::with_code(
                codes::MIGRATION_VERSION_DUPLICATE,
                format!(
                    "data store '{store}': migrations '{previous}' and '{file}' both declare \
                     version {version} — every script of a store needs its own version"
                ),
            ));
        }
        let sql = substitute_placeholders(&file, &raw, placeholders)?;
        let checksum = migration_checksum(&sql);
        scripts.push(MigrationScript {
            file,
            version,
            sql,
            checksum,
        });
    }
    Ok(scripts)
}

/// Read a store's module-resident migrations (`properties["sql.migrations"]`, resolved relative
/// to `binding_dir`) and prepare them with [`build_migration_scripts`]. Empty when the store
/// declares no migrations folder; fails closed when it declares one that is not a directory.
pub fn load_migration_scripts(
    def: &StoreDefinition,
    binding_dir: &Path,
    placeholders: &MigrationPlaceholders,
) -> Result<Vec<MigrationScript>, DataStoreError> {
    let files = crate::config::read_migration_files(def, binding_dir)?;
    build_migration_scripts(&def.name, files, placeholders)
}

/// The ledger's `CREATE TABLE`, idempotent, in `dialect`.
pub fn ledger_ddl(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Postgres => {
            "CREATE TABLE IF NOT EXISTS sutra_store_migrations (\
             store_name VARCHAR(255) NOT NULL, version VARCHAR(255) NOT NULL, \
             script VARCHAR(512) NOT NULL, checksum CHAR(64) NOT NULL, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, \
             PRIMARY KEY (store_name, version))"
        }
        SqlDialect::Mysql => {
            "CREATE TABLE IF NOT EXISTS sutra_store_migrations (\
             store_name VARCHAR(255) NOT NULL, version VARCHAR(255) NOT NULL, \
             script VARCHAR(512) NOT NULL, checksum CHAR(64) NOT NULL, \
             applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, \
             PRIMARY KEY (store_name, version))"
        }
        SqlDialect::Mssql => {
            "IF OBJECT_ID(N'sutra_store_migrations', N'U') IS NULL \
             CREATE TABLE sutra_store_migrations (\
             store_name VARCHAR(255) NOT NULL, version VARCHAR(255) NOT NULL, \
             script VARCHAR(512) NOT NULL, checksum CHAR(64) NOT NULL, \
             applied_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(), \
             PRIMARY KEY (store_name, version))"
        }
    }
}

/// What to do with one script, given the store's recorded `(version → checksum)` rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerAction {
    /// Recorded with the same checksum — already applied.
    Skip,
    /// Not recorded — run it, then record it.
    Apply,
}

/// Decide [`LedgerAction`] for `script`; a recorded row with a different checksum is the
/// [`codes::MIGRATION_CHECKSUM`] refusal.
pub fn ledger_action(
    store: &str,
    script: &MigrationScript,
    recorded: &BTreeMap<String, String>,
) -> Result<LedgerAction, DataStoreError> {
    match recorded.get(&script.version) {
        None => Ok(LedgerAction::Apply),
        Some(sum) if sum.trim().eq_ignore_ascii_case(&script.checksum) => Ok(LedgerAction::Skip),
        Some(sum) => Err(DataStoreError::with_code(
            codes::MIGRATION_CHECKSUM,
            format!(
                "data store '{store}': migration '{}' (version {}) was applied with checksum {} \
                 but now has checksum {} — an applied migration must not be edited; restore it \
                 and add a new, higher-versioned V<n>__ script with the change",
                script.file,
                script.version,
                sum.trim(),
                script.checksum
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ph() -> MigrationPlaceholders {
        MigrationPlaceholders {
            tenant: "acme".into(),
            module: "orders".into(),
            version: "1.2.0".into(),
            store_name: "accounts".into(),
            store_table: "data_store".into(),
        }
    }

    #[test]
    fn substitutes_all_five_placeholders() {
        let sql = "-- ${package.tenant}/${package.module}@${package.version} ${store.name} ${store.table}";
        assert_eq!(
            substitute_placeholders("V1__a.sql", sql, &ph()).unwrap(),
            "-- acme/orders@1.2.0 accounts data_store"
        );
    }

    #[test]
    fn escape_and_dollar_quoting_untouched() {
        let sql = "SELECT '$${x}', $$ body $$, $1, $tag$ a $tag$, 5$";
        assert_eq!(
            substitute_placeholders("V1__a.sql", sql, &ph()).unwrap(),
            "SELECT '${x}', $$ body $$, $1, $tag$ a $tag$, 5$"
        );
    }

    #[test]
    fn unknown_placeholder_refused_with_name_and_line() {
        let err = substitute_placeholders("V3__x.sql", "SELECT 1;\nSELECT '${env.HOME}';", &ph())
            .unwrap_err();
        assert_eq!(err.code(), Some(codes::MIGRATION_PLACEHOLDER));
        let msg = err.to_string();
        assert!(msg.contains("V3__x.sql") && msg.contains("line 2"), "{msg}");
        assert!(msg.contains("${env.HOME}"), "{msg}");
    }

    #[test]
    fn unterminated_placeholder_refused() {
        let err =
            substitute_placeholders("V1__a.sql", "SELECT '${package.tenant\n'", &ph()).unwrap_err();
        assert_eq!(err.code(), Some(codes::MIGRATION_PLACEHOLDER));
        assert!(err.to_string().contains("not closed"));
        assert!(substitute_placeholders("V1__a.sql", "x ${", &ph()).is_err());
    }

    #[test]
    fn checksum_is_of_the_substituted_text() {
        let raw = vec![(
            "V1__seed.sql".to_string(),
            "INSERT ${package.tenant}".to_string(),
        )];
        let a = build_migration_scripts("s", raw.clone(), &ph()).unwrap();
        let mut other = ph();
        other.tenant = "globex".into();
        let b = build_migration_scripts("s", raw, &other).unwrap();
        assert_eq!(a[0].sql, "INSERT acme");
        assert_eq!(a[0].checksum, migration_checksum("INSERT acme"));
        assert_eq!(a[0].checksum.len(), 64);
        assert_ne!(a[0].checksum, b[0].checksum);
    }

    #[test]
    fn versions_and_numeric_order() {
        assert_eq!(migration_version("V001__init.sql"), "1");
        assert_eq!(migration_version("V0__zero.sql"), "0");
        assert_eq!(migration_version("001_schema.sql"), "001_schema");
        assert_eq!(versioned_file_number("V1_x.sql"), None);
        let files = ["V10__c.sql", "V2__b.sql", "V001__a.sql"]
            .iter()
            .map(|f| (f.to_string(), String::new()))
            .collect();
        let s = build_migration_scripts("s", files, &ph()).unwrap();
        let order: Vec<_> = s.iter().map(|m| m.file.as_str()).collect();
        assert_eq!(order, ["V001__a.sql", "V2__b.sql", "V10__c.sql"]);
    }

    #[test]
    fn mixed_names_keep_filename_order() {
        let files = ["b.sql", "V10__c.sql", "V2__a.sql"]
            .iter()
            .map(|f| (f.to_string(), String::new()))
            .collect();
        let s = build_migration_scripts("s", files, &ph()).unwrap();
        let order: Vec<_> = s.iter().map(|m| m.file.as_str()).collect();
        assert_eq!(order, ["V10__c.sql", "V2__a.sql", "b.sql"]);
        assert_eq!(s[2].version, "b");
    }

    #[test]
    fn duplicate_version_refused() {
        let files = vec![
            ("V1__a.sql".to_string(), String::new()),
            ("V001__b.sql".to_string(), String::new()),
        ];
        let err = build_migration_scripts("s", files, &ph()).unwrap_err();
        assert_eq!(err.code(), Some(codes::MIGRATION_VERSION_DUPLICATE));
    }

    #[test]
    fn ledger_decisions() {
        let s = build_migration_scripts("s", vec![("V1__a.sql".into(), "X".into())], &ph())
            .unwrap()
            .remove(0);
        let mut recorded = BTreeMap::new();
        assert_eq!(
            ledger_action("s", &s, &recorded).unwrap(),
            LedgerAction::Apply
        );
        recorded.insert("1".to_string(), s.checksum.clone());
        assert_eq!(
            ledger_action("s", &s, &recorded).unwrap(),
            LedgerAction::Skip
        );
        recorded.insert("1".to_string(), "0".repeat(64));
        let err = ledger_action("s", &s, &recorded).unwrap_err();
        assert_eq!(err.code(), Some(codes::MIGRATION_CHECKSUM));
        assert!(err.to_string().contains("V1__a.sql"));
    }

    #[test]
    fn placeholders_for_store_pick_the_written_table() {
        let mut def = StoreDefinition {
            name: "accounts".into(),
            store_type: "sql".into(),
            properties: BTreeMap::new(),
            structure: None,
        };
        assert_eq!(
            MigrationPlaceholders::for_store(&def, "t", "m", "v").store_table,
            "data_store"
        );
        def.structure = Some(crate::config::StructureRef {
            schema: "s".into(),
            type_name: "T".into(),
            columns: BTreeMap::new(),
        });
        assert_eq!(
            MigrationPlaceholders::for_store(&def, "t", "m", "v").store_table,
            "accounts"
        );
        def.properties
            .insert("sql.table".into(), "acct_rows".into());
        assert_eq!(
            MigrationPlaceholders::for_store(&def, "t", "m", "v").store_table,
            "acct_rows"
        );
    }
}
