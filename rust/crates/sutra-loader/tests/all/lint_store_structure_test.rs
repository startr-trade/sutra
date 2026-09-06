//! Projected data stores — the seven `SUTRA.CONFIG.DATASTORE.*` diagnostics `sutra lint` raises
//! for a store that declares a `structure:` block,
//! driven end-to-end through [`sutra_loader::lint_dir`] over real package directories.
//!
//! Every case is built from ONE base package — a flat `AccountRecord` type, a matching table, a
//! realistic migration per dialect — and varies exactly one thing, so a failure names the rule
//! it broke. Each diagnostic has a fire case AND a no-fire case, and the out-of-subset corpus
//! asserts the load-bearing posture: **unparseable DDL degrades to the WARNING and raises no
//! ERROR at all**.

use std::path::Path;

use sutra_loader::{lint_dir, LintReport, LintSeverity};

const PACKAGE_YAML: &str = "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"demo\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n";

const NOT_FLAT: &str = "SUTRA.CONFIG.DATASTORE.STRUCTURE_NOT_FLAT";
const COLUMN_MISSING: &str = "SUTRA.CONFIG.DATASTORE.COLUMN_MISSING";
const TYPE_MISMATCH: &str = "SUTRA.CONFIG.DATASTORE.COLUMN_TYPE_MISMATCH";
const KEY_MISMATCH: &str = "SUTRA.CONFIG.DATASTORE.KEY_MISMATCH";
const NAME_INVALID: &str = "SUTRA.CONFIG.DATASTORE.COLUMN_NAME_INVALID";
const UNVERIFIABLE: &str = "SUTRA.CONFIG.DATASTORE.DDL_UNVERIFIABLE";
const UNMAPPED: &str = "SUTRA.CONFIG.DATASTORE.COLUMN_UNMAPPED";
const DATASTORE_INVALID: &str = "SUTRA.CONFIG.DATASTORE.INVALID";

// ==========================================================================================
// The base package
// ==========================================================================================

/// The declared children of the base `AccountRecord` — every one a scalar leaf, so it projects.
const BASE_FIELDS: &str = r#"
      <xs:element name="accountId" type="AccountId"/>
      <xs:element name="balance"   type="Money"/>
      <xs:element name="openedAt"  type="xs:date"/>
      <xs:element name="active"    type="xs:boolean"/>
      <xs:element name="note"      type="Note" minOccurs="0"/>
"#;

/// The named simple types the base fields restrict — the facets column typing is checked against.
const BASE_TYPES: &str = r#"
  <xs:simpleType name="AccountId">
    <xs:restriction base="xs:string"><xs:maxLength value="35"/></xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Money">
    <xs:restriction base="xs:decimal">
      <xs:totalDigits value="18"/><xs:fractionDigits value="2"/>
    </xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Note">
    <xs:restriction base="xs:string"><xs:maxLength value="140"/></xs:restriction>
  </xs:simpleType>
"#;

/// A codec XSD declaring `AccountRecord` over `fields`, plus any extra declarations.
fn xsd(fields: &str, extra: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns="urn:sutra:demo:accounts"
           targetNamespace="urn:sutra:demo:accounts"
           elementFormDefault="qualified">
  <xs:element name="AccountRecord">
    <xs:complexType>
      <xs:sequence>{fields}      </xs:sequence>
    </xs:complexType>
  </xs:element>
{BASE_TYPES}{extra}
</xs:schema>
"#
    )
}

/// A `datastores.yaml` for one projected `accounts` store, with an optional `columns:` mapping
/// block and optional extra store properties.
fn datastores(columns: &str, extra: &str) -> String {
    format!(
        "datastores:\n  \
         - name: accounts\n    \
           type: sql\n    \
           structure:\n      \
             schema: urn:accounts\n      \
             type: AccountRecord\n{columns}    \
           sql:\n      \
             url-ref: env:ACCOUNTS_DB_URL\n      \
             migrations: migrations/accounts\n{extra}"
    )
}

/// The PostgreSQL spelling of the base table — plus the index and seed a real migration carries.
///
/// A projected table is the design's §4.3 shape: the engine's CONTROL columns (`store_key`, the
/// PRIMARY KEY; `rev`; `updated_at`) plus one column per declared field. There is no
/// `store_name` — a projected table *is* one store.
const PG_MIGRATION: &str = r#"-- The `accounts` store's own table (PostgreSQL spellings).
CREATE TABLE IF NOT EXISTS accounts (
  store_key   VARCHAR(512)  NOT NULL,
  account_id  VARCHAR(35)   NOT NULL,
  balance     NUMERIC(18,2) NOT NULL DEFAULT 0,
  opened_at   DATE          NOT NULL,
  active      BOOLEAN       NOT NULL DEFAULT TRUE,
  note        VARCHAR(140),
  rev         BIGINT        NOT NULL DEFAULT 1,
  updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (store_key)
);

CREATE INDEX IF NOT EXISTS accounts_opened_at ON accounts (opened_at);

INSERT INTO accounts (store_key, account_id, balance, opened_at, active)
VALUES ('alice', 'alice', 100.00, DATE '2026-01-01', TRUE)
ON CONFLICT (store_key) DO NOTHING;
"#;

/// The MySQL / MariaDB spelling: backquoted identifiers, `TINYINT(1)`, an inline `KEY`, and the
/// trailing table options.
const MYSQL_MIGRATION: &str = r#"CREATE TABLE IF NOT EXISTS `accounts` (
  `store_key`  VARCHAR(191)  NOT NULL,
  `account_id` VARCHAR(35)   NOT NULL,
  `balance`    DECIMAL(18,2) NOT NULL DEFAULT 0.00,
  `opened_at`  DATE          NOT NULL,
  `active`     TINYINT(1)    NOT NULL DEFAULT 1,
  `note`       VARCHAR(140)  NULL,
  `rev`        BIGINT        NOT NULL DEFAULT 1,
  `updated_at` DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`store_key`),
  KEY `idx_opened_at` (`opened_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `accounts` (`store_key`, `account_id`, `balance`, `opened_at`, `active`)
VALUES ('alice', 'alice', 100.00, '2026-01-01', 1);
"#;

/// The SQL Server spelling: a schema-qualified bracketed name, `NVARCHAR`, `BIT`, named default
/// constraints, a clustered primary key and `GO` batch separators.
const MSSQL_MIGRATION: &str = r#"CREATE TABLE [dbo].[accounts] (
  [store_key]  NVARCHAR(450) NOT NULL,
  [account_id] NVARCHAR(35)  NOT NULL,
  [balance]    DECIMAL(18,2) NOT NULL CONSTRAINT DF_accounts_balance DEFAULT (0),
  [opened_at]  DATE          NOT NULL,
  [active]     BIT           NOT NULL CONSTRAINT DF_accounts_active DEFAULT (1),
  [note]       NVARCHAR(140) NULL,
  [rev]        BIGINT        NOT NULL CONSTRAINT DF_accounts_rev DEFAULT (1),
  [updated_at] DATETIME2     NOT NULL CONSTRAINT DF_accounts_at DEFAULT (SYSUTCDATETIME()),
  CONSTRAINT PK_accounts PRIMARY KEY CLUSTERED ([store_key])
)
GO

CREATE INDEX IX_accounts_opened ON [dbo].[accounts] ([opened_at])
GO
"#;

// ==========================================================================================
// Harness
// ==========================================================================================

/// One package under test: the codec XSD, the `datastores.yaml`, and the migration scripts.
struct Package {
    xsd: String,
    datastores: String,
    migrations: Vec<(&'static str, String)>,
}

impl Package {
    /// The base package: the flat type, the matching PostgreSQL table, no overrides.
    fn base() -> Package {
        Package {
            xsd: xsd(BASE_FIELDS, ""),
            datastores: datastores("", ""),
            migrations: vec![("V001__accounts.sql", PG_MIGRATION.to_string())],
        }
    }

    fn with_migration(mut self, file: &'static str, sql: &str) -> Package {
        self.migrations.push((file, sql.to_string()));
        self
    }

    /// Replace the (single) V001 migration.
    fn migration(mut self, sql: &str) -> Package {
        self.migrations = vec![("V001__accounts.sql", sql.to_string())];
        self
    }

    fn fields(mut self, fields: &str, extra: &str) -> Package {
        self.xsd = xsd(fields, extra);
        self
    }

    fn datastores(mut self, yaml: String) -> Package {
        self.datastores = yaml;
        self
    }

    fn lint(&self) -> LintReport {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root, "package.yaml", PACKAGE_YAML);
        write(root, "schemas/accounts/accounts.xsd", &self.xsd);
        write(
            root,
            "schemas/accounts/codec-manifest.yaml",
            "schemaKind: xsd\nformats: [xml, json]\n",
        );
        write(root, "datastores.yaml", &self.datastores);
        for (file, sql) in &self.migrations {
            write(root, &format!("migrations/accounts/{file}"), sql);
        }
        lint_dir(root)
    }
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// Every diagnostic carrying `code`.
fn of_code<'r>(report: &'r LintReport, code: &str) -> Vec<&'r str> {
    report
        .diagnostics
        .iter()
        .filter(|d| d.code == code)
        .map(|d| d.message.as_str())
        .collect()
}

fn fires(report: &LintReport, code: &str) -> bool {
    !of_code(report, code).is_empty()
}

fn error_codes(report: &LintReport) -> Vec<&str> {
    report
        .diagnostics
        .iter()
        .filter(|d| d.severity == LintSeverity::Error)
        .map(|d| d.code.as_str())
        .collect()
}

/// No projection diagnostic of any kind, and no error anywhere in the package.
fn assert_silent(report: &LintReport, what: &str) {
    let noise: Vec<String> = report
        .diagnostics
        .iter()
        .filter(|d| d.code.starts_with("SUTRA.CONFIG.DATASTORE."))
        .map(|d| format!("[{:?}] {} — {}", d.severity, d.code, d.message))
        .collect();
    assert!(noise.is_empty(), "{what} should be silent, got: {noise:#?}");
    assert!(
        error_codes(report).is_empty(),
        "{what} raised unrelated errors: {:#?}",
        report.errors().map(|d| d.to_string()).collect::<Vec<_>>()
    );
}

// ==========================================================================================
// The clean corpus
// ==========================================================================================

/// A realistic migration in each shipped dialect, against the SAME declared structure, verifies
/// clean — the baseline every fire case below deviates from by exactly one thing.
#[test]
fn realistic_migrations_verify_clean_in_every_dialect() {
    for (dialect, sql) in [
        ("postgres", PG_MIGRATION),
        ("mysql", MYSQL_MIGRATION),
        ("mssql", MSSQL_MIGRATION),
    ] {
        let report = Package::base().migration(sql).lint();
        assert_silent(&report, dialect);
    }
}

/// A store with no `structure:` block is not verified at all — the historical opaque key→JSON
/// store, unchanged, even when its migrations are wildly outside the parse subset.
#[test]
fn a_store_without_a_structure_block_is_untouched() {
    let package = Package::base()
        .datastores(
            "datastores:\n  - name: accounts\n    type: sql\n    sql:\n      \
             url-ref: env:ACCOUNTS_DB_URL\n      migrations: migrations/accounts\n"
                .to_string(),
        )
        .migration(
            "CREATE OR REPLACE FUNCTION f() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ \
             LANGUAGE plpgsql;",
        );
    assert_silent(&package.lint(), "a store with no structure block");
}

// ==========================================================================================
// The five ERROR diagnostics
// ==========================================================================================

/// §4.2 — a nested, repeated or open child is a package-time ERROR naming the child and the
/// remedy. A type with none of them projects.
#[test]
fn structure_not_flat_fires_per_offending_child_and_not_on_a_flat_type() {
    let nested = r#"
      <xs:element name="accountId" type="AccountId"/>
      <xs:element name="owner">
        <xs:complexType><xs:sequence><xs:element name="name" type="xs:string"/></xs:sequence></xs:complexType>
      </xs:element>
"#;
    let repeated = r#"
      <xs:element name="accountId" type="AccountId"/>
      <xs:element name="tag" type="xs:string" maxOccurs="unbounded"/>
"#;
    let open = r#"
      <xs:element name="accountId" type="AccountId"/>
      <xs:any processContents="lax"/>
"#;
    for (label, fields, offender) in [
        ("nested", nested, "owner"),
        ("repeated", repeated, "tag"),
        ("open", open, "(any)"),
    ] {
        let report = Package::base().fields(fields, "").lint();
        let messages = of_code(&report, NOT_FLAT);
        assert_eq!(messages.len(), 1, "{label}: {report:#?}");
        assert!(
            messages[0].contains(offender)
                && messages[0].contains("Flatten the type, or remove the 'structure' block"),
            "{label} message must name the child and the remedy: {}",
            messages[0]
        );
        // Flatness short-circuits: no column diagnostics pile on top of the real fault.
        assert!(!fires(&report, COLUMN_MISSING), "{label}");
    }
    assert!(!fires(&Package::base().lint(), NOT_FLAT));
}

/// Which table the projection lives in: the store-named one when the migrations create several
/// (an audit sidecar is ordinary), or the one the store names with `sql.table`. A `sql.table`
/// no migration creates is unprovable, not wrong.
#[test]
fn the_projected_table_is_selected_by_name_or_by_sql_table() {
    let with_sidecar = format!(
        "{PG_MIGRATION}\nCREATE TABLE accounts_audit (\n  at TIMESTAMP NOT NULL,\n  \
         who VARCHAR(35) NOT NULL,\n  PRIMARY KEY (at, who)\n);\n"
    );
    assert_silent(
        &Package::base().migration(&with_sidecar).lint(),
        "a sidecar table beside the store-named one",
    );

    // The same DDL with the projected table renamed — resolved by an explicit `sql.table`.
    let renamed = with_sidecar.replace("accounts (\n", "account_ledger (\n");
    assert_silent(
        &Package::base()
            .migration(&renamed)
            .datastores(datastores("", "      table: account_ledger\n"))
            .lint(),
        "a projected table named by sql.table",
    );

    let report = Package::base()
        .datastores(datastores("", "      table: nowhere\n"))
        .lint();
    assert!(
        of_code(&report, UNVERIFIABLE)[0].contains("sql.table 'nowhere'"),
        "{report:#?}"
    );
    assert!(error_codes(&report).is_empty(), "{report:#?}");
}

/// A declared field with no column is an ERROR naming the field, the column it projects to, and
/// both remedies (a migration, or a `columns:` mapping).
#[test]
fn column_missing_fires_only_when_the_column_is_absent() {
    let without_opened_at = PG_MIGRATION.replace("  opened_at   DATE          NOT NULL,\n", "");
    let report = Package::base().migration(&without_opened_at).lint();
    let messages = of_code(&report, COLUMN_MISSING);
    assert_eq!(messages.len(), 1, "{report:#?}");
    assert!(
        messages[0].contains("'openedAt'")
            && messages[0].contains("'opened_at'")
            && messages[0].contains("V-numbered migration")
            && messages[0].contains("'columns:'"),
        "{}",
        messages[0]
    );
    assert!(!fires(&Package::base().lint(), COLUMN_MISSING));
}

/// §4.5 — the column must be able to hold the declared facet range. Each row narrows one column
/// past what the declaration needs; the base package proves the same shapes pass when they fit.
#[test]
fn column_type_mismatch_fires_on_a_column_that_cannot_hold_the_declared_range() {
    let cases: &[(&str, &str, &str, &str)] = &[
        (
            "a maxLength=35 string in VARCHAR(10)",
            "account_id  VARCHAR(35)   NOT NULL",
            "account_id  VARCHAR(10)   NOT NULL",
            "'accountId'",
        ),
        (
            "a fractional decimal in an integer column",
            "balance     NUMERIC(18,2) NOT NULL DEFAULT 0",
            "balance     BIGINT        NOT NULL DEFAULT 0",
            "'balance'",
        ),
        (
            "totalDigits/fractionDigits past the column's precision",
            "balance     NUMERIC(18,2) NOT NULL DEFAULT 0",
            "balance     NUMERIC(10,1) NOT NULL DEFAULT 0",
            "'balance'",
        ),
        (
            "a date field in a text column",
            "opened_at   DATE          NOT NULL",
            "opened_at   VARCHAR(10)   NOT NULL",
            "'openedAt'",
        ),
        (
            "an optional field in a NOT NULL column with no default",
            "note        VARCHAR(140),",
            "note        VARCHAR(140) NOT NULL,",
            "is optional",
        ),
    ];
    for (label, from, to, expected) in cases {
        let sql = PG_MIGRATION.replace(from, to);
        assert_ne!(sql, PG_MIGRATION, "{label}: the substitution must apply");
        let report = Package::base().migration(&sql).lint();
        let messages = of_code(&report, TYPE_MISMATCH);
        assert_eq!(messages.len(), 1, "{label}: {report:#?}");
        assert!(messages[0].contains(expected), "{label}: {}", messages[0]);
    }
    assert!(!fires(&Package::base().lint(), TYPE_MISMATCH));
}

/// §4.3 — the row's identity is the STORE KEY, never a declared field: a table with no key, or
/// one keyed on anything but `store_key` alone, is an ERROR. (A business-keyed table would let
/// two store keys collide on one row, which is why this is not merely a style preference.)
#[test]
fn key_mismatch_fires_without_a_key_and_when_it_is_not_the_store_key() {
    let keyless = PG_MIGRATION.replace(",\n  PRIMARY KEY (store_key)", "");
    let report = Package::base().migration(&keyless).lint();
    assert!(
        of_code(&report, KEY_MISMATCH)[0].contains("declares no PRIMARY KEY"),
        "{report:#?}"
    );

    // Keyed on a declared BUSINESS field instead of the store key.
    let business_key = PG_MIGRATION.replace("PRIMARY KEY (store_key)", "PRIMARY KEY (account_id)");
    let report = Package::base().migration(&business_key).lint();
    let messages = of_code(&report, KEY_MISMATCH);
    assert_eq!(messages.len(), 1, "{report:#?}");
    assert!(
        messages[0].contains("'account_id'")
            && messages[0].contains("'store_key' must be the PRIMARY KEY"),
        "{}",
        messages[0]
    );

    // Keyed on a surrogate column the projection never writes.
    let surrogate = PG_MIGRATION.replace(
        "  PRIMARY KEY (store_key)",
        "  row_id      BIGINT        NOT NULL DEFAULT 1,\n  PRIMARY KEY (row_id)",
    );
    let report = Package::base().migration(&surrogate).lint();
    assert!(
        of_code(&report, KEY_MISMATCH)[0].contains("'row_id'"),
        "{report:#?}"
    );

    // A COMPOSITE key that merely includes the store key is still not the store key.
    let composite = PG_MIGRATION.replace(
        "PRIMARY KEY (store_key)",
        "PRIMARY KEY (store_key, account_id)",
    );
    let report = Package::base().migration(&composite).lint();
    assert!(fires(&report, KEY_MISMATCH), "{report:#?}");

    assert!(!fires(&Package::base().lint(), KEY_MISMATCH));
}

/// §4.3, the runtime↔lint contract this test exists to guard: the three control columns are
/// EXPECTED, not unmapped strays. Their absence is `COLUMN_MISSING` (the runtime cannot serve the
/// store without them); their presence is silent — never a `COLUMN_UNMAPPED` warning, which is
/// what a projected table built exactly as the runtime's own dialect suites build it would
/// otherwise trip on every single time.
#[test]
fn control_columns_are_required_and_never_reported_as_unmapped() {
    // Present → silent. (`realistic_migrations_verify_clean_in_every_dialect` asserts the same
    // for all three dialects; this states the rule in isolation.)
    assert!(!fires(&Package::base().lint(), UNMAPPED));
    assert!(!fires(&Package::base().lint(), COLUMN_MISSING));

    // Absent → one COLUMN_MISSING naming each, with the remedy.
    let without_control = PG_MIGRATION
        .replace("  store_key   VARCHAR(512)  NOT NULL,\n", "")
        .replace("  rev         BIGINT        NOT NULL DEFAULT 1,\n", "")
        .replace(
            "  updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,\n",
            "",
        )
        .replace("PRIMARY KEY (store_key)", "PRIMARY KEY (account_id)")
        .replace("(store_key, account_id", "(account_id")
        .replace("VALUES ('alice', 'alice',", "VALUES ('alice',")
        .replace("ON CONFLICT (store_key)", "ON CONFLICT (account_id)");
    let report = Package::base().migration(&without_control).lint();
    let messages = of_code(&report, COLUMN_MISSING);
    assert_eq!(messages.len(), 1, "reported once, whole: {report:#?}");
    for control in ["'store_key'", "'rev'", "'updated_at'"] {
        assert!(messages[0].contains(control), "{}", messages[0]);
    }
    assert!(messages[0].contains("V-numbered migration") || messages[0].contains("migration"));

    // A declared field that folds onto a control column is COLUMN_NAME_INVALID, not a silent
    // fight with the runtime over the column.
    let fields = format!("{BASE_FIELDS}      <xs:element name=\"rev\" type=\"xs:int\"/>\n");
    let report = Package::base().fields(&fields, "").lint();
    let messages = of_code(&report, NAME_INVALID);
    assert_eq!(messages.len(), 1, "{report:#?}");
    assert!(
        messages[0].contains("control column") && messages[0].contains("'columns:'"),
        "{}",
        messages[0]
    );
}

/// §4.4 — a folded name that is reserved (or collides, or is over-length) is an ERROR the
/// `columns:` mapping resolves.
#[test]
fn column_name_invalid_fires_on_a_reserved_fold_and_a_mapping_resolves_it() {
    let fields = format!("{BASE_FIELDS}      <xs:element name=\"order\" type=\"AccountId\"/>\n");
    let with_order_column = PG_MIGRATION.replace(
        "  note        VARCHAR(140),",
        "  note        VARCHAR(140),\n  order_ref   VARCHAR(35)   NOT NULL,",
    );

    let unmapped = Package::base()
        .fields(&fields, "")
        .migration(&with_order_column)
        .lint();
    let messages = of_code(&unmapped, NAME_INVALID);
    assert_eq!(messages.len(), 1, "{unmapped:#?}");
    assert!(
        messages[0].contains("reserved word 'order'")
            && messages[0].contains("Map the offending field(s) explicitly under 'columns:'"),
        "{}",
        messages[0]
    );

    let mapped = Package::base()
        .fields(&fields, "")
        .datastores(datastores("      columns:\n        order: order_ref\n", ""))
        .migration(&with_order_column)
        .lint();
    assert_silent(&mapped, "a reserved name resolved by a columns: mapping");
}

// ==========================================================================================
// The two WARNING diagnostics — and the false-positive guard
// ==========================================================================================

/// **The load-bearing posture.** Every one of these is legitimate SQL that this parser does not
/// model. Each must degrade the store to exactly one `DDL_UNVERIFIABLE` warning and raise NO
/// error at all — a linter that cried wolf here would teach authors to ignore it.
#[test]
fn out_of_subset_sql_degrades_to_a_warning_and_never_to_an_error() {
    let cases: &[(&str, &str)] = &[
        (
            "a plpgsql trigger function (verbatim from the money-transfer example)",
            "CREATE TABLE IF NOT EXISTS accounts (account_id VARCHAR(35) NOT NULL PRIMARY KEY);\n\
             CREATE OR REPLACE FUNCTION reject_credit() RETURNS trigger AS $$\n\
             BEGIN\n  IF NEW.balance < 0 THEN RAISE EXCEPTION 'no'; END IF;\n  RETURN NEW;\n\
             END;\n$$ LANGUAGE plpgsql;\n",
        ),
        (
            "a T-SQL procedural guard block",
            "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'accounts')\nBEGIN\n  \
             CREATE TABLE accounts (account_id NVARCHAR(35) NOT NULL PRIMARY KEY)\nEND\n",
        ),
        (
            "an ALTER TABLE clause outside the subset",
            "CREATE TABLE accounts (account_id VARCHAR(35) NOT NULL PRIMARY KEY);\n\
             ALTER TABLE accounts SET SCHEMA archive;\n",
        ),
        (
            "a table created elsewhere (only ALTERed here)",
            "ALTER TABLE accounts ADD COLUMN note VARCHAR(140);\n",
        ),
        (
            "several tables, none named after the store",
            "CREATE TABLE ledger_a (id VARCHAR(10) PRIMARY KEY);\n\
             CREATE TABLE ledger_b (id VARCHAR(10) PRIMARY KEY);\n",
        ),
        (
            "an unterminated literal",
            "CREATE TABLE accounts (account_id VARCHAR(35) NOT NULL PRIMARY KEY, \
             note VARCHAR(140) DEFAULT 'oops);\n",
        ),
    ];
    for (label, sql) in cases {
        let report = Package::base().migration(sql).lint();
        assert_eq!(
            of_code(&report, UNVERIFIABLE).len(),
            1,
            "{label} must warn exactly once: {report:#?}"
        );
        for code in [
            NOT_FLAT,
            COLUMN_MISSING,
            TYPE_MISMATCH,
            KEY_MISMATCH,
            NAME_INVALID,
        ] {
            assert!(
                !fires(&report, code),
                "{label} must NOT raise {code}: {report:#?}"
            );
        }
        assert!(
            error_codes(&report).is_empty(),
            "{label} must raise no error: {:#?}",
            report.errors().map(|d| d.to_string()).collect::<Vec<_>>()
        );
    }
}

/// A store that declares a structure but ships no migrations is unprovable, not wrong. A column
/// type outside the comparable set is likewise unprovable — and reported honestly, per field.
#[test]
fn unprovable_shapes_warn_with_honest_wording() {
    // No migrations at all — the layout check owns the "no scripts" ERROR, so the projection
    // pass is asserted on its own by pointing the store at a folder with an unrelated script.
    let elsewhere = Package::base().migration("-- nothing but a comment\n");
    let report = elsewhere.lint();
    assert!(
        of_code(&report, UNVERIFIABLE)[0].contains("not provable"),
        "{report:#?}"
    );

    // An uncomparable column type: the column exists, so nothing is missing — only the type
    // comparison is withheld.
    let jsonb = PG_MIGRATION.replace("note        VARCHAR(140),", "note        JSONB,");
    let report = Package::base().migration(&jsonb).lint();
    let messages = of_code(&report, UNVERIFIABLE);
    assert_eq!(messages.len(), 1, "{report:#?}");
    assert!(
        messages[0].contains("'note'") && messages[0].contains("JSONB"),
        "{}",
        messages[0]
    );
    assert!(error_codes(&report).is_empty(), "{report:#?}");
}

/// A column the projection never writes is a WARNING (a legacy or operator column), never a
/// block — but a `NOT NULL` one with no default is called out as write-blocking.
#[test]
fn column_unmapped_warns_and_never_blocks() {
    let legacy = PG_MIGRATION.replace(
        "  note        VARCHAR(140),",
        "  note        VARCHAR(140),\n  legacy_ref  VARCHAR(20),",
    );
    let report = Package::base().migration(&legacy).lint();
    let messages = of_code(&report, UNMAPPED);
    assert_eq!(messages.len(), 1, "{report:#?}");
    assert!(messages[0].contains("'legacy_ref'"), "{}", messages[0]);
    assert!(
        error_codes(&report).is_empty(),
        "an unmapped column never blocks"
    );

    let blocking = PG_MIGRATION.replace(
        "  note        VARCHAR(140),",
        "  note        VARCHAR(140),\n  legacy_ref  VARCHAR(20) NOT NULL,",
    );
    let report = Package::base().migration(&blocking).lint();
    assert!(
        of_code(&report, UNMAPPED)[0].contains("every insert fail"),
        "{report:#?}"
    );

    assert!(!fires(&Package::base().lint(), UNMAPPED));
}

// ==========================================================================================
// Schema evolution (design §5) and migration ordering
// ==========================================================================================

/// The design's §5 table, row by row. Each starts from the base package and applies the change
/// on both sides (schema + migration) exactly as an author would.
#[test]
fn the_design_s_evolution_rows_behave_as_specified() {
    let with_nickname = format!(
        "{BASE_FIELDS}      <xs:element name=\"nickname\" type=\"Note\" minOccurs=\"{{MIN}}\"/>\n"
    );
    let optional_field = with_nickname.replace("{MIN}", "0");
    let required_field = with_nickname.replace("{MIN}", "1");

    // 1. Add an OPTIONAL scalar field + a nullable column → additive, silent.
    let report = Package::base()
        .fields(&optional_field, "")
        .with_migration(
            "V002__nickname.sql",
            "ALTER TABLE accounts ADD COLUMN nickname VARCHAR(140);",
        )
        .lint();
    assert_silent(&report, "an added optional field with a nullable column");

    // 2. Add a REQUIRED scalar field whose column is NOT NULL with no DEFAULT → ERROR: rows
    //    written before the ALTER cannot satisfy it.
    let report = Package::base()
        .fields(&required_field, "")
        .with_migration(
            "V002__nickname.sql",
            "ALTER TABLE accounts ADD COLUMN nickname VARCHAR(140) NOT NULL;",
        )
        .lint();
    assert!(
        of_code(&report, TYPE_MISMATCH)[0].contains("rows written before that migration"),
        "{report:#?}"
    );
    //    …and clean once it carries a DEFAULT.
    let report = Package::base()
        .fields(&required_field, "")
        .with_migration(
            "V002__nickname.sql",
            "ALTER TABLE accounts ADD COLUMN nickname VARCHAR(140) NOT NULL DEFAULT '';",
        )
        .lint();
    assert_silent(&report, "an added required field with a defaulted column");

    // 3. Remove a field → the column becomes unmapped (WARNING), data untouched.
    let without_note = BASE_FIELDS.replace(
        "      <xs:element name=\"note\"      type=\"Note\" minOccurs=\"0\"/>\n",
        "",
    );
    let report = Package::base().fields(&without_note, "").lint();
    assert!(
        of_code(&report, UNMAPPED)[0].contains("'note'"),
        "{report:#?}"
    );
    assert!(
        error_codes(&report).is_empty(),
        "a removed field never blocks"
    );

    // 4. Widen a facet (maxLength 35 → 70) → ERROR until the ALTER ships, then clean.
    let widened = BASE_TYPES.replace(
        "<xs:restriction base=\"xs:string\"><xs:maxLength value=\"35\"/></xs:restriction>",
        "<xs:restriction base=\"xs:string\"><xs:maxLength value=\"70\"/></xs:restriction>",
    );
    let widened_xsd = xsd(BASE_FIELDS, "").replace(BASE_TYPES, &widened);
    let mut package = Package::base();
    package.xsd = widened_xsd.clone();
    let report = package.lint();
    assert!(
        of_code(&report, TYPE_MISMATCH)[0].contains("declared maxLength 70"),
        "{report:#?}"
    );
    let mut package = Package::base().with_migration(
        "V002__widen.sql",
        "ALTER TABLE accounts ALTER COLUMN account_id TYPE VARCHAR(70);",
    );
    package.xsd = widened_xsd;
    assert_silent(&package.lint(), "a widened facet with its ALTER shipped");

    // 5. Scalar → nested is STRUCTURE_NOT_FLAT (the projection is given up explicitly).
    let nested = BASE_FIELDS.replace(
        "<xs:element name=\"note\"      type=\"Note\" minOccurs=\"0\"/>",
        "<xs:element name=\"note\"><xs:complexType><xs:sequence>\
         <xs:element name=\"text\" type=\"xs:string\"/></xs:sequence></xs:complexType></xs:element>",
    );
    let report = Package::base().fields(&nested, "").lint();
    assert!(
        of_code(&report, NOT_FLAT)[0].contains("'note'"),
        "{report:#?}"
    );

    // 6. Rename, with the `columns:` mapping preserving the physical column.
    let renamed = BASE_FIELDS.replace("name=\"accountId\"", "name=\"acctRef\"");
    let report = Package::base()
        .fields(&renamed, "")
        .datastores(datastores(
            "      columns:\n        acctRef: account_id\n",
            "",
        ))
        .lint();
    assert_silent(&report, "a rename preserved by a columns: mapping");
}

/// The effective shape is every migration applied in version order — V002's `ALTER`s are as
/// binding as V001's `CREATE`.
#[test]
fn the_effective_shape_reflects_every_migration_in_order() {
    // V001 creates a table that does NOT satisfy the structure; V002 fixes it. Clean only if
    // both are applied, in order.
    let v001 = "CREATE TABLE accounts (\n  store_key VARCHAR(512) NOT NULL,\n  \
                account_id VARCHAR(10) NOT NULL,\n  \
                balance NUMERIC(18,2) NOT NULL,\n  scratch INT NOT NULL DEFAULT 0,\n  \
                rev BIGINT NOT NULL DEFAULT 1,\n  \
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n  \
                PRIMARY KEY (store_key)\n);";
    let v002 = "ALTER TABLE accounts ALTER COLUMN account_id TYPE VARCHAR(35);\n\
                ALTER TABLE accounts ADD COLUMN opened_at DATE NOT NULL DEFAULT CURRENT_DATE;\n\
                ALTER TABLE accounts ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE;\n\
                ALTER TABLE accounts ADD COLUMN note VARCHAR(140);\n\
                ALTER TABLE accounts DROP COLUMN scratch;";
    let report = Package::base()
        .migration(v001)
        .with_migration("V002__evolve.sql", v002)
        .lint();
    assert_silent(&report, "V001 + V002 applied in order");

    // Without V002 the same V001 is not enough — proof the ordering test is not vacuous.
    let report = Package::base().migration(v001).lint();
    assert!(
        fires(&report, COLUMN_MISSING) && fires(&report, TYPE_MISMATCH),
        "{report:#?}"
    );
}

// ==========================================================================================
// Schema resolution
// ==========================================================================================

/// A `structure:` naming something that does not exist is a definite fault (ERROR); a schema
/// whose fields simply cannot be enumerated here is unprovable (WARNING).
#[test]
fn schema_resolution_separates_the_unknown_from_the_unverifiable() {
    // An unknown schema and an unknown type are both definite faults.
    for (label, yaml, expected) in [
        (
            "unknown schema",
            datastores("", "").replace("schema: urn:accounts", "schema: urn:nope"),
            "declares no codec for",
        ),
        (
            "unknown type",
            datastores("", "").replace("type: AccountRecord", "type: NoSuchType"),
            "neither as a type nor as a root element",
        ),
    ] {
        let report = Package::base().datastores(yaml).lint();
        let messages = of_code(&report, DATASTORE_INVALID);
        assert_eq!(messages.len(), 1, "{label}: {report:#?}");
        assert!(messages[0].contains(expected), "{label}: {}", messages[0]);
    }

    // An engine-provided codec has an open type set — unprovable, not wrong.
    let report = Package::base()
        .datastores(
            datastores("", "").replace("schema: urn:accounts", "schema: urn:sutra:codec:json"),
        )
        .lint();
    assert!(
        of_code(&report, UNVERIFIABLE)[0].contains("engine-provided codec"),
        "{report:#?}"
    );
    assert!(error_codes(&report).is_empty(), "{report:#?}");
}

/// The loader's code registry and the fault's own code are one string, not two that can drift.
#[test]
fn the_shared_codes_are_string_identical_to_the_projection_s() {
    use sutra_datastore::projection::codes as projection;
    assert_eq!(projection::STRUCTURE_NOT_FLAT, NOT_FLAT);
    assert_eq!(projection::COLUMN_NAME_INVALID, NAME_INVALID);
    assert_eq!(
        sutra_loader::error::codes::CONFIG_DATASTORE_STRUCTURE_NOT_FLAT,
        NOT_FLAT
    );
    assert_eq!(
        sutra_loader::error::codes::CONFIG_DATASTORE_COLUMN_NAME_INVALID,
        NAME_INVALID
    );
    assert_eq!(
        sutra_loader::error::codes::CONFIG_DATASTORE_COLUMN_MISSING,
        COLUMN_MISSING
    );
    assert_eq!(
        sutra_loader::error::codes::CONFIG_DATASTORE_COLUMN_TYPE_MISMATCH,
        TYPE_MISMATCH
    );
    assert_eq!(
        sutra_loader::error::codes::CONFIG_DATASTORE_KEY_MISMATCH,
        KEY_MISMATCH
    );
    assert_eq!(
        sutra_loader::error::codes::CONFIG_DATASTORE_DDL_UNVERIFIABLE,
        UNVERIFIABLE
    );
    assert_eq!(
        sutra_loader::error::codes::CONFIG_DATASTORE_COLUMN_UNMAPPED,
        UNMAPPED
    );
}
