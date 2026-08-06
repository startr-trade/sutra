//! The projected-store RUNTIME — the SQL, the value marshalling and the first-use table check a
//! store that declares a `structure` is served through (design `datastore-schema-projection.md`
//! §4.6).
//!
//! Where [`crate::projection`] answers *what columns does this declared type have*, this module
//! answers *what statements does a store with those columns run, and what goes on the wire*. It is
//! the whole dialect seam the design asks for (§6: "the three provider modules gain a shared
//! projection module; the URL-scheme dispatch stays") — there is deliberately **no**
//! `DataStoreProvider` SPI. [`SqlDialect`] carries the three differences that actually exist
//! (placeholder syntax, the write-side cast, the read-side text rendering); everything else — the
//! statement shapes, the bind order, the JSON marshalling, the verification rules — is written
//! once, here, and is pure.
//!
//! Pure means: no connection, no driver type, no async. That keeps this module inside the
//! `--no-default-features` build with `config`/`projection`, and it is what makes the SQL shapes
//! tier-1 assertable as strings rather than only observable against a live database.
//!
//! ## The projected table
//!
//! A projected store's rows live in the AUTHOR's own table — the one their
//! `migrations/<store>/V00n__*.sql` creates — not in the generic `data_store` blob table:
//!
//! ```sql
//! CREATE TABLE accounts (            -- the store's name, or its `sql.table` property
//!   store_key  VARCHAR(512) PRIMARY KEY,
//!   account_id VARCHAR(35)  NOT NULL,   -- one column per declared scalar, in declared order
//!   balance    NUMERIC(18,2) NOT NULL,
//!   opened_at  DATE,                    -- an optional field's column must admit NULL
//!   rev        BIGINT       NOT NULL,
//!   updated_at TIMESTAMPTZ  NOT NULL
//! );
//! ```
//!
//! There is no `store_name` column: the table IS the store, so the single-column `store_key`
//! predicate is the whole isolation — the same reasoning that collapsed the namespace columns on
//! the KV path. There is no `deployment_id` and no RLS, per the standing ruling that a business
//! store's data carries across a version bump. `rev`/`updated_at` are the engine's control
//! columns and carry exactly the KV path's semantics (insert at `1`, `+1` on every write, CAS
//! keys on `rev`).
//!
//! ## Round-trip fidelity
//!
//! `get(put(v)) == v` holds byte-for-byte for the scalar classes the design's type mapping (§4.5)
//! puts in a column whose canonical text form is exact: strings, decimals **including the written
//! scale**, integers, booleans and dates, plus an absent optional field. Values travel as text in
//! both directions — bound as text with the dialect's write-side cast, read back through the
//! dialect's canonical text rendering — so a decimal's scale is decided by the author's own
//! `NUMERIC(p,s)` and never by an intermediate `f64`. See [`ValueClass`] for the per-class
//! statement of what is exact and what is canonicalised.

use serde_json::{Map, Value as Json};
use sutra_xsd::Builtin;

use crate::error::DataStoreError;
use crate::projection::{ProjectedField, Projection};

/// The runtime diagnostic codes a projected store raises (design §4.2/§4.6). They ride
/// [`DataStoreError::code`] and render as a `[CODE] ` prefix on the message.
///
/// Note where they end up: unlike `SUTRA.RUNTIME.DATASTORE.CONFLICT`, which the executor
/// raises as a first-class diagnostic code, these are carried *inside* the message of a
/// `SUTRA.RUNTIME.UNEXPECTED` diagnostic (the store SPI hands the executor a string). They are
/// therefore greppable and stable, but they are not the instance's diagnostic code — do not
/// describe them to operators as if they were.
pub mod codes {
    /// A write carried a field the declared structure does not declare. The design refuses
    /// exactly one outcome — silently dropping it (§4.2).
    pub const UNDECLARED_FIELD: &str = "SUTRA.RUNTIME.DATASTORE.UNDECLARED_FIELD";
    /// The live table cannot satisfy the projection: a column is missing, a column an optional
    /// field needs is `NOT NULL`, or a column the projection never writes is mandatory. Raised on
    /// first use and fails the store closed (§4.6, RULED 2026-08-04).
    pub const PROJECTION_UNSATISFIABLE: &str = "SUTRA.RUNTIME.DATASTORE.PROJECTION_UNSATISFIABLE";
    /// A projected store was handed a value that is not a record (a scalar, an array, or null).
    /// A projected row IS its declared fields, so there is nowhere for a non-record to go.
    pub const VALUE_NOT_A_RECORD: &str = "SUTRA.RUNTIME.DATASTORE.VALUE_NOT_A_RECORD";
}

/// The key column of a projected table — the whole row predicate (no `store_name`: the table is
/// the store).
pub const KEY_COLUMN: &str = "store_key";
/// The revision column — insert at `1`, `+1` on every write, the CAS predicate.
pub const REV_COLUMN: &str = "rev";
/// The write-timestamp column, set from the dialect's current-timestamp function.
pub const UPDATED_AT_COLUMN: &str = "updated_at";
/// Every column the engine owns in a projected table. A declared field may not claim one.
pub const CONTROL_COLUMNS: [&str; 3] = [KEY_COLUMN, REV_COLUMN, UPDATED_AT_COLUMN];

// ---- dialects ---------------------------------------------------------------------------

/// The three shipped SQL dialects, carrying only what genuinely differs between them. This is
/// the whole dialect seam — deliberately an enum of three behaviours rather than a provider SPI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    /// PostgreSQL — `$n` placeholders, explicit write-side `CAST`, `CAST(col AS TEXT)` on read.
    Postgres,
    /// MySQL / MariaDB — `?` placeholders, implicit string coercion, `CAST(col AS CHAR)` on read.
    Mysql,
    /// Microsoft SQL Server — `@Pn` placeholders, implicit string coercion, `CONVERT` on read.
    Mssql,
}

impl SqlDialect {
    /// The `n`-th (1-based) parameter placeholder in this dialect's syntax.
    pub fn placeholder(self, n: usize) -> String {
        match self {
            SqlDialect::Postgres => format!("${n}"),
            SqlDialect::Mysql => "?".to_string(),
            SqlDialect::Mssql => format!("@P{n}"),
        }
    }

    /// This dialect's current-timestamp function, as the KV path already writes it.
    pub fn now(self) -> &'static str {
        match self {
            SqlDialect::Postgres | SqlDialect::Mysql => "CURRENT_TIMESTAMP",
            SqlDialect::Mssql => "SYSUTCDATETIME()",
        }
    }

    /// The expression a value of `class` is written through.
    ///
    /// Every value is BOUND as text (or NULL); PostgreSQL types its parameters, so a text
    /// parameter needs an explicit cast into the column's type family before the assignment cast
    /// to the author's declared width/precision can happen. MySQL and SQL Server coerce a string
    /// literal to the target column's type implicitly, so their placeholder stands alone.
    fn write_expr(self, class: ValueClass, n: usize) -> String {
        let placeholder = self.placeholder(n);
        match (self, class) {
            (SqlDialect::Postgres, ValueClass::Text) => placeholder,
            (SqlDialect::Postgres, ValueClass::Number) => format!("CAST({placeholder} AS NUMERIC)"),
            (SqlDialect::Postgres, ValueClass::Boolean) => {
                format!("CAST({placeholder} AS BOOLEAN)")
            }
            (SqlDialect::Postgres, ValueClass::Date) => format!("CAST({placeholder} AS DATE)"),
            (SqlDialect::Postgres, ValueClass::DateTime) => {
                format!("CAST({placeholder} AS TIMESTAMPTZ)")
            }
            (SqlDialect::Postgres, ValueClass::Time) => format!("CAST({placeholder} AS TIME)"),
            (SqlDialect::Mysql | SqlDialect::Mssql, _) => placeholder,
        }
    }

    /// The expression a column of `class` is read back through — the dialect's canonical text
    /// rendering, which is what makes the round-trip lexical rather than float-mediated.
    fn read_expr(self, class: ValueClass, column: &str) -> String {
        match self {
            SqlDialect::Postgres => format!("CAST({column} AS TEXT)"),
            SqlDialect::Mysql => format!("CAST({column} AS CHAR)"),
            // SQL Server's default date renderings are locale-shaped ("Aug  4 2026"), so the
            // ISO styles are named explicitly: 23 = yyyy-mm-dd, 126 = ISO-8601 date-time.
            SqlDialect::Mssql => match class {
                ValueClass::Date => format!("CONVERT(NVARCHAR(MAX), {column}, 23)"),
                ValueClass::DateTime => format!("CONVERT(NVARCHAR(MAX), {column}, 126)"),
                _ => format!("CONVERT(NVARCHAR(MAX), {column})"),
            },
        }
    }

    /// Render an `information_schema` expression as text — the catalog columns are domain-typed
    /// on PostgreSQL (`sql_identifier`, `yes_or_no`), so every dialect gets an explicit cast and
    /// the probe decodes uniformly as three strings.
    fn as_text(self, expr: &str) -> String {
        match self {
            SqlDialect::Postgres => format!("CAST({expr} AS TEXT)"),
            SqlDialect::Mysql => format!("CAST({expr} AS CHAR)"),
            SqlDialect::Mssql => format!("CONVERT(NVARCHAR(MAX), {expr})"),
        }
    }

    /// The catalog predicate restricting the probe to the connection's own schema/database. SQL
    /// Server's `INFORMATION_SCHEMA` view is already database-local.
    fn catalog_scope(self) -> &'static str {
        match self {
            SqlDialect::Postgres => "table_schema = CURRENT_SCHEMA() AND ",
            SqlDialect::Mysql => "table_schema = DATABASE() AND ",
            SqlDialect::Mssql => "",
        }
    }
}

// ---- value classes ----------------------------------------------------------------------

/// The marshalling class a declared [`Builtin`] falls into — the runtime half of the design's
/// advisory type mapping (§4.5), collapsed to the distinctions that change what goes on the wire.
///
/// | Class | Builtins | Round trip |
/// |---|---|---|
/// | [`Text`](ValueClass::Text) | `string`, `gYear`, `gYearMonth` | exact |
/// | [`Number`](ValueClass::Number) | `decimal` and the whole integer family | exact, **including the written scale** — the author's `NUMERIC(p,s)` decides it, never an `f64` |
/// | [`Boolean`](ValueClass::Boolean) | `boolean` | exact (each dialect's own rendering is normalised back to a JSON boolean) |
/// | [`Date`](ValueClass::Date) | `date` | exact (`yyyy-mm-dd` in all three dialects) |
/// | [`DateTime`](ValueClass::DateTime) | `dateTime` | **canonicalised**, not preserved verbatim: the column stores an instant, so the value read back is the dialect's rendering of it, normalised to ISO-8601 (`T` separator, `+HH:MM` offset). A written offset or a sub-second precision the column cannot hold does not survive |
/// | [`Time`](ValueClass::Time) | `time` | canonicalised to the column's precision |
///
/// `base64Binary` has no class: it is refused when the projected store is built, rather than
/// silently round-tripped through a lossy text rendering (see [`ProjectedStore::new`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueClass {
    /// Character data — bound and read as itself.
    Text,
    /// A numeric value, carried as its exact decimal literal.
    Number,
    /// A truth value.
    Boolean,
    /// A calendar date.
    Date,
    /// An instant.
    DateTime,
    /// A time of day.
    Time,
}

impl ValueClass {
    /// The class a declared builtin marshals through, or `None` for a builtin this phase does not
    /// project.
    pub fn of(builtin: Builtin) -> Option<ValueClass> {
        Some(match builtin {
            Builtin::String | Builtin::GYear | Builtin::GYearMonth => ValueClass::Text,
            Builtin::Boolean => ValueClass::Boolean,
            Builtin::Date => ValueClass::Date,
            Builtin::DateTime => ValueClass::DateTime,
            Builtin::Time => ValueClass::Time,
            Builtin::Decimal
            | Builtin::Integer
            | Builtin::NonNegativeInteger
            | Builtin::NonPositiveInteger
            | Builtin::PositiveInteger
            | Builtin::NegativeInteger
            | Builtin::Long
            | Builtin::Int
            | Builtin::Short
            | Builtin::Byte
            | Builtin::UnsignedLong
            | Builtin::UnsignedInt
            | Builtin::UnsignedShort
            | Builtin::UnsignedByte => ValueClass::Number,
            Builtin::Base64Binary => return None,
        })
    }
}

// ---- the live table, as the catalog reports it -------------------------------------------

/// One column of the LIVE table, as `information_schema` (or the dialect equivalent) reports it —
/// the input to [`ProjectedStore::verify`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActualColumn {
    /// The physical column name as stored by the catalog.
    pub name: String,
    /// Whether the column admits `NULL`.
    pub nullable: bool,
    /// Whether the column has a `DEFAULT`.
    pub has_default: bool,
}

// ---- the projected store ------------------------------------------------------------------

/// A store bound to its projection and its physical table — everything the dialect modules need
/// to serve a projected store, resolved once at plan time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedStore {
    store: String,
    table: String,
    projection: Projection,
    classes: Vec<ValueClass>,
}

impl ProjectedStore {
    /// Bind `projection` to the physical `table`, checking everything the SQL below then assumes.
    ///
    /// Fails closed when the table name is not a plain (optionally schema-qualified) SQL
    /// identifier — the name is interpolated into every statement, so it is validated here rather
    /// than quoted, exactly as the projection's column names already are — when a declared field
    /// claims one of the engine's [`CONTROL_COLUMNS`], or when a declared field's type has no
    /// runtime marshalling in this phase (`base64Binary`).
    pub fn new(
        store: &str,
        table: impl Into<String>,
        projection: Projection,
    ) -> Result<ProjectedStore, DataStoreError> {
        let table = table.into().trim().to_string();
        if !is_table_identifier(&table) {
            return Err(DataStoreError::new(format!(
                "data store '{store}' declares a structure but its table name '{table}' is not a \
                 SQL identifier. A projected store's table is named by its 'sql.table' property, \
                 or by the store name when that property is absent."
            )));
        }
        // A declared field claiming one of the engine's control columns is a naming fault, and
        // carries the same code the linter raises for it (design §4.3).
        let claimed: Vec<String> = projection
            .fields
            .iter()
            .filter(|field| {
                CONTROL_COLUMNS
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(&field.column))
            })
            .map(|field| format!("'{}' → '{}'", field.field, field.column))
            .collect();
        if !claimed.is_empty() {
            return Err(DataStoreError::with_code(
                crate::projection::codes::COLUMN_NAME_INVALID,
                format!(
                    "data store '{store}': structure type '{}' declares field(s) {} that resolve \
                     to a control column the engine owns ({}). Map them elsewhere under \
                     'columns:'.",
                    projection.type_name,
                    claimed.join(", "),
                    CONTROL_COLUMNS.join(", ")
                ),
            ));
        }
        let mut faults: Vec<String> = Vec::new();
        let mut classes = Vec::with_capacity(projection.fields.len());
        for field in &projection.fields {
            match ValueClass::of(field.builtin) {
                Some(class) => classes.push(class),
                None => {
                    classes.push(ValueClass::Text);
                    faults.push(format!(
                        "field '{}' is declared '{}', which a projected column does not carry in \
                         this release — declare it as a string, or remove the 'structure' block \
                         and keep the opaque store",
                        field.field,
                        field.builtin.name()
                    ));
                }
            }
        }
        if !faults.is_empty() {
            return Err(DataStoreError::new(format!(
                "data store '{store}' cannot project structure type '{}': {}",
                projection.type_name,
                faults.join("; ")
            )));
        }
        Ok(ProjectedStore {
            store: store.to_string(),
            table,
            projection,
            classes,
        })
    }

    /// The store's declared name.
    pub fn store(&self) -> &str {
        &self.store
    }

    /// The physical table the rows live in.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The declared projection.
    pub fn projection(&self) -> &Projection {
        &self.projection
    }

    fn fields(&self) -> impl Iterator<Item = (&ProjectedField, ValueClass)> {
        self.projection
            .fields
            .iter()
            .zip(self.classes.iter().copied())
    }

    // ---- statements. Bind order is ALWAYS statement-text order, so one binding sequence
    // ---- serves the positional dialects (`?`, `@Pn`) and the numbered one alike.

    /// `SELECT <declared columns> FROM <table> WHERE store_key = ?` — binds `[key]`, and returns
    /// the columns in declared order, so [`Self::row_to_json`] reads them positionally.
    pub fn select_sql(&self, dialect: SqlDialect) -> String {
        let columns: Vec<String> = self
            .fields()
            .map(|(f, class)| dialect.read_expr(class, &f.column))
            .collect();
        format!(
            "SELECT {} FROM {} WHERE {KEY_COLUMN} = {}",
            columns.join(", "),
            self.table,
            dialect.placeholder(1)
        )
    }

    /// `SELECT rev FROM <table> WHERE store_key = ?` — binds `[key]`.
    pub fn revision_sql(&self, dialect: SqlDialect) -> String {
        format!(
            "SELECT {REV_COLUMN} FROM {} WHERE {KEY_COLUMN} = {}",
            self.table,
            dialect.placeholder(1)
        )
    }

    /// The portable `SELECT … FOR UPDATE` substitute: a guaranteed non-no-op `UPDATE` that takes
    /// the row-exclusive lock to commit. Binds `[key]`; a no-op on an absent key.
    pub fn lock_sql(&self, dialect: SqlDialect) -> String {
        format!(
            "UPDATE {} SET {REV_COLUMN} = {REV_COLUMN} + 1 WHERE {KEY_COLUMN} = {}",
            self.table,
            dialect.placeholder(1)
        )
    }

    /// `DELETE FROM <table> WHERE store_key = ?` — binds `[key]`.
    pub fn delete_sql(&self, dialect: SqlDialect) -> String {
        format!(
            "DELETE FROM {} WHERE {KEY_COLUMN} = {}",
            self.table,
            dialect.placeholder(1)
        )
    }

    /// The upsert's `UPDATE` half — binds `[fields…, key]`. `rev`/`updated_at` move exactly as on
    /// the KV path.
    pub fn update_sql(&self, dialect: SqlDialect) -> String {
        format!(
            "UPDATE {} SET {} WHERE {KEY_COLUMN} = {}",
            self.table,
            self.set_clause(dialect),
            dialect.placeholder(self.projection.fields.len() + 1)
        )
    }

    /// The compare-and-set `UPDATE` — binds `[fields…, key, expected_rev]`. Zero rows affected is
    /// the detected conflict (a concurrent commit bumped `rev`, or the row is gone).
    pub fn update_if_revision_sql(&self, dialect: SqlDialect) -> String {
        let n = self.projection.fields.len();
        format!(
            "UPDATE {} SET {} WHERE {KEY_COLUMN} = {} AND {REV_COLUMN} = {}",
            self.table,
            self.set_clause(dialect),
            dialect.placeholder(n + 1),
            dialect.placeholder(n + 2)
        )
    }

    fn set_clause(&self, dialect: SqlDialect) -> String {
        let mut sets: Vec<String> = self
            .fields()
            .enumerate()
            .map(|(i, (f, class))| format!("{} = {}", f.column, dialect.write_expr(class, i + 1)))
            .collect();
        sets.push(format!("{REV_COLUMN} = {REV_COLUMN} + 1"));
        sets.push(format!("{UPDATED_AT_COLUMN} = {}", dialect.now()));
        sets.join(", ")
    }

    /// The upsert's `INSERT` half — binds `[key, fields…]`, seeding `rev` at 1.
    pub fn insert_sql(&self, dialect: SqlDialect) -> String {
        let columns: Vec<&str> = std::iter::once(KEY_COLUMN)
            .chain(self.projection.columns())
            .chain([REV_COLUMN, UPDATED_AT_COLUMN])
            .collect();
        let mut values = vec![dialect.placeholder(1)];
        values.extend(
            self.fields()
                .enumerate()
                .map(|(i, (_, class))| dialect.write_expr(class, i + 2)),
        );
        values.push("1".to_string());
        values.push(dialect.now().to_string());
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table,
            columns.join(", "),
            values.join(", ")
        )
    }

    /// The first-use catalog probe — binds `[table]`, and yields `(column_name, is_nullable,
    /// has_default)` as three strings per row. An empty result means the table is not visible on
    /// this connection at all.
    pub fn columns_probe_sql(&self, dialect: SqlDialect) -> String {
        format!(
            "SELECT {}, {}, {} FROM information_schema.columns WHERE {}LOWER(table_name) = \
             LOWER({})",
            dialect.as_text("column_name"),
            dialect.as_text("is_nullable"),
            dialect.as_text("CASE WHEN column_default IS NULL THEN 'N' ELSE 'Y' END"),
            dialect.catalog_scope(),
            dialect.placeholder(1)
        )
    }

    // ---- marshalling -----------------------------------------------------------------------

    /// The declared fields of `value`, in projected column order, as the text (or NULL) each
    /// column is bound with.
    ///
    /// Fails closed when `value` is not a record, and — the point of the whole design — when it
    /// carries a field the structure does not declare
    /// ([`UNDECLARED_FIELD`](codes::UNDECLARED_FIELD)): a projected row IS its declared scalars,
    /// so there is nowhere for an extra field to go and dropping it silently is the one outcome
    /// §4.2 refuses. An ABSENT declared field binds NULL — which is what makes the optional
    /// column's `NULL` and a missing key the same state, and why [`Self::row_to_json`] reads NULL
    /// back as an absent key rather than an explicit JSON null.
    pub fn bind_values(
        &self,
        key: &str,
        value: &Json,
    ) -> Result<Vec<Option<String>>, DataStoreError> {
        let Json::Object(record) = value else {
            return Err(DataStoreError::with_code(
                codes::VALUE_NOT_A_RECORD,
                format!(
                    "data store '{}'[{key}] declares structure '{}', so its value must be a \
                     record of the declared fields — got {}.",
                    self.store,
                    self.projection.type_name,
                    json_kind(value)
                ),
            ));
        };
        let undeclared: Vec<&str> = record
            .keys()
            .filter(|k| self.projection.field(k).is_none())
            .map(|k| k.as_str())
            .collect();
        if !undeclared.is_empty() {
            return Err(DataStoreError::with_code(
                codes::UNDECLARED_FIELD,
                format!(
                    "data store '{}'[{key}] was written with field(s) [{}], which structure type \
                     '{}' does not declare (declared: [{}]). A projected store's row IS its \
                     declared scalars — declare the field and ship the column in a migration, or \
                     remove the 'structure' block and keep the opaque store.",
                    self.store,
                    undeclared.join(", "),
                    self.projection.type_name,
                    self.projection
                        .fields
                        .iter()
                        .map(|f| f.field.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        let mut cells = Vec::with_capacity(self.projection.fields.len());
        for (field, class) in self.fields() {
            cells.push(self.cell_for(key, field, class, record.get(&field.field))?);
        }
        Ok(cells)
    }

    fn cell_for(
        &self,
        key: &str,
        field: &ProjectedField,
        class: ValueClass,
        value: Option<&Json>,
    ) -> Result<Option<String>, DataStoreError> {
        let value = match value {
            None | Some(Json::Null) => return Ok(None),
            Some(v) => v,
        };
        let text = match (class, value) {
            (ValueClass::Text, Json::String(s)) => s.clone(),
            (ValueClass::Text, Json::Number(n)) => n.to_string(),
            (ValueClass::Text, Json::Bool(b)) => b.to_string(),
            // The exact decimal literal, never an f64 round trip: `serde_json`'s
            // `arbitrary_precision` keeps the written form ("12.50" stays "12.50"), and the
            // author's NUMERIC(p,s) decides the stored scale.
            (ValueClass::Number, Json::Number(n)) => n.to_string(),
            (ValueClass::Number, Json::String(s)) => s.clone(),
            // Every dialect accepts 1/0 for its own truth type (PostgreSQL BOOLEAN, MySQL
            // TINYINT(1), SQL Server BIT); "true"/"false" is not portable to the integer ones.
            (ValueClass::Boolean, Json::Bool(b)) => bool_literal(*b),
            (ValueClass::Boolean, Json::Number(n)) => bool_literal(n.to_string() != "0"),
            (ValueClass::Boolean, Json::String(s)) => match parse_bool(s) {
                Some(b) => bool_literal(b),
                None => return Err(self.value_err(key, field, value)),
            },
            (ValueClass::Date | ValueClass::DateTime | ValueClass::Time, Json::String(s)) => {
                s.clone()
            }
            _ => return Err(self.value_err(key, field, value)),
        };
        Ok(Some(text))
    }

    fn value_err(&self, key: &str, field: &ProjectedField, value: &Json) -> DataStoreError {
        DataStoreError::new(format!(
            "data store '{}'[{key}] field '{}' is declared '{}' but was written as {} — a \
             projected column cannot hold it.",
            self.store,
            field.field,
            field.builtin.name(),
            json_kind(value)
        ))
    }

    /// Rebuild the stored record from one row of [`Self::select_sql`], positionally.
    ///
    /// A NULL cell yields an ABSENT key rather than an explicit JSON null: NULL is how an
    /// optional field's absence is stored, and absence is what the caller wrote. (A value written
    /// with an explicit `null` for an optional field therefore reads back with that key omitted —
    /// the single normalisation a projected store applies, and one FEEL does not distinguish.)
    pub fn row_to_json(&self, cells: &[Option<String>]) -> Json {
        let mut record = Map::new();
        for ((field, class), cell) in self.fields().zip(cells.iter()) {
            let Some(text) = cell else { continue };
            record.insert(field.field.clone(), decode(class, text));
        }
        Json::Object(record)
    }

    // ---- first-use verification ------------------------------------------------------------

    /// Check the LIVE table against the projection (design §4.6, RULED fail-closed).
    ///
    /// Lint proves the package-time case from the package's own migrations; this proves the
    /// deployed case, and catches the table that DRIFTED from them — a hand-applied
    /// `ALTER TABLE … DROP COLUMN` / `… SET NOT NULL` / `… ADD COLUMN x NOT NULL`. Every fault is
    /// reported, not the first, and each names its column.
    ///
    /// Type compatibility is deliberately NOT checked here: the catalog's type names are three
    /// different vocabularies and comparing them charitably enough to avoid false refusals is
    /// lint's job (`COLUMN_TYPE_MISMATCH`, from the package's own DDL). What this checks is
    /// structural satisfiability — is there a column to write, and can the write succeed.
    pub fn verify(&self, actual: &[ActualColumn]) -> Result<(), DataStoreError> {
        if actual.is_empty() {
            return Err(DataStoreError::with_code(
                codes::PROJECTION_UNSATISFIABLE,
                format!(
                    "data store '{}' declares structure '{}', but table '{}' has no columns \
                     visible on this connection — the table does not exist, or it lives in a \
                     schema this connection does not search. Ship it in \
                     migrations/{}/, or point 'sql.table' at the right table.",
                    self.store, self.projection.type_name, self.table, self.store
                ),
            ));
        }
        let find = |name: &str| actual.iter().find(|c| c.name.eq_ignore_ascii_case(name));
        let mut faults: Vec<String> = Vec::new();
        for control in CONTROL_COLUMNS {
            if find(control).is_none() {
                faults.push(format!(
                    "column '{control}' is missing (a projected table carries store_key, rev and \
                     updated_at alongside the declared columns)"
                ));
            }
        }
        for field in &self.projection.fields {
            match find(&field.column) {
                None => faults.push(format!(
                    "column '{}' (declared field '{}') is missing",
                    field.column, field.field
                )),
                Some(column) if field.nullable && !column.nullable => faults.push(format!(
                    "column '{}' is NOT NULL, but declared field '{}' is optional — an absent \
                     value has no way to be stored",
                    field.column, field.field
                )),
                Some(_) => {}
            }
        }
        for column in actual {
            let control = CONTROL_COLUMNS
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&column.name));
            if control || self.projection.by_column(&column.name).is_some() {
                continue;
            }
            if !column.nullable && !column.has_default {
                faults.push(format!(
                    "column '{}' is NOT NULL with no DEFAULT, and structure '{}' declares no \
                     field for it — every insert would fail",
                    column.name, self.projection.type_name
                ));
            }
        }
        if faults.is_empty() {
            return Ok(());
        }
        Err(DataStoreError::with_code(
            codes::PROJECTION_UNSATISFIABLE,
            format!(
                "data store '{}' cannot be served: table '{}' does not satisfy structure '{}' — \
                 {}. The deployed table has drifted from the package's migrations; re-apply them \
                 (or ship the ALTER) before the store can be used.",
                self.store,
                self.table,
                self.projection.type_name,
                faults.join("; ")
            ),
        ))
    }
}

// ---- free helpers ---------------------------------------------------------------------------

/// The physical table a store's rows live in: its `sql.table` property when set, else the store's
/// own name. (Lint applies the same two rungs, plus a third — the single table the package's
/// migrations create — which needs the migration SQL the runtime does not parse.)
pub fn table_for(def: &crate::config::StoreDefinition) -> String {
    def.properties
        .get("sql.table")
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .unwrap_or(def.name.as_str())
        .to_string()
}

/// A plain SQL identifier, optionally schema-qualified (`schema.table`). The table name is
/// interpolated into every statement, so this is the injection boundary.
fn is_table_identifier(table: &str) -> bool {
    !table.is_empty()
        && table.split('.').count() <= 2
        && table.split('.').all(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
}

fn bool_literal(b: bool) -> String {
    if b { "1" } else { "0" }.to_string()
}

fn parse_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Some(true),
        "false" | "f" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}

/// One cell's canonical text, back to the JSON shape the caller wrote.
fn decode(class: ValueClass, text: &str) -> Json {
    match class {
        ValueClass::Text | ValueClass::Date | ValueClass::Time => Json::String(text.to_string()),
        // `arbitrary_precision` keeps the DB's rendering verbatim, so a NUMERIC(18,2)'s "12.50"
        // survives as "12.50" rather than collapsing to 12.5.
        ValueClass::Number => match serde_json::from_str::<Json>(text) {
            Ok(v @ Json::Number(_)) => v,
            _ => Json::String(text.to_string()),
        },
        ValueClass::Boolean => match parse_bool(text) {
            Some(b) => Json::Bool(b),
            None => Json::String(text.to_string()),
        },
        ValueClass::DateTime => Json::String(canonical_datetime(text)),
    }
}

/// Normalise a dialect's date-time rendering toward ISO-8601: `' '` → `'T'` (MySQL and
/// PostgreSQL both use a space) and a bare hour offset → `+HH:MM` (PostgreSQL renders `+00`).
fn canonical_datetime(text: &str) -> String {
    let mut out = match text.find(' ') {
        Some(i) => {
            let mut s = text.to_string();
            s.replace_range(i..i + 1, "T");
            s
        }
        None => text.to_string(),
    };
    let tail = out.len().saturating_sub(3);
    let is_bare_offset = out.len() >= 3
        && out[tail..].starts_with(['+', '-'])
        && out[tail + 1..].chars().all(|c| c.is_ascii_digit())
        && !out[..tail].ends_with('T');
    if is_bare_offset {
        out.push_str(":00");
    }
    out
}

fn json_kind(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "a boolean",
        Json::Number(_) => "a number",
        Json::String(_) => "a string",
        Json::Array(_) => "an array",
        Json::Object(_) => "a record",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use sutra_xsd::{FieldDecl, FieldFacets, FieldShape};

    use super::*;
    use crate::projection::{NamingRules, Projection};

    fn scalar(name: &str, builtin: Builtin, optional: bool) -> FieldDecl {
        FieldDecl {
            name: name.to_string(),
            is_attribute: false,
            occurs_min: if optional { 0 } else { 1 },
            occurs_max: Some(1),
            in_choice: false,
            shape: FieldShape::Scalar {
                builtin,
                facets: FieldFacets::default(),
            },
        }
    }

    /// The showcase record every SQL-shape assertion below is written against: one field of each
    /// marshalling class, the last one optional.
    fn account_fields() -> Vec<FieldDecl> {
        vec![
            scalar("accountId", Builtin::String, false),
            scalar("balance", Builtin::Decimal, false),
            scalar("version", Builtin::Int, false),
            scalar("frozen", Builtin::Boolean, false),
            scalar("openedAt", Builtin::Date, true),
        ]
    }

    fn accounts() -> ProjectedStore {
        let projection = Projection::derive(
            "AccountRecord",
            &account_fields(),
            &BTreeMap::new(),
            NamingRules::default(),
        )
        .expect("flat");
        ProjectedStore::new("accounts", "accounts", projection).expect("projectable")
    }

    #[test]
    fn postgres_statements_carry_numbered_placeholders_and_write_casts() {
        let store = accounts();
        assert_eq!(
            store.select_sql(SqlDialect::Postgres),
            "SELECT CAST(account_id AS TEXT), CAST(balance AS TEXT), CAST(version AS TEXT), \
             CAST(frozen AS TEXT), CAST(opened_at AS TEXT) FROM accounts WHERE store_key = $1"
        );
        assert_eq!(
            store.update_sql(SqlDialect::Postgres),
            "UPDATE accounts SET account_id = $1, balance = CAST($2 AS NUMERIC), \
             version = CAST($3 AS NUMERIC), frozen = CAST($4 AS BOOLEAN), \
             opened_at = CAST($5 AS DATE), rev = rev + 1, updated_at = CURRENT_TIMESTAMP \
             WHERE store_key = $6"
        );
        assert_eq!(
            store.insert_sql(SqlDialect::Postgres),
            "INSERT INTO accounts (store_key, account_id, balance, version, frozen, opened_at, \
             rev, updated_at) VALUES ($1, $2, CAST($3 AS NUMERIC), CAST($4 AS NUMERIC), \
             CAST($5 AS BOOLEAN), CAST($6 AS DATE), 1, CURRENT_TIMESTAMP)"
        );
        assert_eq!(
            store.update_if_revision_sql(SqlDialect::Postgres),
            "UPDATE accounts SET account_id = $1, balance = CAST($2 AS NUMERIC), \
             version = CAST($3 AS NUMERIC), frozen = CAST($4 AS BOOLEAN), \
             opened_at = CAST($5 AS DATE), rev = rev + 1, updated_at = CURRENT_TIMESTAMP \
             WHERE store_key = $6 AND rev = $7"
        );
        assert_eq!(
            store.lock_sql(SqlDialect::Postgres),
            "UPDATE accounts SET rev = rev + 1 WHERE store_key = $1"
        );
        assert_eq!(
            store.delete_sql(SqlDialect::Postgres),
            "DELETE FROM accounts WHERE store_key = $1"
        );
        assert_eq!(
            store.revision_sql(SqlDialect::Postgres),
            "SELECT rev FROM accounts WHERE store_key = $1"
        );
    }

    #[test]
    fn mysql_statements_carry_positional_placeholders_and_no_write_cast() {
        let store = accounts();
        assert_eq!(
            store.select_sql(SqlDialect::Mysql),
            "SELECT CAST(account_id AS CHAR), CAST(balance AS CHAR), CAST(version AS CHAR), \
             CAST(frozen AS CHAR), CAST(opened_at AS CHAR) FROM accounts WHERE store_key = ?"
        );
        assert_eq!(
            store.update_sql(SqlDialect::Mysql),
            "UPDATE accounts SET account_id = ?, balance = ?, version = ?, frozen = ?, \
             opened_at = ?, rev = rev + 1, updated_at = CURRENT_TIMESTAMP WHERE store_key = ?"
        );
        assert_eq!(
            store.insert_sql(SqlDialect::Mysql),
            "INSERT INTO accounts (store_key, account_id, balance, version, frozen, opened_at, \
             rev, updated_at) VALUES (?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP)"
        );
        assert_eq!(
            store.update_if_revision_sql(SqlDialect::Mysql),
            "UPDATE accounts SET account_id = ?, balance = ?, version = ?, frozen = ?, \
             opened_at = ?, rev = rev + 1, updated_at = CURRENT_TIMESTAMP WHERE store_key = ? \
             AND rev = ?"
        );
    }

    #[test]
    fn mssql_statements_carry_at_p_placeholders_and_iso_date_conversions() {
        let store = accounts();
        assert_eq!(
            store.select_sql(SqlDialect::Mssql),
            "SELECT CONVERT(NVARCHAR(MAX), account_id), CONVERT(NVARCHAR(MAX), balance), \
             CONVERT(NVARCHAR(MAX), version), CONVERT(NVARCHAR(MAX), frozen), \
             CONVERT(NVARCHAR(MAX), opened_at, 23) FROM accounts WHERE store_key = @P1"
        );
        assert_eq!(
            store.update_sql(SqlDialect::Mssql),
            "UPDATE accounts SET account_id = @P1, balance = @P2, version = @P3, frozen = @P4, \
             opened_at = @P5, rev = rev + 1, updated_at = SYSUTCDATETIME() WHERE store_key = @P6"
        );
        assert_eq!(
            store.insert_sql(SqlDialect::Mssql),
            "INSERT INTO accounts (store_key, account_id, balance, version, frozen, opened_at, \
             rev, updated_at) VALUES (@P1, @P2, @P3, @P4, @P5, @P6, 1, SYSUTCDATETIME())"
        );
    }

    #[test]
    fn the_catalog_probe_is_scoped_per_dialect() {
        let store = accounts();
        assert_eq!(
            store.columns_probe_sql(SqlDialect::Postgres),
            "SELECT CAST(column_name AS TEXT), CAST(is_nullable AS TEXT), \
             CAST(CASE WHEN column_default IS NULL THEN 'N' ELSE 'Y' END AS TEXT) \
             FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA() AND \
             LOWER(table_name) = LOWER($1)"
        );
        assert!(store
            .columns_probe_sql(SqlDialect::Mysql)
            .contains("table_schema = DATABASE() AND LOWER(table_name) = LOWER(?)"));
        assert!(store
            .columns_probe_sql(SqlDialect::Mssql)
            .contains("WHERE LOWER(table_name) = LOWER(@P1)"));
    }

    #[test]
    fn an_undeclared_field_fails_the_write_closed() {
        let store = accounts();
        let err = store
            .bind_values(
                "k1",
                &json!({"accountId": "A1", "balance": 1, "version": 1, "frozen": false,
                        "nickname": "rainy day"}),
            )
            .expect_err("undeclared field must not be dropped");
        assert_eq!(err.code(), Some(codes::UNDECLARED_FIELD));
        assert!(err.to_string().contains("nickname"), "{err}");
        assert!(err.to_string().contains("AccountRecord"), "{err}");
    }

    #[test]
    fn a_non_record_value_fails_the_write_closed() {
        let store = accounts();
        for value in [json!(42), json!("x"), json!([1, 2]), json!(null)] {
            let err = store.bind_values("k1", &value).expect_err("not a record");
            assert_eq!(err.code(), Some(codes::VALUE_NOT_A_RECORD));
        }
    }

    #[test]
    fn binds_are_declared_order_text_and_an_absent_optional_is_null() {
        let store = accounts();
        let value: Json = serde_json::from_str(
            r#"{"accountId":"ACC-1","balance":12.50,"version":7,"frozen":true}"#,
        )
        .unwrap();
        assert_eq!(
            store.bind_values("k1", &value).unwrap(),
            vec![
                Some("ACC-1".to_string()),
                Some("12.50".to_string()), // the WRITTEN scale, not 12.5
                Some("7".to_string()),
                Some("1".to_string()), // portable truth literal
                None,                  // absent optional → NULL
            ]
        );
    }

    #[test]
    fn a_row_decodes_back_to_the_written_record_including_the_decimal_scale() {
        let store = accounts();
        let written: Json = serde_json::from_str(
            r#"{"accountId":"ACC-1","balance":12.50,"version":7,"frozen":true}"#,
        )
        .unwrap();
        // What each dialect hands back for those binds (PostgreSQL renders BOOLEAN as "true";
        // MySQL/SQL Server render their integer truth types as "1" — both decode to `true`).
        for truth in ["true", "1"] {
            let row = [
                Some("ACC-1".to_string()),
                Some("12.50".to_string()),
                Some("7".to_string()),
                Some(truth.to_string()),
                None,
            ];
            let read = store.row_to_json(&row);
            assert_eq!(read, written);
            assert_eq!(read.to_string(), written.to_string(), "byte-equal");
            assert_eq!(read["balance"].to_string(), "12.50", "the written scale");
        }
    }

    #[test]
    fn an_explicit_null_and_an_absent_field_store_and_read_alike() {
        let store = accounts();
        let explicit = json!({"accountId":"A","balance":1,"version":1,"frozen":false,
                              "openedAt":null});
        let absent = json!({"accountId":"A","balance":1,"version":1,"frozen":false});
        assert_eq!(
            store.bind_values("k", &explicit).unwrap(),
            store.bind_values("k", &absent).unwrap()
        );
        let row = store.bind_values("k", &explicit).unwrap();
        assert_eq!(store.row_to_json(&row), absent);
    }

    #[test]
    fn a_date_round_trips_and_a_datetime_is_canonicalised() {
        assert_eq!(decode(ValueClass::Date, "2026-08-04"), json!("2026-08-04"));
        // MySQL's space separator and PostgreSQL's bare hour offset both normalise to ISO-8601.
        assert_eq!(
            decode(ValueClass::DateTime, "2026-08-04 12:00:00"),
            json!("2026-08-04T12:00:00")
        );
        assert_eq!(
            decode(ValueClass::DateTime, "2026-08-04 12:00:00+00"),
            json!("2026-08-04T12:00:00+00:00")
        );
        assert_eq!(
            decode(ValueClass::DateTime, "2026-08-04T12:00:00.123"),
            json!("2026-08-04T12:00:00.123")
        );
    }

    #[test]
    fn a_high_precision_decimal_survives_beyond_f64() {
        let exact = "0.12345678901234567890123";
        assert_eq!(decode(ValueClass::Number, exact).to_string(), exact);
    }

    #[test]
    fn verification_accepts_a_satisfying_table() {
        let store = accounts();
        let mut actual = control_columns();
        actual.extend([
            column("account_id", false, false),
            column("balance", false, false),
            column("version", false, false),
            column("frozen", false, false),
            column("opened_at", true, false),
        ]);
        store.verify(&actual).expect("satisfiable");
        // Case-insensitively, as unquoted SQL identifiers are (SQL Server's catalog preserves
        // the authored case).
        let upper: Vec<ActualColumn> = actual
            .iter()
            .map(|c| ActualColumn {
                name: c.name.to_ascii_uppercase(),
                ..c.clone()
            })
            .collect();
        store.verify(&upper).expect("case-insensitive");
    }

    #[test]
    fn verification_reports_every_drift_fault_at_once() {
        let store = accounts();
        let mut actual = control_columns();
        actual.extend([
            column("account_id", false, false),
            // `balance` dropped by a hand-applied ALTER
            column("version", false, false),
            column("frozen", false, false),
            column("opened_at", false, false), // SET NOT NULL on an optional field
            column("legacy_note", false, false), // an unmapped, mandatory column
            column("audited_by", false, true), // unmapped but defaulted — fine
        ]);
        let err = store.verify(&actual).expect_err("drift must fail closed");
        assert_eq!(err.code(), Some(codes::PROJECTION_UNSATISFIABLE));
        let message = err.to_string();
        assert!(message.contains("'balance'"), "{message}");
        assert!(message.contains("'opened_at' is NOT NULL"), "{message}");
        assert!(message.contains("'legacy_note'"), "{message}");
        assert!(!message.contains("audited_by"), "{message}");
    }

    #[test]
    fn verification_refuses_a_table_that_is_not_there() {
        let err = accounts().verify(&[]).expect_err("absent table");
        assert_eq!(err.code(), Some(codes::PROJECTION_UNSATISFIABLE));
        assert!(err.to_string().contains("no columns visible"));
    }

    #[test]
    fn verification_requires_the_control_columns() {
        let store = accounts();
        let actual = vec![
            column("account_id", false, false),
            column("balance", false, false),
            column("version", false, false),
            column("frozen", false, false),
            column("opened_at", true, false),
        ];
        let err = store.verify(&actual).expect_err("no control columns");
        for control in CONTROL_COLUMNS {
            assert!(err.to_string().contains(control), "{err}");
        }
    }

    #[test]
    fn a_field_claiming_a_control_column_is_refused() {
        let fields = vec![scalar("rev", Builtin::Int, false)];
        let projection =
            Projection::derive("Bad", &fields, &BTreeMap::new(), NamingRules::default()).unwrap();
        let err = ProjectedStore::new("s", "t", projection).expect_err("control-column clash");
        assert_eq!(
            err.code(),
            Some(crate::projection::codes::COLUMN_NAME_INVALID)
        );
        assert!(err.to_string().contains("control column"), "{err}");
    }

    #[test]
    fn a_binary_field_is_refused_rather_than_silently_mangled() {
        let fields = vec![scalar("attachment", Builtin::Base64Binary, false)];
        let projection =
            Projection::derive("Bad", &fields, &BTreeMap::new(), NamingRules::default()).unwrap();
        let err = ProjectedStore::new("s", "t", projection).expect_err("binary unsupported");
        assert!(err.to_string().contains("base64Binary"), "{err}");
    }

    #[test]
    fn a_table_name_that_is_not_an_identifier_is_refused() {
        let projection = Projection::derive(
            "AccountRecord",
            &account_fields(),
            &BTreeMap::new(),
            NamingRules::default(),
        )
        .unwrap();
        for bad in [
            "accounts; DROP TABLE x",
            "1accounts",
            "",
            "a.b.c",
            "acc-ounts",
        ] {
            assert!(
                ProjectedStore::new("s", bad, projection.clone()).is_err(),
                "{bad} must be refused"
            );
        }
        // A schema-qualified name is legitimate.
        assert!(ProjectedStore::new("s", "app.accounts", projection).is_ok());
    }

    #[test]
    fn the_table_defaults_to_the_store_name_and_sql_table_overrides_it() {
        let mut def = crate::config::StoreDefinition {
            name: "accounts".into(),
            store_type: "sql".into(),
            properties: BTreeMap::new(),
            structure: None,
        };
        assert_eq!(table_for(&def), "accounts");
        def.properties
            .insert("sql.table".into(), " account_rows ".into());
        assert_eq!(table_for(&def), "account_rows");
    }

    fn column(name: &str, nullable: bool, has_default: bool) -> ActualColumn {
        ActualColumn {
            name: name.to_string(),
            nullable,
            has_default,
        }
    }

    fn control_columns() -> Vec<ActualColumn> {
        vec![
            column(KEY_COLUMN, false, false),
            column(REV_COLUMN, false, true),
            column(UPDATED_AT_COLUMN, false, true),
        ]
    }
}
