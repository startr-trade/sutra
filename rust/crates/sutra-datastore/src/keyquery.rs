//! Key queries on a key/value (`data_store`) or typed (projected) store — the read behind
//! `<q:store query=…>`.
//!
//! A query selects the rows of ONE store whose key starts with a prefix or lies in a half-open
//! range `[lower, upper)`, keys compared **bytewise** (UTF-8 code-point order). It returns up to
//! `row_limit` rows in ascending key order (with a continuation key when more remain) and any
//! number of aggregates — a row count and, optionally, the exact decimal sum of one value
//! field, each optionally grouped by another value field. Aggregates always cover every
//! matching row: they are computed by the database, independent of the page.
//!
//! Everything here is pure (statement text, bounds, collation rules), so it compiles in the
//! `--no-default-features` build beside [`crate::projected`]; the three dialect modules execute
//! the statements.
//!
//! **Index use.** The predicate is `store_name = ? AND store_key >= ? AND store_key < ?`, which
//! an index leading with `(store_name, store_key)` — the cutover table's primary key — serves as
//! one range scan. That only means bytewise order when the key column's collation IS bytewise.
//! So before a store's first query the dialects verify, once, that the key column's collation is
//! bytewise ([`key_order_probe_sql`]) and that such an index exists ([`key_index_probe_sql`]),
//! and refuse otherwise: a linguistic collation would make the range wrong, and a missing index
//! (a table keyed `(tenant_id, module_id, module_version, store_name, store_key)`, say) would
//! make every query a full scan.
//!
//! **Numbers.** A summed field counts as numeric when it is a JSON number or a string holding a
//! decimal literal (`"30.05"`, `"-1e3"`); a missing field or a JSON `null` is skipped (the row is
//! still counted); any other value — text, a boolean, an object — is counted in
//! [`KeyAggregateCell::non_numeric`], which the engine turns into a failure with its own code.
//!
//! **Typed stores.** A projected (typed-column) store — one whose declaration carries a
//! `structure` — is queried the same way over its OWN table, which has no `store_name` column
//! (the table is the store): the predicate is `store_key >= ? AND store_key < ?`, served by an
//! index leading with `store_key` (the table's primary key). Rows come back as the typed record,
//! exactly as a typed get returns it. A `sum`/`groupBy` path names ONE declared field (its
//! column, via the projection's naming) — a nested path or an undeclared field is refused with
//! [`codes::QUERY_UNSUPPORTED`]. The aggregates are plain SQL aggregates over the column: a
//! numeric column sums exactly; a text column sums the rows holding a decimal literal and counts
//! the rest as non-numeric (as the key/value path does); any other column type counts every
//! present value as non-numeric. A NULL column (an absent optional field) is skipped by a sum and
//! groups under `"null"`, exactly as a missing field does on the key/value path. Group keys are
//! the column's value rendered as the typed get renders it (`true`/`false` for a boolean, the
//! decimal text for a number, ISO-8601 for a date-time).

use std::collections::BTreeMap;

use serde_json::Value as Json;

use crate::error::DataStoreError;
use crate::projected::{ProjectedStore, SqlDialect, ValueClass};
use crate::projection::ProjectedField;

/// Stable codes for key-query contract failures (rendered as the `[CODE]` prefix of the error).
pub mod codes {
    /// The store's key column does not order keys bytewise, so a key range would be neither
    /// correct nor index-backed. The message names the column's collation and the fix.
    pub const QUERY_KEY_ORDER: &str = "SUTRA.RUNTIME.DATASTORE.QUERY_KEY_ORDER";
    /// No index of the store's table leads with `(store_name, store_key)`, so a key range could
    /// only be served by scanning the whole table. The message names the fix.
    pub const QUERY_KEY_INDEX: &str = "SUTRA.RUNTIME.DATASTORE.QUERY_KEY_INDEX";
    /// The query is malformed for its store: a value path that is not one, a limit above the
    /// maximum, or — on a projected (typed-column) store — a `sum`/`groupBy` path that does not
    /// name exactly one declared field.
    pub const QUERY_UNSUPPORTED: &str = "SUTRA.RUNTIME.DATASTORE.QUERY_UNSUPPORTED";
}

/// The most rows one query page may return.
pub const MAX_ROW_LIMIT: u32 = 10_000;

/// One key query against a key/value store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRangeQuery {
    /// Inclusive lower bound of the key range (for a prefix query, the prefix itself).
    pub lower: String,
    /// Exclusive upper bound; `None` = no upper bound (see [`prefix_upper_bound`]).
    pub upper: Option<String>,
    /// Return only rows whose key is greater than this (the previous page's `next`).
    pub after: Option<String>,
    /// `Some(n)`: return at most `n` rows; `None`: return no rows (aggregates only).
    pub row_limit: Option<u32>,
    /// The aggregates, in the order the result reports them.
    pub aggregates: Vec<KeyQueryAggregate>,
}

/// One aggregate of a [`KeyRangeQuery`]: always a row count, plus the sum of `sum_field` when
/// set; grouped by `group_by` when set. Paths are the segments of a dotted value path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyQueryAggregate {
    pub sum_field: Option<Vec<String>>,
    pub group_by: Option<Vec<String>>,
}

/// The result of a [`KeyRangeQuery`].
#[derive(Debug, Clone, PartialEq)]
pub struct KeyQueryPage {
    /// `(key, value)` in ascending key order.
    pub rows: Vec<(String, Json)>,
    /// One entry per requested aggregate, in request order.
    pub aggregates: Vec<KeyAggregateResult>,
    /// Whether more matching rows follow this page.
    pub truncated: bool,
    /// The key to pass as `after` for the next page (`Some` exactly when `truncated`).
    pub next: Option<String>,
}

/// One aggregate's value: a single cell, or a cell per group (keyed by the group field's text;
/// a row without the field groups under `"null"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyAggregateResult {
    Total(KeyAggregateCell),
    Grouped(BTreeMap<String, KeyAggregateCell>),
}

/// The count and (for a sum) the exact decimal sum of one aggregate cell.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyAggregateCell {
    /// Matching rows.
    pub count: i64,
    /// The decimal sum as text, `Some` for a sum aggregate (`"0"` when nothing was summed).
    pub sum: Option<String>,
    /// Rows whose summed field is present but not numeric.
    pub non_numeric: i64,
}

/// The exclusive upper bound of the keys starting with `prefix`: the prefix with its last
/// character replaced by the next code point (dropping trailing characters that have none).
/// `None` when every character is U+10FFFF — no bound exists and the query is open-ended.
/// Bytewise UTF-8 order is code-point order, so this bound is exact.
pub fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let mut next = last as u32 + 1;
        if (0xD800..=0xDFFF).contains(&next) {
            next = 0xE000;
        }
        if let Some(c) = char::from_u32(next) {
            chars.push(c);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// Whether `path` segments are safe to embed in a JSON path (letters, digits, `_`, `-`). The
/// loader already refuses anything else; this is the store's own guard, because the path is part
/// of the statement text rather than a bound parameter.
fn path_is_safe(path: &[String]) -> bool {
    !path.is_empty()
        && path.iter().all(|s| {
            !s.is_empty()
                && s.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        })
}

/// Refuse a query no statement should be built for.
pub fn validate(store: &str, query: &KeyRangeQuery) -> Result<(), DataStoreError> {
    if let Some(limit) = query.row_limit {
        if limit == 0 || limit > MAX_ROW_LIMIT {
            return Err(DataStoreError::with_code(
                codes::QUERY_UNSUPPORTED,
                format!(
                    "key query on store '{store}': a row limit of {limit} is outside 1..={MAX_ROW_LIMIT}"
                ),
            ));
        }
    }
    for agg in &query.aggregates {
        for path in agg.sum_field.iter().chain(agg.group_by.iter()) {
            if !path_is_safe(path) {
                return Err(DataStoreError::with_code(
                    codes::QUERY_UNSUPPORTED,
                    format!(
                        "key query on store '{store}': '{}' is not a value path",
                        path.join(".")
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The WHERE clause and the number of the next free placeholder. Binds, in order: store name,
/// lower, upper (when present), after (when `with_after` and present).
fn where_clause(dialect: SqlDialect, query: &KeyRangeQuery, with_after: bool) -> String {
    let mut n = 1;
    let mut next = || {
        let p = dialect.placeholder(n);
        n += 1;
        p
    };
    let mut sql = format!(" WHERE store_name = {} AND store_key >= {}", next(), next());
    if query.upper.is_some() {
        sql.push_str(&format!(" AND store_key < {}", next()));
    }
    if with_after && query.after.is_some() {
        sql.push_str(&format!(" AND store_key > {}", next()));
    }
    sql
}

/// The bind values for [`rows_sql`] (`with_after = true`) or [`aggregate_sql`] (`false`), in
/// placeholder order.
pub fn binds<'a>(store: &'a str, query: &'a KeyRangeQuery, with_after: bool) -> Vec<&'a str> {
    let mut out = vec![store, query.lower.as_str()];
    if let Some(upper) = &query.upper {
        out.push(upper);
    }
    if with_after {
        if let Some(after) = &query.after {
            out.push(after);
        }
    }
    out
}

/// The page statement: `(store_key, store_value)` in key order, one row more than the limit so
/// the caller can tell whether the page is the last ([`finish_page`]).
pub fn rows_sql(dialect: SqlDialect, query: &KeyRangeQuery, limit: u32) -> String {
    let fetch = u64::from(limit) + 1;
    let filter = where_clause(dialect, query, true);
    match dialect {
        SqlDialect::Mssql => format!(
            "SELECT TOP ({fetch}) store_key, store_value FROM data_store{filter} ORDER BY store_key"
        ),
        SqlDialect::Postgres => format!(
            "SELECT store_key, store_value FROM data_store{filter} ORDER BY store_key LIMIT {fetch}"
        ),
        // A `…_bin` key column is reported with the binary flag, which the driver reads as
        // VARBINARY; the key is converted to text for the result only (the predicate and the
        // ORDER BY stay on the column, so the index still serves them).
        SqlDialect::Mysql => format!(
            "SELECT CONVERT(store_key USING utf8mb4), store_value FROM data_store{filter} \
             ORDER BY store_key LIMIT {fetch}"
        ),
    }
}

/// Split the `limit + 1` fetched rows into the page, `truncated` and `next`.
pub fn finish_page(
    mut rows: Vec<(String, Json)>,
    limit: u32,
) -> (Vec<(String, Json)>, bool, Option<String>) {
    let truncated = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next = if truncated {
        rows.last().map(|(k, _)| k.clone())
    } else {
        None
    };
    (rows, truncated, next)
}

/// A decimal literal, optionally signed, with an optional exponent and surrounding spaces —
/// written with POSIX classes only so PostgreSQL and MySQL read it identically.
const NUMERIC_TEXT: &str =
    "^[[:space:]]*[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?[[:space:]]*$";

/// The JSON-path text of a value path for MySQL / SQL Server (`$."a"."b"`).
fn json_path(path: &[String]) -> String {
    let mut out = String::from("$");
    for segment in path {
        out.push_str(&format!(".\"{segment}\""));
    }
    out
}

/// `(numeric value or NULL, 1 when present-but-not-numeric else 0)` for one summed field.
fn sum_exprs(dialect: SqlDialect, path: &[String]) -> (String, String) {
    match dialect {
        SqlDialect::Postgres => {
            let x = format!("(CAST(store_value AS jsonb) #> '{{{}}}')", path.join(","));
            let num = format!(
                "CASE WHEN jsonb_typeof({x}) = 'number' THEN CAST({x} #>> '{{}}' AS NUMERIC) \
                 WHEN jsonb_typeof({x}) = 'string' AND ({x} #>> '{{}}') ~ '{NUMERIC_TEXT}' \
                 THEN CAST(btrim({x} #>> '{{}}') AS NUMERIC) END"
            );
            let bad = format!(
                "CASE WHEN {x} IS NOT NULL AND jsonb_typeof({x}) <> 'null' AND ({num}) IS NULL \
                 THEN 1 ELSE 0 END"
            );
            (num, bad)
        }
        SqlDialect::Mysql => {
            let x = format!("JSON_EXTRACT(store_value, '{}')", json_path(path));
            let num = format!(
                "CASE WHEN JSON_TYPE({x}) IN ('INTEGER', 'UNSIGNED INTEGER', 'DOUBLE', 'DECIMAL') \
                 THEN CAST(JSON_UNQUOTE({x}) AS DECIMAL(65,30)) \
                 WHEN JSON_TYPE({x}) = 'STRING' AND JSON_UNQUOTE({x}) REGEXP '{NUMERIC_TEXT}' \
                 THEN CAST(TRIM(JSON_UNQUOTE({x})) AS DECIMAL(65,30)) END"
            );
            let bad = format!(
                "CASE WHEN {x} IS NOT NULL AND JSON_TYPE({x}) <> 'NULL' AND ({num}) IS NULL \
                 THEN 1 ELSE 0 END"
            );
            (num, bad)
        }
        SqlDialect::Mssql => {
            let p = json_path(path);
            let scalar = format!("JSON_VALUE(store_value, '{p}')");
            let nested = format!("JSON_QUERY(store_value, '{p}')");
            let num = format!("TRY_CAST(LTRIM(RTRIM({scalar})) AS DECIMAL(38,12))");
            let bad = format!(
                "CASE WHEN ({scalar} IS NOT NULL AND {num} IS NULL) OR {nested} IS NOT NULL \
                 THEN 1 ELSE 0 END"
            );
            (num, bad)
        }
    }
}

/// The text a row groups under: the field's scalar text, a nested value's JSON, or `null`.
fn group_expr(dialect: SqlDialect, path: &[String]) -> String {
    match dialect {
        SqlDialect::Postgres => format!(
            "COALESCE(CAST(store_value AS jsonb) #>> '{{{}}}', 'null')",
            path.join(",")
        ),
        SqlDialect::Mysql => format!(
            "CONVERT(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(store_value, '{}')), 'null') USING utf8mb4)",
            json_path(path)
        ),
        SqlDialect::Mssql => {
            let p = json_path(path);
            format!("COALESCE(JSON_VALUE(store_value, '{p}'), JSON_QUERY(store_value, '{p}'), N'null')")
        }
    }
}

/// One aggregate's statement over every matching row (no `after`, no limit). Columns: (group
/// text when grouped,) count, decimal sum as text (NULL when nothing summed), non-numeric count.
pub fn aggregate_sql(
    dialect: SqlDialect,
    query: &KeyRangeQuery,
    agg: &KeyQueryAggregate,
) -> String {
    let filter = where_clause(dialect, query, false);
    let (num, bad) = match &agg.sum_field {
        Some(path) => sum_exprs(dialect, path),
        None => (
            match dialect {
                SqlDialect::Postgres => "CAST(NULL AS NUMERIC)".to_string(),
                SqlDialect::Mysql => "CAST(NULL AS DECIMAL(65,30))".to_string(),
                SqlDialect::Mssql => "CAST(NULL AS DECIMAL(38,12))".to_string(),
            },
            "0".to_string(),
        ),
    };
    let group = agg
        .group_by
        .as_ref()
        .map(|path| group_expr(dialect, path))
        .unwrap_or_else(|| "''".to_string());
    let inner = format!("SELECT {group} AS g, {num} AS n, {bad} AS b FROM data_store{filter}");
    let (count, sum, bad_total) = aggregate_columns(dialect);
    if agg.group_by.is_some() {
        format!("SELECT g, {count}, {sum}, {bad_total} FROM ({inner}) t GROUP BY g")
    } else {
        format!("SELECT {count}, {sum}, {bad_total} FROM ({inner}) t")
    }
}

/// The outer select list of an aggregate statement: count, sum as text, non-numeric count.
fn aggregate_columns(dialect: SqlDialect) -> (&'static str, &'static str, &'static str) {
    match dialect {
        SqlDialect::Postgres => (
            "COUNT(*)",
            "CAST(SUM(n) AS TEXT)",
            "CAST(COALESCE(SUM(b), 0) AS BIGINT)",
        ),
        SqlDialect::Mysql => (
            "COUNT(*)",
            "CONVERT(CAST(SUM(n) AS CHAR) USING utf8mb4)",
            "CAST(COALESCE(SUM(b), 0) AS SIGNED)",
        ),
        SqlDialect::Mssql => (
            "COUNT_BIG(*)",
            "CAST(SUM(n) AS NVARCHAR(100))",
            "CAST(COALESCE(SUM(b), 0) AS BIGINT)",
        ),
    }
}

/// Build one aggregate cell from a statement row: count, sum text (NULL = nothing summed),
/// non-numeric count. A sum aggregate always reports a sum (`"0"` when no row had the field).
pub fn cell(
    agg: &KeyQueryAggregate,
    count: i64,
    sum: Option<String>,
    non_numeric: i64,
) -> KeyAggregateCell {
    KeyAggregateCell {
        count,
        sum: agg.sum_field.as_ref().map(|_| {
            sum.map(|s| s.trim().to_string())
                .unwrap_or_else(|| "0".to_string())
        }),
        non_numeric,
    }
}

/// Drop trailing fractional zeros (and a bare trailing point) from a decimal text. MySQL and SQL
/// Server sum into a fixed-scale `DECIMAL` (30 and 12 places), whose padding says nothing about
/// the inputs; PostgreSQL's `NUMERIC` keeps the inputs' own scale and is left as it is.
pub fn trim_decimal_zeros(text: &str) -> String {
    let text = text.trim();
    if !text.contains('.') || text.contains(['e', 'E']) {
        return text.to_string();
    }
    let trimmed = text.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() || trimmed == "-" {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// The statement reading the key column's collation (PostgreSQL falls back to the database
/// default when the column declares none). One row, one nullable text column.
pub fn key_order_probe_sql(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Postgres => {
            "SELECT COALESCE(c.collation_name, (SELECT datcollate FROM pg_database \
             WHERE datname = current_database())) FROM information_schema.columns c \
             WHERE c.table_name = 'data_store' AND c.column_name = 'store_key' \
             AND c.table_schema = ANY (current_schemas(false)) LIMIT 1"
        }
        SqlDialect::Mysql => {
            "SELECT CONVERT(COLLATION_NAME USING utf8mb4) FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'data_store' \
             AND COLUMN_NAME = 'store_key'"
        }
        SqlDialect::Mssql => {
            "SELECT CAST(collation_name AS NVARCHAR(256)) FROM sys.columns \
             WHERE object_id = OBJECT_ID('data_store') AND name = 'store_key'"
        }
    }
}

/// The statement counting the indexes of `data_store` whose first two key columns are
/// `(store_name, store_key)` — the index a key range is served by (PostgreSQL also requires the
/// index to order `store_key` in the column's own collation). One row, one `BIGINT`.
pub fn key_index_probe_sql(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Postgres => {
            "SELECT count(*) FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid \
             JOIN pg_namespace n ON n.oid = t.relnamespace \
             WHERE t.relname = 'data_store' AND n.nspname = ANY (current_schemas(false)) \
             AND i.indnkeyatts >= 2 \
             AND (SELECT attname FROM pg_attribute WHERE attrelid = t.oid AND attnum = i.indkey[0]) \
                 = 'store_name' \
             AND (SELECT attname FROM pg_attribute WHERE attrelid = t.oid AND attnum = i.indkey[1]) \
                 = 'store_key' \
             AND i.indcollation[1] = (SELECT attcollation FROM pg_attribute \
                 WHERE attrelid = t.oid AND attname = 'store_key')"
        }
        SqlDialect::Mysql => {
            "SELECT COUNT(*) FROM information_schema.STATISTICS s1 \
             JOIN information_schema.STATISTICS s2 ON s1.TABLE_SCHEMA = s2.TABLE_SCHEMA \
             AND s1.TABLE_NAME = s2.TABLE_NAME AND s1.INDEX_NAME = s2.INDEX_NAME \
             WHERE s1.TABLE_SCHEMA = DATABASE() AND s1.TABLE_NAME = 'data_store' \
             AND s1.SEQ_IN_INDEX = 1 AND s1.COLUMN_NAME = 'store_name' \
             AND s2.SEQ_IN_INDEX = 2 AND s2.COLUMN_NAME = 'store_key'"
        }
        SqlDialect::Mssql => {
            "SELECT COUNT_BIG(*) FROM sys.indexes i \
             JOIN sys.index_columns c1 ON c1.object_id = i.object_id AND c1.index_id = i.index_id \
             AND c1.key_ordinal = 1 \
             JOIN sys.columns k1 ON k1.object_id = c1.object_id AND k1.column_id = c1.column_id \
             JOIN sys.index_columns c2 ON c2.object_id = i.object_id AND c2.index_id = i.index_id \
             AND c2.key_ordinal = 2 \
             JOIN sys.columns k2 ON k2.object_id = c2.object_id AND k2.column_id = c2.column_id \
             WHERE i.object_id = OBJECT_ID('data_store') AND k1.name = 'store_name' \
             AND k2.name = 'store_key'"
        }
    }
}

/// The refusal for a table with no index leading with `(store_name, store_key)`.
pub fn key_index_error(store: &str) -> DataStoreError {
    DataStoreError::with_code(
        codes::QUERY_KEY_INDEX,
        format!(
            "store '{store}': key queries are served by an index whose first two columns are \
             (store_name, store_key), and the data_store table has none — a key range would scan \
             the whole table. Declare PRIMARY KEY (store_name, store_key) in the store's \
             migration (keep any tenant/module/version columns as plain NOT NULL columns), or \
             add CREATE INDEX data_store_key ON data_store (store_name, store_key)."
        ),
    )
}

/// Whether a key-column collation orders keys bytewise (code-point order).
///
/// * PostgreSQL: `C`, `POSIX`, `ucs_basic`, `pg_c_utf8`, or a `C.<encoding>` locale.
/// * MySQL / MariaDB: a `…_bin` collation or `binary` (a binary column has no collation).
/// * SQL Server: a `…_BIN2` collation (code-point order; `nvarchar` compares UTF-16 code
///   units, which differs from code-point order only above U+FFFF).
pub fn is_bytewise_collation(dialect: SqlDialect, collation: Option<&str>) -> bool {
    match (dialect, collation) {
        (SqlDialect::Mysql, None) => true,
        (_, None) => false,
        (SqlDialect::Postgres, Some(name)) => {
            let name = name.trim().to_ascii_lowercase();
            matches!(name.as_str(), "c" | "posix" | "ucs_basic" | "pg_c_utf8")
                || name.starts_with("c.")
        }
        (SqlDialect::Mysql, Some(name)) => {
            let name = name.trim().to_ascii_lowercase();
            name == "binary" || name.ends_with("_bin")
        }
        (SqlDialect::Mssql, Some(name)) => name.to_ascii_uppercase().contains("_BIN2"),
    }
}

/// The refusal for a key column whose collation is not bytewise, naming the fix.
pub fn key_order_error(
    store: &str,
    dialect: SqlDialect,
    collation: Option<&str>,
) -> DataStoreError {
    key_order_error_on(store, dialect, "data_store", collation)
}

/// [`key_order_error`] for the key column of `table`.
fn key_order_error_on(
    store: &str,
    dialect: SqlDialect,
    table: &str,
    collation: Option<&str>,
) -> DataStoreError {
    let fix = match dialect {
        SqlDialect::Postgres => {
            format!("ALTER TABLE {table} ALTER COLUMN store_key TYPE VARCHAR(512) COLLATE \"C\"")
        }
        SqlDialect::Mysql => format!(
            "ALTER TABLE {table} MODIFY store_key VARCHAR(512) CHARACTER SET utf8mb4 \
             COLLATE utf8mb4_bin NOT NULL"
        ),
        SqlDialect::Mssql => format!(
            "ALTER TABLE {table} ALTER COLUMN store_key NVARCHAR(512) COLLATE \
             Latin1_General_100_BIN2 NOT NULL (drop and re-create the primary key around it)"
        ),
    };
    DataStoreError::with_code(
        codes::QUERY_KEY_ORDER,
        format!(
            "store '{store}': key queries compare keys bytewise and need the {table}.store_key \
             column in a bytewise collation so the primary key serves the range; its collation is \
             {}. Declare it bytewise in the store's migration, e.g. {fix}.",
            collation.unwrap_or("unknown")
        ),
    )
}

// ---- typed (projected) stores ---------------------------------------------------------------

/// The declared field one `sum`/`groupBy` path of a typed-store query names, or the refusal.
fn typed_field<'p>(
    projected: &'p ProjectedStore,
    role: &str,
    path: &[String],
) -> Result<(&'p ProjectedField, ValueClass), DataStoreError> {
    let refuse = |why: String| {
        DataStoreError::with_code(
            codes::QUERY_UNSUPPORTED,
            format!(
                "key query on store '{}': {role} '{}' {why}",
                projected.store(),
                path.join(".")
            ),
        )
    };
    if path.len() != 1 {
        return Err(refuse(format!(
            "is a nested path; the store declares structure '{}' (typed columns), so an \
             aggregate names exactly one declared field",
            projected.projection().type_name
        )));
    }
    projected.field_class(&path[0]).ok_or_else(|| {
        let declared: Vec<&str> = projected
            .projection()
            .fields
            .iter()
            .map(|f| f.field.as_str())
            .collect();
        refuse(format!(
            "is not a field of structure '{}' (declared: {})",
            projected.projection().type_name,
            declared.join(", ")
        ))
    })
}

/// Refuse a typed-store query no statement should be built for: everything [`validate`]
/// refuses, plus a `sum`/`groupBy` path that does not name exactly one declared field.
pub fn validate_typed(
    projected: &ProjectedStore,
    query: &KeyRangeQuery,
) -> Result<(), DataStoreError> {
    validate(projected.store(), query)?;
    for agg in &query.aggregates {
        if let Some(path) = &agg.sum_field {
            typed_field(projected, "sum field", path)?;
        }
        if let Some(path) = &agg.group_by {
            typed_field(projected, "groupBy", path)?;
        }
    }
    Ok(())
}

/// The typed-store WHERE clause — the key range alone (no `store_name`: the table is the
/// store). Binds, in order: lower, upper (when present), after (when `with_after` and present).
fn typed_where_clause(dialect: SqlDialect, query: &KeyRangeQuery, with_after: bool) -> String {
    let mut n = 1;
    let mut next = || {
        let p = dialect.placeholder(n);
        n += 1;
        p
    };
    let mut sql = format!(" WHERE store_key >= {}", next());
    if query.upper.is_some() {
        sql.push_str(&format!(" AND store_key < {}", next()));
    }
    if with_after && query.after.is_some() {
        sql.push_str(&format!(" AND store_key > {}", next()));
    }
    sql
}

/// The bind values for [`typed_rows_sql`] (`with_after = true`) or [`typed_aggregate_sql`]
/// (`false`), in placeholder order.
pub fn typed_binds(query: &KeyRangeQuery, with_after: bool) -> Vec<&str> {
    let mut out = binds("", query, with_after);
    out.remove(0);
    out
}

/// The typed page statement: `store_key` then the declared columns as text (in declared order,
/// so [`ProjectedStore::row_to_json`] reads cells `1..`), one row more than the limit.
pub fn typed_rows_sql(
    dialect: SqlDialect,
    projected: &ProjectedStore,
    query: &KeyRangeQuery,
    limit: u32,
) -> String {
    let fetch = u64::from(limit) + 1;
    let filter = typed_where_clause(dialect, query, true);
    let columns = projected.read_list(dialect);
    let table = projected.table();
    match dialect {
        SqlDialect::Mssql => format!(
            "SELECT TOP ({fetch}) store_key, {columns} FROM {table}{filter} ORDER BY store_key"
        ),
        SqlDialect::Postgres => format!(
            "SELECT store_key, {columns} FROM {table}{filter} ORDER BY store_key LIMIT {fetch}"
        ),
        // A `…_bin` key column reads back as VARBINARY (see [`rows_sql`]).
        SqlDialect::Mysql => format!(
            "SELECT CONVERT(store_key USING utf8mb4), {columns} FROM {table}{filter} \
             ORDER BY store_key LIMIT {fetch}"
        ),
    }
}

/// `(numeric value or NULL, 1 when present-but-not-numeric else 0)` for one summed column.
fn typed_sum_exprs(
    dialect: SqlDialect,
    field: &ProjectedField,
    class: ValueClass,
) -> (String, String) {
    let col = field.column.as_str();
    match class {
        ValueClass::Number => {
            let num = match dialect {
                // An integer column sums in its own (overflowable) type on SQL Server.
                SqlDialect::Mssql if field.builtin != sutra_xsd::Builtin::Decimal => {
                    format!("CAST({col} AS DECIMAL(38,0))")
                }
                _ => col.to_string(),
            };
            (num, "0".to_string())
        }
        ValueClass::Text => match dialect {
            SqlDialect::Postgres => (
                format!("CASE WHEN {col} ~ '{NUMERIC_TEXT}' THEN CAST(btrim({col}) AS NUMERIC) END"),
                format!(
                    "CASE WHEN {col} IS NOT NULL AND NOT ({col} ~ '{NUMERIC_TEXT}') THEN 1 ELSE 0 END"
                ),
            ),
            SqlDialect::Mysql => (
                format!(
                    "CASE WHEN {col} REGEXP '{NUMERIC_TEXT}' \
                     THEN CAST(TRIM({col}) AS DECIMAL(65,30)) END"
                ),
                format!(
                    "CASE WHEN {col} IS NOT NULL AND NOT ({col} REGEXP '{NUMERIC_TEXT}') \
                     THEN 1 ELSE 0 END"
                ),
            ),
            SqlDialect::Mssql => {
                let num = format!("TRY_CAST(LTRIM(RTRIM({col})) AS DECIMAL(38,12))");
                let bad = format!("CASE WHEN {col} IS NOT NULL AND {num} IS NULL THEN 1 ELSE 0 END");
                (num, bad)
            }
        },
        _ => (
            null_numeric(dialect).to_string(),
            format!("CASE WHEN {col} IS NOT NULL THEN 1 ELSE 0 END"),
        ),
    }
}

/// A typed NULL in the dialect's decimal type (the sum column of a count-only aggregate).
fn null_numeric(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Postgres => "CAST(NULL AS NUMERIC)",
        SqlDialect::Mysql => "CAST(NULL AS DECIMAL(65,30))",
        SqlDialect::Mssql => "CAST(NULL AS DECIMAL(38,12))",
    }
}

/// The text a row of a typed store groups under: the column's canonical text, or `null`.
fn typed_group_expr(dialect: SqlDialect, field: &ProjectedField, class: ValueClass) -> String {
    let text = dialect.read_expr(class, &field.column);
    match dialect {
        SqlDialect::Postgres => format!("COALESCE({text}, 'null')"),
        SqlDialect::Mysql => format!("CONVERT(COALESCE({text}, 'null') USING utf8mb4)"),
        SqlDialect::Mssql => format!("COALESCE({text}, N'null')"),
    }
}

/// One aggregate's statement over every matching row of a typed store — real SQL aggregates
/// over the store's own table, same columns as [`aggregate_sql`]: (group text when grouped,)
/// count, decimal sum as text (NULL when nothing summed), non-numeric count. A path
/// [`validate_typed`] refuses is refused here too.
pub fn typed_aggregate_sql(
    dialect: SqlDialect,
    projected: &ProjectedStore,
    query: &KeyRangeQuery,
    agg: &KeyQueryAggregate,
) -> Result<String, DataStoreError> {
    let filter = typed_where_clause(dialect, query, false);
    let (num, bad) = match &agg.sum_field {
        Some(path) => {
            let (field, class) = typed_field(projected, "sum field", path)?;
            typed_sum_exprs(dialect, field, class)
        }
        None => (null_numeric(dialect).to_string(), "0".to_string()),
    };
    let group = match &agg.group_by {
        Some(path) => {
            let (field, class) = typed_field(projected, "groupBy", path)?;
            typed_group_expr(dialect, field, class)
        }
        None => "''".to_string(),
    };
    let table = projected.table();
    let inner = format!("SELECT {group} AS g, {num} AS n, {bad} AS b FROM {table}{filter}");
    let (count, sum, bad_total) = aggregate_columns(dialect);
    Ok(if agg.group_by.is_some() {
        format!("SELECT g, {count}, {sum}, {bad_total} FROM ({inner}) t GROUP BY g")
    } else {
        format!("SELECT {count}, {sum}, {bad_total} FROM ({inner}) t")
    })
}

/// The group key a typed store reports for a group's raw text: the value rendered as the typed
/// get renders it (`"null"` stays `"null"` — an absent optional field).
pub fn typed_group_key(projected: &ProjectedStore, agg: &KeyQueryAggregate, raw: String) -> String {
    let Some((_, class)) = agg
        .group_by
        .as_ref()
        .filter(|p| p.len() == 1)
        .and_then(|p| projected.field_class(&p[0]))
    else {
        return raw;
    };
    if raw == "null" {
        return raw;
    }
    match crate::projected::decode(class, &raw) {
        Json::String(s) => s,
        other => other.to_string(),
    }
}

/// The typed-store equivalent of [`key_order_probe_sql`] for `table` (a validated, optionally
/// schema-qualified identifier — interpolated, not bound).
pub fn typed_key_order_probe_sql(dialect: SqlDialect, table: &str) -> String {
    let (schema, name) = split_table(table);
    match dialect {
        SqlDialect::Postgres => {
            let scope = match schema {
                Some(s) => format!("LOWER(c.table_schema) = LOWER('{s}')"),
                None => "c.table_schema = ANY (current_schemas(false))".to_string(),
            };
            format!(
                "SELECT COALESCE(c.collation_name, (SELECT datcollate FROM pg_database \
                 WHERE datname = current_database())) FROM information_schema.columns c \
                 WHERE LOWER(c.table_name) = LOWER('{name}') AND c.column_name = 'store_key' \
                 AND {scope} LIMIT 1"
            )
        }
        SqlDialect::Mysql => {
            let scope = match schema {
                Some(s) => format!("'{s}'"),
                None => "DATABASE()".to_string(),
            };
            format!(
                "SELECT CONVERT(COLLATION_NAME USING utf8mb4) FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = {scope} AND LOWER(TABLE_NAME) = LOWER('{name}') \
                 AND COLUMN_NAME = 'store_key'"
            )
        }
        SqlDialect::Mssql => format!(
            "SELECT CAST(collation_name AS NVARCHAR(256)) FROM sys.columns \
             WHERE object_id = OBJECT_ID('{table}') AND name = 'store_key'"
        ),
    }
}

/// The typed-store equivalent of [`key_index_probe_sql`]: the indexes of `table` whose FIRST key
/// column is `store_key` (PostgreSQL: ordered in the column's own collation). One `BIGINT`.
pub fn typed_key_index_probe_sql(dialect: SqlDialect, table: &str) -> String {
    let (schema, name) = split_table(table);
    match dialect {
        SqlDialect::Postgres => {
            let scope = match schema {
                Some(s) => format!("n.nspname = LOWER('{s}')"),
                None => "n.nspname = ANY (current_schemas(false))".to_string(),
            };
            format!(
                "SELECT count(*) FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 WHERE t.relname = LOWER('{name}') AND {scope} AND i.indnkeyatts >= 1 \
                 AND (SELECT attname FROM pg_attribute WHERE attrelid = t.oid \
                 AND attnum = i.indkey[0]) = 'store_key' \
                 AND i.indcollation[0] = (SELECT attcollation FROM pg_attribute \
                 WHERE attrelid = t.oid AND attname = 'store_key')"
            )
        }
        SqlDialect::Mysql => {
            let scope = match schema {
                Some(s) => format!("'{s}'"),
                None => "DATABASE()".to_string(),
            };
            format!(
                "SELECT COUNT(*) FROM information_schema.STATISTICS \
                 WHERE TABLE_SCHEMA = {scope} AND LOWER(TABLE_NAME) = LOWER('{name}') \
                 AND SEQ_IN_INDEX = 1 AND COLUMN_NAME = 'store_key'"
            )
        }
        SqlDialect::Mssql => format!(
            "SELECT COUNT_BIG(*) FROM sys.indexes i \
             JOIN sys.index_columns c1 ON c1.object_id = i.object_id AND c1.index_id = i.index_id \
             AND c1.key_ordinal = 1 \
             JOIN sys.columns k1 ON k1.object_id = c1.object_id AND k1.column_id = c1.column_id \
             WHERE i.object_id = OBJECT_ID('{table}') AND k1.name = 'store_key'"
        ),
    }
}

/// `schema.table` → `(Some(schema), table)`; `table` → `(None, table)`.
fn split_table(table: &str) -> (Option<&str>, &str) {
    match table.split_once('.') {
        Some((schema, name)) => (Some(schema), name),
        None => (None, table),
    }
}

/// The refusal for a typed table whose `store_key` column is not bytewise, naming the fix.
pub fn typed_key_order_error(
    projected: &ProjectedStore,
    dialect: SqlDialect,
    collation: Option<&str>,
) -> DataStoreError {
    key_order_error_on(projected.store(), dialect, projected.table(), collation)
}

/// The refusal for a typed table with no index leading with `store_key`.
pub fn typed_key_index_error(projected: &ProjectedStore) -> DataStoreError {
    let (store, table) = (projected.store(), projected.table());
    DataStoreError::with_code(
        codes::QUERY_KEY_INDEX,
        format!(
            "store '{store}': key queries are served by an index whose first column is \
             store_key, and table {table} has none — a key range would scan the whole table. \
             Declare store_key the table's PRIMARY KEY in the store's migration, or add \
             CREATE INDEX {}_key ON {table} (store_key).",
            table.replace('.', "_")
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(upper: Option<&str>, after: Option<&str>) -> KeyRangeQuery {
        KeyRangeQuery {
            lower: "src|".into(),
            upper: upper.map(str::to_string),
            after: after.map(str::to_string),
            row_limit: Some(10),
            aggregates: vec![],
        }
    }

    #[test]
    fn the_prefix_bound_is_the_next_code_point() {
        assert_eq!(prefix_upper_bound("src|").as_deref(), Some("src}"));
        assert_eq!(prefix_upper_bound("a").as_deref(), Some("b"));
        assert_eq!(prefix_upper_bound("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_upper_bound("\u{D7FF}").as_deref(), Some("\u{E000}"));
        assert_eq!(prefix_upper_bound("\u{10FFFF}"), None);
        assert_eq!(prefix_upper_bound(""), None);
        // Every key with the prefix sorts inside [prefix, bound) bytewise.
        let bound = prefix_upper_bound("src|2026-10-").unwrap();
        for key in ["src|2026-10-", "src|2026-10-01|x", "src|2026-10-\u{10FFFF}"] {
            assert!(key >= "src|2026-10-" && key < bound.as_str(), "{key}");
        }
        assert!("src|2026-11-01" >= bound.as_str());
    }

    #[test]
    fn rows_statements_bind_in_placeholder_order_per_dialect() {
        let q = query(Some("src}"), Some("src|5"));
        assert_eq!(
            rows_sql(SqlDialect::Postgres, &q, 10),
            "SELECT store_key, store_value FROM data_store WHERE store_name = $1 AND store_key \
             >= $2 AND store_key < $3 AND store_key > $4 ORDER BY store_key LIMIT 11"
        );
        assert!(rows_sql(SqlDialect::Mssql, &q, 10).starts_with("SELECT TOP (11) "));
        assert!(rows_sql(SqlDialect::Mysql, &q, 10).contains("store_key > ? ORDER BY"));
        assert!(rows_sql(SqlDialect::Mysql, &q, 10).starts_with("SELECT CONVERT(store_key USING"));
        assert_eq!(binds("s", &q, true), vec!["s", "src|", "src}", "src|5"]);
        // Aggregates ignore `after`: they cover every matching row.
        assert_eq!(binds("s", &q, false), vec!["s", "src|", "src}"]);
        let open = query(None, None);
        assert!(!rows_sql(SqlDialect::Postgres, &open, 10).contains("store_key <"));
    }

    #[test]
    fn aggregate_statements_group_through_a_derived_table() {
        let q = query(Some("src}"), Some("ignored"));
        let agg = KeyQueryAggregate {
            sum_field: Some(vec!["amt".into(), "value".into()]),
            group_by: Some(vec!["dir".into()]),
        };
        let pg = aggregate_sql(SqlDialect::Postgres, &q, &agg);
        assert!(pg.contains("#> '{amt,value}'"), "{pg}");
        assert!(pg.ends_with("GROUP BY g"), "{pg}");
        assert!(!pg.contains("$4"), "no after: {pg}");
        let my = aggregate_sql(SqlDialect::Mysql, &q, &agg);
        assert!(my.contains("'$.\"amt\".\"value\"'"), "{my}");
        let ms = aggregate_sql(SqlDialect::Mssql, &q, &agg);
        assert!(
            ms.contains("COUNT_BIG(*)") && ms.contains("JSON_VALUE"),
            "{ms}"
        );
        let count = KeyQueryAggregate {
            sum_field: None,
            group_by: None,
        };
        assert!(!aggregate_sql(SqlDialect::Postgres, &q, &count).contains("GROUP BY"));
    }

    #[test]
    fn an_unsafe_path_or_limit_is_refused() {
        let mut q = query(None, None);
        q.aggregates.push(KeyQueryAggregate {
            sum_field: Some(vec!["a'b".into()]),
            group_by: None,
        });
        assert_eq!(
            validate("s", &q).unwrap_err().code(),
            Some(codes::QUERY_UNSUPPORTED)
        );
        let mut q = query(None, None);
        q.row_limit = Some(MAX_ROW_LIMIT + 1);
        assert!(validate("s", &q).is_err());
        q.row_limit = Some(MAX_ROW_LIMIT);
        assert!(validate("s", &q).is_ok());
    }

    #[test]
    fn a_page_reports_truncation_and_the_next_key() {
        let rows: Vec<(String, Json)> = (0..4).map(|i| (format!("k{i}"), Json::Null)).collect();
        let (page, truncated, next) = finish_page(rows.clone(), 3);
        assert_eq!(page.len(), 3);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("k2"));
        let (page, truncated, next) = finish_page(rows, 4);
        assert_eq!((page.len(), truncated, next), (4, false, None));
    }

    #[test]
    fn fixed_scale_padding_is_trimmed() {
        assert_eq!(trim_decimal_zeros("3.750000000000"), "3.75");
        assert_eq!(trim_decimal_zeros("30.000"), "30");
        assert_eq!(trim_decimal_zeros("0.000"), "0");
        assert_eq!(trim_decimal_zeros("-0.50"), "-0.5");
        assert_eq!(trim_decimal_zeros("120"), "120");
    }

    #[test]
    fn a_sum_cell_reports_zero_when_nothing_was_summed() {
        let sum = KeyQueryAggregate {
            sum_field: Some(vec!["amount".into()]),
            group_by: None,
        };
        assert_eq!(cell(&sum, 0, None, 0).sum.as_deref(), Some("0"));
        assert_eq!(
            cell(&sum, 2, Some("30.05".into()), 0).sum.as_deref(),
            Some("30.05")
        );
        let count = KeyQueryAggregate {
            sum_field: None,
            group_by: None,
        };
        assert_eq!(cell(&count, 2, None, 0).sum, None);
    }

    #[test]
    fn bytewise_collations_are_recognised_per_dialect() {
        for c in ["C", "POSIX", "C.UTF-8", "c.utf8", "ucs_basic", "pg_c_utf8"] {
            assert!(is_bytewise_collation(SqlDialect::Postgres, Some(c)), "{c}");
        }
        for c in ["en_US.utf8", "default", "und-x-icu"] {
            assert!(!is_bytewise_collation(SqlDialect::Postgres, Some(c)), "{c}");
        }
        assert!(is_bytewise_collation(
            SqlDialect::Mysql,
            Some("utf8mb4_bin")
        ));
        assert!(is_bytewise_collation(SqlDialect::Mysql, Some("ascii_bin")));
        assert!(is_bytewise_collation(SqlDialect::Mysql, None));
        assert!(!is_bytewise_collation(
            SqlDialect::Mysql,
            Some("utf8mb4_0900_ai_ci")
        ));
        assert!(is_bytewise_collation(
            SqlDialect::Mssql,
            Some("Latin1_General_100_BIN2")
        ));
        assert!(!is_bytewise_collation(
            SqlDialect::Mssql,
            Some("SQL_Latin1_General_CP1_CI_AS")
        ));
        let e = key_order_error("s", SqlDialect::Postgres, Some("en_US.utf8"));
        assert_eq!(e.code(), Some(codes::QUERY_KEY_ORDER));
        assert!(e.to_string().contains("COLLATE \"C\""));
    }

    // ---- typed (projected) stores -------------------------------------------------------

    fn ledger() -> ProjectedStore {
        use sutra_xsd::{Builtin, FieldDecl, FieldFacets, FieldShape};
        let scalar = |name: &str, builtin: Builtin, optional: bool| FieldDecl {
            name: name.to_string(),
            is_attribute: false,
            occurs_min: if optional { 0 } else { 1 },
            occurs_max: Some(1),
            in_choice: false,
            shape: FieldShape::Scalar {
                builtin,
                facets: FieldFacets::default(),
            },
        };
        let fields = vec![
            scalar("itemId", Builtin::String, false),
            scalar("amount", Builtin::Decimal, false),
            scalar("units", Builtin::Int, false),
            scalar("shelf", Builtin::String, true),
            scalar("closed", Builtin::Boolean, false),
            scalar("addedOn", Builtin::Date, true),
        ];
        let projection = crate::projection::Projection::derive(
            "LedgerEntry",
            &fields,
            &BTreeMap::new(),
            crate::projection::NamingRules::default(),
        )
        .expect("flat");
        ProjectedStore::new("ledger", "ledger_entries", projection).expect("projectable")
    }

    fn typed_agg(sum: Option<&str>, group: Option<&str>) -> KeyQueryAggregate {
        let path = |p: &str| p.split('.').map(str::to_string).collect::<Vec<_>>();
        KeyQueryAggregate {
            sum_field: sum.map(path),
            group_by: group.map(path),
        }
    }

    #[test]
    fn typed_rows_read_the_declared_columns_over_the_store_table() {
        let store = ledger();
        let q = query(Some("src}"), Some("src|5"));
        assert_eq!(
            typed_rows_sql(SqlDialect::Postgres, &store, &q, 10),
            "SELECT store_key, CAST(item_id AS TEXT), CAST(amount AS TEXT), CAST(units AS TEXT), \
             CAST(shelf AS TEXT), CAST(closed AS TEXT), CAST(added_on AS TEXT) FROM \
             ledger_entries WHERE store_key >= $1 AND store_key < $2 AND store_key > $3 \
             ORDER BY store_key LIMIT 11"
        );
        let my = typed_rows_sql(SqlDialect::Mysql, &store, &q, 10);
        assert!(my.starts_with("SELECT CONVERT(store_key USING utf8mb4), CAST(item_id AS CHAR)"));
        assert!(my.contains("FROM ledger_entries WHERE store_key >= ? AND store_key < ?"));
        let ms = typed_rows_sql(SqlDialect::Mssql, &store, &q, 10);
        assert!(ms.starts_with("SELECT TOP (11) store_key, "), "{ms}");
        assert!(ms.contains("CONVERT(NVARCHAR(MAX), added_on, 23)"), "{ms}");
        assert!(!ms.contains("store_name"));
        // No store name: the table is the store.
        assert_eq!(typed_binds(&q, true), vec!["src|", "src}", "src|5"]);
        assert_eq!(typed_binds(&q, false), vec!["src|", "src}"]);
    }

    #[test]
    fn typed_aggregates_are_sql_aggregates_over_the_named_columns() {
        let store = ledger();
        let q = query(Some("src}"), Some("ignored"));
        let pg = typed_aggregate_sql(
            SqlDialect::Postgres,
            &store,
            &q,
            &typed_agg(Some("amount"), Some("shelf")),
        )
        .unwrap();
        assert_eq!(
            pg,
            "SELECT g, COUNT(*), CAST(SUM(n) AS TEXT), CAST(COALESCE(SUM(b), 0) AS BIGINT) FROM \
             (SELECT COALESCE(CAST(shelf AS TEXT), 'null') AS g, amount AS n, 0 AS b FROM \
             ledger_entries WHERE store_key >= $1 AND store_key < $2) t GROUP BY g"
        );
        let count =
            typed_aggregate_sql(SqlDialect::Postgres, &store, &q, &typed_agg(None, None)).unwrap();
        assert!(!count.contains("GROUP BY") && count.contains("CAST(NULL AS NUMERIC)"));
        // SQL Server widens an integer column before summing it.
        let ms = typed_aggregate_sql(
            SqlDialect::Mssql,
            &store,
            &q,
            &typed_agg(Some("units"), None),
        )
        .unwrap();
        assert!(ms.contains("CAST(units AS DECIMAL(38,0)) AS n"), "{ms}");
        // A text column sums its decimal literals and counts the rest as non-numeric.
        let text = typed_aggregate_sql(
            SqlDialect::Mysql,
            &store,
            &q,
            &typed_agg(Some("itemId"), None),
        )
        .unwrap();
        assert!(text.contains("item_id REGEXP"), "{text}");
        // Any other column type counts every present value as non-numeric.
        let date = typed_aggregate_sql(
            SqlDialect::Postgres,
            &store,
            &q,
            &typed_agg(Some("addedOn"), None),
        )
        .unwrap();
        assert!(
            date.contains("CASE WHEN added_on IS NOT NULL THEN 1 ELSE 0 END"),
            "{date}"
        );
    }

    #[test]
    fn a_typed_aggregate_path_names_exactly_one_declared_field() {
        let store = ledger();
        for (sum, group) in [
            (Some("amount.value"), None),
            (Some("missing"), None),
            (None, Some("shelf.code")),
            (None, Some("item_id")),
        ] {
            let mut q = query(None, None);
            q.aggregates.push(typed_agg(sum, group));
            let e = validate_typed(&store, &q).unwrap_err();
            assert_eq!(
                e.code(),
                Some(codes::QUERY_UNSUPPORTED),
                "{sum:?} {group:?}"
            );
            assert!(
                typed_aggregate_sql(SqlDialect::Postgres, &store, &q, &q.aggregates[0]).is_err()
            );
        }
        let mut q = query(None, None);
        q.aggregates.push(typed_agg(Some("amount"), Some("closed")));
        assert!(validate_typed(&store, &q).is_ok());
    }

    #[test]
    fn typed_group_keys_render_as_the_typed_get_does() {
        let store = ledger();
        let by_closed = typed_agg(None, Some("closed"));
        assert_eq!(typed_group_key(&store, &by_closed, "1".into()), "true");
        assert_eq!(typed_group_key(&store, &by_closed, "f".into()), "false");
        assert_eq!(typed_group_key(&store, &by_closed, "null".into()), "null");
        let by_shelf = typed_agg(None, Some("shelf"));
        assert_eq!(typed_group_key(&store, &by_shelf, "A-1".into()), "A-1");
        let by_amount = typed_agg(None, Some("amount"));
        assert_eq!(typed_group_key(&store, &by_amount, "12.50".into()), "12.50");
    }

    #[test]
    fn typed_probes_target_the_store_table_and_lead_with_the_key() {
        let pg = typed_key_index_probe_sql(SqlDialect::Postgres, "ledger_entries");
        assert!(pg.contains("t.relname = LOWER('ledger_entries')"), "{pg}");
        assert!(pg.contains("i.indkey[0]) = 'store_key'"), "{pg}");
        assert!(pg.contains("current_schemas(false)"), "{pg}");
        let qualified = typed_key_order_probe_sql(SqlDialect::Postgres, "app.ledger_entries");
        assert!(
            qualified.contains("LOWER(c.table_schema) = LOWER('app')"),
            "{qualified}"
        );
        assert!(
            typed_key_order_probe_sql(SqlDialect::Mysql, "ledger_entries")
                .contains("LOWER(TABLE_NAME) = LOWER('ledger_entries')")
        );
        assert!(
            typed_key_index_probe_sql(SqlDialect::Mysql, "ledger_entries")
                .contains("SEQ_IN_INDEX = 1 AND COLUMN_NAME = 'store_key'")
        );
        assert!(
            typed_key_index_probe_sql(SqlDialect::Mssql, "ledger_entries")
                .contains("OBJECT_ID('ledger_entries')")
        );
        let store = ledger();
        let e = typed_key_order_error(&store, SqlDialect::Postgres, Some("en_US.utf8"));
        assert_eq!(e.code(), Some(codes::QUERY_KEY_ORDER));
        assert!(e.to_string().contains("ALTER TABLE ledger_entries"), "{e}");
        assert_eq!(
            typed_key_index_error(&store).code(),
            Some(codes::QUERY_KEY_INDEX)
        );
    }
}
