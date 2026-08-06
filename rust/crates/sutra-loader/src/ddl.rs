//! The DDL subset — the effective table shape a package's own migrations produce, derived
//! **statically** (no database, no credentials) for the projected-store lint (design
//! `datastore-schema-projection.md` §4.7).
//!
//! [`parse_migrations`] replays a store's `migrations/<store>/V*.sql` in migration-version order
//! and applies, in sequence, `CREATE TABLE` column definitions and
//! `ALTER TABLE ADD|ALTER|MODIFY|CHANGE|RENAME|DROP COLUMN`, plus the key-bearing forms
//! (inline/table-level `PRIMARY KEY`, `ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY`, a `UNIQUE`
//! constraint or unique index). The result is the [`TableShape`] each declared column, its type,
//! its nullability and the table's key resolve to.
//!
//! # The posture: fail toward the WARNING, never toward a false ERROR
//!
//! This is the load-bearing rule of the module, and every parsing decision below is subordinate
//! to it. **A linter that cries wolf on legitimate DDL is worse than no linter**, because
//! authors learn to ignore it. So anything outside the subset — a procedural block, a
//! dialect-specific clause that is not modelled, a table created elsewhere, an unterminated
//! literal, a statement that simply does not parse — degrades the affected shape to
//! "unverifiable" ([`DdlShape::Unverifiable`] / [`TableShape::complete`] = `false`), which the
//! linter reports as the `SUTRA.CONFIG.DATASTORE.DDL_UNVERIFIABLE` **warning**. It never
//! produces a `COLUMN_MISSING`, a `COLUMN_TYPE_MISMATCH` or a `KEY_MISMATCH` from DDL it did
//! not fully understand.
//!
//! Concretely:
//!
//! | Input | Outcome |
//! |---|---|
//! | A statement whose leading keywords are not in the subset (`CREATE FUNCTION`, `DO $$…$$`, `EXEC`, a bare `BEGIN … END` block) | whole store → [`DdlShape::Unverifiable`] |
//! | An `ALTER TABLE` action that is not modelled | whole store → [`DdlShape::Unverifiable`] |
//! | An unterminated string / quoted identifier / block comment | whole store → [`DdlShape::Unverifiable`] |
//! | An `ALTER TABLE` on a table this run never saw created | that ONE table is marked [`TableShape::complete`] = `false` |
//! | A column type outside the comparable set (`JSONB`, `UUID`, a domain type) | the column exists; only its type comparison is [`Fit::Unprovable`] |
//! | A statement that provably cannot change a table's columns or key (`INSERT`, `CREATE INDEX`, `COMMENT ON`, `GRANT`, `DROP TRIGGER`, …) | ignored, verification continues |
//!
//! The last row is what keeps the linter useful: a real migration is mostly DDL plus seed
//! `INSERT`s and indexes, and degrading on those would make every package unverifiable. Only
//! statements from the explicit shape-neutral list are skipped; everything unrecognised
//! degrades.
//!
//! # Dialects
//!
//! One parser covers PostgreSQL, MySQL/MariaDB and SQL Server, because the *subset* is nearly
//! identical across them; the spellings differ and are all accepted
//! ([`SqlType`] documents the mapping). Identifiers may be unquoted, `"double quoted"`,
//! `` `backquoted` `` or `[bracketed]`, and are compared case-insensitively throughout — a
//! charitable comparison can only mask a fault, never invent one.

use std::collections::BTreeMap;

use sutra_datastore::projection::{Builtin, FieldFacets};

/// The parsed type of a column, reduced to what a facet comparison can act on. The raw spelling
/// is kept separately in [`ColumnDef::declared_type`] for diagnostics.
///
/// | Variant | Spellings accepted |
/// |---|---|
/// | `Text` | `VARCHAR(n)`, `CHARACTER VARYING(n)`, `CHAR(n)`, `NCHAR(n)`, `NVARCHAR(n)`, `VARCHAR2(n)`, and the unbounded `TEXT`, `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `NTEXT`, `CLOB`, `NVARCHAR(MAX)`, `VARCHAR(MAX)` |
/// | `Numeric` | `NUMERIC(p,s)`, `DECIMAL(p,s)`, `DEC(p,s)`, `NUMBER(p,s)` |
/// | `Integer` | `TINYINT` (8), `SMALLINT`/`INT2` (16), `MEDIUMINT` (24), `INT`/`INTEGER`/`INT4`/`SERIAL` (32), `BIGINT`/`INT8`/`BIGSERIAL` (64) |
/// | `Boolean` | `BOOLEAN`, `BOOL`, `BIT`, `BIT(1)`, `TINYINT(1)` |
/// | `Date` / `Time` / `Timestamp` | `DATE`; `TIME[(n)]`, `TIMETZ`; `TIMESTAMP[(n)]`, `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE`, `DATETIME`, `DATETIME2[(n)]`, `SMALLDATETIME`, `DATETIMEOFFSET` |
/// | `Binary` | `BYTEA`, `BLOB`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BINARY(n)`, `VARBINARY(n)`, `VARBINARY(MAX)`, `IMAGE` |
/// | `Float` | `REAL`, `FLOAT[(n)]`, `DOUBLE`, `DOUBLE PRECISION`, `FLOAT4`, `FLOAT8` |
/// | `Unknown` | everything else — the column exists, but no comparison is attempted |
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlType {
    /// Character data. `max_chars = None` is unbounded (`TEXT`, `NVARCHAR(MAX)`).
    Text {
        /// The declared character cap, `None` when unbounded.
        max_chars: Option<u64>,
    },
    /// Exact numeric. Both parts are `None` for a bare `NUMERIC`/`DECIMAL` (arbitrary precision).
    Numeric {
        /// Total digits.
        precision: Option<u64>,
        /// Digits right of the decimal point.
        scale: Option<u64>,
    },
    /// Exact integral, with the column's signed width in bits.
    Integer {
        /// The signed width (8/16/24/32/64).
        bits: u32,
    },
    /// Approximate binary floating point — never an exact-decimal target.
    Float,
    /// A two-valued column (`BOOLEAN`, `BIT`, `TINYINT(1)`).
    Boolean,
    /// A calendar date with no time part.
    Date,
    /// A time of day with no date part.
    Time,
    /// An instant (with or without a zone — the distinction does not change what the projection
    /// can store).
    Timestamp,
    /// Binary data. `max_bytes = None` is unbounded (`BYTEA`, `BLOB`, `VARBINARY(MAX)`).
    Binary {
        /// The declared byte cap, `None` when unbounded.
        max_bytes: Option<u64>,
    },
    /// Outside the comparable set — carried verbatim so the diagnostic can name it.
    Unknown(String),
}

/// One column of the effective table shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// The column name, unquoted (case as written; compared case-insensitively).
    pub name: String,
    /// The type exactly as the migration spells it (`VARCHAR(10)`) — for diagnostics.
    pub declared_type: String,
    /// The parsed type.
    pub ty: SqlType,
    /// Whether the column admits `NULL`. Defaults to `true` (SQL's default) unless the
    /// definition says `NOT NULL` or the column is part of an inline `PRIMARY KEY`.
    pub nullable: bool,
    /// Whether the column carries a `DEFAULT` (or is an auto-increment / identity / serial
    /// column, which supplies its own value).
    pub has_default: bool,
    /// Whether this definition arrived through an `ALTER TABLE` rather than the `CREATE TABLE`.
    /// A `NOT NULL` column with no `DEFAULT` added by an `ALTER` cannot be satisfied by rows
    /// that already exist — the design's §5 "add a required field" case.
    pub from_alter: bool,
}

/// The effective shape of one table after every migration has been applied in order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableShape {
    /// The table name as first written (unquoted, unqualified).
    pub name: String,
    /// The columns, in the order they came to exist.
    pub columns: Vec<ColumnDef>,
    /// The declared `PRIMARY KEY` columns, in declaration order. Empty when none was declared.
    pub primary_key: Vec<String>,
    /// The columns of a `UNIQUE` constraint / unique index, used as the key only when no
    /// `PRIMARY KEY` was declared (an author may key a table that way).
    pub unique_key: Vec<String>,
    /// Whether this run saw the table's full history. `false` when an `ALTER TABLE` touched a
    /// table no parsed `CREATE TABLE` produced — its shape is created elsewhere, so anything
    /// derived from it is unverifiable rather than wrong.
    pub complete: bool,
}

impl TableShape {
    /// The column of this name (compared case-insensitively, as unquoted SQL identifiers are).
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// The key columns: the `PRIMARY KEY` if one was declared, else a unique constraint/index.
    pub fn key(&self) -> &[String] {
        if self.primary_key.is_empty() {
            &self.unique_key
        } else {
            &self.primary_key
        }
    }
}

/// The outcome of replaying one store's migrations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlShape {
    /// Every statement was inside the subset. The map is keyed by the lowercased table name.
    Parsed(BTreeMap<String, TableShape>),
    /// A statement was outside the subset, so nothing about this store is provable. Carries the
    /// script and the honest reason, both of which the warning quotes.
    Unverifiable {
        /// The migration file (archive-relative, e.g. `migrations/accounts/V002__alter.sql`).
        file: String,
        /// Why the parse stopped — quoted verbatim in the diagnostic.
        reason: String,
    },
}

/// Replay `scripts` — `(archive-relative file name, SQL text)` pairs, **already in migration
/// order** — into the effective table shapes.
///
/// The first statement outside the subset stops the replay and yields
/// [`DdlShape::Unverifiable`]: a shape derived from a partially-understood script would be a
/// guess, and a guess is exactly what must not become an ERROR.
pub fn parse_migrations(scripts: &[(String, String)]) -> DdlShape {
    let mut tables: BTreeMap<String, TableShape> = BTreeMap::new();
    for (file, sql) in scripts {
        let tokens = match tokenize(sql) {
            Ok(tokens) => tokens,
            Err(reason) => {
                return DdlShape::Unverifiable {
                    file: file.clone(),
                    reason,
                }
            }
        };
        for statement in split_statements(&tokens) {
            if let Err(reason) = apply(&mut tables, &statement) {
                return DdlShape::Unverifiable {
                    file: file.clone(),
                    reason,
                };
            }
        }
    }
    DdlShape::Parsed(tables)
}

// ==========================================================================================
// Tokenizer
// ==========================================================================================

/// One lexical token. String literals keep no content — nothing in the subset reads one.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// An unquoted word or numeric literal (keyword, identifier, digits).
    Word(String),
    /// A quoted identifier (`"x"`, `` `x` ``, `[x]`) with the quoting stripped.
    Ident(String),
    /// A string literal (single-quoted, or a PostgreSQL `$tag$…$tag$` block).
    Str,
    /// A single punctuation character.
    Punct(char),
}

impl Tok {
    /// The identifier text of a name token (a bare word or a quoted identifier).
    fn ident(&self) -> Option<&str> {
        match self {
            Tok::Word(w) => Some(w),
            Tok::Ident(i) => Some(i),
            _ => None,
        }
    }

    /// Whether this is the (case-insensitive) unquoted keyword `word`. A *quoted* identifier is
    /// never a keyword — `"table"` is a column named table.
    fn is(&self, word: &str) -> bool {
        matches!(self, Tok::Word(w) if w.eq_ignore_ascii_case(word))
    }
}

/// Lex one script. Comments are dropped; an unterminated literal / identifier / comment is an
/// `Err`, which degrades the whole store (never a false error).
fn tokenize(sql: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // ---- comments ----
        if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'*') {
            let mut depth = 1;
            i += 2;
            while i < chars.len() && depth > 0 {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth > 0 {
                return Err("an unterminated /* block comment".to_string());
            }
            continue;
        }
        // ---- string literal ----
        if c == '\'' {
            i += 1;
            loop {
                match chars.get(i) {
                    None => return Err("an unterminated string literal".to_string()),
                    Some('\\') => i += 2, // MySQL-style backslash escape
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => i += 2,
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(_) => i += 1,
                }
            }
            out.push(Tok::Str);
            continue;
        }
        // ---- dollar-quoted body ($$ … $$ / $tag$ … $tag$) ----
        if c == '$' {
            if let Some(tag_end) = dollar_tag_end(&chars, i) {
                let tag: String = chars[i..=tag_end].iter().collect();
                let mut j = tag_end + 1;
                let tag_chars: Vec<char> = tag.chars().collect();
                loop {
                    if j + tag_chars.len() > chars.len() {
                        return Err(format!("an unterminated {tag} quoted block"));
                    }
                    if chars[j..j + tag_chars.len()] == tag_chars[..] {
                        j += tag_chars.len();
                        break;
                    }
                    j += 1;
                }
                i = j;
                out.push(Tok::Str);
                continue;
            }
            out.push(Tok::Punct('$'));
            i += 1;
            continue;
        }
        // ---- quoted identifiers ----
        if let Some(close) = matching_quote(c) {
            i += 1;
            let mut ident = String::new();
            loop {
                match chars.get(i) {
                    None => return Err(format!("an unterminated {c}quoted{close} identifier")),
                    Some(ch) if *ch == close && chars.get(i + 1) == Some(&close) => {
                        ident.push(close);
                        i += 2;
                    }
                    Some(ch) if *ch == close => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        ident.push(*ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Ident(ident));
            continue;
        }
        // ---- word ----
        if c.is_ascii_alphanumeric() || c == '_' || c == '#' || c == '@' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_ascii_alphanumeric()
                    || chars[i] == '_'
                    || chars[i] == '#'
                    || chars[i] == '@')
            {
                i += 1;
            }
            out.push(Tok::Word(chars[start..i].iter().collect()));
            continue;
        }
        out.push(Tok::Punct(c));
        i += 1;
    }
    Ok(out)
}

/// The index of the closing `$` of a dollar-quote tag starting at `start`, if this really is one
/// (`$$` or `$tag$` — a `$1` placeholder is not).
fn dollar_tag_end(chars: &[char], start: usize) -> Option<usize> {
    let mut i = start + 1;
    while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
        i += 1;
    }
    (chars.get(i) == Some(&'$')).then_some(i)
}

/// The closing quote for an opening identifier quote.
fn matching_quote(c: char) -> Option<char> {
    match c {
        '"' => Some('"'),
        '`' => Some('`'),
        '[' => Some(']'),
        _ => None,
    }
}

/// Cut the token stream into statements at top-level `;` and at the SQL Server batch separator
/// `GO`. A `GO` inside parentheses is left alone; a column genuinely named `go` would mis-split
/// and the statement would then fail to parse — degrading to a warning, which is the safe
/// direction.
fn split_statements(tokens: &[Tok]) -> Vec<Vec<Tok>> {
    let mut out = Vec::new();
    let mut current: Vec<Tok> = Vec::new();
    let mut depth = 0i32;
    for tok in tokens {
        match tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth -= 1,
            Tok::Punct(';') if depth <= 0 => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
                continue;
            }
            Tok::Word(w) if depth <= 0 && w.eq_ignore_ascii_case("go") => {
                if !current.is_empty() {
                    out.push(std::mem::take(&mut current));
                }
                continue;
            }
            _ => {}
        }
        current.push(tok.clone());
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

// ==========================================================================================
// Statement application
// ==========================================================================================

/// Leading keywords of statements that provably cannot change a table's columns or key. Every
/// one of them is common in a real migration, so skipping them is what keeps a package
/// verifiable at all; anything NOT on this list degrades.
const SHAPE_NEUTRAL: &[&[&str]] = &[
    &["INSERT"],
    &["UPDATE"],
    &["DELETE"],
    &["MERGE"],
    &["TRUNCATE"],
    &["SELECT"],
    &["SET"],
    &["USE"],
    &["GRANT"],
    &["REVOKE"],
    &["COMMENT"],
    &["ANALYZE"],
    &["ANALYSE"],
    &["VACUUM"],
    &["BEGIN"],
    &["COMMIT"],
    &["ROLLBACK"],
    &["START", "TRANSACTION"],
    &["CREATE", "INDEX"],
    &["CREATE", "SCHEMA"],
    &["CREATE", "SEQUENCE"],
    &["CREATE", "VIEW"],
    &["CREATE", "EXTENSION"],
    &["CREATE", "DATABASE"],
    &["CREATE", "TYPE"],
    &["ALTER", "SEQUENCE"],
    &["ALTER", "INDEX"],
    &["DROP", "INDEX"],
    &["DROP", "VIEW"],
    &["DROP", "SEQUENCE"],
    &["DROP", "TRIGGER"],
    &["DROP", "FUNCTION"],
    &["DROP", "PROCEDURE"],
    &["DROP", "TYPE"],
    &["DROP", "SCHEMA"],
];

/// Apply one statement to the accumulating shapes. `Err` = outside the subset (degrade).
fn apply(tables: &mut BTreeMap<String, TableShape>, statement: &[Tok]) -> Result<(), String> {
    let mut cur = Cursor::new(statement);
    if cur.eat_seq(&["CREATE", "UNIQUE", "INDEX"]) {
        return apply_unique_index(tables, &mut cur);
    }
    for prefix in SHAPE_NEUTRAL {
        let mut probe = Cursor::new(statement);
        if probe.eat_seq(prefix) {
            return Ok(());
        }
    }
    if cur.eat_seq(&["CREATE", "TABLE"]) {
        return apply_create_table(tables, &mut cur);
    }
    if cur.eat_seq(&["DROP", "TABLE"]) {
        cur.eat_seq(&["IF", "EXISTS"]);
        while let Some(name) = cur.next_name() {
            tables.remove(&name.to_ascii_lowercase());
            if !cur.eat_punct(',') {
                break;
            }
        }
        return Ok(());
    }
    if cur.eat_seq(&["ALTER", "TABLE"]) {
        return apply_alter_table(tables, &mut cur);
    }
    Err(format!(
        "the statement '{}' is outside the DDL subset this lint parses",
        preview(statement)
    ))
}

/// `CREATE TABLE [IF NOT EXISTS] <name> ( <definitions> ) [table options]`.
fn apply_create_table(
    tables: &mut BTreeMap<String, TableShape>,
    cur: &mut Cursor<'_>,
) -> Result<(), String> {
    let if_not_exists = cur.eat_seq(&["IF", "NOT", "EXISTS"]);
    let Some(name) = cur.next_name() else {
        return Err("a CREATE TABLE with no readable table name".to_string());
    };
    let Some(body) = cur.group() else {
        return Err(format!(
            "a CREATE TABLE '{name}' without a parenthesised column list (a CREATE TABLE … AS \
             SELECT / LIKE is outside the subset)"
        ));
    };
    let key = name.to_ascii_lowercase();
    if if_not_exists && tables.contains_key(&key) {
        return Ok(()); // the DDL is a no-op against an existing table; so is this
    }
    let mut shape = TableShape {
        name: name.clone(),
        complete: true,
        ..TableShape::default()
    };
    for item in split_commas(&body) {
        apply_table_item(&mut shape, &item, false)?;
    }
    tables.insert(key, shape);
    Ok(())
}

/// One entry of a `CREATE TABLE` body / one `ALTER TABLE ADD` action: either a table-level
/// constraint or a column definition.
fn apply_table_item(shape: &mut TableShape, item: &[Tok], from_alter: bool) -> Result<(), String> {
    let mut cur = Cursor::new(item);
    // `CONSTRAINT <name>` prefixes a table-level constraint.
    if cur.eat_seq(&["CONSTRAINT"]) {
        cur.next_name();
    }
    if cur.eat_seq(&["PRIMARY", "KEY"]) {
        // A `CLUSTERED` / `NONCLUSTERED` qualifier (SQL Server) sits before the column list.
        cur.eat_seq(&["CLUSTERED"]);
        cur.eat_seq(&["NONCLUSTERED"]);
        let Some(cols) = cur.group() else {
            return Err("a PRIMARY KEY constraint with no column list".to_string());
        };
        shape.primary_key = name_list(&cols);
        for column in shape.columns.iter_mut() {
            if shape
                .primary_key
                .iter()
                .any(|k| k.eq_ignore_ascii_case(&column.name))
            {
                column.nullable = false;
            }
        }
        return Ok(());
    }
    if cur.eat_seq(&["UNIQUE"]) {
        cur.eat_seq(&["KEY"]);
        cur.eat_seq(&["INDEX"]);
        cur.next_name_before_group();
        if let Some(cols) = cur.group() {
            if shape.unique_key.is_empty() {
                shape.unique_key = name_list(&cols);
            }
        }
        return Ok(());
    }
    if cur.eat_seq(&["FOREIGN", "KEY"])
        || cur.eat_seq(&["CHECK"])
        || cur.eat_seq(&["KEY"])
        || cur.eat_seq(&["INDEX"])
        || cur.eat_seq(&["FULLTEXT"])
        || cur.eat_seq(&["SPATIAL"])
        || cur.eat_seq(&["PERIOD"])
        || cur.eat_seq(&["EXCLUDE"])
    {
        return Ok(()); // shape-neutral table constraints
    }
    let column = parse_column_def(&mut cur, from_alter)?;
    if column.primary_key {
        shape.primary_key = vec![column.def.name.clone()];
    }
    upsert_column(shape, column.def);
    Ok(())
}

/// Insert or replace a column definition, preserving its position when it already exists.
fn upsert_column(shape: &mut TableShape, column: ColumnDef) {
    match shape
        .columns
        .iter_mut()
        .find(|c| c.name.eq_ignore_ascii_case(&column.name))
    {
        Some(existing) => *existing = column,
        None => shape.columns.push(column),
    }
}

/// `CREATE UNIQUE INDEX [IF NOT EXISTS] <name> ON <table> ( <columns> )`.
fn apply_unique_index(
    tables: &mut BTreeMap<String, TableShape>,
    cur: &mut Cursor<'_>,
) -> Result<(), String> {
    cur.eat_seq(&["IF", "NOT", "EXISTS"]);
    cur.next_name_before_word("ON");
    if !cur.eat_seq(&["ON"]) {
        return Ok(()); // not a form we model; an index never changes columns, so this is safe
    }
    let Some(table) = cur.next_name() else {
        return Ok(());
    };
    let Some(cols) = cur.group() else {
        return Ok(());
    };
    // A partial index (`WHERE …`) does not key the whole table.
    if cur.rest_has_word("WHERE") {
        return Ok(());
    }
    let shape = table_entry(tables, &table);
    if shape.unique_key.is_empty() {
        shape.unique_key = name_list(&cols);
    }
    Ok(())
}

/// The shape for `name`, creating an INCOMPLETE one when this run never saw its `CREATE TABLE`
/// (created elsewhere — unverifiable, never wrong).
fn table_entry<'a>(tables: &'a mut BTreeMap<String, TableShape>, name: &str) -> &'a mut TableShape {
    tables
        .entry(name.to_ascii_lowercase())
        .or_insert_with(|| TableShape {
            name: name.to_string(),
            complete: false,
            ..TableShape::default()
        })
}

/// `ALTER TABLE [IF EXISTS] [ONLY] <name> <action> [, <action> …]`.
fn apply_alter_table(
    tables: &mut BTreeMap<String, TableShape>,
    cur: &mut Cursor<'_>,
) -> Result<(), String> {
    cur.eat_seq(&["IF", "EXISTS"]);
    cur.eat_seq(&["ONLY"]);
    let Some(name) = cur.next_name() else {
        return Err("an ALTER TABLE with no readable table name".to_string());
    };
    let actions = split_commas(cur.remaining());
    let mut last_was_add_column = false;
    for action in &actions {
        // `RENAME TO` re-keys the whole map, so it is handled before the shape is borrowed.
        let mut probe = Cursor::new(action);
        if probe.eat_seq(&["RENAME", "TO"])
            || probe.eat_seq(&["RENAME"]) && !probe.peek_word_is("COLUMN")
        {
            let Some(new) = probe.next_name() else {
                return Err("an ALTER TABLE RENAME with no readable target".to_string());
            };
            let mut shape = tables
                .remove(&name.to_ascii_lowercase())
                .unwrap_or(TableShape {
                    name: name.clone(),
                    complete: false,
                    ..TableShape::default()
                });
            shape.name = new.clone();
            tables.insert(new.to_ascii_lowercase(), shape);
            last_was_add_column = false;
            continue;
        }
        let shape = table_entry(tables, &name);
        let mut act = Cursor::new(action);
        if act.eat_seq(&["ADD"]) {
            act.eat_seq(&["COLUMN"]);
            act.eat_seq(&["IF", "NOT", "EXISTS"]);
            apply_table_item(shape, act.remaining(), true)?;
            last_was_add_column = true;
            continue;
        }
        if act.eat_seq(&["DROP"]) {
            act.eat_seq(&["COLUMN"]);
            act.eat_seq(&["IF", "EXISTS"]);
            let Some(column) = act.next_name() else {
                return Err("an ALTER TABLE DROP with no readable column name".to_string());
            };
            shape
                .columns
                .retain(|c| !c.name.eq_ignore_ascii_case(&column));
            shape
                .primary_key
                .retain(|k| !k.eq_ignore_ascii_case(&column));
            shape
                .unique_key
                .retain(|k| !k.eq_ignore_ascii_case(&column));
            last_was_add_column = false;
            continue;
        }
        if act.eat_seq(&["ALTER"]) {
            act.eat_seq(&["COLUMN"]);
            apply_alter_column(shape, &mut act)?;
            last_was_add_column = false;
            continue;
        }
        if act.eat_seq(&["MODIFY"]) {
            act.eat_seq(&["COLUMN"]);
            apply_table_item(shape, act.remaining(), true)?;
            last_was_add_column = false;
            continue;
        }
        if act.eat_seq(&["CHANGE"]) {
            act.eat_seq(&["COLUMN"]);
            let Some(old) = act.next_name() else {
                return Err("an ALTER TABLE CHANGE with no readable column name".to_string());
            };
            shape.columns.retain(|c| !c.name.eq_ignore_ascii_case(&old));
            rename_key(shape, &old, None);
            apply_table_item(shape, act.remaining(), true)?;
            last_was_add_column = false;
            continue;
        }
        if act.eat_seq(&["RENAME", "COLUMN"]) {
            let (Some(old), true, Some(new)) =
                (act.next_name(), act.eat_seq(&["TO"]), act.next_name())
            else {
                return Err(
                    "an ALTER TABLE RENAME COLUMN outside the `old TO new` form".to_string()
                );
            };
            if let Some(column) = shape
                .columns
                .iter_mut()
                .find(|c| c.name.eq_ignore_ascii_case(&old))
            {
                column.name = new.clone();
            }
            rename_key(shape, &old, Some(&new));
            last_was_add_column = false;
            continue;
        }
        // SQL Server's `ALTER TABLE t ADD a INT, b INT` repeats no keyword on later columns.
        if last_was_add_column {
            apply_table_item(shape, action, true)?;
            continue;
        }
        return Err(format!(
            "the ALTER TABLE action '{}' is outside the DDL subset this lint parses",
            preview(action)
        ));
    }
    Ok(())
}

/// Rewrite (or drop) a column name inside the key lists after a rename/removal.
fn rename_key(shape: &mut TableShape, old: &str, new: Option<&str>) {
    for key in [&mut shape.primary_key, &mut shape.unique_key] {
        match new {
            Some(new) => {
                for entry in key.iter_mut() {
                    if entry.eq_ignore_ascii_case(old) {
                        *entry = new.to_string();
                    }
                }
            }
            None => key.retain(|k| !k.eq_ignore_ascii_case(old)),
        }
    }
}

/// `ALTER [COLUMN] <name> …` — the PostgreSQL `TYPE`/`SET NOT NULL`/`SET DEFAULT` forms and the
/// SQL Server `<name> <type> [NULL|NOT NULL]` form.
fn apply_alter_column(shape: &mut TableShape, cur: &mut Cursor<'_>) -> Result<(), String> {
    let Some(name) = cur.next_name() else {
        return Err("an ALTER COLUMN with no readable column name".to_string());
    };
    let existing = shape.column(&name).cloned();
    if cur.eat_seq(&["SET", "DATA", "TYPE"]) || cur.eat_seq(&["TYPE"]) {
        let (declared_type, ty) = parse_type(cur)?;
        let mut column = existing.unwrap_or(ColumnDef {
            name: name.clone(),
            declared_type: declared_type.clone(),
            ty: ty.clone(),
            nullable: true,
            has_default: false,
            from_alter: true,
        });
        column.declared_type = declared_type;
        column.ty = ty;
        upsert_column(shape, column);
        return Ok(());
    }
    if cur.eat_seq(&["SET", "NOT", "NULL"]) || cur.eat_seq(&["DROP", "NOT", "NULL"]) {
        let set = cur.first_word_was("SET");
        if let Some(mut column) = existing {
            column.nullable = !set;
            column.from_alter = true;
            upsert_column(shape, column);
        }
        return Ok(());
    }
    if cur.eat_seq(&["SET", "DEFAULT"]) || cur.eat_seq(&["DROP", "DEFAULT"]) {
        let set = cur.first_word_was("SET");
        if let Some(mut column) = existing {
            column.has_default = set;
            upsert_column(shape, column);
        }
        return Ok(());
    }
    // SQL Server: `ALTER COLUMN <name> <type> [NULL | NOT NULL]`.
    if cur.peek_is_type_start() {
        let (declared_type, ty) = parse_type(cur)?;
        let mut nullable = existing.as_ref().is_none_or(|c| c.nullable);
        if cur.eat_seq(&["NOT", "NULL"]) {
            nullable = false;
        } else if cur.eat_seq(&["NULL"]) {
            nullable = true;
        }
        upsert_column(
            shape,
            ColumnDef {
                name,
                declared_type,
                ty,
                nullable,
                has_default: existing.is_some_and(|c| c.has_default),
                from_alter: true,
            },
        );
        return Ok(());
    }
    Err(format!(
        "the ALTER COLUMN form '{}' is outside the DDL subset this lint parses",
        preview(cur.all())
    ))
}

/// A parsed column definition plus whether it carried an inline `PRIMARY KEY`.
struct ParsedColumn {
    def: ColumnDef,
    primary_key: bool,
}

/// `<name> <type> [constraints…]`. Unrecognised trailing clauses are skipped deliberately: they
/// cannot change the name or the type, and degrading on a `COLLATE` or a `COMMENT` would make
/// most real DDL unverifiable.
fn parse_column_def(cur: &mut Cursor<'_>, from_alter: bool) -> Result<ParsedColumn, String> {
    let Some(name) = cur.next_name() else {
        return Err(format!(
            "the column definition '{}' has no readable name",
            preview(cur.all())
        ));
    };
    let (declared_type, ty) = parse_type(cur)?;
    let mut nullable = true;
    let mut has_default = matches!(ty, SqlType::Integer { .. })
        && declared_type.to_ascii_uppercase().contains("SERIAL");
    let mut primary_key = false;
    while !cur.done() {
        if cur.eat_seq(&["NOT", "NULL"]) {
            nullable = false;
        } else if cur.eat_seq(&["PRIMARY", "KEY"]) {
            primary_key = true;
            nullable = false;
        } else if cur.eat_seq(&["NULL"]) {
            nullable = true;
        } else if cur.eat_seq(&["DEFAULT"])
            // A column that supplies its own value needs no bind, exactly like a DEFAULT.
            || cur.eat_seq(&["IDENTITY"])
            || cur.eat_seq(&["AUTO_INCREMENT"])
            || cur.eat_seq(&["AUTOINCREMENT"])
            || cur.eat_seq(&["GENERATED"])
        {
            has_default = true;
            cur.skip_expression();
        } else {
            cur.skip_one();
        }
    }
    Ok(ParsedColumn {
        def: ColumnDef {
            name,
            declared_type,
            ty,
            nullable,
            has_default,
            from_alter,
        },
        primary_key,
    })
}

/// Parse a type reference: the raw spelling (for diagnostics) and its [`SqlType`].
fn parse_type(cur: &mut Cursor<'_>) -> Result<(String, SqlType), String> {
    let Some(first) = cur.next_word() else {
        return Err(format!(
            "the column definition '{}' has no type",
            preview(cur.all())
        ));
    };
    let mut spelling = first.clone();
    let mut words = vec![first.to_ascii_uppercase()];
    // Multi-word type names: CHARACTER VARYING, DOUBLE PRECISION, NATIONAL CHARACTER …
    for extra in ["VARYING", "PRECISION", "CHARACTER", "CHAR"] {
        if words.len() < 3 && cur.peek_word_is(extra) {
            let word = cur.next_word().unwrap_or_default();
            spelling.push(' ');
            spelling.push_str(&word);
            words.push(word.to_ascii_uppercase());
        }
    }
    let mut args: Vec<String> = Vec::new();
    if cur.peek_punct('(') {
        if let Some(group) = cur.group() {
            args = name_list(&group);
            spelling.push('(');
            spelling.push_str(&args.join(","));
            spelling.push(')');
        }
    }
    // TIMESTAMP/TIME [WITH|WITHOUT] TIME ZONE
    if cur.eat_seq(&["WITH", "TIME", "ZONE"]) {
        spelling.push_str(" WITH TIME ZONE");
    } else if cur.eat_seq(&["WITHOUT", "TIME", "ZONE"]) {
        spelling.push_str(" WITHOUT TIME ZONE");
    }
    if cur.eat_seq(&["UNSIGNED"]) {
        spelling.push_str(" UNSIGNED");
    }
    let ty = classify_type(&words.join(" "), &args);
    Ok((spelling, ty))
}

/// Map a normalised (uppercased, space-joined) type name plus its arguments to a [`SqlType`].
fn classify_type(name: &str, args: &[String]) -> SqlType {
    let arg = |i: usize| -> Option<u64> { args.get(i).and_then(|a| a.parse::<u64>().ok()) };
    let is_max = args.first().is_some_and(|a| a.eq_ignore_ascii_case("max"));
    match name {
        "VARCHAR" | "NVARCHAR" | "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" | "CHARACTER"
        | "CHARACTER VARYING" | "NATIONAL CHARACTER" | "NATIONAL CHAR" | "CHAR VARYING"
        | "NCHAR VARYING" => SqlType::Text {
            max_chars: if is_max { None } else { arg(0) },
        },
        "TEXT" | "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" | "NTEXT" | "CLOB" | "NCLOB"
        | "LONG VARCHAR" | "STRING" => SqlType::Text { max_chars: None },
        "NUMERIC" | "DECIMAL" | "DEC" | "NUMBER" => SqlType::Numeric {
            precision: arg(0),
            scale: arg(1).or(Some(0).filter(|_| args.len() == 1)),
        },
        "TINYINT" => {
            if arg(0) == Some(1) {
                SqlType::Boolean
            } else {
                SqlType::Integer { bits: 8 }
            }
        }
        "SMALLINT" | "INT2" | "SMALLSERIAL" | "SERIAL2" => SqlType::Integer { bits: 16 },
        "MEDIUMINT" => SqlType::Integer { bits: 24 },
        "INT" | "INTEGER" | "INT4" | "SERIAL" | "SERIAL4" => SqlType::Integer { bits: 32 },
        "BIGINT" | "INT8" | "BIGSERIAL" | "SERIAL8" => SqlType::Integer { bits: 64 },
        "BOOLEAN" | "BOOL" => SqlType::Boolean,
        "BIT" => {
            if args.is_empty() || arg(0) == Some(1) {
                SqlType::Boolean
            } else {
                SqlType::Unknown(format!("BIT({})", args.join(",")))
            }
        }
        "DATE" => SqlType::Date,
        "TIME" | "TIMETZ" => SqlType::Time,
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" | "DATETIME2" | "SMALLDATETIME"
        | "DATETIMEOFFSET" => SqlType::Timestamp,
        "BYTEA" | "BLOB" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" | "IMAGE" => {
            SqlType::Binary { max_bytes: None }
        }
        "BINARY" | "VARBINARY" => SqlType::Binary {
            max_bytes: if is_max { None } else { arg(0) },
        },
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" => SqlType::Float,
        other => SqlType::Unknown(other.to_string()),
    }
}

/// The first few tokens of a statement, for a diagnostic that has to name what it choked on
/// without echoing a whole script (or a literal that might be a secret).
fn preview(tokens: &[Tok]) -> String {
    let mut out = String::new();
    for tok in tokens.iter().take(6) {
        let text = match tok {
            Tok::Word(w) => w.clone(),
            Tok::Ident(i) => format!("\"{i}\""),
            Tok::Str => "'…'".to_string(),
            Tok::Punct(c) => c.to_string(),
        };
        if !out.is_empty() && !matches!(tok, Tok::Punct(_)) {
            out.push(' ');
        }
        out.push_str(&text);
    }
    if tokens.len() > 6 {
        out.push_str(" …");
    }
    out
}

/// Split a token slice on top-level commas.
fn split_commas(tokens: &[Tok]) -> Vec<Vec<Tok>> {
    let mut out = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0i32;
    for tok in tokens {
        match tok {
            Tok::Punct('(') => depth += 1,
            Tok::Punct(')') => depth -= 1,
            Tok::Punct(',') if depth == 0 => {
                out.push(std::mem::take(&mut current));
                continue;
            }
            _ => {}
        }
        current.push(tok.clone());
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// The identifier / literal words of a comma-separated list (`(a, b DESC)` → `["a", "b"]`).
fn name_list(tokens: &[Tok]) -> Vec<String> {
    split_commas(tokens)
        .iter()
        .filter_map(|item| item.first().and_then(|t| t.ident()).map(str::to_string))
        .collect()
}

// ==========================================================================================
// Cursor
// ==========================================================================================

/// A forward-only cursor over one statement's tokens.
struct Cursor<'a> {
    tokens: &'a [Tok],
    pos: usize,
    /// The first word of the most recent `eat_seq` alternation — lets a caller distinguish
    /// `SET NOT NULL` from `DROP NOT NULL` after matching either.
    last_seq_head: String,
}

impl<'a> Cursor<'a> {
    fn new(tokens: &'a [Tok]) -> Cursor<'a> {
        Cursor {
            tokens,
            pos: 0,
            last_seq_head: String::new(),
        }
    }

    fn all(&self) -> &'a [Tok] {
        self.tokens
    }

    fn remaining(&self) -> &'a [Tok] {
        &self.tokens[self.pos.min(self.tokens.len())..]
    }

    fn done(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn skip_one(&mut self) {
        if self.peek_punct('(') {
            let _ = self.group();
        } else {
            self.pos += 1;
        }
    }

    /// Skip a `DEFAULT` / `GENERATED` / `IDENTITY` expression: a parenthesised group, or one
    /// token plus its optional argument list (`nextval('s')`, `IDENTITY(1,1)`). Whatever is left
    /// over is skipped token-by-token by the constraint scan, which recognises the keywords it
    /// cares about wherever they appear.
    fn skip_expression(&mut self) {
        if self.peek_punct('(') {
            let _ = self.group();
            return;
        }
        if !self.done() {
            self.pos += 1;
        }
        if self.peek_punct('(') {
            let _ = self.group();
        }
    }

    /// Match a whole keyword sequence, consuming it only on a full match.
    fn eat_seq(&mut self, words: &[&str]) -> bool {
        let mut i = self.pos;
        for word in words {
            match self.tokens.get(i) {
                Some(tok) if tok.is(word) => i += 1,
                _ => return false,
            }
        }
        self.last_seq_head = words.first().unwrap_or(&"").to_string();
        self.pos = i;
        true
    }

    /// Whether the sequence matched by the last [`Cursor::eat_seq`] started with `word`.
    fn first_word_was(&self, word: &str) -> bool {
        self.last_seq_head.eq_ignore_ascii_case(word)
    }

    fn eat_punct(&mut self, c: char) -> bool {
        if matches!(self.tokens.get(self.pos), Some(Tok::Punct(p)) if *p == c) {
            self.pos += 1;
            return true;
        }
        false
    }

    fn peek_punct(&self, c: char) -> bool {
        matches!(self.tokens.get(self.pos), Some(Tok::Punct(p)) if *p == c)
    }

    fn peek_word_is(&self, word: &str) -> bool {
        self.tokens.get(self.pos).is_some_and(|t| t.is(word))
    }

    /// Whether the next token could start a type name (the SQL Server
    /// `ALTER COLUMN <name> <type>` form). The `ALTER TABLE` action verbs are excluded, so an
    /// unmodelled `ALTER COLUMN c SET STORAGE PLAIN` degrades instead of being read as a type
    /// named `SET`.
    fn peek_is_type_start(&self) -> bool {
        const NOT_TYPES: &[&str] = &[
            "SET", "DROP", "ADD", "RESET", "RESTART", "USING", "OWNER", "SCHEMA", "ENABLE",
            "DISABLE", "VALIDATE", "ATTACH", "DETACH", "INHERIT", "WITH", "WITHOUT",
        ];
        matches!(self.tokens.get(self.pos), Some(Tok::Word(w))
            if !NOT_TYPES.iter().any(|n| w.eq_ignore_ascii_case(n)))
    }

    fn next_word(&mut self) -> Option<String> {
        match self.tokens.get(self.pos) {
            Some(Tok::Word(w)) => {
                self.pos += 1;
                Some(w.clone())
            }
            _ => None,
        }
    }

    /// The next (possibly qualified, possibly quoted) name, reduced to its last segment —
    /// `public."Account"` → `Account`.
    fn next_name(&mut self) -> Option<String> {
        let mut name = self.tokens.get(self.pos)?.ident()?.to_string();
        self.pos += 1;
        while self.peek_punct('.') {
            self.pos += 1;
            match self.tokens.get(self.pos).and_then(|t| t.ident()) {
                Some(next) => {
                    name = next.to_string();
                    self.pos += 1;
                }
                None => break,
            }
        }
        Some(name)
    }

    /// Consume names until the next `(` — an index name that may be qualified.
    fn next_name_before_group(&mut self) {
        while !self.done() && !self.peek_punct('(') {
            self.pos += 1;
        }
    }

    /// Consume tokens until the given keyword (exclusive) — an index name before its `ON`.
    fn next_name_before_word(&mut self, word: &str) {
        while !self.done() && !self.peek_word_is(word) {
            self.pos += 1;
        }
    }

    fn rest_has_word(&self, word: &str) -> bool {
        self.remaining().iter().any(|t| t.is(word))
    }

    /// Consume a balanced parenthesised group and return its interior.
    fn group(&mut self) -> Option<Vec<Tok>> {
        if !self.eat_punct('(') {
            return None;
        }
        let start = self.pos;
        let mut depth = 1i32;
        while self.pos < self.tokens.len() {
            match self.tokens[self.pos] {
                Tok::Punct('(') => depth += 1,
                Tok::Punct(')') => {
                    depth -= 1;
                    if depth == 0 {
                        let inner = self.tokens[start..self.pos].to_vec();
                        self.pos += 1;
                        return Some(inner);
                    }
                }
                _ => {}
            }
            self.pos += 1;
        }
        None
    }
}

// ==========================================================================================
// Facet ⇄ column compatibility
// ==========================================================================================

/// The verdict of comparing one declared field against one column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fit {
    /// The column can hold the declared value space.
    Fits,
    /// A definite fault — the reason clause names both sides.
    Mismatch(String),
    /// Not provable either way (an uncomparable column type, an unbounded declared type in a
    /// bounded column, a declared type with no ruled column mapping). **Never an error.**
    Unprovable(String),
}

/// Compare one declared scalar (its builtin plus the effective facets of its restriction chain)
/// against a column, per the advisory type mapping (design §4.5).
///
/// The comparison is deliberately asymmetric: a **provable** overflow of the declared value
/// space (a `maxLength = 35` field in `VARCHAR(10)`, a fractional decimal in an integer column,
/// `totalDigits`/`fractionDigits` that do not fit the column's precision/scale) is a
/// [`Fit::Mismatch`]; everything that merely *might* not fit is [`Fit::Unprovable`].
pub fn column_fit(builtin: Builtin, facets: &FieldFacets, column: &ColumnDef) -> Fit {
    if let SqlType::Unknown(name) = &column.ty {
        return Fit::Unprovable(format!(
            "column type '{name}' is outside the set this lint compares"
        ));
    }
    match family_of(builtin) {
        Family::Text => text_fit(facets, column),
        Family::Decimal => decimal_fit(facets, column),
        Family::Integral => integral_fit(builtin, facets, column),
        Family::Boolean => match column.ty {
            SqlType::Boolean => Fit::Fits,
            _ => Fit::Mismatch(mismatch(builtin, column)),
        },
        Family::Date => match column.ty {
            // A date widens into an instant column without loss; the reverse does not.
            SqlType::Date | SqlType::Timestamp => Fit::Fits,
            _ => Fit::Mismatch(mismatch(builtin, column)),
        },
        Family::Time => match column.ty {
            SqlType::Time => Fit::Fits,
            _ => Fit::Mismatch(mismatch(builtin, column)),
        },
        Family::DateTime => match column.ty {
            SqlType::Timestamp => Fit::Fits,
            _ => Fit::Mismatch(mismatch(builtin, column)),
        },
        Family::Binary => match column.ty {
            SqlType::Binary { max_bytes } => match (max_bytes, facets.max_length.or(facets.length))
            {
                (Some(cap), Some(declared)) if declared > cap => Fit::Mismatch(format!(
                    "declared maxLength {declared} does not fit column type '{}'",
                    column.declared_type
                )),
                _ => Fit::Fits,
            },
            _ => Fit::Mismatch(mismatch(builtin, column)),
        },
        Family::Unmapped => Fit::Unprovable(format!(
            "declared type '{builtin:?}' has no ruled column mapping, so column type '{}' can be \
             neither confirmed nor refuted",
            column.declared_type
        )),
    }
}

/// The advisory-mapping family a declared builtin belongs to.
enum Family {
    Text,
    Decimal,
    Integral,
    Boolean,
    Date,
    Time,
    DateTime,
    Binary,
    /// No row in the §4.5 mapping table (`xs:gYear`, `xs:gYearMonth`).
    Unmapped,
}

fn family_of(builtin: Builtin) -> Family {
    match builtin {
        Builtin::String => Family::Text,
        Builtin::Decimal => Family::Decimal,
        Builtin::Boolean => Family::Boolean,
        Builtin::Date => Family::Date,
        Builtin::DateTime => Family::DateTime,
        Builtin::Time => Family::Time,
        Builtin::Base64Binary => Family::Binary,
        Builtin::GYear | Builtin::GYearMonth => Family::Unmapped,
        Builtin::Integer
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
        | Builtin::UnsignedByte => Family::Integral,
    }
}

fn mismatch(builtin: Builtin, column: &ColumnDef) -> String {
    format!(
        "a declared '{builtin:?}' value cannot be stored in column type '{}'",
        column.declared_type
    )
}

fn text_fit(facets: &FieldFacets, column: &ColumnDef) -> Fit {
    let SqlType::Text { max_chars } = column.ty else {
        return Fit::Mismatch(format!(
            "a declared string cannot be stored in column type '{}'",
            column.declared_type
        ));
    };
    let Some(cap) = max_chars else {
        return Fit::Fits; // TEXT / NVARCHAR(MAX) hold any declared length
    };
    // An enumeration bounds the value space even when no length facet does.
    let declared = facets
        .length
        .into_iter()
        .chain(facets.max_length)
        .min()
        .or_else(|| {
            facets.enumeration.as_ref().map(|values| {
                values
                    .iter()
                    .map(|v| v.chars().count() as u64)
                    .max()
                    .unwrap_or(0)
            })
        });
    match declared {
        Some(declared) if declared > cap => Fit::Mismatch(format!(
            "declared maxLength {declared} does not fit column type '{}'",
            column.declared_type
        )),
        Some(_) => Fit::Fits,
        None => Fit::Unprovable(format!(
            "the declared string is unbounded (no maxLength / length / enumeration facet) while \
             column type '{}' is capped, so a value could be rejected or truncated",
            column.declared_type
        )),
    }
}

fn decimal_fit(facets: &FieldFacets, column: &ColumnDef) -> Fit {
    match column.ty {
        SqlType::Numeric { precision, scale } => {
            if let (Some(scale), Some(fraction)) = (scale, facets.fraction_digits) {
                if fraction > scale {
                    return Fit::Mismatch(format!(
                        "declared fractionDigits {fraction} does not fit column type '{}'",
                        column.declared_type
                    ));
                }
            }
            if let (Some(precision), Some(total)) = (precision, facets.total_digits) {
                if total > precision {
                    return Fit::Mismatch(format!(
                        "declared totalDigits {total} does not fit column type '{}'",
                        column.declared_type
                    ));
                }
                if let (Some(scale), Some(fraction)) = (scale, facets.fraction_digits) {
                    if total.saturating_sub(fraction) > precision.saturating_sub(scale) {
                        return Fit::Mismatch(format!(
                            "declared totalDigits {total} / fractionDigits {fraction} needs {} \
                             integral digits, more than column type '{}' holds",
                            total.saturating_sub(fraction),
                            column.declared_type
                        ));
                    }
                }
            }
            Fit::Fits
        }
        SqlType::Integer { bits } => match facets.fraction_digits {
            Some(0) => match facets.total_digits {
                Some(total) if total > max_digits(bits) => Fit::Mismatch(format!(
                    "declared totalDigits {total} does not fit column type '{}'",
                    column.declared_type
                )),
                _ => Fit::Fits,
            },
            Some(fraction) => Fit::Mismatch(format!(
                "a fractional decimal (fractionDigits {fraction}) cannot be stored in integer \
                 column type '{}'",
                column.declared_type
            )),
            None => Fit::Unprovable(format!(
                "the declared decimal has no fractionDigits facet, so whether it fits integer \
                 column type '{}' cannot be decided",
                column.declared_type
            )),
        },
        _ => Fit::Mismatch(format!(
            "a declared decimal cannot be stored in column type '{}'",
            column.declared_type
        )),
    }
}

fn integral_fit(builtin: Builtin, facets: &FieldFacets, column: &ColumnDef) -> Fit {
    let digits = facets.total_digits;
    match column.ty {
        SqlType::Integer { bits } => {
            if let Some(total) = digits {
                return if total > max_digits(bits) {
                    Fit::Mismatch(format!(
                        "declared totalDigits {total} does not fit column type '{}'",
                        column.declared_type
                    ))
                } else {
                    Fit::Fits
                };
            }
            match required_bits(builtin) {
                // An arbitrary-precision integer (xs:integer and its sign-restricted kin) has no
                // width to compare; an overflow raises rather than truncating, so this stays
                // silent instead of warning on every such field.
                None => Fit::Fits,
                Some(required) if required > bits => Fit::Mismatch(format!(
                    "declared '{builtin:?}' needs {required} bits, more than column type '{}' \
                     holds",
                    column.declared_type
                )),
                Some(_) => Fit::Fits,
            }
        }
        SqlType::Numeric { precision, scale } => match (precision, digits) {
            (Some(precision), Some(total))
                if total > precision.saturating_sub(scale.unwrap_or(0)) =>
            {
                Fit::Mismatch(format!(
                    "declared totalDigits {total} does not fit column type '{}'",
                    column.declared_type
                ))
            }
            _ => Fit::Fits,
        },
        _ => Fit::Mismatch(format!(
            "a declared integer cannot be stored in column type '{}'",
            column.declared_type
        )),
    }
}

/// The largest number of decimal digits a signed integer column of this width always holds.
fn max_digits(bits: u32) -> u64 {
    match bits {
        0..=8 => 2,
        9..=16 => 4,
        17..=24 => 7,
        25..=32 => 9,
        _ => 18,
    }
}

/// The signed width a declared integral builtin needs, or `None` when it is arbitrary-precision.
fn required_bits(builtin: Builtin) -> Option<u32> {
    Some(match builtin {
        Builtin::Byte => 8,
        Builtin::Short | Builtin::UnsignedByte => 16,
        Builtin::Int | Builtin::UnsignedShort => 32,
        Builtin::Long | Builtin::UnsignedInt => 64,
        Builtin::UnsignedLong => 65,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shapes(sql: &str) -> BTreeMap<String, TableShape> {
        match parse_migrations(&[("V001__t.sql".to_string(), sql.to_string())]) {
            DdlShape::Parsed(tables) => tables,
            DdlShape::Unverifiable { reason, .. } => panic!("expected a parse, got: {reason}"),
        }
    }

    fn one(sql: &str) -> TableShape {
        let tables = shapes(sql);
        assert_eq!(tables.len(), 1, "expected exactly one table: {tables:?}");
        tables.into_values().next().unwrap()
    }

    fn unverifiable(sql: &str) -> String {
        match parse_migrations(&[("V001__t.sql".to_string(), sql.to_string())]) {
            DdlShape::Unverifiable { reason, .. } => reason,
            DdlShape::Parsed(tables) => panic!("expected a degrade, parsed: {tables:?}"),
        }
    }

    #[test]
    fn type_spellings_across_dialects_map_to_one_model() {
        // (declared type, expected parse) — the §4.5 mapping's three dialect columns.
        let cases: &[(&str, SqlType)] = &[
            (
                "VARCHAR(35)",
                SqlType::Text {
                    max_chars: Some(35),
                },
            ),
            (
                "CHARACTER VARYING(35)",
                SqlType::Text {
                    max_chars: Some(35),
                },
            ),
            (
                "NVARCHAR(35)",
                SqlType::Text {
                    max_chars: Some(35),
                },
            ),
            ("NVARCHAR(MAX)", SqlType::Text { max_chars: None }),
            ("TEXT", SqlType::Text { max_chars: None }),
            ("LONGTEXT", SqlType::Text { max_chars: None }),
            (
                "NUMERIC(18,5)",
                SqlType::Numeric {
                    precision: Some(18),
                    scale: Some(5),
                },
            ),
            (
                "DECIMAL(18,5)",
                SqlType::Numeric {
                    precision: Some(18),
                    scale: Some(5),
                },
            ),
            ("SMALLINT", SqlType::Integer { bits: 16 }),
            ("INT", SqlType::Integer { bits: 32 }),
            ("INTEGER", SqlType::Integer { bits: 32 }),
            ("BIGINT", SqlType::Integer { bits: 64 }),
            ("BOOLEAN", SqlType::Boolean),
            ("TINYINT(1)", SqlType::Boolean),
            ("TINYINT", SqlType::Integer { bits: 8 }),
            ("BIT", SqlType::Boolean),
            ("DATE", SqlType::Date),
            ("TIMESTAMP", SqlType::Timestamp),
            ("TIMESTAMP WITH TIME ZONE", SqlType::Timestamp),
            ("TIMESTAMPTZ", SqlType::Timestamp),
            ("DATETIME", SqlType::Timestamp),
            ("DATETIME2", SqlType::Timestamp),
            ("TIME", SqlType::Time),
            ("BYTEA", SqlType::Binary { max_bytes: None }),
            ("BLOB", SqlType::Binary { max_bytes: None }),
            ("VARBINARY(MAX)", SqlType::Binary { max_bytes: None }),
            (
                "VARBINARY(64)",
                SqlType::Binary {
                    max_bytes: Some(64),
                },
            ),
            ("DOUBLE PRECISION", SqlType::Float),
            ("JSONB", SqlType::Unknown("JSONB".to_string())),
        ];
        for (spelling, expected) in cases {
            let table = one(&format!("CREATE TABLE t (c {spelling});"));
            assert_eq!(
                table.columns[0].ty, *expected,
                "type spelling {spelling} parsed as {:?}",
                table.columns[0].ty
            );
        }
    }

    #[test]
    fn create_table_captures_names_nullability_defaults_and_key() {
        let table = one(r#"-- a comment
            CREATE TABLE IF NOT EXISTS "public"."Account" (
              account_id  VARCHAR(35)  NOT NULL,
              opened_at   TIMESTAMP    NULL DEFAULT CURRENT_TIMESTAMP,
              balance     NUMERIC(18,2) NOT NULL DEFAULT 0,
              /* block */ note        TEXT,
              PRIMARY KEY (account_id)
            );"#);
        assert_eq!(table.name, "Account");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["account_id", "opened_at", "balance", "note"]
        );
        assert!(!table.column("account_id").unwrap().nullable);
        assert!(table.column("opened_at").unwrap().nullable);
        assert!(table.column("opened_at").unwrap().has_default);
        assert!(table.column("note").unwrap().nullable);
        assert!(!table.column("note").unwrap().has_default);
        assert_eq!(table.key(), ["account_id"]);
        assert!(table.complete);
    }

    #[test]
    fn inline_primary_key_marks_the_column_not_null() {
        let table = one("CREATE TABLE t (id VARCHAR(20) PRIMARY KEY, x INT);");
        assert_eq!(table.key(), ["id"]);
        assert!(!table.column("id").unwrap().nullable);
    }

    #[test]
    fn later_migrations_apply_in_order() {
        let scripts = vec![
            (
                "V001__init.sql".to_string(),
                "CREATE TABLE acct (id VARCHAR(20) NOT NULL, tmp INT, PRIMARY KEY (id));"
                    .to_string(),
            ),
            (
                "V002__alter.sql".to_string(),
                "ALTER TABLE acct ADD COLUMN opened_at DATE NULL;\n\
                 ALTER TABLE acct DROP COLUMN tmp;\n\
                 ALTER TABLE acct ALTER COLUMN id TYPE VARCHAR(35);"
                    .to_string(),
            ),
        ];
        let DdlShape::Parsed(tables) = parse_migrations(&scripts) else {
            panic!("expected a parse");
        };
        let table = &tables["acct"];
        assert!(table.column("tmp").is_none(), "V002 dropped tmp");
        assert_eq!(
            table.column("id").unwrap().ty,
            SqlType::Text {
                max_chars: Some(35)
            }
        );
        assert!(table.column("opened_at").is_some());
        assert!(table.column("opened_at").unwrap().from_alter);
    }

    #[test]
    fn alter_forms_across_dialects_are_understood() {
        // Each pair applies to the same starting table; the assertion is the resulting column.
        let base = "CREATE TABLE t (id INT NOT NULL, name VARCHAR(10));";
        let cases: &[(&str, &str, SqlType, bool)] = &[
            (
                "ALTER TABLE t ALTER COLUMN name TYPE VARCHAR(70);",
                "name",
                SqlType::Text {
                    max_chars: Some(70),
                },
                true,
            ),
            (
                "ALTER TABLE t MODIFY COLUMN name VARCHAR(70) NOT NULL;",
                "name",
                SqlType::Text {
                    max_chars: Some(70),
                },
                false,
            ),
            (
                "ALTER TABLE t ALTER COLUMN name NVARCHAR(70) NOT NULL;",
                "name",
                SqlType::Text {
                    max_chars: Some(70),
                },
                false,
            ),
            (
                "ALTER TABLE t CHANGE COLUMN name label VARCHAR(70);",
                "label",
                SqlType::Text {
                    max_chars: Some(70),
                },
                true,
            ),
            (
                "ALTER TABLE t RENAME COLUMN name TO label;",
                "label",
                SqlType::Text {
                    max_chars: Some(10),
                },
                true,
            ),
            (
                "ALTER TABLE t ADD opened DATE, closed DATE;",
                "closed",
                SqlType::Date,
                true,
            ),
        ];
        for (alter, column, ty, nullable) in cases {
            let table = one(&format!("{base}\n{alter}"));
            let found = table
                .column(column)
                .unwrap_or_else(|| panic!("{alter} should leave column {column}"));
            assert_eq!(found.ty, *ty, "{alter}");
            assert_eq!(found.nullable, *nullable, "{alter}");
        }
    }

    #[test]
    fn alter_can_set_and_drop_not_null_and_default() {
        let table = one("CREATE TABLE t (id INT, x INT NOT NULL DEFAULT 1);\n\
             ALTER TABLE t ALTER COLUMN id SET NOT NULL;\n\
             ALTER TABLE t ALTER COLUMN x DROP NOT NULL;\n\
             ALTER TABLE t ALTER COLUMN x DROP DEFAULT;");
        assert!(!table.column("id").unwrap().nullable);
        assert!(table.column("x").unwrap().nullable);
        assert!(!table.column("x").unwrap().has_default);
    }

    #[test]
    fn key_comes_from_a_table_constraint_an_alter_or_a_unique_index() {
        let by_alter = one("CREATE TABLE t (id INT NOT NULL);\n\
             ALTER TABLE t ADD CONSTRAINT t_pk PRIMARY KEY (id);");
        assert_eq!(by_alter.key(), ["id"]);
        let by_index = one("CREATE TABLE t (id INT NOT NULL);\n\
             CREATE UNIQUE INDEX t_id ON t (id);");
        assert_eq!(by_index.key(), ["id"]);
        let keyless = one("CREATE TABLE t (id INT NOT NULL);");
        assert!(keyless.key().is_empty());
    }

    #[test]
    fn shape_neutral_statements_never_degrade() {
        // Every one of these is common in a real migration and cannot change a column or a key.
        let table = one("CREATE TABLE t (id INT PRIMARY KEY, note TEXT);\n\
             CREATE INDEX t_note ON t (note);\n\
             INSERT INTO t (id, note) VALUES (1, 'hello; world');\n\
             UPDATE t SET note = 'x' WHERE id = 1;\n\
             DELETE FROM t WHERE id = 2;\n\
             COMMENT ON TABLE t IS 'a table';\n\
             GRANT SELECT ON t TO someone;\n\
             DROP TRIGGER IF EXISTS trg ON t;\n\
             ANALYZE t;");
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn out_of_subset_input_degrades_and_never_guesses() {
        // (input, the fragment the reason must name)
        let cases: &[(&str, &str)] = &[
            (
                "CREATE OR REPLACE FUNCTION f() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;",
                "outside the DDL subset",
            ),
            ("DO $$ BEGIN PERFORM 1; END $$;", "DO"),
            ("EXEC sp_rename 'a.b', 'c', 'COLUMN';", "EXEC"),
            ("CREATE TABLE t AS SELECT * FROM other;", "CREATE TABLE 't'"),
            ("ALTER TABLE t SET SCHEMA archive;", "outside the DDL subset"),
            ("ALTER TABLE t OWNER TO someone;", "OWNER"),
            (
                "ALTER TABLE t ALTER COLUMN c SET STORAGE PLAIN;",
                "outside the DDL subset",
            ),
            ("CREATE TABLE t (id INT, note VARCHAR(10)", "CREATE TABLE 't'"),
            (
                "CREATE TABLE t (note TEXT DEFAULT 'unterminated);",
                "unterminated string literal",
            ),
            ("BEGIN ATOMIC INSERT INTO t VALUES (1); END", "END"),
        ];
        for (sql, fragment) in cases {
            let reason = unverifiable(sql);
            assert!(reason.contains(fragment), "reason for {sql} was {reason}");
        }
    }

    #[test]
    fn an_alter_to_an_unseen_table_marks_only_that_table_incomplete() {
        let tables = shapes(
            "CREATE TABLE mine (id INT PRIMARY KEY);\n\
             ALTER TABLE elsewhere ADD COLUMN x INT;",
        );
        assert!(tables["mine"].complete);
        assert!(!tables["elsewhere"].complete);
    }

    #[test]
    fn sql_server_go_batches_and_bracket_identifiers_parse() {
        let table = one("CREATE TABLE [dbo].[Account] (\n\
               [account_id] NVARCHAR(35) NOT NULL,\n\
               [balance] DECIMAL(18,2) NOT NULL,\n\
               CONSTRAINT PK_Account PRIMARY KEY CLUSTERED ([account_id])\n\
             )\n\
             GO\n\
             ALTER TABLE [dbo].[Account] ADD [opened_at] DATETIME2 NULL\n\
             GO");
        assert_eq!(table.name, "Account");
        assert_eq!(table.key(), ["account_id"]);
        assert!(table.column("opened_at").is_some());
    }

    #[test]
    fn mysql_backticks_table_options_and_inline_keys_parse() {
        let table = one("CREATE TABLE `account` (\n\
               `account_id` VARCHAR(35) NOT NULL,\n\
               `balance` DECIMAL(18,2) NOT NULL DEFAULT 0.00,\n\
               `active` TINYINT(1) NOT NULL DEFAULT 1,\n\
               PRIMARY KEY (`account_id`),\n\
               KEY `idx_balance` (`balance`)\n\
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
        assert_eq!(table.key(), ["account_id"]);
        assert_eq!(table.column("active").unwrap().ty, SqlType::Boolean);
        assert_eq!(table.columns.len(), 3);
    }

    #[test]
    fn text_fit_is_provable_only_when_the_declaration_is_bounded() {
        let bounded = ColumnDef {
            name: "c".into(),
            declared_type: "VARCHAR(10)".into(),
            ty: SqlType::Text {
                max_chars: Some(10),
            },
            nullable: true,
            has_default: false,
            from_alter: false,
        };
        let unbounded = ColumnDef {
            declared_type: "TEXT".into(),
            ty: SqlType::Text { max_chars: None },
            ..bounded.clone()
        };
        let long = FieldFacets {
            max_length: Some(35),
            ..FieldFacets::default()
        };
        let short = FieldFacets {
            max_length: Some(8),
            ..FieldFacets::default()
        };
        let enumerated = FieldFacets {
            enumeration: Some(vec!["OPEN".into(), "SUSPENDED".into()]),
            ..FieldFacets::default()
        };
        assert!(matches!(
            column_fit(Builtin::String, &long, &bounded),
            Fit::Mismatch(_)
        ));
        assert_eq!(column_fit(Builtin::String, &short, &bounded), Fit::Fits);
        assert_eq!(column_fit(Builtin::String, &long, &unbounded), Fit::Fits);
        assert_eq!(
            column_fit(Builtin::String, &enumerated, &bounded),
            Fit::Fits,
            "the longest enumeration value bounds the value space"
        );
        assert!(matches!(
            column_fit(Builtin::String, &FieldFacets::default(), &bounded),
            Fit::Unprovable(_)
        ));
        assert_eq!(
            column_fit(Builtin::String, &FieldFacets::default(), &unbounded),
            Fit::Fits
        );
    }

    #[test]
    fn decimal_and_integer_fits_follow_precision_and_width() {
        let numeric = |p: u64, s: u64| ColumnDef {
            name: "c".into(),
            declared_type: format!("NUMERIC({p},{s})"),
            ty: SqlType::Numeric {
                precision: Some(p),
                scale: Some(s),
            },
            nullable: true,
            has_default: false,
            from_alter: false,
        };
        let integer = |bits: u32, spelling: &str| ColumnDef {
            name: "c".into(),
            declared_type: spelling.to_string(),
            ty: SqlType::Integer { bits },
            nullable: true,
            has_default: false,
            from_alter: false,
        };
        let money = FieldFacets {
            total_digits: Some(18),
            fraction_digits: Some(5),
            ..FieldFacets::default()
        };
        assert_eq!(
            column_fit(Builtin::Decimal, &money, &numeric(18, 5)),
            Fit::Fits
        );
        assert!(matches!(
            column_fit(Builtin::Decimal, &money, &numeric(18, 2)),
            Fit::Mismatch(_)
        ));
        assert!(matches!(
            column_fit(Builtin::Decimal, &money, &numeric(10, 5)),
            Fit::Mismatch(_)
        ));
        assert!(
            matches!(
                column_fit(Builtin::Decimal, &money, &integer(64, "BIGINT")),
                Fit::Mismatch(_)
            ),
            "a fractional decimal cannot live in an integer column"
        );
        assert!(matches!(
            column_fit(
                Builtin::Decimal,
                &FieldFacets::default(),
                &integer(64, "BIGINT")
            ),
            Fit::Unprovable(_)
        ));
        assert_eq!(
            column_fit(
                Builtin::Int,
                &FieldFacets::default(),
                &integer(32, "INTEGER")
            ),
            Fit::Fits
        );
        assert!(matches!(
            column_fit(
                Builtin::Long,
                &FieldFacets::default(),
                &integer(16, "SMALLINT")
            ),
            Fit::Mismatch(_)
        ));
        assert_eq!(
            column_fit(
                Builtin::Integer,
                &FieldFacets::default(),
                &integer(32, "INTEGER")
            ),
            Fit::Fits,
            "an arbitrary-precision integer has no width to compare — silence, not noise"
        );
    }

    #[test]
    fn family_mismatches_and_uncomparable_types_split_error_from_warning() {
        let column = |ty: SqlType, spelling: &str| ColumnDef {
            name: "c".into(),
            declared_type: spelling.to_string(),
            ty,
            nullable: true,
            has_default: false,
            from_alter: false,
        };
        let none = FieldFacets::default();
        assert!(matches!(
            column_fit(
                Builtin::String,
                &none,
                &column(SqlType::Integer { bits: 32 }, "INT")
            ),
            Fit::Mismatch(_)
        ));
        assert!(matches!(
            column_fit(Builtin::DateTime, &none, &column(SqlType::Date, "DATE")),
            Fit::Mismatch(_)
        ));
        assert_eq!(
            column_fit(
                Builtin::Date,
                &none,
                &column(SqlType::Timestamp, "TIMESTAMP")
            ),
            Fit::Fits
        );
        assert_eq!(
            column_fit(
                Builtin::Boolean,
                &none,
                &column(SqlType::Boolean, "TINYINT(1)")
            ),
            Fit::Fits
        );
        assert!(
            matches!(
                column_fit(
                    Builtin::String,
                    &none,
                    &column(SqlType::Unknown("JSONB".into()), "JSONB")
                ),
                Fit::Unprovable(_)
            ),
            "an uncomparable column type is unprovable, never a mismatch"
        );
        assert!(matches!(
            column_fit(Builtin::GYear, &none, &column(SqlType::Date, "DATE")),
            Fit::Unprovable(_)
        ));
    }
}
