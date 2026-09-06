//! Schema projection — the flat-structure classification and column naming a typed-column
//! data store is derived from.
//!
//! A store that declares a [`StructureRef`](crate::config::StructureRef) says "my rows ARE these
//! declared scalars". This module turns the declared type's children — enumerated by
//! [`sutra_xsd::Schema::fields_of`] for XSD, or the JSON-Schema equivalent — into either:
//!
//! - a [`Projection`]: the ordered column list, each with its physical name, nullability,
//!   declared builtin and effective facets; or
//! - a [`ProjectionError`] naming every offending child and why.
//!
//! It is deliberately **pure**: no connection, no dialect, no SQL. That is what lets the
//! deploy-time lint (which has no database) and the runtime providers (which do) derive the
//! *same* projection from the *same* declaration — the property the whole design rests on.
//! It is also why this module lives beside the `datastores.yaml` loader rather than beside the
//! providers: the `providers` feature is off in a lint/wasm build, and this must still compile.
//!
//! ## Consumers
//!
//! - **P2 (runtime)** — the three dialect modules bind [`ProjectedField::column`] /
//!   [`ProjectedField::builtin`] / [`ProjectedField::facets`] into their `SELECT`/upsert, and
//!   check the live table against [`Projection::columns`] on first use.
//! - **P3 (lint)** — `sutra-loader` renders [`ProjectionError`] under
//!   [`ProjectionError::code`], and compares [`Projection`] against the table shape parsed from
//!   the package's own migrations.

use std::collections::BTreeMap;
use std::fmt;

pub use sutra_xsd::{Builtin, FieldDecl, FieldFacets, FieldShape};

use crate::config::StructureRef;

/// The lint diagnostic codes this module's faults render as (design §4.6). Owned here rather
/// than in the linter so the code a fault carries and the fault itself cannot drift apart.
pub mod codes {
    /// The declared type has a nested, repeated or open child — or no projectable child at all.
    pub const STRUCTURE_NOT_FLAT: &str = "SUTRA.CONFIG.DATASTORE.STRUCTURE_NOT_FLAT";
    /// A folded column name collides, is reserved, exceeds the identifier cap, or is not an
    /// identifier at all — and no explicit `columns:` mapping resolves it.
    pub const COLUMN_NAME_INVALID: &str = "SUTRA.CONFIG.DATASTORE.COLUMN_NAME_INVALID";
}

/// PostgreSQL's identifier cap — the narrowest of the three shipped dialects (MySQL 64, SQL
/// Server 128), so a name that clears it clears all of them.
pub const DEFAULT_IDENTIFIER_MAX_LEN: usize = 63;

/// The identifier constraints column names are checked against.
///
/// **P1 assumption**: one shared, conservative rule set rather than a per-dialect one — the
/// narrowest length cap and the union of the three dialects' reserved words. A name that passes
/// is portable across all three; a name that fails may be legal in *some* dialect and is
/// resolvable with an explicit `columns:` mapping either way. Per-dialect rule sets are a
/// substitution of these two fields, not an API change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamingRules {
    /// The maximum identifier length, in characters.
    pub max_identifier_len: usize,
    /// The reserved words a column name may not be (compared lowercased). Must be sorted.
    pub reserved: &'static [&'static str],
}

impl Default for NamingRules {
    fn default() -> NamingRules {
        NamingRules {
            max_identifier_len: DEFAULT_IDENTIFIER_MAX_LEN,
            reserved: RESERVED_WORDS,
        }
    }
}

/// One projected column: a declared scalar field bound to a physical column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedField {
    /// The declared field name (an element or attribute local name).
    pub field: String,
    /// The physical column name — the default fold, or the author's `columns:` override.
    pub column: String,
    /// Whether the column must admit NULL: the field is optional (`minOccurs = 0`) or sits in a
    /// `choice`, whose unselected branches are absent by construction.
    pub nullable: bool,
    /// Whether the declared field is an XML attribute rather than a child element. Carried so a
    /// provider can round-trip the distinction; it does not affect the column.
    pub is_attribute: bool,
    /// The declared builtin — the left column of the advisory type mapping (design §4.4).
    pub builtin: Builtin,
    /// The effective facets of the declared type's restriction chain, which bound what the
    /// column has to hold (`maxLength` → `VARCHAR(n)`, `totalDigits`/`fractionDigits` →
    /// `NUMERIC(p,s)`). Empty for a JSON-Schema-declared field — see
    /// `sutra_codec_schema::json_schema_fields`.
    pub facets: FieldFacets,
}

/// A declared structure resolved to an ordered column list — the flat row shape of a projected
/// store.
///
/// Column order is exactly the order the field enumeration reports, which each schema tier fixes
/// in the way its own model makes meaningful: XSD reports declared child order (attributes first,
/// then the content model in declaration order — an XML sequence is genuinely ordered), JSON
/// Schema reports property-name order (a JSON object is an unordered set, so authoring order
/// carries no meaning). Both are reproducible, which is what lets a provider build positional
/// binds against a projection and a linter report in a stable order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    /// The declared type name this projects (`structure.type`).
    pub type_name: String,
    /// The projected columns, in declared order.
    pub fields: Vec<ProjectedField>,
}

impl Projection {
    /// Classify `fields` (the declared children of `type_name`) and derive the projection.
    ///
    /// Classification is exactly the design's §4.2 table:
    ///
    /// | Declared shape | Projection |
    /// |---|---|
    /// | Scalar leaf, `maxOccurs = 1` (element or attribute) | column |
    /// | Scalar leaf, `minOccurs = 0` | nullable column |
    /// | Scalar leaf inside a `choice` | nullable column |
    /// | Complex child | [`NotFlatReason::ComplexChild`] |
    /// | Any child with `maxOccurs > 1` | [`NotFlatReason::Repeated`] |
    /// | Open content (`xs:any`, JSON Schema `additionalProperties`) | [`NotFlatReason::OpenContent`] |
    ///
    /// Flatness is decided first and reported whole: if any child fails it, the error lists
    /// *every* offending child, and naming is not attempted — column names for a structure that
    /// cannot be projected at all would be noise on top of the real fault. Naming faults are
    /// likewise reported whole.
    ///
    /// `columns` is the author's `columns:` override map (declared field name → physical column
    /// name); an entry naming a field the type does not declare is itself a fault, so a typo in
    /// the mapping cannot silently do nothing.
    pub fn derive(
        type_name: &str,
        fields: &[FieldDecl],
        columns: &BTreeMap<String, String>,
        rules: NamingRules,
    ) -> Result<Projection, ProjectionError> {
        let not_flat: Vec<NotFlatFault> = fields
            .iter()
            .filter_map(|field| {
                not_flat_reason(field).map(|reason| NotFlatFault {
                    field: field.name.clone(),
                    reason,
                })
            })
            .collect();
        if !not_flat.is_empty() {
            return Err(ProjectionError::NotFlat {
                type_name: type_name.to_string(),
                faults: not_flat,
            });
        }
        if fields.is_empty() {
            return Err(ProjectionError::NoFields {
                type_name: type_name.to_string(),
            });
        }

        let mut naming: Vec<NamingFault> = columns
            .keys()
            .filter(|name| !fields.iter().any(|f| &&f.name == name))
            .map(|name| NamingFault::UnknownField {
                field: name.clone(),
            })
            .collect();

        let mut projected = Vec::with_capacity(fields.len());
        for field in fields {
            let column = match columns.get(&field.name) {
                Some(explicit) => explicit.trim().to_string(),
                None => default_column_name(&field.name),
            };
            if let Some(fault) = identifier_fault(&field.name, &column, rules) {
                naming.push(fault);
            }
            let (builtin, facets) = field
                .scalar()
                .expect("flatness classification admitted only scalar leaves");
            projected.push(ProjectedField {
                field: field.name.clone(),
                column,
                nullable: field.is_optional(),
                is_attribute: field.is_attribute,
                builtin,
                facets: facets.clone(),
            });
        }
        naming.extend(collisions(&projected));

        if naming.is_empty() {
            Ok(Projection {
                type_name: type_name.to_string(),
                fields: projected,
            })
        } else {
            Err(ProjectionError::Naming {
                type_name: type_name.to_string(),
                faults: naming,
            })
        }
    }

    /// The physical column names, in declared order.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.column.as_str())
    }

    /// The projected field with this declared name.
    pub fn field(&self, declared_name: &str) -> Option<&ProjectedField> {
        self.fields.iter().find(|f| f.field == declared_name)
    }

    /// The projected field bound to this physical column (compared case-insensitively, as
    /// unquoted SQL identifiers are).
    pub fn by_column(&self, column: &str) -> Option<&ProjectedField> {
        self.fields
            .iter()
            .find(|f| f.column.eq_ignore_ascii_case(column))
    }
}

impl StructureRef {
    /// Derive this declaration's [`Projection`] from the declared type's children, under the
    /// default [`NamingRules`].
    ///
    /// `fields` comes from whichever schema tier declares the type —
    /// [`sutra_xsd::Schema::fields_of`] for XSD, `sutra_codec_schema::json_schema_fields` for
    /// JSON Schema. Resolving `structure.schema` to one of those is the caller's job (it owns
    /// the loaded module schemas); this crate deliberately depends on neither loader.
    pub fn project(&self, fields: &[FieldDecl]) -> Result<Projection, ProjectionError> {
        Projection::derive(
            &self.type_name,
            fields,
            &self.columns,
            NamingRules::default(),
        )
    }
}

/// Why one declared child cannot become a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotFlatReason {
    /// The child's declared type is not a scalar leaf — nested element content, a
    /// `simpleContent` text-plus-attributes type, or a JSON-Schema object.
    ComplexChild,
    /// The child may occur more than once (`maxOccurs` > 1 or `unbounded`, including a bound
    /// inherited from a repeatable enclosing group; or a JSON-Schema array).
    Repeated,
    /// Open content: an `xs:any` wildcard, or a JSON Schema that admits properties it does not
    /// declare. The child set is unbounded, so no closed column list can describe it.
    OpenContent,
}

impl NotFlatReason {
    /// The clause naming this fault in a diagnostic message.
    pub fn describe(&self) -> &'static str {
        match self {
            NotFlatReason::ComplexChild => "is not a scalar leaf (nested content)",
            NotFlatReason::Repeated => "may occur more than once",
            NotFlatReason::OpenContent => "is open content",
        }
    }
}

/// One child that blocks projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFlatFault {
    /// The declared child name.
    pub field: String,
    /// Why it blocks projection.
    pub reason: NotFlatReason,
}

/// One column-name fault. Every one is resolvable by an explicit `columns:` mapping, which is
/// why the design makes them errors rather than silent renames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamingFault {
    /// Two or more declared fields resolve to the same column (compared case-insensitively, as
    /// unquoted SQL identifiers are).
    Collision {
        /// The contested column name.
        column: String,
        /// Every declared field that resolved to it, in declared order.
        fields: Vec<String>,
    },
    /// The column name is a reserved word (see [`NamingRules::reserved`]).
    Reserved {
        /// The declared field.
        field: String,
        /// The offending column name.
        column: String,
    },
    /// The column name exceeds the identifier cap.
    TooLong {
        /// The declared field.
        field: String,
        /// The offending column name.
        column: String,
        /// Its length, in characters.
        len: usize,
        /// The cap it exceeded.
        cap: usize,
    },
    /// The column name is not a SQL identifier at all — empty after folding, starting with a
    /// digit, or carrying a character outside `[A-Za-z0-9_]`.
    NotAnIdentifier {
        /// The declared field.
        field: String,
        /// The offending column name (possibly empty).
        column: String,
    },
    /// A `columns:` entry names a field the declared type does not have — a typo that would
    /// otherwise silently do nothing.
    UnknownField {
        /// The name the mapping used.
        field: String,
    },
}

/// A declared structure that cannot be projected. Every variant carries the whole fault set,
/// not the first fault, so one lint pass reports everything the author has to fix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionError {
    /// The declared type is not entirely scalar (design §4.2).
    NotFlat {
        /// The declared type name.
        type_name: String,
        /// Every offending child.
        faults: Vec<NotFlatFault>,
    },
    /// The declared type has no children to project — a simple type, an empty content model, or
    /// a JSON schema with no `properties`. There is no row shape to derive.
    NoFields {
        /// The declared type name.
        type_name: String,
    },
    /// The declared fields are flat, but their column names are not usable (design §4.3).
    Naming {
        /// The declared type name.
        type_name: String,
        /// Every offending name.
        faults: Vec<NamingFault>,
    },
}

impl ProjectionError {
    /// The lint diagnostic code this fault renders as (design §4.6) — see [`codes`].
    pub fn code(&self) -> &'static str {
        match self {
            ProjectionError::NotFlat { .. } | ProjectionError::NoFields { .. } => {
                codes::STRUCTURE_NOT_FLAT
            }
            ProjectionError::Naming { .. } => codes::COLUMN_NAME_INVALID,
        }
    }

    /// The declared type name the fault is about.
    pub fn type_name(&self) -> &str {
        match self {
            ProjectionError::NotFlat { type_name, .. }
            | ProjectionError::NoFields { type_name }
            | ProjectionError::Naming { type_name, .. } => type_name,
        }
    }
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProjectionError::NotFlat { type_name, faults } => {
                write!(f, "declared structure type '{type_name}' is not flat: ")?;
                for (i, fault) in faults.iter().enumerate() {
                    if i > 0 {
                        f.write_str("; ")?;
                    }
                    write!(f, "'{}' {}", fault.field, fault.reason.describe())?;
                }
                f.write_str(
                    ". Flatten the type, or remove the 'structure' block and keep the opaque \
                     store.",
                )
            }
            ProjectionError::NoFields { type_name } => write!(
                f,
                "declared structure type '{type_name}' has no fields to project (a simple type, \
                 an empty content model, or a schema declaring no properties). Declare its \
                 scalar fields, or remove the 'structure' block and keep the opaque store."
            ),
            ProjectionError::Naming { type_name, faults } => {
                write!(
                    f,
                    "declared structure type '{type_name}' has unusable column names: "
                )?;
                for (i, fault) in faults.iter().enumerate() {
                    if i > 0 {
                        f.write_str("; ")?;
                    }
                    match fault {
                        NamingFault::Collision { column, fields } => write!(
                            f,
                            "fields [{}] all resolve to column '{column}'",
                            fields.join(", ")
                        )?,
                        NamingFault::Reserved { field, column } => {
                            write!(f, "'{field}' resolves to reserved word '{column}'")?
                        }
                        NamingFault::TooLong {
                            field,
                            column,
                            len,
                            cap,
                        } => write!(
                            f,
                            "'{field}' resolves to '{column}' ({len} characters, over the {cap} \
                             character identifier cap)"
                        )?,
                        NamingFault::NotAnIdentifier { field, column } => write!(
                            f,
                            "'{field}' resolves to '{column}', which is not a SQL identifier"
                        )?,
                        NamingFault::UnknownField { field } => write!(
                            f,
                            "the 'columns' mapping names '{field}', which the type does not \
                             declare"
                        )?,
                    }
                }
                f.write_str(". Map the offending field(s) explicitly under 'columns:'.")
            }
        }
    }
}

impl std::error::Error for ProjectionError {}

/// Classify one declared child against the §4.2 table. `None` means it projects as a column.
///
/// Open content is decided before repetition so an unbounded `xs:any` reports as the open
/// content it is; repetition is decided before shape so a repeated nested child reports the
/// more fundamental fault.
fn not_flat_reason(field: &FieldDecl) -> Option<NotFlatReason> {
    match field.shape {
        FieldShape::Any => Some(NotFlatReason::OpenContent),
        _ if field.is_repeated() => Some(NotFlatReason::Repeated),
        FieldShape::Complex => Some(NotFlatReason::ComplexChild),
        FieldShape::Scalar { .. } => None,
    }
}

/// Every column claimed by more than one declared field, in declared order.
fn collisions(projected: &[ProjectedField]) -> Vec<NamingFault> {
    let mut faults = Vec::new();
    let mut reported: Vec<String> = Vec::new();
    for (i, field) in projected.iter().enumerate() {
        let key = field.column.to_ascii_lowercase();
        if reported.contains(&key) {
            continue;
        }
        let claimants: Vec<String> = projected
            .iter()
            .filter(|other| other.column.eq_ignore_ascii_case(&field.column))
            .map(|other| other.field.clone())
            .collect();
        if claimants.len() > 1 {
            reported.push(key);
            faults.push(NamingFault::Collision {
                column: projected[i].column.clone(),
                fields: claimants,
            });
        }
    }
    faults
}

/// Check one resolved column name against the identifier rules — shape, then length, then
/// reservation, so the most fundamental fault is the one reported.
fn identifier_fault(field: &str, column: &str, rules: NamingRules) -> Option<NamingFault> {
    let not_identifier = column.is_empty()
        || column.starts_with(|c: char| c.is_ascii_digit())
        || !column
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if not_identifier {
        return Some(NamingFault::NotAnIdentifier {
            field: field.to_string(),
            column: column.to_string(),
        });
    }
    let len = column.chars().count();
    if len > rules.max_identifier_len {
        return Some(NamingFault::TooLong {
            field: field.to_string(),
            column: column.to_string(),
            len,
            cap: rules.max_identifier_len,
        });
    }
    if is_reserved(column, rules.reserved) {
        return Some(NamingFault::Reserved {
            field: field.to_string(),
            column: column.to_string(),
        });
    }
    None
}

/// Whether `column` is a reserved word in `reserved` (which must be sorted, lowercase).
fn is_reserved(column: &str, reserved: &[&str]) -> bool {
    let lowered = column.to_ascii_lowercase();
    reserved.binary_search(&lowered.as_str()).is_ok()
}

/// The default column name for a declared field: `lowerCamel` → `snake_case`, ASCII-folded
/// (design §4.3).
///
/// Three passes in one: non-ASCII letters fold to their unaccented ASCII equivalent (`é` → `e`,
/// `ß` → `ss`) and anything unfoldable becomes a separator; a case boundary
/// (`lower|digit` → `UPPER`, or the last capital of a run before a lowercase) inserts `_`; every
/// non-alphanumeric run collapses to a single `_`, with leading and trailing ones trimmed.
///
/// The result can still be unusable — empty, digit-initial, reserved, over-length. That is a
/// [`NamingFault`], not a silent repair: the author resolves it with a `columns:` mapping, which
/// is the only way the engine can know which physical name they meant.
pub fn default_column_name(field: &str) -> String {
    let folded: String = field
        .chars()
        .flat_map(|c| {
            let mapped = if c.is_ascii() {
                None
            } else {
                Some(fold_char(c))
            };
            match mapped {
                Some(s) => s.chars().collect::<Vec<char>>(),
                None => vec![c],
            }
        })
        .collect();

    let chars: Vec<char> = folded.chars().collect();
    let mut out = String::with_capacity(chars.len() + 8);
    for (i, c) in chars.iter().copied().enumerate() {
        if !c.is_ascii_alphanumeric() {
            if !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
            continue;
        }
        let previous = i.checked_sub(1).map(|p| chars[p]);
        let next = chars.get(i + 1).copied();
        let boundary = c.is_ascii_uppercase()
            && match (previous, next) {
                // `accountId` → `account_id`, `iso4217Code` → `iso4217_code`.
                (Some(p), _) if p.is_ascii_lowercase() || p.is_ascii_digit() => true,
                // `IBANCode` → `iban_code`: the last capital of a run starts the next word.
                (Some(p), Some(n)) if p.is_ascii_uppercase() && n.is_ascii_lowercase() => true,
                _ => false,
            };
        if boundary && !out.is_empty() && !out.ends_with('_') {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

/// Fold one non-ASCII character to its ASCII equivalent, or to a separator when it has none.
/// Case is preserved so the camel-case boundary pass still sees it.
fn fold_char(c: char) -> &'static str {
    match c {
        'À'..='Å' => "A",
        'Æ' => "AE",
        'Ç' => "C",
        'È'..='Ë' => "E",
        'Ì'..='Ï' => "I",
        'Ð' => "D",
        'Ñ' => "N",
        'Ò'..='Ö' | 'Ø' => "O",
        'Ù'..='Ü' => "U",
        'Ý' => "Y",
        'Þ' => "TH",
        'ß' => "ss",
        'à'..='å' => "a",
        'æ' => "ae",
        'ç' => "c",
        'è'..='ë' => "e",
        'ì'..='ï' => "i",
        'ð' => "d",
        'ñ' => "n",
        'ò'..='ö' | 'ø' => "o",
        'ù'..='ü' => "u",
        'ý' | 'ÿ' => "y",
        'þ' => "th",
        'Œ' => "OE",
        'œ' => "oe",
        'Š' => "S",
        'š' => "s",
        'Ž' => "Z",
        'ž' => "z",
        _ => "_",
    }
}

/// The reserved-word set column names are checked against: the union of the PostgreSQL, MySQL /
/// MariaDB and SQL Server reserved words that a business field name could plausibly fold onto.
///
/// **Sorted and lowercase** — [`is_reserved`] binary-searches it, and a test pins both
/// invariants. Union rather than per-dialect by the P1 assumption on [`NamingRules`]: a name
/// that clears this set is portable, and a false positive costs one `columns:` line, where a
/// false negative costs a syntax error against a live database.
pub const RESERVED_WORDS: &[&str] = &[
    "add",
    "all",
    "alter",
    "analyze",
    "and",
    "any",
    "are",
    "array",
    "as",
    "asc",
    "authorization",
    "backup",
    "begin",
    "between",
    "bigint",
    "binary",
    "blob",
    "both",
    "break",
    "browse",
    "bulk",
    "by",
    "cascade",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "checkpoint",
    "close",
    "cluster",
    "coalesce",
    "collate",
    "column",
    "commit",
    "compute",
    "constraint",
    "contains",
    "continue",
    "convert",
    "create",
    "cross",
    "current",
    "current_date",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "cursor",
    "database",
    "databases",
    "dbcc",
    "deallocate",
    "dec",
    "decimal",
    "declare",
    "default",
    "delete",
    "deny",
    "desc",
    "disk",
    "distinct",
    "distributed",
    "double",
    "drop",
    "dual",
    "dump",
    "each",
    "else",
    "end",
    "errlvl",
    "escape",
    "except",
    "exec",
    "execute",
    "exists",
    "exit",
    "explain",
    "external",
    "extract",
    "false",
    "fetch",
    "file",
    "fillfactor",
    "float",
    "for",
    "force",
    "foreign",
    "freetext",
    "freeze",
    "from",
    "full",
    "function",
    "grant",
    "group",
    "having",
    "holdlock",
    "identity",
    "if",
    "ignore",
    "ilike",
    "in",
    "index",
    "initially",
    "inner",
    "inout",
    "insert",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
    "key",
    "kill",
    "leading",
    "leave",
    "left",
    "like",
    "limit",
    "lineno",
    "load",
    "lock",
    "long",
    "longblob",
    "longtext",
    "match",
    "mediumblob",
    "mediumint",
    "mediumtext",
    "merge",
    "minus",
    "national",
    "natural",
    "nocheck",
    "nonclustered",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "of",
    "off",
    "offset",
    "offsets",
    "on",
    "only",
    "open",
    "optimize",
    "option",
    "or",
    "order",
    "out",
    "outer",
    "over",
    "overlaps",
    "partition",
    "percent",
    "pivot",
    "placing",
    "plan",
    "precision",
    "primary",
    "print",
    "proc",
    "procedure",
    "public",
    "purge",
    "raiserror",
    "range",
    "read",
    "reads",
    "readtext",
    "real",
    "reconfigure",
    "references",
    "regexp",
    "rename",
    "repeat",
    "replace",
    "restore",
    "restrict",
    "return",
    "returning",
    "revoke",
    "right",
    "rlike",
    "rollback",
    "row",
    "rowcount",
    "rowguidcol",
    "rows",
    "rule",
    "save",
    "schema",
    "schemas",
    "select",
    "session_user",
    "set",
    "setuser",
    "show",
    "shutdown",
    "similar",
    "smallint",
    "some",
    "spatial",
    "sql",
    "ssl",
    "starting",
    "statistics",
    "straight_join",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "terminated",
    "textsize",
    "then",
    "tinyblob",
    "tinyint",
    "tinytext",
    "to",
    "top",
    "trailing",
    "tran",
    "transaction",
    "trigger",
    "true",
    "truncate",
    "tsequal",
    "union",
    "unique",
    "unlock",
    "unsigned",
    "update",
    "updatetext",
    "usage",
    "use",
    "user",
    "using",
    "values",
    "varbinary",
    "varchar",
    "variadic",
    "varying",
    "verbose",
    "view",
    "waitfor",
    "when",
    "where",
    "while",
    "window",
    "with",
    "within",
    "writetext",
    "xor",
    "zerofill",
];

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_xsd::{Schema, WILDCARD_FIELD};

    fn scalar(name: &str, min: u32, max: Option<u32>, in_choice: bool) -> FieldDecl {
        FieldDecl {
            name: name.to_string(),
            is_attribute: false,
            occurs_min: min,
            occurs_max: max,
            in_choice,
            shape: FieldShape::Scalar {
                builtin: Builtin::String,
                facets: FieldFacets::default(),
            },
        }
    }

    fn shaped(name: &str, shape: FieldShape, max: Option<u32>) -> FieldDecl {
        FieldDecl {
            name: name.to_string(),
            is_attribute: false,
            occurs_min: 1,
            occurs_max: max,
            in_choice: false,
            shape,
        }
    }

    fn derive(fields: &[FieldDecl]) -> Result<Projection, ProjectionError> {
        Projection::derive("T", fields, &BTreeMap::new(), NamingRules::default())
    }

    // ---- §4.2 classification, one positive + one negative per row ------------------------

    #[test]
    fn scalar_leaf_with_max_occurs_one_is_a_column() {
        // Positive: an element and an attribute both project.
        let attribute = FieldDecl {
            is_attribute: true,
            ..scalar("attrOne", 1, Some(1), false)
        };
        let projection = derive(&[scalar("plain", 1, Some(1), false), attribute]).unwrap();
        assert_eq!(
            projection.columns().collect::<Vec<_>>(),
            ["plain", "attr_one"]
        );
        assert!(projection.fields.iter().all(|f| !f.nullable));
        assert!(projection.field("attrOne").unwrap().is_attribute);

        // Negative: the same field repeated is NOT a column.
        assert!(matches!(
            derive(&[scalar("plain", 1, Some(2), false)]),
            Err(ProjectionError::NotFlat { .. })
        ));
    }

    #[test]
    fn min_occurs_zero_is_a_nullable_column() {
        // Positive.
        let projection = derive(&[scalar("optional", 0, Some(1), false)]).unwrap();
        assert!(projection.field("optional").unwrap().nullable);
        // Negative: minOccurs = 1 outside a choice is NOT nullable.
        let projection = derive(&[scalar("required", 1, Some(1), false)]).unwrap();
        assert!(!projection.field("required").unwrap().nullable);
    }

    #[test]
    fn a_choice_member_is_a_nullable_column_however_its_own_min_occurs_reads() {
        // Positive: minOccurs = 1, but a choice branch is absent whenever another is taken.
        let projection = derive(&[scalar("branch", 1, Some(1), true)]).unwrap();
        assert!(projection.field("branch").unwrap().nullable);
        // Negative: the identical declaration outside a choice is not nullable.
        let projection = derive(&[scalar("branch", 1, Some(1), false)]).unwrap();
        assert!(!projection.field("branch").unwrap().nullable);
    }

    #[test]
    fn a_complex_child_blocks_projection() {
        // Negative (blocks).
        let error = derive(&[
            scalar("ok", 1, Some(1), false),
            shaped("nested", FieldShape::Complex, Some(1)),
        ])
        .unwrap_err();
        assert_eq!(
            error,
            ProjectionError::NotFlat {
                type_name: "T".to_string(),
                faults: vec![NotFlatFault {
                    field: "nested".to_string(),
                    reason: NotFlatReason::ComplexChild,
                }],
            }
        );
        assert_eq!(error.code(), codes::STRUCTURE_NOT_FLAT);
        assert!(error.to_string().contains("'nested'"));
        // Positive: the same type without the complex child projects.
        assert!(derive(&[scalar("ok", 1, Some(1), false)]).is_ok());
    }

    #[test]
    fn max_occurs_above_one_blocks_projection_bounded_or_unbounded() {
        for max in [Some(2), Some(9), None] {
            let error = derive(&[scalar("many", 1, max, false)]).unwrap_err();
            assert!(
                matches!(&error, ProjectionError::NotFlat { faults, .. }
                    if faults[0].reason == NotFlatReason::Repeated),
                "maxOccurs {max:?}"
            );
        }
        // Positive boundary: maxOccurs = 1 projects.
        assert!(derive(&[scalar("many", 1, Some(1), false)]).is_ok());
    }

    #[test]
    fn open_content_blocks_projection_repeated_or_not() {
        for max in [Some(1), None] {
            let error = derive(&[shaped(WILDCARD_FIELD, FieldShape::Any, max)]).unwrap_err();
            assert!(
                matches!(&error, ProjectionError::NotFlat { faults, .. }
                    if faults[0].reason == NotFlatReason::OpenContent),
                "maxOccurs {max:?}"
            );
        }
        // Positive: a closed type with the same field count projects.
        assert!(derive(&[scalar("closed", 1, Some(1), false)]).is_ok());
    }

    #[test]
    fn every_offending_child_is_reported_not_just_the_first() {
        let error = derive(&[
            shaped("nested", FieldShape::Complex, Some(1)),
            scalar("fine", 1, Some(1), false),
            scalar("many", 1, None, false),
            shaped(WILDCARD_FIELD, FieldShape::Any, Some(1)),
        ])
        .unwrap_err();
        let ProjectionError::NotFlat { faults, .. } = &error else {
            panic!("expected NotFlat, got {error:?}");
        };
        assert_eq!(
            faults,
            &[
                NotFlatFault {
                    field: "nested".to_string(),
                    reason: NotFlatReason::ComplexChild
                },
                NotFlatFault {
                    field: "many".to_string(),
                    reason: NotFlatReason::Repeated
                },
                NotFlatFault {
                    field: WILDCARD_FIELD.to_string(),
                    reason: NotFlatReason::OpenContent
                },
            ]
        );
    }

    #[test]
    fn a_type_with_no_fields_has_nothing_to_project() {
        let error = derive(&[]).unwrap_err();
        assert_eq!(
            error,
            ProjectionError::NoFields {
                type_name: "T".to_string()
            }
        );
        assert_eq!(error.code(), codes::STRUCTURE_NOT_FLAT);
    }

    #[test]
    fn column_order_is_declared_order() {
        let projection = derive(&[
            scalar("zulu", 1, Some(1), false),
            scalar("alpha", 1, Some(1), false),
            scalar("mike", 1, Some(1), false),
        ])
        .unwrap();
        assert_eq!(
            projection.columns().collect::<Vec<_>>(),
            ["zulu", "alpha", "mike"],
            "declared order, never sorted"
        );
    }

    // ---- §4.3 naming ---------------------------------------------------------------------

    #[test]
    fn default_folding_is_lower_camel_to_snake_case_ascii_folded() {
        let cases = [
            ("accountId", "account_id"),
            ("openedAt", "opened_at"),
            ("id", "id"),
            ("AccountId", "account_id"),
            ("IBAN", "iban"),
            ("IBANCode", "iban_code"),
            ("XMLHttpRequest", "xml_http_request"),
            ("iso4217Code", "iso4217_code"),
            ("value2X", "value2_x"),
            ("already_snake", "already_snake"),
            ("café", "cafe"),
            ("Ünïcødé", "unicode"),
            ("straße", "strasse"),
            ("with space", "with_space"),
            ("dotted.name", "dotted_name"),
            ("__leading", "leading"),
            ("trailing__", "trailing"),
            ("a--b", "a_b"),
        ];
        for (declared, expected) in cases {
            assert_eq!(default_column_name(declared), expected, "{declared}");
        }
    }

    #[test]
    fn names_that_fold_to_nothing_usable_are_faults_not_panics() {
        // Empty after folding, digit-initial: both NotAnIdentifier, neither a panic.
        for declared in ["", "   ", "日本語", "2fast"] {
            let error = derive(&[scalar(declared, 1, Some(1), false)]).unwrap_err();
            assert!(
                matches!(&error, ProjectionError::Naming { faults, .. }
                    if matches!(faults[0], NamingFault::NotAnIdentifier { .. })),
                "{declared}: {error:?}"
            );
            assert_eq!(error.code(), codes::COLUMN_NAME_INVALID);
        }
    }

    #[test]
    fn collisions_after_folding_are_reported_with_every_claimant() {
        let error = derive(&[
            scalar("accountId", 1, Some(1), false),
            scalar("account_id", 1, Some(1), false),
            scalar("AccountID", 1, Some(1), false),
            scalar("other", 1, Some(1), false),
        ])
        .unwrap_err();
        let ProjectionError::Naming { faults, .. } = &error else {
            panic!("expected Naming, got {error:?}");
        };
        assert_eq!(
            faults,
            &[NamingFault::Collision {
                column: "account_id".to_string(),
                fields: vec![
                    "accountId".to_string(),
                    "account_id".to_string(),
                    "AccountID".to_string(),
                ],
            }],
            "one fault naming every claimant, reported once"
        );

        // The explicit mapping resolves it — the whole point of the mapping existing.
        let overrides = BTreeMap::from([("AccountID".to_string(), "legacy_acct".to_string())]);
        let error = Projection::derive(
            "T",
            &[
                scalar("accountId", 1, Some(1), false),
                scalar("AccountID", 1, Some(1), false),
            ],
            &overrides,
            NamingRules::default(),
        );
        assert!(error.is_ok(), "{error:?}");
    }

    #[test]
    fn reserved_words_are_faults_and_an_override_resolves_them() {
        let error = derive(&[scalar("order", 1, Some(1), false)]).unwrap_err();
        assert!(
            matches!(&error, ProjectionError::Naming { faults, .. }
            if faults[0] == NamingFault::Reserved {
                field: "order".to_string(),
                column: "order".to_string(),
            }),
            "{error:?}"
        );
        // Case-insensitively reserved.
        assert!(derive(&[scalar("SELECT", 1, Some(1), false)]).is_err());
        // Resolved by mapping.
        let overrides = BTreeMap::from([("order".to_string(), "order_ref".to_string())]);
        assert!(Projection::derive(
            "T",
            &[scalar("order", 1, Some(1), false)],
            &overrides,
            NamingRules::default()
        )
        .is_ok());
        // Negative: a near-miss that is NOT reserved projects cleanly.
        assert!(derive(&[scalar("orderRef", 1, Some(1), false)]).is_ok());
    }

    #[test]
    fn over_length_identifiers_are_faults_at_the_cap_boundary() {
        let cap = NamingRules::default().max_identifier_len;
        let at_cap = "a".repeat(cap);
        assert!(
            derive(&[scalar(&at_cap, 1, Some(1), false)]).is_ok(),
            "at cap"
        );

        let over = "a".repeat(cap + 1);
        let error = derive(&[scalar(&over, 1, Some(1), false)]).unwrap_err();
        assert!(
            matches!(&error, ProjectionError::Naming { faults, .. }
                if matches!(faults[0], NamingFault::TooLong { len, cap: c, .. } if len == cap + 1 && c == cap)),
            "{error:?}"
        );
        // Resolved by mapping.
        let overrides = BTreeMap::from([(over.clone(), "short".to_string())]);
        assert!(Projection::derive(
            "T",
            &[scalar(&over, 1, Some(1), false)],
            &overrides,
            NamingRules::default()
        )
        .is_ok());
    }

    #[test]
    fn an_override_naming_an_undeclared_field_is_a_fault() {
        let overrides = BTreeMap::from([("typoField".to_string(), "whatever".to_string())]);
        let error = Projection::derive(
            "T",
            &[scalar("realField", 1, Some(1), false)],
            &overrides,
            NamingRules::default(),
        )
        .unwrap_err();
        assert!(
            matches!(&error, ProjectionError::Naming { faults, .. }
                if faults.contains(&NamingFault::UnknownField { field: "typoField".to_string() })),
            "{error:?}"
        );
    }

    #[test]
    fn an_override_is_itself_checked_against_the_identifier_rules() {
        for bad in ["select", "has space", "2col", ""] {
            let overrides = BTreeMap::from([("f".to_string(), bad.to_string())]);
            let error = Projection::derive(
                "T",
                &[scalar("f", 1, Some(1), false)],
                &overrides,
                NamingRules::default(),
            );
            assert!(error.is_err(), "override '{bad}' must not pass unchecked");
        }
    }

    #[test]
    fn the_reserved_word_set_is_sorted_lowercase_and_deduplicated() {
        for pair in RESERVED_WORDS.windows(2) {
            assert!(
                pair[0] < pair[1],
                "RESERVED_WORDS must be sorted and deduplicated: '{}' then '{}'",
                pair[0],
                pair[1]
            );
        }
        assert!(RESERVED_WORDS
            .iter()
            .all(|w| w.chars().all(|c| c.is_ascii_lowercase() || c == '_')));
        // The binary search the checker relies on agrees with a linear scan.
        for word in ["select", "order", "user", "table"] {
            assert!(is_reserved(word, RESERVED_WORDS), "{word}");
            assert!(is_reserved(&word.to_uppercase(), RESERVED_WORDS), "{word}");
        }
        assert!(!is_reserved("account_id", RESERVED_WORDS));
    }

    // ---- end to end over a compiled schema ------------------------------------------------

    fn compile(body: &str) -> Schema {
        let source = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns="urn:sutra:test:proj"
           targetNamespace="urn:sutra:test:proj" elementFormDefault="qualified">
{body}
</xs:schema>
"#
        );
        Schema::compile(source.as_bytes()).unwrap()
    }

    #[test]
    fn a_compiled_schema_projects_columns_with_effective_facets() {
        let schema = compile(
            r#"
  <xs:simpleType name="Text35">
    <xs:restriction base="xs:string"><xs:maxLength value="35"/></xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Text16">
    <xs:restriction base="Text35"><xs:maxLength value="16"/></xs:restriction>
  </xs:simpleType>
  <xs:element name="AccountRecord">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="accountId" type="Text16"/>
        <xs:element name="openedAt" type="xs:date"/>
        <xs:element name="balance" type="xs:decimal" minOccurs="0"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
"#,
        );
        let fields = schema.fields_of("AccountRecord").unwrap();
        let projection = Projection::derive(
            "AccountRecord",
            &fields,
            &BTreeMap::from([("openedAt".to_string(), "opened_on".to_string())]),
            NamingRules::default(),
        )
        .unwrap();

        assert_eq!(
            projection.columns().collect::<Vec<_>>(),
            ["account_id", "opened_on", "balance"]
        );
        let account = projection.field("accountId").unwrap();
        assert_eq!(account.builtin, Builtin::String);
        assert_eq!(account.facets.max_length, Some(16), "narrowest wins");
        assert!(!account.nullable);
        assert!(projection.field("balance").unwrap().nullable);
        assert_eq!(
            projection.by_column("ACCOUNT_ID").map(|f| f.field.as_str()),
            Some("accountId"),
            "columns match case-insensitively"
        );
    }

    #[test]
    fn a_nested_declaration_fails_with_the_offending_child_named() {
        let schema = compile(
            r#"
  <xs:element name="Nested">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="id" type="xs:string"/>
        <xs:element name="party" type="Party"/>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
  <xs:complexType name="Party">
    <xs:sequence><xs:element name="name" type="xs:string"/></xs:sequence>
  </xs:complexType>
"#,
        );
        let fields = schema.fields_of("Nested").unwrap();
        let error = Projection::derive("Nested", &fields, &BTreeMap::new(), NamingRules::default())
            .unwrap_err();
        assert_eq!(error.code(), codes::STRUCTURE_NOT_FLAT);
        let message = error.to_string();
        assert!(message.contains("'party'"), "{message}");
        assert!(
            message.contains("remove the 'structure' block"),
            "{message}"
        );
    }
}
