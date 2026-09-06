//! `datastores.yaml` parsing + module-resident migration loading — the store-config
//! loader and the migration-reading half of the resource-layout scan.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use serde_yaml::Value;

use crate::error::DataStoreError;

/// The default connection-pool ceiling when a store (or the engine datasource) declares no
/// `maxConnections` — the historical hardcoded value, now a single shared constant so no
/// pool-creation site carries a magic number.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 8;

/// Config-driven connection-pool sizing for a store — the `datastores.yaml` `maxConnections`
/// / `acquireTimeout` parameters, resolved with the shared default. Threaded to every
/// pool-creation site so pool sizing is configuration, not a hardcode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// The pool's maximum connection count (`maxConnections`); [`DEFAULT_MAX_CONNECTIONS`]
    /// when unset.
    pub max_connections: u32,
    /// The pool acquire timeout (`acquireTimeout`, seconds); `None` keeps the driver default.
    pub acquire_timeout: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> PoolConfig {
        PoolConfig {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            acquire_timeout: None,
        }
    }
}

/// Store types with a first-class provider. This crate ships `sql`; `file` is recognised
/// at parse time but has no Rust provider yet.
const KNOWN_TYPES: &[&str] = &["sql", "file"];

/// Keys consumed as first-class fields; the rest flow to the flattened property bag.
const RESERVED_KEYS: &[&str] = &["name", "type", "auth", "auth-scheme", "structure"];

/// The keys the optional `structure` block accepts. Anything else is an error — the block is
/// small and closed, so a typo must not be swallowed as an ignored key.
const STRUCTURE_KEYS: &[&str] = &["schema", "type", "columns"];

/// The optional `structure:` block of a store declaration — "my rows ARE these declared
/// scalars".
///
/// Absent it, a store behaves exactly as it always has: one opaque JSON document per key. It is
/// purely additive, so every existing deployment is unaffected, and a record too nested to
/// project simply declares none.
///
/// This type is only the *declaration*. Resolving [`StructureRef::schema`] to a compiled schema
/// is the caller's job — it owns the loaded module schemas — and turning the resolved type's
/// fields into columns is [`StructureRef::project`](crate::projection::Projection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureRef {
    /// `schema:` — the module schema/codec the type is declared in (a `schemas/<folder>` codec
    /// of this package, named the way the codec binding names it).
    pub schema: String,
    /// `type:` — the declared type within that schema: an XSD `complexType` or global element,
    /// or a JSON Schema definition.
    pub type_name: String,
    /// `columns:` — the optional override map, declared field name → physical column name. It
    /// exists because the author writes the DDL by hand and may already have column names; it
    /// is also how every naming fault (collision, reserved word, over-length) is resolved.
    pub columns: BTreeMap<String, String>,
}

/// One entry from a module's `datastores.yaml` — the store definition. The
/// provider-specific wiring (the store's OWN connection, `sql.url-ref` etc.) lives in the
/// flattened dotted `properties` bag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreDefinition {
    /// The store name a `<bpmn:dataStoreReference>` references.
    pub name: String,
    /// Provider type — `"sql"` or `"file"`.
    pub store_type: String,
    /// Provider-specific extras, flattened to dotted string keys (`sql.url-ref`, …).
    pub properties: BTreeMap<String, String>,
    /// The optional declared row structure. `None` keeps the historical opaque key→JSON store.
    pub structure: Option<StructureRef>,
}

impl StoreDefinition {
    /// A property by literal key or its `*-ref` secret-ref key, resolving `env:VAR` refs —
    /// the ref wins. Fails closed on an unset env
    /// var or an unsupported ref scheme.
    pub fn resolved(&self, literal_key: &str) -> Result<Option<String>, DataStoreError> {
        let ref_key = format!("{literal_key}-ref");
        if let Some(r) = self
            .properties
            .get(&ref_key)
            .filter(|s| !s.trim().is_empty())
        {
            return resolve_ref(r).map(Some);
        }
        Ok(self.properties.get(literal_key).cloned())
    }

    /// Resolve this store's connection-pool sizing from its `maxConnections` /
    /// `acquireTimeout` (seconds) properties, defaulting to [`PoolConfig::default`] when
    /// unset (so an unconfigured store keeps the historical behaviour). Fail-closed on a
    /// non-positive-integer value.
    pub fn pool_config(&self) -> Result<PoolConfig, DataStoreError> {
        let max_connections = match self.properties.get("maxConnections") {
            None => DEFAULT_MAX_CONNECTIONS,
            Some(raw) => raw
                .trim()
                .parse::<u32>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| {
                    DataStoreError::new(format!(
                        "data store '{}' has an invalid maxConnections '{}' (expected a \
                         positive integer)",
                        self.name, raw
                    ))
                })?,
        };
        let acquire_timeout = match self.properties.get("acquireTimeout") {
            None => None,
            Some(raw) => {
                let secs = raw
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        DataStoreError::new(format!(
                            "data store '{}' has an invalid acquireTimeout '{}' (expected a \
                         positive integer number of seconds)",
                            self.name, raw
                        ))
                    })?;
                Some(Duration::from_secs(secs))
            }
        };
        Ok(PoolConfig {
            max_connections,
            acquire_timeout,
        })
    }
}

/// Resolve a secret-ref (`env:VAR`). `k8s:` / `vault:` schemes are a follow-on.
pub fn resolve_ref(secret_ref: &str) -> Result<String, DataStoreError> {
    if let Some(var) = secret_ref.strip_prefix("env:") {
        return std::env::var(var).map_err(|_| {
            DataStoreError::new(format!(
                "data-store secret-ref '{secret_ref}' resolves to no value (environment \
                 variable '{var}' is not set)."
            ))
        });
    }
    Err(DataStoreError::new(format!(
        "unsupported data-store secret-ref scheme '{secret_ref}' (only env: is supported \
         here — use env-var indirection to a mounted secret)."
    )))
}

/// Parse a `datastores.yaml` document into [`StoreDefinition`]s. Loader contract:
/// unknown `type` and duplicate `name` fail closed; every non-reserved key flattens into
/// the dotted property bag.
pub fn parse_datastores(yaml: &str) -> Result<Vec<StoreDefinition>, DataStoreError> {
    let parsed: Value = serde_yaml::from_str(yaml)
        .map_err(|e| DataStoreError::new(format!("datastores YAML parse failed: {e}")))?;
    if parsed.is_null() {
        return Ok(Vec::new());
    }
    let root = parsed
        .as_mapping()
        .ok_or_else(|| DataStoreError::new("datastores YAML must be a mapping at the root"))?;
    let Some(stores) = root.get(Value::from("datastores")) else {
        return Ok(Vec::new());
    };
    let list = stores
        .as_sequence()
        .ok_or_else(|| DataStoreError::new("datastores YAML key 'datastores' must be a list"))?;

    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (i, item) in list.iter().enumerate() {
        let entry = item.as_mapping().ok_or_else(|| {
            DataStoreError::new(format!("datastores YAML entry {i} must be a mapping"))
        })?;
        let name = required_string(entry, "name", i)?;
        let store_type = required_string(entry, "type", i)?;
        if !KNOWN_TYPES.contains(&store_type.as_str()) {
            return Err(DataStoreError::new(format!(
                "datastores YAML store '{name}' has unknown type '{store_type}' (expected \
                 one of {KNOWN_TYPES:?})"
            )));
        }
        if !seen.insert(name.clone()) {
            return Err(DataStoreError::new(format!(
                "datastores YAML declares duplicate store name '{name}'"
            )));
        }
        let mut properties = BTreeMap::new();
        for (k, v) in entry {
            if let Some(key) = k.as_str() {
                if RESERVED_KEYS.contains(&key) {
                    continue;
                }
                flatten(&mut properties, key, v);
            }
        }
        let structure = parse_structure(entry, &name)?;
        out.push(StoreDefinition {
            name,
            store_type,
            properties,
            structure,
        });
    }
    Ok(out)
}

/// Parse a store entry's optional `structure` block. Fail-closed in the loader's established
/// style: a non-mapping block, an unknown key inside it, a missing/blank `schema` or `type`, and
/// a malformed `columns` entry are all errors rather than ignored input — the block is the
/// contract between a hand-written table and the engine's binds, so a typo that silently
/// disabled projection would be the worst outcome.
fn parse_structure(
    entry: &serde_yaml::Mapping,
    store: &str,
) -> Result<Option<StructureRef>, DataStoreError> {
    let Some(block) = entry.get(Value::from("structure")) else {
        return Ok(None);
    };
    let block = block.as_mapping().ok_or_else(|| {
        DataStoreError::new(format!(
            "data store '{store}' has a 'structure' block that is not a mapping (expected \
             {STRUCTURE_KEYS:?})"
        ))
    })?;
    for key in block.keys() {
        let key = key.as_str().unwrap_or_default();
        if !STRUCTURE_KEYS.contains(&key) {
            return Err(DataStoreError::new(format!(
                "data store '{store}' has unknown key '{key}' in its 'structure' block (expected \
                 one of {STRUCTURE_KEYS:?})"
            )));
        }
    }
    let schema = structure_string(block, "schema", store)?;
    let type_name = structure_string(block, "type", store)?;

    let mut columns = BTreeMap::new();
    if let Some(mapping) = block.get(Value::from("columns")) {
        let mapping = mapping.as_mapping().ok_or_else(|| {
            DataStoreError::new(format!(
                "data store '{store}' has a 'structure.columns' that is not a mapping of \
                 declared field name to column name"
            ))
        })?;
        for (field, column) in mapping {
            let (Some(field), Some(column)) = (field.as_str(), column.as_str()) else {
                return Err(DataStoreError::new(format!(
                    "data store '{store}' has a non-string entry in 'structure.columns' \
                     (expected fieldName: column_name)"
                )));
            };
            let (field, column) = (field.trim(), column.trim());
            if field.is_empty() || column.is_empty() {
                return Err(DataStoreError::new(format!(
                    "data store '{store}' has a blank entry in 'structure.columns' (expected \
                     fieldName: column_name)"
                )));
            }
            columns.insert(field.to_string(), column.to_string());
        }
    }
    Ok(Some(StructureRef {
        schema,
        type_name,
        columns,
    }))
}

fn structure_string(
    block: &serde_yaml::Mapping,
    key: &str,
    store: &str,
) -> Result<String, DataStoreError> {
    block
        .get(Value::from(key))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            DataStoreError::new(format!(
                "data store '{store}' has a 'structure' block missing required string '{key}'"
            ))
        })
}

/// Read a store's module-resident migrations — `properties["sql.migrations"]` resolved
/// relative to the binding's version directory, `*.sql` in filename order. The idempotent
/// SQL the provider runs once on first use.
/// Empty when the store declares no migrations folder; fails closed when it declares one
/// that is not a directory.
pub fn load_migrations(
    def: &StoreDefinition,
    binding_dir: &Path,
) -> Result<Vec<String>, DataStoreError> {
    let Some(rel) = def
        .properties
        .get("sql.migrations")
        .filter(|s| !s.trim().is_empty())
    else {
        return Ok(Vec::new());
    };
    let dir = binding_dir.join(rel);
    if !dir.is_dir() {
        return Err(DataStoreError::new(format!(
            "data store '{}' declares migrations at '{}', which is not a directory.",
            def.name,
            dir.display()
        )));
    }
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .map_err(|e| {
            DataStoreError::new(format!(
                "failed to read migrations for data store '{}' at '{}': {e}",
                def.name,
                dir.display()
            ))
        })?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    let mut scripts = Vec::new();
    for f in files {
        scripts.push(std::fs::read_to_string(&f).map_err(|e| {
            DataStoreError::new(format!(
                "failed to read migrations for data store '{}' at '{}': {e}",
                def.name,
                f.display()
            ))
        })?);
    }
    Ok(scripts)
}

fn flatten(out: &mut BTreeMap<String, String>, prefix: &str, value: &Value) {
    match value {
        Value::Null => {}
        Value::Mapping(nested) => {
            for (k, v) in nested {
                if let Some(k) = k.as_str() {
                    flatten(out, &format!("{prefix}.{k}"), v);
                }
            }
        }
        Value::String(s) => {
            out.insert(prefix.to_string(), s.clone());
        }
        Value::Bool(b) => {
            out.insert(prefix.to_string(), b.to_string());
        }
        Value::Number(n) => {
            out.insert(prefix.to_string(), n.to_string());
        }
        Value::Sequence(_) | Value::Tagged(_) => {
            // Lists do not occur in store declarations — keep the flattening scalar-only
            // and skip them.
        }
    }
}

fn required_string(
    entry: &serde_yaml::Mapping,
    key: &str,
    index: usize,
) -> Result<String, DataStoreError> {
    entry
        .get(Value::from(key))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            DataStoreError::new(format!(
                "datastores YAML entry {index} missing required string '{key}'"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MONEY_TRANSFER_STYLE: &str = r#"
datastores:
  - name: accounts
    type: sql
    sql:
      url-ref: env:ACCOUNTS_DB_URL
      username-ref: env:ACCOUNTS_DB_USER
      password-ref: env:ACCOUNTS_DB_PASSWORD
      migrations: migrations/accounts
    dataClass: financial
"#;

    #[test]
    fn parses_money_transfer_shape() {
        let defs = parse_datastores(MONEY_TRANSFER_STYLE).unwrap();
        assert_eq!(defs.len(), 1);
        let d = &defs[0];
        assert_eq!(d.name, "accounts");
        assert_eq!(d.store_type, "sql");
        assert_eq!(d.properties["sql.url-ref"], "env:ACCOUNTS_DB_URL");
        assert_eq!(d.properties["sql.migrations"], "migrations/accounts");
        assert_eq!(d.properties["dataClass"], "financial");
    }

    #[test]
    fn duplicate_and_unknown_type_fail_closed() {
        let dup = "datastores:\n  - name: a\n    type: sql\n  - name: a\n    type: sql\n";
        assert!(parse_datastores(dup).is_err());
        let unknown = "datastores:\n  - name: a\n    type: redis\n";
        assert!(parse_datastores(unknown).is_err());
    }

    #[test]
    fn pool_config_parses_or_defaults_and_fails_closed() {
        // Unset → the shared default, no acquire timeout (historical behaviour).
        let unset = parse_datastores("datastores:\n  - name: a\n    type: sql\n").unwrap();
        assert_eq!(
            unset[0].pool_config().unwrap(),
            PoolConfig {
                max_connections: DEFAULT_MAX_CONNECTIONS,
                acquire_timeout: None,
            }
        );

        // Configured values are threaded through.
        let set = parse_datastores(
            "datastores:\n  - name: a\n    type: sql\n    maxConnections: 32\n    \
             acquireTimeout: 5\n",
        )
        .unwrap();
        assert_eq!(
            set[0].pool_config().unwrap(),
            PoolConfig {
                max_connections: 32,
                acquire_timeout: Some(Duration::from_secs(5)),
            }
        );

        // A malformed value fails closed.
        let bad =
            parse_datastores("datastores:\n  - name: a\n    type: sql\n    maxConnections: nope\n")
                .unwrap();
        assert!(bad[0].pool_config().is_err());
    }

    #[test]
    fn ref_wins_over_literal_and_missing_env_fails() {
        let mut props = BTreeMap::new();
        props.insert("sql.url".to_string(), "literal".to_string());
        props.insert(
            "sql.url-ref".to_string(),
            "env:SUTRA_DATASTORE_TEST_UNSET_VAR".to_string(),
        );
        let def = StoreDefinition {
            name: "a".into(),
            store_type: "sql".into(),
            properties: props,
            structure: None,
        };
        assert!(
            def.resolved("sql.url").is_err(),
            "ref wins and fails closed"
        );
    }

    #[test]
    fn a_store_without_a_structure_block_is_unchanged() {
        let defs = parse_datastores(MONEY_TRANSFER_STYLE).unwrap();
        assert_eq!(
            defs[0].structure, None,
            "purely additive — opaque as before"
        );
    }

    #[test]
    fn parses_the_structure_block_with_its_column_overrides() {
        let defs = parse_datastores(
            "datastores:\n\
             \x20 - name: accounts\n\
             \x20   type: sql\n\
             \x20   structure:\n\
             \x20     schema: urn:sutra:codec:transfer\n\
             \x20     type: AccountRecord\n\
             \x20     columns:\n\
             \x20       accountId: account_id\n\
             \x20       openedAt: opened_at\n\
             \x20   sql:\n\
             \x20     url-ref: env:ACCOUNTS_DB_URL\n",
        )
        .unwrap();
        let structure = defs[0].structure.as_ref().unwrap();
        assert_eq!(structure.schema, "urn:sutra:codec:transfer");
        assert_eq!(structure.type_name, "AccountRecord");
        assert_eq!(
            structure.columns,
            BTreeMap::from([
                ("accountId".to_string(), "account_id".to_string()),
                ("openedAt".to_string(), "opened_at".to_string()),
            ])
        );
        // The block is consumed as a first-class field, NOT flattened into the property bag.
        assert!(defs[0]
            .properties
            .keys()
            .all(|k| !k.starts_with("structure")));
        assert_eq!(defs[0].properties["sql.url-ref"], "env:ACCOUNTS_DB_URL");
    }

    #[test]
    fn the_columns_map_is_optional() {
        let defs = parse_datastores(
            "datastores:\n  - name: a\n    type: sql\n    structure:\n      schema: s\n      \
             type: T\n",
        )
        .unwrap();
        let structure = defs[0].structure.as_ref().unwrap();
        assert!(structure.columns.is_empty());
    }

    #[test]
    fn malformed_structure_blocks_fail_closed() {
        let cases = [
            // Not a mapping.
            ("structure: nope", "scalar block"),
            ("structure:", "null block"),
            ("structure:\n      - schema: s", "list block"),
            // Missing / blank required keys.
            ("structure:\n      type: T", "no schema"),
            ("structure:\n      schema: s", "no type"),
            ("structure:\n      schema: '  '\n      type: T", "blank schema"),
            ("structure:\n      schema: s\n      type: ''", "blank type"),
            // Unknown key — a typo must not be swallowed.
            (
                "structure:\n      schema: s\n      type: T\n      colums:\n        a: b",
                "misspelled columns",
            ),
            (
                "structure:\n      schema: s\n      type: T\n      key: id",
                "unsupported key",
            ),
            // Malformed columns.
            (
                "structure:\n      schema: s\n      type: T\n      columns: nope",
                "columns not a mapping",
            ),
            (
                "structure:\n      schema: s\n      type: T\n      columns:\n        a: ''",
                "blank column name",
            ),
            (
                "structure:\n      schema: s\n      type: T\n      columns:\n        a:\n          \
                 b: c",
                "nested column value",
            ),
        ];
        for (block, what) in cases {
            let yaml = format!("datastores:\n  - name: a\n    type: sql\n    {block}\n");
            assert!(
                parse_datastores(&yaml).is_err(),
                "{what} must fail closed:\n{yaml}"
            );
        }
    }
}
