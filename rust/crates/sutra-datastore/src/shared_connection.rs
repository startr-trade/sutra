//! Which data stores of two deployments would share rows.
//!
//! A key/value store's rows are keyed `(store_name, store_key)`, with no deployment in the key. So
//! two deployments that declare the same store name on the same connection read and write each
//! other's rows. That is a legitimate deployment choice, so nothing here refuses anything; the
//! caller only warns, because the same outcome also follows from a forgotten `datastores.yaml`
//! override.

use crate::config::StoreDefinition;
use crate::coverage::COVERAGE_STORE_NAME;

const URL_KEY: &str = "sql.url";
const URL_REF_KEY: &str = "sql.url-ref";

/// One store on one connection. `connection` may be a resolved URL, so it stays in memory: compare
/// it, never report it.
#[derive(Clone, PartialEq, Eq)]
pub struct StoreConnectionUse {
    pub store: String,
    pub connection: String,
}

/// The stores whose rows can be shared: `sql` stores, without the coverage store, whose rows the
/// engine keys per deployment.
fn shareable(stores: &[StoreDefinition]) -> impl Iterator<Item = &StoreDefinition> {
    stores
        .iter()
        .filter(|s| s.store_type == "sql" && s.name != COVERAGE_STORE_NAME)
}

fn declared_connection(store: &StoreDefinition) -> Option<&str> {
    [URL_REF_KEY, URL_KEY]
        .iter()
        .filter_map(|key| store.properties.get(*key))
        .map(|value| value.trim())
        .find(|value| !value.is_empty())
}

/// Each store's connection as authored (the reference text, or a literal). For a caller without
/// the target's environment: two references with one name are taken to resolve alike there.
pub fn declared_store_connections(stores: &[StoreDefinition]) -> Vec<StoreConnectionUse> {
    shareable(stores)
        .filter_map(|store| {
            Some(StoreConnectionUse {
                store: store.name.clone(),
                connection: declared_connection(store)?.to_string(),
            })
        })
        .collect()
}

/// Each store's connection as this process resolves it, so two reference names that lead to one
/// database compare equal. A reference that does not resolve here compares by its text.
pub fn resolved_store_connections(stores: &[StoreDefinition]) -> Vec<StoreConnectionUse> {
    shareable(stores)
        .filter_map(|store| {
            let connection = match store.resolved(URL_KEY) {
                Ok(Some(url)) if !url.trim().is_empty() => url,
                _ => declared_connection(store)?.to_string(),
            };
            Some(StoreConnectionUse {
                store: store.name.clone(),
                connection,
            })
        })
        .collect()
}

/// Stores a candidate shares with one other deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedStoreConnection {
    pub other_deployment: String,
    pub stores: Vec<String>,
}

impl SharedStoreConnection {
    /// The warning text, naming both deployments and the stores, never the connection.
    pub fn describe(&self, candidate_label: &str) -> String {
        format!(
            "deployment {candidate_label} and deployment {} use the same connection for data \
             store(s) {}, so they share those stores' rows. If that is not intended, give this \
             deployment its own connection in its datastores.yaml",
            self.other_deployment,
            self.stores.join(", ")
        )
    }
}

/// Every other deployment holding a store of the same name on the same connection as the
/// candidate. `others` is `(deployment label, its stores)`.
pub fn shared_store_connections(
    candidate: &[StoreConnectionUse],
    others: &[(String, Vec<StoreConnectionUse>)],
) -> Vec<SharedStoreConnection> {
    others
        .iter()
        .filter_map(|(label, uses)| {
            let stores: Vec<String> = candidate
                .iter()
                .filter(|mine| uses.iter().any(|theirs| theirs == *mine))
                .map(|mine| mine.store.clone())
                .collect();
            (!stores.is_empty()).then(|| SharedStoreConnection {
                other_deployment: label.clone(),
                stores,
            })
        })
        .collect()
}

/// The stores whose authored connection names neither the tenant nor the module, so any co-hosted
/// copy of this package resolves the same connection and shares their rows. A tenant called
/// `default` tells two deployments apart no better than having no tenant does.
pub fn stores_without_deployment_specific_connection(
    stores: &[StoreDefinition],
    tenant: &str,
    module: &str,
) -> Vec<String> {
    let squash = |text: &str| -> String {
        text.chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .map(|c| c.to_ascii_lowercase())
            .collect()
    };
    let mut tokens = vec![squash(module)];
    if !tenant.eq_ignore_ascii_case("default") {
        tokens.push(squash(tenant));
    }
    tokens.retain(|t| !t.is_empty());
    shareable(stores)
        .filter(|store| {
            declared_connection(store).is_some_and(|connection| {
                let connection = squash(connection);
                !tokens.iter().any(|t| connection.contains(t.as_str()))
            })
        })
        .map(|store| store.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sql_store(name: &str, key: &str, value: &str) -> StoreDefinition {
        StoreDefinition {
            name: name.to_string(),
            store_type: "sql".to_string(),
            properties: BTreeMap::from([(key.to_string(), value.to_string())]),
            structure: None,
        }
    }

    fn uses(pairs: &[(&str, &str)]) -> Vec<StoreConnectionUse> {
        pairs
            .iter()
            .map(|(store, connection)| StoreConnectionUse {
                store: store.to_string(),
                connection: connection.to_string(),
            })
            .collect()
    }

    #[test]
    fn same_store_name_on_same_connection_is_shared_and_named() {
        let found = shared_store_connections(
            &uses(&[("accounts", "c1"), ("audit", "c1")]),
            &[("other".to_string(), uses(&[("accounts", "c1")]))],
        );
        assert_eq!(
            found,
            vec![SharedStoreConnection {
                other_deployment: "other".to_string(),
                stores: vec!["accounts".to_string()],
            }]
        );
        let text = found[0].describe("mine");
        assert!(text.contains("mine") && text.contains("other") && text.contains("accounts"));
        assert!(!text.contains("c1"), "the connection is never reported");
    }

    #[test]
    fn another_connection_or_another_store_name_is_not_shared() {
        let others = [(
            "other".to_string(),
            uses(&[("accounts", "c2"), ("ledger", "c1")]),
        )];
        assert!(shared_store_connections(&uses(&[("accounts", "c1")]), &others).is_empty());
    }

    #[test]
    fn the_coverage_store_and_file_stores_are_not_shareable() {
        let mut file_store = sql_store("files", URL_KEY, "c1");
        file_store.store_type = "file".to_string();
        let stores = vec![sql_store(COVERAGE_STORE_NAME, URL_KEY, "c1"), file_store];
        assert!(declared_store_connections(&stores).is_empty());
    }

    #[test]
    fn a_reference_that_does_not_resolve_compares_by_its_text() {
        let stores = vec![sql_store("s", URL_REF_KEY, "env:SUTRA_TEST_UNSET_E2G_URL")];
        let found = resolved_store_connections(&stores);
        assert_eq!(found[0].connection, "env:SUTRA_TEST_UNSET_E2G_URL");
    }

    #[test]
    fn a_connection_naming_the_module_or_tenant_is_deployment_specific() {
        let stores = vec![
            sql_store("a", URL_REF_KEY, "env:MONEY_TRANSFER_DB_URL"),
            sql_store("b", URL_REF_KEY, "env:ACME_DB_URL"),
            sql_store("c", URL_REF_KEY, "env:DB_URL"),
        ];
        assert_eq!(
            stores_without_deployment_specific_connection(&stores, "acme", "money-transfer"),
            vec!["c".to_string()]
        );
    }

    #[test]
    fn the_default_tenant_makes_nothing_specific() {
        let stores = vec![sql_store("a", URL_REF_KEY, "env:DEFAULT_DB_URL")];
        assert_eq!(
            stores_without_deployment_specific_connection(&stores, "default", "orders"),
            vec!["a".to_string()]
        );
    }
}
