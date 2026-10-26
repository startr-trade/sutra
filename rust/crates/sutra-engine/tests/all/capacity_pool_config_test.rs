//! Capacity pools at deployment load: a channel's `pool:` must name a pool the engine
//! configuration declares, and the channel's tenant must be on that pool's allow-list —
//! otherwise the deployment does not load. Without a datasource the pools are counted in
//! memory, per process.

use sutra_channels::{load_channel_definitions, CapacityPool};
use sutra_engine::capacity::{
    build_capacity_store, configured_pools, install_configured_pools, validate_pool_references,
};
use sutra_executor::DeploymentId;

fn channels(tenant: &str, pool: &str) -> Vec<sutra_channels::ChannelDefinition> {
    let yaml = format!(
        "channels:\n- name: call-start\n  transport: http\n  pool: {pool}\n- name: plain\n  transport: http\n"
    );
    load_channel_definitions(yaml.as_bytes(), tenant, "calls", "1.0.0", "channels.yaml")
        .expect("channels load")
}

/// One test, because the declared pools are process-wide engine configuration.
#[test]
fn a_channel_naming_an_undeclared_or_disallowed_pool_fails_to_load() {
    let dep = DeploymentId::of("dep-0000000000000000000000e1").unwrap();
    install_configured_pools(&[
        CapacityPool::new("trunk-main", 30).with_tenants(vec!["default".to_string()]),
        CapacityPool::new("seats", 5),
    ]);
    assert_eq!(configured_pools().len(), 2);

    // Declared, tenant allowed (and a pool with no allow-list admits every tenant).
    validate_pool_references(&channels("default", "trunk-main"), &dep).expect("loads");
    validate_pool_references(&channels("acme", "seats"), &dep).expect("loads");

    let undeclared = validate_pool_references(&channels("default", "nowhere"), &dep)
        .expect_err("undeclared pool");
    assert!(
        undeclared.contains("SUTRA.CONFIG.CHANNEL.POOL_UNDECLARED"),
        "{undeclared}"
    );
    let disallowed = validate_pool_references(&channels("acme", "trunk-main"), &dep)
        .expect_err("tenant outside the allow-list");
    assert!(
        disallowed.contains("SUTRA.CONFIG.CHANNEL.POOL_TENANT_NOT_ALLOWED"),
        "{disallowed}"
    );

    // No datasource: an in-memory store over the declared pools.
    let store = build_capacity_store(None).expect("pools are declared");
    assert_eq!(store.pool("seats").map(|p| p.slots), Some(5));

    // No pools declared: no store, and any `pool:` reference fails to load.
    install_configured_pools(&[]);
    assert!(build_capacity_store(None).is_none());
    assert!(validate_pool_references(&channels("acme", "seats"), &dep).is_err());
}
