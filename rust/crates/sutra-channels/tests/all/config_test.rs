//! `channels.yaml` loader semantics proven against the
//! REAL shipped example files (read-only), plus the loader's error cases and the
//! `ContentTypeMatcher` + `DeferredAckRegistry` contracts.

use std::path::PathBuf;
use std::time::Duration;

use sutra_channels::{content_type, load_channel_definitions, DeferredAckRegistry};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn read(path: &str) -> Vec<u8> {
    let full = repo_root().join(path);
    std::fs::read(&full).unwrap_or_else(|e| panic!("read {}: {e}", full.display()))
}

// ---- the real money-transfer channels.yaml ---------------------------------------------

#[test]
fn money_transfer_channels_load_with_path_derived_identity() {
    let yaml = read(
        "examples/money-transfer/deployments-src/default--money-transfer--1.0.0/channels.yaml",
    );
    let defs =
        load_channel_definitions(&yaml, "default", "money-transfer", "1.0.0", "channels.yaml")
            .expect("loads");
    let names: Vec<&str> = defs
        .iter()
        .map(|d| d.binding.channel_name.as_str())
        .collect();
    assert_eq!(
        names,
        vec![
            "transfer-request",
            "transfer-queue",
            "transfer-topic",
            "balance",
            "coverage-query",
            "coverage-reset"
        ]
    );
    // Labels survive as observability metadata; the runtime identity is NOT derived from them.
    let transfer = &defs[0];
    assert_eq!(
        transfer.binding.namespace.module_key(),
        "default/money-transfer/1.0.0"
    );
    // A freshly parsed binding carries `unresolved()` — the archive-load path stamps the real
    // manifest-hash id at activation; there is no identity at parse time.
    assert_eq!(
        transfer.binding.deployment_id(),
        sutra_executor::DeploymentId::unresolved()
    );
    assert_eq!(transfer.binding.tenant(), "default");
    // The channel is the codec binder (YAML-authoritative). A user codec is referenced by
    // its path-derived URN: schemas/transfer/ → urn:transfer.
    assert_eq!(transfer.binding.codec, "urn:transfer");
    assert_eq!(transfer.transport.as_deref(), Some("http"));
    assert_eq!(
        transfer.bind_spec.as_deref(),
        Some("POST /channels/transfer-request")
    );
    assert_eq!(
        transfer.bind_method_and_path(),
        ("POST".to_string(), "/channels/transfer-request".to_string())
    );
    // auth: {scheme, apikey: {value, header}} — scheme first-class, sub-keys flattened
    // WITHOUT the auth. prefix.
    assert_eq!(transfer.auth_scheme.as_deref(), Some("apikey"));
    assert_eq!(
        transfer.properties.get("apikey.value").map(String::as_str),
        Some("transfer-demo-key")
    );
    assert_eq!(
        transfer.properties.get("apikey.header").map(String::as_str),
        Some("X-Api-Key")
    );
    // Singleton/serial contract + explicit HTTP ack-mode.
    assert!(transfer.singleton());
    assert_eq!(transfer.effective_ack_mode(), "on-complete");

    // The rabbitmq channel: broker ack-mode stays transport-side (on-persist default),
    // ${ENV} secret references stay literal in the property bag until channel startup.
    let queue = &defs[1];
    assert_eq!(queue.transport.as_deref(), Some("rabbitmq"));
    assert_eq!(queue.effective_ack_mode(), "on-persist");
    assert_eq!(
        queue.properties.get("username").map(String::as_str),
        Some("${RABBITMQ_USERNAME}")
    );
    assert_eq!(
        queue.properties.get("queue").map(String::as_str),
        Some("transfer-queue-q")
    );
    assert_eq!(
        queue.properties.get("prefetch-count").map(String::as_str),
        Some("1")
    );

    // The kafka channel: dotted property keys ride the bag verbatim.
    let topic = &defs[2];
    assert_eq!(
        topic
            .properties
            .get("bootstrap.servers")
            .map(String::as_str),
        Some("kafka:9092")
    );
    assert_eq!(
        topic
            .properties
            .get("auto.offset.reset")
            .map(String::as_str),
        Some("earliest")
    );

    // balance: no ack-mode declared → the HTTP default keeps the sync request/reply.
    let balance = &defs[3];
    assert_eq!(balance.effective_ack_mode(), "on-complete");
    assert_eq!(balance.resolve_path(), "/channels/balance");
    assert!(!balance.singleton());
}

#[test]
fn approval_hold_channels_load_all_seven() {
    let yaml =
        read("examples/approval-hold/deployments-src/default--approval--1.0.0/channels.yaml");
    let defs = load_channel_definitions(&yaml, "default", "approval", "1.0.0", "channels.yaml")
        .expect("loads");
    assert_eq!(defs.len(), 7);
    assert!(defs.iter().all(|d| d.binding.codec == "urn:approval"));
    assert!(defs.iter().all(|d| d.transport.as_deref() == Some("http")));
    assert!(defs
        .iter()
        .all(|d| d.auth_scheme.as_deref() == Some("apikey")));
}

// ---- loader error / edge cases (ChannelConfigLoader semantics) ----------------------------

#[test]
fn empty_yaml_yields_no_definitions() {
    assert!(
        load_channel_definitions(b"", "t", "m", "1.0.0", "test.yaml")
            .expect("empty is fine")
            .is_empty()
    );
    assert!(
        load_channel_definitions(b"channels: []", "t", "m", "1.0.0", "test.yaml")
            .expect("empty list is fine")
            .is_empty()
    );
}

#[test]
fn non_mapping_root_is_rejected() {
    let err = load_channel_definitions(b"- just\n- a list\n", "t", "m", "1.0.0", "test.yaml")
        .expect_err("rejects");
    assert_eq!(err.code, "SUTRA.PARSE.YAML.PARSE_ERROR");
}

#[test]
fn entry_without_name_is_rejected() {
    let yaml = b"channels:\n  - transport: http\n";
    let err = load_channel_definitions(yaml, "t", "m", "1.0.0", "test.yaml").expect_err("rejects");
    assert_eq!(err.code, "SUTRA.PARSE.YAML.PARSE_ERROR");
    assert!(err.message.contains("name"), "{}", err.message);
}

#[test]
fn legacy_module_version_process_keys_are_ignored_the_path_wins() {
    let yaml = b"channels:\n  - name: c1\n    module: legacy-mod\n    version: 9.9.9\n    process: legacy-proc\n    transport: http\n    auth-scheme: apikey\n";
    let defs = load_channel_definitions(yaml, "t", "m", "1.0.0", "test.yaml").expect("loads");
    assert_eq!(defs[0].binding.namespace.module_key(), "t/m/1.0.0");
    // Reserved-and-ignored keys never leak into the transport property bag.
    assert!(!defs[0].properties.contains_key("module"));
    assert!(!defs[0].properties.contains_key("process"));
    assert_eq!(defs[0].auth_scheme.as_deref(), Some("apikey"));
}

#[test]
fn broadcast_and_concurrency_settings_ride_the_binding() {
    let yaml = b"channels:\n  - name: fan\n    broadcast: true\n    max-concurrent-instances: 3\n    use-only-in-flight-for-concurrency-cap: false\n";
    let defs = load_channel_definitions(yaml, "t", "m", "1.0.0", "test.yaml").expect("loads");
    assert!(defs[0].binding.broadcast);
    assert_eq!(defs[0].binding.max_concurrent_instances, Some(3));
    assert!(!defs[0].binding.use_only_in_flight_for_concurrency_cap);
}

#[test]
fn non_positive_concurrency_cap_is_rejected() {
    let yaml = b"channels:\n  - name: c\n    max-concurrent-instances: 0\n";
    let err = load_channel_definitions(yaml, "t", "m", "1.0.0", "test.yaml").expect_err("rejects");
    assert_eq!(err.code, "SUTRA.PARSE.YAML.PARSE_ERROR");
}

#[test]
fn custom_path_property_and_payload_cap_are_first_class() {
    let yaml = b"channels:\n  - name: c\n    transport: http\n    auth-scheme: apikey\n    path: custom/inbound\n    payload-cap-bytes: 1024\n";
    let defs = load_channel_definitions(yaml, "t", "m", "1.0.0", "test.yaml").expect("loads");
    assert_eq!(defs[0].resolve_path(), "/custom/inbound");
    assert_eq!(defs[0].payload_cap_bytes, Some(1024));
}

// ---- ContentTypeMatcher --------------------------------------------------------------------

#[test]
fn content_type_matcher_covers_the_declared_pattern_shapes() {
    let xml = vec![
        "application/xml".to_string(),
        "text/xml".to_string(),
        "application/*+xml".to_string(),
    ];
    // Exact + parameters stripped + case-insensitive.
    assert!(content_type::accepts(&xml, Some("application/xml")));
    assert!(content_type::accepts(
        &xml,
        Some("application/xml; charset=utf-8")
    ));
    assert!(content_type::accepts(&xml, Some("Application/XML")));
    // RFC 6839 structured-syntax-suffix wildcard.
    assert!(content_type::accepts(&xml, Some("application/soap+xml")));
    // Genuine mismatch rejects (the VoIP call-drop case).
    assert!(!content_type::accepts(&xml, Some("audio/opus")));
    assert!(!content_type::accepts(&xml, Some("application/json")));

    // Full + subtype wildcards.
    assert!(content_type::accepts(
        &["*/*".to_string()],
        Some("audio/opus")
    ));
    assert!(content_type::accepts(
        &["application/*".to_string()],
        Some("application/anything")
    ));
    assert!(!content_type::accepts(
        &["application/*".to_string()],
        Some("text/plain")
    ));

    // Fail-open: no declared types / blank inbound both admit.
    assert!(content_type::accepts(&[], Some("audio/opus")));
    assert!(content_type::accepts(&xml, None));
    assert!(content_type::accepts(&xml, Some("  ")));
}

// ---- DeferredAckRegistry (the MessageAckRegistry semantic contract) -------------------------
// The registry is `Send + Sync` (Mutex interior, `Send` callbacks — it is shared between
// the engine actor thread, the sweep task and the transports), so the recorders here are
// atomics rather than the old `Rc<RefCell<..>>` pair.

use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

fn counter() -> (Arc<AtomicU32>, impl FnMut() + Send + 'static) {
    let c: Arc<AtomicU32> = Arc::default();
    let inner = Arc::clone(&c);
    (c, move || {
        inner.fetch_add(1, Ordering::SeqCst);
    })
}

#[test]
fn completed_instance_fires_the_ack_exactly_once() {
    let registry = DeferredAckRegistry::new(16, Duration::from_secs(3600));
    let (acks, on_ack) = counter();
    let (nacks, on_nack) = counter();
    assert!(registry.register("i-1", "ch", on_ack, on_nack));
    registry.on_instance_completed("i-1");
    registry.on_instance_completed("i-1"); // second event: entry already removed
    assert_eq!(acks.load(Ordering::SeqCst), 1);
    assert_eq!(nacks.load(Ordering::SeqCst), 0);
    assert_eq!(registry.pending_count(), 0);
}

#[test]
fn failed_instance_fires_the_nack_exactly_once() {
    let registry = DeferredAckRegistry::new(16, Duration::from_secs(3600));
    let (acks, on_ack) = counter();
    let (nacks, on_nack) = counter();
    registry.register("i-2", "ch", on_ack, on_nack);
    registry.on_instance_failed("i-2");
    registry.on_instance_failed("i-2");
    assert_eq!(acks.load(Ordering::SeqCst), 0);
    assert_eq!(nacks.load(Ordering::SeqCst), 1);
}

#[test]
fn duplicate_registration_is_a_no_op_first_wins() {
    let registry = DeferredAckRegistry::new(16, Duration::from_secs(3600));
    let (first_acks, on_first_ack) = counter();
    let (second_acks, on_second_ack) = counter();
    assert!(registry.register("i-3", "ch", on_first_ack, || {}));
    assert!(!registry.register("i-3", "ch", on_second_ack, || {}));
    registry.on_instance_completed("i-3");
    assert_eq!(first_acks.load(Ordering::SeqCst), 1);
    assert_eq!(second_acks.load(Ordering::SeqCst), 0);
}

#[test]
fn timeout_sweep_nacks_expired_entries() {
    let registry = DeferredAckRegistry::new(16, Duration::ZERO); // everything expires
    let (nacks, on_nack) = counter();
    registry.register("i-4", "ch", || {}, on_nack);
    assert_eq!(registry.sweep_timeouts(), 1);
    assert_eq!(nacks.load(Ordering::SeqCst), 1);
    assert_eq!(registry.pending_count(), 0);
    // A later terminal event is a no-op — the callback already fired once.
    registry.on_instance_failed("i-4");
    assert_eq!(nacks.load(Ordering::SeqCst), 1);
}

#[test]
fn lru_eviction_at_the_size_cap_nacks_the_oldest_entry() {
    let registry = DeferredAckRegistry::new(1, Duration::from_secs(3600));
    let (old_nacks, on_old_nack) = counter();
    registry.register("old", "ch", || {}, on_old_nack);
    assert!(registry.register("new", "ch", || {}, || {}));
    assert_eq!(old_nacks.load(Ordering::SeqCst), 1); // evicted with redelivery semantics
    assert_eq!(registry.pending_count(), 1);
}

#[test]
fn ack_registry_observes_the_executor_listener_bus() {
    // The wiring: the registry IS a ProcessExecutionListener — registered on the
    // TokenExecutor it acks on INSTANCE_COMPLETED without dispatcher involvement.
    use std::collections::BTreeMap;
    use sutra_executor::listener::ExecutionListener;

    let registry = Rc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let (acks, on_ack) = counter();
    let event = sutra_executor::listener::InstanceEvent {
        deployment: sutra_executor::DeploymentId::unresolved(),
        labels: BTreeMap::new(),
        instance_id: "i-listener".to_string(),
        process_id: "p".to_string(),
        module_version: String::new(),
        audit_sink: None,
    };
    registry.register("i-listener", "ch", on_ack, || {});
    ExecutionListener::on_instance_completed(registry.as_ref(), &event);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
}

#[test]
fn shared_registry_rides_the_rc_listener_bus_via_the_deferred_ack_listener() {
    // The production wiring shape: ONE `Arc`-shared registry (actor thread + sweep task +
    // flips) observing the executor's `Rc<dyn ExecutionListener>` fan-out through the
    // `DeferredAckListener` adapter.
    use std::collections::BTreeMap;
    use sutra_channels::DeferredAckListener;
    use sutra_executor::listener::ExecutionListener;

    let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
    let listener: Rc<dyn ExecutionListener> =
        Rc::new(DeferredAckListener::new(Arc::clone(&registry)));
    let (acks, on_ack) = counter();
    registry.register("i-shared", "ch", on_ack, || {});
    let event = sutra_executor::listener::InstanceEvent {
        deployment: sutra_executor::DeploymentId::unresolved(),
        labels: BTreeMap::new(),
        instance_id: "i-shared".to_string(),
        process_id: "p".to_string(),
        module_version: String::new(),
        audit_sink: None,
    };
    listener.on_instance_completed(&event);
    assert_eq!(acks.load(Ordering::SeqCst), 1);
    assert_eq!(registry.pending_count(), 0);
}

// ---- DeferredAckRegistry edge cases --------------------------------------------------------

#[test]
fn a_terminal_event_for_an_unregistered_instance_is_a_no_op() {
    // An orphan completed/failed event (nothing registered) does nothing and does not panic.
    let registry = DeferredAckRegistry::new(16, Duration::from_secs(3600));
    registry.on_instance_completed("never-registered");
    registry.on_instance_failed("also-never-registered");
    assert_eq!(registry.pending_count(), 0);
}

#[test]
fn a_panicking_callback_is_swallowed_and_the_entry_is_still_removed() {
    // A callback that panics must not unwind through the registry / the executor listener
    // bus; the entry is still removed. (A "thread panicked" line on stderr is expected and
    // harmless.)
    let registry = DeferredAckRegistry::new(16, Duration::from_secs(3600));
    registry.register("i-panic", "ch", || panic!("boom in callback"), || {});
    registry.on_instance_completed("i-panic"); // must not propagate the panic
    assert_eq!(registry.pending_count(), 0);
}

#[test]
fn a_thousand_register_then_complete_operations_settle_cleanly() {
    // Pins the bounded bookkeeping across 1000 register→complete pairs (the cross-thread
    // smoke lives in the in-crate `ack.rs` suite).
    let registry = DeferredAckRegistry::new(1024 + 16, Duration::from_secs(3600));
    let acks: Arc<AtomicU32> = Arc::default();
    for i in 0..1000 {
        let inner = Arc::clone(&acks);
        assert!(registry.register(
            &format!("i-{i}"),
            "ch",
            move || {
                inner.fetch_add(1, Ordering::SeqCst);
            },
            || {}
        ));
    }
    assert_eq!(registry.pending_count(), 1000);
    for i in 0..1000 {
        registry.on_instance_completed(&format!("i-{i}"));
    }
    assert_eq!(acks.load(Ordering::SeqCst), 1000);
    assert_eq!(registry.pending_count(), 0);
}
