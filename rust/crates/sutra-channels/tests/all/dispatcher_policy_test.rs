//! Channel-layer dispatcher POLICY: the payload byte cap (per-channel override, global
//! default, `0` disables), the tenant quota gate, the feature gate, the per-channel
//! concurrency cap, and channel-registration collision / namespace isolation / registry
//! merge. Enforcement lives in `dispatch.rs` behind optional seams; each test wires exactly
//! the seam it exercises.

use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    ChannelBinding, ChannelEngine, ChannelRegistry, CodecRegistry, ConcurrencyStore, Diagnostic,
    DispatchOutcome, DrainingSink, FeatureProvider, InMemoryConcurrencyStore, InboundChain,
    InboundMessage, Namespace, PayloadCapPolicy, ProcessModuleRegistry, QuotaCheckResult,
    TenantQuotaEnforcer, ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

use crate::support::drive;

const TENANT: &str = "acme";
const MODULE_KEY: &str = "acme/orders-canary/1.0.0";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "orders-canary", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000f1").expect("valid deployment id")
}

/// echo (channel `orders-in`) + gated-echo (channel `gated-channel`) + work (channel
/// `capped-in`) — one module, three subscribing start events.
const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="echo">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="orders-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="echo"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
  <bpmn:process id="gated-echo">
    <bpmn:startEvent id="GS">
      <bpmn:extensionElements><q:source channel="gated-channel" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="GT" implementation="echo"/>
    <bpmn:endEvent id="GE"/>
    <bpmn:sequenceFlow id="gf1" sourceRef="GS" targetRef="GT"/>
    <bpmn:sequenceFlow id="gf2" sourceRef="GT" targetRef="GE"/>
  </bpmn:process>
  <bpmn:process id="capped">
    <bpmn:startEvent id="CS">
      <bpmn:extensionElements><q:source channel="capped-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="CT" implementation="echo"/>
    <bpmn:endEvent id="CE"/>
    <bpmn:sequenceFlow id="cf1" sourceRef="CS" targetRef="CT"/>
    <bpmn:sequenceFlow id="cf2" sourceRef="CT" targetRef="CE"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn executor() -> (TokenExecutor, Rc<DrainingSink>) {
    let tasks = TaskRegistry::new().register("echo", |_input, _ctx| {
        let mut out = BTreeMap::new();
        out.insert("processed".to_string(), FeelValue::Boolean(true));
        Ok(FeelValue::Map(out))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    (executor, sink)
}

fn module_registry() -> ProcessModuleRegistry {
    let module = BpmnModelLoader::new()
        .load(BPMN.as_bytes())
        .expect("BPMN loads");
    let mut processes = ProcessModuleRegistry::new();
    processes.register(&deployment(), &module);
    processes
}

fn inbound(channel: &str, tenant: &str, body: Vec<u8>) -> InboundMessage {
    InboundMessage {
        tenant: tenant.to_string(),
        module_key: MODULE_KEY.to_string(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: body.into(),
        content_type: Some("application/xml".to_string()),
        idempotency_key: "msg-1".to_string(),
        explicit_event_id: false,
        received_at: "2026-05-20T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn expect_err(result: Result<DispatchOutcome, Diagnostic>) -> Diagnostic {
    match result {
        Err(d) => d,
        Ok(_) => panic!("expected a dispatch failure"),
    }
}

// ============================ channel-name collision ====================================

#[test]
fn channel_collision_with_different_binding_raises_collision() {
    let mut registry = ChannelRegistry::new();
    registry
        .register(ChannelBinding::new(
            "orders-in",
            namespace(),
            deployment(),
            "",
        ))
        .expect("first registration");
    // Same channel URN (namespace + name), DIFFERENT binding (a different codec).
    let d = registry
        .register(ChannelBinding::new(
            "orders-in",
            namespace(),
            deployment(),
            "a-different-codec",
        ))
        .expect_err("collision");
    assert_eq!(d.code, "SUTRA.CHANNEL.NAME.COLLISION");
    assert_eq!(registry.size(), 1);
}

#[test]
fn channel_registration_with_same_binding_is_idempotent() {
    let mut registry = ChannelRegistry::new();
    registry
        .register(ChannelBinding::new(
            "orders-in",
            namespace(),
            deployment(),
            "",
        ))
        .expect("first");
    registry
        .register(ChannelBinding::new(
            "orders-in",
            namespace(),
            deployment(),
            "",
        ))
        .expect("idempotent re-register");
    assert_eq!(registry.size(), 1);
}

#[test]
fn same_channel_name_in_a_different_namespace_does_not_collide() {
    // The two-tenant feature: the same channel NAME under a different namespace is distinct.
    let mut registry = ChannelRegistry::new();
    registry
        .register(ChannelBinding::new(
            "orders-in",
            namespace(),
            deployment(),
            "",
        ))
        .expect("acme");
    registry
        .register(ChannelBinding::new(
            "orders-in",
            Namespace::new("globex", "orders-canary", "1.0.0"),
            DeploymentId::of("dep-0000000000000000000000f3").expect("valid deployment id"),
            "",
        ))
        .expect("globex — distinct URN, no collision");
    assert_eq!(registry.size(), 2);
}

// ============================ payload-cap policy ========================================

fn engine_with_policy(policy: PayloadCapPolicy) -> ChannelEngine {
    let (exec, sink) = executor();
    ChannelEngine::builder(
        exec,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(ChannelBinding::new(
        "orders-in",
        namespace(),
        deployment(),
        "",
    ))
    .with_process_registry(module_registry())
    .with_payload_cap_policy(policy)
    .build()
}

#[test]
fn payload_under_cap_is_dispatched() {
    let engine = engine_with_policy(PayloadCapPolicy::of_global(1024).unwrap());
    let out = drive(engine.dispatch(&inbound("orders-in", TENANT, vec![0u8; 512])))
        .expect("under cap dispatches");
    assert!(matches!(out, DispatchOutcome::Completed { .. }));
}

#[test]
fn payload_exactly_at_cap_is_dispatched() {
    // The cap is inclusive: payloadBytes == cap passes.
    let engine = engine_with_policy(PayloadCapPolicy::of_global(1024).unwrap());
    drive(engine.dispatch(&inbound("orders-in", TENANT, vec![0u8; 1024])))
        .expect("at cap dispatches");
}

#[test]
fn payload_over_cap_is_rejected_with_payload_too_large_and_attributes() {
    let engine = engine_with_policy(PayloadCapPolicy::of_global(1024).unwrap());
    let d = expect_err(drive(engine.dispatch(&inbound(
        "orders-in",
        TENANT,
        vec![0u8; 2048],
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.PAYLOAD_TOO_LARGE");
    assert_eq!(
        d.attributes.get("channelId").map(String::as_str),
        Some("orders-in")
    );
    assert_eq!(
        d.attributes.get("payloadBytes").map(String::as_str),
        Some("2048")
    );
    assert_eq!(
        d.attributes.get("effectiveCapBytes").map(String::as_str),
        Some("1024")
    );
}

#[test]
fn per_channel_override_beats_global_default() {
    // Tiny global (128 B) but the per-channel override raises orders-in to 8 KiB.
    let policy = PayloadCapPolicy::try_new(128, vec![("orders-in".to_string(), 8 * 1024)]).unwrap();
    let engine = engine_with_policy(policy);
    drive(engine.dispatch(&inbound("orders-in", TENANT, vec![0u8; 4096])))
        .expect("4 KiB passes the 8 KiB per-channel override");
}

#[test]
fn unconfigured_channel_falls_back_to_global_default() {
    let policy =
        PayloadCapPolicy::try_new(256, vec![("some-other-channel".to_string(), 1024 * 1024)])
            .unwrap();
    let engine = engine_with_policy(policy);
    let d = expect_err(drive(engine.dispatch(&inbound(
        "orders-in",
        TENANT,
        vec![0u8; 1024],
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.PAYLOAD_TOO_LARGE");
    assert_eq!(
        d.attributes.get("effectiveCapBytes").map(String::as_str),
        Some("256")
    );
}

#[test]
fn cap_of_zero_disables_enforcement_and_allows_arbitrarily_large_payload() {
    let engine = engine_with_policy(PayloadCapPolicy::of_global(0).unwrap());
    drive(engine.dispatch(&inbound("orders-in", TENANT, vec![0u8; 4 * 1024 * 1024])))
        .expect("cap=0 disables the check");
}

#[test]
fn per_channel_override_of_zero_disables_only_that_channel() {
    // Global 1 KiB but orders-in override = 0 (unlimited); a sibling channel still caps.
    let (exec, sink) = executor();
    let policy = PayloadCapPolicy::try_new(1024, vec![("orders-in".to_string(), 0)]).unwrap();
    let engine = ChannelEngine::builder(
        exec,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(ChannelBinding::new(
        "orders-in",
        namespace(),
        deployment(),
        "",
    ))
    .with_binding(ChannelBinding::new(
        "strict-channel",
        namespace(),
        deployment(),
        "",
    ))
    .with_process_registry(module_registry())
    .with_payload_cap_policy(policy)
    .build();

    // orders-in is unlimited → big payload accepted.
    drive(engine.dispatch(&inbound("orders-in", TENANT, vec![0u8; 64 * 1024])))
        .expect("orders-in unlimited");
    // strict-channel uses the 1 KiB global → 4 KiB rejected (before routing → no handler needed).
    let d = expect_err(drive(engine.dispatch(&inbound(
        "strict-channel",
        TENANT,
        vec![0u8; 4096],
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.PAYLOAD_TOO_LARGE");
}

// ============================ tenant quota ordering =====================================

struct DenyAllQuota;
#[async_trait::async_trait(?Send)]
impl TenantQuotaEnforcer for DenyAllQuota {
    async fn check_inbound(
        &self,
        tenant: &str,
        _d: &DeploymentId,
        channel: &str,
    ) -> QuotaCheckResult {
        QuotaCheckResult::Denied {
            reason: "SUTRA.INBOUND.QUOTA_EXCEEDED_RATE".to_string(),
            detail: format!("Channel '{channel}': tenant '{tenant}' denied for test"),
        }
    }
}

#[test]
fn quota_denied_rejects_before_tenant_binding_check() {
    // Wrong tenant on acme's endpoint: a binding check first would surface
    // TENANT_CHANNEL_NOT_ALLOWED; QUOTA_EXCEEDED_RATE proves the quota gate runs first.
    let (exec, sink) = executor();
    let engine = ChannelEngine::builder(
        exec,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(ChannelBinding::new(
        "orders-in",
        namespace(),
        deployment(),
        "",
    ))
    .with_process_registry(module_registry())
    .with_quota_enforcer(Rc::new(DenyAllQuota))
    .build();
    let d = expect_err(drive(engine.dispatch(&inbound(
        "orders-in",
        "evil-corp",
        Vec::new(),
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.QUOTA_EXCEEDED_RATE");
}

// ============================ channel feature-gate ======================================

struct FixedFeature(bool);
impl FeatureProvider for FixedFeature {
    fn is_enabled(&self, _expression: &str) -> bool {
        self.0
    }
}

fn gated_engine(enabled: bool) -> ChannelEngine {
    let (exec, sink) = executor();
    let mut binding = ChannelBinding::new("gated-channel", namespace(), deployment(), "");
    binding.enabled_expression = Some("${feature.newPipeline}".to_string());
    ChannelEngine::builder(
        exec,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(binding)
    .with_process_registry(module_registry())
    .with_feature_provider(Rc::new(FixedFeature(enabled)))
    .build()
}

#[test]
fn disabled_channel_gate_rejects_inbound_with_feature_disabled_code() {
    let engine = gated_engine(false);
    let d = expect_err(drive(engine.dispatch(&inbound(
        "gated-channel",
        TENANT,
        Vec::new(),
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.FEATURE_DISABLED");
    assert_eq!(
        d.attributes.get("channel").map(String::as_str),
        Some("gated-channel")
    );
}

#[test]
fn enabled_channel_gate_passes_inbound_through() {
    let engine = gated_engine(true);
    drive(engine.dispatch(&inbound("gated-channel", TENANT, b"<order/>".to_vec())))
        .expect("enabled gate proceeds");
}

// ============================ concurrency cap ===========================================

fn capped_engine(store: Rc<dyn ConcurrencyStore>, cap: Option<u32>) -> ChannelEngine {
    let (exec, sink) = executor();
    let mut binding = ChannelBinding::new("capped-in", namespace(), deployment(), "");
    binding.max_concurrent_instances = cap;
    ChannelEngine::builder(
        exec,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(binding)
    .with_process_registry(module_registry())
    .with_concurrency_store(store)
    .build()
}

#[test]
fn at_capacity_inbound_is_rejected_then_slot_frees_on_completion() {
    // The active count is sourced from the concurrency store (the persisted channel_instance
    // table in production; here we drive it directly). cap=1, one RUNNING slot held → reject;
    // free the slot (terminal) → a fresh inbound succeeds. (Single-threaded analogue of the
    // background-thread hold.)
    let store = Rc::new(InMemoryConcurrencyStore::new());
    let engine = capped_engine(Rc::clone(&store) as Rc<dyn ConcurrencyStore>, Some(1));
    // Occupy the one slot (an admitted instance holding it).
    drive(store.record_started(&deployment(), "inst-1", "capped-in"));
    let d = expect_err(drive(engine.dispatch(&inbound(
        "capped-in",
        TENANT,
        Vec::new(),
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.CHANNEL_AT_CAPACITY");
    assert_eq!(
        d.attributes.get("channel").map(String::as_str),
        Some("capped-in")
    );
    assert_eq!(d.attributes.get("cap").map(String::as_str), Some("1"));
    // Free the slot (instance terminal) → the next inbound flows through.
    drive(store.record_terminal(&deployment(), "inst-1"));
    drive(engine.dispatch(&inbound("capped-in", TENANT, Vec::new()))).expect("slot freed");
}

#[test]
fn unbounded_channel_never_rejects() {
    // No cap declared → admission is unconditional; 50 sequential dispatches pass.
    let store = Rc::new(InMemoryConcurrencyStore::new());
    let engine = capped_engine(Rc::clone(&store) as Rc<dyn ConcurrencyStore>, None);
    for _ in 0..50 {
        drive(engine.dispatch(&inbound("capped-in", TENANT, Vec::new())))
            .expect("unbounded admits everything");
    }
}

// ============================ RESOLVE_MODULE_NOT_FOUND ==================================

#[test]
fn unknown_module_raises_resolve_module_not_found() {
    // A channel bound to a module with NO registered processes and no relay bridge wired.
    let (exec, sink) = executor();
    let orphan_ns = Namespace::new(TENANT, "missing-module", "1.0.0");
    let engine = ChannelEngine::builder(
        exec,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(ChannelBinding::new(
        "orphan-channel",
        orphan_ns.clone(),
        DeploymentId::of("dep-0000000000000000000000f4").expect("valid deployment id"),
        "",
    ))
    .build();
    let mut msg = inbound("orphan-channel", TENANT, Vec::new());
    msg.module_key = orphan_ns.module_key();
    let d = expect_err(drive(engine.dispatch(&msg)));
    assert_eq!(d.code, "SUTRA.RESOLVE.MODULE.NOT_FOUND");
}

// ============================ registry merge ===========================================

#[test]
fn resolve_sees_processes_merged_from_a_second_file() {
    // A module version whose bpmn/ folder holds two files registers under the SAME
    // deployment twice; register() accumulates, so BOTH processes resolve.
    fn one_process(process_id: &str) -> String {
        format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
              <bpmn:process id="{process_id}">
                <bpmn:startEvent id="S"/>
                <bpmn:endEvent id="E"/>
                <bpmn:sequenceFlow id="f" sourceRef="S" targetRef="E"/>
              </bpmn:process>
            </bpmn:definitions>"#
        )
    }
    let dep = DeploymentId::of("dep-0000000000000000000000f2").expect("valid deployment id");
    let loader = BpmnModelLoader::new();
    let mut registry = ProcessModuleRegistry::new();
    registry.register(
        &dep,
        &loader
            .load(one_process("approval-hold").as_bytes())
            .unwrap(),
    );
    registry.register(
        &dep,
        &loader
            .load(one_process("template-showcase").as_bytes())
            .unwrap(),
    );

    assert!(registry.find_in_module(&dep, "approval-hold").is_some());
    assert!(registry.find_in_module(&dep, "template-showcase").is_some());
    // A process id that lives in no file is still absent.
    assert!(registry.find_in_module(&dep, "does-not-exist").is_none());
}
