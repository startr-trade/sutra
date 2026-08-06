//! The Knative `ack-mode: on-complete` RESPONSE-HOLD, end to end over the real
//! axum router, the real engine actor and the real `DeferredAckRegistry`.
//!
//! Knative Eventing's data-plane contract makes the subscriber's HTTP response the settle
//! signal (there is no detached ack), so `on-complete` here is a bounded hold of the push
//! response, released by the settle callbacks the park arm registers on the registry. What
//! this suite pins:
//!
//! - **held until complete** — a delivery that PARKS gets no response until the relay-resumed
//!   instance reaches `INSTANCE_COMPLETED`, and the response is then `202` (accepted);
//! - **failure status** — an instance that reaches `INSTANCE_FAILED` releases the hold with a
//!   NON-retryable `422` (the data-plane contract's "other 4xx ⇒ do not retry" — the sender
//!   routes it to its `deadLetterSink` instead of re-driving a flow that already failed);
//! - **dead-letter status** — a dead-lettered dispatch (non-idempotent process failed,
//!   consumed at-most-once) is an instance failure too, so `on-complete` answers `422` while
//!   `on-persist` keeps the `202`;
//! - **on-persist unchanged** — the same parking flow under the transport default answers
//!   `202` AT THE PARK, holding nothing and registering nothing;
//! - **no park ⇒ no hold** — an `on-complete` delivery that runs to completion inside the
//!   dispatch answers immediately (`Settled`, the registry untouched);
//! - **hold timeout** — the bound (`on-complete.hold-timeout`) releases the response as `202`
//!   with the loud `SUTRA.INBOUND.KNATIVE.HOLD_TIMEOUT` diagnostic, and the instance keeps
//!   running (its registry entry is still pending, and a later relay still completes it) —
//!   the documented per-delivery degrade to `on-persist`.
//!
//! No container is involved: a real Knative Broker needs a Kubernetes cluster, so the honest
//! substitute is the real router + real engine (the sender half of the contract is the status
//! codes asserted above, which the k8s conformance tier exercises against a live Broker).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use sutra_bpmn::BpmnModelLoader;
use sutra_channels::config::{ChannelBinding, ChannelDefinition, Namespace};
use sutra_channels::http::{spawn_engine, EngineHandle};
use sutra_channels::{
    AliasRecord, ChannelEngine, CodecRegistry, DeferredAckListener, DeferredAckRegistry,
    DeploymentId, Diagnostic, DispatchOutcome, DrainingSink, FormatRegistry, InboundChain,
    InboundMessage, InstanceBridge, OutboxEmission, SuspendedInstance, TimerWaitRecord,
    ValidatorRegistry,
};
use sutra_executor::{TaskError, TaskRegistry, TokenExecutor};
// Force-link the builtin formats so `CodecRegistry::with_builtins()` sees `raw-text` (the
// inventory registration only exists when the crate is linked — the composition root's job).
use sutra_formats as _;
use sutra_transport_knative::{knative_router_dynamic, knative_routes_of, KnativeRouteTable};
use tower::util::ServiceExt;

const TENANT: &str = "acme";
/// The knative-bound inbound channel (subscription `orders-sub`).
const START_CHANNEL: &str = "orders-in";
/// The relay channel resuming the parked instance — driven straight through the engine
/// handle (it is the correlated second delivery, not a knative push).
const RELAY_CHANNEL: &str = "relay-in";
/// A knative-bound channel whose process runs to completion inside the dispatch.
const SYNC_CHANNEL: &str = "sync-in";
/// A knative-bound channel whose (non-idempotent) process fails inside the dispatch — the
/// dispatcher dead-letters it.
const FAIL_FAST_CHANNEL: &str = "fail-fast-in";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "approval", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000041").expect("valid deployment id")
}

/// start(`orders-in`, correlate alias e2eId=event.body) → userTask(`relay-in`) → end.
const HOLD_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="hold">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="orders-in" name="payload"/>
        <q:alias name="e2eId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U" name="Approve">
      <bpmn:extensionElements>
        <q:source channel="relay-in" name="decision"/>
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// Like `hold`, but the RESUME runs a `boom` task — the resumed instance reaches
/// `INSTANCE_FAILED`, which is what fires the registry's nack.
const HOLD_FAIL_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="hold-fail">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="orders-in" name="payload"/>
        <q:alias name="e2eId" expression="event.body" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:userTask id="U" name="Approve">
      <bpmn:extensionElements>
        <q:source channel="relay-in" name="decision"/>
      </bpmn:extensionElements>
    </bpmn:userTask>
    <bpmn:serviceTask id="T" implementation="boom"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
    <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="T"/>
    <bpmn:sequenceFlow id="f3" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// start(`sync-in`) → end — no wait state, so the dispatch never parks.
const SYNC_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="echo-sync">
    <bpmn:startEvent id="S2">
      <bpmn:extensionElements>
        <q:source channel="sync-in" name="payload"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="f" sourceRef="S2" targetRef="E2"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// start(`fail-fast-in`) → boom → end, on the DEFAULT (non-idempotent) process posture, so
/// the execution failure is dead-lettered inside the dispatch.
const FAIL_FAST_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="fail-fast">
    <bpmn:startEvent id="S3">
      <bpmn:extensionElements>
        <q:source channel="fail-fast-in" name="payload"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T3" implementation="boom"/>
    <bpmn:endEvent id="E3"/>
    <bpmn:sequenceFlow id="g1" sourceRef="S3" targetRef="T3"/>
    <bpmn:sequenceFlow id="g2" sourceRef="T3" targetRef="E3"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// A STATEFUL in-memory [`InstanceBridge`] — serves the park it captured back to the
/// relay-resume path, so one engine can park an instance and genuinely resume it.
#[derive(Default)]
struct ParkingBridge {
    parked: RefCell<Option<(String, SuspendedInstance)>>,
    aliases: RefCell<Vec<AliasRecord>>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for ParkingBridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
        snapshot: &SuspendedInstance,
        aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.parked.borrow_mut() = Some((instance_id.to_string(), snapshot.clone()));
        *self.aliases.borrow_mut() = aliases.to_vec();
        Ok(())
    }

    async fn load(
        &self,
        _deployment: &DeploymentId,
        instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(self
            .parked
            .borrow()
            .as_ref()
            .filter(|(id, _)| id == instance_id)
            .map(|(_, snapshot)| snapshot.clone()))
    }

    async fn find_live_alias(
        &self,
        _deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        let matches = self
            .aliases
            .borrow()
            .iter()
            .any(|a| a.name == name && a.value == value);
        Ok(self
            .parked
            .borrow()
            .as_ref()
            .filter(|_| matches)
            .map(|(id, _)| id.clone()))
    }

    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.parked.borrow_mut() = None;
        Ok(())
    }

    async fn commit_repark(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _snapshot: &SuspendedInstance,
        _satisfied_wait_nodes: &[String],
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }

    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }

    /// A fatally-failed resume marks the instance FAILED (the durable failure record). No
    /// knative case drives one, so the double just drops the parked row's resumability.
    async fn commit_failed(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _failure_code: &str,
        _detail: &str,
    ) -> Result<(), Diagnostic> {
        Ok(())
    }
}

/// One knative channel definition: `transport: knative`, bound to `subscription`, with the
/// authored `ack-mode` (and optional `on-complete.hold-timeout`).
fn definition(channel: &str, subscription: &str, props: &[(&str, &str)]) -> ChannelDefinition {
    let binding = ChannelBinding::new(channel, namespace(), deployment(), "raw-text");
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    properties.insert("subscription".to_string(), subscription.to_string());
    for (k, v) in props {
        properties.insert(k.to_string(), v.to_string());
    }
    ChannelDefinition {
        binding,
        transport: Some("knative".to_string()),
        bind_spec: None,
        codec: None,
        cloud_events_mode: None,
        auth_scheme: None,
        idempotency_key_header: None,
        payload_cap_bytes: None,
        properties,
    }
}

/// The production wiring shape in miniature: ONE `Arc` registry, on the executor's listener
/// bus (terminal events settle entries) AND on the `ChannelEngine` (the park arm registers
/// the router's settle callbacks).
fn build_engine(
    bpmn: &'static str,
    bridge: Rc<ParkingBridge>,
    registry: Arc<DeferredAckRegistry>,
) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let sync_module = BpmnModelLoader::new()
        .load(SYNC_BPMN.as_bytes())
        .expect("sync BPMN loads");
    let fail_fast_module = BpmnModelLoader::new()
        .load(FAIL_FAST_BPMN.as_bytes())
        .expect("fail-fast BPMN loads");
    let tasks = TaskRegistry::new().register("boom", |_input, _ctx| {
        Err(TaskError::Failed("synthetic task failure".to_string()))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .with_listener(Rc::new(DeferredAckListener::new(Arc::clone(&registry))))
        .build();
    let mut builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    for channel in [
        START_CHANNEL,
        RELAY_CHANNEL,
        SYNC_CHANNEL,
        FAIL_FAST_CHANNEL,
    ] {
        builder = builder.with_binding(ChannelBinding::new(
            channel,
            namespace(),
            deployment(),
            "raw-text",
        ));
    }
    builder
        .with_module(&deployment(), &module)
        .with_module(&deployment(), &sync_module)
        .with_module(&deployment(), &fail_fast_module)
        .with_instance_bridge(bridge as Rc<dyn InstanceBridge>)
        .with_deferred_acks(registry)
        .build()
}

/// The engine actor + the mounted knative router + the shared registry.
struct Harness {
    router: Router,
    handle: EngineHandle,
    registry: Arc<DeferredAckRegistry>,
}

impl Harness {
    fn new(bpmn: &'static str, defs: Vec<ChannelDefinition>) -> Harness {
        let registry = Arc::new(DeferredAckRegistry::new(16, Duration::from_secs(3600)));
        let actor_registry = Arc::clone(&registry);
        let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
            build_engine(bpmn, Rc::new(ParkingBridge::default()), actor_registry)
        });
        let routes = KnativeRouteTable::new();
        routes.swap(knative_routes_of(&defs).expect("routes build"));
        let router = knative_router_dynamic(&routes, handle.clone());
        Harness {
            router,
            handle,
            registry,
        }
    }

    /// POST a delivery to `/knative/<subscription>` (no `ce-*` headers — the partial-CE
    /// guard is a separate unit test; here the idempotency key is the body hash).
    fn push(&self, subscription: &str, body: &'static str) -> tokio::task::JoinHandle<StatusCode> {
        let router = self.router.clone();
        let uri = format!("/knative/{subscription}");
        tokio::spawn(async move {
            let request = Request::builder()
                .method("POST")
                .uri(uri)
                .header("Content-Type", "text/plain")
                .body(Body::from(body))
                .expect("request builds");
            router.oneshot(request).await.expect("response").status()
        })
    }

    /// Drive the correlated relay straight through the engine actor (the second delivery
    /// that resumes the parked instance).
    async fn relay(&self, body: &str) -> Result<DispatchOutcome, Diagnostic> {
        self.handle
            .dispatch(InboundMessage {
                tenant: TENANT.to_string(),
                module_key: namespace().module_key(),
                channel: RELAY_CHANNEL.to_string(),
                headers: BTreeMap::new(),
                body: body.as_bytes().to_vec().into(),
                content_type: Some("text/plain".to_string()),
                idempotency_key: format!("relay-{body}"),
                explicit_event_id: false,
                received_at: "2026-07-28T10:00:00Z".to_string(),
                cloud_event: None,
            })
            .await
    }

    /// Wait (bounded) until the registry holds `expected` entries — the observable proof
    /// that the push parked and its settle callbacks were registered.
    async fn await_pending(&self, expected: usize) {
        for _ in 0..200 {
            if self.registry.pending_count() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "registry never reached {expected} pending entries (was {})",
            self.registry.pending_count()
        );
    }
}

/// True when the push is still un-answered after a grace window — "the response is HELD".
async fn still_held(task: &mut tokio::task::JoinHandle<StatusCode>) -> bool {
    tokio::time::timeout(Duration::from_millis(250), task)
        .await
        .is_err()
}

#[tokio::test(flavor = "multi_thread")]
async fn on_complete_holds_the_push_response_until_the_instance_completes() {
    let harness = Harness::new(
        HOLD_BPMN,
        vec![definition(
            START_CHANNEL,
            "orders-sub",
            &[
                ("ack-mode", "on-complete"),
                ("on-complete.hold-timeout", "PT30S"),
            ],
        )],
    );

    let mut push = harness.push("orders-sub", "K-100");
    harness.await_pending(1).await;
    assert!(
        still_held(&mut push).await,
        "the push response must NOT be answered while the instance is parked"
    );

    // The relay resumes the instance to COMPLETED — the executor listener bus fires the
    // registry's ack, which releases the held response.
    let outcome = harness.relay("K-100").await.expect("relay resumes");
    assert!(matches!(outcome, DispatchOutcome::Completed { .. }));

    let status = tokio::time::timeout(Duration::from_secs(5), push)
        .await
        .expect("the hold is released at INSTANCE_COMPLETED")
        .expect("push task");
    assert_eq!(status, StatusCode::ACCEPTED, "completed ⇒ 202 accepted");
    assert_eq!(harness.registry.pending_count(), 0, "the entry is consumed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_instance_releases_the_hold_with_the_non_retryable_status() {
    let harness = Harness::new(
        HOLD_FAIL_BPMN,
        vec![definition(
            START_CHANNEL,
            "orders-sub",
            &[("ack-mode", "on-complete")],
        )],
    );

    let mut push = harness.push("orders-sub", "K-500");
    harness.await_pending(1).await;
    assert!(still_held(&mut push).await);

    // The resume FAILS — `INSTANCE_FAILED` fires the registry's nack (permanent reject).
    let _ = harness.relay("K-500").await;

    let status = tokio::time::timeout(Duration::from_secs(5), push)
        .await
        .expect("the hold is released at INSTANCE_FAILED")
        .expect("push task");
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "failed ⇒ a NON-retryable 4xx (Knative routes it to the deadLetterSink; a retry \
         would only re-fail)"
    );
    assert_eq!(harness.registry.pending_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn on_persist_answers_at_the_park_and_registers_nothing() {
    // The transport DEFAULT (no `ack-mode` declared) — the regression pin: a parking flow
    // answers 202 immediately, holds nothing, and never touches the deferred-ack registry.
    let harness = Harness::new(
        HOLD_BPMN,
        vec![definition(START_CHANNEL, "orders-sub", &[])],
    );

    let status = tokio::time::timeout(Duration::from_secs(5), harness.push("orders-sub", "K-200"))
        .await
        .expect("on-persist answers at dispatch return, without holding")
        .expect("push task");
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        harness.registry.pending_count(),
        0,
        "on-persist hands no settle callbacks to the engine"
    );

    // …and the instance really did park (the relay still finds and completes it).
    let outcome = harness.relay("K-200").await.expect("relay resumes");
    assert!(matches!(outcome, DispatchOutcome::Completed { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_on_complete_delivery_that_never_parks_answers_immediately() {
    let harness = Harness::new(
        HOLD_BPMN,
        vec![definition(
            SYNC_CHANNEL,
            "sync-sub",
            &[("ack-mode", "on-complete")],
        )],
    );

    let status = tokio::time::timeout(Duration::from_secs(5), harness.push("sync-sub", "K-1"))
        .await
        .expect("a run-to-completion dispatch answers at return (Settled, no hold)")
        .expect("push task");
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(harness.registry.pending_count(), 0, "no park ⇒ no entry");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_lettered_delivery_is_a_failure_under_on_complete_and_an_accept_under_on_persist() {
    // A non-idempotent process that fails is CONSUMED at-most-once with a durable
    // incident. `on-persist` keeps that 202; `on-complete` promises "2xx ⇒ the instance
    // completed", so the same outcome carries the permanent-reject status instead.
    let harness = Harness::new(
        HOLD_BPMN,
        vec![
            definition(
                FAIL_FAST_CHANNEL,
                "fail-oc-sub",
                &[("ack-mode", "on-complete")],
            ),
            definition(FAIL_FAST_CHANNEL, "fail-op-sub", &[]),
        ],
    );

    let on_complete =
        tokio::time::timeout(Duration::from_secs(5), harness.push("fail-oc-sub", "K-9"))
            .await
            .expect("dead-lettered dispatches never hold")
            .expect("push task");
    assert_eq!(on_complete, StatusCode::UNPROCESSABLE_ENTITY);

    let on_persist =
        tokio::time::timeout(Duration::from_secs(5), harness.push("fail-op-sub", "K-9"))
            .await
            .expect("on-persist answers at dispatch return")
            .expect("push task");
    assert_eq!(on_persist, StatusCode::ACCEPTED, "the 202 is unchanged");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_hold_timeout_releases_the_response_as_accepted_and_leaves_the_instance_running() {
    // The documented per-delivery degrade: the bound expires, the response goes out as an
    // ACCEPT (the intake is durable — a redelivery would only be deduped) with the loud
    // `SUTRA.INBOUND.KNATIVE.HOLD_TIMEOUT` WARN, and the instance keeps running.
    let harness = Harness::new(
        HOLD_BPMN,
        vec![definition(
            START_CHANNEL,
            "orders-sub",
            &[
                ("ack-mode", "on-complete"),
                ("on-complete.hold-timeout", "PT0.2S"),
            ],
        )],
    );

    let status = tokio::time::timeout(Duration::from_secs(5), harness.push("orders-sub", "K-300"))
        .await
        .expect("the bound releases the response")
        .expect("push task");
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "hold expiry ⇒ 202 (on-persist for this delivery)"
    );
    assert_eq!(
        harness.registry.pending_count(),
        1,
        "the hold expiring must NOT settle or cancel the instance — it is still running"
    );

    // The instance completes later, exactly as on-persist would have left it; the settle
    // callback finds a closed receiver and is a no-op (no panic, entry consumed).
    let outcome = harness.relay("K-300").await.expect("relay resumes");
    assert!(matches!(outcome, DispatchOutcome::Completed { .. }));
    assert_eq!(harness.registry.pending_count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unbound_subscription_is_a_retryable_404_under_either_ack_mode() {
    // Knative's data-plane contract lists 404 as RETRYABLE ("endpoint does not exist") —
    // right for a route that may appear on the next activation flip.
    let harness = Harness::new(
        HOLD_BPMN,
        vec![definition(
            START_CHANNEL,
            "orders-sub",
            &[("ack-mode", "on-complete")],
        )],
    );
    let status = tokio::time::timeout(Duration::from_secs(5), harness.push("nope-sub", "K-404"))
        .await
        .expect("unbound subscriptions never hold")
        .expect("push task");
    assert_eq!(status, StatusCode::NOT_FOUND);
}
