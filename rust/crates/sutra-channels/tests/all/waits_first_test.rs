//! Waits first: before a start event is resolved, the dispatcher asks whether the message answers a
//! wait an instance of the channel's owner is parked at. These pin the cheap path (no alias lookup
//! where nothing can be waiting), the refusals (a failed lookup, an instance not at the wait), and
//! which processes and versions are tried.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasRecord, ChannelBinding, ChannelEngine, CodecRegistry, Diagnostic, DrainingSink,
    InboundChain, InboundMessage, InstanceBridge, InstanceClaimOutcome, Namespace, OutboxEmission,
    OwnerScopes, SuspendedInstance, TimerWaitRecord, ValidatorRegistry, WAIT_PENDING_ATTR,
};
use sutra_codec_spi::{CodecValue, DecodeResult, PayloadCodec};
use sutra_executor::{
    archive_key, builtin_key, logical_urn, DeploymentId, TaskRegistry, TokenExecutor,
};

use crate::support::drive;

const TENANT: &str = "acme";

fn dep(n: u8) -> DeploymentId {
    DeploymentId::of(&format!("dep-0000000000000000000000{n:02x}")).unwrap()
}

/// A bridge whose aliases, parked instance and store health each test sets.
#[derive(Default)]
struct Bridge {
    /// `(alias name, value)` → instance.
    aliases: HashMap<(String, String), String>,
    loaded: Option<SuspendedInstance>,
    lookup_fails: bool,
    lookups: RefCell<Vec<(String, String, String)>>,
    parks: RefCell<usize>,
    resumes: RefCell<usize>,
}

#[async_trait::async_trait(?Send)]
impl InstanceBridge for Bridge {
    async fn commit_park(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _snapshot: &SuspendedInstance,
        _aliases: &[AliasRecord],
        _timer_waits: &[TimerWaitRecord],
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.parks.borrow_mut() += 1;
        Ok(())
    }
    async fn load(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<Option<SuspendedInstance>, Diagnostic> {
        Ok(self.loaded.clone())
    }
    async fn find_live_alias(
        &self,
        deployment: &DeploymentId,
        name: &str,
        value: &str,
    ) -> Result<Option<String>, Diagnostic> {
        self.lookups.borrow_mut().push((
            deployment.value().to_string(),
            name.to_string(),
            value.to_string(),
        ));
        if self.lookup_fails {
            return Err(Diagnostic::error(
                "SUTRA.TEST.STORE_DOWN",
                "the store is down",
            ));
        }
        Ok(self
            .aliases
            .get(&(name.to_string(), value.to_string()))
            .cloned())
    }
    async fn commit_complete(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        *self.resumes.borrow_mut() += 1;
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
        *self.resumes.borrow_mut() += 1;
        Ok(())
    }
    async fn commit_emissions(
        &self,
        _deployment: &DeploymentId,
        _emissions: &[OutboxEmission],
    ) -> Result<(), Diagnostic> {
        Ok(())
    }
    async fn claim_instance(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<InstanceClaimOutcome, Diagnostic> {
        Ok(InstanceClaimOutcome::Granted)
    }
    async fn release_instance(
        &self,
        _deployment: &DeploymentId,
        _instance_id: &str,
    ) -> Result<(), Diagnostic> {
        Ok(())
    }
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

/// Reports the message type the body names, and counts its decodes.
struct EchoTypeCodec {
    decodes: Arc<AtomicUsize>,
}

impl PayloadCodec for EchoTypeCodec {
    fn name(&self) -> &str {
        "echo-type"
    }
    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/plain".to_string()]
    }
    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        self.decodes.fetch_add(1, Ordering::SeqCst);
        let s = String::from_utf8_lossy(body).into_owned();
        DecodeResult::ok(
            CodecValue::Text(s.clone()),
            content_type.unwrap_or("text/plain"),
        )
        .with_message_type(&s)
    }
    fn encode(&self, _p: &CodecValue, _ct: Option<&str>) -> Result<Vec<u8>, String> {
        Err("decode-only".to_string())
    }
}

/// `p`: start on `both-in` (type `Order`) with a correlate alias on the `corr` header, then a
/// receive task on `both-in` (type `Reply`); and a start on `start-in`.
const START_AND_WAIT: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="both-in" messageTypeValue="Order"/>
        <q:alias name="corrKey" expression="event.headers.corr" unique="true" onConflict="correlate"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:startEvent id="S2">
      <bpmn:extensionElements><q:source channel="start-in"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:receiveTask id="W">
      <bpmn:extensionElements><q:source channel="both-in" messageTypeValue="Reply"/></bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="W"/>
    <bpmn:sequenceFlow id="f3" sourceRef="S2" targetRef="W"/>
    <bpmn:sequenceFlow id="f2" sourceRef="W" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

/// Two processes, each waiting on `relay-in` with its own alias.
const TWO_WAITERS: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="a">
    <bpmn:startEvent id="SA"><bpmn:extensionElements><q:source channel="a-in"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:receiveTask id="WA">
      <bpmn:extensionElements>
        <q:source channel="relay-in"/>
        <q:alias name="aKey" expression="event.headers.corr"/>
      </bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:endEvent id="EA"/>
    <bpmn:sequenceFlow id="a1" sourceRef="SA" targetRef="WA"/>
    <bpmn:sequenceFlow id="a2" sourceRef="WA" targetRef="EA"/>
  </bpmn:process>
  <bpmn:process id="b">
    <bpmn:startEvent id="SB"><bpmn:extensionElements><q:source channel="b-in"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:receiveTask id="WB">
      <bpmn:extensionElements>
        <q:source channel="relay-in"/>
        <q:alias name="bKey" expression="event.headers.corr"/>
      </bpmn:extensionElements>
    </bpmn:receiveTask>
    <bpmn:endEvent id="EB"/>
    <bpmn:sequenceFlow id="b1" sourceRef="SB" targetRef="WB"/>
    <bpmn:sequenceFlow id="b2" sourceRef="WB" targetRef="EB"/>
  </bpmn:process>
</bpmn:definitions>"#;

struct Setup {
    engine: ChannelEngine,
    decodes: Arc<AtomicUsize>,
}

/// One engine serving `bpmn` under each of `deployments` (one owner), every channel bound with
/// `codec`. `archive` scopes the echo codec to each deployment, else it is one engine built-in.
fn setup(bpmn: &str, deployments: &[DeploymentId], archive: bool, bridge: Rc<Bridge>) -> Setup {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let decodes = Arc::new(AtomicUsize::new(0));
    let mut codecs = CodecRegistry::with_builtins();
    let logical = logical_urn("codec", "echo-type");
    if archive {
        for d in deployments {
            codecs.register_under(
                &archive_key(&logical, d),
                Arc::new(EchoTypeCodec {
                    decodes: Arc::clone(&decodes),
                }),
            );
        }
    } else {
        codecs.register_under(
            &builtin_key(&logical),
            Arc::new(EchoTypeCodec {
                decodes: Arc::clone(&decodes),
            }),
        );
    }
    let channels = ["both-in", "start-in", "relay-in", "a-in", "b-in"];
    let mut builder = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            codecs,
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    );
    let mut versions = Vec::new();
    for (i, d) in deployments.iter().enumerate() {
        let ns = Namespace::new(TENANT, "waits", &format!("{}.0.0", i + 1));
        let bindings: Vec<ChannelBinding> = channels
            .iter()
            .map(|c| ChannelBinding::new(c, ns.clone(), d.clone(), "echo-type"))
            .collect();
        if i == 0 {
            for b in &bindings {
                builder = builder.with_binding(b.clone());
            }
        }
        builder = builder.with_module(d, &module);
        versions.push(("owner-waits".to_string(), d.clone(), bindings));
    }
    let engine = builder
        .with_owner_scopes(Arc::new(OwnerScopes::build(versions)))
        .with_instance_bridge(bridge as Rc<dyn InstanceBridge>)
        .build();
    Setup { engine, decodes }
}

fn message(channel: &str, body: &str, corr: Option<&str>) -> InboundMessage {
    let mut headers = BTreeMap::new();
    if let Some(corr) = corr {
        headers.insert("corr".to_string(), corr.to_string());
    }
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: Namespace::new(TENANT, "waits", "1.0.0").module_key(),
        channel: channel.to_string(),
        headers,
        body: body.as_bytes().to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: "msg-1".to_string(),
        explicit_event_id: false,
        received_at: "2026-11-14T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn parked(process: &str, deployment: &DeploymentId, at: &str) -> SuspendedInstance {
    SuspendedInstance {
        process_id: process.to_string(),
        deployment_id: deployment.value().to_string(),
        status: "SUSPENDED".to_string(),
        suspended: true,
        waiting_nodes: vec![at.to_string()],
        completed_nodes: vec!["S".to_string()],
        start_node: "S".to_string(),
        ..Default::default()
    }
}

#[test]
fn the_fast_path_makes_no_alias_lookup() {
    let bridge = Rc::new(Bridge::default());
    let s = setup(
        START_AND_WAIT,
        &[namespace_dep()],
        false,
        Rc::clone(&bridge),
    );
    // A start-only channel: nothing can be waiting.
    drive(
        s.engine
            .dispatch(&message("start-in", "Anything", Some("C-1"))),
    )
    .expect("starts");
    assert!(bridge.lookups.borrow().is_empty());
    assert_eq!(*bridge.parks.borrow(), 1, "it started");
}

#[test]
fn one_version_a_message_of_the_starts_type_never_probes() {
    // The wait carries its own alias, so a probe would look up `waitKey`; the start's own
    // `onConflict="correlate"` check looks up `corrKey` whatever happens first.
    let bpmn = START_AND_WAIT.replace(
        r#"<q:source channel="both-in" messageTypeValue="Reply"/>"#,
        r#"<q:source channel="both-in" messageTypeValue="Reply"/><q:alias name="waitKey" expression="event.headers.corr"/>"#,
    );
    let bridge = Rc::new(Bridge::default());
    let s = setup(&bpmn, &[namespace_dep()], false, Rc::clone(&bridge));
    // `both-in` has a wait, but typed `Reply`: an `Order` cannot answer it.
    drive(s.engine.dispatch(&message("both-in", "Order", Some("C-1")))).expect("starts");
    let names: Vec<String> = bridge
        .lookups
        .borrow()
        .iter()
        .map(|l| l.1.clone())
        .collect();
    assert!(
        !names.contains(&"waitKey".to_string()),
        "no probe: {names:?}"
    );
    assert_eq!(*bridge.parks.borrow(), 1);
    // A `Reply` with no parked instance is probed, then fails as an uncorrelated relay.
    let _ = drive(s.engine.dispatch(&message("both-in", "Reply", Some("C-2"))));
    assert!(bridge
        .lookups
        .borrow()
        .iter()
        .any(|l| l.1 == "waitKey" && l.2 == "C-2"));
}

#[test]
fn a_correlated_reply_resumes_and_starts_nothing() {
    let d = namespace_dep();
    let bridge = Rc::new(Bridge {
        aliases: HashMap::from([(("corrKey".into(), "C-1".into()), "inst-1".into())]),
        loaded: Some(parked("p", &d, "W")),
        ..Default::default()
    });
    let s = setup(START_AND_WAIT, &[d], false, Rc::clone(&bridge));
    drive(s.engine.dispatch(&message("both-in", "Reply", Some("C-1")))).expect("resumes");
    assert_eq!(*bridge.resumes.borrow(), 1);
    assert_eq!(*bridge.parks.borrow(), 0, "no new instance");
}

#[test]
fn a_failed_store_lookup_refuses_rather_than_starts() {
    // An untyped wait on a channel that also starts: the probe runs, and its store fails.
    let bpmn = START_AND_WAIT
        .replace(
            r#"<q:source channel="both-in" messageTypeValue="Order"/>"#,
            r#"<q:source channel="both-in"/>"#,
        )
        .replace(
            r#"<q:source channel="both-in" messageTypeValue="Reply"/>"#,
            r#"<q:source channel="both-in"/>"#,
        );
    let bridge = Rc::new(Bridge {
        lookup_fails: true,
        ..Default::default()
    });
    let s = setup(&bpmn, &[namespace_dep()], false, Rc::clone(&bridge));
    let refused = drive(s.engine.dispatch(&message("both-in", "Order", Some("C-1"))));
    assert_eq!(
        refused.err().map(|d| d.code),
        Some("SUTRA.TEST.STORE_DOWN".to_string())
    );
    assert_eq!(*bridge.parks.borrow(), 0, "never guessed into a start");
}

#[test]
fn an_alias_hit_not_at_the_wait_is_refused_not_started() {
    let d = namespace_dep();
    let bridge = Rc::new(Bridge {
        aliases: HashMap::from([(("corrKey".into(), "C-1".into()), "inst-1".into())]),
        // Correlated, but not parked yet: the reply overtook the park.
        loaded: Some(SuspendedInstance {
            suspended: false,
            status: "RUNNING".to_string(),
            ..parked("p", &d, "W")
        }),
        ..Default::default()
    });
    let s = setup(START_AND_WAIT, &[d], false, Rc::clone(&bridge));
    let refused = drive(s.engine.dispatch(&message("both-in", "Reply", Some("C-1"))))
        .expect_err("not at the wait");
    assert_eq!(
        refused
            .attributes
            .get(WAIT_PENDING_ATTR)
            .map(String::as_str),
        Some("true"),
        "{}: {}",
        refused.code,
        refused.message
    );
    assert_eq!(*bridge.parks.borrow(), 0);
    assert_eq!(*bridge.resumes.borrow(), 0);
}

#[test]
fn the_second_process_on_a_channel_is_tried() {
    let d = namespace_dep();
    let bridge = Rc::new(Bridge {
        aliases: HashMap::from([(("bKey".into(), "C-9".into()), "inst-b".into())]),
        loaded: Some(SuspendedInstance {
            start_node: "SB".to_string(),
            completed_nodes: vec!["SB".to_string()],
            ..parked("b", &d, "WB")
        }),
        ..Default::default()
    });
    let s = setup(TWO_WAITERS, &[d], false, Rc::clone(&bridge));
    drive(s.engine.dispatch(&message("relay-in", "Any", Some("C-9")))).expect("b resumes");
    let names: Vec<String> = bridge
        .lookups
        .borrow()
        .iter()
        .map(|l| l.1.clone())
        .collect();
    assert_eq!(names, vec!["aKey".to_string(), "bKey".to_string()]);
    assert_eq!(*bridge.resumes.borrow(), 1);
}

#[test]
fn an_identical_codec_decodes_once() {
    // Two versions bound to one engine built-in codec: the older version's probe reuses the decode.
    let bridge = Rc::new(Bridge::default());
    let s = setup(TWO_WAITERS, &[dep(1), dep(2)], false, Rc::clone(&bridge));
    let _ = drive(s.engine.dispatch(&message("relay-in", "Any", Some("C-1"))));
    let probes: Vec<String> = bridge
        .lookups
        .borrow()
        .iter()
        .map(|l| l.0.clone())
        .collect();
    assert!(
        probes.contains(&dep(2).value().to_string()),
        "v2 was probed: {probes:?}"
    );
    // One decode for both versions' probes; the miss's relay walk decodes nothing more.
    assert_eq!(s.decodes.load(Ordering::SeqCst), 1);
}

#[test]
fn two_archive_codecs_decode_once_each() {
    let bridge = Rc::new(Bridge::default());
    let s = setup(TWO_WAITERS, &[dep(1), dep(2)], true, Rc::clone(&bridge));
    let _ = drive(s.engine.dispatch(&message("relay-in", "Any", Some("C-1"))));
    assert_eq!(
        s.decodes.load(Ordering::SeqCst),
        2,
        "the older version decodes with its own codec"
    );
}

fn namespace_dep() -> DeploymentId {
    dep(1)
}
