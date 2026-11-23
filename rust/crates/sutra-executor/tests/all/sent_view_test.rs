//! The `sent` view: every outbound emission binds a read-only view of what it will put on the
//! wire, sealed through the executor's `OutboundSealSeam`, so a later BPMN step can record it.

use std::rc::Rc;

use crate::common::*;
use sutra_executor::{
    CollectingSink, DeploymentId, EmissionSink, OutboundChannelRegistry, OutboundSealRequest,
    OutboundSealSeam, ResolvedOutboundChannel, TaskRegistry, TokenExecutor,
};
use sutra_feel::FeelValue;

/// A seam whose codec "signs" every body it governs by appending a trailer, and records what it
/// was asked.
#[derive(Default)]
struct TrailerSeam {
    asked: std::cell::RefCell<Vec<(Option<String>, String)>>,
}

impl OutboundSealSeam for TrailerSeam {
    fn seal_outbound(
        &self,
        _deployment: &DeploymentId,
        request: OutboundSealRequest<'_>,
    ) -> Result<Option<Vec<u8>>, String> {
        self.asked.borrow_mut().push((
            request.declared_codec.map(str::to_string),
            request.channel_codec.to_string(),
        ));
        let mut sealed = request.body.to_vec();
        sealed.extend_from_slice(b"|sig");
        Ok(Some(sealed))
    }
}

struct RefusingSeam;

impl OutboundSealSeam for RefusingSeam {
    fn seal_outbound(
        &self,
        _deployment: &DeploymentId,
        _request: OutboundSealRequest<'_>,
    ) -> Result<Option<Vec<u8>>, String> {
        Err("SUTRA.OUTBOUND.VALIDATION_REJECT".to_string())
    }
}

fn send_then_record(send: &str, declarations: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:extensionElements>{declarations}</bpmn:extensionElements>
            <bpmn:startEvent id="S"/>
            <bpmn:sendTask id="Send">
              <bpmn:extensionElements>{send}</bpmn:extensionElements>
            </bpmn:sendTask>
            <bpmn:serviceTask id="Record" implementation="${{record}}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Send"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Send" targetRef="Record"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Record" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

/// A `record` task that copies `sent` into `recorded`, as a flow's audit step would.
fn recording_registry() -> TaskRegistry {
    TaskRegistry::new().register("record", |_, ctx| {
        let sent = ctx.variable("sent").cloned().unwrap_or(FeelValue::Null);
        ok_map(&[("recorded", sent)])
    })
}

fn field<'a>(view: &'a FeelValue, name: &str) -> Option<&'a FeelValue> {
    match view {
        FeelValue::Map(m) => m.get(name),
        _ => None,
    }
}

#[tokio::test]
async fn the_step_after_a_send_reads_the_sealed_body_and_outbox_key() {
    let process = proc(
        &send_then_record(
            r#"<q:send destination="https://ops.example/notify" contentType="text/plain" codec="signed"/>"#,
            "",
        ),
        "p1",
    );
    let sink = Rc::new(CollectingSink::new());
    let seam = Rc::new(TrailerSeam::default());
    let result = TokenExecutor::builder(recording_registry())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_outbound_seal_seam(Rc::clone(&seam) as Rc<dyn OutboundSealSeam>)
        .build()
        .execute_sync(&process, vars(&[("payload.body", string("hello"))]))
        .await
        .unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    let emission = &emissions[0];
    assert!(
        emission.sealed,
        "sealed at the node, so the commit must not seal again"
    );
    assert_eq!(emission.body_utf8(), "hello|sig");
    assert!(!emission.outbox_key.is_empty());
    // The author's codec is the declared one; a bare destination infers none.
    assert_eq!(
        seam.asked.borrow().as_slice(),
        &[(Some("signed".to_string()), String::new())]
    );

    let recorded = result.output("recorded").expect("the record step saw sent");
    assert_eq!(field(recorded, "body"), Some(&string("hello|sig")));
    assert_eq!(
        field(recorded, "bodyBase64"),
        None,
        "valid UTF-8 has no base64"
    );
    assert_eq!(field(recorded, "contentType"), Some(&string("text/plain")));
    assert_eq!(
        field(recorded, "destination"),
        Some(&string("https://ops.example/notify"))
    );
    assert_eq!(
        field(recorded, "outboxKey"),
        Some(&string(&emission.outbox_key))
    );
    assert_eq!(field(recorded, "node"), Some(&string("Send")));
    assert_eq!(field(recorded, "kind"), Some(&string("send")));
    assert_eq!(field(recorded, "mode"), Some(&string("native")));
    assert!(
        result.output("sent.redacted").is_none(),
        "nothing is masked in an instance that masks nothing"
    );
}

#[tokio::test]
async fn a_channel_send_is_held_to_the_channels_codec() {
    let process = proc(&send_then_record(r#"<q:send channel="out"/>"#, ""), "p1");
    let mut channels = OutboundChannelRegistry::new();
    channels.register(
        &DeploymentId::unresolved(),
        ResolvedOutboundChannel::resolve(
            "out",
            "https",
            "https://peer.example/in",
            None,
            None,
            None,
            "none",
        )
        .with_codec("peer-codec"),
    );
    let seam = Rc::new(TrailerSeam::default());
    let result = TokenExecutor::builder(recording_registry())
        .with_outbound_channels(channels)
        .with_outbound_seal_seam(Rc::clone(&seam) as Rc<dyn OutboundSealSeam>)
        .build()
        .execute_sync(&process, vars(&[("payload.body", string("x"))]))
        .await
        .unwrap();
    assert_eq!(
        seam.asked.borrow().as_slice(),
        &[(None, "peer-codec".to_string())]
    );
    let recorded = result.output("recorded").unwrap();
    assert_eq!(
        field(recorded, "destination"),
        Some(&string("https://peer.example/in"))
    );
    assert_eq!(field(recorded, "body"), Some(&string("x|sig")));
}

#[tokio::test]
async fn a_body_that_is_not_utf8_also_carries_base64() {
    struct BinarySeam;
    impl OutboundSealSeam for BinarySeam {
        fn seal_outbound(
            &self,
            _deployment: &DeploymentId,
            _request: OutboundSealRequest<'_>,
        ) -> Result<Option<Vec<u8>>, String> {
            Ok(Some(vec![0xff, 0x00, 0x41]))
        }
    }
    let process = proc(
        &send_then_record(r#"<q:send destination="https://ops.example/bin"/>"#, ""),
        "p1",
    );
    let result = TokenExecutor::builder(recording_registry())
        .with_outbound_seal_seam(Rc::new(BinarySeam) as Rc<dyn OutboundSealSeam>)
        .build()
        .execute_sync(&process, vars(&[("payload.body", string("x"))]))
        .await
        .unwrap();
    let recorded = result.output("recorded").unwrap();
    assert_eq!(field(recorded, "bodyBase64"), Some(&string("/wBB")));
    assert_eq!(field(recorded, "contentType"), Some(&FeelValue::Null));
}

#[tokio::test]
async fn a_refused_seal_leaves_the_emission_to_the_commit_and_binds_nothing() {
    let process = proc(
        &send_then_record(r#"<q:send destination="https://ops.example/notify"/>"#, ""),
        "p1",
    );
    let sink = Rc::new(CollectingSink::new());
    let result = TokenExecutor::builder(recording_registry())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_outbound_seal_seam(Rc::new(RefusingSeam) as Rc<dyn OutboundSealSeam>)
        .build()
        .execute_sync(&process, vars(&[("payload.body", string("hello"))]))
        .await
        .unwrap();
    let emission = &sink.emissions()[0];
    assert!(!emission.sealed, "the commit re-checks and fails the step");
    assert_eq!(emission.body_utf8(), "hello");
    assert_eq!(result.output("recorded"), Some(&FeelValue::Null));
}

#[tokio::test]
async fn without_a_seam_sent_shows_the_body_and_the_commit_seals() {
    let process = proc(
        &send_then_record(r#"<q:send destination="https://ops.example/notify"/>"#, ""),
        "p1",
    );
    let sink = Rc::new(CollectingSink::new());
    let result = TokenExecutor::builder(recording_registry())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build()
        .execute_sync(&process, vars(&[("payload.body", string("hello"))]))
        .await
        .unwrap();
    assert!(!sink.emissions()[0].sealed);
    let recorded = result.output("recorded").unwrap();
    assert_eq!(field(recorded, "body"), Some(&string("hello")));
}

#[tokio::test]
async fn an_instance_that_masks_anything_masks_what_it_sent() {
    let send = r#"<q:send destination="https://ops.example/notify">
                    <q:header name="x-ref" value="&quot;r-1&quot;"/>
                  </q:send>"#;
    let process = proc(
        &send_then_record(send, r#"<q:variable name="card" sensitive="true"/>"#),
        "p1",
    );
    let result = TokenExecutor::builder(recording_registry())
        .with_feel()
        .with_outbound_seal_seam(Rc::new(TrailerSeam::default()) as Rc<dyn OutboundSealSeam>)
        .build()
        .execute_sync(&process, vars(&[("payload.body", string("4111"))]))
        .await
        .unwrap();
    let sent = result.output("sent").unwrap();
    assert_eq!(field(sent, "body"), Some(&string("4111|sig")));
    let masked = result.output("sent.redacted").expect("a masked companion");
    assert_eq!(field(masked, "body"), Some(&string("[REDACTED]")));
    assert_eq!(
        field(masked, "headers"),
        Some(&fmap(&[("x-ref", string("[REDACTED]"))]))
    );
    assert_eq!(field(masked, "outboxKey"), field(sent, "outboxKey"));
}

/// The guide's recording pattern, as written there: a data task after the send writes `sent` to a
/// data store keyed by its outbox key.
#[tokio::test]
async fn a_data_task_after_the_send_stores_what_was_sent() {
    use sutra_executor::{DataStore, InMemoryDataStore};
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:dataObject id="doSent" name="sent"/>
            <bpmn:dataStore id="hops" name="hops"/>
            <bpmn:dataStoreReference id="refHop" dataStoreRef="hops">
              <bpmn:extensionElements><q:store key="sent.outboxKey"/></bpmn:extensionElements>
            </bpmn:dataStoreReference>
            <bpmn:startEvent id="S"/>
            <bpmn:sendTask id="SendRequest">
              <bpmn:extensionElements><q:send destination="https://peer.example/in"/></bpmn:extensionElements>
            </bpmn:sendTask>
            <bpmn:serviceTask id="RecordSent" name="Record what was sent">
              <bpmn:dataOutputAssociation>
                <bpmn:sourceRef>doSent</bpmn:sourceRef>
                <bpmn:targetRef>refHop</bpmn:targetRef>
              </bpmn:dataOutputAssociation>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="SendRequest"/>
            <bpmn:sequenceFlow id="toRecord" sourceRef="SendRequest" targetRef="RecordSent"/>
            <bpmn:sequenceFlow id="f3" sourceRef="RecordSent" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let hops = Rc::new(InMemoryDataStore::new("hops"));
    let lookup = Rc::clone(&hops);
    let sink = Rc::new(CollectingSink::new());
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "hops").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_outbound_seal_seam(Rc::new(TrailerSeam::default()) as Rc<dyn OutboundSealSeam>)
        .build()
        .execute_sync(&process, vars(&[("payload.body", string("<Doc/>"))]))
        .await
        .unwrap();
    let key = sink.emissions()[0].outbox_key.clone();
    let stored = hops.get(&key).await.unwrap().expect("a row per hop");
    assert_eq!(field(&stored, "body"), Some(&string("<Doc/>|sig")));
    assert_eq!(field(&stored, "outboxKey"), Some(&string(&key)));
}
