//! `PayloadCodec::seal`: the governing codec's last transform of outbound bytes.
//!
//! A template render reaches the wire without passing through `encode`, so a codec that must
//! change what it sends (append a signature, say) has exactly one place to do it: after its own
//! read-back of the bytes passed, before they are committed to the outbox. These tests pin that
//! order and the fail-closed posture, for a send and for a reply, and that a codec that does not
//! seal leaves every byte as it was.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    ChannelBinding, ChannelEngine, CodecRegistry, CollectingOutbox, DispatchOutcome, DrainingSink,
    FormatRegistry, InboundChain, InboundMessage, Namespace, ValidatorRegistry,
};
use sutra_codec_spi::{CodecValue, DecodeResult, IssueSeverity, PayloadCodec, ValidationIssue};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

use crate::outbound_validation_test::{ContractCodec, CODEC as CONTRACT_CODEC};
use crate::support::drive;

const SEAL_CODEC: &str = "seal-codec";
const MARK: &str = "<!--sealed-->";
const GOOD: &str = "<Packet><Document>ok</Document></Packet>";
const BAD: &str = "<Packet></Packet>";
/// A valid body the sealing codec refuses to seal (standing in for an unusable key).
const UNSEALABLE: &str = "<Packet><Document>ok</Document><NoSeal/></Packet>";
const TENANT: &str = "acme";

/// The contract of [`ContractCodec`] (the body must contain `<Document>`), plus a seal that
/// appends [`MARK`].
struct SealCodec;

impl PayloadCodec for SealCodec {
    fn name(&self) -> &str {
        SEAL_CODEC
    }

    fn accepted_content_types(&self) -> Vec<String> {
        vec!["application/xml".to_string()]
    }

    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        let text = String::from_utf8_lossy(body).to_string();
        let mut result = DecodeResult::ok(
            CodecValue::Text(text.clone()),
            content_type.unwrap_or("application/xml"),
        );
        if !text.contains("<Document>") {
            result.issues.push(ValidationIssue {
                code: "TEST.MISSING_DOCUMENT".to_string(),
                severity: IssueSeverity::Error,
                path: "/Packet/Document".to_string(),
                message: "the packet carries no <Document>".to_string(),
                value: None,
            });
        }
        result
    }

    fn encode(&self, payload: &CodecValue, _content_type: Option<&str>) -> Result<Vec<u8>, String> {
        match payload {
            CodecValue::Text(s) => Ok(s.clone().into_bytes()),
            other => Err(format!("cannot encode {other:?}")),
        }
    }

    fn seal(&self, body: &[u8], _content_type: Option<&str>) -> Result<Option<Vec<u8>>, String> {
        let text = String::from_utf8_lossy(body);
        if text.contains("<NoSeal/>") {
            return Err("the sealing key is unusable".to_string());
        }
        Ok(Some(format!("{text}{MARK}").into_bytes()))
    }
}

fn codecs() -> CodecRegistry {
    let mut codecs = CodecRegistry::with_builtins();
    codecs.register_shared(Arc::new(ContractCodec));
    codecs.register_shared(Arc::new(SealCodec));
    codecs
}

fn chain() -> InboundChain {
    InboundChain::new(
        codecs(),
        FormatRegistry::with_builtins(),
        ValidatorRegistry::new(),
    )
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000c5").expect("valid deployment id")
}

fn seal(
    declared: Option<&str>,
    channel_codec: &str,
    body: &str,
) -> Result<Option<Vec<u8>>, sutra_channels::Diagnostic> {
    chain().seal_outbound(
        &deployment(),
        "out",
        declared,
        channel_codec,
        body.as_bytes(),
        Some("application/xml"),
        "Node 'Send' of process 'p'",
    )
}

// ---- the chain ------------------------------------------------------------------------------

#[test]
fn a_sealing_codec_rewrites_what_it_admitted() {
    let sealed = seal(Some(SEAL_CODEC), "", GOOD)
        .expect("a valid body seals")
        .expect("the codec sealed");
    assert_eq!(String::from_utf8(sealed).unwrap(), format!("{GOOD}{MARK}"));
    // The channel's codec governs the same way when the output states none.
    assert!(seal(None, SEAL_CODEC, GOOD).unwrap().is_some());
}

#[test]
fn validation_runs_before_the_seal() {
    // An invalid body is refused by the read-back; the seal never sees it.
    let err = seal(Some(SEAL_CODEC), "", BAD).expect_err("the contract refuses it");
    assert_eq!(err.code, sutra_channels::codes::OUTBOUND_VALIDATION_REJECT);
}

#[test]
fn a_seal_error_refuses_the_message() {
    let err = seal(Some(SEAL_CODEC), "", UNSEALABLE).expect_err("the seal fails");
    assert_eq!(err.code, sutra_channels::codes::OUTBOUND_SEAL_FAILED);
    assert!(
        err.message.contains("the sealing key is unusable"),
        "{}",
        err.message
    );
    assert!(err.message.contains("Node 'Send'"), "{}", err.message);
    assert_eq!(
        err.attributes.get("codec").map(String::as_str),
        Some(SEAL_CODEC)
    );
}

#[test]
fn a_codec_that_does_not_seal_and_schema_less_egress_send_as_rendered() {
    assert_eq!(seal(Some(CONTRACT_CODEC), "", GOOD).unwrap(), None);
    assert_eq!(seal(None, "raw-text", GOOD).unwrap(), None);
    assert_eq!(seal(None, "", GOOD).unwrap(), None);
}

// ---- through the dispatcher -----------------------------------------------------------------

fn namespace() -> Namespace {
    Namespace::new(TENANT, "seal", "1.0.0")
}

/// S → Render (`responseBody` := the body of the inbound) → Send (codec stated) → Reply (codec
/// stated) → E.
const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Send" implementation="render">
      <bpmn:extensionElements>
        <q:send destination="notify://ops/out" codec="seal-codec"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Reply" implementation="render">
      <bpmn:extensionElements><q:reply codec="seal-codec"/></bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Send"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Send" targetRef="Reply"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Reply" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn engine(outbox: Rc<CollectingOutbox>) -> ChannelEngine {
    let module = BpmnModelLoader::new().load(BPMN.as_bytes()).expect("BPMN");
    let tasks = TaskRegistry::new().register("render", |input, _ctx| {
        let body = match input {
            FeelValue::Map(m) => match m.get("req") {
                Some(FeelValue::String(s)) => s.clone(),
                other => panic!("expected the inbound text, got {other:?}"),
            },
            other => panic!("expected the task input map, got {other:?}"),
        };
        Ok(FeelValue::Map(BTreeMap::from([(
            "responseBody".to_string(),
            FeelValue::String(body),
        )])))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    ChannelEngine::builder(executor, sink, chain())
        .with_binding(ChannelBinding::new(
            "in",
            namespace(),
            deployment(),
            "raw-text",
        ))
        .with_module(&deployment(), &module)
        .with_outbox(outbox)
        .build()
}

fn inbound(body: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: "in".to_string(),
        headers: BTreeMap::new(),
        body: body.as_bytes().to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: "seal-1".to_string(),
        explicit_event_id: true,
        received_at: "2026-11-19T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

#[test]
fn a_send_and_a_reply_leave_sealed() {
    let outbox = Rc::new(CollectingOutbox::new());
    let engine = engine(Rc::clone(&outbox));
    let outcome = drive(engine.dispatch(&inbound(GOOD))).expect("dispatch");
    let DispatchOutcome::Completed { reply, .. } = outcome else {
        panic!("expected Completed, got {outcome:?}");
    };
    let reply = reply.expect("the declared reply");
    assert_eq!(
        String::from_utf8(reply.body.into_inner()).unwrap(),
        format!("{GOOD}{MARK}"),
        "the reply is sealed"
    );
    let sent = outbox.emissions();
    assert_eq!(sent.len(), 1, "one send");
    assert_eq!(
        sent[0].body_utf8(),
        format!("{GOOD}{MARK}"),
        "the send is sealed"
    );
}

#[test]
fn a_seal_error_sends_nothing() {
    let outbox = Rc::new(CollectingOutbox::new());
    let engine = engine(Rc::clone(&outbox));
    let result = drive(engine.dispatch(&inbound(UNSEALABLE)));
    let code = match result {
        Err(d) => d.code,
        Ok(DispatchOutcome::DeadLettered { cause_code, .. }) => cause_code,
        Ok(other) => panic!("expected a refusal, got {other:?}"),
    };
    assert_eq!(code, sutra_channels::codes::OUTBOUND_SEAL_FAILED);
    assert!(outbox.is_empty(), "nothing reached the outbox");
}

// ---- sealed at the emitting node (the executor's `OutboundSealSeam`) -------------------------

/// S → Render → Send (codec stated) → Record (copies `sent` out) → E.
const BPMN_RECORDED: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="p">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in" name="req"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Send" implementation="render">
      <bpmn:extensionElements>
        <q:send destination="notify://ops/out" codec="seal-codec"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Record" implementation="record"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Send"/>
    <bpmn:sequenceFlow id="f2" sourceRef="Send" targetRef="Record"/>
    <bpmn:sequenceFlow id="f3" sourceRef="Record" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

type Recorded = Rc<std::cell::RefCell<Option<FeelValue>>>;

fn engine_sealing_at_the_node(outbox: Rc<CollectingOutbox>, recorded: Recorded) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(BPMN_RECORDED.as_bytes())
        .expect("BPMN");
    let tasks = TaskRegistry::new()
        .register("render", |input, _ctx| {
            let FeelValue::Map(m) = input else {
                panic!("expected the task input map")
            };
            Ok(FeelValue::Map(BTreeMap::from([(
                "responseBody".to_string(),
                m.get("req").cloned().expect("the inbound text"),
            )])))
        })
        .register("record", move |_, ctx| {
            *recorded.borrow_mut() = ctx.variable("sent").cloned();
            Ok(FeelValue::Map(BTreeMap::new()))
        });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .with_outbound_seal_seam(Rc::new(chain()) as Rc<dyn sutra_executor::OutboundSealSeam>)
        .build();
    ChannelEngine::builder(executor, sink, chain())
        .with_binding(ChannelBinding::new(
            "in",
            namespace(),
            deployment(),
            "raw-text",
        ))
        .with_module(&deployment(), &module)
        .with_outbox(outbox)
        .build()
}

#[test]
fn sealed_at_the_node_the_flow_sees_the_bytes_the_outbox_holds_sealed_once() {
    let outbox = Rc::new(CollectingOutbox::new());
    let recorded: Recorded = Rc::default();
    let engine = engine_sealing_at_the_node(Rc::clone(&outbox), Rc::clone(&recorded));
    drive(engine.dispatch(&inbound(GOOD))).expect("dispatch");
    let sent = outbox.emissions();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].body_utf8(),
        format!("{GOOD}{MARK}"),
        "sealed once, not again at the commit"
    );
    let Some(FeelValue::Map(view)) = recorded.borrow().clone() else {
        panic!("the step after the send saw no sent view");
    };
    assert_eq!(
        view.get("body"),
        Some(&FeelValue::String(format!("{GOOD}{MARK}")))
    );
    assert_eq!(
        view.get("outboxKey"),
        Some(&FeelValue::String(sent[0].outbox_key.clone()))
    );
}

#[test]
fn sealed_at_the_node_a_seal_error_still_sends_nothing() {
    let outbox = Rc::new(CollectingOutbox::new());
    let engine = engine_sealing_at_the_node(Rc::clone(&outbox), Rc::default());
    let result = drive(engine.dispatch(&inbound(UNSEALABLE)));
    let code = match result {
        Err(d) => d.code,
        Ok(DispatchOutcome::DeadLettered { cause_code, .. }) => cause_code,
        Ok(other) => panic!("expected a refusal, got {other:?}"),
    };
    assert_eq!(code, sutra_channels::codes::OUTBOUND_SEAL_FAILED);
    assert!(outbox.is_empty(), "nothing reached the outbox");
}
