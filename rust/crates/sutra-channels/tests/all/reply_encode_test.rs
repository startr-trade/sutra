//! Outbound reply encoding (slice 7c — native reply): a flow produces
//! a structured `responseObject` and the engine encodes it via the channel codec to the
//! INBOUND content type (json in → json out, the "native reply" continuity); a
//! pre-serialised `responseBody` wins (manual override); no `responseObject` leaves the
//! outputs untouched (no reply body).

use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    ChannelBinding, ChannelEngine, CodecRegistry, DispatchOutcome, DrainingSink, InboundChain,
    InboundMessage, Namespace, ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

use crate::support::drive;

const TENANT: &str = "acme";

/// Start event decodes via the channel codec; the `reply` service task echoes the payload
/// back as `responseObject`.
const BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="echo">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="reply-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T" implementation="reply"/>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#;

fn namespace() -> Namespace {
    Namespace::new(TENANT, "mod", "1.0.0")
}

fn engine(
    reply_task: impl Fn(&FeelValue) -> BTreeMap<String, FeelValue> + 'static,
) -> ChannelEngine {
    let module = BpmnModelLoader::new().load(BPMN.as_bytes()).expect("BPMN");
    let tasks = TaskRegistry::new().register("reply", move |input, _ctx| {
        Ok(FeelValue::Map(reply_task(input)))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(ChannelBinding::new(
        "reply-in",
        namespace(),
        DeploymentId::of("dep-000000000000000000000041").expect("valid deployment id"),
        "json",
    ))
    .with_module(
        &DeploymentId::of("dep-000000000000000000000041").expect("valid deployment id"),
        &module,
    )
    .build()
}

fn inbound(body: &[u8], content_type: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: "reply-in".to_string(),
        headers: BTreeMap::new(),
        body: body.to_vec().into(),
        content_type: Some(content_type.to_string()),
        idempotency_key: "m1".to_string(),
        explicit_event_id: false,
        received_at: "2026-06-29T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

#[test]
fn response_object_is_encoded_to_inbound_content_type() {
    // The task echoes the decoded payload back as responseObject.
    let engine = engine(|input| BTreeMap::from([("responseObject".to_string(), input.clone())]));

    let outcome =
        drive(engine.dispatch(&inbound(b"{\"k\":\"v\"}", "application/json"))).expect("dispatch");
    let DispatchOutcome::Completed { reply, .. } = outcome else {
        panic!("expected Completed");
    };
    let reply = reply.expect("a native reply body");
    let body = String::from_utf8(reply.body.into_inner()).expect("utf8");
    assert!(body.contains("\"k\""), "encoded body: {body}");
    assert!(body.contains("\"v\""), "encoded body: {body}");
    // Native reply continuity: the reply content-type mirrors the inbound.
    assert_eq!(reply.content_type, "application/json");
}

#[test]
fn manual_response_body_wins_over_codec_encode() {
    // The flow set responseBody itself → the engine does not re-encode.
    let engine = engine(|input| {
        BTreeMap::from([
            ("responseObject".to_string(), input.clone()),
            (
                "responseBody".to_string(),
                FeelValue::String("manual".to_string()),
            ),
        ])
    });

    let DispatchOutcome::Completed { reply, .. } =
        drive(engine.dispatch(&inbound(b"{\"k\":\"v\"}", "application/json"))).expect("dispatch")
    else {
        panic!("expected Completed");
    };
    assert_eq!(reply.expect("reply").body.into_inner(), b"manual");
}

#[test]
fn no_response_object_leaves_outputs_untouched() {
    let engine =
        engine(|_input| BTreeMap::from([("processed".to_string(), FeelValue::Boolean(true))]));

    let DispatchOutcome::Completed { reply, outputs, .. } =
        drive(engine.dispatch(&inbound(b"{\"k\":\"v\"}", "application/json"))).expect("dispatch")
    else {
        panic!("expected Completed");
    };
    assert!(reply.is_none(), "no responseObject ⇒ no reply body");
    assert_eq!(
        outputs.get("processed"),
        Some(&serde_json::Value::Bool(true))
    );
}
