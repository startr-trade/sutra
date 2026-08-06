//! `<q:reply>` → emission and `<q:send channel>` resolution: destination resolution, CloudEvents
//! binary mode, auth-resolver binding and author-declared headers. At the executor level emissions
//! land in an [`sutra_executor::EmissionSink`] (collected, not delivered — transport sits above).

use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::qbindings::ReplyMode;
use sutra_executor::{
    CollectingSink, DeploymentId, EmissionSink, OutboundChannelRegistry, ResolvedOutboundChannel,
    ResolvedSecret, TaskRegistry, TokenExecutor,
};

// ---- ReplyEmissionTest -----------------------------------------------------------------

#[tokio::test]
async fn native_reply_happy_path_collects_emission() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Echo" implementation="${echo}">
              <bpmn:extensionElements>
                <q:reply mode="native"
                         destination="https://example.com/cb"
                         contentType="text/plain"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Echo"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Echo" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry =
        TaskRegistry::new().register("echo", |_, _| ok_map(&[("payload.body", string("hello"))]));
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(registry)
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();
    executor.execute_sync(&process, vars(&[])).await.unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    let e = &emissions[0];
    assert_eq!(e.mode, ReplyMode::Native);
    assert_eq!(e.body_utf8(), "hello");
    assert_eq!(e.destination, "https://example.com/cb");
    assert_eq!(e.content_type.as_deref(), Some("text/plain"));
    // NATIVE → no CloudEvent envelope.
    assert!(e.cloud_event.is_none());
}

#[tokio::test]
async fn cloud_event_binary_mode_populates_cloud_event_on_reply() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="cloudevent-binary"
                         destination="https://example.com/cb"
                         type="com.example.Done"
                         source="/sutra/test"
                         subject="abc-42"
                         datacontenttype="application/json"/>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();
    executor
        .execute_sync(&process, vars(&[("payload.body", string("{\"ok\":true}"))]))
        .await
        .unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    let reply = &emissions[0];
    assert_eq!(reply.mode, ReplyMode::CloudeventBinary);
    let ce = reply.cloud_event.as_ref().expect("cloud event present");
    assert_eq!(ce.ce_type, "com.example.Done");
    assert_eq!(ce.source, "/sutra/test");
    assert_eq!(ce.subject.as_deref(), Some("abc-42"));
    assert_eq!(ce.data_content_type.as_deref(), Some("application/json"));
}

#[tokio::test]
async fn inbound_reply_to_override_resolves_destination_when_binding_has_none() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native"/>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();
    executor
        .execute_sync(
            &process,
            vars(&[("replyTo", string("https://override.example/cb"))]),
        )
        .await
        .unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(emissions[0].destination, "https://override.example/cb");
}

#[tokio::test]
async fn required_reply_with_no_destination_raises_dest_required_not_set() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native" required="true"/>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::new(CollectingSink::new()) as Rc<dyn EmissionSink>)
        .build();

    let e = executor
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "SUTRA.OUTBOUND.REPLY_DEST_REQUIRED_NOT_SET");
}

#[tokio::test]
async fn bearer_auth_with_matching_resolver_populates_auth_ref_on_outbound() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native"
                         destination="https://example.com/cb"
                         auth="bearer"
                         authSecretRef="env:CALLBACK_TOKEN"/>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_auth_ref_resolver(|auth_ref| {
            Some(ResolvedSecret {
                scheme: auth_ref.scheme.clone(),
                secret: b"t0k3n".to_vec(),
                header: auth_ref.header.clone(),
            })
        })
        .build();
    executor.execute_sync(&process, vars(&[])).await.unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    let auth_ref = emissions[0].auth_ref.as_ref().expect("auth ref present");
    assert_eq!(auth_ref.scheme, "bearer");
    assert_eq!(auth_ref.secret_ref, "env:CALLBACK_TOKEN");
}

#[tokio::test]
async fn auth_scheme_with_no_registered_resolver_raises_auth_resolver_not_found() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native"
                         destination="https://example.com/cb"
                         auth="apikey"
                         authSecretRef="custom:something"
                         authHeader="X-API-Key"/>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    // Registry returns None — no resolver claims the "custom" URI scheme.
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::new(CollectingSink::new()) as Rc<dyn EmissionSink>)
        .with_auth_ref_resolver(|_| None)
        .build();

    let e = executor
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "SUTRA.OUTBOUND.REPLY_AUTH_RESOLVER_NOT_FOUND");
}

#[tokio::test]
async fn reply_on_end_event_emits_before_instance_completes() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native" destination="https://example.com/cb"/>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();
    let result = executor
        .execute_sync(&process, vars(&[("payload.body", string("ok"))]))
        .await
        .unwrap();

    assert!(result.visited_nodes.contains("E"));
    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(emissions[0].body_utf8(), "ok");
    assert_eq!(emissions[0].destination, "https://example.com/cb");
}

// ---- SendChannelTest ----------------------------------------------------------------------

fn message_throw(send_element: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateThrowEvent id="Notify">
              <bpmn:extensionElements>
                {send_element}
              </bpmn:extensionElements>
              <bpmn:messageEventDefinition/>
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Notify"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn outbound_registry(channel: ResolvedOutboundChannel) -> OutboundChannelRegistry {
    let mut registry = OutboundChannelRegistry::new();
    registry.register(&DeploymentId::unresolved(), channel);
    registry
}

#[tokio::test]
async fn send_via_outbound_channel_resolves_destination_and_enqueues() {
    let process = proc(
        &message_throw(r#"<q:send channel="responses-out" messageType="invoice.settled"/>"#),
        "p1",
    );
    let registry = outbound_registry(ResolvedOutboundChannel::resolve(
        "responses-out",
        "http",
        "https://callbacks.example/order-status",
        None,
        None,
        None,
        "none",
    ));
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_outbound_channels(registry)
        .build();
    let result = executor
        .execute_sync(
            &process,
            vars(&[("payload.body", string("<invoice-settled/>"))]),
        )
        .await
        .unwrap();

    assert!(result.visited_nodes.contains("Notify"));
    assert!(result.visited_nodes.contains("E"));
    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    let reply = &emissions[0];
    assert_eq!(reply.destination, "https://callbacks.example/order-status");
    assert_eq!(reply.mode, ReplyMode::Native);
    assert!(reply.auth_ref.is_none());
    assert_eq!(reply.body_utf8(), "<invoice-settled/>");
}

#[tokio::test]
async fn send_via_outbound_channel_carries_channel_auth_and_mode() {
    let process = proc(&message_throw(r#"<q:send channel="responses-out"/>"#), "p1");
    let registry = outbound_registry(ResolvedOutboundChannel::resolve(
        "responses-out",
        "http",
        "https://callbacks.example/out",
        Some("bearer"),
        Some("env:CALLBACK_TOKEN"),
        None,
        "structured",
    ));
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_outbound_channels(registry)
        .build();
    executor
        .execute_sync(&process, vars(&[("payload.body", string("{}"))]))
        .await
        .unwrap();

    let emissions = sink.emissions();
    let reply = &emissions[0];
    assert_eq!(reply.mode, ReplyMode::CloudeventStructured);
    let auth_ref = reply.auth_ref.as_ref().expect("auth ref present");
    assert_eq!(auth_ref.scheme, "bearer");
    assert_eq!(auth_ref.secret_ref, "env:CALLBACK_TOKEN");
}

#[tokio::test]
async fn send_to_unknown_outbound_channel_fails_closed_at_emit() {
    let process = proc(&message_throw(r#"<q:send channel="nope"/>"#), "p1");

    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::new(CollectingSink::new()) as Rc<dyn EmissionSink>)
        .with_outbound_channels(OutboundChannelRegistry::new()) // empty — "nope" unknown
        .build();

    let e = executor
        .execute_sync(&process, vars(&[("payload.body", string("x"))]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), "SUTRA.CONFIG.CHANNEL.OUTBOUND_UNKNOWN");
}

#[test]
fn send_with_neither_channel_nor_destination_fails_at_load() {
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(message_throw(r#"<q:send contentType="text/plain"/>"#).as_bytes())
        .unwrap_err();
    assert_eq!(
        e.code,
        sutra_bpmn::codes::PARSE_Q_SEND_CHANNEL_OR_DESTINATION
    );
}

#[test]
fn send_with_both_channel_and_destination_fails_at_load() {
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(message_throw(r#"<q:send channel="c" destination="https://x/y"/>"#).as_bytes())
        .unwrap_err();
    assert_eq!(
        e.code,
        sutra_bpmn::codes::PARSE_Q_SEND_CHANNEL_OR_DESTINATION
    );
}

#[tokio::test]
async fn service_task_sends_its_produced_reply_to_the_outbound_channel() {
    // The send emits the PRODUCED reply (responseBody), NOT the inbound payload.body.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Render" implementation="${render}">
              <bpmn:extensionElements>
                <q:send channel="responses-out" messageType="invoice.settled"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Render"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Render" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");

    let registry = outbound_registry(ResolvedOutboundChannel::resolve(
        "responses-out",
        "http",
        "https://callbacks.example/out",
        None,
        None,
        None,
        "none",
    ));
    let tasks = TaskRegistry::new().register("render", |_, _| {
        ok_map(&[("responseBody", string("<invoice-settled/>"))])
    });
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_outbound_channels(registry)
        .build();
    executor
        .execute_sync(
            &process,
            vars(&[("payload.body", string("<order-created-inbound/>"))]),
        )
        .await
        .unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(
        emissions[0].body_utf8(),
        "<invoice-settled/>",
        "the send emits the produced reply, not the inbound payload"
    );
    assert_eq!(emissions[0].destination, "https://callbacks.example/out");
}

#[tokio::test]
async fn send_with_explicit_destination_still_works() {
    let process = proc(
        &message_throw(r#"<q:send destination="https://ops.example/notify"/>"#),
        "p1",
    );
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .build();
    executor
        .execute_sync(&process, vars(&[("payload.body", string("flagged"))]))
        .await
        .unwrap();
    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(emissions[0].destination, "https://ops.example/notify");
}

// ---- author-declared <q:header> on outbound send/reply ---------------------------------

#[tokio::test]
async fn send_carries_author_declared_headers_feel_resolved() {
    // A variable-ref header and a numeric-literal header resolve; a null-valued header is omitted.
    let send = r#"<q:send destination="https://ops.example/notify">
                    <q:header name="txnId" value="txnId"/>
                    <q:header name="amount" value="42"/>
                    <q:header name="skip" value="null"/>
                  </q:send>"#;
    let process = proc(&message_throw(send), "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_feel()
        .build();
    executor
        .execute_sync(
            &process,
            vars(&[("payload.body", string("x")), ("txnId", string("TX-42"))]),
        )
        .await
        .unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    let headers = &emissions[0].headers;
    // FEEL over the process context — the variable resolves, the literal resolves canonically.
    assert_eq!(headers.get("txnId").map(String::as_str), Some("TX-42"));
    assert_eq!(headers.get("amount").map(String::as_str), Some("42"));
    // A header whose value resolves null is omitted (not set to "").
    assert!(
        !headers.contains_key("skip"),
        "null-valued header is omitted"
    );
}

#[tokio::test]
async fn reply_carries_author_declared_headers_feel_resolved() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native" destination="https://example.com/cb">
                  <q:header name="corrId" value="corrId"/>
                </q:reply>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    let sink = Rc::new(CollectingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn EmissionSink>)
        .with_feel()
        .build();
    executor
        .execute_sync(
            &process,
            vars(&[("payload.body", string("{}")), ("corrId", string("CID-9"))]),
        )
        .await
        .unwrap();

    let emissions = sink.emissions();
    assert_eq!(emissions.len(), 1);
    assert_eq!(
        emissions[0].headers.get("corrId").map(String::as_str),
        Some("CID-9"),
        "a reply leg carries author headers too (it is itself a coverage hop)"
    );
}
