//! A `<q:source dedupKey>` a flow declares is read at dispatch, wherever it says to read: a header,
//! the CloudEvents id, or a path in the body. What the channel is configured to do decides nothing,
//! and a declared key is protected like any other: a check that fails refuses the delivery.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::Write;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::cloudevents::{self, CeMode, WrapDefaults};
use sutra_channels::{
    ChannelBinding, ChannelEngine, CodecRegistry, DedupCheckOutcome, DeploymentId, Diagnostic,
    DispatchOutcome, DrainingSink, InMemoryInboxStore, InboundChain, InboundMessage, InboxStore,
    Namespace, ValidatorRegistry,
};
use sutra_executor::{TaskRegistry, TokenExecutor};

use crate::support::drive;

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "keys", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-0000000000000000000000d1").expect("valid deployment id")
}

fn flow(dedup_key: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="keyed">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements>
        <q:source channel="in" name="payload" dedupKey="{dedup_key}"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
  </bpmn:process>
</bpmn:definitions>"#
    )
}

fn engine(dedup_key: &str, codec: &str, inbox: Rc<dyn InboxStore>) -> ChannelEngine {
    let module = BpmnModelLoader::new()
        .load(flow(dedup_key).as_bytes())
        .expect("BPMN loads");
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(TaskRegistry::new())
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
    .with_binding(ChannelBinding::new("in", namespace(), deployment(), codec))
    .with_module(&deployment(), &module)
    .with_inbox(inbox)
    .build()
}

fn message(headers: &[(&str, &str)], body: &[u8]) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: "in".to_string(),
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>(),
        body: body.to_vec().into(),
        content_type: Some("application/json".to_string()),
        // What a transport with no explicit signal hands over: a body hash, which never dedupes.
        idempotency_key: "sha-of-the-body".to_string(),
        explicit_event_id: false,
        received_at: "2026-11-04T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

/// How two identical deliveries come out.
fn twice(dedup_key: &str, first: &InboundMessage, second: &InboundMessage) -> (bool, bool) {
    let engine = engine(dedup_key, "json", Rc::new(InMemoryInboxStore::new()));
    let duplicate = |result: Result<DispatchOutcome, Diagnostic>| {
        matches!(
            result.expect("delivery runs"),
            DispatchOutcome::Duplicate { .. }
        )
    };
    (
        duplicate(drive(engine.dispatch(first))),
        duplicate(drive(engine.dispatch(second))),
    )
}

const CE_HEADERS: [(&str, &str); 4] = [
    ("ce-id", "evt-1"),
    ("ce-source", "urn:test"),
    ("ce-type", "t.happened"),
    ("ce-specversion", "1.0"),
];

#[test]
fn a_declared_header_dedupes_with_no_key_header_on_the_channel() {
    let msg = message(&[("X-Event", "e-1")], br#"{"n":1}"#);
    assert_eq!(twice("header.X-Event", &msg, &msg), (false, true));
}

#[test]
fn a_declared_header_is_found_in_any_case() {
    let msg = message(&[("x-event", "e-1")], br#"{"n":1}"#);
    assert_eq!(twice("header.X-Event", &msg, &msg), (false, true));
}

#[test]
fn another_value_of_a_declared_header_is_another_event() {
    let first = message(&[("X-Event", "e-1")], br#"{"n":1}"#);
    let second = message(&[("X-Event", "e-2")], br#"{"n":1}"#);
    assert_eq!(twice("header.X-Event", &first, &second), (false, false));
}

/// `cloudevents-mode: none` hands over the headers and the raw body and nothing else.
#[test]
fn a_declared_cloud_event_id_dedupes_from_the_headers_of_a_channel_that_does_not_read_them() {
    let msg = message(&CE_HEADERS, br#"{"n":1}"#);
    assert_eq!(twice("ce.id", &msg, &msg), (false, true));
}

#[test]
fn a_declared_cloud_event_id_dedupes_from_the_envelope_of_a_channel_that_does_not_read_it() {
    let msg = message(
        &[],
        br#"{"specversion":"1.0","id":"evt-1","source":"urn:test","type":"t","data":{"n":1}}"#,
    );
    assert_eq!(twice("ce.id", &msg, &msg), (false, true));
}

/// A channel whose transport parsed the event, with a key header that outranked its id.
#[test]
fn a_declared_cloud_event_id_is_read_though_a_key_header_supplied_the_key() {
    let mut first = message(&[], br#"{"n":1}"#);
    first.idempotency_key = "header-value-1".to_string();
    first.explicit_event_id = true;
    let parsed = cloudevents::extract(
        CeMode::Structured,
        "in",
        &BTreeMap::new(),
        Some("application/cloudevents+json"),
        br#"{"specversion":"1.0","id":"evt-1","source":"urn:test","type":"t","data":{"n":1}}"#,
        WrapDefaults {
            source: None,
            event_type: None,
        },
    )
    .unwrap();
    first.cloud_event = parsed.cloud_event.map(Box::new);
    let mut second = first.clone();
    second.idempotency_key = "header-value-2".to_string();
    assert_eq!(twice("ce.id", &first, &second), (false, true));
}

/// An id the engine made up for a wrapped message identifies nothing, so it is no key.
#[test]
fn an_id_the_engine_generated_is_not_a_declared_cloud_event_id() {
    let wrapped = |body: &[u8]| {
        let extraction = cloudevents::extract(
            CeMode::Wrap,
            "in",
            &BTreeMap::new(),
            Some("application/json"),
            body,
            WrapDefaults {
                source: None,
                event_type: None,
            },
        )
        .unwrap();
        let mut msg = message(&[], body);
        msg.cloud_event = extraction.cloud_event.map(Box::new);
        msg
    };
    // The same generated id on both, as a transport retrying one wrapped message would not have.
    let first = wrapped(br#"{"n":1}"#);
    assert_eq!(twice("ce.id", &first, &first), (false, false));
}

/// A key the transport already checked is not checked a second time as the declared one: the second
/// check would find the first's pending record.
#[test]
fn a_declared_key_equal_to_the_transports_is_checked_once() {
    let inbox = Rc::new(ScriptedInbox::default());
    let engine = engine("ce.id", "json", Rc::clone(&inbox) as Rc<dyn InboxStore>);
    let mut msg = message(&CE_HEADERS, br#"{"n":1}"#);
    msg.idempotency_key = "evt-1".to_string();
    msg.explicit_event_id = true;
    drive(engine.dispatch(&msg)).expect("delivery runs");
    assert_eq!(*inbox.checks.borrow(), ["evt-1"]);
}

/// A broker's own id for the message, which its transport hands over as a header of its own.
#[test]
fn a_declared_message_id_dedupes_on_the_id_the_transport_handed_over() {
    let msg = message(
        &[(sutra_channels::NATIVE_MESSAGE_ID_HEADER, "broker-id-1")],
        br#"{"n":1}"#,
    );
    assert_eq!(twice("amqp.message-id", &msg, &msg), (false, true));
}

#[test]
fn a_declared_message_id_whose_check_fails_is_refused() {
    let msg = message(
        &[(sutra_channels::NATIVE_MESSAGE_ID_HEADER, "broker-id-1")],
        br#"{"n":1}"#,
    );
    assert_eq!(
        refused("amqp.message-id", &msg, DedupCheckOutcome::Unavailable),
        "SUTRA.INBOUND.DEDUP_CHECK_UNAVAILABLE"
    );
}

#[test]
fn a_message_missing_a_declared_message_id_runs_as_one_with_no_key_and_warns() {
    absent_key_runs_and_warns("amqp.message-id");
}

/// A package an earlier engine accepted: it runs as if it declared nothing, delivery after delivery.
#[test]
fn an_unrecognised_form_runs_as_no_key_without_a_warning_per_delivery() {
    let log = CapturedLog::default();
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let msg = message(&[], br#"{"id":"same"}"#);
    let outcome = tracing::subscriber::with_default(subscriber, || twice("payload.id", &msg, &msg));
    assert_eq!(outcome, (false, false));
    assert_eq!(
        String::from_utf8(log.0.lock().unwrap().clone()).unwrap(),
        ""
    );
}

// ---- absent from the message --------------------------------------------------------------------

#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Two deliveries with nothing to dedupe on both run, as a delivery with no key does, and the log
/// says which channel and which declaration, at warn level.
fn absent_key_runs_and_warns(dedup_key: &str) {
    let log = CapturedLog::default();
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let msg = message(&[], br#"{"n":1}"#);
    let outcome = tracing::subscriber::with_default(subscriber, || twice(dedup_key, &msg, &msg));
    assert_eq!(outcome, (false, false), "both run");
    let logged = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    assert!(logged.contains("WARN"), "{dedup_key}: {logged}");
    assert!(logged.contains("in"), "{dedup_key}: {logged}");
    assert!(logged.contains(dedup_key), "{dedup_key}: {logged}");
}

#[test]
fn a_message_missing_a_declared_header_runs_as_one_with_no_key_and_warns() {
    absent_key_runs_and_warns("header.X-Event");
}

#[test]
fn a_message_missing_a_declared_cloud_event_id_runs_as_one_with_no_key_and_warns() {
    absent_key_runs_and_warns("ce.id");
}

#[test]
fn a_message_missing_a_declared_body_path_runs_as_one_with_no_key_and_warns() {
    absent_key_runs_and_warns("body.id");
}

// ---- a check that fails --------------------------------------------------------------------------

#[derive(Default)]
struct ScriptedInbox {
    answer: RefCell<Option<DedupCheckOutcome>>,
    checks: RefCell<Vec<String>>,
}

#[async_trait::async_trait(?Send)]
impl InboxStore for ScriptedInbox {
    async fn record_seen(
        &self,
        _deployment: &DeploymentId,
        _channel: &str,
        event_id: &str,
    ) -> DedupCheckOutcome {
        self.checks.borrow_mut().push(event_id.to_string());
        self.answer.borrow().unwrap_or(DedupCheckOutcome::First)
    }
}

fn refused(dedup_key: &str, msg: &InboundMessage, answer: DedupCheckOutcome) -> String {
    let inbox = Rc::new(ScriptedInbox::default());
    *inbox.answer.borrow_mut() = Some(answer);
    let engine = engine(dedup_key, "json", Rc::clone(&inbox) as Rc<dyn InboxStore>);
    let refusal = drive(engine.dispatch(msg)).expect_err("refused, not processed unchecked");
    refusal.code
}

#[test]
fn a_declared_header_whose_check_fails_is_refused() {
    let msg = message(&[("X-Event", "e-1")], br#"{"n":1}"#);
    assert_eq!(
        refused("header.X-Event", &msg, DedupCheckOutcome::Unavailable),
        "SUTRA.INBOUND.DEDUP_CHECK_UNAVAILABLE"
    );
    assert_eq!(
        refused("header.X-Event", &msg, DedupCheckOutcome::InDoubt),
        "SUTRA.INBOUND.DEDUP_IN_DOUBT"
    );
}

#[test]
fn a_declared_cloud_event_id_whose_check_fails_is_refused() {
    let msg = message(&CE_HEADERS, br#"{"n":1}"#);
    assert_eq!(
        refused("ce.id", &msg, DedupCheckOutcome::Unavailable),
        "SUTRA.INBOUND.DEDUP_CHECK_UNAVAILABLE"
    );
    assert_eq!(
        refused("ce.id", &msg, DedupCheckOutcome::InDoubt),
        "SUTRA.INBOUND.DEDUP_IN_DOUBT"
    );
}

#[test]
fn a_declared_body_path_whose_check_fails_is_refused() {
    let msg = message(&[], br#"{"id":"k-1"}"#);
    assert_eq!(
        refused("body.id", &msg, DedupCheckOutcome::Unavailable),
        "SUTRA.INBOUND.DEDUP_CHECK_UNAVAILABLE"
    );
}
