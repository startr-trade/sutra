//! The inbound chain's dispatch-level semantics — codec invoke, validator chain,
//! `<q:onValidation>` modes, message-type start-event routing, per-tenant rule
//! resolution — plus the intake-pipeline additions this crate owns: the FROZEN `validation.*`
//! summary variables, the validator-crash → `RUNTIME.VALIDATOR.UNCAUGHT` conversion, the
//! DMN payload projection, alias materialisation + `onConflict` semantics, inbox
//! dedup, broadcast fan-out, tenant agreement, payload caps, and the wait-state
//! fail-fast.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{
    AliasStore, ChannelBinding, ChannelEngine, CodecRegistry, ContentValidator, Diagnostic,
    DispatchOutcome, DmnContentValidator, DrainingSink, InMemoryAliasStore, InMemoryInboxStore,
    InMemoryIncidentSink, InboundChain, InboundMessage, InboxStore, IncidentSink, Namespace,
    SrlContentValidator, ValidatorRegistry, ValidatorTier, ACK_DISPOSITION_ATTR,
};
use sutra_codec_spi::{CodecValue, DecodeResult, IssueSeverity, PayloadCodec, ValidationIssue};
use sutra_executor::{
    archive_key, logical_urn, DeploymentId, TaskError, TaskRegistry, TokenExecutor, Variables,
};
use sutra_feel::FeelValue;

use crate::support::drive;

const TENANT: &str = "acme";

fn namespace() -> Namespace {
    Namespace::new(TENANT, "orders-canary", "1.0.0")
}

fn deployment() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000021").expect("valid deployment id")
}

fn inbound(channel: &str, body: &[u8], content_type: &str) -> InboundMessage {
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: namespace().module_key(),
        channel: channel.to_string(),
        headers: BTreeMap::new(),
        body: body.to_vec().into(),
        content_type: Some(content_type.to_string()),
        idempotency_key: "msg-1".to_string(),
        explicit_event_id: false,
        received_at: "2026-05-23T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

fn bpmn_with_source(validators: &[&str]) -> String {
    let mut chain = String::new();
    if !validators.is_empty() {
        chain.push_str("<q:validators>");
        for v in validators {
            chain.push_str(&format!("<q:complexValidator source=\"{v}\"/>"));
        }
        chain.push_str("</q:validators>");
    }
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="payload">{chain}</q:source>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="capture"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn bpmn_with_on_validation(mode: &str, error_code: Option<&str>, validators: &[&str]) -> String {
    let mut chain = String::new();
    if !validators.is_empty() {
        chain.push_str("<q:validators>");
        for v in validators {
            chain.push_str(&format!("<q:complexValidator source=\"{v}\"/>"));
        }
        chain.push_str("</q:validators>");
    }
    let error_attr = error_code
        .map(|c| format!(" errorCode=\"{c}\""))
        .unwrap_or_default();
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="payload">{chain}</q:source>
                <q:onValidation mode="{mode}"{error_attr}/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="capture"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

/// Captured task-invocation observation (an atomic-reference pair).
#[derive(Default)]
struct Captured {
    input: Option<FeelValue>,
    vars: Option<BTreeMap<String, FeelValue>>,
}

struct Harness {
    engine: ChannelEngine,
    captured: Rc<RefCell<Captured>>,
    aliases: Rc<InMemoryAliasStore>,
    inbox: Rc<InMemoryInboxStore>,
}

/// Always-pass validator.
struct NoopValidator(String);

impl ContentValidator for NoopValidator {
    fn name(&self) -> &str {
        &self.0
    }
    fn validate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<ValidationIssue>, String> {
        Ok(Vec::new())
    }
}

/// Validator returning a single ERROR issue (optionally with a reason value).
struct FailingValidator {
    name: String,
    value: Option<String>,
    tier: ValidatorTier,
}

impl FailingValidator {
    fn named(name: &str) -> FailingValidator {
        FailingValidator {
            name: name.to_string(),
            value: None,
            tier: ValidatorTier::Content,
        }
    }
}

impl ContentValidator for FailingValidator {
    fn name(&self) -> &str {
        &self.name
    }
    fn tier(&self) -> ValidatorTier {
        self.tier
    }
    fn validate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<ValidationIssue>, String> {
        Ok(vec![ValidationIssue {
            code: "SUTRA.RUNTIME.VALIDATOR.UNCAUGHT".to_string(),
            severity: IssueSeverity::Error,
            path: "/test".to_string(),
            message: format!("synthetic failure from {}", self.name),
            value: self.value.clone(),
        }])
    }
}

/// Stub codec decoding the body as UTF-8 AND reporting it as the message type.
struct MessageTypeEchoCodec;

impl PayloadCodec for MessageTypeEchoCodec {
    fn name(&self) -> &str {
        "mt-echo"
    }
    fn accepted_content_types(&self) -> Vec<String> {
        vec!["text/plain".to_string()]
    }
    fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
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

fn build_harness(
    bpmn: &str,
    codec: &str,
    register: impl FnOnce(&mut CodecRegistry, &mut ValidatorRegistry),
) -> Harness {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");

    let captured: Rc<RefCell<Captured>> = Rc::default();
    let captured_in_task = Rc::clone(&captured);
    let tasks = TaskRegistry::new().register("capture", move |input, ctx| {
        let mut c = captured_in_task.borrow_mut();
        c.input = Some(input.clone());
        c.vars = Some(ctx.variables().to_feel_context());
        Ok(FeelValue::Map(BTreeMap::new()))
    });

    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();

    let mut codecs = CodecRegistry::with_builtins();
    let mut validators = ValidatorRegistry::new();
    register(&mut codecs, &mut validators);

    let aliases = Rc::new(InMemoryAliasStore::new());
    let inbox = Rc::new(InMemoryInboxStore::new());
    let engine = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            codecs,
            sutra_channels::FormatRegistry::with_builtins(),
            validators,
        ),
    )
    .with_binding(ChannelBinding::new(
        "orders-in",
        namespace(),
        deployment(),
        codec,
    ))
    .with_module(&deployment(), &module)
    .with_alias_store(Rc::clone(&aliases) as Rc<dyn sutra_channels::AliasStore>)
    .with_inbox(Rc::clone(&inbox) as Rc<dyn sutra_channels::InboxStore>)
    .build();
    Harness {
        engine,
        captured,
        aliases,
        inbox,
    }
}

type ValidatorSetup = Box<dyn FnOnce(&mut ValidatorRegistry)>;

fn harness(bpmn: &str, validators: Vec<ValidatorSetup>) -> Harness {
    build_harness(bpmn, "raw-text", move |_codecs, registry| {
        for v in validators {
            v(registry);
        }
    })
}

fn captured_var(h: &Harness, name: &str) -> Option<FeelValue> {
    h.captured
        .borrow()
        .vars
        .as_ref()
        .and_then(|v| v.get(name).cloned())
}

fn expect_err(result: Result<DispatchOutcome, Diagnostic>) -> Diagnostic {
    match result {
        Err(d) => d,
        Ok(_) => panic!("expected a dispatch failure"),
    }
}

// ===== Per-tenant rule resolution (module rule → global SPI validator → fail-closed) =====

#[test]
fn rule_not_deployed_to_tenant_fails_closed() {
    let h = harness(&bpmn_with_source(&["order-rules.dmn"]), vec![]);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"hi",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.VALIDATE.VALIDATOR_NOT_FOUND");
}

#[test]
fn rule_deployed_under_module_key_resolves_and_runs() {
    let key = archive_key(&logical_urn("rule", "order-rules.dmn"), &deployment());
    let h = harness(
        &bpmn_with_source(&["order-rules.dmn"]),
        vec![Box::new(move |r| {
            r.register_under(&key, NoopValidator("order-rules.dmn".to_string()))
        })],
    );
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"hi", "text/plain")),
    )
    .expect("dispatch succeeds");
    assert_eq!(
        captured_var(&h, "payload"),
        Some(FeelValue::String("hi".to_string()))
    );
}

#[test]
fn builtin_spi_validator_resolves_globally_for_every_tenant() {
    let h = harness(
        &bpmn_with_source(&["schema-check"]),
        vec![Box::new(|r| {
            r.register(NoopValidator("schema-check".to_string()))
        })],
    );
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"hi", "text/plain")),
    )
    .expect("dispatch succeeds");
    assert_eq!(
        captured_var(&h, "payload"),
        Some(FeelValue::String("hi".to_string()))
    );
}

#[test]
fn unknown_validator_not_in_library_fails_with_not_found() {
    let h = harness(&bpmn_with_source(&["nope.dmn"]), vec![]);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"hi",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.VALIDATE.VALIDATOR_NOT_FOUND");
}

// ===== codec invoke ============

#[test]
fn codec_raw_text_happy_path_decodes_to_string_and_exposes_payload_variable() {
    let h = harness(&bpmn_with_source(&[]), vec![]);
    drive(h.engine.dispatch(&inbound(
        "orders-in",
        "hello world".as_bytes(),
        "text/plain",
    )))
    .expect("dispatch succeeds");
    assert_eq!(
        captured_var(&h, "payload"),
        Some(FeelValue::String("hello world".to_string()))
    );
    // The typed payload is fed to the service task as the input argument.
    assert_eq!(
        h.captured.borrow().input,
        Some(FeelValue::String("hello world".to_string()))
    );
}

#[test]
fn codec_unknown_raises_inbound_codec_not_found() {
    let h = build_harness(&bpmn_with_source(&[]), "does-not-exist", |_, _| {});
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"x",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.CODEC_NOT_FOUND");
}

#[test]
fn capability_mismatch_rejects_before_decode() {
    // json accepts only application/json + application/*+json — audio/opus is the
    // auditable call-drop case.
    let h = build_harness(&bpmn_with_source(&[]), "json", |_, _| {});
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"{}",
        "audio/opus",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.CAPABILITY_MISMATCH");
}

// ===== Message-type routing =====

const ROUTER_BPMN: &str = r#"<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0">
  <bpmn:process id="router">
    <bpmn:startEvent id="S_ORDER">
      <bpmn:extensionElements>
        <q:source channel="orders-in" name="payload" messageTypeValue="order.created"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:startEvent id="S_INVOICE">
      <bpmn:extensionElements>
        <q:source channel="orders-in" name="payload" messageTypeValue="invoice.settled"/>
      </bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="T_ORDER" implementation="order"/>
    <bpmn:serviceTask id="T_INVOICE" implementation="invoice"/>
    <bpmn:endEvent id="E_ORDER"/>
    <bpmn:endEvent id="E_INVOICE"/>
    <bpmn:sequenceFlow id="f1" sourceRef="S_ORDER" targetRef="T_ORDER"/>
    <bpmn:sequenceFlow id="f2" sourceRef="T_ORDER" targetRef="E_ORDER"/>
    <bpmn:sequenceFlow id="f3" sourceRef="S_INVOICE" targetRef="T_INVOICE"/>
    <bpmn:sequenceFlow id="f4" sourceRef="T_INVOICE" targetRef="E_INVOICE"/>
  </bpmn:process>
</bpmn:definitions>"#;

struct RouterHarness {
    engine: ChannelEngine,
    ran_order: Rc<RefCell<Option<FeelValue>>>,
    ran_invoice: Rc<RefCell<Option<FeelValue>>>,
}

fn router_harness() -> RouterHarness {
    let ns = Namespace::new(TENANT, "router-mod", "1.0.0");
    let dep = DeploymentId::of("dep-000000000000000000000022").expect("valid deployment id");
    let module = BpmnModelLoader::new()
        .load(ROUTER_BPMN.as_bytes())
        .expect("BPMN loads");

    let ran_order: Rc<RefCell<Option<FeelValue>>> = Rc::default();
    let ran_invoice: Rc<RefCell<Option<FeelValue>>> = Rc::default();
    let (p, c) = (Rc::clone(&ran_order), Rc::clone(&ran_invoice));
    let tasks = TaskRegistry::new()
        .register("order", move |input, _| {
            *p.borrow_mut() = Some(input.clone());
            Ok(FeelValue::Map(BTreeMap::new()))
        })
        .register("invoice", move |input, _| {
            *c.borrow_mut() = Some(input.clone());
            Ok(FeelValue::Map(BTreeMap::new()))
        });

    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let mut codecs = CodecRegistry::with_builtins();
    codecs.register(MessageTypeEchoCodec);
    let engine = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            codecs,
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(ChannelBinding::new("orders-in", ns, dep.clone(), "mt-echo"))
    .with_module(&dep, &module)
    .build();
    RouterHarness {
        engine,
        ran_order,
        ran_invoice,
    }
}

fn router_inbound(body: &str) -> InboundMessage {
    let ns = Namespace::new(TENANT, "router-mod", "1.0.0");
    InboundMessage {
        tenant: TENANT.to_string(),
        module_key: ns.module_key(),
        channel: "orders-in".to_string(),
        headers: BTreeMap::new(),
        body: body.as_bytes().to_vec().into(),
        content_type: Some("text/plain".to_string()),
        idempotency_key: "msg-1".to_string(),
        explicit_event_id: false,
        received_at: "2026-05-23T10:00:00Z".to_string(),
        cloud_event: None,
    }
}

#[test]
fn message_type_selects_start_event_and_runs_only_its_branch() {
    let h = router_harness();
    drive(h.engine.dispatch(&router_inbound("order.created"))).expect("dispatch succeeds");
    assert_eq!(
        *h.ran_order.borrow(),
        Some(FeelValue::String("order.created".to_string()))
    );
    assert_eq!(*h.ran_invoice.borrow(), None);
}

#[test]
fn different_message_type_routes_to_the_other_start_event() {
    let h = router_harness();
    drive(h.engine.dispatch(&router_inbound("invoice.settled"))).expect("dispatch succeeds");
    assert_eq!(
        *h.ran_invoice.borrow(),
        Some(FeelValue::String("invoice.settled".to_string()))
    );
    assert_eq!(*h.ran_order.borrow(), None);
}

#[test]
fn unhandled_message_type_fails_closed_with_no_start_event_diagnostic() {
    let h = router_harness();
    let d = expect_err(drive(
        h.engine.dispatch(&router_inbound("shipment.dispatched")),
    ));
    assert_eq!(d.code, "SUTRA.INBOUND.NO_START_EVENT_FOR_MESSAGE_TYPE");
}

#[test]
fn input_name_override_projects_under_custom_variable_name() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="body"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="capture"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let h = harness(bpmn, vec![]);
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"abc", "text/plain")),
    )
    .expect("dispatch succeeds");
    assert_eq!(
        captured_var(&h, "body"),
        Some(FeelValue::String("abc".to_string()))
    );
    assert_eq!(captured_var(&h, "payload"), None);
}

// ===== validator chain ====================

fn validation_issues(h: &Harness) -> Vec<FeelValue> {
    match captured_var(h, "payload.validation") {
        Some(FeelValue::List(issues)) => issues,
        other => panic!("expected payload.validation list, got {other:?}"),
    }
}

#[test]
fn validator_chain_happy_path_payload_validation_is_empty() {
    let h = harness(
        &bpmn_with_source(&["noop"]),
        vec![Box::new(|r| r.register(NoopValidator("noop".to_string())))],
    );
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"ok", "text/plain")),
    )
    .expect("dispatch succeeds");
    assert!(validation_issues(&h).is_empty());
}

#[test]
fn validator_chain_accumulates_issues_across_multiple_validators() {
    let h = harness(
        &bpmn_with_source(&["noop", "boom"]),
        vec![
            Box::new(|r| r.register(NoopValidator("noop".to_string()))),
            Box::new(|r| r.register(FailingValidator::named("boom"))),
        ],
    );
    // No <q:onValidation>: errors are visible in payload.validation but execution proceeds.
    drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain"))).expect("dispatch succeeds");
    let issues = validation_issues(&h);
    assert_eq!(issues.len(), 1);
    let FeelValue::Map(issue) = &issues[0] else {
        panic!("issue is a map");
    };
    assert_eq!(
        issue.get("severity"),
        Some(&FeelValue::String("ERROR".to_string()))
    );
    let Some(FeelValue::String(message)) = issue.get("message") else {
        panic!("message present");
    };
    assert!(message.contains("synthetic failure from boom"));
}

// ===== q:simpleValidator — field content validator at a FEEL path =====

#[test]
fn simple_validator_runs_on_the_value_resolved_at_its_path() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="payload">
                  <q:validators>
                    <q:simpleValidator ref="currency-check" path="payload"/>
                  </q:validators>
                </q:source>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="capture"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;

    struct CurrencyCheck {
        seen: std::sync::Arc<std::sync::Mutex<Option<FeelValue>>>,
    }
    impl ContentValidator for CurrencyCheck {
        fn name(&self) -> &str {
            "currency-check"
        }
        fn validate(
            &self,
            value: &FeelValue,
            _v: &Variables,
        ) -> Result<Vec<ValidationIssue>, String> {
            *self.seen.lock().unwrap() = Some(value.clone());
            if value != &FeelValue::String("USD".to_string()) {
                return Ok(vec![ValidationIssue::error(
                    "SUTRA.RUNTIME.VALIDATOR.UNCAUGHT",
                    "/payload",
                    format!(
                        "bad currency: {}",
                        sutra_feel::value::canonical_string_of(value)
                    ),
                )]);
            }
            Ok(Vec::new())
        }
    }

    // Invalid value → the validator (fed the path-resolved value) emits an issue.
    let seen: std::sync::Arc<std::sync::Mutex<Option<FeelValue>>> = Default::default();
    let seen_for_validator = std::sync::Arc::clone(&seen);
    let bad = harness(
        bpmn,
        vec![Box::new(move |r| {
            r.register(CurrencyCheck {
                seen: seen_for_validator,
            })
        })],
    );
    drive(
        bad.engine
            .dispatch(&inbound("orders-in", b"XXX", "text/plain")),
    )
    .expect("dispatch succeeds");
    assert_eq!(
        *seen.lock().unwrap(),
        Some(FeelValue::String("XXX".to_string()))
    );
    let issues = validation_issues(&bad);
    assert_eq!(issues.len(), 1);

    // Valid value → no issues.
    let seen: std::sync::Arc<std::sync::Mutex<Option<FeelValue>>> = Default::default();
    let seen_for_validator = std::sync::Arc::clone(&seen);
    let ok = harness(
        bpmn,
        vec![Box::new(move |r| {
            r.register(CurrencyCheck {
                seen: seen_for_validator,
            })
        })],
    );
    drive(
        ok.engine
            .dispatch(&inbound("orders-in", b"USD", "text/plain")),
    )
    .expect("dispatch succeeds");
    assert!(validation_issues(&ok).is_empty());
}

// ===== <q:onValidation> policy modes =====

#[test]
fn on_validation_route_continues_execution_with_issues_visible() {
    let h = harness(
        &bpmn_with_on_validation("route", None, &["boom"]),
        vec![Box::new(|r| r.register(FailingValidator::named("boom")))],
    );
    drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain")))
        .expect("route mode continues");
    assert_eq!(validation_issues(&h).len(), 1);
}

#[test]
fn on_validation_reject_raises_inbound_validation_reject() {
    let h = harness(
        &bpmn_with_on_validation("reject", None, &["boom"]),
        vec![Box::new(|r| r.register(FailingValidator::named("boom")))],
    );
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"x",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.VALIDATION_REJECT");
    // Service task never executed (instance not started).
    assert!(h.captured.borrow().input.is_none());
}

#[test]
fn on_validation_error_raises_with_configured_error_code() {
    let h = harness(
        &bpmn_with_on_validation("error", Some("PAYMENT_REJECTED"), &["boom"]),
        vec![Box::new(|r| r.register(FailingValidator::named("boom")))],
    );
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"x",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.VALIDATION_ERROR");
    assert_eq!(
        d.attributes.get("errorCode"),
        Some(&"PAYMENT_REJECTED".to_string())
    );
}

#[test]
fn on_validation_reject_skipped_when_all_validators_pass() {
    let h = harness(
        &bpmn_with_on_validation("reject", None, &["noop"]),
        vec![Box::new(|r| r.register(NoopValidator("noop".to_string())))],
    );
    drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain")))
        .expect("no errors → reject policy does not fire");
    assert!(h.captured.borrow().input.is_some());
}

// ===== the FROZEN validation.* summary ================

fn summary(h: &Harness) -> BTreeMap<String, FeelValue> {
    match captured_var(h, "validation") {
        Some(FeelValue::Map(m)) => m,
        other => panic!("expected validation summary map, got {other:?}"),
    }
}

#[test]
fn validation_summary_ok_when_no_issues() {
    let h = harness(&bpmn_with_source(&[]), vec![]);
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"clean", "text/plain")),
    )
    .expect("dispatch succeeds");
    let s = summary(&h);
    assert_eq!(s.get("outcome"), Some(&FeelValue::String("OK".into())));
    assert_eq!(s.get("tier"), Some(&FeelValue::String("n/a".into())));
    assert_eq!(
        s.get("firstReasonCode"),
        Some(&FeelValue::String(String::new()))
    );
    assert_eq!(s.get("firstIssue"), Some(&FeelValue::String(String::new())));
    assert_eq!(s.get("issues"), Some(&FeelValue::List(Vec::new())));
}

#[test]
fn validation_summary_fatal_with_first_reason_code_from_the_value_slot() {
    // The first ERROR issue's `value` slot is the vendor reason code (e.g. E990).
    let h = harness(
        &bpmn_with_on_validation("route", None, &["partner-gateway"]),
        vec![Box::new(|r| {
            r.register(FailingValidator {
                name: "partner-gateway".to_string(),
                value: Some("E990".to_string()),
                tier: ValidatorTier::Content,
            })
        })],
    );
    drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain")))
        .expect("route mode continues");
    let s = summary(&h);
    assert_eq!(s.get("outcome"), Some(&FeelValue::String("FATAL".into())));
    assert_eq!(s.get("tier"), Some(&FeelValue::String("content".into())));
    assert_eq!(
        s.get("firstReasonCode"),
        Some(&FeelValue::String("E990".into()))
    );
    let Some(FeelValue::String(first_issue)) = s.get("firstIssue") else {
        panic!("firstIssue present");
    };
    assert!(first_issue.contains("synthetic failure from partner-gateway"));
    assert!(matches!(s.get("issues"), Some(FeelValue::List(l)) if l.len() == 1));
}

#[test]
fn validation_summary_tier_is_structural_for_structural_validators() {
    let h = harness(
        &bpmn_with_on_validation("route", None, &["xsd-ish"]),
        vec![Box::new(|r| {
            r.register(FailingValidator {
                name: "xsd-ish".to_string(),
                value: None,
                tier: ValidatorTier::Structural,
            })
        })],
    );
    drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain")))
        .expect("route mode continues");
    assert_eq!(
        summary(&h).get("tier"),
        Some(&FeelValue::String("structural".into()))
    );
}

#[test]
fn fatal_decode_short_circuits_validators_and_routes_the_summary() {
    // Malformed JSON: FATAL decode — the codec issues ride the summary, the validator
    // chain never runs, and mode=route still starts the flow (payload is null).
    let bpmn = bpmn_with_on_validation("route", None, &["never-runs"]);
    let ran: std::sync::Arc<std::sync::Mutex<bool>> = Default::default();
    struct NeverRuns(std::sync::Arc<std::sync::Mutex<bool>>);
    impl ContentValidator for NeverRuns {
        fn name(&self) -> &str {
            "never-runs"
        }
        fn validate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<ValidationIssue>, String> {
            *self.0.lock().unwrap() = true;
            Ok(Vec::new())
        }
    }
    let ran_in_validator = std::sync::Arc::clone(&ran);
    let h = build_harness(&bpmn, "json", move |_c, r| {
        r.register(NeverRuns(ran_in_validator));
    });
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"{oops", "application/json")),
    )
    .expect("route mode continues");
    assert!(
        !*ran.lock().unwrap(),
        "tier-2 must not run after a FATAL decode"
    );
    let s = summary(&h);
    assert_eq!(s.get("outcome"), Some(&FeelValue::String("FATAL".into())));
    assert_eq!(s.get("tier"), Some(&FeelValue::String("structural".into())));
}

#[test]
fn fatal_decode_with_reject_mode_rejects_the_transport() {
    let h = build_harness(
        &bpmn_with_on_validation("reject", None, &[]),
        "json",
        |_, _| {},
    );
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"{oops",
        "application/json",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.VALIDATION_REJECT");
}

// ===== Validator crash → synthetic RUNTIME.VALIDATOR.UNCAUGHT =====

#[test]
fn validator_error_becomes_synthetic_uncaught_issue() {
    struct Crashing;
    impl ContentValidator for Crashing {
        fn name(&self) -> &str {
            "crashing"
        }
        fn validate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<ValidationIssue>, String> {
            Err("kaboom".to_string())
        }
    }
    let h = harness(
        &bpmn_with_source(&["crashing"]),
        vec![Box::new(|r| r.register(Crashing))],
    );
    // A misbehaving validator must not crash the inbound path — the issue is routable.
    drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain"))).expect("dispatch succeeds");
    let issues = validation_issues(&h);
    assert_eq!(issues.len(), 1);
    let FeelValue::Map(issue) = &issues[0] else {
        panic!("issue is a map");
    };
    assert_eq!(
        issue.get("code"),
        Some(&FeelValue::String(
            "SUTRA.RUNTIME.VALIDATOR.UNCAUGHT".into()
        ))
    );
    let Some(FeelValue::String(msg)) = issue.get("message") else {
        panic!("message present");
    };
    assert!(msg.contains("crashing") && msg.contains("kaboom"), "{msg}");
}

#[test]
fn validator_panic_becomes_synthetic_uncaught_issue() {
    struct Panicking;
    impl ContentValidator for Panicking {
        fn name(&self) -> &str {
            "panicking"
        }
        fn validate(&self, _p: &FeelValue, _v: &Variables) -> Result<Vec<ValidationIssue>, String> {
            panic!("boom at runtime");
        }
    }
    let h = harness(
        &bpmn_with_source(&["panicking"]),
        vec![Box::new(|r| r.register(Panicking))],
    );
    drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain"))).expect("dispatch succeeds");
    let issues = validation_issues(&h);
    assert_eq!(issues.len(), 1);
    let FeelValue::Map(issue) = &issues[0] else {
        panic!("issue is a map");
    };
    assert_eq!(
        issue.get("code"),
        Some(&FeelValue::String(
            "SUTRA.RUNTIME.VALIDATOR.UNCAUGHT".into()
        ))
    );
}

// ===== DMN validators through the intake ===========================

const AMOUNT_DMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20230324/MODEL/"
             namespace="urn:test:amounts">
  <decision id="amount-rules.dmn" name="Amount rules">
    <decisionTable hitPolicy="COLLECT">
      <input id="i1">
        <inputExpression typeRef="number"><text>amount</text></inputExpression>
      </input>
      <output id="o1" name="issue" typeRef="string"/>
      <rule id="r_neg">
        <inputEntry><text>&lt; 0</text></inputEntry>
        <outputEntry><text>"invalid-amount"</text></outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>"#;

#[test]
fn dmn_validator_receives_the_map_projection_and_emits_issues() {
    // json codec → map payload → the payload projection feeds the map itself as the FEEL
    // context; `amount < 0` fires the DMN rule.
    let defs = sutra_dmn::DmnFileLoader::new()
        .load(AMOUNT_DMN.as_bytes())
        .expect("DMN loads");
    let decision = defs.decision("amount-rules.dmn").expect("decision").clone();
    let key = archive_key(&logical_urn("rule", "amount-rules.dmn"), &deployment());
    let h = build_harness(
        &bpmn_with_source(&["amount-rules.dmn"]),
        "json",
        move |_c, r| {
            r.register_under(
                &key,
                DmnContentValidator::new(sutra_dmn::DmnRulesetValidator::new(decision)),
            );
        },
    );
    drive(h.engine.dispatch(&inbound(
        "orders-in",
        br#"{"amount": -100}"#,
        "application/json",
    )))
    .expect("dispatch succeeds");
    let issues = validation_issues(&h);
    assert_eq!(issues.len(), 1);
    let FeelValue::Map(issue) = &issues[0] else {
        panic!("issue is a map");
    };
    assert_eq!(
        issue.get("message"),
        Some(&FeelValue::String("invalid-amount".into()))
    );
    // …and a clean payload passes.
    let h2 = {
        let defs = sutra_dmn::DmnFileLoader::new()
            .load(AMOUNT_DMN.as_bytes())
            .expect("DMN loads");
        let decision = defs.decision("amount-rules.dmn").expect("decision").clone();
        let key = archive_key(&logical_urn("rule", "amount-rules.dmn"), &deployment());
        build_harness(
            &bpmn_with_source(&["amount-rules.dmn"]),
            "json",
            move |_c, r| {
                r.register_under(
                    &key,
                    DmnContentValidator::new(sutra_dmn::DmnRulesetValidator::new(decision)),
                );
            },
        )
    };
    drive(h2.engine.dispatch(&inbound(
        "orders-in",
        br#"{"amount": 100}"#,
        "application/json",
    )))
    .expect("dispatch succeeds");
    assert!(validation_issues(&h2).is_empty());
}

// ===== `.srl` rulesets as tier-2 validators, mixed with DMN on one chain ============

const CURRENCY_SRL: &str = r#"
rule "currency-not-usd"
when
  exists(currency) and currency != "USD"
then
  report("TEST.CURRENCY_NOT_USD", "currency", "invalid-currency");
end
"#;

/// A `<q:validators>` chain may MIX engines: a `.dmn` decision table and a `.srl` ruleset both
/// resolve out of the same archive-keyed `ValidatorRegistry`, both receive the same map
/// projection, and their issues ACCUMULATE into one `payload.validation` list in DECLARATION
/// order. This is the shape a mixed rail module ships (clock rules DMN, field rules `.srl`).
#[test]
fn a_dmn_and_an_srl_validator_compose_on_one_chain_in_declaration_order() {
    let defs = sutra_dmn::DmnFileLoader::new()
        .load(AMOUNT_DMN.as_bytes())
        .expect("DMN loads");
    let decision = defs.decision("amount-rules.dmn").expect("decision").clone();
    let dmn_key = archive_key(&logical_urn("rule", "amount-rules.dmn"), &deployment());
    let srl_key = archive_key(&logical_urn("rule", "field-rules.srl"), &deployment());
    let h = build_harness(
        &bpmn_with_source(&["amount-rules.dmn", "field-rules.srl"]),
        "json",
        move |_c, r| {
            r.register_under(
                &dmn_key,
                DmnContentValidator::new(sutra_dmn::DmnRulesetValidator::new(decision)),
            );
            r.register_under(
                &srl_key,
                SrlContentValidator::new("field-rules.srl", CURRENCY_SRL),
            );
        },
    );
    drive(h.engine.dispatch(&inbound(
        "orders-in",
        br#"{"amount": -100, "currency": "EUR"}"#,
        "application/json",
    )))
    .expect("dispatch succeeds");

    let issues = validation_issues(&h);
    assert_eq!(
        issues.len(),
        2,
        "both engines' issues accumulate: {issues:?}"
    );
    let field = |i: usize, key: &str| match &issues[i] {
        FeelValue::Map(m) => m.get(key).cloned(),
        other => panic!("issue is a map, got {other:?}"),
    };
    assert_eq!(
        field(0, "message"),
        Some(FeelValue::String("invalid-amount".into())),
        "the DMN validator is declared first"
    );
    assert_eq!(
        field(1, "code"),
        Some(FeelValue::String("TEST.CURRENCY_NOT_USD".into()))
    );
    assert_eq!(
        field(1, "message"),
        Some(FeelValue::String("invalid-currency".into()))
    );
    // The `.srl` engine reports ERROR severity, so the frozen summary is FATAL/content — the routing the
    // downstream gateway keys on, regardless of which engine produced the issue.
    assert_eq!(
        captured_var(&h, "validation").and_then(|v| match v {
            FeelValue::Map(m) => m.get("outcome").cloned(),
            _ => None,
        }),
        Some(FeelValue::String("FATAL".into()))
    );
}

/// A clean payload silences BOTH engines — no issues, no summary escalation.
#[test]
fn a_dmn_and_an_srl_validator_both_stay_silent_on_a_clean_payload() {
    let defs = sutra_dmn::DmnFileLoader::new()
        .load(AMOUNT_DMN.as_bytes())
        .expect("DMN loads");
    let decision = defs.decision("amount-rules.dmn").expect("decision").clone();
    let dmn_key = archive_key(&logical_urn("rule", "amount-rules.dmn"), &deployment());
    let srl_key = archive_key(&logical_urn("rule", "field-rules.srl"), &deployment());
    let h = build_harness(
        &bpmn_with_source(&["amount-rules.dmn", "field-rules.srl"]),
        "json",
        move |_c, r| {
            r.register_under(
                &dmn_key,
                DmnContentValidator::new(sutra_dmn::DmnRulesetValidator::new(decision)),
            );
            r.register_under(
                &srl_key,
                SrlContentValidator::new("field-rules.srl", CURRENCY_SRL),
            );
        },
    );
    drive(h.engine.dispatch(&inbound(
        "orders-in",
        br#"{"amount": 100, "currency": "USD"}"#,
        "application/json",
    )))
    .expect("dispatch succeeds");
    assert!(validation_issues(&h).is_empty());
}

/// Fail-closed: a `.srl` whose condition errors at evaluation (here a non-boolean condition)
/// becomes the synthetic `RUNTIME.VALIDATOR.UNCAUGHT` ERROR issue — never a dropped message,
/// never a crashed intake.
#[test]
fn an_srl_evaluation_failure_becomes_the_synthetic_uncaught_issue() {
    const BROKEN_SRL: &str = r#"
rule "broken"
when
  amount + 1
then
  report("TEST.NEVER", "x", "never");
end
"#;
    let key = archive_key(&logical_urn("rule", "broken.srl"), &deployment());
    let h = build_harness(&bpmn_with_source(&["broken.srl"]), "json", move |_c, r| {
        r.register_under(&key, SrlContentValidator::new("broken.srl", BROKEN_SRL))
    });
    drive(h.engine.dispatch(&inbound(
        "orders-in",
        br#"{"amount": 1}"#,
        "application/json",
    )))
    .expect("dispatch succeeds");
    let issues = validation_issues(&h);
    assert_eq!(issues.len(), 1);
    let FeelValue::Map(issue) = &issues[0] else {
        panic!("issue is a map");
    };
    assert_eq!(
        issue.get("code"),
        Some(&FeelValue::String(
            "SUTRA.RUNTIME.VALIDATOR.UNCAUGHT".into()
        ))
    );
}

// ===== alias materialisation + onConflict semantics =====

fn alias_bpmn(on_conflict: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="payload"/>
                <q:alias name="e2eId" expression="payload" unique="true" onConflict="{on_conflict}"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="capture"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

#[test]
fn alias_values_materialise_into_variables_before_execution() {
    let h = harness(&alias_bpmn("reject"), vec![]);
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"E2E-42", "text/plain")),
    )
    .expect("dispatch succeeds");
    assert_eq!(
        captured_var(&h, "e2eId"),
        Some(FeelValue::String("E2E-42".to_string()))
    );
}

#[test]
fn alias_rows_retire_when_the_sync_instance_completes() {
    // A sync run reaches a terminal state inside the dispatch — its alias rows retire, so
    // the SAME value dispatches cleanly again (the AliasRetiringListener contract).
    let h = harness(&alias_bpmn("reject"), vec![]);
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"E2E-42", "text/plain")),
    )
    .expect("first dispatch");
    drive(
        h.engine
            .dispatch(&inbound("orders-in", b"E2E-42", "text/plain")),
    )
    .expect("second dispatch — the first instance's rows are retired");
}

#[test]
fn alias_unique_conflict_with_reject_policy_aborts_the_start() {
    let h = harness(&alias_bpmn("reject"), vec![]);
    // A LIVE row for another instance already owns the value.
    h.aliases
        .record(&deployment(), "other-instance", "e2eId", "E2E-42", true);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"E2E-42",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.ALIAS_CONFLICT_REJECT");
    // The instance never started.
    assert!(h.captured.borrow().input.is_none());
}

#[test]
fn alias_unique_conflict_with_correlate_policy_rejects_the_second_arrival_in_v1() {
    let h = harness(&alias_bpmn("correlate"), vec![]);
    h.aliases
        .record(&deployment(), "other-instance", "e2eId", "E2E-42", true);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"E2E-42",
        "text/plain",
    ))));
    // v1: correlate records the redirect but the second arrival is still rejected.
    assert_eq!(d.code, "SUTRA.INBOUND.ALIAS_CONFLICT_REJECT");
    assert!(d.message.contains("correlate"), "{}", d.message);
}

#[test]
fn alias_multi_with_non_list_result_fails_with_multi_not_list() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="payload"/>
                <q:alias name="items" expression="payload" multi="true"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="capture"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let h = harness(bpmn, vec![]);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"scalar",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.ALIAS_MULTI_NOT_LIST");
}

// ===== inbox dedup (first-observer-wins) ================

#[test]
fn explicit_event_id_dedups_the_second_delivery() {
    let h = harness(&bpmn_with_source(&[]), vec![]);
    let mut msg = inbound("orders-in", b"hello", "text/plain");
    msg.idempotency_key = "evt-1".to_string();
    msg.explicit_event_id = true;
    assert!(matches!(
        drive(h.engine.dispatch(&msg)).expect("first delivery"),
        DispatchOutcome::Completed { .. }
    ));
    assert!(matches!(
        drive(h.engine.dispatch(&msg)).expect("second delivery"),
        DispatchOutcome::Duplicate
    ));
}

#[test]
fn sha_fallback_key_never_suppresses_a_repost() {
    // The isolation-test semantics: identical business payloads posted twice both run.
    let h = harness(&bpmn_with_source(&[]), vec![]);
    let msg = inbound("orders-in", b"same-bytes", "text/plain");
    for _ in 0..2 {
        assert!(matches!(
            drive(h.engine.dispatch(&msg)).expect("delivery runs"),
            DispatchOutcome::Completed { .. }
        ));
    }
    // The inbox never even saw the sha fallback key.
    assert!(drive(h.inbox.record_seen(
        &deployment(),
        "orders-in",
        &msg.idempotency_key
    )));
}

// ===== Guards: tenant agreement, payload cap, ambiguity, broadcast, wait states =====

#[test]
fn tenant_mismatch_is_rejected() {
    let h = harness(&bpmn_with_source(&[]), vec![]);
    let mut msg = inbound("orders-in", b"x", "text/plain");
    msg.tenant = "mallory".to_string();
    let d = expect_err(drive(h.engine.dispatch(&msg)));
    assert_eq!(d.code, "SUTRA.INBOUND.REJECTED.TENANT_CHANNEL_NOT_ALLOWED");
}

#[test]
fn unknown_channel_is_rejected() {
    let h = harness(&bpmn_with_source(&[]), vec![]);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "nope",
        b"x",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.RESOLVE.CHANNEL.UNKNOWN");
}

#[test]
fn ambiguous_handlers_on_a_non_broadcast_channel_fail_closed() {
    // Two processes subscribe the same (channel, catch-all) — non-broadcast must resolve
    // to exactly one.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="a">
            <bpmn:startEvent id="SA">
              <bpmn:extensionElements><q:source channel="orders-in" name="payload"/></bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="EA"/>
            <bpmn:sequenceFlow id="fa" sourceRef="SA" targetRef="EA"/>
          </bpmn:process>
          <bpmn:process id="b">
            <bpmn:startEvent id="SB">
              <bpmn:extensionElements><q:source channel="orders-in" name="payload"/></bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="EB"/>
            <bpmn:sequenceFlow id="fb" sourceRef="SB" targetRef="EB"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let h = harness(bpmn, vec![]);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"x",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.AMBIGUOUS_HANDLER");
}

#[test]
fn broadcast_channel_fans_out_to_every_matching_process() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="a">
            <bpmn:startEvent id="SA">
              <bpmn:extensionElements><q:source channel="orders-in" name="payload"/></bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="TA" implementation="mark-a"/>
            <bpmn:endEvent id="EA"/>
            <bpmn:sequenceFlow id="fa1" sourceRef="SA" targetRef="TA"/>
            <bpmn:sequenceFlow id="fa2" sourceRef="TA" targetRef="EA"/>
          </bpmn:process>
          <bpmn:process id="b">
            <bpmn:startEvent id="SB">
              <bpmn:extensionElements><q:source channel="orders-in" name="payload"/></bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="TB" implementation="mark-b"/>
            <bpmn:endEvent id="EB"/>
            <bpmn:sequenceFlow id="fb1" sourceRef="SB" targetRef="TB"/>
            <bpmn:sequenceFlow id="fb2" sourceRef="TB" targetRef="EB"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let ran: Rc<RefCell<Vec<&'static str>>> = Rc::default();
    let (ra, rb) = (Rc::clone(&ran), Rc::clone(&ran));
    let tasks = TaskRegistry::new()
        .register("mark-a", move |_, _| {
            ra.borrow_mut().push("a");
            Ok(FeelValue::Map(BTreeMap::new()))
        })
        .register("mark-b", move |_, _| {
            rb.borrow_mut().push("b");
            Ok(FeelValue::Map(BTreeMap::new()))
        });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let mut binding = ChannelBinding::new("orders-in", namespace(), deployment(), "raw-text");
    binding.broadcast = true;
    let engine = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            CodecRegistry::with_builtins(),
            sutra_channels::FormatRegistry::with_builtins(),
            ValidatorRegistry::new(),
        ),
    )
    .with_binding(binding)
    .with_module(&deployment(), &module)
    .build();
    drive(engine.dispatch(&inbound("orders-in", b"x", "text/plain"))).expect("broadcast dispatch");
    assert_eq!(*ran.borrow(), vec!["a", "b"]);
}

#[test]
fn wait_state_process_fails_fast_with_persistence_required() {
    // A userTask parks — without an InstanceStore the inbound fails BEFORE executing.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="parked">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements><q:source channel="orders-in" name="payload"/></bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:userTask id="U" name="Approve"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="U"/>
            <bpmn:sequenceFlow id="f2" sourceRef="U" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let h = harness(bpmn, vec![]);
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"x",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.INBOUND.PERSISTENCE_REQUIRED");
}

// ===== body.<path> dedupKey derivation =====================
// A `<q:source dedupKey="body.<path>">` spec re-derives the dedup key from the DECODED payload,
// overriding the transport-supplied key AND driving inbox dedup on it; a missing/blank field
// or a non-body spec falls back to the transport key untouched.

fn bpmn_with_idempotency(spec: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="payload" dedupKey="{spec}"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="capture"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

fn inbound_with_key(channel: &str, body: &[u8], content_type: &str, key: &str) -> InboundMessage {
    let mut msg = inbound(channel, body, content_type);
    msg.idempotency_key = key.to_string();
    msg
}

fn captured_idempotency_key(h: &Harness) -> String {
    match captured_var(h, "event") {
        Some(FeelValue::Map(m)) => match m.get("idempotencyKey") {
            Some(FeelValue::String(s)) => s.clone(),
            other => panic!("idempotencyKey slot: {other:?}"),
        },
        other => panic!("event map: {other:?}"),
    }
}

#[test]
fn body_path_extracts_nested_field_overriding_the_transport_key() {
    let h = build_harness(
        &bpmn_with_idempotency("body.GrpHdr.MsgId"),
        "json",
        |_, _| {},
    );
    drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"GrpHdr":{"MsgId":"ABC-9"}}"#,
        "application/json",
        "sha256-fallback",
    )))
    .expect("dispatch succeeds");
    assert_eq!(captured_idempotency_key(&h), "ABC-9");
}

#[test]
fn body_path_dedup_key_deduplicates_a_redelivery() {
    // A body.<path> dedupKey DRIVES inbox dedup (it does not merely re-project the
    // FEEL variable). Two deliveries whose decoded body yields the same key are deduped: the second
    // is a first-observer-wins DUPLICATE, even though the transport keys differ / are non-explicit.
    let h = build_harness(
        &bpmn_with_idempotency("body.GrpHdr.MsgId"),
        "json",
        |_, _| {},
    );
    let first = drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"GrpHdr":{"MsgId":"DUP-1"}}"#,
        "application/json",
        "sha256-a",
    )))
    .expect("first dispatch");
    assert!(
        matches!(first, DispatchOutcome::Completed { .. }),
        "got {first:?}"
    );
    let second = drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"GrpHdr":{"MsgId":"DUP-1"}}"#,
        "application/json",
        "sha256-b",
    )))
    .expect("second dispatch");
    assert!(
        matches!(second, DispatchOutcome::Duplicate),
        "got {second:?}"
    );
}

#[test]
fn body_path_missing_field_falls_back_to_the_transport_key() {
    let h = build_harness(
        &bpmn_with_idempotency("body.GrpHdr.Nope"),
        "json",
        |_, _| {},
    );
    drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"GrpHdr":{"MsgId":"ABC-9"}}"#,
        "application/json",
        "transport-key-7",
    )))
    .expect("dispatch succeeds");
    assert_eq!(captured_idempotency_key(&h), "transport-key-7");
}

#[test]
fn body_path_blank_value_falls_back_to_the_transport_key() {
    let h = build_harness(
        &bpmn_with_idempotency("body.GrpHdr.MsgId"),
        "json",
        |_, _| {},
    );
    drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"GrpHdr":{"MsgId":"   "}}"#,
        "application/json",
        "supplied",
    )))
    .expect("dispatch succeeds");
    assert_eq!(captured_idempotency_key(&h), "supplied");
}

#[test]
fn body_path_numeric_leaf_is_stringified() {
    let h = build_harness(&bpmn_with_idempotency("body.Seq"), "json", |_, _| {});
    drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"Seq": 42}"#,
        "application/json",
        "supplied",
    )))
    .expect("dispatch succeeds");
    assert_eq!(captured_idempotency_key(&h), "42");
}

#[test]
fn no_body_spec_leaves_the_transport_supplied_key_untouched() {
    let h = build_harness(&bpmn_with_source(&[]), "json", |_, _| {});
    drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"GrpHdr":{"MsgId":"ABC-9"}}"#,
        "application/json",
        "header-supplied-key",
    )))
    .expect("dispatch succeeds");
    assert_eq!(captured_idempotency_key(&h), "header-supplied-key");
}

#[test]
fn header_spec_is_left_to_transport_the_engine_does_not_override() {
    // A `headers.*` (non-`body.`) spec is resolved transport-side; the engine must not
    // touch the key post-decode.
    let h = build_harness(
        &bpmn_with_idempotency("headers.X-Request-Id"),
        "json",
        |_, _| {},
    );
    drive(h.engine.dispatch(&inbound_with_key(
        "orders-in",
        br#"{"GrpHdr":{"MsgId":"ABC-9"}}"#,
        "application/json",
        "header-value-resolved-upstream",
    )))
    .expect("dispatch succeeds");
    assert_eq!(
        captured_idempotency_key(&h),
        "header-value-resolved-upstream"
    );
}

// ===== idempotency-gated inbound retry (execution failure ack disposition) ============
// A process whose service task FAILS (executor Err). The ack disposition is gated on the
// process's `<q:process idempotent>` assertion: a non-idempotent process is consumed +
// dead-lettered (at-most-once, no requeue); an idempotent process is tagged for requeue (retry).

fn bpmn_failing_task(idempotent: Option<bool>) -> String {
    let process_el = match idempotent {
        Some(v) => format!("<q:process idempotent=\"{v}\"/>"),
        None => String::new(),
    };
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="echo">
            <bpmn:extensionElements>{process_el}</bpmn:extensionElements>
            <bpmn:startEvent id="S">
              <bpmn:extensionElements>
                <q:source channel="orders-in" name="payload"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="boom"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

struct FailHarness {
    engine: ChannelEngine,
    incidents: Rc<InMemoryIncidentSink>,
}

/// An engine whose `boom` service task always fails (executor Err), with an incident sink wired.
fn build_fail_harness(bpmn: &str) -> FailHarness {
    let module = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect("BPMN loads");
    let tasks = TaskRegistry::new().register("boom", |_input, _ctx| {
        Err(TaskError::Failed("synthetic task failure".to_string()))
    });
    let sink = Rc::new(DrainingSink::new());
    let executor = TokenExecutor::builder(tasks)
        .with_feel()
        .with_emission_sink(Rc::clone(&sink) as Rc<dyn sutra_executor::EmissionSink>)
        .build();
    let codecs = CodecRegistry::with_builtins();
    let validators = ValidatorRegistry::new();
    let incidents = Rc::new(InMemoryIncidentSink::new());
    let engine = ChannelEngine::builder(
        executor,
        sink,
        InboundChain::new(
            codecs,
            sutra_channels::FormatRegistry::with_builtins(),
            validators,
        ),
    )
    .with_binding(ChannelBinding::new(
        "orders-in",
        namespace(),
        deployment(),
        "raw-text",
    ))
    .with_module(&deployment(), &module)
    .with_inbox(Rc::new(InMemoryInboxStore::new()) as Rc<dyn InboxStore>)
    .with_incident_sink(Rc::clone(&incidents) as Rc<dyn IncidentSink>)
    .build();
    FailHarness { engine, incidents }
}

#[test]
fn non_idempotent_failure_is_dead_lettered_acked_and_recorded() {
    // Default (undeclared) is non-idempotent (fail-closed): the failure is CONSUMED (DeadLettered ⇒
    // the transport acks), a durable incident is recorded, and it is NOT requeued.
    let h = build_fail_harness(&bpmn_failing_task(None));
    let out = drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain")))
        .expect("dispatch returns a consumed outcome (not an Err → not a requeue)");
    match out {
        DispatchOutcome::DeadLettered {
            code, cause_code, ..
        } => {
            assert_eq!(code, "SUTRA.INBOUND.NON_IDEMPOTENT_FAILURE");
            assert_eq!(cause_code, "SUTRA.RUNTIME.TASK.UNCAUGHT");
        }
        other => panic!("expected DeadLettered, got {other:?}"),
    }
    let recorded = h.incidents.incidents();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].process_id, "echo");
    assert_eq!(recorded[0].channel, "orders-in");
    assert_eq!(recorded[0].failure_code, "SUTRA.RUNTIME.TASK.UNCAUGHT");
}

#[test]
fn the_dead_lettered_incident_captures_everything_a_replay_needs() {
    // P0-4: the incident used to be metadata only — a replay was not even reconstructible from
    // it. It now carries the consumed message itself plus the routing keys the intake path needs,
    // and its `deployment` is the RESOLVED dep-<hex> pin (a module_key there fails the durable
    // sink's id validation outright, so the row would be dropped and only the log floor survive).
    let h = build_fail_harness(&bpmn_failing_task(None));
    let mut message = inbound("orders-in", b"{\"orderId\":\"A-1\"}", "application/json");
    message
        .headers
        .insert("x-corr".to_string(), "corr-9".to_string());
    message.idempotency_key = "evt-1".to_string();

    drive(h.engine.dispatch(&message)).expect("consumed");

    let recorded = h.incidents.incidents();
    assert_eq!(recorded.len(), 1);
    let incident = &recorded[0];
    assert_eq!(
        incident.deployment,
        deployment().value(),
        "the isolation key is the deployment pin, never the module_key namespace string"
    );
    assert_eq!(incident.tenant, TENANT);
    assert_eq!(incident.module_key, namespace().module_key());
    assert_eq!(incident.channel, "orders-in");
    assert_eq!(incident.dedup_key, "evt-1");
    assert_eq!(
        incident.payload.as_deref(),
        Some(&b"{\"orderId\":\"A-1\"}"[..]),
        "the consumed body is captured verbatim"
    );
    assert_eq!(
        incident.headers.get("x-corr").map(String::as_str),
        Some("corr-9"),
        "headers replay verbatim"
    );
    assert_eq!(incident.content_type.as_deref(), Some("application/json"));
}

#[test]
fn explicit_non_idempotent_false_is_dead_lettered() {
    let h = build_fail_harness(&bpmn_failing_task(Some(false)));
    let out =
        drive(h.engine.dispatch(&inbound("orders-in", b"x", "text/plain"))).expect("consumed");
    assert!(
        matches!(out, DispatchOutcome::DeadLettered { .. }),
        "got {out:?}"
    );
    assert_eq!(h.incidents.len(), 1);
}

#[test]
fn idempotent_failure_requeues_and_records_no_incident() {
    // An idempotent process's failure is RETRY-SAFE: the dispatcher returns an Err tagged for
    // requeue (the transport maps it to NackRequeue), and NO incident is dead-lettered.
    let h = build_fail_harness(&bpmn_failing_task(Some(true)));
    let d = expect_err(drive(h.engine.dispatch(&inbound(
        "orders-in",
        b"x",
        "text/plain",
    ))));
    assert_eq!(d.code, "SUTRA.RUNTIME.TASK.UNCAUGHT");
    assert_eq!(
        d.attributes.get(ACK_DISPOSITION_ATTR).map(String::as_str),
        Some("requeue"),
        "idempotent failure must be tagged retry-safe"
    );
    assert!(
        h.incidents.is_empty(),
        "idempotent failure must not dead-letter"
    );
}
