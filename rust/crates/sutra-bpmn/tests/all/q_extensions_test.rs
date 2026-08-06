//! Parse-side tests for the q:
//! namespace extensions per `xsd/q.xsd`: `<q:source>` (full attribute set, nested validators,
//! message-type), `<q:onValidation>`, `<q:dispatch>` + `<q:case>`, `<q:alias>`, `<q:reply>`,
//! and per-element `<q:audit>` — well-formed parses plus the `SUTRA.PARSE.Q_*` diagnostics.

use sutra_bpmn::qbindings::{
    AckMode, AliasConflict, AuditCapture, DataClass, OnNoMatch, OnValidationMode,
    OutboundAuthScheme, ReplyMode,
};
use sutra_bpmn::{codes, BpmnModelLoader, ProcessModule, SutraError};

fn load(bpmn: &str) -> Result<ProcessModule, SutraError> {
    BpmnModelLoader::new().load(bpmn.as_bytes())
}

fn assert_load_fails_with_code(bpmn: &str, expected: &str) {
    let e = load(bpmn).unwrap_err();
    assert_eq!(e.code, expected, "{e}");
}

// ===== <q:source> full attribute set =====

#[test]
fn parses_source_with_full_attribute_set() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="orders.in"
                          ack="on-complete"
                          dedupKey="header.X-Request-Id"
                          type="com.example.OrderCreated"
                          dataClass="pii"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let b = process.bindings_for("Start");
    assert_eq!(b.sources.len(), 1);
    let src = &b.sources[0];
    assert_eq!(src.channel, "orders.in");
    assert_eq!(src.ack, AckMode::OnComplete);
    assert_eq!(src.dedup_key.as_deref(), Some("header.X-Request-Id"));
    assert_eq!(
        src.message_type.as_deref(),
        Some("com.example.OrderCreated")
    );
    assert_eq!(src.data_class, DataClass::Pii);
}

#[test]
fn source_defaults_ack_and_data_class_when_absent() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="alpha"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let src = &process.bindings_for("Start").sources[0];
    assert_eq!(src.ack, AckMode::OnPersist);
    assert_eq!(src.data_class, DataClass::None);
    assert!(src.dedup_key.is_none());
    assert!(src.message_type.is_none());
}

// ===== process-level <q:process idempotent> + dedupKey rename =====

#[test]
fn process_idempotent_flag_parses_true() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:extensionElements>
              <q:process idempotent="true"/>
            </bpmn:extensionElements>
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="orders.in"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    assert!(module.process("p1").unwrap().idempotent);
}

#[test]
fn process_idempotent_defaults_false_when_undeclared() {
    // Fail-closed: an undeclared process is treated as NON-idempotent.
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="orders.in"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    assert!(!module.process("p1").unwrap().idempotent);
    // Explicit false also parses to false.
    let explicit = bpmn.replace(
        "<bpmn:startEvent id=\"Start\">",
        "<bpmn:extensionElements><q:process idempotent=\"false\"/></bpmn:extensionElements>\n<bpmn:startEvent id=\"Start\">",
    );
    assert!(!load(&explicit).unwrap().process("p1").unwrap().idempotent);
}

#[test]
fn retired_idempotency_key_attribute_is_a_deploy_error() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="orders.in" idempotencyKey="header.X-Request-Id"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_SOURCE_IDEMPOTENCY_KEY_RENAMED);
}

#[test]
fn parses_source_with_name_and_validators_chain() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="alpha" name="body">
                  <q:validators>
                    <q:complexValidator source="order.dmn"/>
                    <q:complexValidator source="extra.dmn"/>
                    <q:simpleValidator ref="iso-4217-currency" path="payload.body.Ccy"/>
                  </q:validators>
                </q:source>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let b = process.bindings_for("Start");
    let src = b.source().expect("source present");
    assert_eq!(src.channel, "alpha");
    assert_eq!(src.name, "body");
    assert_eq!(src.complex_validators, vec!["order.dmn", "extra.dmn"]);
    assert_eq!(src.simple_validators.len(), 1);
    assert_eq!(src.simple_validators[0].reference, "iso-4217-currency");
    assert_eq!(src.simple_validators[0].path, "payload.body.Ccy");
}

#[test]
fn source_defaults_name() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="alpha"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let src = process.bindings_for("Start").source().unwrap().clone();
    assert_eq!(src.name, "payload");
    assert!(src.complex_validators.is_empty());
    assert!(src.simple_validators.is_empty());
    assert!(src.message_type_value.is_none());
    assert!(src.message_type_pattern.is_none());
}

#[test]
fn simple_validator_requires_ref_and_path() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="alpha">
                  <q:validators>
                    <q:simpleValidator ref="iso-4217-currency"/>
                  </q:validators>
                </q:source>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_SIMPLE_VALIDATOR_INCOMPLETE);
}

fn source_with(source_attrs: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="alpha" {source_attrs}/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

#[test]
fn parses_message_type_subscription() {
    let module = load(&source_with(r#"messageTypeValue="order.created.001.14""#)).unwrap();
    let src = module
        .process("p1")
        .unwrap()
        .bindings_for("Start")
        .source()
        .unwrap()
        .clone();
    assert_eq!(
        src.message_type_value.as_deref(),
        Some("order.created.001.14")
    );
    assert!(src.message_type_pattern.is_none());

    let module = load(&source_with(r#"messageTypePattern="order\.created\..*""#)).unwrap();
    let src = module
        .process("p1")
        .unwrap()
        .bindings_for("Start")
        .source()
        .unwrap()
        .clone();
    assert_eq!(
        src.message_type_pattern.as_deref(),
        Some(r"order\.created\..*")
    );
    assert!(src.message_type_value.is_none());
}

#[test]
fn message_type_value_and_pattern_are_mutually_exclusive() {
    assert_load_fails_with_code(
        &source_with(r#"messageTypeValue="a" messageTypePattern="b.*""#),
        codes::PARSE_Q_SOURCE_MESSAGE_TYPE_CONFLICT,
    );
}

#[test]
fn codec_on_source_is_rejected() {
    // The codec is YAML-authoritative — declaring it on q:source is a parse error.
    assert_load_fails_with_code(
        &source_with(r#"codec="xml""#),
        codes::PARSE_Q_SOURCE_CODEC_NOT_ALLOWED,
    );
}

#[test]
fn multiple_sources_on_one_start_event_rejected() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="alpha"/>
                <q:source channel="beta"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_SOURCE_MULTIPLE);
}

// ===== <q:onValidation> =====

#[test]
fn parses_on_validation_route_with_error_code() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:onValidation mode="route" errorCode="ERR_VALIDATION"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let ov = process.bindings_for("Start").on_validation.clone().unwrap();
    assert_eq!(ov.mode, OnValidationMode::Route);
    assert_eq!(ov.error_code.as_deref(), Some("ERR_VALIDATION"));
}

#[test]
fn on_validation_invalid_mode_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:onValidation mode="discard"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_ON_VALIDATION_INVALID_MODE);
}

// ===== <q:dispatch> + <q:case> =====

#[test]
fn parses_dispatch_table_with_multiple_cases() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="Route" calledElement="placeholder">
              <bpmn:extensionElements>
                <q:dispatch default="fallbackProc" onNoMatch="skip">
                  <q:case when="payload.kind = 'a'" calledElement="procA" scope="common"/>
                  <q:case when="payload.kind = 'b'" calledElement="procB" scope="tenant"/>
                  <q:case when="payload.kind = 'c'" calledElement="procC"/>
                </q:dispatch>
              </bpmn:extensionElements>
            </bpmn:callActivity>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Route"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Route" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let d = process.bindings_for("Route").dispatch.clone().unwrap();
    assert_eq!(d.default_called_element.as_deref(), Some("fallbackProc"));
    assert_eq!(d.on_no_match, OnNoMatch::Skip);
    assert_eq!(d.cases.len(), 3);
    assert_eq!(d.cases[0].when, "payload.kind = 'a'");
    assert_eq!(d.cases[0].called_element, "procA");
    assert_eq!(d.cases[1].called_element, "procB");
    assert_eq!(d.cases[2].called_element, "procC");
}

#[test]
fn dispatch_case_missing_when_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="Route" calledElement="placeholder">
              <bpmn:extensionElements>
                <q:dispatch>
                  <q:case calledElement="procA"/>
                </q:dispatch>
              </bpmn:extensionElements>
            </bpmn:callActivity>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Route"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Route" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_CASE_MISSING_WHEN);
}

#[test]
fn dispatch_case_missing_called_element_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="Route" calledElement="placeholder">
              <bpmn:extensionElements>
                <q:dispatch>
                  <q:case when="payload.kind = 'a'"/>
                </q:dispatch>
              </bpmn:extensionElements>
            </bpmn:callActivity>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Route"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Route" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_CASE_MISSING_CALLED_ELEMENT);
}

// ===== <q:alias> =====

#[test]
fn parses_alias_bindings() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:alias name="orderId" expression="payload.orderId"
                         unique="true" onConflict="correlate"/>
                <q:alias name="lineItems" expression="payload.items" multi="true"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let aliases = &process.bindings_for("Start").aliases;
    assert_eq!(aliases.len(), 2);
    let a1 = &aliases[0];
    assert_eq!(a1.name, "orderId");
    assert_eq!(a1.expression, "payload.orderId");
    assert!(a1.unique);
    assert!(!a1.multi);
    assert_eq!(a1.on_conflict, Some(AliasConflict::Correlate));
    let a2 = &aliases[1];
    assert_eq!(a2.name, "lineItems");
    assert!(a2.multi);
    assert!(!a2.unique);
    assert_eq!(a2.on_conflict, None);
}

#[test]
fn alias_missing_name_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:alias expression="payload.foo"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_ALIAS_MISSING_NAME);
}

#[test]
fn alias_missing_expression_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:alias name="orderId"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_ALIAS_MISSING_EXPRESSION);
}

// ===== <q:reply> =====

#[test]
fn parses_reply_on_service_task() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Echo" implementation="${echo}">
              <bpmn:extensionElements>
                <q:reply mode="cloudevent-binary"
                         destination="https://example.com/cb"
                         contentType="application/json"
                         messageType="invoice.settled.001.12"
                         required="true"
                         type="com.example.OrderProcessed"
                         source="/sutra/orders"
                         subject="orderId-42"
                         datacontenttype="application/json"
                         auth="bearer"
                         authSecretRef="env:CALLBACK_TOKEN"
                         authHeader="Authorization"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Echo"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Echo" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let r = process.bindings_for("Echo").reply.clone().unwrap();
    assert_eq!(r.mode, ReplyMode::CloudeventBinary);
    assert_eq!(r.destination.as_deref(), Some("https://example.com/cb"));
    assert_eq!(r.content_type.as_deref(), Some("application/json"));
    assert_eq!(r.message_type.as_deref(), Some("invoice.settled.001.12"));
    assert!(r.required);
    assert_eq!(r.ce_type.as_deref(), Some("com.example.OrderProcessed"));
    assert_eq!(r.ce_source.as_deref(), Some("/sutra/orders"));
    assert_eq!(r.ce_subject.as_deref(), Some("orderId-42"));
    assert_eq!(r.ce_data_content_type.as_deref(), Some("application/json"));
    assert_eq!(r.auth, Some(OutboundAuthScheme::Bearer));
    assert_eq!(r.auth_secret_ref.as_deref(), Some("env:CALLBACK_TOKEN"));
    assert_eq!(r.auth_header.as_deref(), Some("Authorization"));
}

#[test]
fn reply_mode_mapping_covers_all_four_enum_values() {
    for (xml, expected) in [
        ("native", ReplyMode::Native),
        ("cloudevent-binary", ReplyMode::CloudeventBinary),
        ("cloudevent-structured", ReplyMode::CloudeventStructured),
        ("match-inbound", ReplyMode::MatchInbound),
    ] {
        let bpmn = format!(
            r#"<?xml version="1.0"?>
            <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                              xmlns:q="urn:sutra:q:1.0">
              <bpmn:process id="p1">
                <bpmn:startEvent id="S"/>
                <bpmn:endEvent id="E">
                  <bpmn:extensionElements>
                    <q:reply mode="{xml}"/>
                  </bpmn:extensionElements>
                </bpmn:endEvent>
                <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
              </bpmn:process>
            </bpmn:definitions>"#
        );
        let module = load(&bpmn).unwrap();
        let process = module.process("p1").unwrap();
        let r = process.bindings_for("E").reply.clone().unwrap();
        assert_eq!(r.mode, expected, "xml mode '{xml}'");
    }
}

#[test]
fn reply_invalid_mode_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="batch"/>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_REPLY_INVALID_MODE);
}

// ===== author-declared <q:header> on <q:reply> / <q:send> =====

#[test]
fn parses_reply_with_author_headers() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native" destination="https://example.com/cb">
                  <q:header name="txnId" value="event.headers.X-Txn-Id"/>
                  <q:header name="corrId" value="corrId"/>
                </q:reply>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let r = process.bindings_for("E").reply.clone().unwrap();
    assert_eq!(r.headers.len(), 2);
    assert_eq!(r.headers[0].name, "txnId");
    assert_eq!(r.headers[0].value, "event.headers.X-Txn-Id");
    assert_eq!(r.headers[1].name, "corrId");
    assert_eq!(r.headers[1].value, "corrId");
}

#[test]
fn parses_send_with_author_headers() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:intermediateThrowEvent id="Notify">
              <bpmn:extensionElements>
                <q:send channel="responses-out">
                  <q:header name="txnId" value="txnId"/>
                </q:send>
              </bpmn:extensionElements>
              <bpmn:messageEventDefinition/>
            </bpmn:intermediateThrowEvent>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Notify"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Notify" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let s = process.bindings_for("Notify").send.clone().unwrap();
    assert_eq!(s.headers.len(), 1);
    assert_eq!(s.headers[0].name, "txnId");
    assert_eq!(s.headers[0].value, "txnId");
}

#[test]
fn reply_with_headers_defaults_to_empty_when_absent() {
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
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    assert!(process
        .bindings_for("E")
        .reply
        .clone()
        .unwrap()
        .headers
        .is_empty());
}

#[test]
fn header_without_name_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native">
                  <q:header value="txnId"/>
                </q:reply>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_HEADER_INCOMPLETE);
}

#[test]
fn header_without_value_emits_diagnostic() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:endEvent id="E">
              <bpmn:extensionElements>
                <q:reply mode="native">
                  <q:header name="txnId"/>
                </q:reply>
              </bpmn:extensionElements>
            </bpmn:endEvent>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    assert_load_fails_with_code(bpmn, codes::PARSE_Q_HEADER_INCOMPLETE);
}

// ===== per-element <q:audit> =====

#[test]
fn parses_per_node_audit() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Sensitive" implementation="${redactor}">
              <bpmn:extensionElements>
                <q:audit sink="kafka" target="audit.sensitive" capture="metadata"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sensitive"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sensitive" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let audit = process.bindings_for("Sensitive").audit.clone().unwrap();
    assert_eq!(audit.sink, "kafka");
    assert_eq!(audit.target.as_deref(), Some("audit.sensitive"));
    assert_eq!(audit.capture, AuditCapture::Metadata);
}

#[test]
fn per_node_audit_does_not_collide_with_process_level_audit_version() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:extensionElements>
              <q:audit version="3.2.1"/>
            </bpmn:extensionElements>
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T" implementation="${t}">
              <bpmn:extensionElements>
                <q:audit capture="none"/>
              </bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    assert_eq!(process.module_version, "3.2.1");
    let node_audit = process.bindings_for("T").audit.clone().unwrap();
    assert_eq!(node_audit.capture, AuditCapture::None);
    assert_eq!(node_audit.sink, "sql"); // default per xsd/q.xsd#AuditType
                                        // The Start event has no per-node q:audit overlay — its bindings are empty.
    assert!(process.bindings_for("S").audit.is_none());
}

// ===== Composite — multiple q: elements on one node =====

#[test]
fn multiple_q_elements_on_one_start_event_coexist() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="p1">
            <bpmn:startEvent id="Start">
              <bpmn:extensionElements>
                <q:source channel="orders.in" ack="on-complete" dataClass="financial"/>
                <q:onValidation mode="reject"/>
                <q:alias name="orderId" expression="payload.orderId" unique="true"/>
                <q:audit sink="sql" capture="payload"/>
              </bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:endEvent id="End"/>
            <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let module = load(bpmn).unwrap();
    let process = module.process("p1").unwrap();
    let b = process.bindings_for("Start");
    assert_eq!(b.sources.len(), 1);
    assert!(b.source().is_some());
    assert_eq!(b.source().unwrap().data_class, DataClass::Financial);
    assert!(b.on_validation.is_some());
    assert_eq!(
        b.on_validation.as_ref().unwrap().mode,
        OnValidationMode::Reject
    );
    assert_eq!(b.aliases.len(), 1);
    assert!(b.audit.is_some());
}
