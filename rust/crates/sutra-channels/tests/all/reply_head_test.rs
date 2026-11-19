//! `<q:reply status>` and `<q:header>` on a synchronous HTTP answer, over the axum transport: the
//! status and headers the flow declares reach the caller, a reply with no body still answers with
//! them, and a value that cannot be answered is refused with a coded 500 that never shows it.

use std::collections::BTreeMap;
use std::rc::Rc;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use sutra_bpmn::BpmnModelLoader;
use sutra_channels::http::{channel_router, spawn_engine};
use sutra_channels::{
    load_channel_definitions, ChannelEngine, CodecRegistry, DrainingSink, InboundChain,
    ValidatorRegistry,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;
use tower::util::ServiceExt;

const KEY: &str = "reply-head-key";
const STATUS_INVALID: &str = "SUTRA.OUTBOUND.REPLY_STATUS_INVALID";
const HEADER_INVALID: &str = "SUTRA.OUTBOUND.REPLY_HEADER_INVALID";

fn dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000051").expect("valid deployment id")
}

/// One channel (`POST /channels/api`, json) over `S → T → E`, where `T` runs `implementation`
/// and carries `reply`. `render` answers `{"ok":true}`; `silent` renders nothing.
fn app(reply: &str, implementation: &str, ack_mode: &str) -> Router {
    let yaml = format!(
        "channels:\n  - name: api\n    transport: http\n    bind: \"POST /channels/api\"\n    \
         codec: json\n    ack-mode: {ack_mode}\n    auth:\n      scheme: apikey\n      apikey:\n        \
         value: {KEY}\n"
    );
    let mut defs = load_channel_definitions(yaml.as_bytes(), "t", "api-mod", "1.0.0", "test.yaml")
        .expect("loads");
    for d in &mut defs {
        d.binding.deployment = dep();
    }
    let defs_for_router = defs.clone();
    let bpmn = format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="api">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements><q:source channel="api" name="payload"/></bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="{implementation}">
              <bpmn:extensionElements>{reply}</bpmn:extensionElements>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    );
    let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
        let module = BpmnModelLoader::new()
            .load(bpmn.as_bytes())
            .expect("BPMN loads");
        let tasks = TaskRegistry::new()
            .register("render", |_input, _ctx| {
                Ok(FeelValue::Map(BTreeMap::from([
                    (
                        "responseBody".to_string(),
                        FeelValue::String("{\"ok\":true}".to_string()),
                    ),
                    (
                        "responseContentType".to_string(),
                        FeelValue::String("application/json".to_string()),
                    ),
                ])))
            })
            .register("silent", |_input, _ctx| Ok(FeelValue::Map(BTreeMap::new())));
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
        .with_channel_definitions(&defs)
        .with_module(&dep(), &module)
        .build()
    });
    channel_router(&defs_for_router, handle).expect("router builds")
}

struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
}

impl Answer {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

async fn post(app: &Router, body: &str) -> Answer {
    let request = Request::builder()
        .method("POST")
        .uri("/channels/api")
        .header("Content-Type", "application/json")
        .header("X-Api-Key", KEY)
        .body(Body::from(body.to_string()))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    Answer {
        status,
        headers,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    }
}

fn problem_code(answer: &Answer) -> String {
    let problem: serde_json::Value =
        serde_json::from_str(&answer.body).expect("a problem document");
    problem["code"].as_str().unwrap_or_default().to_string()
}

#[tokio::test]
async fn a_literal_status_and_headers_answer_201_with_a_location() {
    let app = app(
        r#"<q:reply status="201">
             <q:header name="Location" value="'/items/' + payload.id"/>
             <q:header name="X-Trace" value="payload.trace"/>
           </q:reply>"#,
        "render",
        "on-complete",
    );
    let a = post(&app, r#"{"id":"42","trace":"t-1"}"#).await;
    assert_eq!(a.status, StatusCode::CREATED, "{}", a.body);
    assert_eq!(a.header("location"), Some("/items/42"));
    assert_eq!(a.header("x-trace"), Some("t-1"));
    assert_eq!(a.header("content-type"), Some("application/json"));
    assert_eq!(a.body, "{\"ok\":true}");
}

#[tokio::test]
async fn a_feel_status_is_evaluated_per_request() {
    let app = app(
        r#"<q:reply status="if payload.found then 200 else 404"/>"#,
        "render",
        "on-complete",
    );
    assert_eq!(post(&app, r#"{"found":true}"#).await.status, StatusCode::OK);
    assert_eq!(
        post(&app, r#"{"found":false}"#).await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_status_given_as_digits_in_a_string_is_taken() {
    let app = app(
        r#"<q:reply status="payload.code"/>"#,
        "render",
        "on-complete",
    );
    assert_eq!(
        post(&app, r#"{"code":"409"}"#).await.status,
        StatusCode::CONFLICT
    );
    assert_eq!(
        post(&app, r#"{"code":400}"#).await.status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn a_null_status_keeps_200_and_a_null_header_is_omitted() {
    let app = app(
        r#"<q:reply status="payload.code"><q:header name="X-Opt" value="payload.missing"/></q:reply>"#,
        "render",
        "on-complete",
    );
    let a = post(&app, r#"{}"#).await;
    assert_eq!(a.status, StatusCode::OK, "{}", a.body);
    assert!(a.header("x-opt").is_none());
    assert_eq!(a.body, "{\"ok\":true}");
}

#[tokio::test]
async fn a_reply_without_a_body_answers_401_with_its_challenge() {
    let app = app(
        r#"<q:reply status="401">
             <q:header name="WWW-Authenticate" value="'Bearer realm=&quot;api&quot;'"/>
           </q:reply>"#,
        "silent",
        "on-complete",
    );
    let a = post(&app, r#"{}"#).await;
    assert_eq!(a.status, StatusCode::UNAUTHORIZED);
    assert_eq!(a.header("www-authenticate"), Some("Bearer realm=\"api\""));
    assert!(a.body.is_empty(), "{}", a.body);
}

#[tokio::test]
async fn an_auth_check_answers_200_with_identity_headers_and_no_body() {
    let app = app(
        r#"<q:reply status="if payload.ok then 200 else 401">
             <q:header name="X-User-Id" value="if payload.ok then payload.user else null"/>
             <q:header name="X-Scope" value="if payload.ok then 'read' else null"/>
             <q:header name="X-Scope" value="if payload.ok then 'write' else null"/>
           </q:reply>"#,
        "silent",
        "on-complete",
    );
    let a = post(&app, r#"{"ok":true,"user":"u-7"}"#).await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(a.header("x-user-id"), Some("u-7"));
    let scopes: Vec<_> = a
        .headers
        .get_all("x-scope")
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    assert_eq!(
        scopes,
        ["read", "write"],
        "a repeated name is a second line"
    );
    assert!(a.body.is_empty());

    let a = post(&app, r#"{"ok":false}"#).await;
    assert_eq!(a.status, StatusCode::UNAUTHORIZED);
    assert!(a.header("x-user-id").is_none());
}

#[tokio::test]
async fn a_204_is_sent_without_a_body() {
    let app = app(r#"<q:reply status="204"/>"#, "render", "on-complete");
    let a = post(&app, r#"{}"#).await;
    assert_eq!(a.status, StatusCode::NO_CONTENT);
    assert!(a.body.is_empty(), "{}", a.body);
    assert!(a.header("content-type").is_none());
}

#[tokio::test]
async fn a_status_expression_that_gives_no_valid_status_is_a_coded_500() {
    let app = app(
        r#"<q:reply status="payload.code"/>"#,
        "render",
        "on-complete",
    );
    for body in [
        r#"{"code":700}"#,
        r#"{"code":"Created"}"#,
        r#"{"code":101}"#,
    ] {
        let a = post(&app, body).await;
        assert_eq!(a.status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(problem_code(&a), STATUS_INVALID, "{body}");
    }
}

#[tokio::test]
async fn a_redirect_whose_location_is_null_is_a_coded_500() {
    let app = app(
        r#"<q:reply status="payload.code"><q:header name="Location" value="payload.next"/></q:reply>"#,
        "silent",
        "on-complete",
    );
    let a = post(&app, r#"{"code":302}"#).await;
    assert_eq!(a.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(problem_code(&a), STATUS_INVALID);
    let a = post(&app, r#"{"code":302,"next":"/there"}"#).await;
    assert_eq!(a.status, StatusCode::FOUND);
    assert_eq!(a.header("location"), Some("/there"));
}

#[tokio::test]
async fn a_header_value_with_a_line_break_is_refused_without_showing_it() {
    let app = app(
        r#"<q:reply><q:header name="X-Echo" value="payload.v"/></q:reply>"#,
        "render",
        "on-complete",
    );
    let a = post(&app, r#"{"v":"secret-1\r\nSet-Cookie: x=1"}"#).await;
    assert_eq!(a.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(problem_code(&a), HEADER_INVALID);
    assert!(!a.body.contains("secret-1"), "{}", a.body);
    assert!(a.header("set-cookie").is_none());
}

#[tokio::test]
async fn an_asynchronous_ack_keeps_202_and_its_receipt_and_adds_the_headers() {
    let app = app(
        r#"<q:reply status="201"><q:header name="X-Batch" value="payload.id"/></q:reply>"#,
        "silent",
        "on-persist",
    );
    let a = post(&app, r#"{"id":"b-9"}"#).await;
    assert_eq!(a.status, StatusCode::ACCEPTED, "the ack contract stays 202");
    assert_eq!(a.header("x-batch"), Some("b-9"));
    assert!(a.header("sutra-receipt").is_some());
    let receipt: serde_json::Value = serde_json::from_str(&a.body).expect("the receipt body");
    assert!(receipt["instanceId"].is_string(), "{receipt}");
}

#[tokio::test]
async fn a_reply_without_status_or_headers_answers_as_before() {
    let app = app("<q:reply/>", "render", "on-complete");
    let a = post(&app, r#"{}"#).await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(a.body, "{\"ok\":true}");
}
