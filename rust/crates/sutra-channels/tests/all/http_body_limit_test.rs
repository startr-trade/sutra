//! The inbound HTTP request-body ceiling: 2 MiB by default, raised per channel with
//! `max-body-bytes`; a body over the ceiling answers 413 `SUTRA.INBOUND.BODY_TOO_LARGE` in the
//! engine's problem shape, whether `Content-Length` declares it or the stream only reveals it.

use std::collections::BTreeMap;
use std::rc::Rc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use sutra_bpmn::BpmnModelLoader;
use sutra_channels::http::{channel_router, spawn_engine};
use sutra_channels::{
    load_channel_definitions, ChannelEngine, CodecRegistry, DrainingSink, InboundChain,
    ValidatorRegistry, DEFAULT_HTTP_MAX_BODY_BYTES,
};
use sutra_executor::{DeploymentId, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;
use tower::util::ServiceExt;

const KEY: &str = "body-limit-key";
const BODY_TOO_LARGE: &str = "SUTRA.INBOUND.BODY_TOO_LARGE";

fn dep() -> DeploymentId {
    DeploymentId::of("dep-000000000000000000000084").expect("valid deployment id")
}

/// One synchronous json channel (`POST /channels/bulk`) over `S → T → E`; `T` answers
/// `{"ok":true}`. `override_line` is spliced into the channel entry (empty for none).
fn app(override_line: &str) -> Router {
    let yaml = format!(
        "channels:\n  - name: bulk\n    transport: http\n    bind: \"POST /channels/bulk\"\n    \
         codec: json\n{override_line}    auth:\n      scheme: apikey\n      \
         apikey:\n        value: {KEY}\n"
    );
    let mut defs = load_channel_definitions(yaml.as_bytes(), "t", "bulk-mod", "1.0.0", "test.yaml")
        .expect("loads");
    for d in &mut defs {
        d.binding.deployment = dep();
    }
    let defs_for_router = defs.clone();
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          <bpmn:process id="bulk">
            <bpmn:startEvent id="S">
              <bpmn:extensionElements><q:source channel="bulk" name="payload"/></bpmn:extensionElements>
            </bpmn:startEvent>
            <bpmn:serviceTask id="T" implementation="render"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let handle = spawn_engine(tokio::runtime::Handle::current(), move || {
        let module = BpmnModelLoader::new()
            .load(bpmn.as_bytes())
            .expect("BPMN loads");
        let tasks = TaskRegistry::new().register("render", |_input, _ctx| {
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
        .with_channel_definitions(&defs)
        .with_module(&dep(), &module)
        .build()
    });
    channel_router(&defs_for_router, handle).expect("router builds")
}

/// A json document of exactly `len` bytes.
fn json_of_len(len: usize) -> String {
    let wrapper = r#"{"blob":""}"#.len();
    format!(r#"{{"blob":"{}"}}"#, "a".repeat(len - wrapper))
}

/// POST `body`; `declare_length` sets `Content-Length` (otherwise the size shows only as the
/// body streams in). Returns the status and the problem `code` (empty when not a problem).
async fn post(app: &Router, body: String, declare_length: bool) -> (StatusCode, String) {
    let mut request = Request::builder()
        .method("POST")
        .uri("/channels/bulk")
        .header("Content-Type", "application/json")
        .header("X-Api-Key", KEY);
    if declare_length {
        request = request.header("Content-Length", body.len().to_string());
    }
    let request = request.body(Body::from(body)).expect("request builds");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let code = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|v| v["code"].as_str().map(str::to_string))
        .unwrap_or_default();
    (status, code)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_one_byte_over_the_default_is_413_streamed_or_declared() {
    let app = app("");
    let over = json_of_len(DEFAULT_HTTP_MAX_BODY_BYTES + 1);
    let (status, code) = post(&app, over.clone(), false).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(code, BODY_TOO_LARGE);
    let (status, code) = post(&app, over, true).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(code, BODY_TOO_LARGE);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_at_the_default_ceiling_is_served() {
    let app = app("");
    let (status, code) = post(&app, json_of_len(DEFAULT_HTTP_MAX_BODY_BYTES), true).await;
    assert_eq!(status, StatusCode::OK, "code {code}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_channel_override_raises_the_ceiling() {
    let app = app("    max-body-bytes: 4194304\n");
    let (status, code) = post(&app, json_of_len(DEFAULT_HTTP_MAX_BODY_BYTES + 1), false).await;
    assert_eq!(status, StatusCode::OK, "code {code}");
    let (status, code) = post(&app, json_of_len(4 * 1024 * 1024 + 1), true).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(code, BODY_TOO_LARGE);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_override_refuses_the_channel() {
    let yaml =
        "channels:\n  - name: bulk\n    transport: http\n    bind: \"POST /channels/bulk\"\n    \
                codec: json\n    max-body-bytes: 4MB\n    auth:\n      scheme: apikey\n      \
                apikey:\n        value: k\n";
    let defs = load_channel_definitions(yaml.as_bytes(), "t", "bulk-mod", "1.0.0", "test.yaml")
        .expect("loads");
    let err = sutra_channels::http_routes_of(&defs)
        .err()
        .expect("refused");
    assert_eq!(err.code, "SUTRA.CONFIG.PROPERTY.INVALID");
}
