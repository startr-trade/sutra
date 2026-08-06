//! Generator tests. Inputs are built hermetically from inline `channels.yaml` / BPMN / datastore
//! text via the real parsers (the same path the engine + CLI feed the generator), so the tests
//! exercise the actual manifest→spec projection, not hand-built structs.
//!
//! `tests/golden/mini-package.openapi.yaml` is the drift gate for the generator itself (the
//! Factor-13 equivalent for a surface that cannot be a committed static file). Regenerate it with
//! `UPDATE_GOLDEN=1 cargo test -p sutra-openapi golden` after an intentional generator change.

use std::sync::Arc;

use serde_json::Value;
use sutra_bpmn::model::ProcessModule;
use sutra_bpmn::BpmnModelLoader;
use sutra_channels::{load_channel_definitions, ChannelDefinition};
use sutra_datastore::{parse_datastores, StoreDefinition};

use super::*;

const CHANNELS_YAML: &[u8] = br#"
channels:
  - name: balance
    transport: http
    codec: urn:sutra:codec:json
  - name: balance-response
    direction: outbound
    transport: http
    bind: "http://localhost:8080/callback"
"#;

const FLOW_BPMN: &[u8] = br##"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_fixture"
                  targetNamespace="urn:sutra:module:mini-package:1.0.0">
  <bpmn:dataStore id="accountsStore" name="accounts"/>
  <bpmn:process id="balance-query" name="Balance query" isExecutable="true">
    <bpmn:dataObject id="doAccount" name="account"/>
    <bpmn:dataStoreReference id="dsrAccount" name="accounts" dataStoreRef="accountsStore">
      <bpmn:extensionElements><q:store key="payload.accountId"/></bpmn:extensionElements>
    </bpmn:dataStoreReference>
    <bpmn:startEvent id="Start" name="BalanceQuery received">
      <bpmn:extensionElements>
        <q:source channel="balance" messageTypeValue="BalanceQuery"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>B1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="B1" sourceRef="Start" targetRef="Load"/>
    <bpmn:serviceTask id="Load" name="Load account (read-only)">
      <bpmn:incoming>B1</bpmn:incoming><bpmn:outgoing>B2</bpmn:outgoing>
      <bpmn:dataInputAssociation>
        <bpmn:sourceRef>dsrAccount</bpmn:sourceRef>
        <bpmn:targetRef>doAccount</bpmn:targetRef>
      </bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="B2" sourceRef="Load" targetRef="Reply"/>
    <bpmn:serviceTask id="Reply" name="Reply Balance" implementation="reply.hbs">
      <bpmn:extensionElements>
        <q:reply mode="native" contentType="application/xml"/>
      </bpmn:extensionElements>
      <bpmn:incoming>B2</bpmn:incoming><bpmn:outgoing>B3</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="B3" sourceRef="Reply" targetRef="End"/>
    <bpmn:endEvent id="End" name="Done"><bpmn:incoming>B3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"##;

struct Fixture {
    channels: Vec<ChannelDefinition>,
    modules: Vec<Arc<ProcessModule>>,
    stores: Vec<StoreDefinition>,
}

fn mini_package() -> Fixture {
    let channels = load_channel_definitions(
        CHANNELS_YAML,
        "acme",
        "mini-package",
        "1.0.0",
        "channels.yaml",
    )
    .expect("channels parse");
    let module = BpmnModelLoader::new().load(FLOW_BPMN).expect("bpmn parse");
    let stores = parse_datastores("").unwrap_or_default();
    Fixture {
        channels,
        modules: vec![Arc::new(module)],
        stores,
    }
}

fn mini_package_spec() -> Value {
    let f = mini_package();
    deployment_spec(&DeploymentApi {
        deployment_id: "dep-000000000000000000000001",
        tenant: "acme",
        module: "mini-package",
        version: "1.0.0",
        channels: &f.channels,
        modules: &f.modules,
        stores: &f.stores,
    })
}

#[test]
fn http_inbound_channel_becomes_a_path_item() {
    let spec = mini_package_spec();
    let op = &spec["paths"]["/channels/balance"]["post"];
    assert_eq!(op["x-sutra-channel"], "balance");
    assert_eq!(op["x-sutra-nature"], "sync-request-reply");
    assert_eq!(op["x-sutra-transport"], "http");
    assert_eq!(op["x-sutra-codec"], "urn:sutra:codec:json");
    // Reachable BPMN: balance-query, start event Start, message type BalanceQuery.
    let reach = op["x-sutra-reachable-processes"].as_array().unwrap();
    assert_eq!(reach.len(), 1);
    assert_eq!(reach[0]["processId"], "balance-query");
    assert_eq!(reach[0]["startEventId"], "Start");
    assert_eq!(reach[0]["messageTypeValue"], "BalanceQuery");
    assert_eq!(op["x-sutra-inbound-message-types"][0], "BalanceQuery");
    // A synchronous endpoint replies 200.
    assert!(op["responses"]["200"].is_object());
}

#[test]
fn reply_binding_surfaces_as_an_emission() {
    let spec = mini_package_spec();
    let emits = spec["paths"]["/channels/balance"]["post"]["x-sutra-reachable-processes"][0]
        ["emits"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        emits.iter().any(|e| e["via"] == "reply"
            && e["contentType"] == "application/xml"
            && e["mode"] == "native"),
        "expected a native reply emission with application/xml, got {emits:?}"
    );
}

#[test]
fn outbound_channel_is_described_not_a_path() {
    let spec = mini_package_spec();
    // The outbound channel must NOT be an inbound path.
    assert!(spec["paths"].get("/callback").is_none());
    let outbound = spec["x-sutra-outbound"].as_array().unwrap();
    let entry = outbound
        .iter()
        .find(|e| e["channel"] == "balance-response")
        .expect("balance-response outbound entry");
    assert_eq!(entry["direction"], "outbound");
    assert_eq!(entry["destination"], "http://localhost:8080/callback");
}

#[test]
fn datastore_inventory_lists_process_referenced_stores() {
    let spec = mini_package_spec();
    let stores = spec["x-sutra-datastores"].as_array().unwrap();
    let accounts = stores
        .iter()
        .find(|s| s["name"] == "accounts")
        .expect("accounts store in inventory");
    // Referenced by the process (a `<q:store>` read) but not declared in datastores.yaml.
    assert_eq!(accounts["declared"], false);
    assert!(accounts["access"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "read"));
    assert!(accounts["keyExpressions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v == "payload.accountId"));
}

#[test]
fn spec_is_openapi_31() {
    let spec = mini_package_spec();
    assert_eq!(spec["openapi"], OPENAPI_VERSION);
    assert_eq!(
        spec["info"]["x-sutra-deployment-id"],
        "dep-000000000000000000000001"
    );
    assert!(spec["paths"].is_object());
}

#[test]
fn yaml_render_has_no_arbitrary_precision_artifacts() {
    // A number in the tree (concurrency cap style) must render as a plain scalar, never
    // serde_json's arbitrary_precision private Number marker.
    let v = serde_json::json!({ "cap": 42, "nested": { "n": 7 } });
    let yaml = render_yaml(&v);
    assert!(
        !yaml.contains("$serde_json::private::Number"),
        "yaml: {yaml}"
    );
    assert!(yaml.contains("cap: 42"), "yaml: {yaml}");
}

#[test]
fn generation_is_deterministic() {
    assert_eq!(
        render_yaml(&mini_package_spec()),
        render_yaml(&mini_package_spec())
    );
}

#[test]
fn matches_golden() {
    let golden_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/mini-package.openapi.yaml");
    let generated = render_yaml(&mini_package_spec());
    if std::env::var("UPDATE_GOLDEN").is_ok() {
        std::fs::create_dir_all(golden_path.parent().unwrap()).unwrap();
        std::fs::write(&golden_path, &generated).unwrap();
        return;
    }
    let golden = std::fs::read_to_string(&golden_path).unwrap_or_else(|_| {
        panic!(
            "missing golden {}; regenerate with UPDATE_GOLDEN=1 cargo test -p sutra-openapi golden",
            golden_path.display()
        )
    });
    assert_eq!(
        generated, golden,
        "generated spec drifted from the golden; if intentional, regenerate with UPDATE_GOLDEN=1"
    );
}
