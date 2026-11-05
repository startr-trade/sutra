//! Endpoint exclusivity at deploy time, end to end: real packages through the real deploy paths
//! into a real engine. An HTTP route, a queue or a subscription belongs to one deployment and a
//! second claimant is refused at deploy, not at boot, with the first still serving. A topic is
//! broadcast, so every subscriber receives its own copy. A data store that would share rows only
//! warns.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::archive_activation_conformance::{
    boot_archive_engine, boot_db_engine, boot_db_engine_with_pools, capture_server, fresh_db,
    http_request, package, package_yaml, place_archive, post, temp_root, wait_until, write,
    API_KEY,
};
use crate::leadership_it::{consumer_count, declare_queue, rabbit_port, raw_connection};

const AUTH: &str = "    auth:\n      scheme: apikey\n      apikey:\n        value: flip-key\n        header: X-Api-Key\n";

/// A package whose process starts on `channel` and answers `POST <path>` with `marker`.
fn http_package(module: &str, channel: &str, path: &str, marker: &str) -> PathBuf {
    assert_eq!(API_KEY, "flip-key", "AUTH spells the fixture's key");
    let root = temp_root("claims-pkg");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_ping"
                  targetNamespace="urn:sutra:module:{module}:1.0.0">
  <bpmn:process id="ping" name="Ping" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="{channel}"/></bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="Reply"/>
    <bpmn:serviceTask id="Reply" name="Render the marker" implementation="marker.hbs">
      <bpmn:extensionElements><q:reply mode="native" contentType="text/plain"/></bpmn:extensionElements>
      <bpmn:incoming>F1</bpmn:incoming><bpmn:outgoing>F2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="F2" sourceRef="Reply" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>F2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let channels = format!(
        "channels:\n  - name: {channel}\n    transport: http\n    bind: \"POST {path}\"\n{AUTH}"
    );
    write(&root, "bpmn/ping.bpmn", &bpmn);
    write(&root, "templates/marker.hbs", marker);
    write(&root, "channels.yaml", &channels);
    write(&root, "package.yaml", &package_yaml("t1", module));
    root
}

/// A package whose process starts on one inbound channel described by `channel_yaml` and reports
/// that it ran by sending to `http://<sink>/<module>-done`.
fn inbound_package(module: &str, channel: &str, channel_yaml: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("claims-pkg");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_in"
                  targetNamespace="urn:sutra:module:{module}:1.0.0">
  <bpmn:process id="in" name="Inbound" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="{channel}"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Done"/>
    <bpmn:sendTask id="Done" name="Report">
      <bpmn:extensionElements><q:send destination="http://{sink}/{module}-done"/></bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Done" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    write(&root, "bpmn/in.bpmn", &bpmn);
    write(
        &root,
        "channels.yaml",
        &format!("channels:\n  - name: {channel}\n{channel_yaml}"),
    );
    write(&root, "package.yaml", &package_yaml("t1", module));
    root
}

/// `root`'s package relabelled.
fn relabelled(root: PathBuf, labels: &str) -> PathBuf {
    write(
        &root,
        "package.yaml",
        &format!("labels:\n{labels}engine:\n  minContract: 1\n"),
    );
    root
}

/// What `/sutra/deployments` says.
async fn status_of(addr: SocketAddr) -> serde_json::Value {
    let (_, body) = get(addr, "/sutra/deployments").await;
    json_of(&body)
}

fn failed_slots(status: &serde_json::Value) -> Vec<String> {
    let mut slots: Vec<String> = status["failed"]
        .as_array()
        .map(|failed| {
            failed
                .iter()
                .filter_map(|f| f["slot"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    slots.sort();
    slots
}

fn json_of(body: &str) -> serde_json::Value {
    serde_json::from_str(body.trim()).unwrap_or_else(|e| panic!("json body ({e}): {body}"))
}

async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || http_request(addr, "GET", &path, b""))
        .await
        .expect("get task")
}

async fn delete(addr: SocketAddr, path: &str) -> (u16, String) {
    let path = path.to_string();
    tokio::task::spawn_blocking(move || http_request(addr, "DELETE", &path, b""))
        .await
        .expect("delete task")
}

/// How many rows the store holds as `active`: a refusal must not have stored one.
async fn active_rows(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM deployment_archive WHERE status = 'active'")
        .fetch_one(pool)
        .await
        .expect("count active rows")
}

// ---- the db source: POST /admin/deployments ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn two_packages_on_one_http_path_are_refused_at_deploy_not_at_boot() {
    let (pool, url) = fresh_db().await;
    let first = package(&http_package("first", "ping", "/shared/ping", "first"));
    let second = package(&http_package("second", "pong", "/shared/ping", "second"));

    let engine = boot_db_engine(url.clone()).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &first).await;
    assert_eq!(status, 200, "first deploy: {body}");
    assert_eq!(
        post(addr, "/shared/ping", b"{}").await,
        (200, "first".into())
    );

    // The fast answer changes nothing and says the same thing the deploy will.
    let (status, body) = post(addr, "/admin/deployments/check", &second).await;
    assert_eq!(status, 200, "a verdict is a 200: {body}");
    let verdict = json_of(&body);
    assert_eq!(verdict["ok"], false);
    assert_eq!(verdict["collisions"][0]["endpoint"], "POST /shared/ping");
    assert_eq!(verdict["collisions"][0]["holder"], "t1--first--1.0.0");
    assert_eq!(verdict["collisions"][0]["claimant"], "t1--second--1.0.0");

    // The second package names a taken route: refused at deploy, with its code.
    let (status, body) = post(addr, "/admin/deployments", &second).await;
    assert_eq!(status, 409, "second deploy: {body}");
    let refusal = json_of(&body);
    assert_eq!(refusal["code"], "SUTRA.DEPLOY.ENDPOINT.COLLISION");
    let message = refusal["error"].as_str().unwrap();
    assert!(message.contains("POST /shared/ping"), "{message}");
    assert!(message.contains("t1--first--1.0.0"), "{message}");

    // Old state intact: nothing stored, the first still answers on the same route.
    assert_eq!(active_rows(&pool).await, 1);
    assert_eq!(
        post(addr, "/shared/ping", b"{}").await,
        (200, "first".into())
    );
    let (status, _) = get(addr, "/sutra/health/ready").await;
    assert_eq!(status, 200);
    engine.shutdown().await;

    // A restart sees one deployment and no reason to refuse to boot.
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    assert_eq!(
        post(addr, "/shared/ping", b"{}").await,
        (200, "first".into())
    );

    // The route is free once its holder has gone: the same archive now lands.
    let (status, body) = delete(addr, "/admin/deployments/t1--first--1.0.0").await;
    assert_eq!(status, 200, "undeploy: {body}");
    let (status, body) = post(addr, "/admin/deployments", &second).await;
    assert_eq!(status, 200, "second deploy once the route is free: {body}");
    assert_eq!(
        post(addr, "/shared/ping", b"{}").await,
        (200, "second".into())
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_hot_deploy_into_its_own_slot_is_not_a_collision_with_itself() {
    let (_pool, url) = fresh_db().await;
    let v1 = package(&http_package("app", "ping", "/app/ping", "v1"));
    let v2 = package(&http_package("app", "ping", "/app/ping", "v2"));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2 replaces v1 in its slot: {body}");
    assert_eq!(post(addr, "/app/ping", b"{}").await, (200, "v2".into()));
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn two_packages_on_one_queue_are_refused_and_the_first_keeps_consuming() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let port = rabbit_port();
    let queue = format!("claims-{}", std::process::id());
    declare_queue(port, &queue).await;
    std::env::set_var("E3_RABBIT_USER", "guest");
    std::env::set_var("E3_RABBIT_PASSWORD", "guest");
    let on_queue = format!(
        "    transport: rabbitmq\n    codec: json\n    host: 127.0.0.1\n    port: {port}\n    \
         queue: {queue}\n    username: env:E3_RABBIT_USER\n    password: env:E3_RABBIT_PASSWORD\n"
    );
    let first = package(&inbound_package("first", "first-in", &on_queue, sink));
    let second = package(&inbound_package("second", "second-in", &on_queue, sink));

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &first).await;
    assert_eq!(status, 200, "first deploy: {body}");
    assert!(
        wait_until(30, || {
            let queue = queue.clone();
            async move { consumer_count(port, &queue).await == 1 }
        })
        .await,
        "the first deployment consumes the queue"
    );

    let (status, body) = post(addr, "/admin/deployments", &second).await;
    assert_eq!(status, 409, "second deploy: {body}");
    assert_eq!(json_of(&body)["code"], "SUTRA.DEPLOY.ENDPOINT.COLLISION");
    assert!(body.contains(&queue), "the refusal names the queue: {body}");
    assert_eq!(active_rows(&pool).await, 1);
    assert_eq!(consumer_count(port, &queue).await, 1, "no second consumer");

    // The real cycle on the old state: a message on the queue runs the first package only.
    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    channel
        .basic_publish(
            "",
            &queue,
            lapin::options::BasicPublishOptions::default(),
            br#"{"n":1}"#,
            lapin::BasicProperties::default()
                .with_message_id("m1".into())
                .with_content_type("application/json".into()),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
    connection.close(200, "published").await.ok();
    assert!(
        wait_until(30, || async { capture.delivered("/first-done") >= 1 }).await,
        "the first deployment still serves the queue"
    );
    assert_eq!(capture.delivered("/second-done"), 0);
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn two_packages_on_one_topic_are_allowed_and_each_receives_its_own_copy() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let on_topic = "    transport: dapr\n    codec: json\n    topic: shared.events\n";
    let first = package(&inbound_package("first", "first-in", on_topic, sink));
    let second = package(&inbound_package("second", "second-in", on_topic, sink));

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &first).await;
    assert_eq!(status, 200, "first deploy: {body}");
    let (status, body) = post(addr, "/admin/deployments", &second).await;
    assert_eq!(status, 200, "second deploy on the same topic: {body}");
    assert_eq!(active_rows(&pool).await, 2);

    let (status, body) = post(addr, "/dapr/shared.events", br#"{"n":1}"#).await;
    assert_eq!(status, 200, "the push: {body}");
    assert!(
        wait_until(30, || async {
            capture.delivered("/first-done") == 1 && capture.delivered("/second-done") == 1
        })
        .await,
        "each subscriber received its own copy"
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_shared_data_store_connection_warns_and_is_never_refused() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var("E3_SHARED_DB_URL", &url);
    std::env::set_var("E3_OTHER_NAME_SAME_DB_URL", &url);
    let with_stores = |module: &str, path: &str, connection_ref: &str, store: &str| {
        let root = http_package(module, "ping", path, module);
        write(
            &root,
            "datastores.yaml",
            &format!(
                "datastores:\n  - name: {store}\n    type: sql\n    sql:\n      url-ref: {connection_ref}\n"
            ),
        );
        package(&root)
    };
    let first = with_stores("first", "/first/ping", "env:E3_SHARED_DB_URL", "accounts");
    // Another reference name, and the same database: the engine compares what they resolve to.
    let second = with_stores(
        "second",
        "/second/ping",
        "env:E3_OTHER_NAME_SAME_DB_URL",
        "accounts",
    );
    let third = with_stores("third", "/third/ping", "env:E3_SHARED_DB_URL", "ledger");

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &first).await;
    assert_eq!(status, 200, "first: {body}");
    assert!(json_of(&body).get("warnings").is_none(), "alone: {body}");

    let (status, body) = post(addr, "/admin/deployments", &second).await;
    assert_eq!(status, 200, "a shared connection is never refused: {body}");
    let warning = &json_of(&body)["warnings"][0];
    assert_eq!(warning["code"], "SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION");
    let message = warning["message"].as_str().unwrap();
    assert!(message.contains("t1--first--1.0.0"), "{message}");
    assert!(message.contains("accounts"), "{message}");
    assert!(!message.contains("postgres://"), "no connection: {message}");
    assert_eq!(
        post(addr, "/second/ping", b"{}").await,
        (200, "second".into())
    );

    // One connection and another store name share no rows.
    let (status, body) = post(addr, "/admin/deployments", &third).await;
    assert_eq!(status, 200, "third: {body}");
    assert!(json_of(&body).get("warnings").is_none(), "{body}");
    engine.shutdown().await;
}

// ---- versions of one tenant and module: one owner ---------------------------------------------

/// A package that parks on `hold-start` until a correlated `hold-response`, then reports through
/// `http://<sink>/<version>-done`, so which version's model ran is observable. New requests are
/// answered by `<version>` on `POST /owned/ping`.
pub(crate) fn versioned_hold_package(
    tenant: &str,
    module: &str,
    version: &str,
    sink: SocketAddr,
) -> PathBuf {
    let root = temp_root("claims-pkg");
    let hold = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_hold"
                  targetNamespace="urn:sutra:module:{module}:{version}">
  <bpmn:process id="hold" name="Hold" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="hold-start"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="CallOut"/>
    <bpmn:serviceTask id="CallOut" name="Call the partner" implementation="channel:callout">
      <bpmn:extensionElements>
        <q:source channel="hold-response"/>
        <q:alias name="holdKey" expression="payload.key" unique="true"/>
        <q:timeout duration="PT120S"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming>
      <bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="CallOut" targetRef="Done"/>
    <bpmn:sendTask id="Done" name="Report">
      <bpmn:extensionElements><q:send destination="http://{sink}/{version}-done"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Done" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let ping = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_ping"
                  targetNamespace="urn:sutra:module:{module}:{version}">
  <bpmn:process id="ping" name="Ping" isExecutable="true">
    <bpmn:startEvent id="PingStart">
      <bpmn:extensionElements><q:source channel="ping"/></bpmn:extensionElements>
      <bpmn:outgoing>p1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="p1" sourceRef="PingStart" targetRef="Reply"/>
    <bpmn:serviceTask id="Reply" name="Render the marker" implementation="marker.hbs">
      <bpmn:extensionElements><q:reply mode="native" contentType="text/plain"/></bpmn:extensionElements>
      <bpmn:incoming>p1</bpmn:incoming><bpmn:outgoing>p2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="p2" sourceRef="Reply" targetRef="PingEnd"/>
    <bpmn:endEvent id="PingEnd"><bpmn:incoming>p2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    let inbound = |name: &str, path: &str| {
        format!("  - name: {name}\n    transport: http\n    bind: \"POST {path}\"\n    codec: json\n{AUTH}")
    };
    let channels = format!(
        "channels:\n{}{}{}  - name: callout\n    direction: outbound\n    transport: http\n    bind: \"http://{sink}/callout\"\n",
        inbound("hold-start", "/owned/hold-start"),
        inbound("hold-response", "/owned/hold-response"),
        inbound("ping", "/owned/ping"),
    );
    write(&root, "bpmn/hold.bpmn", &hold);
    write(&root, "bpmn/ping.bpmn", &ping);
    write(&root, "templates/marker.hbs", version);
    write(&root, "channels.yaml", &channels);
    write(
        &root,
        "package.yaml",
        &format!(
            "labels:\n  \"tenant\": \"{tenant}\"\n  \"module\": \"{module}\"\n  \"version\": \"{version}\"\nengine:\n  minContract: 1\n"
        ),
    );
    root
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_new_version_takes_the_route_and_the_parked_old_instance_still_completes() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let v1 = package(&versioned_hold_package("t1", "app", "1.0.0", sink));
    let v2 = package(&versioned_hold_package("t1", "app", "2.0.0", sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;

    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "1.0.0".into())
    );

    // Park an instance on v1: its channel call has gone out, and it waits for the response.
    let (status, body) = post(addr, "/owned/hold-start", br#"{"key":"K1"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    assert!(wait_until(10, || async { capture.delivered("/callout") >= 1 }).await);

    // v2 on the same routes, in another slot: one owner, so it takes them at the flip.
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2 takes the route: {body}");
    let warning = &json_of(&body)["warnings"][0];
    assert_eq!(warning["code"], "SUTRA.DEPLOY.ENDPOINT.SUPERSEDED");
    assert!(warning["message"]
        .as_str()
        .unwrap()
        .contains("t1--app--1.0.0"));

    // New requests reach v2.
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "2.0.0".into())
    );
    assert_eq!(
        active_rows(&pool).await,
        1,
        "v1 was demoted with v2's commit"
    );
    let (_, status_body) = get(addr, "/sutra/deployments").await;
    let deployments = json_of(&status_body);
    assert_eq!(deployments["active"][0]["slot"], "t1--app--2.0.0");
    assert_eq!(deployments["draining"].as_array().unwrap().len(), 1);

    // The parked instance resumes through the route v2 now serves, and finishes on v1's model.
    let (status, body) = post(
        addr,
        "/owned/hold-response",
        br#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "the response resumes the pinned instance: {body}"
    );
    assert!(
        wait_until(30, || async { capture.delivered("/1.0.0-done") >= 1 }).await,
        "the parked instance completes on v1"
    );
    assert_eq!(capture.delivered("/2.0.0-done"), 0, "never on v2's model");

    // Quiescent now, so v1 retires the way a replaced slot does.
    assert!(
        wait_until(30, || async {
            let (_, body) = get(addr, "/sutra/deployments").await;
            json_of(&body)["draining"]
                .as_array()
                .is_some_and(Vec::is_empty)
        })
        .await,
        "the drained v1 retires once nothing is pinned to it"
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_different_module_or_tenant_on_that_route_is_still_refused() {
    let (pool, url) = fresh_db().await;
    let (sink, _capture) = capture_server().await;
    let owner = package(&versioned_hold_package("t1", "app", "1.0.0", sink));
    let other_module = package(&versioned_hold_package("t1", "other", "2.0.0", sink));
    let other_tenant = package(&versioned_hold_package("t2", "app", "2.0.0", sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &owner).await;
    assert_eq!(status, 200, "owner: {body}");

    for (label, archive) in [("module", &other_module), ("tenant", &other_tenant)] {
        let (status, body) = post(addr, "/admin/deployments", archive).await;
        assert_eq!(status, 409, "another {label}: {body}");
        assert_eq!(json_of(&body)["code"], "SUTRA.DEPLOY.ENDPOINT.COLLISION");
    }
    assert_eq!(active_rows(&pool).await, 1);
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "1.0.0".into())
    );
    engine.shutdown().await;
}

// ---- the dir source: the folder watcher -------------------------------------------------------

/// The folder form of the takeover: the new version's archive lands beside the old one's, the old
/// one drains, and its file stays where the operator put it.
#[tokio::test(flavor = "multi_thread")]
async fn a_folder_lets_a_new_version_take_the_route_from_the_old_one() {
    let (sink, _capture) = capture_server().await;
    let v1 = package(&versioned_hold_package("t1", "app", "1.0.0", sink));
    let v2 = package(&versioned_hold_package("t1", "app", "2.0.0", sink));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-app-1.sutra", &v1);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "1.0.0".into())
    );

    // Sorts earlier than the holder: arrival decides at runtime, not the file name.
    place_archive(&dir, "a-app-2.sutra", &v2);
    assert!(
        wait_until(10, || async {
            post(addr, "/owned/ping", b"{}").await == (200, "2.0.0".into())
        })
        .await,
        "the new version takes the route"
    );
    let (_, body) = get(addr, "/sutra/deployments").await;
    let deployments = json_of(&body);
    assert_eq!(
        deployments["active"].as_array().unwrap().len(),
        1,
        "{deployments}"
    );
    assert_eq!(deployments["active"][0]["slot"], "a-app-2.sutra");
    assert!(
        deployments["failed"].as_array().unwrap().is_empty(),
        "{deployments}"
    );
    assert!(dir.join("z-app-1.sutra").exists());
    engine.shutdown().await;
}

/// Two archives on one route, no record of who holds it, no database to ask: nothing can rank them
/// so neither deploys, and everything else boots.
#[tokio::test(flavor = "multi_thread")]
async fn a_first_boot_with_two_claimants_deploys_neither_and_boots_the_rest() {
    let first = package(&http_package("first", "ping", "/shared/ping", "first"));
    let second = package(&http_package("second", "pong", "/shared/ping", "second"));
    let third = package(&http_package("third", "other", "/other", "third"));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "a-first.sutra", &first);
    place_archive(&dir, "b-second.sutra", &second);
    place_archive(&dir, "c-third.sutra", &third);

    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/other", b"{}").await, (200, "third".into()));
    assert_eq!(post(addr, "/shared/ping", b"{}").await.0, 404);
    assert!(
        wait_until(10, || async {
            let status = status_of(addr).await;
            failed_slots(&status) == ["a-first.sutra", "b-second.sutra"]
                && status["failed"][0]["error"].as_str().is_some_and(|e| {
                    e.starts_with("[SUTRA.DEPLOY.ENDPOINT.COLLISION]")
                        && e.contains("a-first.sutra")
                        && e.contains("b-second.sutra")
                })
        })
        .await,
        "both report Failed, naming the files: {}",
        status_of(addr).await
    );

    // One leaves: the other's bytes never changed, and it lands anyway.
    std::fs::remove_file(dir.join("a-first.sutra")).unwrap();
    assert!(
        wait_until(10, || async {
            post(addr, "/shared/ping", b"{}").await == (200, "second".into())
        })
        .await,
        "the route passes to the waiting archive"
    );
    engine.shutdown().await;
}

/// With no database nothing survives a restart, so a holder and the archive it refused are two
/// claimants with no record again. Fail closed, not by path order.
#[tokio::test(flavor = "multi_thread")]
async fn without_a_database_a_restart_with_a_refused_file_deploys_neither() {
    let first = package(&http_package("first", "ping", "/shared/ping", "first"));
    let second = package(&http_package("second", "pong", "/shared/ping", "second"));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-first.sutra", &first);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    place_archive(&dir, "a-second.sutra", &second);
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["a-second.sutra"]
        })
        .await
    );
    engine.shutdown().await;

    let engine = boot_archive_engine(dir.clone(), None).await;
    assert_eq!(post(engine.local_addr, "/shared/ping", b"{}").await.0, 404);
    engine.shutdown().await;
}

/// The record is what a restart reads, so the refused file cannot win
/// by sorting first, and two replicas booting together read the same answer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_folder_restart_with_a_refused_file_keeps_the_holder_on_every_replica() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let first = package(&http_package("first", "ping", "/shared/ping", "first"));
    let second = package(&http_package("second", "pong", "/shared/ping", "second"));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-first.sutra", &first);
    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    let addr = engine.local_addr;
    assert_eq!(
        post(addr, "/shared/ping", b"{}").await,
        (200, "first".into())
    );
    place_archive(&dir, "a-second.sutra", &second);
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["a-second.sutra"]
        })
        .await,
        "the newcomer is refused"
    );
    engine.shutdown().await;

    let (one, two) = tokio::join!(
        boot_archive_engine(dir.clone(), Some(url.clone())),
        boot_archive_engine(dir.clone(), Some(url.clone()))
    );
    for engine in [&one, &two] {
        assert_eq!(
            post(engine.local_addr, "/shared/ping", b"{}").await,
            (200, "first".into()),
            "the holder keeps the route across the restart"
        );
        assert!(
            wait_until(10, || async {
                failed_slots(&status_of(engine.local_addr).await) == ["a-second.sutra"]
            })
            .await,
            "the refused file stays refused"
        );
    }
    one.shutdown().await;
    two.shutdown().await;

    // The holder leaves while everything is down: the record is stale, and the other takes over.
    std::fs::remove_file(dir.join("z-first.sutra")).unwrap();
    let engine = boot_archive_engine(dir.clone(), Some(url)).await;
    assert_eq!(
        post(engine.local_addr, "/shared/ping", b"{}").await,
        (200, "second".into())
    );
    engine.shutdown().await;
}

/// `1.9.0` sorts after `1.10.0`, and so does the z- file holding it. Neither decides anything.
#[tokio::test(flavor = "multi_thread")]
async fn an_older_file_that_sorts_later_does_not_take_the_route_at_boot() {
    let (sink, _capture) = capture_server().await;
    let older = package(&versioned_hold_package("t1", "app", "1.9.0", sink));
    let newer = package(&versioned_hold_package("t1", "app", "1.10.0", sink));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "a-newer.sutra", &newer);
    place_archive(&dir, "z-older.sutra", &older);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "1.10.0".into())
    );

    // Touching the older file re-queues it, and still it does not take the route back.
    place_archive(&dir, "z-older.sutra", &older);
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "1.10.0".into())
    );
    let status = status_of(addr).await;
    assert_eq!(status["active"][0]["slot"], "a-newer.sutra", "{status}");
    assert!(failed_slots(&status).is_empty(), "{status}");
    engine.shutdown().await;
}

/// The restart a folder owes a superseded version: its file is there, its instances are parked, and
/// it must be registered to resume them. No route of its serves, and it finishes on its own model.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_parked_old_version_in_a_folder_still_resumes_after_a_restart() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let (sink, capture) = capture_server().await;
    let v1 = package(&versioned_hold_package("t1", "app", "1.0.0", sink));
    let v2 = package(&versioned_hold_package("t1", "app", "2.0.0", sink));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-app-1.sutra", &v1);
    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/owned/hold-start", br#"{"key":"K1"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    assert!(wait_until(10, || async { capture.delivered("/callout") >= 1 }).await);
    place_archive(&dir, "a-app-2.sutra", &v2);
    assert!(
        wait_until(10, || async {
            post(addr, "/owned/ping", b"{}").await == (200, "2.0.0".into())
        })
        .await
    );
    engine.shutdown().await;

    let engine = boot_archive_engine(dir.clone(), Some(url)).await;
    let addr = engine.local_addr;
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "2.0.0".into())
    );
    assert!(
        wait_until(10, || async {
            status_of(addr).await["draining"]
                .as_array()
                .is_some_and(|d| d.len() == 1)
        })
        .await,
        "the superseded version is registered to drain: {}",
        status_of(addr).await
    );
    let (status, body) = post(
        addr,
        "/owned/hold-response",
        br#"{"key":"K1","status":"done"}"#,
    )
    .await;
    assert_eq!(
        status, 200,
        "the response resumes the pinned instance: {body}"
    );
    assert!(
        wait_until(30, || async { capture.delivered("/1.0.0-done") >= 1 }).await,
        "the parked instance completes on v1"
    );
    assert_eq!(capture.delivered("/2.0.0-done"), 0, "never on v2's model");
    assert!(
        wait_until(30, || async {
            status_of(addr).await["draining"]
                .as_array()
                .is_some_and(Vec::is_empty)
        })
        .await,
        "quiescent now, so it retires, and its file stays"
    );
    assert!(dir.join("z-app-1.sutra").exists());
    engine.shutdown().await;
}

/// v2 serves and v1 stands aside. v2's file is then rewritten with content another owner's route
/// refuses, so the path falls back on what it served. Removing that file must bring v1 back, though
/// v1's bytes never changed, so only an engine that kept them can.
#[tokio::test(flavor = "multi_thread")]
async fn removing_a_refused_new_version_gives_the_route_back_to_the_old_one() {
    let labels_v2 = "  \"tenant\": \"t1\"\n  \"module\": \"app\"\n  \"version\": \"2.0.0\"\n";
    let v1 = package(&http_package("app", "ping", "/app/ping", "v1"));
    let other = package(&http_package("other", "taken", "/taken", "other"));
    let v2 = package(&relabelled(
        http_package("app", "ping", "/app/ping", "v2"),
        labels_v2,
    ));
    let refused_root = relabelled(
        http_package("app", "ping", "/app/ping", "v2-refused"),
        labels_v2,
    );
    let mut channels = std::fs::read_to_string(refused_root.join("channels.yaml")).unwrap();
    channels.push_str(&format!(
        "  - name: taken-too\n    transport: http\n    bind: \"POST /taken\"\n{AUTH}"
    ));
    write(&refused_root, "channels.yaml", &channels);
    let refused = package(&refused_root);

    let dir = temp_root("claims-dir");
    place_archive(&dir, "v1.sutra", &v1);
    place_archive(&dir, "other.sutra", &other);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;

    place_archive(&dir, "v2.sutra", &v2);
    assert!(
        wait_until(10, || async {
            post(addr, "/app/ping", b"{}").await == (200, "v2".into())
        })
        .await,
        "v2 takes the route"
    );
    place_archive(&dir, "v2.sutra", &refused);
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["v2.sutra"]
        })
        .await,
        "the rewrite is refused"
    );
    assert_eq!(post(addr, "/app/ping", b"{}").await, (200, "v2".into()));

    std::fs::remove_file(dir.join("v2.sutra")).unwrap();
    assert!(
        wait_until(10, || async {
            post(addr, "/app/ping", b"{}").await == (200, "v1".into())
        })
        .await,
        "the route is not left dark: {}",
        status_of(addr).await
    );
    assert!(failed_slots(&status_of(addr).await).is_empty());
    engine.shutdown().await;
}

/// The refusal the transports find, not the claim check: v2's auth reference does not resolve. It
/// used to win at the scan, fail at the flip, and leave nothing behind it when removed.
#[tokio::test(flavor = "multi_thread")]
async fn removing_a_new_version_that_cannot_serve_leaves_the_old_one_serving() {
    let v1 = package(&http_package("app", "ping", "/app/ping", "v1"));
    let v2_root = relabelled(
        http_package("app", "ping", "/app/ping", "v2"),
        "  \"tenant\": \"t1\"\n  \"module\": \"app\"\n  \"version\": \"2.0.0\"\n",
    );
    write(
        &v2_root,
        "channels.yaml",
        "channels:\n  - name: ping\n    transport: http\n    bind: \"POST /app/ping\"\n    \
         auth:\n      scheme: apikey\n      apikey:\n        value: env:E3_NEVER_SET_ANYWHERE\n        \
         header: X-Api-Key\n",
    );
    let v2 = package(&v2_root);
    let dir = temp_root("claims-dir");
    place_archive(&dir, "v1.sutra", &v1);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;

    place_archive(&dir, "v2.sutra", &v2);
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["v2.sutra"]
        })
        .await
    );
    assert_eq!(post(addr, "/app/ping", b"{}").await, (200, "v1".into()));
    engine.shutdown().await;

    // And a restart with it still there gives the same answer.
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/app/ping", b"{}").await, (200, "v1".into()));

    std::fs::remove_file(dir.join("v2.sutra")).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    assert_eq!(post(addr, "/app/ping", b"{}").await, (200, "v1".into()));
    engine.shutdown().await;
}

/// A restart would serve v1 once v2 is gone, so the running engine does as well.
#[tokio::test(flavor = "multi_thread")]
async fn removing_the_new_version_gives_the_route_back_to_the_old_one() {
    let (sink, _capture) = capture_server().await;
    let v1 = package(&versioned_hold_package("t1", "app", "1.0.0", sink));
    let v2 = package(&versioned_hold_package("t1", "app", "2.0.0", sink));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "v1.sutra", &v1);
    place_archive(&dir, "v2.sutra", &v2);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(
        post(addr, "/owned/ping", b"{}").await,
        (200, "2.0.0".into())
    );
    std::fs::remove_file(dir.join("v2.sutra")).unwrap();
    assert!(
        wait_until(10, || async {
            post(addr, "/owned/ping", b"{}").await == (200, "1.0.0".into())
        })
        .await,
        "the route is not left dark"
    );
    engine.shutdown().await;
}

/// Two archives swapping routes in one change. Judged as a whole set there is no collision at all.
#[tokio::test(flavor = "multi_thread")]
async fn two_archives_swapping_routes_both_land() {
    let dir = temp_root("claims-dir");
    place_archive(
        &dir,
        "a.sutra",
        &package(&http_package("a", "in-a", "/one", "a")),
    );
    place_archive(
        &dir,
        "b.sutra",
        &package(&http_package("b", "in-b", "/two", "b")),
    );
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/one", b"{}").await, (200, "a".into()));

    place_archive(
        &dir,
        "a.sutra",
        &package(&http_package("a", "in-a", "/two", "a")),
    );
    place_archive(
        &dir,
        "b.sutra",
        &package(&http_package("b", "in-b", "/one", "b")),
    );
    assert!(
        wait_until(10, || async {
            post(addr, "/one", b"{}").await == (200, "b".into())
                && post(addr, "/two", b"{}").await == (200, "a".into())
        })
        .await,
        "both land: {}",
        status_of(addr).await
    );
    assert!(failed_slots(&status_of(addr).await).is_empty());
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unlabeled_packages_do_not_count_as_one_owner() {
    let unlabeled = |name: &str, path: &str| {
        let root = http_package(name, "in", path, name);
        write(&root, "package.yaml", "engine:\n  minContract: 1\n");
        package(&root)
    };
    let dir = temp_root("claims-dir");
    place_archive(&dir, "one.sutra", &unlabeled("one", "/same"));
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/same", b"{}").await, (200, "one".into()));

    // It used to take the route and drain the holder, as if it were the next version.
    place_archive(&dir, "two.sutra", &unlabeled("two", "/same"));
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["two.sutra"]
        })
        .await,
        "refused: {}",
        status_of(addr).await
    );
    assert_eq!(post(addr, "/same", b"{}").await, (200, "one".into()));
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn labels_that_join_alike_are_not_one_owner() {
    let dir = temp_root("claims-dir");
    let one = relabelled(
        http_package("c", "in", "/same", "one"),
        "  \"tenant\": \"a--b\"\n  \"module\": \"c\"\n  \"version\": \"1.0.0\"\n",
    );
    let two = relabelled(
        http_package("b--c", "in", "/same", "two"),
        "  \"tenant\": \"a\"\n  \"module\": \"b--c\"\n  \"version\": \"2.0.0\"\n",
    );
    place_archive(&dir, "one.sutra", &package(&one));
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/same", b"{}").await, (200, "one".into()));
    place_archive(&dir, "two.sutra", &package(&two));
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["two.sutra"]
        })
        .await,
        "refused: {}",
        status_of(addr).await
    );
    engine.shutdown().await;
}

/// On a topic a version left beside its successor would process every event twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_version_on_a_topic_replaces_the_old_one_in_a_folder() {
    let (sink, _capture) = capture_server().await;
    let on_topic = "    transport: dapr\n    codec: json\n    topic: orders\n";
    let version = |v: &str| {
        relabelled(
            inbound_package("app", "in", on_topic, sink),
            &format!("  \"tenant\": \"t1\"\n  \"module\": \"app\"\n  \"version\": \"{v}\"\n"),
        )
    };
    let dir = temp_root("claims-dir");
    place_archive(&dir, "v1.sutra", &package(&version("1.0.0")));
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    place_archive(&dir, "v2.sutra", &package(&version("2.0.0")));
    assert!(
        wait_until(10, || async {
            let status = status_of(addr).await;
            status["active"].as_array().is_some_and(|a| a.len() == 1)
                && status["active"][0]["slot"] == "v2.sutra"
        })
        .await,
        "only v2 stays active: {}",
        status_of(addr).await
    );
    engine.shutdown().await;
}

/// An active row as an older engine could have left it, with no notification to wake a listener.
async fn insert_active_row(
    pool: &sqlx::PgPool,
    module: &str,
    version: &str,
    bytes: &[u8],
    age: &str,
) {
    let archive = sutra_loader::read_archive(bytes).unwrap();
    let id = archive.deployment.id.value().to_string();
    sqlx::query(&format!(
        "INSERT INTO deployment_archive (deployment_id, slot, tenant, module, version, status, \
         revision, bytes, checksum, activated_at) VALUES ($1, $2, 't1', $3, $4, 'active', 1, $5, \
         $1, now() - interval '{age}')"
    ))
    .bind(&id)
    .bind(format!("t1--{module}--{version}"))
    .bind(module)
    .bind(version)
    .bind(bytes)
    .execute(pool)
    .await
    .unwrap();
}

/// Rows an older engine let share an endpoint. They no longer stop the boot or block an
/// unrelated deploy: the earliest activated holds, and the later row stops being active.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn colliding_rows_settle_on_the_earliest_activated_and_block_nothing() {
    let (pool, url) = fresh_db().await;
    let engine = boot_db_engine(url.clone()).await;
    engine.shutdown().await;
    let earlier = package(&http_package("earlier", "ping", "/shared", "earlier"));
    let later = package(&http_package("later", "pong", "/shared", "later"));
    insert_active_row(&pool, "later", "1.0.0", &later, "1 day").await;
    insert_active_row(&pool, "earlier", "1.0.0", &earlier, "2 days").await;

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/shared", b"{}").await, (200, "earlier".into()));
    let status = status_of(addr).await;
    assert_eq!(failed_slots(&status), ["t1--later--1.0.0"], "{status}");
    assert_eq!(
        active_rows(&pool).await,
        1,
        "the later row is no longer active"
    );

    let unrelated = package(&http_package("unrelated", "free", "/free", "free"));
    let (code, body) = post(addr, "/admin/deployments", &unrelated).await;
    assert_eq!(code, 200, "an unrelated deploy goes through: {body}");
    assert_eq!(post(addr, "/free", b"{}").await, (200, "free".into()));
    engine.shutdown().await;
}

/// On the db source: both versions of one owner active, as an older engine could leave a topic.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn colliding_rows_of_one_owner_settle_on_the_highest_version() {
    let (pool, url) = fresh_db().await;
    let (sink, _capture) = capture_server().await;
    let engine = boot_db_engine(url.clone()).await;
    engine.shutdown().await;
    for version in ["1.10.0", "1.9.0"] {
        let bytes = package(&versioned_hold_package("t1", "app", version, sink));
        insert_active_row(&pool, "app", version, &bytes, "1 day").await;
    }
    let engine = boot_db_engine(url).await;
    assert_eq!(
        post(engine.local_addr, "/owned/ping", b"{}").await,
        (200, "1.10.0".into())
    );
    assert_eq!(active_rows(&pool).await, 1);
    engine.shutdown().await;
}

/// A deploy needs one connection, however small the pool. Waiters hold what they have and ask
/// for nothing more, so concurrent deploys cannot exhaust it. Two here because the deploy listener
/// holds one for as long as the engine runs, so a pool of one cannot run the db source at all.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn deploys_need_only_the_connection_holding_the_admission_lock() {
    let (_pool, url) = fresh_db().await;
    let engine = boot_db_engine_with_pools(
        url,
        sutra_engine::config::DatasourcePools {
            max_connections: 2,
            dispatch_max_connections: 2,
        },
    )
    .await;
    let addr = engine.local_addr;
    let archives: Vec<Vec<u8>> = (0..3)
        .map(|n| {
            package(&http_package(
                &format!("m{n}"),
                "in",
                &format!("/m{n}"),
                "ok",
            ))
        })
        .collect();
    let (one, two, three) = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        tokio::join!(
            post(addr, "/admin/deployments", &archives[0]),
            post(addr, "/admin/deployments", &archives[1]),
            post(addr, "/admin/deployments", &archives[2]),
        )
    })
    .await
    .expect("deploys do not block on their own lock");
    for (code, body) in [one, two, three] {
        assert_eq!(code, 200, "{body}");
    }
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_folder_refuses_a_later_claimant_and_the_first_keeps_serving() {
    let first = package(&http_package("first", "ping", "/shared/ping", "first"));
    let second = package(&http_package("second", "pong", "/shared/ping", "second"));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-first.sutra", &first);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(
        post(addr, "/shared/ping", b"{}").await,
        (200, "first".into())
    );

    // Sorts earlier than the holder, and still the holder keeps the route: it got there first.
    place_archive(&dir, "a-second.sutra", &second);
    assert!(
        wait_until(10, || async {
            let (_, body) = get(addr, "/sutra/deployments").await;
            json_of(&body)["failed"][0]["slot"] == "a-second.sutra"
        })
        .await,
        "the newcomer is refused"
    );
    assert_eq!(
        post(addr, "/shared/ping", b"{}").await,
        (200, "first".into())
    );
    engine.shutdown().await;
}

/// The hot-flip defect: a route set that could not be built only warned, the registries flipped
/// anyway, and the new deployment reported Active with the old routes still served.
#[tokio::test(flavor = "multi_thread")]
async fn a_flip_whose_routes_cannot_build_is_refused_whole() {
    let v1 = package(&http_package("app", "ping", "/app/ping", "v1"));
    let v2_root = http_package("app", "ping", "/app/ping", "v2");
    write(
        &v2_root,
        "channels.yaml",
        "channels:\n  - name: ping\n    transport: http\n    bind: \"POST /app/ping\"\n    \
         auth:\n      scheme: apikey\n      apikey:\n        value: env:E3_NEVER_SET_ANYWHERE\n        \
         header: X-Api-Key\n",
    );
    let v2 = package(&v2_root);
    let dir = temp_root("claims-dir");
    place_archive(&dir, "app.sutra", &v1);
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/app/ping", b"{}").await, (200, "v1".into()));

    place_archive(&dir, "app.sutra", &v2);
    assert!(
        wait_until(10, || async {
            let (_, body) = get(addr, "/sutra/deployments").await;
            json_of(&body)["failed"][0]["slot"] == "app.sutra"
        })
        .await,
        "the flip that cannot land is reported failed"
    );
    let (_, body) = get(addr, "/sutra/deployments").await;
    let status = json_of(&body);
    assert_eq!(status["active"].as_array().unwrap().len(), 1, "{status}");
    assert_eq!(
        post(addr, "/app/ping", b"{}").await,
        (200, "v1".into()),
        "the old model and the old routes both still serve"
    );
    engine.shutdown().await;
}

// ---- what each transport claims ---------------------------------------------------------------

fn claims_of(channel_yaml: &str) -> Vec<sutra_channels::EndpointClaim> {
    let yaml = format!("channels:\n  - name: in\n{channel_yaml}");
    let definitions =
        sutra_channels::load_channel_definitions(yaml.as_bytes(), "t", "m", "1", "channels.yaml")
            .expect("channel yaml");
    let declared = sutra_transport_spi::declared_endpoints("id", "label", &definitions);
    assert!(declared.unchecked.is_empty(), "{:?}", declared.unchecked);
    declared.endpoints.into_iter().map(|e| e.claim).collect()
}

#[test]
fn queues_subscriptions_and_consumer_groups_are_exclusive_and_carry_their_broker() {
    use sutra_channels::EndpointClaim;
    let cases = [
        (
            "    transport: rabbitmq\n    host: b1\n    port: 5672\n    queue: q\n",
            "b1:5672 vhost / queue q",
        ),
        (
            "    transport: amqp\n    host: b1\n    port: 5672\n    queue: q\n",
            "b1:5672 queue q",
        ),
        (
            "    transport: aws-sqs\n    region: r1\n    queue.url: https://sqs/q\n",
            "endpoint default queue https://sqs/q",
        ),
        (
            "    transport: gcp-pubsub\n    project-id: p1\n    subscription: s\n",
            "project p1 endpoint default subscription s",
        ),
        (
            "    transport: rabbitmq\n    host: B2\n    port: 5672\n    queue: q\n",
            "b2:5672 vhost / queue q",
        ),
        (
            "    transport: file\n    spool.dir: /var/spool/in/\n",
            "spool /var/spool/in",
        ),
    ];
    for (yaml, identity) in cases {
        assert_eq!(
            claims_of(yaml),
            vec![EndpointClaim::exclusive(identity)],
            "{yaml}"
        );
    }
}

/// Spellings of one endpoint that used to pass as different ones.
#[test]
fn one_endpoint_has_one_identity_however_it_is_written() {
    let same = [
        // An omitted port is the default one, and TLS changes which that is.
        (
            "    transport: amqp\n    host: b1\n    queue: q\n",
            "    transport: amqp\n    host: b1\n    port: 5672\n    queue: q\n",
        ),
        (
            "    transport: amqp\n    host: b1\n    tls: true\n    queue: q\n",
            "    transport: amqp\n    host: b1\n    port: 5671\n    queue: q\n",
        ),
        // A group is one owner's on any topic, in any server order or host case.
        (
            "    transport: kafka\n    bootstrap.servers: b1:9092\n    topic: t1\n    group.id: g\n",
            "    transport: kafka\n    bootstrap.servers: b1:9092\n    topic: t2\n    group.id: g\n",
        ),
        (
            "    transport: kafka\n    bootstrap.servers: b1:9092,b2:9092\n    topic: t\n    group.id: g\n",
            "    transport: kafka\n    bootstrap.servers: B2:9092, b1:9092\n    topic: t\n    group.id: g\n",
        ),
        (
            "    transport: rabbitmq\n    host: Broker.Example\n    queue: q\n",
            "    transport: rabbitmq\n    host: broker.example\n    queue: q\n",
        ),
        // The URL carries the region.
        (
            "    transport: aws-sqs\n    region: r1\n    queue.url: https://sqs/q\n",
            "    transport: aws-sqs\n    region: r2\n    queue.url: HTTPS://SQS/q/\n",
        ),
        (
            "    transport: file\n    spool.dir: /a/b\n",
            "    transport: file\n    spool.dir: /a/./b//\n",
        ),
    ];
    for (one, other) in same {
        assert_eq!(claims_of(one), claims_of(other), "{one} / {other}");
        assert!(!claims_of(one).is_empty(), "{one}");
    }
    let different = [
        (
            "    transport: amqp\n    host: b1\n    tls: true\n    queue: q\n",
            "    transport: amqp\n    host: b1\n    queue: q\n",
        ),
        (
            "    transport: kafka\n    bootstrap.servers: b1\n    topic: t\n    group.id: g1\n",
            "    transport: kafka\n    bootstrap.servers: b1\n    topic: t\n    group.id: g2\n",
        ),
        // A case difference in a queue name is a different queue.
        (
            "    transport: rabbitmq\n    host: b1\n    queue: Q\n",
            "    transport: rabbitmq\n    host: b1\n    queue: q\n",
        ),
        // Removing `..` lexically could name another directory through a symlink.
        (
            "    transport: file\n    spool.dir: /a/link/../b\n",
            "    transport: file\n    spool.dir: /a/b\n",
        ),
        // One topic name on two pub/sub components is two topics.
        (
            "    transport: dapr\n    topic: x\n    pubsub.name: bus-a\n",
            "    transport: dapr\n    topic: x\n    pubsub.name: bus-b\n",
        ),
    ];
    for (one, other) in different {
        assert_ne!(claims_of(one), claims_of(other), "{one} / {other}");
    }
}

/// Identities reach refusals, logs and the holder record, and none of them may hold a password.
#[test]
fn an_inline_credential_does_not_reach_an_identity() {
    let claims = claims_of("    transport: rabbitmq\n    host: user:secret@b1\n    queue: q\n");
    assert!(!claims[0].identity.contains("secret"), "{claims:?}");
}

#[test]
fn a_channel_under_a_path_another_transport_serves_is_reported() {
    let yaml = "channels:\n  - name: in\n    transport: http\n    bind: \"POST /dapr/orders\"\n";
    let definitions =
        sutra_channels::load_channel_definitions(yaml.as_bytes(), "t", "m", "1", "channels.yaml")
            .expect("channel yaml");
    let declared = sutra_transport_spi::declared_endpoints("id", "label", &definitions);
    assert_eq!(declared.shadowed.len(), 1, "{:?}", declared.shadowed);
    assert_eq!(declared.shadowed[0].2, "dapr");
}

#[test]
fn only_topics_are_broadcast() {
    use sutra_channels::EndpointClaim;
    assert_eq!(
        claims_of("    transport: dapr\n    topic: x\n"),
        vec![EndpointClaim::broadcast("topic x")]
    );
    assert_eq!(
        claims_of("    transport: amqp\n    host: b1\n    port: 5672\n    topic: x\n"),
        vec![EndpointClaim::broadcast("b1:5672 topic x")]
    );
}

#[test]
fn an_unset_kafka_group_follows_the_channel_name() {
    use sutra_channels::EndpointClaim;
    assert_eq!(
        claims_of("    transport: kafka\n    bootstrap.servers: b1\n    topic: x\n"),
        vec![EndpointClaim::exclusive_to_new_deployments(
            "b1 group sutra-in",
            "Set a group.id of its own on each channel; an unset group.id defaults to \
             sutra-<channel>, so the same channel name in two deployments lands in one group",
        )]
    );
}

#[test]
fn an_outbound_channel_claims_nothing() {
    assert!(claims_of(
        "    transport: http\n    direction: outbound\n    bind: \"http://sink/x\"\n"
    )
    .is_empty());
}

// ---- an owner of its own: what a channel name and the holder rule imply -------------------------

fn unlabeled_http(name: &str, channel: &str, path: &str) -> Vec<u8> {
    let root = http_package(name, channel, path, name);
    write(&root, "package.yaml", "engine:\n  minContract: 1\n");
    package(&root)
}

/// One unlabeled package moved to a new route. Unlabeled packages are owners of their own, so it is
/// not the next version of the first, and the channel name they share can belong to only one. It
/// used to commit and then bring down the rebuild, on this replica and at every restart.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn an_unlabeled_redeploy_on_a_new_route_is_refused_and_the_engine_survives() {
    let (pool, url) = fresh_db().await;
    let engine = boot_db_engine(url.clone()).await;
    let addr = engine.local_addr;
    let (status, body) = post(
        addr,
        "/admin/deployments",
        &unlabeled_http("one", "in", "/a"),
    )
    .await;
    assert_eq!(status, 200, "first: {body}");
    let (status, body) = post(
        addr,
        "/admin/deployments",
        &unlabeled_http("two", "in", "/b"),
    )
    .await;
    assert_eq!(status, 409, "the second holds the channel name too: {body}");
    assert_eq!(json_of(&body)["code"], "SUTRA.DEPLOY.ENDPOINT.COLLISION");
    assert_eq!(active_rows(&pool).await, 1);
    assert_eq!(post(addr, "/a", b"{}").await, (200, "one".into()));
    engine.shutdown().await;

    let engine = boot_db_engine(url).await;
    assert_eq!(
        post(engine.local_addr, "/a", b"{}").await,
        (200, "one".into())
    );
    engine.shutdown().await;
}

/// Two rows that already share a channel name, as a replica of an earlier build could store: the
/// boot settles them on the earliest rather than reaching the registry with both.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn rows_sharing_a_channel_name_settle_on_the_earliest_and_the_boot_survives() {
    let (pool, url) = fresh_db().await;
    let engine = boot_db_engine(url.clone()).await;
    engine.shutdown().await;
    let (one, two) = (
        unlabeled_http("one", "in", "/a"),
        unlabeled_http("two", "in", "/b"),
    );
    for (bytes, age) in [(&one, "2 days"), (&two, "1 day")] {
        let id = sutra_loader::read_archive(bytes)
            .unwrap()
            .deployment
            .id
            .value()
            .to_string();
        sqlx::query(&format!(
            "INSERT INTO deployment_archive (deployment_id, slot, tenant, module, version, status, \
             revision, bytes, checksum, activated_at) VALUES ($1, $2, 'unlabeled', 'unlabeled', \
             'unlabeled', 'active', 1, $3, $1, now() - interval '{age}')"
        ))
        .bind(&id)
        .bind(format!("unlabeled--unlabeled--unlabeled--{id}"))
        .bind(bytes.as_slice())
        .execute(&pool)
        .await
        .unwrap();
    }
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/a", b"{}").await, (200, "one".into()));
    assert_eq!(post(addr, "/b", b"{}").await.0, 404);
    assert_eq!(active_rows(&pool).await, 1);
    engine.shutdown().await;
}

/// Unlabeled files, or two files of one labeled version, on one channel name and separate routes.
/// Nothing ranks them, so neither deploys and everything else boots. It used to be a failed boot.
#[tokio::test(flavor = "multi_thread")]
async fn files_sharing_a_channel_name_deploy_neither_and_boot_the_rest() {
    let dir = temp_root("claims-dir");
    place_archive(&dir, "a.sutra", &unlabeled_http("one", "in", "/a"));
    place_archive(&dir, "b.sutra", &unlabeled_http("two", "in", "/b"));
    place_archive(
        &dir,
        "c.sutra",
        &package(&http_package("same", "ch", "/c", "c")),
    );
    place_archive(
        &dir,
        "d.sutra",
        &package(&http_package("same", "ch", "/d", "d")),
    );
    place_archive(
        &dir,
        "e.sutra",
        &package(&http_package("third", "other", "/e", "third")),
    );
    let engine = boot_archive_engine(dir, None).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/e", b"{}").await, (200, "third".into()));
    assert_eq!(post(addr, "/a", b"{}").await.0, 404);
    assert_eq!(post(addr, "/c", b"{}").await.0, 404);
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["a.sutra", "b.sutra", "c.sutra", "d.sutra"]
        })
        .await,
        "{}",
        status_of(addr).await
    );
    engine.shutdown().await;
}

// ---- a pod that cannot verify the holder's file -------------------------------------------------

async fn holder_owner_of(pool: &sqlx::PgPool, identity: &str) -> Option<String> {
    sqlx::query_scalar("SELECT holder_label FROM deployment_endpoint_holder WHERE identity = $1")
        .bind(identity)
        .fetch_optional(pool)
        .await
        .expect("read the holder record")
}

/// One pod's copy of the holder's file does not verify. That pod must not hand the endpoint to the
/// archive it refused, or the whole fleet would follow the record. Only the file being gone does.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_pod_with_a_damaged_copy_of_the_holders_file_leaves_it_the_endpoint() {
    let (pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let first = package(&http_package("first", "ping", "/shared/ping", "first"));
    let second = package(&http_package("second", "pong", "/shared/ping", "second"));

    let healthy = temp_root("claims-dir");
    place_archive(&healthy, "z-first.sutra", &first);
    let serving = boot_archive_engine(healthy.clone(), Some(url.clone())).await;
    place_archive(&healthy, "a-second.sutra", &second);
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(serving.local_addr).await) == ["a-second.sutra"]
        })
        .await
    );

    let damaged = temp_root("claims-dir");
    place_archive(&damaged, "z-first.sutra", b"not an archive");
    place_archive(&damaged, "a-second.sutra", &second);
    let bad_pod = boot_archive_engine(damaged.clone(), Some(url.clone())).await;
    assert_eq!(
        post(bad_pod.local_addr, "/shared/ping", b"{}").await.0,
        404,
        "the refused archive stays refused on the pod that cannot judge the holder"
    );
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(
        holder_owner_of(&pool, "POST /shared/ping").await.as_deref(),
        Some("t1--first--1.0.0"),
        "the record still names the holder"
    );
    assert_eq!(
        post(serving.local_addr, "/shared/ping", b"{}").await,
        (200, "first".into()),
        "and the rest of the fleet keeps it"
    );
    serving.shutdown().await;

    // Gone is a different thing from unjudged.
    std::fs::remove_file(damaged.join("z-first.sutra")).unwrap();
    assert!(
        wait_until(10, || async {
            post(bad_pod.local_addr, "/shared/ping", b"{}").await == (200, "second".into())
        })
        .await,
        "once the file is gone the endpoint passes on"
    );
    bad_pod.shutdown().await;
}

/// The holder's archive reads fine and only its secret is missing on this pod. Its owner is known
/// then, so it counts as a claim, and a new pod reads the same answer as every other.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_pod_missing_the_holders_secret_leaves_it_the_endpoint() {
    let (pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let secret = "E3_HOLDER_POD_SECRET";
    std::env::set_var(secret, API_KEY);
    let root = http_package("first", "ping", "/shared/ping", "first");
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: ping\n    transport: http\n    bind: \"POST /shared/ping\"\n    \
             auth:\n      scheme: apikey\n      apikey:\n        value: env:{secret}\n        \
             header: X-Api-Key\n"
        ),
    );
    let first = package(&root);
    let second = package(&http_package("second", "pong", "/shared/ping", "second"));

    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-first.sutra", &first);
    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    assert_eq!(
        post(engine.local_addr, "/shared/ping", b"{}").await,
        (200, "first".into())
    );
    engine.shutdown().await;

    std::env::remove_var(secret);
    place_archive(&dir, "a-second.sutra", &second);
    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    assert_eq!(
        post(engine.local_addr, "/shared/ping", b"{}").await.0,
        404,
        "it does not serve the holder's route for it, and does not give it away"
    );
    assert_eq!(
        holder_owner_of(&pool, "POST /shared/ping").await.as_deref(),
        Some("t1--first--1.0.0")
    );
    engine.shutdown().await;

    std::env::set_var(secret, API_KEY);
    let engine = boot_archive_engine(dir, Some(url)).await;
    assert_eq!(
        post(engine.local_addr, "/shared/ping", b"{}").await,
        (200, "first".into()),
        "with its secret back the holder serves"
    );
    engine.shutdown().await;
    std::env::remove_var(secret);
}

/// A folder that cannot be listed says nothing about its files, so nothing drains.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_folder_that_cannot_be_listed_changes_nothing() {
    use std::os::unix::fs::PermissionsExt;
    let dir = temp_root("claims-dir");
    place_archive(
        &dir,
        "app.sutra",
        &package(&http_package("app", "ping", "/app/ping", "app")),
    );
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    let answer = post(addr, "/app/ping", b"{}").await;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(answer, (200, "app".into()));
    engine.shutdown().await;
}

// ---- a refused archive that used to serve -------------------------------------------------------

/// `POST <path>` parks on a timer, then reports `http://<sink>/<module>-done`.
fn timer_package(module: &str, path: &str, duration: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("claims-pkg");
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_timed"
                  targetNamespace="urn:sutra:module:{module}:1.0.0">
  <bpmn:process id="timed" name="Timed" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="timed-in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Pause"/>
    <bpmn:intermediateCatchEvent id="Pause" name="Timer">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:timerEventDefinition><bpmn:timeDuration>{duration}</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="Pause" targetRef="Done"/>
    <bpmn:sendTask id="Done" name="Report">
      <bpmn:extensionElements><q:send destination="http://{sink}/{module}-done"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Done" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    write(&root, "bpmn/timed.bpmn", &bpmn);
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: timed-in\n    transport: http\n    bind: \"POST {path}\"\n    codec: json\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &package_yaml("t1", module));
    root
}

/// The first boot of this engine on a folder an older engine let two archives share. Nothing records
/// who held the route, so neither deploys, but the instance parked on the one that used to serve
/// still has its definition to resume into.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_refused_archive_that_used_to_serve_still_resumes_after_a_restart() {
    let (pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let (sink, capture) = capture_server().await;
    let served = package(&timer_package("served", "/shared/timed", "PT4S", sink));
    let rival = package(&http_package("rival", "pong", "/shared/timed", "rival"));
    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-served.sutra", &served);
    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    let (status, body) = post(engine.local_addr, "/shared/timed", br#"{"n":1}"#).await;
    assert!(status < 300, "parked: {status} {body}");
    engine.shutdown().await;

    sqlx::query("DELETE FROM deployment_endpoint_holder")
        .execute(&pool)
        .await
        .unwrap();
    place_archive(&dir, "a-rival.sutra", &rival);

    let engine = boot_archive_engine(dir, Some(url)).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/shared/timed", b"{}").await.0, 404, "neither");
    assert!(
        wait_until(30, || async { capture.delivered("/served-done") >= 1 }).await,
        "the parked instance completes on its own definition: {}",
        status_of(addr).await
    );
    engine.shutdown().await;
}

// ---- redelivery after a partial failure ---------------------------------------------------------

/// A POST carrying `headers`.
async fn push(addr: SocketAddr, path: &str, headers: &[(&str, &str)], body: &[u8]) -> u16 {
    use std::io::{Read, Write};
    let head: String = headers
        .iter()
        .map(|(k, v)| format!("{k}: {v}\r\n"))
        .collect();
    let (path, body) = (path.to_string(), body.to_vec());
    tokio::task::spawn_blocking(move || {
        let mut stream = std::net::TcpStream::connect(addr).expect("connect");
        write!(
            stream,
            "POST {path} HTTP/1.1\r\nHost: localhost\r\n{head}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .expect("request head");
        stream.write_all(&body).expect("request body");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("response");
        response
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .expect("status code")
    })
    .await
    .expect("push task")
}

const DAPR_ON_TOPIC: &str = "    transport: dapr\n    topic: shared.events\n";
const KNATIVE_ON_SUBSCRIPTION: &str = "    transport: knative\n    subscription: shared-sub\n";

fn with_labels(root: PathBuf, module: &str, version: &str) -> PathBuf {
    relabelled(
        root,
        &format!(
            "  \"tenant\": \"t1\"\n  \"module\": \"{module}\"\n  \"version\": \"{version}\"\n"
        ),
    )
}

/// `accepting` takes the event, and `failing` caps its payload below it, so it answers non-2xx until
/// a fixed version replaces it. Sends `delivery` three times: twice while one
/// subscriber fails, and once it is fixed. Returns how many times `accepting` ran.
async fn fan_out_runs(
    transport: &str,
    route: &str,
    headers: &[(&str, &str)],
    delivery: &[u8],
    dedup_key: Option<&str>,
) -> u64 {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let accepting_root = inbound_package(
        "accepting",
        "accepting-in",
        &format!("{transport}    codec: json\n"),
        sink,
    );
    if let Some(key) = dedup_key {
        let bpmn = std::fs::read_to_string(accepting_root.join("bpmn/in.bpmn")).unwrap();
        write(
            &accepting_root,
            "bpmn/in.bpmn",
            &bpmn.replace(
                "<q:source channel=\"accepting-in\"/>",
                &format!("<q:source channel=\"accepting-in\" dedupKey=\"{key}\"/>"),
            ),
        );
    }
    let accepting = package(&accepting_root);
    let failing = package(&inbound_package(
        "failing",
        "failing-in",
        &format!("{transport}    codec: json\n    payload-cap-bytes: 1\n"),
        sink,
    ));
    let fixed = package(&with_labels(
        inbound_package(
            "failing",
            "fixed-in",
            &format!("{transport}    codec: json\n"),
            sink,
        ),
        "failing",
        "2.0.0",
    ));

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    for archive in [&accepting, &failing] {
        let (status, body) = post(addr, "/admin/deployments", archive).await;
        assert_eq!(status, 200, "deploy: {body}");
    }
    for attempt in 1..=2 {
        let status = push(addr, route, headers, delivery).await;
        assert!(status >= 300, "attempt {attempt} fails for one: {status}");
    }
    assert!(wait_until(30, || async { capture.delivered("/accepting-done") >= 1 }).await);

    let (status, body) = post(addr, "/admin/deployments", &fixed).await;
    assert_eq!(status, 200, "fix: {body}");
    let status = push(addr, route, headers, delivery).await;
    assert!(status < 300, "every subscriber accepts: {status}");
    assert!(
        wait_until(30, || async { capture.delivered("/failing-done") == 1 }).await,
        "the subscriber that failed runs once it can"
    );
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let runs = capture.delivered("/accepting-done") as u64;
    engine.shutdown().await;
    runs
}

fn cloud_event() -> Vec<u8> {
    br#"{"specversion":"1.0","id":"evt-1","source":"urn:test","type":"t.happened","datacontenttype":"application/json","data":{"n":1}}"#.to_vec()
}

/// With an event id and nothing declared: the transport hands the id over as explicit, and the inbox
/// of each subscriber absorbs what it has seen.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_dapr_redelivery_does_not_rerun_a_subscriber_that_accepted_a_cloud_event() {
    let runs = fan_out_runs(
        DAPR_ON_TOPIC,
        "/dapr/shared.events",
        &[("Content-Type", "application/cloudevents+json")],
        &cloud_event(),
        None,
    )
    .await;
    assert_eq!(runs, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_knative_redelivery_does_not_rerun_a_subscriber_that_accepted_a_cloud_event() {
    let runs = fan_out_runs(
        KNATIVE_ON_SUBSCRIPTION,
        "/knative/shared-sub",
        &[
            ("Content-Type", "application/json"),
            ("ce-id", "evt-1"),
            ("ce-source", "urn:test"),
            ("ce-type", "t.happened"),
            ("ce-specversion", "1.0"),
        ],
        br#"{"n":1}"#,
        None,
    )
    .await;
    assert_eq!(runs, 1);
}

/// A declared key drives the same dedup with no event id at all.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_declared_body_key_absorbs_a_redelivery_with_no_event_id() {
    let runs = fan_out_runs(
        DAPR_ON_TOPIC,
        "/dapr/shared.events",
        &[("Content-Type", "application/json")],
        br#"{"n":1}"#,
        Some("body.n"),
    )
    .await;
    assert_eq!(runs, 1);
}

/// No event id, no declared key: nothing tells a redelivery from a new event with the same body, so
/// it runs again. This is the limit the lint warning tells an author about.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn with_no_event_id_and_no_declared_key_a_redelivery_runs_again() {
    let runs = fan_out_runs(
        DAPR_ON_TOPIC,
        "/dapr/shared.events",
        &[("Content-Type", "application/json")],
        br#"{"n":1}"#,
        None,
    )
    .await;
    assert_eq!(runs, 3, "each delivery ran it");
}

const CE_HEADERS: [(&str, &str); 5] = [
    ("Content-Type", "application/json"),
    ("ce-id", "evt-1"),
    ("ce-source", "urn:test"),
    ("ce-type", "t.happened"),
    ("ce-specversion", "1.0"),
];

/// A declared `ce.id` is read at dispatch, so a channel told not to read CloudEvents dedupes on it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_declared_cloud_event_id_absorbs_a_knative_redelivery_with_cloud_events_off() {
    let runs = fan_out_runs(
        "    transport: knative\n    subscription: shared-sub\n    cloudevents-mode: none\n",
        "/knative/shared-sub",
        &CE_HEADERS,
        br#"{"n":1}"#,
        Some("ce.id"),
    )
    .await;
    assert_eq!(runs, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_declared_cloud_event_id_absorbs_a_dapr_redelivery_with_cloud_events_off() {
    let runs = fan_out_runs(
        "    transport: dapr\n    topic: shared.events\n    cloudevents-mode: none\n",
        "/dapr/shared.events",
        &[("Content-Type", "application/cloudevents+json")],
        &cloud_event(),
        Some("ce.id"),
    )
    .await;
    assert_eq!(runs, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_declared_header_absorbs_a_dapr_redelivery_with_no_key_header_on_the_channel() {
    let runs = fan_out_runs(
        DAPR_ON_TOPIC,
        "/dapr/shared.events",
        &[("Content-Type", "application/json"), ("X-Event", "e-1")],
        br#"{"n":1}"#,
        Some("header.X-Event"),
    )
    .await;
    assert_eq!(runs, 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_declared_header_absorbs_a_knative_redelivery_with_no_key_header_on_the_channel() {
    let runs = fan_out_runs(
        KNATIVE_ON_SUBSCRIPTION,
        "/knative/shared-sub",
        &[("Content-Type", "application/json"), ("X-Event", "e-1")],
        br#"{"n":1}"#,
        Some("header.X-Event"),
    )
    .await;
    assert_eq!(runs, 1);
}

/// Declared, and absent from the message: it runs as a delivery with no key does.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_redelivery_missing_its_declared_key_runs_again() {
    let runs = fan_out_runs(
        DAPR_ON_TOPIC,
        "/dapr/shared.events",
        &[("Content-Type", "application/json")],
        br#"{"n":1}"#,
        Some("header.X-Event"),
    )
    .await;
    assert_eq!(runs, 3);
}

/// `POST /keyed/start` parks at `First`, and each of two waits on `/keyed/answer` reports what it was.
/// The waits declare `dedup_key`, so a repeat of the first answer must not satisfy the second wait.
fn relay_package(dedup_key: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("claims-pkg");
    let source = format!(
        r#"<q:source channel="answer" name="answer" dedupKey="{dedup_key}"/><q:alias name="caseRef" expression="answer.ref"/>"#
    );
    let bpmn = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_keyed"
                  targetNamespace="urn:sutra:module:keyed:1.0.0">
  <bpmn:process id="keyed" name="Keyed" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="start"/>
        <q:alias name="caseRef" expression="payload.ref" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="First"/>
    <bpmn:receiveTask id="First" name="First answer">
      <bpmn:extensionElements>{source}</bpmn:extensionElements>
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f2" sourceRef="First" targetRef="Second"/>
    <bpmn:receiveTask id="Second" name="Second answer">
      <bpmn:extensionElements>{source}</bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Second" targetRef="Done"/>
    <bpmn:sendTask id="Done" name="Report">
      <bpmn:extensionElements><q:send destination="http://{sink}/keyed-done"/></bpmn:extensionElements>
      <bpmn:incoming>f3</bpmn:incoming><bpmn:outgoing>f4</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f4" sourceRef="Done" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f4</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>
"#
    );
    write(&root, "bpmn/keyed.bpmn", &bpmn);
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: start\n    transport: http\n    bind: \"POST /keyed/start\"\n    codec: json\n{AUTH}  - name: answer\n    transport: http\n    bind: \"POST /keyed/answer\"\n    codec: json\n    cloudevents-mode: none\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &package_yaml("t1", "keyed"));
    root
}

/// The wait reads its declared key too: a repeat of an answer is not the next answer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_declared_key_on_a_wait_stops_a_repeat_answering_the_next_wait() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(
        addr,
        "/admin/deployments",
        &package(&relay_package("header.X-Event", sink)),
    )
    .await;
    assert_eq!(status, 200, "deploy: {body}");
    let (status, body) = post(addr, "/keyed/start", br#"{"ref":"r-1"}"#).await;
    assert!(status < 300, "start: {status} {body}");
    let answer = |event: &'static str| async move {
        push(
            addr,
            "/keyed/answer",
            &[
                ("Content-Type", "application/json"),
                ("X-Api-Key", API_KEY),
                ("X-Event", event),
            ],
            br#"{"ref":"r-1"}"#,
        )
        .await
    };
    let status = answer("first").await;
    assert!(status < 300, "first answer: {status}");
    let status = answer("first").await;
    assert!(status < 300, "a repeat answers 2xx: {status}");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(
        capture.delivered("/keyed-done"),
        0,
        "the repeat did not satisfy the second wait"
    );
    let status = answer("second").await;
    assert!(status < 300, "second answer: {status}");
    assert!(wait_until(30, || async { capture.delivered("/keyed-done") == 1 }).await);
    engine.shutdown().await;
}

// ---- a consumer group already shared when the engine upgrades -----------------------------------

const DEFAULTED_GROUP: &str =
    "    transport: kafka\n    bootstrap.servers: 127.0.0.1:9\n    topic: TOPIC\n    codec: json\n";

fn grouped(module: &str, sink: SocketAddr) -> Vec<u8> {
    package(&inbound_package(
        module,
        "in",
        &DEFAULTED_GROUP.replace("TOPIC", module),
        sink,
    ))
}

fn active_slots(status: &serde_json::Value) -> Vec<String> {
    let mut slots: Vec<String> = status["active"]
        .as_array()
        .map(|active| {
            active
                .iter()
                .filter_map(|a| a["slot"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    slots.sort();
    slots
}

/// The status once the folder watcher has published it. A boot serves before the watcher's first
/// tick, and until that tick the status is empty, whatever the folder holds.
async fn first_status_of(addr: SocketAddr) -> serde_json::Value {
    let published = |status: &serde_json::Value| {
        !active_slots(status).is_empty() || !failed_slots(status).is_empty()
    };
    assert!(
        wait_until(10, || async { published(&status_of(addr).await) }).await,
        "the watcher never published a status: {}",
        status_of(addr).await
    );
    status_of(addr).await
}

/// Without a database: what a boot finds running goes on running. A new file is a new deploy.
#[tokio::test(flavor = "multi_thread")]
async fn a_group_shared_at_boot_keeps_running_and_a_new_claimant_is_refused() {
    let (sink, _capture) = capture_server().await;
    let dir = temp_root("claims-dir");
    place_archive(&dir, "a.sutra", &grouped("one", sink));
    place_archive(&dir, "b.sutra", &grouped("two", sink));
    let engine = boot_archive_engine(dir.clone(), None).await;
    let addr = engine.local_addr;
    let status = first_status_of(addr).await;
    assert_eq!(active_slots(&status), ["a.sutra", "b.sutra"], "{status}");
    assert!(failed_slots(&status).is_empty(), "{status}");

    place_archive(&dir, "c.sutra", &grouped("three", sink));
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["c.sutra"]
        })
        .await,
        "{}",
        status_of(addr).await
    );
    assert_eq!(active_slots(&status_of(addr).await), ["a.sutra", "b.sutra"]);
    engine.shutdown().await;
}

/// With the record, a restart still knows who was in and who was refused.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_shared_group_survives_a_folder_restart_and_the_refused_stays_refused() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let (sink, _capture) = capture_server().await;
    let dir = temp_root("claims-dir");
    place_archive(&dir, "a.sutra", &grouped("one", sink));
    place_archive(&dir, "b.sutra", &grouped("two", sink));
    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    let addr = engine.local_addr;
    assert_eq!(
        active_slots(&first_status_of(addr).await),
        ["a.sutra", "b.sutra"]
    );
    place_archive(&dir, "c.sutra", &grouped("three", sink));
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["c.sutra"]
        })
        .await
    );
    engine.shutdown().await;

    let engine = boot_archive_engine(dir, Some(url)).await;
    let addr = engine.local_addr;
    let status = first_status_of(addr).await;
    assert_eq!(active_slots(&status), ["a.sutra", "b.sutra"], "{status}");
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["c.sutra"]
        })
        .await
    );
    engine.shutdown().await;
}

/// Rows already active on one group keep running, undemoted, and a new deploy claiming it is refused.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn rows_sharing_a_group_stay_active_and_a_new_deploy_is_refused() {
    let (pool, url) = fresh_db().await;
    let (sink, _capture) = capture_server().await;
    let engine = boot_db_engine(url.clone()).await;
    engine.shutdown().await;
    insert_active_row(&pool, "one", "1.0.0", &grouped("one", sink), "2 days").await;
    insert_active_row(&pool, "two", "1.0.0", &grouped("two", sink), "1 day").await;

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let status = status_of(addr).await;
    assert_eq!(
        active_slots(&status),
        ["t1--one--1.0.0", "t1--two--1.0.0"],
        "{status}"
    );
    assert!(failed_slots(&status).is_empty(), "{status}");
    assert_eq!(active_rows(&pool).await, 2, "neither row was demoted");

    let (code, body) = post(addr, "/admin/deployments", &grouped("three", sink)).await;
    assert_eq!(code, 409, "a new claimant: {body}");
    assert_eq!(json_of(&body)["code"], "SUTRA.DEPLOY.ENDPOINT.COLLISION");

    let unrelated = package(&http_package("unrelated", "free", "/free", "free"));
    let (code, body) = post(addr, "/admin/deployments", &unrelated).await;
    assert_eq!(
        code, 200,
        "the flip is not blocked by what already shares: {body}"
    );
    assert_eq!(active_rows(&pool).await, 3);
    engine.shutdown().await;
}

// ---- what the holder record cannot hold ---------------------------------------------------------

/// What the record cannot store fails that archive alone. It used to fail the write for the whole
/// folder, and a boot with it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn an_identity_or_label_the_record_cannot_hold_fails_only_that_archive() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let long_path = format!("/{}", "x".repeat(3000));
    let long_label = "m".repeat(1100);
    let dir = temp_root("claims-dir");
    place_archive(
        &dir,
        "long-route.sutra",
        &package(&http_package("longroute", "long", &long_path, "x")),
    );
    place_archive(
        &dir,
        "nul-route.sutra",
        &package(&http_package("nulroute", "nul", "/a\\0b", "x")),
    );
    let long_module = http_package("longlabel", "lab", "/labelled", "x");
    write(
        &long_module,
        "package.yaml",
        &package_yaml("t1", &long_label),
    );
    place_archive(&dir, "long-label.sutra", &package(&long_module));
    place_archive(
        &dir,
        "good.sutra",
        &package(&http_package("good", "good", "/good", "good")),
    );

    let engine = boot_archive_engine(dir, Some(url)).await;
    let addr = engine.local_addr;
    assert_eq!(post(addr, "/good", b"{}").await, (200, "good".into()));
    let status = first_status_of(addr).await;
    assert_eq!(
        failed_slots(&status),
        ["long-label.sutra", "long-route.sutra", "nul-route.sutra"],
        "{status}"
    );
    engine.shutdown().await;
}

// ---- two contents of one version on a topic -----------------------------------------------------

/// Nothing ranks them, so the one serving keeps serving, and a restart gives the same answer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_second_content_of_a_version_on_a_topic_leaves_the_first_serving_after_a_restart() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let (sink, _capture) = capture_server().await;
    let yaml = format!("{DAPR_ON_TOPIC}    codec: json\n");
    let first = package(&inbound_package("app", "in", &yaml, sink));
    let second_root = inbound_package("app", "in", &yaml, sink);
    let bpmn = std::fs::read_to_string(second_root.join("bpmn/in.bpmn")).unwrap();
    write(
        &second_root,
        "bpmn/in.bpmn",
        &format!("{bpmn}<!-- other content -->\n"),
    );
    let second = package(&second_root);
    let bystander = package(&inbound_package("bystander", "other", &yaml, sink));

    let dir = temp_root("claims-dir");
    place_archive(&dir, "z-first.sutra", &first);
    place_archive(&dir, "m-bystander.sutra", &bystander);
    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    let addr = engine.local_addr;
    place_archive(&dir, "a-second.sutra", &second);
    assert!(
        wait_until(10, || async {
            failed_slots(&status_of(addr).await) == ["a-second.sutra"]
        })
        .await,
        "{}",
        status_of(addr).await
    );
    assert_eq!(
        active_slots(&status_of(addr).await),
        ["m-bystander.sutra", "z-first.sutra"]
    );
    engine.shutdown().await;

    let engine = boot_archive_engine(dir, Some(url)).await;
    let status = first_status_of(engine.local_addr).await;
    assert_eq!(
        active_slots(&status),
        ["m-bystander.sutra", "z-first.sutra"],
        "{status}"
    );
    engine.shutdown().await;
}

// ---- a broker's own id for a message ------------------------------------------------------------

fn artemis_port() -> u16 {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::SyncRunner;
    use testcontainers::{Container, GenericImage, ImageExt};
    static BROKER: std::sync::OnceLock<(Container<GenericImage>, u16)> = std::sync::OnceLock::new();
    BROKER
        .get_or_init(|| {
            std::thread::spawn(|| {
                let container = GenericImage::new("apache/activemq-artemis", "latest-alpine")
                    .with_exposed_port(5672.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("AMQ221007"))
                    .with_env_var("ARTEMIS_USER", "artemis")
                    .with_env_var("ARTEMIS_PASSWORD", "artemis")
                    .with_env_var("ANONYMOUS_LOGIN", "true")
                    .start()
                    .expect("start the AMQP broker");
                sutra_testkit::reap_on_exit(container.id());
                let port = container.get_host_port_ipv4(5672).expect("mapped 5672");
                (container, port)
            })
            .join()
            .expect("broker bootstrap")
        })
        .1
}

/// The same message, sent every half second, whichever of them the consumer is attached for. A flow
/// that declares the message id runs once however many arrive, and another id is another event.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_declared_amqp_message_id_absorbs_the_same_message_arriving_again() {
    use fe2o3_amqp::types::messaging::{ApplicationProperties, Message, MessageId, Properties};
    use fe2o3_amqp::{Connection, Sender, Session};

    let port = artemis_port();
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let queue = format!("q-{}", std::process::id());
    let root = inbound_package(
        "keyed",
        "amqp-in",
        &format!(
            "    transport: amqp\n    host: 127.0.0.1\n    port: {port}\n    queue: {queue}\n    codec: json\n"
        ),
        sink,
    );
    let bpmn = std::fs::read_to_string(root.join("bpmn/in.bpmn")).unwrap();
    write(
        &root,
        "bpmn/in.bpmn",
        &bpmn.replace(
            "<q:source channel=\"amqp-in\"/>",
            "<q:source channel=\"amqp-in\" dedupKey=\"amqp.message-id\"/>",
        ),
    );
    let engine = boot_db_engine(url).await;
    let (status, body) = post(engine.local_addr, "/admin/deployments", &package(&root)).await;
    assert_eq!(status, 200, "deploy: {body}");

    let mut connection = Connection::open(
        "keyed-producer",
        format!("amqp://127.0.0.1:{port}").as_str(),
    )
    .await
    .expect("open");
    let mut session = Session::begin(&mut connection).await.expect("session");
    let mut sender = Sender::attach(&mut session, "keyed-producer", queue.as_str())
        .await
        .expect("sender");
    let message = |id: &str| {
        Message::builder()
            .properties(
                Properties::builder()
                    .message_id(MessageId::String(id.to_string()))
                    .build(),
            )
            .application_properties(
                ApplicationProperties::builder()
                    .insert("content-type", "application/json")
                    .build(),
            )
            .data(br#"{"n":1}"#.to_vec())
            .build()
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while capture.delivered("/keyed-done") == 0 && std::time::Instant::now() < deadline {
        sender.send(message("same-id")).await.expect("send");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(capture.delivered("/keyed-done") >= 1, "the consumer ran it");
    for _ in 0..3 {
        sender.send(message("same-id")).await.expect("send");
    }
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    assert_eq!(capture.delivered("/keyed-done"), 1, "however many arrived");

    sender.send(message("other-id")).await.expect("send");
    assert!(
        wait_until(30, || async { capture.delivered("/keyed-done") == 2 }).await,
        "another id is another event"
    );
    let _ = sender.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    engine.shutdown().await;
}

// ---- a form an earlier engine accepted ----------------------------------------------------------

/// `bytes` as an earlier CLI would have sealed it: its source on `channel` declaring a `dedupKey` form
/// nothing reads. Sealing refuses that now, so the archive is put back together by hand.
fn sealed_with_unrecognised_dedup_key(bytes: &[u8], channel: &str) -> Vec<u8> {
    let mut entries = crate::archive_activation_conformance::entries_of(bytes);
    let manifest_name = sutra_loader::archive::MANIFEST_FILE_NAME;
    let manifest = String::from_utf8(entries[manifest_name].clone()).unwrap();
    let mut manifest = sutra_loader::ArchiveManifest::parse(&manifest).unwrap();
    let (declared, undeclared) = (
        format!("<q:source channel=\"{channel}\"/>"),
        format!("<q:source channel=\"{channel}\" dedupKey=\"payload.id\"/>"),
    );
    let mut replaced = 0;
    for (name, content) in entries.iter_mut() {
        if !name.starts_with("bpmn/") {
            continue;
        }
        let text = String::from_utf8(content.clone()).unwrap();
        if text.contains(&declared) {
            *content = text.replace(&declared, &undeclared).into_bytes();
            let artifact = manifest
                .artifacts
                .iter_mut()
                .find(|artifact| &artifact.path == name)
                .expect("listed");
            artifact.sha256 = sutra_loader::archive::sha256_hex(content);
            replaced += 1;
        }
    }
    assert_eq!(replaced, 1, "one source on {channel}");
    entries.insert(manifest_name.to_string(), manifest.to_yaml().into_bytes());
    sutra_loader::write_archive(&entries).expect("re-zip")
}

/// One deployment active, and none reported failed.
async fn assert_it_stays_running(addr: SocketAddr) {
    let status = status_of(addr).await;
    assert_eq!(status["active"].as_array().unwrap().len(), 1, "{status}");
    assert!(failed_slots(&status).is_empty(), "{status}");
}

/// A stored row an earlier engine accepted boots, serves with no declared key, and an instance parked
/// on it finishes after a restart.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_stored_row_with_an_unrecognised_dedup_key_boots_serves_and_resumes() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot_db_engine(url.clone()).await;
    engine.shutdown().await;
    let older = sealed_with_unrecognised_dedup_key(
        &package(&timer_package("older", "/older/timed", "PT3S", sink)),
        "timed-in",
    );
    insert_active_row(&pool, "older", "1.0.0", &older, "1 day").await;

    let engine = boot_db_engine(url.clone()).await;
    assert_it_stays_running(engine.local_addr).await;
    for _ in 0..2 {
        let (status, body) = post(engine.local_addr, "/older/timed", br#"{"id":"same"}"#).await;
        assert!(status < 300, "it serves: {status} {body}");
    }
    engine.shutdown().await;

    let engine = boot_db_engine(url).await;
    assert_it_stays_running(engine.local_addr).await;
    assert!(
        wait_until(30, || async { capture.delivered("/older-done") == 2 }).await,
        "both parked instances finish: the form dedupes nothing"
    );
    assert_eq!(active_rows(&pool).await, 1);
    engine.shutdown().await;
}

/// A folder cannot say whether a file was ever deployed, so it loads, whichever it is.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_folder_file_with_an_unrecognised_dedup_key_boots_serves_and_resumes() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let (sink, capture) = capture_server().await;
    let older = sealed_with_unrecognised_dedup_key(
        &package(&timer_package("older", "/older/timed", "PT3S", sink)),
        "timed-in",
    );
    let dir = temp_root("claims-dir");
    place_archive(&dir, "older.sutra", &older);

    let engine = boot_archive_engine(dir.clone(), Some(url.clone())).await;
    assert_it_stays_running(engine.local_addr).await;
    let (status, body) = post(engine.local_addr, "/older/timed", br#"{"id":"same"}"#).await;
    assert!(status < 300, "it serves: {status} {body}");
    engine.shutdown().await;

    let engine = boot_archive_engine(dir, Some(url)).await;
    assert_it_stays_running(engine.local_addr).await;
    assert!(
        wait_until(30, || async { capture.delivered("/older-done") == 1 }).await,
        "the parked instance finishes"
    );
    engine.shutdown().await;
}

/// What already runs keeps running; a deploy of the same bytes is new, and refused.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_new_deploy_of_an_unrecognised_dedup_key_is_still_refused() {
    let (pool, url) = fresh_db().await;
    let (sink, _capture) = capture_server().await;
    let older = sealed_with_unrecognised_dedup_key(
        &package(&timer_package("older", "/older/timed", "PT3S", sink)),
        "timed-in",
    );
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &older).await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body.contains("SUTRA.PARSE.Q_SOURCE.DEDUP_KEY_UNRECOGNISED"),
        "{body}"
    );
    assert!(body.contains("payload.id"), "{body}");
    assert!(body.contains("amqp.message-id"), "{body}");
    assert_eq!(active_rows(&pool).await, 0);
    let (status, body) = post(addr, "/admin/deployments/check", &older).await;
    assert_eq!(status, 400, "the check gives the deploy's answer: {body}");
    engine.shutdown().await;
}
