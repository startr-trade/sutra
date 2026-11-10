//! What a same-owner upgrade does to the version it drains, with v2 holding the routes and v1 still
//! holding running instances: v1's instances finish on v1, the channels only v1 declares stay served
//! by v1 until it retires, another owner is refused them meanwhile, and a literal `local://` send
//! does not cross packages. Tests named `what_*` still only record behaviour that a later change
//! decides.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::archive_activation_conformance::{
    boot_db_engine, capture_server, fresh_db, http_request, package, post, temp_root, wait_until,
    write, Capture,
};
use crate::leadership_it::{declare_queue, rabbit_port, raw_connection};

const AUTH: &str = "    auth:\n      scheme: apikey\n      apikey:\n        value: flip-key\n        header: X-Api-Key\n";

fn labels(tenant: &str, module: &str, version: &str) -> String {
    format!(
        "labels:\n  \"tenant\": \"{tenant}\"\n  \"module\": \"{module}\"\n  \"version\": \"{version}\"\nengine:\n  minContract: 1\n"
    )
}

fn definitions(module: &str, version: &str, id: &str, body: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_{id}"
                  targetNamespace="urn:sutra:module:{module}:{version}">
{body}
</bpmn:definitions>
"#
    )
}

/// Sends to `http://<sink>/<path>`, then ends.
fn report_and_end(id: &str, incoming: &str, sink: SocketAddr, path: &str) -> String {
    format!(
        r#"    <bpmn:sendTask id="{id}" name="Report">
      <bpmn:extensionElements><q:send destination="http://{sink}/{path}"/></bpmn:extensionElements>
      <bpmn:incoming>{incoming}</bpmn:incoming><bpmn:outgoing>{id}Out</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="{id}Out" sourceRef="{id}" targetRef="{id}End"/>
    <bpmn:endEvent id="{id}End"><bpmn:incoming>{id}Out</bpmn:incoming></bpmn:endEvent>"#
    )
}

/// One package: `gate` parks until `gate-release`, and then uses everything internal it has. It
/// starts `child` over a local channel, answers a parked `waiter` over a local channel, and runs
/// `called` as a call activity. Each of those reports to a path naming its version, so which
/// definition ran is observable. `quick` is the plain new external request.
fn internal_package(version: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let module = "app";
    let gate = format!(
        r#"  <bpmn:process id="gate" name="Gate" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="release" type="object"/>
      <q:variable name="responseBody" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="gate-start"/></bpmn:extensionElements>
      <bpmn:outgoing>g1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="g1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm the wait">
      <bpmn:incoming>g1</bpmn:incoming><bpmn:outgoing>g2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>release</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="g2" sourceRef="Arm" targetRef="AwaitRelease"/>
    <bpmn:receiveTask id="AwaitRelease" name="Await the release">
      <bpmn:extensionElements>
        <q:source channel="gate-release" name="release"/>
        <q:alias name="gateKey" expression="release.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>g2</bpmn:incoming><bpmn:outgoing>g3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="g3" sourceRef="AwaitRelease" targetRef="BuildChild"/>
    <bpmn:serviceTask id="BuildChild" name="Build the internal message">
      <bpmn:incoming>g3</bpmn:incoming><bpmn:outgoing>g4</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{ref: release.key}}</bpmn:from><bpmn:to>responseBody</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="g4" sourceRef="BuildChild" targetRef="StartChild"/>
    <bpmn:sendTask id="StartChild" name="Start the child">
      <bpmn:extensionElements><q:send channel="to-child"/></bpmn:extensionElements>
      <bpmn:incoming>g4</bpmn:incoming><bpmn:outgoing>g5</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="g5" sourceRef="StartChild" targetRef="AnswerWaiter"/>
    <bpmn:sendTask id="AnswerWaiter" name="Answer the waiter">
      <bpmn:extensionElements><q:send channel="to-waiter"/></bpmn:extensionElements>
      <bpmn:incoming>g5</bpmn:incoming><bpmn:outgoing>g6</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="g6" sourceRef="AnswerWaiter" targetRef="CallIt"/>
    <bpmn:callActivity id="CallIt" calledElement="called">
      <bpmn:incoming>g6</bpmn:incoming><bpmn:outgoing>g7</bpmn:outgoing>
    </bpmn:callActivity>
    <bpmn:sequenceFlow id="g7" sourceRef="CallIt" targetRef="Done"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("Done", "g7", sink, &format!("{version}-gate-done")),
    );
    let child = format!(
        r#"  <bpmn:process id="child" name="Child" isExecutable="true">
    <bpmn:startEvent id="ChildStart">
      <bpmn:extensionElements><q:source channel="child-in"/></bpmn:extensionElements>
      <bpmn:outgoing>c1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="c1" sourceRef="ChildStart" targetRef="ChildDone"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("ChildDone", "c1", sink, &format!("{version}-child-done")),
    );
    let waiter = format!(
        r#"  <bpmn:process id="waiter" name="Waiter" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="WaiterStart">
      <bpmn:extensionElements><q:source channel="waiter-start"/></bpmn:extensionElements>
      <bpmn:outgoing>w1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="w1" sourceRef="WaiterStart" targetRef="WaiterArm"/>
    <bpmn:serviceTask id="WaiterArm" name="Arm the wait">
      <bpmn:incoming>w1</bpmn:incoming><bpmn:outgoing>w2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{ref: payload.key}}</bpmn:from><bpmn:to>answer</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="w2" sourceRef="WaiterArm" targetRef="AwaitAnswer"/>
    <bpmn:receiveTask id="AwaitAnswer" name="Await the answer">
      <bpmn:extensionElements>
        <q:source channel="waiter-answer" name="answer"/>
        <q:alias name="waiterRef" expression="answer.ref" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>w2</bpmn:incoming><bpmn:outgoing>w3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="w3" sourceRef="AwaitAnswer" targetRef="WaiterDone"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("WaiterDone", "w3", sink, &format!("{version}-waiter-done")),
    );
    let called = format!(
        r#"  <bpmn:process id="called" name="Called" isExecutable="true">
    <bpmn:startEvent id="CalledStart"><bpmn:outgoing>k1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="k1" sourceRef="CalledStart" targetRef="CalledDone"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("CalledDone", "k1", sink, &format!("{version}-called-done")),
    );
    let quick = format!(
        r#"  <bpmn:process id="quick" name="Quick" isExecutable="true">
    <bpmn:startEvent id="QuickStart">
      <bpmn:extensionElements><q:source channel="quick-start"/></bpmn:extensionElements>
      <bpmn:outgoing>q1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="q1" sourceRef="QuickStart" targetRef="QuickDone"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("QuickDone", "q1", sink, &format!("{version}-quick-done")),
    );
    for (name, body) in [
        ("gate", gate),
        ("child", child),
        ("waiter", waiter),
        ("called", called),
        ("quick", quick),
    ] {
        write(
            &root,
            &format!("bpmn/{name}.bpmn"),
            &definitions(module, version, name, &body),
        );
    }
    let http = |name: &str| {
        format!(
            "  - name: {name}\n    transport: http\n    bind: \"POST /d/{name}\"\n    codec: json\n{AUTH}"
        )
    };
    let local_in = |name: &str| {
        format!(
            "  - name: {name}\n    transport: local\n    codec: json\n    cloudevents-mode: none\n"
        )
    };
    let local_out = |name: &str, to: &str| {
        format!(
            "  - name: {name}\n    direction: outbound\n    transport: local\n    bind: local://{to}\n    cloudevents-mode: none\n"
        )
    };
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n{}{}{}{}{}{}{}{}",
            http("gate-start"),
            http("gate-release"),
            http("waiter-start"),
            http("quick-start"),
            local_in("child-in"),
            local_in("waiter-answer"),
            local_out("to-child", "child-in"),
            local_out("to-waiter", "waiter-answer"),
        ),
    );
    write(&root, "package.yaml", &labels("t1", module, version));
    root
}

async fn status_of(addr: SocketAddr) -> serde_json::Value {
    let (_, body) =
        tokio::task::spawn_blocking(move || http_request(addr, "GET", "/sutra/deployments", b""))
            .await
            .expect("get task");
    serde_json::from_str(body.trim()).expect("status json")
}

async fn draining_count(addr: SocketAddr) -> usize {
    status_of(addr).await["draining"]
        .as_array()
        .map_or(0, Vec::len)
}

struct Drained {
    engine: sutra_engine::RunningEngine,
    capture: Capture,
    pool: sqlx::PgPool,
}

/// v1 with a parked `gate` (key G1) and a parked `waiter` (key G1, the ref the gate answers with),
/// then v2 on the same routes, so v1 is draining with both still waiting.
async fn drained_v1_with_parked_instances() -> Drained {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let v1 = package(&internal_package("1.0.0", sink));
    let v2 = package(&internal_package("2.0.0", sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/d/gate-start", br#"{"key":"G1"}"#).await;
    assert_eq!(status, 200, "park the gate: {body}");
    let (status, body) = post(addr, "/d/waiter-start", br#"{"key":"G1"}"#).await;
    assert_eq!(status, 200, "park the waiter: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2 takes the routes: {body}");
    assert_eq!(draining_count(addr).await, 1, "v1 is draining");
    Drained {
        engine,
        capture,
        pool,
    }
}

/// The release, which sets v1's internal work going.
async fn release_the_gate(addr: SocketAddr) {
    let (status, body) = post(addr, "/d/gate-release", br#"{"key":"G1"}"#).await;
    assert_eq!(status, 200, "the release resumes v1: {body}");
}

/// Why each undelivered row of the outbox is stuck, for a failure message.
async fn outbox_state(pool: &sqlx::PgPool) -> String {
    let rows: Vec<(String, i32, Option<String>)> = sqlx::query_as(
        "SELECT destination, attempt_count, last_diagnostic_json::text FROM outbox_entry",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    format!("{rows:?}")
}

// 1
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_draining_version_starts_a_child_over_a_local_channel_on_its_own_definition() {
    let run = drained_v1_with_parked_instances().await;
    release_the_gate(run.engine.local_addr).await;
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-child-done") >= 1
        })
        .await,
        "the child ran on v1. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert_eq!(run.capture.delivered("/2.0.0-child-done"), 0, "never on v2");
    run.engine.shutdown().await;
}

// 2
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_draining_version_answers_its_own_parked_instance_over_a_local_channel() {
    let run = drained_v1_with_parked_instances().await;
    release_the_gate(run.engine.local_addr).await;
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-waiter-done") >= 1
        })
        .await,
        "the waiter completed on v1. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert_eq!(
        run.capture.delivered("/2.0.0-waiter-done"),
        0,
        "never on v2"
    );
    run.engine.shutdown().await;
}

// 3
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_draining_version_runs_its_own_called_process() {
    let run = drained_v1_with_parked_instances().await;
    release_the_gate(run.engine.local_addr).await;
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-called-done") >= 1
        })
        .await,
        "the called process ran on v1. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert_eq!(run.capture.delivered("/2.0.0-called-done"), 0, "never v2's");
    run.engine.shutdown().await;
}

// 4
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_new_external_request_during_the_drain_runs_only_on_the_new_version() {
    let run = drained_v1_with_parked_instances().await;
    let addr = run.engine.local_addr;
    let (status, body) = post(addr, "/d/quick-start", br#"{"n":1}"#).await;
    assert_eq!(status, 200, "before the release: {body}");
    release_the_gate(addr).await;
    let (status, body) = post(addr, "/d/quick-start", br#"{"n":2}"#).await;
    assert_eq!(status, 200, "after the release: {body}");
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/2.0.0-quick-done") >= 2
        })
        .await,
        "both ran on v2. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert_eq!(run.capture.delivered("/1.0.0-quick-done"), 0, "never on v1");
    run.engine.shutdown().await;
}

// 5
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_draining_version_retires_only_when_its_work_has_finished() {
    let run = drained_v1_with_parked_instances().await;
    let addr = run.engine.local_addr;
    // Parked: several sweep ticks pass and it stays.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_eq!(draining_count(addr).await, 1, "parked instances pin v1");

    release_the_gate(addr).await;
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-gate-done") >= 1
                && run.capture.delivered("/1.0.0-child-done") >= 1
                && run.capture.delivered("/1.0.0-waiter-done") >= 1
                && run.capture.delivered("/1.0.0-called-done") >= 1
        })
        .await,
        "all of v1's work finished on v1. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert!(
        wait_until(30, || async { draining_count(addr).await == 0 }).await,
        "then v1 retires. outbox: {}",
        outbox_state(&run.pool).await
    );
    let v1_instances: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM deployment_archive WHERE version = '1.0.0' AND status = 'retired'",
    )
    .fetch_one(&run.pool)
    .await
    .unwrap();
    assert_eq!(v1_instances, 1, "retired in the store");
    run.engine.shutdown().await;
}

// ---- 6: a queue channel ------------------------------------------------------------------------

/// Parks at an intermediate message catch on queue channel `q-in`, correlated by `key`.
fn queue_catch_package(version: &str, queue: &str, port: u16, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let process = format!(
        r#"  <bpmn:process id="queued" name="Queued" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="q-start"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm the wait">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>answer</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Arm" targetRef="Await"/>
    <bpmn:intermediateCatchEvent id="Await" name="Await the queue">
      <bpmn:extensionElements>
        <q:source channel="q-in" name="answer"/>
        <q:alias name="queueKey" expression="answer.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="f3" sourceRef="Await" targetRef="Done"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("Done", "f3", sink, &format!("{version}-q-done")),
    );
    write(
        &root,
        "bpmn/queued.bpmn",
        &definitions("qapp", version, "queued", &process),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: q-start\n    transport: http\n    bind: \"POST /q/start\"\n    codec: json\n{AUTH}  - name: q-in\n    transport: rabbitmq\n    codec: json\n    host: 127.0.0.1\n    port: {port}\n    queue: {queue}\n    username: env:E3_RABBIT_USER\n    password: env:E3_RABBIT_PASSWORD\n"
        ),
    );
    write(&root, "package.yaml", &labels("t1", "qapp", version));
    root
}

// 6
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_parked_old_version_resumes_from_a_queue_the_new_version_took() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let port = rabbit_port();
    let queue = format!("drain-q-{}", std::process::id());
    declare_queue(port, &queue).await;
    std::env::set_var("E3_RABBIT_USER", "guest");
    std::env::set_var("E3_RABBIT_PASSWORD", "guest");
    let v1 = package(&queue_catch_package("1.0.0", &queue, port, sink));
    let v2 = package(&queue_catch_package("2.0.0", &queue, port, sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/q/start", br#"{"key":"Q1"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2 takes the queue: {body}");
    assert_eq!(draining_count(addr).await, 1);

    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    channel
        .basic_publish(
            "",
            &queue,
            lapin::options::BasicPublishOptions::default(),
            br#"{"key":"Q1"}"#,
            lapin::BasicProperties::default()
                .with_message_id("q1".into())
                .with_content_type("application/json".into()),
        )
        .await
        .expect("publish")
        .await
        .expect("confirm");
    connection.close(200, "published").await.ok();
    assert!(
        wait_until(30, || async { capture.delivered("/1.0.0-q-done") >= 1 }).await,
        "the parked instance finished on v1. outbox: {}",
        outbox_state(&pool).await
    );
    assert_eq!(capture.delivered("/2.0.0-q-done"), 0, "never on v2");
    engine.shutdown().await;
}

// ---- 7: the relay's fallback across owners -----------------------------------------------------

/// Parks at a receive task on channel `shared-name`, reachable at `POST <path>`.
fn same_channel_package(tenant: &str, module: &str, path: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let process = format!(
        r#"  <bpmn:process id="held" name="Held" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="start-{module}"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm the wait">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>answer</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Arm" targetRef="Await"/>
    <bpmn:receiveTask id="Await" name="Await">
      <bpmn:extensionElements>
        <q:source channel="shared-name" name="answer"/>
        <q:alias name="heldKey" expression="answer.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Await" targetRef="Done"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("Done", "f3", sink, &format!("{tenant}-{module}-done")),
    );
    write(
        &root,
        "bpmn/held.bpmn",
        &definitions(module, "1.0.0", "held", &process),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: start-{module}\n    transport: http\n    bind: \"POST {path}/start\"\n    codec: json\n{AUTH}  - name: shared-name\n    transport: http\n    bind: \"POST {path}/resume\"\n    codec: json\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &labels(tenant, module, "1.0.0"));
    root
}

/// 7. Records behaviour, and it is a leak: the relay's fallback scope is every draining deployment,
/// whoever owns it. Nothing should depend on this; it asserts only that the scenario was set up, so
/// the suite stays green until a decision is made.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn what_a_message_on_one_owners_channel_does_to_another_owners_draining_instance() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let other = package(&same_channel_package("t2", "victim", "/victim", sink));
    let mine = package(&same_channel_package("t1", "caller", "/caller", sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &other).await;
    assert_eq!(status, 200, "other owner: {body}");
    let (status, body) = post(addr, "/victim/start", br#"{"key":"SAME"}"#).await;
    assert_eq!(status, 200, "park on the other owner: {body}");
    // Undeployed with an instance parked: that owner's deployment is now draining.
    let (status, body) = tokio::task::spawn_blocking(move || {
        http_request(addr, "DELETE", "/admin/deployments/t2--victim--1.0.0", b"")
    })
    .await
    .expect("delete task");
    assert_eq!(status, 200, "undeploy: {body}");
    assert_eq!(draining_count(addr).await, 1);
    let (status, body) = post(addr, "/admin/deployments", &mine).await;
    assert_eq!(status, 200, "this owner: {body}");

    // A message on THIS owner's channel, with the alias value the other owner's instance parked on.
    let (status, body) = post(addr, "/caller/resume", br#"{"key":"SAME"}"#).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    println!(
        "OBSERVED 7 message on t1's channel with t2's alias value: {status} {body}; t2's parked \
         instance completed {} time(s)",
        capture.delivered("/t2-victim-done")
    );
    engine.shutdown().await;
}

// ---- 8: v2 omits a channel v1 has --------------------------------------------------------------

/// `gate-start` and `gate-release`, and a v1-only start `only-v1-start`.
fn package_with_or_without_extra(version: &str, extra: bool, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let release = if extra {
        "extra-release"
    } else {
        "kept-release"
    };
    let process = format!(
        r#"  <bpmn:process id="held" name="Held" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="kept-start"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm the wait">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>answer</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Arm" targetRef="Await"/>
    <bpmn:receiveTask id="Await" name="Await">
      <bpmn:extensionElements>
        <q:source channel="{release}" name="answer"/>
        <q:alias name="heldKey" expression="answer.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Await" targetRef="Done"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("Done", "f3", sink, &format!("{version}-held-done")),
    );
    write(
        &root,
        "bpmn/held.bpmn",
        &definitions("omit", version, "held", &process),
    );
    let mut channels = format!(
        "channels:\n  - name: kept-start\n    transport: http\n    bind: \"POST /o/kept-start\"\n    codec: json\n{AUTH}  - name: kept-release\n    transport: http\n    bind: \"POST /o/kept-release\"\n    codec: json\n{AUTH}"
    );
    if extra {
        let started = format!(
            r#"  <bpmn:process id="extra" name="Extra" isExecutable="true">
    <bpmn:startEvent id="ExtraStart">
      <bpmn:extensionElements><q:source channel="only-v1-start"/></bpmn:extensionElements>
      <bpmn:outgoing>e1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="e1" sourceRef="ExtraStart" targetRef="ExtraDone"/>
{done}
  </bpmn:process>"#,
            done = report_and_end("ExtraDone", "e1", sink, &format!("{version}-extra-done")),
        );
        write(
            &root,
            "bpmn/extra.bpmn",
            &definitions("omit", version, "extra", &started),
        );
        channels.push_str(&format!(
            "  - name: extra-release\n    transport: http\n    bind: \"POST /o/extra-release\"\n    codec: json\n{V1_AUTH}  - name: only-v1-start\n    transport: http\n    bind: \"POST /o/only-v1-start\"\n    codec: json\n{V1_AUTH}"
        ));
    }
    write(&root, "channels.yaml", &channels);
    write(&root, "package.yaml", &labels("t1", "omit", version));
    root
}

/// The v1-only channels of [`package_with_or_without_extra`] authenticate with a key of their own.
const V1_AUTH: &str = "    auth:\n      scheme: apikey\n      apikey:\n        value: v1-key\n        header: X-Api-Key\n";

/// A POST carrying `key` as its API key.
async fn post_keyed(addr: SocketAddr, path: &str, key: &str, body: &[u8]) -> (u16, String) {
    use std::io::{Read, Write};
    let port = addr.port();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: {key}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let body = body.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.write_all(head.as_bytes()).expect("head");
        stream.write_all(&body).expect("body");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("response");
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("status code");
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or_default();
        (status, body)
    })
    .await
    .expect("post task")
}

/// v1 has `extra-release` and `only-v1-start`, which v2 lacks, with `keys` parked on
/// `extra-release`, and v2 deployed over it.
async fn v1_parked_on_its_own_channel(keys: &[&str]) -> (Drained, String) {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let v1 = package(&package_with_or_without_extra("1.0.0", true, sink));
    let v2 = package(&package_with_or_without_extra("2.0.0", false, sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    for key in keys {
        let start = format!(r#"{{"key":"{key}"}}"#);
        let (status, body) = post(addr, "/o/kept-start", start.as_bytes()).await;
        assert_eq!(status, 200, "park {key}: {body}");
    }
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2: {body}");
    assert_eq!(draining_count(addr).await, 1, "v1 is draining");
    (
        Drained {
            engine,
            capture,
            pool,
        },
        body,
    )
}

async fn release_on_v1s_channel(addr: SocketAddr, key: &str) -> (u16, String) {
    let body = format!(r#"{{"key":"{key}"}}"#);
    post_keyed(addr, "/o/extra-release", "v1-key", body.as_bytes()).await
}

/// 8. A channel only v1 declares stays served by v1: a resume reaches the parked instance, and a
/// start runs on v1. The deploy says so.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_channel_only_the_old_version_has_stays_served() {
    let (run, deployed) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    assert!(
        deployed.contains("Channels only it declares stay served by it until it retires"),
        "{deployed}"
    );
    assert!(deployed.contains("POST /o/extra-release"), "{deployed}");
    let (status, body) = post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}").await;
    assert_eq!(status, 200, "a start on v1's own channel: {body}");
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "the resume: {body}");
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-held-done") == 1
                && run.capture.delivered("/1.0.0-extra-done") == 1
        })
        .await,
        "both ran on v1. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert_eq!(run.capture.delivered("/2.0.0-held-done"), 0);
    assert_eq!(run.capture.delivered("/2.0.0-extra-done"), 0);
    run.engine.shutdown().await;
}

/// The kept channel is v1's, auth included: v2's key does not open it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn its_auth_is_v1s() {
    let (run, _) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    let (status, body) = post(addr, "/o/only-v1-start", b"{}").await;
    assert_eq!(status, 401, "v2's key: {body}");
    let (status, body) = post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}").await;
    assert_eq!(status, 200, "v1's key: {body}");
    run.engine.shutdown().await;
}

/// Undeploying v2 does not stop what v1 keeps: it serves until v1 itself retires, and then stops.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn undeploying_the_new_version_keeps_v1s_channel_until_v1_retires() {
    let (run, _) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    let (status, body) = tokio::task::spawn_blocking(move || {
        http_request(addr, "DELETE", "/admin/deployments/t1--omit--2.0.0", b"")
    })
    .await
    .expect("delete task");
    assert_eq!(status, 200, "undeploy v2: {body}");
    let (status, body) = post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}").await;
    assert_eq!(status, 200, "still served after v2 is gone: {body}");
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "the resume: {body}");
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-held-done") == 1
                && run.capture.delivered("/1.0.0-extra-done") == 1
        })
        .await,
        "ran on v1. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert!(
        wait_until(30, || async { draining_count(addr).await == 0 }).await,
        "then v1 retires (and v2 with it): {}",
        status_of(addr).await
    );
    assert!(
        wait_until(10, || async {
            post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}")
                .await
                .0
                == 404
        })
        .await,
        "and its channel stops"
    );
    run.engine.shutdown().await;
}

/// Undeploying v2 while v1 drains rolls back: v1 serves again the channel it shared with v2
/// (`kept-start`), so a new start there runs on v1. That too ends when v1 retires.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn undeploying_the_new_version_gives_v1_back_the_channels_it_shared() {
    let (run, _) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    let (status, body) = tokio::task::spawn_blocking(move || {
        http_request(addr, "DELETE", "/admin/deployments/t1--omit--2.0.0", b"")
    })
    .await
    .expect("delete task");
    assert_eq!(status, 200, "undeploy v2: {body}");
    let (status, body) = post(addr, "/o/kept-start", br#"{"key":"N"}"#).await;
    assert_eq!(status, 200, "the shared channel after v2 is gone: {body}");
    // v1's process waits on v1's own release channel, so both instances resume there.
    for key in ["K", "N"] {
        let (status, body) = release_on_v1s_channel(addr, key).await;
        assert_eq!(status, 200, "the resume of {key}: {body}");
    }
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-held-done") == 2
        })
        .await,
        "both ran on v1. outbox: {}",
        outbox_state(&run.pool).await
    );
    assert_eq!(run.capture.delivered("/2.0.0-held-done"), 0);
    assert!(
        wait_until(30, || async { draining_count(addr).await == 0 }).await,
        "then v1 retires: {}",
        status_of(addr).await
    );
    assert!(
        wait_until(10, || async {
            post(addr, "/o/kept-start", br#"{"key":"L"}"#).await.0 == 404
        })
        .await,
        "and the shared channel stops with it"
    );
    run.engine.shutdown().await;
}

/// 8b. A resume on v2's channel of another name cannot reach v1's instance parked on its own
/// channel (Q2: no runtime mapping), and v1's own channel still can.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_resume_on_the_new_versions_other_channel_does_not_reach_v1() {
    let (run, _) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    let (status, body) = post(addr, "/o/kept-release", br#"{"key":"K"}"#).await;
    assert_ne!(status, 200, "v2's channel: {body}");
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "v1's channel: {body}");
    assert!(
        wait_until(30, || async {
            run.capture.delivered("/1.0.0-held-done") == 1
        })
        .await
    );
    run.engine.shutdown().await;
}

// ---- R6: another owner on what v1 keeps ---------------------------------------------------------

/// Another owner's package serving `POST /o/extra-release`, v1's kept route.
fn grabbing_package(sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    write(
        &root,
        "bpmn/grab.bpmn",
        &definitions(
            "grab",
            "1.0.0",
            "grab",
            &started_process("grab", "grab-in", sink),
        ),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: grab-in\n    transport: http\n    bind: \"POST /o/extra-release\"\n    codec: json\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &labels("t2", "grab", "1.0.0"));
    root
}

/// Another owner is refused the endpoint v1 keeps, with v1 named and its instances counted.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn another_owner_is_refused_a_retained_endpoint_naming_v1_and_its_count() {
    let (run, _) = v1_parked_on_its_own_channel(&["K1", "K2"]).await;
    let addr = run.engine.local_addr;
    let grab = package(&grabbing_package(SocketAddr::from(([127, 0, 0, 1], 9))));
    let (status, body) = post(addr, "/admin/deployments", &grab).await;
    assert_eq!(status, 409, "{body}");
    for part in [
        "SUTRA.DEPLOY.ENDPOINT.COLLISION",
        "POST /o/extra-release",
        "stays with draining deployment t1--omit--1.0.0",
        "(2 running instances)",
        "until it retires",
    ] {
        assert!(body.contains(part), "{part}: {body}");
    }
    let stored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM deployment_archive WHERE module = 'grab'")
            .fetch_one(&run.pool)
            .await
            .unwrap();
    assert_eq!(stored, 0);
    run.engine.shutdown().await;
}

/// Once v1 retires the endpoint is free, and the same deploy lands.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn it_is_released_when_v1_retires() {
    let (run, _) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    let (sink, grabbed) = capture_server().await;
    let grab = package(&grabbing_package(sink));
    let (status, body) = post(addr, "/admin/deployments", &grab).await;
    assert_eq!(status, 409, "while v1 drains: {body}");
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        wait_until(30, || async { draining_count(addr).await == 0 }).await,
        "v1 retires"
    );
    let (status, body) = post(addr, "/admin/deployments", &grab).await;
    assert_eq!(status, 200, "after v1 retired: {body}");
    let (status, body) = post(addr, "/o/extra-release", br#"{"key":"K"}"#).await;
    assert_eq!(status, 200, "now the other owner's: {body}");
    assert!(
        wait_until(30, || async {
            grabbed.delivered("/grab-grab-in-started") == 1
        })
        .await
    );
    run.engine.shutdown().await;
}

/// The check endpoint gives the deploy's answer, with the count the sweep read.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn the_check_endpoint_reports_it() {
    let (run, _) = v1_parked_on_its_own_channel(&["K1", "K2"]).await;
    let addr = run.engine.local_addr;
    let grab = package(&grabbing_package(SocketAddr::from(([127, 0, 0, 1], 9))));
    assert!(
        wait_until(10, || async {
            status_of(addr).await["drainingInstances"]
                .as_object()
                .is_some_and(|m| m.values().any(|n| n == 2))
        })
        .await,
        "the sweep counted v1: {}",
        status_of(addr).await
    );
    let (status, body) = post(addr, "/admin/deployments/check", &grab).await;
    assert_eq!(status, 200, "{body}");
    let verdict: serde_json::Value = serde_json::from_str(body.trim()).unwrap();
    assert_eq!(verdict["ok"], false, "{verdict}");
    let collision = &verdict["collisions"][0];
    assert_eq!(collision["holder"], "t1--omit--1.0.0", "{verdict}");
    assert_eq!(collision["holderDraining"], true, "{verdict}");
    assert_eq!(collision["holderInstances"], 2, "{verdict}");
    assert!(
        collision["message"]
            .as_str()
            .unwrap_or("")
            .contains("(2 running instances)"),
        "{verdict}"
    );
    run.engine.shutdown().await;
}

// ---- R7: retirement ----------------------------------------------------------------------------

/// v1 with kept channels retires once its work is done, and its channels stop with it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_version_with_kept_channels_retires_at_zero_and_stops_serving() {
    let (run, _) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_eq!(draining_count(addr).await, 1, "the parked instance pins v1");
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        wait_until(30, || async { draining_count(addr).await == 0 }).await,
        "v1 retires at zero. outbox: {}",
        outbox_state(&run.pool).await
    );
    let retired: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM deployment_archive WHERE module = 'omit' AND version = '1.0.0' \
         AND status = 'retired'",
    )
    .fetch_one(&run.pool)
    .await
    .unwrap();
    assert_eq!(retired, 1);
    assert!(
        wait_until(10, || async {
            release_on_v1s_channel(addr, "K").await.0 == 404
                && post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}")
                    .await
                    .0
                    == 404
        })
        .await,
        "its channels stop"
    );
    run.engine.shutdown().await;
}

/// A start can land on v1 between the count that let it retire and the retirement. A later sweep
/// finds the instance and opens v1 again. The landing is simulated by writing an instance row for
/// v1 just after it retires.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_retired_version_reopens_when_an_instance_landed() {
    let (run, _) = v1_parked_on_its_own_channel(&["K"]).await;
    let addr = run.engine.local_addr;
    // A parked instance on v2, whose row is copied under v1's id.
    let (status, body) = post(addr, "/o/kept-start", br#"{"key":"ON-V2"}"#).await;
    assert_eq!(status, 200, "{body}");
    let v1_id: String = sqlx::query_scalar(
        "SELECT deployment_id FROM deployment_archive WHERE module = 'omit' AND version = '1.0.0'",
    )
    .fetch_one(&run.pool)
    .await
    .unwrap();
    let v2_id: String = sqlx::query_scalar(
        "SELECT deployment_id FROM deployment_archive WHERE module = 'omit' AND version = '2.0.0'",
    )
    .fetch_one(&run.pool)
    .await
    .unwrap();
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "{body}");
    let pool = run.pool.clone();
    let id = v1_id.clone();
    assert!(
        wait_until(30, || {
            let (pool, id) = (pool.clone(), id.clone());
            async move {
                sqlx::query_scalar::<_, String>(
                    "SELECT status FROM deployment_archive WHERE deployment_id = $1",
                )
                .bind(&id)
                .fetch_one(&pool)
                .await
                .is_ok_and(|status| status == "retired")
            }
        })
        .await,
        "v1 retires"
    );
    let mut tx = run.pool.begin().await.unwrap();
    sqlx::query(
        "CREATE TEMP TABLE landed ON COMMIT DROP AS SELECT * FROM instance_state \
         WHERE deployment_id = $1 AND terminal_at IS NULL LIMIT 1",
    )
    .bind(&v2_id)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("UPDATE landed SET deployment_id = $1, instance_id = gen_random_uuid()")
        .bind(&v1_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO instance_state SELECT * FROM landed")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let pool = run.pool.clone();
    let id = v1_id.clone();
    assert!(
        wait_until(15, || {
            let (pool, id) = (pool.clone(), id.clone());
            async move {
                sqlx::query_scalar::<_, String>(
                    "SELECT status FROM deployment_archive WHERE deployment_id = $1",
                )
                .bind(&id)
                .fetch_one(&pool)
                .await
                .is_ok_and(|status| status == "draining")
            }
        })
        .await,
        "a sweep opens v1 again"
    );
    assert!(
        wait_until(10, || async { draining_count(addr).await == 1 }).await,
        "and it drains, served again: {}",
        status_of(addr).await
    );
    run.engine.shutdown().await;
}

// ---- the folder source --------------------------------------------------------------------------

/// A folder engine on `dir` with a database.
async fn boot_folder(dir: PathBuf, url: String) -> sutra_engine::RunningEngine {
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    crate::archive_activation_conformance::boot_archive_engine(dir, Some(url)).await
}

/// v1's file and a parked instance on it, then v2's file beside it, active.
async fn folder_with_v1_parked(
    sink: SocketAddr,
) -> (sutra_engine::RunningEngine, PathBuf, String, sqlx::PgPool) {
    let (pool, url) = fresh_db().await;
    let dir = temp_root("drain-dir");
    let place = crate::archive_activation_conformance::place_archive;
    place(
        &dir,
        "omit-1.sutra",
        &package(&package_with_or_without_extra("1.0.0", true, sink)),
    );
    let engine = boot_folder(dir.clone(), url.clone()).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/o/kept-start", br#"{"key":"K"}"#).await;
    assert_eq!(status, 200, "park on v1: {body}");
    place(
        &dir,
        "omit-2.sutra",
        &package(&package_with_or_without_extra("2.0.0", false, sink)),
    );
    assert!(
        wait_until(15, || async { draining_count(addr).await == 1 }).await,
        "v2 takes over: {}",
        status_of(addr).await
    );
    (engine, dir, url, pool)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_folder_keeps_a_channel_only_the_old_file_has() {
    let (sink, capture) = capture_server().await;
    let (engine, _dir, _url, _pool) = folder_with_v1_parked(sink).await;
    let addr = engine.local_addr;
    let (status, body) = post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}").await;
    assert_eq!(status, 200, "a start on v1's channel: {body}");
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "the resume: {body}");
    assert!(
        wait_until(30, || async {
            capture.delivered("/1.0.0-held-done") == 1
                && capture.delivered("/1.0.0-extra-done") == 1
        })
        .await
    );
    engine.shutdown().await;
}

/// Removing v1's file is an undeploy: what it kept stops, while its instance stays resumable by
/// other means.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn removing_the_file_stops_it() {
    let (sink, _capture) = capture_server().await;
    let (engine, dir, _url, _pool) = folder_with_v1_parked(sink).await;
    let addr = engine.local_addr;
    let (status, _) = post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}").await;
    assert_eq!(status, 200, "served while the file is there");
    std::fs::remove_file(dir.join("omit-1.sutra")).unwrap();
    assert!(
        wait_until(15, || async {
            post_keyed(addr, "/o/only-v1-start", "v1-key", b"{}")
                .await
                .0
                == 404
        })
        .await,
        "the file is gone, so is the channel"
    );
    assert_eq!(
        draining_count(addr).await,
        1,
        "v1 still drains its instance"
    );
    engine.shutdown().await;
}

/// The folder refuses another owner's archive on v1's kept endpoint until v1 retires, and a restart
/// gives the same answer.
async fn folder_refuses_then_releases(restart: bool) {
    let (sink, capture) = capture_server().await;
    let (mut engine, dir, url, _pool) = folder_with_v1_parked(sink).await;
    crate::archive_activation_conformance::place_archive(
        &dir,
        "grab.sutra",
        &package(&grabbing_package(sink)),
    );
    let refused = |addr: SocketAddr| async move {
        status_of(addr).await["failed"].as_array().is_some_and(|f| {
            f.iter().any(|f| {
                f["slot"] == "grab.sutra"
                    && f["error"]
                        .as_str()
                        .unwrap_or("")
                        .contains("stays with draining deployment t1--omit--1.0.0")
            })
        })
    };
    assert!(
        wait_until(15, || refused(engine.local_addr)).await,
        "{}",
        status_of(engine.local_addr).await
    );
    if restart {
        engine.shutdown().await;
        engine = boot_folder(dir.clone(), url).await;
        assert!(
            wait_until(15, || refused(engine.local_addr)).await,
            "a restart keeps the answer: {}",
            status_of(engine.local_addr).await
        );
    }
    let addr = engine.local_addr;
    let (status, body) = release_on_v1s_channel(addr, "K").await;
    assert_eq!(status, 200, "{body}");
    assert!(
        wait_until(30, || async { capture.delivered("/1.0.0-held-done") == 1 }).await,
        "v1's instance finishes"
    );
    assert!(
        wait_until(30, || async {
            status_of(addr).await["active"]
                .as_array()
                .is_some_and(|a| a.iter().any(|a| a["slot"] == "grab.sutra"))
        })
        .await,
        "v1 retires, and the other owner's archive deploys: {}",
        status_of(addr).await
    );
    engine.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_folder_refuses_another_owner_until_v1_retires() {
    folder_refuses_then_releases(false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_restart_keeps_the_answer() {
    folder_refuses_then_releases(true).await;
}

// ---- 9: v2 changes a channel it keeps the name of ----------------------------------------------

/// One channel `modified-in` at `POST <path>`, with `codec`, resuming a parked `held` by `answer.key`.
fn modified_channel_package(version: &str, codec: &str, path: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let process = format!(
        r#"  <bpmn:process id="held" name="Held" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="m-start"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm the wait">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>answer</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Arm" targetRef="Await"/>
    <bpmn:receiveTask id="Await" name="Await">
      <bpmn:extensionElements>
        <q:source channel="modified-in" name="answer"/>
        <q:alias name="heldKey" expression="answer.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Await" targetRef="Done"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("Done", "f3", sink, &format!("{version}-modified-done")),
    );
    write(
        &root,
        "bpmn/held.bpmn",
        &definitions("modified", version, "held", &process),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: m-start\n    transport: http\n    bind: \"POST /m/start\"\n    codec: json\n{AUTH}  - name: modified-in\n    transport: http\n    bind: \"POST {path}\"\n    codec: {codec}\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &labels("t1", "modified", version));
    root
}

/// A POST with its own content type, which the shared helper fixes to JSON.
async fn post_typed(
    addr: SocketAddr,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> (u16, String) {
    use std::io::{Read, Write};
    let port = addr.port();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: flip-key\r\n\
         Content-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let body = body.to_vec();
    tokio::task::spawn_blocking(move || {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.write_all(head.as_bytes()).expect("head");
        stream.write_all(&body).expect("body");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("response");
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("status code");
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or_default();
        (status, body)
    })
    .await
    .expect("post task")
}

/// 9a. Records behaviour: v2 kept the channel's name and route and changed its codec.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn what_decodes_a_resume_on_a_channel_the_new_version_changed() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let v1 = package(&modified_channel_package(
        "1.0.0",
        "json",
        "/m/resume",
        sink,
    ));
    let v2 = package(&modified_channel_package("2.0.0", "xml", "/m/resume", sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    for key in ["JSONKEY", "XMLKEY"] {
        let start = format!(r#"{{"key":"{key}"}}"#);
        let (status, body) = post(addr, "/m/start", start.as_bytes()).await;
        assert_eq!(status, 200, "park {key}: {body}");
    }
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2: {body}");
    println!("OBSERVED 9a v2 deploy: {body}");

    let (status, body) = post_typed(
        addr,
        "/m/resume",
        "application/json",
        br#"{"key":"JSONKEY"}"#,
    )
    .await;
    println!("OBSERVED 9a v1-format (application/json) resume: {status} {body}");
    let (status, body) = post_typed(
        addr,
        "/m/resume",
        "application/xml",
        b"<m><key>XMLKEY</key></m>",
    )
    .await;
    println!("OBSERVED 9a v2-format (application/xml) resume: {status} {body}");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    println!(
        "OBSERVED 9a after 5s: v1 done {}, v2 done {}, draining {}",
        capture.delivered("/1.0.0-modified-done"),
        capture.delivered("/2.0.0-modified-done"),
        draining_count(addr).await
    );
    engine.shutdown().await;
}

/// 9b. v2 keeps the channel's name and moves its route: v1's old address is gone, the new one
/// resumes v1, and the deploy warns with the parked count.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_moved_address_is_gone_and_warned() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let v1 = package(&modified_channel_package("1.0.0", "json", "/m/old", sink));
    let v2 = package(&modified_channel_package("2.0.0", "json", "/m/new", sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/m/start", br#"{"key":"K"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2: {body}");
    for part in [
        "SUTRA.DEPLOY.ENDPOINT.CHANNEL_CHANGED",
        "channel 'modified-in' changes address from t1--modified--1.0.0",
        "1 instance of t1--modified--1.0.0 is parked on it",
    ] {
        assert!(body.contains(part), "{part}: {body}");
    }
    let (status, body) = post(addr, "/m/old", br#"{"key":"K"}"#).await;
    assert_eq!(status, 404, "v1's old address is gone: {body}");
    let (status, body) = post(addr, "/m/new", br#"{"key":"K"}"#).await;
    assert_eq!(status, 200, "the new address resumes v1: {body}");
    assert!(
        wait_until(30, || async {
            capture.delivered("/1.0.0-modified-done") == 1
        })
        .await
    );
    assert_eq!(capture.delivered("/2.0.0-modified-done"), 0);
    engine.shutdown().await;
}

/// v2 moves a queue channel to another queue: v1's consumer on the old queue stops, a message there
/// waits for nobody, the new queue resumes v1, and the deploy warns.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_moved_queue_stops_v1s_consumer_and_warns() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let port = rabbit_port();
    let pid = std::process::id();
    let (old_queue, new_queue) = (format!("drain-old-{pid}"), format!("drain-new-{pid}"));
    declare_queue(port, &old_queue).await;
    declare_queue(port, &new_queue).await;
    std::env::set_var("E3_RABBIT_USER", "guest");
    std::env::set_var("E3_RABBIT_PASSWORD", "guest");
    let v1 = package(&queue_catch_package("1.0.0", &old_queue, port, sink));
    let v2 = package(&queue_catch_package("2.0.0", &new_queue, port, sink));
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/q/start", br#"{"key":"Q1"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2: {body}");
    for part in [
        "SUTRA.DEPLOY.ENDPOINT.CHANNEL_CHANGED",
        "channel 'q-in' changes address",
        "1 instance of t1--qapp--1.0.0 is parked on it",
    ] {
        assert!(body.contains(part), "{part}: {body}");
    }

    let connection = raw_connection(port).await;
    let channel = connection.create_channel().await.expect("channel");
    let publish = |queue: String, id: &'static str| {
        let channel = channel.clone();
        async move {
            channel
                .basic_publish(
                    "",
                    &queue,
                    lapin::options::BasicPublishOptions::default(),
                    br#"{"key":"Q1"}"#,
                    lapin::BasicProperties::default()
                        .with_message_id(id.into())
                        .with_content_type("application/json".into()),
                )
                .await
                .expect("publish")
                .await
                .expect("confirm");
        }
    };
    publish(old_queue.clone(), "old-1").await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert_eq!(
        capture.delivered("/1.0.0-q-done"),
        0,
        "nothing reads the old queue"
    );
    let waiting = channel
        .basic_get(&old_queue, lapin::options::BasicGetOptions { no_ack: true })
        .await
        .expect("get");
    assert!(waiting.is_some(), "the message waits on the old queue");
    publish(new_queue.clone(), "new-1").await;
    assert!(
        wait_until(30, || async { capture.delivered("/1.0.0-q-done") >= 1 }).await,
        "the new queue resumes v1. outbox: {}",
        outbox_state(&pool).await
    );
    assert_eq!(capture.delivered("/2.0.0-q-done"), 0);
    connection.close(200, "done").await.ok();
    engine.shutdown().await;
}

// ---- 10: a draining version's own message that matches a start event ---------------------------

/// A process started on `channel` that reports `<version>-<channel>-started`.
fn started_process(version: &str, channel: &str, sink: SocketAddr) -> String {
    let safe = channel.replace('-', "");
    format!(
        r#"  <bpmn:process id="p{safe}" name="Started" isExecutable="true">
    <bpmn:startEvent id="S{safe}">
      <bpmn:extensionElements><q:source channel="{channel}"/></bpmn:extensionElements>
      <bpmn:outgoing>f{safe}</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f{safe}" sourceRef="S{safe}" targetRef="R{safe}"/>
{done}
  </bpmn:process>"#,
        done = report_and_end(
            &format!("R{safe}"),
            &format!("f{safe}"),
            sink,
            &format!("{version}-{channel}-started")
        ),
    )
}

/// `sender` parks on `s-release`, then sends one message on each of four channels. v1 declares all
/// four; v2 declares only the `both-` two.
fn sender_package(
    version: &str,
    v1: bool,
    rabbit: u16,
    queues: (&str, &str),
    engine: SocketAddr,
    sink: SocketAddr,
) -> PathBuf {
    let root = temp_root("drain-pkg");
    let sender = format!(
        r#"  <bpmn:process id="sender" name="Sender" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="release" type="object"/>
      <q:variable name="responseBody" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="s-start"/></bpmn:extensionElements>
      <bpmn:outgoing>a1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="a1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm the wait">
      <bpmn:incoming>a1</bpmn:incoming><bpmn:outgoing>a2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>release</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{from: "sender"}}</bpmn:from><bpmn:to>responseBody</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="a2" sourceRef="Arm" targetRef="Await"/>
    <bpmn:receiveTask id="Await" name="Await">
      <bpmn:extensionElements>
        <q:source channel="s-release" name="release"/>
        <q:alias name="senderKey" expression="release.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>a2</bpmn:incoming><bpmn:outgoing>a3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="a3" sourceRef="Await" targetRef="SendOnlyQueue"/>
    <bpmn:sendTask id="SendOnlyQueue" name="Queue, v1 only">
      <bpmn:extensionElements><q:send destination="rabbitmq://127.0.0.1:{rabbit}/{only_queue}" contentType="application/json"/></bpmn:extensionElements>
      <bpmn:incoming>a3</bpmn:incoming><bpmn:outgoing>a4</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a4" sourceRef="SendOnlyQueue" targetRef="SendBothQueue"/>
    <bpmn:sendTask id="SendBothQueue" name="Queue, both versions">
      <bpmn:extensionElements><q:send destination="rabbitmq://127.0.0.1:{rabbit}/{both_queue}" contentType="application/json"/></bpmn:extensionElements>
      <bpmn:incoming>a4</bpmn:incoming><bpmn:outgoing>a5</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a5" sourceRef="SendBothQueue" targetRef="SendOnlyHttp"/>
    <bpmn:sendTask id="SendOnlyHttp" name="HTTP, v1 only">
      <bpmn:extensionElements><q:send channel="to-only-http" contentType="application/json"><q:header name="X-Api-Key" value="&quot;flip-key&quot;"/></q:send></bpmn:extensionElements>
      <bpmn:incoming>a5</bpmn:incoming><bpmn:outgoing>a6</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a6" sourceRef="SendOnlyHttp" targetRef="SendBothHttp"/>
    <bpmn:sendTask id="SendBothHttp" name="HTTP, both versions">
      <bpmn:extensionElements><q:send channel="to-both-http" contentType="application/json"><q:header name="X-Api-Key" value="&quot;flip-key&quot;"/></q:send></bpmn:extensionElements>
      <bpmn:incoming>a6</bpmn:incoming><bpmn:outgoing>a7</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a7" sourceRef="SendBothHttp" targetRef="SenderDone"/>
{done}
  </bpmn:process>"#,
        only_queue = queues.0,
        both_queue = queues.1,
        done = report_and_end("SenderDone", "a7", sink, &format!("{version}-sender-done")),
    );
    write(
        &root,
        "bpmn/sender.bpmn",
        &definitions("sendr", version, "sender", &sender),
    );
    let mut started = vec!["both-queue", "both-http"];
    if v1 {
        started.extend(["only-queue", "only-http"]);
    }
    for channel in &started {
        write(
            &root,
            &format!("bpmn/{channel}.bpmn"),
            &definitions(
                "sendr",
                version,
                &channel.replace('-', ""),
                &started_process(version, channel, sink),
            ),
        );
    }
    let http = |name: &str| {
        format!("  - name: {name}\n    transport: http\n    bind: \"POST /s/{name}\"\n    codec: json\n{AUTH}")
    };
    let queue = |name: &str, on: &str| {
        format!(
            "  - name: {name}\n    transport: rabbitmq\n    codec: json\n    host: 127.0.0.1\n    port: {rabbit}\n    queue: {on}\n    username: env:E3_RABBIT_USER\n    password: env:E3_RABBIT_PASSWORD\n"
        )
    };
    let out = |name: &str, to: &str| {
        format!(
            "  - name: {name}\n    direction: outbound\n    transport: http\n    bind: \"http://{engine}/s/{to}\"\n    cloudevents-mode: none\n"
        )
    };
    let mut channels = format!(
        "channels:\n{}{}{}{}{}{}",
        http("s-start"),
        http("s-release"),
        http("both-http"),
        queue("both-queue", queues.1),
        out("to-only-http", "only-http"),
        out("to-both-http", "both-http"),
    );
    if v1 {
        channels.push_str(&http("only-http"));
        channels.push_str(&queue("only-queue", queues.0));
    }
    write(&root, "channels.yaml", &channels);
    write(&root, "package.yaml", &labels("t1", "sendr", version));
    root
}

/// 10. A draining v1's own message on a channel only it has starts on v1, over a queue and over
/// HTTP; on a channel v2 has too, it starts on v2 (10b).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn an_old_versions_message_starts_on_it_on_a_channel_only_it_has() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let rabbit = rabbit_port();
    let pid = std::process::id();
    let (only_queue, both_queue) = (format!("drain-only-{pid}"), format!("drain-both-{pid}"));
    declare_queue(rabbit, &only_queue).await;
    declare_queue(rabbit, &both_queue).await;
    std::env::set_var("E3_RABBIT_USER", "guest");
    std::env::set_var("E3_RABBIT_PASSWORD", "guest");
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let queues = (only_queue.as_str(), both_queue.as_str());
    let reachable = SocketAddr::from(([127, 0, 0, 1], addr.port()));
    let v1 = package(&sender_package(
        "1.0.0", true, rabbit, queues, reachable, sink,
    ));
    let v2 = package(&sender_package(
        "2.0.0", false, rabbit, queues, reachable, sink,
    ));
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/s/s-start", br#"{"key":"S"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2: {body}");

    let (status, body) = post(addr, "/s/s-release", br#"{"key":"S"}"#).await;
    assert_eq!(status, 200, "release: {body}");
    assert!(
        wait_until(30, || async {
            capture.delivered("/1.0.0-sender-done") == 1
                && capture.delivered("/1.0.0-only-queue-started") == 1
                && capture.delivered("/1.0.0-only-http-started") == 1
                && capture.delivered("/2.0.0-both-queue-started") == 1
                && capture.delivered("/2.0.0-both-http-started") == 1
        })
        .await,
        "outbox: {}",
        outbox_state(&pool).await
    );
    for never in [
        "/2.0.0-only-queue-started",
        "/2.0.0-only-http-started",
        "/1.0.0-both-queue-started",
        "/1.0.0-both-http-started",
    ] {
        assert_eq!(capture.delivered(never), 0, "{never}");
    }
    assert!(
        wait_until(30, || async { draining_count(addr).await == 0 }).await,
        "v1 retires once those finish. outbox: {}",
        outbox_state(&pool).await
    );
    engine.shutdown().await;
}

// ---- 11: `local://` to a different package -----------------------------------------------------

/// A package whose only process starts on `in` and reports `<module>-started`.
fn simple_start_package(module: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    write(
        &root,
        "bpmn/started.bpmn",
        &definitions(
            module,
            "1.0.0",
            "started",
            &started_process(module, "in-local", sink),
        ),
    );
    write(
        &root,
        "channels.yaml",
        "channels:\n  - name: in-local\n    transport: local\n    codec: json\n    cloudevents-mode: none\n",
    );
    write(&root, "package.yaml", &labels("t1", module, "1.0.0"));
    root
}

/// A package that starts on `POST /<name>/go` and sends `local://t1/<name>/1.0.0/in-local`, its own
/// local channel, which lint accepts.
fn local_sender_package(name: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let process = format!(
        r#"  <bpmn:process id="sender" name="Sender" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="responseBody" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="go"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Build"/>
    <bpmn:serviceTask id="Build" name="Build">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{from: "sender"}}</bpmn:from><bpmn:to>responseBody</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Build" targetRef="Send"/>
    <bpmn:sendTask id="Send" name="Send">
      <bpmn:extensionElements><q:send destination="local://t1/{name}/1.0.0/in-local" contentType="application/json"/></bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Send" targetRef="End"/>
    <bpmn:endEvent id="End"><bpmn:incoming>f3</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
{own}"#,
        own = started_process(name, "in-local", sink),
    );
    write(
        &root,
        "bpmn/sender.bpmn",
        &definitions(name, "1.0.0", "sender", &process),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: go\n    transport: http\n    bind: \"POST /{name}/go\"\n    codec: json\n{AUTH}  - name: in-local\n    transport: local\n    codec: json\n    cloudevents-mode: none\n"
        ),
    );
    write(&root, "package.yaml", &labels("t1", name, "1.0.0"));
    root
}

/// `bytes` with `from` replaced by `to` in its BPMN, sealed again by hand as an earlier CLI would
/// have: packaging refuses the result now.
fn resealed_with(bytes: &[u8], from: &str, to: &str) -> Vec<u8> {
    let mut entries = crate::archive_activation_conformance::entries_of(bytes);
    let manifest_name = sutra_loader::archive::MANIFEST_FILE_NAME;
    let manifest = String::from_utf8(entries[manifest_name].clone()).unwrap();
    let mut manifest = sutra_loader::ArchiveManifest::parse(&manifest).unwrap();
    let mut replaced = 0;
    for (name, content) in entries.iter_mut() {
        if !name.starts_with("bpmn/") {
            continue;
        }
        let text = String::from_utf8(content.clone()).unwrap();
        if text.contains(from) {
            *content = text.replace(from, to).into_bytes();
            let artifact = manifest
                .artifacts
                .iter_mut()
                .find(|artifact| &artifact.path == name)
                .expect("listed");
            artifact.sha256 = sutra_loader::archive::sha256_hex(content);
            replaced += 1;
        }
    }
    assert_eq!(replaced, 1, "one file holds {from}");
    entries.insert(manifest_name.to_string(), manifest.to_yaml().into_bytes());
    sutra_loader::write_archive(&entries).expect("re-zip")
}

const CROSSES: &str = "SUTRA.CONFIG.SEND.LOCAL_DESTINATION_CROSSES_PACKAGE";

/// The sender, sealed by hand to send to the `target` package's local channel.
fn crossing_sender(sink: SocketAddr) -> Vec<u8> {
    resealed_with(
        &package(&local_sender_package("bydest", sink)),
        "local://t1/bydest/1.0.0/in-local",
        "local://t1/target/1.0.0/in-local",
    )
}

/// 11. A literal `local://` send to another package is refused by packaging, by the deploy API and by
/// its check, and the other package never starts.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_local_send_across_packages_is_refused_at_deploy() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(
        addr,
        "/admin/deployments",
        &package(&simple_start_package("target", sink)),
    )
    .await;
    assert_eq!(status, 200, "target: {body}");

    // Packaging refuses it.
    let root = local_sender_package("bydest", sink);
    let process = std::fs::read_to_string(root.join("bpmn/sender.bpmn")).unwrap();
    write(
        &root,
        "bpmn/sender.bpmn",
        &process.replace(
            "local://t1/bydest/1.0.0/in-local",
            "local://t1/target/1.0.0/in-local",
        ),
    );
    let out = temp_root("pkg");
    let packaged =
        sutra_loader::assemble_dir(&root, &out, &sutra_loader::PackageOptions::default());
    let Err(refused) = packaged else {
        panic!("packaging refuses it");
    };
    let refusal = format!("{refused:?}");
    assert!(refusal.contains(CROSSES), "{refusal}");

    // An archive sealed without that check is refused by the deploy and by its check.
    let sealed = crossing_sender(sink);
    let (status, body) = post(addr, "/admin/deployments", &sealed).await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains(CROSSES), "{body}");
    assert!(body.contains("local://t1/target/1.0.0/in-local"), "{body}");
    let (status, body) = post(addr, "/admin/deployments/check", &sealed).await;
    assert_eq!(status, 400, "the check gives the deploy's answer: {body}");
    let stored: i64 =
        sqlx::query_scalar("SELECT count(*) FROM deployment_archive WHERE module = 'bydest'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, 0, "nothing stored");
    let (status, _) = post(addr, "/bydest/go", b"{}").await;
    assert_eq!(status, 404, "it does not serve");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(capture.delivered("/target-in-local-started"), 0);
    engine.shutdown().await;
}

/// A row an earlier engine stored with such a send keeps running after the upgrade, and the send
/// reaches the other package as it did then.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_stored_row_keeps_running() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot_db_engine(url.clone()).await;
    let (status, body) = post(
        engine.local_addr,
        "/admin/deployments",
        &package(&simple_start_package("target", sink)),
    )
    .await;
    assert_eq!(status, 200, "target: {body}");
    engine.shutdown().await;
    crate::endpoint_claims_it::insert_active_row(
        &pool,
        "bydest",
        "1.0.0",
        &crossing_sender(sink),
        "1 day",
    )
    .await;

    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let status = status_of(addr).await;
    assert_eq!(status["active"].as_array().unwrap().len(), 2, "{status}");
    assert!(
        crate::endpoint_claims_it::failed_slots(&status).is_empty(),
        "{status}"
    );
    let (status, body) = post(addr, "/bydest/go", b"{}").await;
    assert_eq!(status, 200, "it serves: {body}");
    assert!(
        wait_until(30, || async {
            capture.delivered("/target-in-local-started") == 1
        })
        .await,
        "the stored send still runs. outbox: {}",
        outbox_state(&pool).await
    );
    engine.shutdown().await;
}

/// In a folder a scan is the deploy, so such an archive is `Failed`, at boot as later.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn a_folder_archive_with_a_crossing_send_is_failed() {
    let (_pool, url) = fresh_db().await;
    std::env::set_var(
        "SUTRA_DB_MIGRATIONS",
        crate::outbox_e2e::shipped_migration_roots_env(),
    );
    let (sink, _capture) = capture_server().await;
    let dir = temp_root("drain-dir");
    crate::archive_activation_conformance::place_archive(
        &dir,
        "bydest.sutra",
        &crossing_sender(sink),
    );
    let engine = crate::archive_activation_conformance::boot_archive_engine(dir, Some(url)).await;
    let addr = engine.local_addr;
    assert!(
        wait_until(10, || async {
            status_of(addr).await["failed"]
                .as_array()
                .is_some_and(|f| f.len() == 1)
        })
        .await,
        "{}",
        status_of(addr).await
    );
    let status = status_of(addr).await;
    let failed = status["failed"].as_array().cloned().unwrap_or_default();
    assert!(
        failed[0]["error"].as_str().unwrap_or("").contains(CROSSES),
        "{status}"
    );
    assert!(status["active"].as_array().unwrap().is_empty(), "{status}");
    engine.shutdown().await;
}

// ---- 12: v2 adds a start event where v1 waits --------------------------------------------------

/// `v1` parks at a wait on `reply-in`. `v2` instead has a start event on `reply-in`.
fn wait_or_start_package(version: &str, wait: bool, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let reply_side = if wait {
        format!(
            r#"  <bpmn:process id="held" name="Held" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="begin"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>answer</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Arm" targetRef="Await"/>
    <bpmn:receiveTask id="Await" name="Await">
      <bpmn:extensionElements>
        <q:source channel="reply-in" name="answer"/>
        <q:alias name="heldKey" expression="answer.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>f3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="f3" sourceRef="Await" targetRef="Done"/>
{done}
  </bpmn:process>"#,
            done = report_and_end("Done", "f3", sink, &format!("{version}-resumed")),
        )
    } else {
        format!(
            r#"  <bpmn:process id="fresh" name="Fresh" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="reply-in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Done"/>
{done}
  </bpmn:process>
  <bpmn:process id="begin" name="Begin" isExecutable="true">
    <bpmn:startEvent id="BeginStart">
      <bpmn:extensionElements><q:source channel="begin"/></bpmn:extensionElements>
      <bpmn:outgoing>b1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="b1" sourceRef="BeginStart" targetRef="BeginEnd"/>
    <bpmn:endEvent id="BeginEnd"><bpmn:incoming>b1</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>"#,
            done = report_and_end("Done", "f1", sink, &format!("{version}-started")),
        )
    };
    write(
        &root,
        "bpmn/flow.bpmn",
        &definitions("startwait", version, "flow", &reply_side),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: begin\n    transport: http\n    bind: \"POST /w/begin\"\n    codec: json\n{AUTH}  - name: reply-in\n    transport: http\n    bind: \"POST /w/reply\"\n    codec: json\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &labels("t1", "startwait", version));
    root
}

/// 12. Records behaviour: a correlated reply where v2 has a start event on that channel.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn what_a_correlated_reply_does_when_the_new_version_starts_on_that_channel() {
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let v1 = package(&wait_or_start_package("1.0.0", true, sink));
    let v2 = package(&wait_or_start_package("2.0.0", false, sink));
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/w/begin", br#"{"key":"K"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2: {body}");
    let (status, body) = post(addr, "/w/reply", br#"{"key":"K"}"#).await;
    println!(
        "OBSERVED 12 correlated reply: {status} {}",
        &body[..body.len().min(300)]
    );
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
    println!(
        "OBSERVED 12 v1 instance resumed {} time(s), v2 start ran {} time(s), draining {}",
        capture.delivered("/1.0.0-resumed"),
        capture.delivered("/2.0.0-started"),
        draining_count(addr).await
    );
    engine.shutdown().await;
}

// ---- 13: a draining instance sends on a channel v2 changed or dropped --------------------------

/// `sender` parks, then sends on `to-changed` and `to-dropped`, waits on a timer, and sends on both
/// again. Each version's `to-changed` binds an address naming the version; v2 has no `to-dropped`.
fn outbound_package(version: &str, has_dropped: bool, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    let process = format!(
        r#"  <bpmn:process id="sender" name="Sender" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="release" type="object"/>
      <q:variable name="responseBody" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="begin"/></bpmn:extensionElements>
      <bpmn:outgoing>a1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="a1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm">
      <bpmn:incoming>a1</bpmn:incoming><bpmn:outgoing>a2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{key: payload.key}}</bpmn:from><bpmn:to>release</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{{n: 1}}</bpmn:from><bpmn:to>responseBody</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="a2" sourceRef="Arm" targetRef="Await"/>
    <bpmn:receiveTask id="Await" name="Await">
      <bpmn:extensionElements>
        <q:source channel="release-in" name="release"/>
        <q:alias name="k" expression="release.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>a2</bpmn:incoming><bpmn:outgoing>a3</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:sequenceFlow id="a3" sourceRef="Await" targetRef="Changed1"/>
    <bpmn:sendTask id="Changed1" name="Changed">
      <bpmn:extensionElements><q:send channel="to-changed"/></bpmn:extensionElements>
      <bpmn:incoming>a3</bpmn:incoming><bpmn:outgoing>a4</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a4" sourceRef="Changed1" targetRef="Dropped1"/>
    <bpmn:sendTask id="Dropped1" name="Dropped">
      <bpmn:extensionElements><q:send channel="to-dropped"/></bpmn:extensionElements>
      <bpmn:incoming>a4</bpmn:incoming><bpmn:outgoing>a5</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a5" sourceRef="Dropped1" targetRef="Pause"/>
    <bpmn:intermediateCatchEvent id="Pause" name="Timer">
      <bpmn:incoming>a5</bpmn:incoming><bpmn:outgoing>a6</bpmn:outgoing>
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1S</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="a6" sourceRef="Pause" targetRef="Changed2"/>
    <bpmn:sendTask id="Changed2" name="Changed after the timer">
      <bpmn:extensionElements><q:send channel="to-changed"/></bpmn:extensionElements>
      <bpmn:incoming>a6</bpmn:incoming><bpmn:outgoing>a7</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a7" sourceRef="Changed2" targetRef="Dropped2"/>
    <bpmn:sendTask id="Dropped2" name="Dropped after the timer">
      <bpmn:extensionElements><q:send channel="to-dropped"/></bpmn:extensionElements>
      <bpmn:incoming>a7</bpmn:incoming><bpmn:outgoing>a8</bpmn:outgoing>
    </bpmn:sendTask>
    <bpmn:sequenceFlow id="a8" sourceRef="Dropped2" targetRef="SenderDone"/>
{done}
  </bpmn:process>"#,
        done = report_and_end("SenderDone", "a8", sink, &format!("{version}-sender-done")),
    );
    write(
        &root,
        "bpmn/sender.bpmn",
        &definitions("outb", version, "sender", &process),
    );
    let mut channels = format!(
        "channels:\n  - name: begin\n    transport: http\n    bind: \"POST /x/begin\"\n    codec: json\n{AUTH}  - name: release-in\n    transport: http\n    bind: \"POST /x/release\"\n    codec: json\n{AUTH}  - name: to-changed\n    direction: outbound\n    transport: http\n    bind: \"http://{sink}/{version}-changed\"\n    cloudevents-mode: none\n"
    );
    if has_dropped {
        channels.push_str(&format!(
            "  - name: to-dropped\n    direction: outbound\n    transport: http\n    bind: \"http://{sink}/{version}-dropped\"\n    cloudevents-mode: none\n"
        ));
    }
    write(&root, "channels.yaml", &channels);
    write(&root, "package.yaml", &labels("t1", "outb", version));
    root
}

/// 13. Records behaviour: which definition a draining instance's sends use, before and after a timer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn what_a_draining_instance_sends_on_after_the_new_version_changes_its_channels() {
    let (pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let v1 = package(&outbound_package("1.0.0", true, sink));
    // v2 changes `to-changed` and has no `to-dropped`, so its own process cannot send on that.
    let v2_root = outbound_package("2.0.0", true, sink);
    let channels = std::fs::read_to_string(v2_root.join("channels.yaml")).unwrap();
    let kept = channels
        .split("  - name: to-dropped")
        .next()
        .unwrap()
        .to_string();
    write(&v2_root, "channels.yaml", &kept);
    let process = std::fs::read_to_string(v2_root.join("bpmn/sender.bpmn"))
        .unwrap()
        .replace(
            r#"<q:send channel="to-dropped"/>"#,
            r#"<q:send channel="to-changed"/>"#,
        );
    write(&v2_root, "bpmn/sender.bpmn", &process);
    let v2 = package(&v2_root);
    let (status, body) = post(addr, "/admin/deployments", &v1).await;
    assert_eq!(status, 200, "v1: {body}");
    let (status, body) = post(addr, "/x/begin", br#"{"key":"K"}"#).await;
    assert_eq!(status, 200, "park: {body}");
    let (status, body) = post(addr, "/admin/deployments", &v2).await;
    assert_eq!(status, 200, "v2: {body}");
    let (status, body) = post(addr, "/x/release", br#"{"key":"K"}"#).await;
    println!("OBSERVED 13 release: {status} {body}");
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;
    println!(
        "OBSERVED 13 v1-changed {} (want 2 for both sends on v1's definition), v2-changed {}, \
         v1-dropped {} (2 = v1's definition used), v1 done {}, draining {}. outbox: {}",
        capture.delivered("/1.0.0-changed"),
        capture.delivered("/2.0.0-changed"),
        capture.delivered("/1.0.0-dropped"),
        capture.delivered("/1.0.0-sender-done"),
        draining_count(addr).await,
        outbox_state(&pool).await
    );
    engine.shutdown().await;
}

// ---- 14: a redelivery spanning the flip --------------------------------------------------------

/// Starts on `POST /r/in`, with the idempotency key taken from `header`, reports `<version>-ran`.
fn idempotent_package(version: &str, header: &str, sink: SocketAddr) -> PathBuf {
    let root = temp_root("drain-pkg");
    write(
        &root,
        "bpmn/run.bpmn",
        &definitions(
            "redeliver",
            version,
            "run",
            &format!(
                r#"  <bpmn:process id="run" name="Run" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Ran"/>
{done}
  </bpmn:process>"#,
                done = report_and_end("Ran", "f1", sink, &format!("{version}-ran")),
            ),
        ),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: in\n    transport: http\n    bind: \"POST /r/in\"\n    codec: json\n    idempotency-key-header: {header}\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &labels("t1", "redeliver", version));
    root
}

async fn post_with_header(
    addr: SocketAddr,
    path: &str,
    header: &str,
    value: &str,
) -> (u16, String) {
    use std::io::{Read, Write};
    let port = addr.port();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: flip-key\r\n{header}: {value}\r\n\
         Content-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
    );
    tokio::task::spawn_blocking(move || {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.write_all(request.as_bytes()).expect("request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("response");
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .expect("status code");
        (status, response)
    })
    .await
    .expect("post task")
}

/// 14. Records behaviour: a message v1 processed, sent again after v2 is active.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn what_a_redelivery_spanning_the_flip_does() {
    for (case, v2_header) in [
        ("identical channel", "X-Idem"),
        ("changed key header", "X-Idem-2"),
    ] {
        let (_pool, url) = fresh_db().await;
        let (sink, capture) = capture_server().await;
        let engine = boot_db_engine(url).await;
        let addr = engine.local_addr;
        let v1 = package(&idempotent_package("1.0.0", "X-Idem", sink));
        let v2 = package(&idempotent_package("2.0.0", v2_header, sink));
        let (status, body) = post(addr, "/admin/deployments", &v1).await;
        assert_eq!(status, 200, "v1: {body}");
        let (status, _) = post_with_header(addr, "/r/in", "X-Idem", "EVENT-1").await;
        assert_eq!(status, 200);
        assert!(wait_until(10, || async { capture.delivered("/1.0.0-ran") == 1 }).await);
        // A control on v1 alone: the same key again is a duplicate.
        let (status, _) = post_with_header(addr, "/r/in", "X-Idem", "EVENT-1").await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        println!(
            "OBSERVED 14 [{case}] repeat before the flip: {status}, v1 ran {}",
            capture.delivered("/1.0.0-ran")
        );

        let (status, body) = post(addr, "/admin/deployments", &v2).await;
        assert_eq!(status, 200, "v2: {body}");
        let (status, _) = post_with_header(addr, "/r/in", v2_header, "EVENT-1").await;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        println!(
            "OBSERVED 14 [{case}] redelivery after the flip: {status}, v1 ran {}, v2 ran {}",
            capture.delivered("/1.0.0-ran"),
            capture.delivered("/2.0.0-ran")
        );
        engine.shutdown().await;
    }
}

/// v1 parks each instance at a receive task and a message catch on `answer-in` at once (two
/// message rows) with a timer boundary on the receive task (a timer row). v2 changes that channel's
/// codec. The deploy's warning counts each parked instance once, timer rows aside.
fn parked_kinds_package(version: &str, codec: &str) -> PathBuf {
    let root = temp_root("drain-pkg");
    let process = r#"  <bpmn:process id="kinds" name="Kinds" isExecutable="true">
    <bpmn:extensionElements><q:variables>
      <q:variable name="answer" type="object"/>
      <q:variable name="other" type="object"/>
    </q:variables></bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="k-start"/></bpmn:extensionElements>
      <bpmn:outgoing>f1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Arm"/>
    <bpmn:serviceTask id="Arm" name="Arm">
      <bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{key: payload.key}</bpmn:from><bpmn:to>answer</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
      <bpmn:dataInputAssociation><bpmn:assignment>
        <bpmn:from>{key: payload.key}</bpmn:from><bpmn:to>other</bpmn:to>
      </bpmn:assignment></bpmn:dataInputAssociation>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Arm" targetRef="Fork"/>
    <bpmn:parallelGateway id="Fork">
      <bpmn:incoming>f2</bpmn:incoming><bpmn:outgoing>a1</bpmn:outgoing><bpmn:outgoing>b1</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:sequenceFlow id="a1" sourceRef="Fork" targetRef="Receive"/>
    <bpmn:receiveTask id="Receive" name="Receive">
      <bpmn:extensionElements>
        <q:source channel="answer-in" name="answer"/>
        <q:alias name="receiveKey" expression="answer.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>a1</bpmn:incoming><bpmn:outgoing>a2</bpmn:outgoing>
    </bpmn:receiveTask>
    <bpmn:boundaryEvent id="ReceiveTimeout" attachedToRef="Receive" cancelActivity="true">
      <bpmn:outgoing>t1</bpmn:outgoing>
      <bpmn:timerEventDefinition><bpmn:timeDuration>PT1H</bpmn:timeDuration></bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:sequenceFlow id="t1" sourceRef="ReceiveTimeout" targetRef="TimedOut"/>
    <bpmn:endEvent id="TimedOut"><bpmn:incoming>t1</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="b1" sourceRef="Fork" targetRef="Catch"/>
    <bpmn:intermediateCatchEvent id="Catch" name="Catch">
      <bpmn:extensionElements>
        <q:source channel="answer-in" name="other"/>
        <q:alias name="catchKey" expression="other.key" unique="true"/>
      </bpmn:extensionElements>
      <bpmn:incoming>b1</bpmn:incoming><bpmn:outgoing>b2</bpmn:outgoing>
      <bpmn:messageEventDefinition/>
    </bpmn:intermediateCatchEvent>
    <bpmn:sequenceFlow id="a2" sourceRef="Receive" targetRef="Join"/>
    <bpmn:sequenceFlow id="b2" sourceRef="Catch" targetRef="Join"/>
    <bpmn:parallelGateway id="Join">
      <bpmn:incoming>a2</bpmn:incoming><bpmn:incoming>b2</bpmn:incoming><bpmn:outgoing>j1</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:sequenceFlow id="j1" sourceRef="Join" targetRef="Done"/>
    <bpmn:endEvent id="Done"><bpmn:incoming>j1</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>"#;
    write(
        &root,
        "bpmn/kinds.bpmn",
        &definitions("kinds", version, "kinds", process),
    );
    write(
        &root,
        "channels.yaml",
        &format!(
            "channels:\n  - name: k-start\n    transport: http\n    bind: \"POST /k/start\"\n    codec: json\n{AUTH}  - name: answer-in\n    transport: http\n    bind: \"POST /k/answer\"\n    codec: {codec}\n{AUTH}"
        ),
    );
    write(&root, "package.yaml", &labels("t1", "kinds", version));
    root
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn the_warning_counts_distinct_parked_instances() {
    let (pool, url) = fresh_db().await;
    let engine = boot_db_engine(url).await;
    let addr = engine.local_addr;
    let (status, body) = post(
        addr,
        "/admin/deployments",
        &package(&parked_kinds_package("1.0.0", "json")),
    )
    .await;
    assert_eq!(status, 200, "v1: {body}");
    for key in ["K1", "K2"] {
        let start = format!(r#"{{"key":"{key}"}}"#);
        let (status, body) = post(addr, "/k/start", start.as_bytes()).await;
        assert_eq!(status, 200, "park {key}: {body}");
    }
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    // The fixture role bypasses the row-level policy, so this reads every row.
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT node_id, kind FROM waiting_event WHERE status = 'WAITING' ORDER BY node_id",
    )
    .fetch_all(&pool)
    .await
    .expect("waiting rows");
    let message_rows = rows.iter().filter(|(_, kind)| kind == "MESSAGE").count();
    assert_eq!(message_rows, 4, "two message waits per instance: {rows:?}");
    assert!(
        rows.iter().any(|(_, kind)| kind == "TIMER"),
        "and timer rows: {rows:?}"
    );
    let (status, body) = post(
        addr,
        "/admin/deployments",
        &package(&parked_kinds_package("2.0.0", "xml")),
    )
    .await;
    assert_eq!(status, 200, "v2: {body}");
    assert!(
        body.contains("channel 'answer-in' changes codec from t1--kinds--1.0.0; 2 instances of t1--kinds--1.0.0 are parked on it"),
        "{body}"
    );
    engine.shutdown().await;
}
