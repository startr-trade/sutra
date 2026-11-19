//! Broker connectivity events reach the package, end to end: a real engine with
//! `sutra.engine.connectivity-events.channel` set, a package with an AMQP intake, an AMQP send
//! channel and a `transport: local` `connectivity-in` channel, and an Artemis broker that the
//! test stops and starts. The package records each event it receives by sending it on to a
//! capture server, so the assertions read what the deployment itself saw: a `down` and an `up`
//! for the intake, and a `down` and an `up` for the send channel that sent during the outage,
//! each `up` carrying the outage's start, end and length.

use std::process::Command;
use std::time::{Duration, Instant};

use crate::archive_activation_conformance::{
    archive_engine_config, capture_server, fresh_db, package, place_archive, post, temp_root,
    wait_until, write, API_KEY,
};

const ARTEMIS: &str = "apache/activemq-artemis:latest-alpine";

fn docker(args: &[&str]) -> (bool, String) {
    let out = Command::new("docker").args(args).output().expect("docker");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// A free host port, so the broker comes back on the same one after a restart (a published
/// port chosen by docker would move).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// An Artemis container of the test's own, removed when dropped.
struct Broker {
    name: String,
    port: u16,
}

impl Broker {
    fn start() -> Broker {
        let name = format!("sutra-connectivity-{}", std::process::id());
        let _ = docker(&["rm", "-f", &name]);
        let port = free_port();
        let (ok, text) = docker(&[
            "run",
            "-d",
            "--name",
            &name,
            "-p",
            &format!("127.0.0.1:{port}:5672"),
            "-e",
            "ARTEMIS_USER=artemis",
            "-e",
            "ARTEMIS_PASSWORD=artemis",
            "-e",
            "ANONYMOUS_LOGIN=true",
            ARTEMIS,
        ]);
        assert!(ok, "docker run {ARTEMIS}: {text}");
        let broker = Broker { name, port };
        broker.wait_active(1);
        broker
    }

    /// Wait until the broker has logged that it is active `starts` times (once per start).
    fn wait_active(&self, starts: usize) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let (_, logs) = docker(&["logs", &self.name]);
            if logs.matches("Server is now active").count() >= starts {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "artemis never became active:\n{logs}"
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

fn stop_broker(name: &str) {
    let (ok, text) = docker(&["stop", "-t", "2", name]);
    assert!(ok, "docker stop: {text}");
}

fn restart_broker(name: &str) {
    let (ok, text) = docker(&["start", name]);
    assert!(ok, "docker start: {text}");
    Broker {
        name: name.to_string(),
        port: 0,
    }
    .wait_active_then_forget(2);
}

impl Broker {
    fn wait_active_then_forget(self, starts: usize) {
        self.wait_active(starts);
        std::mem::forget(self);
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

const BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_conn"
                  targetNamespace="urn:sutra:module:conn:1.0.0">
  <bpmn:process id="listen" isExecutable="true">
    <bpmn:startEvent id="S1">
      <bpmn:extensionElements><q:source channel="mq-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:endEvent id="E1"/>
    <bpmn:sequenceFlow id="l1" sourceRef="S1" targetRef="E1"/>
  </bpmn:process>
  <bpmn:process id="emit" isExecutable="true">
    <bpmn:startEvent id="S2">
      <bpmn:extensionElements><q:source channel="emit-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Send" implementation="out.hbs">
      <bpmn:extensionElements>
        <q:send channel="mq-out" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E2"/>
    <bpmn:sequenceFlow id="e1" sourceRef="S2" targetRef="Send"/>
    <bpmn:sequenceFlow id="e2" sourceRef="Send" targetRef="E2"/>
  </bpmn:process>
  <bpmn:process id="record" isExecutable="true">
    <bpmn:startEvent id="S3">
      <bpmn:extensionElements><q:source channel="connectivity-in" name="payload"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:serviceTask id="Record" implementation="record.hbs">
      <bpmn:extensionElements>
        <q:send channel="record-out" mode="native" contentType="application/json"/>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:endEvent id="E3"/>
    <bpmn:sequenceFlow id="r1" sourceRef="S3" targetRef="Record"/>
    <bpmn:sequenceFlow id="r2" sourceRef="Record" targetRef="E3"/>
  </bpmn:process>
</bpmn:definitions>
"#;

const RECORD_HBS: &str = r#"{"code":"{{payload.code}}","channel":"{{payload.channel}}","direction":"{{payload.direction}}","state":"{{payload.state}}","startedAt":"{{payload.startedAt}}","endedAt":"{{#if payload.endedAt}}{{payload.endedAt}}{{/if}}","ms":"{{#if payload.ms}}{{payload.ms}}{{/if}}"}"#;

fn connectivity_package(broker_port: u16) -> Vec<u8> {
    let root = temp_root("connectivity-pkg");
    let channels = format!(
        r#"channels:
  - name: mq-in
    transport: amqp
    codec: json
    cloudevents-mode: none
    host: 127.0.0.1
    port: {broker_port}
    username: env:CONNECTIVITY_IT_USER
    password: env:CONNECTIVITY_IT_PASSWORD
    queue: connectivity-it-in
  - name: mq-out
    direction: outbound
    transport: amqp
    bind: amqp10://127.0.0.1:{broker_port}/connectivity-it-out
    cloudevents-mode: none
  - name: emit-in
    transport: http
    bind: "POST /channels/emit"
    codec: json
    auth:
      scheme: apikey
      apikey:
        value: {API_KEY}
        header: X-Api-Key
  - name: connectivity-in
    transport: local
    codec: json
  - name: record-out
    direction: outbound
    transport: http
    bind: "http://${{CONNECTIVITY_IT_CAPTURE}}/connectivity"
"#
    );
    write(&root, "bpmn/conn.bpmn", BPMN);
    write(&root, "templates/out.hbs", r#"{"n":"{{payload.n}}"}"#);
    write(&root, "templates/record.hbs", RECORD_HBS);
    write(&root, "channels.yaml", &channels);
    write(
        &root,
        "package.yaml",
        "labels:\n  \"tenant\": \"t1\"\n  \"module\": \"conn\"\n  \"version\": \"1.0.0\"\nengine:\n  minContract: 1\n",
    );
    package(&root)
}

fn recorded(capture: &crate::archive_activation_conformance::Capture) -> Vec<serde_json::Value> {
    capture
        .bodies("/connectivity")
        .iter()
        .map(|body| serde_json::from_slice(body).expect("recorded event is JSON"))
        .collect()
}

fn find<'a>(
    events: &'a [serde_json::Value],
    channel: &str,
    state: &str,
) -> Option<&'a serde_json::Value> {
    events
        .iter()
        .find(|e| e["channel"] == channel && e["state"] == state)
}

fn instant(text: &serde_json::Value) -> time::OffsetDateTime {
    time::OffsetDateTime::parse(
        text.as_str().expect("timestamp"),
        &time::format_description::well_known::Rfc3339,
    )
    .expect("RFC 3339 timestamp")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "docker"]
async fn the_package_receives_the_outage_start_and_end_of_its_amqp_channels() {
    // `RUST_LOG` shows the engine's side of a failure.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let broker = tokio::task::spawn_blocking(Broker::start).await.unwrap();
    let (_pool, url) = fresh_db().await;
    let (sink, capture) = capture_server().await;
    std::env::set_var("CONNECTIVITY_IT_USER", "artemis");
    std::env::set_var("CONNECTIVITY_IT_PASSWORD", "artemis");
    std::env::set_var("CONNECTIVITY_IT_CAPTURE", sink.to_string());
    std::env::set_var("SUTRA_SINK_AMQP_USERNAME", "artemis");
    std::env::set_var("SUTRA_SINK_AMQP_PASSWORD", "artemis");

    let dir = temp_root("connectivity-dir");
    place_archive(&dir, "conn.sutra", &connectivity_package(broker.port));
    let mut config = archive_engine_config(dir, Some(url));
    config.connectivity_events_channel = Some("connectivity-in".to_string());
    let engine = sutra_engine::serve(config).await.expect("engine boots");
    let addr = engine.local_addr;

    // A send while the broker is up: the sink connects, nothing is reported.
    let (status, body) = post(addr, "/channels/emit", br#"{"n":"1"}"#).await;
    assert!(status < 300, "emit accepted: {status} {body}");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(recorded(&capture).is_empty(), "no outage, no event");

    // The outage: the intake loses its connection, and a send made during it cannot connect.
    let stopped_at = Instant::now();
    let name = broker.name.clone();
    tokio::task::spawn_blocking(move || stop_broker(&name))
        .await
        .unwrap();
    assert!(
        wait_until(60, || async {
            find(&recorded(&capture), "mq-in", "down").is_some()
        })
        .await,
        "the intake's outage start reaches the package: {:?}",
        recorded(&capture)
    );
    let (status, body) = post(addr, "/channels/emit", br#"{"n":"2"}"#).await;
    assert!(
        status < 300,
        "emit accepted while the broker is down: {status} {body}"
    );
    assert!(
        wait_until(60, || async {
            find(&recorded(&capture), "mq-out", "down").is_some()
        })
        .await,
        "the send channel's outage start reaches the package: {:?}",
        recorded(&capture)
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    let outage = stopped_at.elapsed();
    let name = broker.name.clone();
    tokio::task::spawn_blocking(move || restart_broker(&name))
        .await
        .unwrap();

    // Both ends come back: the intake reattaches, the outbox retries the send and it lands.
    assert!(
        wait_until(90, || async {
            let events = recorded(&capture);
            find(&events, "mq-in", "up").is_some() && find(&events, "mq-out", "up").is_some()
        })
        .await,
        "both outage ends reach the package: {:?}",
        recorded(&capture)
    );
    let events = recorded(&capture);
    for (channel, direction, code) in [
        ("mq-in", "inbound", "SUTRA.INBOUND.AMQP.OUTAGE"),
        ("mq-out", "outbound", "SUTRA.OUTBOUND.AMQP.OUTAGE"),
    ] {
        let down = find(&events, channel, "down").unwrap();
        let up = find(&events, channel, "up").unwrap();
        assert_eq!(down["direction"], direction, "{down}");
        assert_eq!(down["code"], format!("{code}_STARTED"), "{down}");
        assert_eq!(up["code"], format!("{code}_ENDED"), "{up}");
        assert_eq!(down["endedAt"], "", "a down event has no end: {down}");
        assert_eq!(
            up["startedAt"], down["startedAt"],
            "one window: {down} {up}"
        );
        let (started, ended) = (instant(&up["startedAt"]), instant(&up["endedAt"]));
        assert!(ended > started, "{up}");
        let ms: u64 = up["ms"].as_str().unwrap().parse().expect("ms is a number");
        assert!(ms > 0, "{up}");
        let window = (ended - started).whole_milliseconds();
        assert!(
            (window - ms as i128).abs() < 1_000,
            "ms agrees with the timestamps: {up}"
        );
    }
    let inbound_ms: u64 = find(&events, "mq-in", "up").unwrap()["ms"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        inbound_ms as u128 >= (outage.as_millis() / 2),
        "the intake's window covers the outage ({outage:?}): {inbound_ms} ms"
    );
    // One window per channel: one down and one up each, nothing else.
    for channel in ["mq-in", "mq-out"] {
        for state in ["down", "up"] {
            let n = events
                .iter()
                .filter(|e| e["channel"] == channel && e["state"] == state)
                .count();
            assert_eq!(n, 1, "{channel} {state}: {events:?}");
        }
    }
    assert_eq!(events.len(), 4, "{events:?}");
    engine.shutdown().await;
    drop(broker);
}
