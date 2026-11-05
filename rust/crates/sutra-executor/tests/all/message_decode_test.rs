//! The `<q:decode>` data-task step: where it runs in a data task, what it binds, and how it fails.
//! The codec side is a stand-in here; `sutra-channels` tests the real one.

use std::cell::RefCell;
use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::qbindings::MessageDecodeBinding;
use sutra_executor::{
    DataStore, DeploymentId, InMemoryDataStore, MessageDecodeFailure, MessageDecodeFailureKind,
    MessageDecodeOutcome, MessageDecodeSeam, TaskRegistry, TokenExecutor, Variables,
};

/// Binds `{text, contentType}` for the text it is given, or fails as told. Records what it saw.
#[derive(Default)]
struct StubSeam {
    fail: Option<MessageDecodeFailureKind>,
    mask: bool,
    seen: RefCell<Vec<(String, Option<String>)>>,
}

impl MessageDecodeSeam for StubSeam {
    fn decode_message(
        &self,
        _deployment: &DeploymentId,
        _binding: &MessageDecodeBinding,
        text: &str,
        content_type: Option<&str>,
        _vars: &Variables,
    ) -> Result<MessageDecodeOutcome, MessageDecodeFailure> {
        self.seen
            .borrow_mut()
            .push((text.to_string(), content_type.map(str::to_string)));
        if let Some(kind) = self.fail {
            return Err(MessageDecodeFailure {
                kind,
                detail: "E.CODE@/path".to_string(),
            });
        }
        Ok(MessageDecodeOutcome {
            value: fmap(&[("text", string(text))]),
            masked: self.mask.then(|| string("[REDACTED]")),
        })
    }
}

fn flow(extras: &str, task_children: &str, after_task: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                          xmlns:q="urn:sutra:q:1.0">
          {extras}
          <bpmn:dataStore id="notes" name="notes"/>
          <bpmn:process id="p">
            <bpmn:dataStoreReference id="read" dataStoreRef="notes">
              <bpmn:extensionElements><q:store key="'k'"/></bpmn:extensionElements>
            </bpmn:dataStoreReference>
            <bpmn:dataStoreReference id="write" dataStoreRef="notes">
              <bpmn:extensionElements><q:store key="'out'"/></bpmn:extensionElements>
            </bpmn:dataStoreReference>
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T">{task_children}</bpmn:serviceTask>
            {after_task}
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#
    )
}

const DECODE_BODY: &str = r#"<bpmn:extensionElements>
      <q:decode name="message" expression="body" codec="json" contentType="kind"/>
    </bpmn:extensionElements>"#;

fn executor(seam: Option<Rc<StubSeam>>, store: Rc<InMemoryDataStore>) -> TokenExecutor {
    let mut builder = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "notes").then(|| Rc::clone(&store) as Rc<dyn DataStore>)
        });
    if let Some(seam) = seam {
        builder = builder.with_message_decode_seam(seam);
    }
    builder.build()
}

#[tokio::test]
async fn decode_step_runs_after_reads_before_assignments() {
    let store = Rc::new(InMemoryDataStore::new("notes"));
    store
        .put("k", fmap(&[("body", string("stored text"))]))
        .await
        .unwrap();
    let process = proc(
        &flow(
            "",
            r#"<bpmn:extensionElements>
                 <q:decode name="first" expression="row.body" codec="json"/>
                 <q:decode name="second" expression="first.text" codec="json"/>
               </bpmn:extensionElements>
               <bpmn:dataInputAssociation>
                 <bpmn:sourceRef>read</bpmn:sourceRef><bpmn:targetRef>row</bpmn:targetRef>
               </bpmn:dataInputAssociation>
               <bpmn:dataInputAssociation>
                 <bpmn:assignment><bpmn:from>second.text</bpmn:from><bpmn:to>copy</bpmn:to></bpmn:assignment>
               </bpmn:dataInputAssociation>
               <bpmn:dataOutputAssociation>
                 <bpmn:sourceRef>copy</bpmn:sourceRef><bpmn:targetRef>write</bpmn:targetRef>
               </bpmn:dataOutputAssociation>"#,
            "",
        ),
        "p",
    );
    let seam = Rc::new(StubSeam::default());
    let result = executor(Some(Rc::clone(&seam)), Rc::clone(&store))
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();
    assert_eq!(result.output("copy"), Some(&string("stored text")));
    assert_eq!(
        store.get("out").await.unwrap(),
        Some(string("stored text")),
        "the write runs last and sees the assignment"
    );
    assert_eq!(seam.seen.borrow().len(), 2, "document order, both ran");
}

#[tokio::test]
async fn decode_passes_the_content_type_expression_and_binds_the_companion() {
    let process = proc(&flow("", DECODE_BODY, ""), "p");
    let seam = Rc::new(StubSeam {
        mask: true,
        ..StubSeam::default()
    });
    let result = executor(
        Some(Rc::clone(&seam)),
        Rc::new(InMemoryDataStore::new("notes")),
    )
    .execute_sync(
        &process,
        vars(&[("body", string("x")), ("kind", string("application/json"))]),
    )
    .await
    .unwrap();
    assert_eq!(
        seam.seen.borrow()[0],
        ("x".to_string(), Some("application/json".to_string()))
    );
    assert_eq!(
        result.output("message"),
        Some(&fmap(&[("text", string("x"))]))
    );
    assert_eq!(
        result.output("message.redacted"),
        Some(&string("[REDACTED]"))
    );
}

#[tokio::test]
async fn decode_not_text_on_null_input() {
    let process = proc(&flow("", DECODE_BODY, ""), "p");
    for input in [vars(&[]), vars(&[("body", num(7))])] {
        let seam = Rc::new(StubSeam::default());
        let e = executor(
            Some(Rc::clone(&seam)),
            Rc::new(InMemoryDataStore::new("notes")),
        )
        .execute_sync(&process, input)
        .await
        .unwrap_err();
        assert_eq!(e.code(), "SUTRA.RUNTIME.DECODE.NOT_TEXT");
        assert!(seam.seen.borrow().is_empty(), "nothing reached the codec");
    }
}

#[tokio::test]
async fn decode_failure_is_catchable_or_fatal() {
    const CATCHES: &str = r#"<bpmn:boundaryEvent id="B" attachedToRef="T">
        <bpmn:errorEventDefinition errorRef="bad"/>
      </bpmn:boundaryEvent>
      <bpmn:endEvent id="Caught"/>
      <bpmn:sequenceFlow id="f3" sourceRef="B" targetRef="Caught"/>"#;
    for (kind, code) in [
        (
            MessageDecodeFailureKind::Fatal,
            "SUTRA.RUNTIME.DECODE.FAILED",
        ),
        (
            MessageDecodeFailureKind::NotFound,
            "SUTRA.RUNTIME.DECODE.FAILED",
        ),
        (
            MessageDecodeFailureKind::TooLarge,
            "SUTRA.RUNTIME.DECODE.TOO_LARGE",
        ),
    ] {
        let seam = || {
            Some(Rc::new(StubSeam {
                fail: Some(kind),
                ..StubSeam::default()
            }))
        };
        let input = || vars(&[("body", string("SECRET-TEXT"))]);

        let fatal = executor(seam(), Rc::new(InMemoryDataStore::new("notes")))
            .execute_sync(&proc(&flow("", DECODE_BODY, ""), "p"), input())
            .await
            .unwrap_err();
        assert_eq!(fatal.code(), code);
        assert!(!fatal.to_string().contains("SECRET-TEXT"), "{fatal}");

        let error = format!(r#"<bpmn:error id="bad" errorCode="{code}"/>"#);
        let caught = executor(seam(), Rc::new(InMemoryDataStore::new("notes")))
            .execute_sync(&proc(&flow(&error, DECODE_BODY, CATCHES), "p"), input())
            .await
            .unwrap();
        assert!(caught.visited_nodes.contains("Caught"), "{code}");
        assert_eq!(caught.output("message"), None, "nothing was bound");
    }
}

#[tokio::test]
async fn a_decode_step_with_no_seam_fails_closed() {
    let e = executor(None, Rc::new(InMemoryDataStore::new("notes")))
        .execute_sync(
            &proc(&flow("", DECODE_BODY, ""), "p"),
            vars(&[("body", string("x"))]),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.DECODE.FAILED");
}

/// A called process runs over the caller's variables, so it can capture `event.body` itself.
#[tokio::test]
async fn call_activity_sees_the_callers_event() {
    let parent = proc(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="parent">
            <bpmn:startEvent id="S"/>
            <bpmn:callActivity id="C" calledElement="child"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="C"/>
            <bpmn:sequenceFlow id="f2" sourceRef="C" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#,
        "parent",
    );
    let child = proc(
        r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="child">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="T">
              <bpmn:dataInputAssociation>
                <bpmn:assignment><bpmn:from>event.body</bpmn:from><bpmn:to>captured</bpmn:to></bpmn:assignment>
              </bpmn:dataInputAssociation>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="T"/>
            <bpmn:sequenceFlow id="f2" sourceRef="T" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#,
        "child",
    );
    let child = std::sync::Arc::new(child);
    let result = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_process_resolver(move |id| Ok((id == "child").then(|| std::sync::Arc::clone(&child))))
        .build()
        .execute_sync(
            &parent,
            vars(&[("event", fmap(&[("body", string("the message"))]))]),
        )
        .await
        .unwrap();
    assert_eq!(result.output("captured"), Some(&string("the message")));
}
