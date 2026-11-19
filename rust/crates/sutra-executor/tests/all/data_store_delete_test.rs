//! Row deletes from a data task (`<q:store delete="true">`), against the in-memory reference
//! store: the loader surface and its refusals, plain and conditional deletes, the conflict and
//! its error boundary, and a delete rolled back with its transaction scope.

use std::rc::Rc;

use crate::common::*;
use sutra_bpmn::{BpmnModelLoader, Node};
use sutra_executor::{DataStore, InMemoryDataStore, TaskRegistry, TokenExecutor};

/// A process that optionally reads `key` (through `refRead`), optionally writes `touch` to it
/// (through `refPut`, which bumps its revision), then deletes it through `refDel` whose
/// `<q:store>` is `del`. `boundary` adds an error boundary catching the data-store conflict.
fn delete_process(del: &str, read: bool, touch: bool, boundary: bool) -> String {
    let read_task = if read {
        r#"<bpmn:serviceTask id="Read">
          <bpmn:dataInputAssociation>
            <bpmn:sourceRef>refRead</bpmn:sourceRef><bpmn:targetRef>doCur</bpmn:targetRef>
          </bpmn:dataInputAssociation>
        </bpmn:serviceTask>"#
    } else {
        r#"<bpmn:task id="Read"/>"#
    };
    let touch_task = if touch {
        r#"<bpmn:serviceTask id="Touch">
          <bpmn:dataOutputAssociation>
            <bpmn:sourceRef>doTouch</bpmn:sourceRef><bpmn:targetRef>refPut</bpmn:targetRef>
          </bpmn:dataOutputAssociation>
        </bpmn:serviceTask>"#
    } else {
        r#"<bpmn:task id="Touch"/>"#
    };
    let boundary = if boundary {
        r#"<bpmn:boundaryEvent id="Lost" attachedToRef="Del">
          <bpmn:errorEventDefinition errorRef="conflict"/>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="LostEnd"/>
        <bpmn:sequenceFlow id="f9" sourceRef="Lost" targetRef="LostEnd"/>"#
    } else {
        ""
    };
    format!(
        r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:error id="conflict" errorCode="SUTRA.RUNTIME.DATASTORE.CONFLICT"/>
      <bpmn:process id="del">
        <bpmn:dataObject id="doCur" name="current"/>
        <bpmn:dataObject id="doTouch" name="touch"/>
        <bpmn:dataStore id="items" name="items"/>
        <bpmn:dataStoreReference id="refRead" dataStoreRef="items">
          <bpmn:extensionElements><q:store key="id"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:dataStoreReference id="refPut" dataStoreRef="items">
          <bpmn:extensionElements><q:store key="id"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:dataStoreReference id="refDel" dataStoreRef="items">
          <bpmn:extensionElements>{del}</bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:startEvent id="S"/>
        {read_task}
        {touch_task}
        <bpmn:serviceTask id="Del">
          <bpmn:dataOutputAssociation><bpmn:targetRef>refDel</bpmn:targetRef></bpmn:dataOutputAssociation>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Read"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Read" targetRef="Touch"/>
        <bpmn:sequenceFlow id="f3" sourceRef="Touch" targetRef="Del"/>
        <bpmn:sequenceFlow id="f4" sourceRef="Del" targetRef="E"/>
        {boundary}
      </bpmn:process>
    </bpmn:definitions>"#
    )
}

const DELETE: &str = r#"<q:store key="id" delete="true"/>"#;
const DELETE_IF_UNCHANGED: &str = r#"<q:store key="id" delete="true" expect="unchanged"/>"#;

async fn items(rows: &[&str]) -> Rc<InMemoryDataStore> {
    let store = Rc::new(InMemoryDataStore::new("items"));
    for key in rows {
        store.put(key, string(&format!("v-{key}"))).await.unwrap();
    }
    store
}

async fn run(
    store: &Rc<InMemoryDataStore>,
    bpmn: &str,
    id: &str,
) -> Result<sutra_executor::ExecResult, sutra_executor::ExecError> {
    let process = proc(bpmn, "del");
    let lookup = Rc::clone(store);
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "items").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .build()
        .execute_sync(
            &process,
            vars(&[("id", string(id)), ("touch", string("touched"))]),
        )
        .await
}

#[test]
fn the_loader_reads_a_delete_with_no_source_ref() {
    let process = proc(
        &delete_process(DELETE_IF_UNCHANGED, false, false, false),
        "del",
    );
    let Node::DataTask { data_mapping, .. } = process.node("Del").unwrap() else {
        panic!("data task")
    };
    let write = &data_mapping.store_writes[0];
    assert!(write.delete);
    assert!(write.expect_unchanged);
    assert!(write.value_var.is_empty());
}

#[test]
fn a_delete_refuses_attributes_that_make_no_sense_on_it() {
    for (store, needle) in [
        (r#"<q:store key="id" delete="true" field="x"/>"#, "@field"),
        (
            r#"<q:store key="id" delete="true" forUpdate="true"/>"#,
            "@forUpdate",
        ),
        (
            r#"<q:store key="id" delete="true" expect="absent"/>"#,
            "expect=\"absent\"",
        ),
        (
            r#"<q:store key="id" delete="true" query="prefix"/>"#,
            "@query",
        ),
        (r#"<q:store key="id" delete="true" to="'z'"/>"#, "@to"),
        (r#"<q:store key="id" delete="true" limit="5"/>"#, "@limit"),
        (r#"<q:store key="id" delete="true" after="x"/>"#, "@after"),
        (r#"<q:store key="id" delete="true" rows="false"/>"#, "@rows"),
        (
            r#"<q:store key="id" delete="true"><q:aggregate fn="count"/></q:store>"#,
            "<q:aggregate>",
        ),
        (r#"<q:store key="id" delete="yes"/>"#, "not a boolean"),
    ] {
        let e = BpmnModelLoader::new()
            .load(delete_process(store, false, false, false).as_bytes())
            .expect_err(store);
        assert_eq!(e.code, "SUTRA.PARSE.STORE.DELETE_INVALID", "{store}");
        assert!(e.message.contains(needle), "{store}: {}", e.message);
    }
}

#[test]
fn a_delete_reference_cannot_be_read() {
    // refRead is read by the Read task; make it the delete.
    let bpmn = delete_process(DELETE, true, false, false).replacen(
        r#"<q:store key="id"/>"#,
        r#"<q:store key="id" delete="true"/>"#,
        1,
    );
    let e = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect_err("a read of a delete");
    assert_eq!(e.code, "SUTRA.PARSE.STORE.DELETE_INVALID");
    assert!(e.message.contains("a delete is a write"), "{}", e.message);
}

#[test]
fn delete_false_is_an_ordinary_write_that_needs_a_source_ref() {
    let bpmn =
        delete_process(DELETE, false, true, false).replace("delete=\"true\"", "delete=\"false\"");
    let e = BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .expect_err("a write with nothing to write");
    assert_eq!(e.code, "SUTRA.PARSE.DATA_ASSOCIATION.UNSUPPORTED");
    let process = proc(
        &bpmn.replace(
            "<bpmn:targetRef>refDel",
            "<bpmn:sourceRef>doTouch</bpmn:sourceRef><bpmn:targetRef>refDel",
        ),
        "del",
    );
    let Node::DataTask { data_mapping, .. } = process.node("Del").unwrap() else {
        panic!("data task")
    };
    assert!(!data_mapping.store_writes[0].delete);
    assert_eq!(data_mapping.store_writes[0].value_var, "touch");
}

#[tokio::test]
async fn a_delete_removes_the_row_and_leaves_the_others() {
    let store = items(&["a", "b"]).await;
    run(&store, &delete_process(DELETE, false, false, false), "a")
        .await
        .unwrap();
    assert_eq!(store.get("a").await.unwrap(), None);
    assert_eq!(store.get("b").await.unwrap(), Some(string("v-b")));
}

#[tokio::test]
async fn deleting_a_missing_key_is_a_no_op() {
    let store = items(&["b"]).await;
    let result = run(&store, &delete_process(DELETE, false, false, false), "a")
        .await
        .unwrap();
    assert!(result.visited_nodes.contains("E"));
    assert_eq!(store.snapshot().len(), 1);
}

#[tokio::test]
async fn an_unchanged_delete_applies_when_nothing_changed_since_the_read() {
    let store = items(&["a"]).await;
    run(
        &store,
        &delete_process(DELETE_IF_UNCHANGED, true, false, false),
        "a",
    )
    .await
    .unwrap();
    assert_eq!(store.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn an_unchanged_delete_conflicts_when_the_row_changed_since_the_read() {
    let store = items(&["a"]).await;
    let e = run(
        &store,
        &delete_process(DELETE_IF_UNCHANGED, true, true, false),
        "a",
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.DATASTORE.CONFLICT");
    assert!(e.message().contains("deleting from"), "{}", e.message());
    assert_eq!(
        store.get("a").await.unwrap(),
        Some(string("touched")),
        "the row was not deleted"
    );
}

#[tokio::test]
async fn an_unchanged_delete_of_a_key_never_read_conflicts_when_the_key_exists() {
    // Never read = expected absent: a present row is a change.
    let store = items(&["a"]).await;
    let e = run(
        &store,
        &delete_process(DELETE_IF_UNCHANGED, false, false, false),
        "a",
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.DATASTORE.CONFLICT");
    assert!(e.message().contains("was absent"), "{}", e.message());
    assert!(store.get("a").await.unwrap().is_some());

    // ... and is a no-op when it is still absent.
    let empty = items(&[]).await;
    run(
        &empty,
        &delete_process(DELETE_IF_UNCHANGED, true, false, false),
        "a",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn a_delete_conflict_is_catchable_by_an_error_boundary() {
    let store = items(&["a"]).await;
    let result = run(
        &store,
        &delete_process(DELETE_IF_UNCHANGED, true, true, true),
        "a",
    )
    .await
    .unwrap();
    assert!(
        result.visited_nodes.contains("LostEnd"),
        "{:?}",
        result.visited_nodes
    );
    assert!(!result.visited_nodes.contains("E"));
}

/// A delete inside a `<bpmn:transaction>`: committed when the scope completes, rolled back when
/// a later step of the scope cancels it.
const TX_DELETE: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="del">
        <bpmn:dataStore id="items" name="items"/>
        <bpmn:startEvent id="S"/>
        <bpmn:transaction id="Tx">
          <bpmn:dataStoreReference id="refDel" dataStoreRef="items">
            <bpmn:extensionElements><q:store key="id" delete="true"/></bpmn:extensionElements>
          </bpmn:dataStoreReference>
          <bpmn:startEvent id="TxS"/>
          <bpmn:serviceTask id="Del">
            <bpmn:dataOutputAssociation><bpmn:targetRef>refDel</bpmn:targetRef></bpmn:dataOutputAssociation>
          </bpmn:serviceTask>
          <bpmn:exclusiveGateway id="Check" default="tf2"/>
          <bpmn:endEvent id="TxEnd"/>
          <bpmn:endEvent id="TxCancel"><bpmn:cancelEventDefinition/></bpmn:endEvent>
          <bpmn:sequenceFlow id="tf0" sourceRef="TxS" targetRef="Del"/>
          <bpmn:sequenceFlow id="tf1" sourceRef="Del" targetRef="Check"/>
          <bpmn:sequenceFlow id="tf2" sourceRef="Check" targetRef="TxEnd"/>
          <bpmn:sequenceFlow id="tf3" sourceRef="Check" targetRef="TxCancel">
            <bpmn:conditionExpression>cancel</bpmn:conditionExpression>
          </bpmn:sequenceFlow>
        </bpmn:transaction>
        <bpmn:boundaryEvent id="TxCancelled" attachedToRef="Tx"><bpmn:cancelEventDefinition/></bpmn:boundaryEvent>
        <bpmn:endEvent id="Done"/>
        <bpmn:endEvent id="Rejected"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Tx"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Tx" targetRef="Done"/>
        <bpmn:sequenceFlow id="f3" sourceRef="TxCancelled" targetRef="Rejected"/>
      </bpmn:process>
    </bpmn:definitions>"#;

async fn run_tx(store: &Rc<InMemoryDataStore>, cancel: bool) -> sutra_executor::ExecResult {
    let process = proc(TX_DELETE, "del");
    let lookup = Rc::clone(store);
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "items").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .build()
        .execute_sync(
            &process,
            vars(&[("id", string("a")), ("cancel", boolean(cancel))]),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn a_delete_in_a_transaction_scope_commits_with_it() {
    let store = items(&["a"]).await;
    let result = run_tx(&store, false).await;
    assert!(result.visited_nodes.contains("Done"));
    assert_eq!(store.get("a").await.unwrap(), None);
}

#[tokio::test]
async fn a_delete_in_a_cancelled_transaction_scope_is_rolled_back() {
    let store = items(&["a"]).await;
    let result = run_tx(&store, true).await;
    assert!(result.visited_nodes.contains("Rejected"));
    assert_eq!(store.get("a").await.unwrap(), Some(string("v-a")));
}
