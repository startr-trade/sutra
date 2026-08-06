//! The BPMN data store and transaction sub-processes: data-task read/assign/write, task I/O
//! scoping, fail-closed data associations, optimistic-concurrency revision conflicts, and
//! transaction commit/rollback with compensation.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::common::*;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use sutra_bpmn::model::{Assignment, StoreRead, StoreWrite};
use sutra_bpmn::Node;
use sutra_executor::executor::{feel_condition_evaluator, feel_value_evaluator};
use sutra_executor::{
    DataStore, DataStoreTx, InMemoryDataStore, StoreError, TaskRegistry, TokenExecutor,
};
use sutra_feel::FeelValue;

fn account(balance: i64, frozen: bool) -> FeelValue {
    fmap(&[
        ("balance", FeelValue::Number(BigDecimal::from(balance))),
        ("frozen", boolean(frozen)),
    ])
}

// ---- DataTaskTest ------------------------------------------------------------------

const DATA_TASK_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="transfer">
        <bpmn:dataObject id="doFromAccount" name="fromAccount"/>
        <bpmn:dataObject id="doNewBal" name="newBal"/>
        <bpmn:dataStore id="accounts" name="accounts"/>
        <bpmn:dataStoreReference id="refFromRead" dataStoreRef="accounts">
          <bpmn:extensionElements><q:store key="fromId" forUpdate="true"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:dataStoreReference id="refFromWrite" dataStoreRef="accounts">
          <bpmn:extensionElements><q:store key="fromId" field="balance"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="LoadFrom" name="Load payer">
          <bpmn:dataInputAssociation>
            <bpmn:sourceRef>refFromRead</bpmn:sourceRef>
            <bpmn:targetRef>doFromAccount</bpmn:targetRef>
          </bpmn:dataInputAssociation>
        </bpmn:serviceTask>
        <bpmn:serviceTask id="Compute" name="Compute new balance">
          <bpmn:dataInputAssociation>
            <bpmn:assignment><bpmn:from>fromAccount.balance - amount</bpmn:from><bpmn:to>newBal</bpmn:to></bpmn:assignment>
          </bpmn:dataInputAssociation>
        </bpmn:serviceTask>
        <bpmn:serviceTask id="Persist" name="Persist">
          <bpmn:dataOutputAssociation>
            <bpmn:sourceRef>doNewBal</bpmn:sourceRef>
            <bpmn:targetRef>refFromWrite</bpmn:targetRef>
          </bpmn:dataOutputAssociation>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="LoadFrom"/>
        <bpmn:sequenceFlow id="f2" sourceRef="LoadFrom" targetRef="Compute"/>
        <bpmn:sequenceFlow id="f3" sourceRef="Compute" targetRef="Persist"/>
        <bpmn:sequenceFlow id="f4" sourceRef="Persist" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

#[test]
fn no_impl_service_tasks_with_data_ops_parse_to_data_tasks() {
    let process = proc(DATA_TASK_BPMN, "transfer");

    match process.node("LoadFrom").unwrap() {
        Node::DataTask { data_mapping, .. } => {
            assert_eq!(
                data_mapping.store_reads,
                vec![StoreRead {
                    store: "accounts".to_string(),
                    key_expression: "fromId".to_string(),
                    for_update: true,
                    target_var: "fromAccount".to_string(),
                }]
            );
            assert!(data_mapping.assignments.is_empty());
        }
        other => panic!("expected DataTask, got {other:?}"),
    }
    match process.node("Compute").unwrap() {
        Node::DataTask { data_mapping, .. } => assert_eq!(
            data_mapping.assignments,
            vec![Assignment {
                expression: "fromAccount.balance - amount".to_string(),
                target_var: "newBal".to_string(),
            }]
        ),
        other => panic!("expected DataTask, got {other:?}"),
    }
    match process.node("Persist").unwrap() {
        Node::DataTask { data_mapping, .. } => assert_eq!(
            data_mapping.store_writes,
            vec![StoreWrite {
                store: "accounts".to_string(),
                key_expression: "fromId".to_string(),
                field: Some("balance".to_string()),
                value_var: "newBal".to_string(),
                expect_unchanged: false,
            }]
        ),
        other => panic!("expected DataTask, got {other:?}"),
    }
}

#[tokio::test]
async fn data_task_reads_assigns_and_writes_the_store() {
    let process = proc(DATA_TASK_BPMN, "transfer");

    let accounts = Rc::new(InMemoryDataStore::new("accounts"));
    accounts.put("alice", account(100, false)).await.unwrap();
    let lookup = Rc::clone(&accounts);

    let result = TokenExecutor::builder(TaskRegistry::new())
        .with_value_evaluator(feel_value_evaluator())
        .with_data_stores(move |_, name| {
            (name == "accounts").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .build()
        .execute_sync(
            &process,
            vars(&[("fromId", string("alice")), ("amount", num(30))]),
        )
        .await
        .unwrap();

    // Compute wrote newBal = 100 - 30 into the process scope.
    assert_eq!(
        result.output("newBal"),
        Some(&FeelValue::Number(BigDecimal::from(70)))
    );

    // Persist wrote it back into ONLY the balance field; frozen is preserved.
    let alice = accounts.get("alice").await.unwrap().expect("alice present");
    let FeelValue::Map(alice) = alice else {
        panic!("expected map, got {alice:?}")
    };
    assert_eq!(alice.get("balance"), Some(&FeelValue::Number(70.into())));
    assert_eq!(alice.get("frozen"), Some(&boolean(false)));
}

// ---- DataMappingTest ------------------------------------------------------------------

#[tokio::test]
async fn scoped_task_sees_only_mapped_input_and_writes_only_mapped_output() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:dataObject id="doA" name="a"/>
            <bpmn:dataObject id="doReport" name="report"/>
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Seed" implementation="${seed}"/>
            <bpmn:serviceTask id="Scoped" implementation="${scoped}">
              <bpmn:dataInputAssociation><bpmn:sourceRef>doA</bpmn:sourceRef></bpmn:dataInputAssociation>
              <bpmn:dataOutputAssociation><bpmn:targetRef>doReport</bpmn:targetRef></bpmn:dataOutputAssociation>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Seed"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Seed" targetRef="Scoped"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Scoped" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    match process.node("Scoped").unwrap() {
        Node::ServiceTask { data_mapping, .. } => {
            assert_eq!(data_mapping.inputs, vec!["a"]);
            assert_eq!(data_mapping.outputs, vec!["report"]);
        }
        other => panic!("expected ServiceTask, got {other:?}"),
    }

    let registry = TaskRegistry::new()
        .register("seed", |_, _| {
            ok_map(&[("a", string("visible")), ("b", string("hidden"))])
        })
        .register("scoped", |_, ctx| {
            let a = ctx
                .variable("a")
                .map(sutra_feel::value::canonical_string_of)
                .unwrap_or_else(|| "MISSING".to_string());
            let b = ctx
                .variable("b")
                .map(sutra_feel::value::canonical_string_of)
                .unwrap_or_else(|| "MISSING".to_string());
            ok_map(&[
                ("report", string(&format!("a={a};b={b}"))),
                ("extra", string("should-be-dropped")),
            ])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    // The scoped task saw 'a' but NOT 'b'.
    assert_eq!(
        result.output("report"),
        Some(&string("a=visible;b=MISSING"))
    );
    // Its un-mapped output key was dropped.
    assert_eq!(result.output("extra"), None);
    // The shared 'a'/'b' set by Seed are untouched.
    assert_eq!(result.output("a"), Some(&string("visible")));
    assert_eq!(result.output("b"), Some(&string("hidden")));
}

#[tokio::test]
async fn association_free_task_keeps_shared_scope() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Seed" implementation="${seed}"/>
            <bpmn:serviceTask id="Plain" implementation="${plain}"/>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Seed"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Seed" targetRef="Plain"/>
            <bpmn:sequenceFlow id="f3" sourceRef="Plain" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let process = proc(bpmn, "p1");
    match process.node("Plain").unwrap() {
        Node::ServiceTask { data_mapping, .. } => assert!(data_mapping.is_empty()),
        other => panic!("expected ServiceTask, got {other:?}"),
    }

    let registry = TaskRegistry::new()
        .register("seed", |_, _| ok_map(&[("shared", string("yes"))]))
        .register("plain", |_, ctx| {
            let saw = ctx
                .variable("shared")
                .map(sutra_feel::value::canonical_string_of)
                .unwrap_or_else(|| "MISSING".to_string());
            ok_map(&[("sawShared", string(&saw)), ("anyKey", string("kept"))])
        });
    let result = TokenExecutor::builder(registry)
        .build()
        .execute_sync(&process, vars(&[]))
        .await
        .unwrap();

    assert_eq!(result.output("sawShared"), Some(&string("yes")));
    assert_eq!(result.output("anyKey"), Some(&string("kept")));
}

#[test]
fn data_association_on_script_task_fails_closed() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:dataObject id="doA" name="a"/>
            <bpmn:startEvent id="S"/>
            <bpmn:scriptTask id="Sc">
              <bpmn:script>derive.hbs</bpmn:script>
              <bpmn:dataInputAssociation><bpmn:sourceRef>doA</bpmn:sourceRef></bpmn:dataInputAssociation>
            </bpmn:scriptTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Sc"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Sc" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert_eq!(
        e.code,
        sutra_bpmn::codes::PARSE_DATA_ASSOCIATION_UNSUPPORTED
    );
}

#[test]
fn transformation_in_data_association_fails_closed() {
    let bpmn = r#"<?xml version="1.0"?>
        <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
          <bpmn:process id="p1">
            <bpmn:dataObject id="doA" name="a"/>
            <bpmn:startEvent id="S"/>
            <bpmn:serviceTask id="Scoped" implementation="${scoped}">
              <bpmn:dataInputAssociation>
                <bpmn:sourceRef>doA</bpmn:sourceRef>
                <bpmn:transformation>y</bpmn:transformation>
              </bpmn:dataInputAssociation>
            </bpmn:serviceTask>
            <bpmn:endEvent id="E"/>
            <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Scoped"/>
            <bpmn:sequenceFlow id="f2" sourceRef="Scoped" targetRef="E"/>
          </bpmn:process>
        </bpmn:definitions>"#;
    let e = sutra_bpmn::BpmnModelLoader::new()
        .load(bpmn.as_bytes())
        .unwrap_err();
    assert_eq!(
        e.code,
        sutra_bpmn::codes::PARSE_DATA_ASSOCIATION_UNSUPPORTED
    );
}

// ---- DataTaskOptimisticConcurrencyTest -----------------------------------------------

const CAS_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="cas">
        <bpmn:dataStore id="accounts" name="accounts"/>
        <bpmn:startEvent id="S"/>
        <bpmn:dataObject id="doVal" name="val"/>
        <bpmn:dataStoreReference id="refRead" dataStoreRef="accounts">
          <bpmn:extensionElements><q:store key="'k'"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:dataStoreReference id="refWrite" dataStoreRef="accounts">
          <bpmn:extensionElements><q:store key="'k'" expect="unchanged"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:serviceTask id="Rw">
          <bpmn:dataInputAssociation>
            <bpmn:sourceRef>refRead</bpmn:sourceRef><bpmn:targetRef>doVal</bpmn:targetRef>
          </bpmn:dataInputAssociation>
          <bpmn:dataOutputAssociation>
            <bpmn:sourceRef>doVal</bpmn:sourceRef><bpmn:targetRef>refWrite</bpmn:targetRef>
          </bpmn:dataOutputAssociation>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Rw"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Rw" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#;

/// A rev-tracking store double; `bump_after_read` simulates a concurrent writer bumping the
/// revision between this instance's read and its compare-and-set write.
struct RevStore {
    data: RefCell<HashMap<String, FeelValue>>,
    rev: RefCell<i64>,
    bump_after_read: RefCell<bool>,
}

impl RevStore {
    fn new(rev: i64, bump_after_read: bool) -> RevStore {
        RevStore {
            data: RefCell::new(HashMap::new()),
            rev: RefCell::new(rev),
            bump_after_read: RefCell::new(bump_after_read),
        }
    }
}

#[async_trait(?Send)]
impl DataStore for RevStore {
    fn name(&self) -> &str {
        "accounts"
    }
    async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
        Ok(self.data.borrow().get(key).cloned())
    }
    async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
        self.data.borrow_mut().insert(key.to_string(), value);
        *self.rev.borrow_mut() += 1;
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.data.borrow_mut().remove(key);
        Ok(())
    }
    async fn revision(&self, _key: &str) -> Result<i64, StoreError> {
        let r = *self.rev.borrow();
        if *self.bump_after_read.borrow() {
            *self.rev.borrow_mut() += 1; // concurrent writer wins the race
            *self.bump_after_read.borrow_mut() = false;
        }
        Ok(r)
    }
    async fn put_if_revision(
        &self,
        key: &str,
        value: FeelValue,
        expected_rev: i64,
    ) -> Result<bool, StoreError> {
        if expected_rev != *self.rev.borrow() {
            return Ok(false); // stale — a concurrent write bumped the revision
        }
        self.data.borrow_mut().insert(key.to_string(), value);
        *self.rev.borrow_mut() += 1;
        Ok(true)
    }
    async fn begin(&self) -> Result<Option<Rc<dyn DataStoreTx>>, StoreError> {
        Ok(None) // autocommit test double
    }
}

async fn run_cas(
    store: Rc<RevStore>,
) -> Result<sutra_executor::ExecResult, sutra_executor::ExecError> {
    let process = proc(CAS_BPMN, "cas");
    let lookup = Rc::clone(&store);
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "accounts").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .build()
        .execute_sync(&process, vars(&[]))
        .await
}

#[tokio::test]
async fn stale_revision_conflicts() {
    let store = Rc::new(RevStore::new(1, true));
    store
        .data
        .borrow_mut()
        .insert("k".to_string(), string("v0"));

    let e = run_cas(Rc::clone(&store)).await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.DATASTORE.CONFLICT");
    assert_eq!(
        store.data.borrow().get("k"),
        Some(&string("v0")),
        "the conflicting write never landed"
    );
}

#[tokio::test]
async fn unchanged_revision_writes_normally() {
    let store = Rc::new(RevStore::new(1, false));
    store
        .data
        .borrow_mut()
        .insert("k".to_string(), string("v0"));

    let result = run_cas(Rc::clone(&store)).await.unwrap();
    assert!(result.visited_nodes.contains("E"));
    assert_eq!(store.data.borrow().get("k"), Some(&string("v0")));
}

// ---- TransactionSubProcessTest ----------------------------------------------------------

const TX_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="transfer">
        <bpmn:dataStore id="accounts" name="accounts"/>
        <bpmn:startEvent id="S"/>
        <bpmn:transaction id="Tx" name="Transfer">
          <bpmn:dataObject id="doFromAccount" name="fromAccount"/>
          <bpmn:dataObject id="doNewFrom" name="newFrom"/>
          <bpmn:dataStoreReference id="refRead" dataStoreRef="accounts">
            <bpmn:extensionElements><q:store key="fromId" forUpdate="true"/></bpmn:extensionElements>
          </bpmn:dataStoreReference>
          <bpmn:dataStoreReference id="refWrite" dataStoreRef="accounts">
            <bpmn:extensionElements><q:store key="fromId" field="balance"/></bpmn:extensionElements>
          </bpmn:dataStoreReference>
          <bpmn:startEvent id="TxS"/>
          <bpmn:serviceTask id="LoadFrom" name="Load payer">
            <bpmn:dataInputAssociation>
              <bpmn:sourceRef>refRead</bpmn:sourceRef>
              <bpmn:targetRef>doFromAccount</bpmn:targetRef>
            </bpmn:dataInputAssociation>
          </bpmn:serviceTask>
          <bpmn:exclusiveGateway id="Valid" default="tf2"/>
          <bpmn:serviceTask id="Compute" name="Compute new balance">
            <bpmn:dataInputAssociation>
              <bpmn:assignment><bpmn:from>fromAccount.balance - amount</bpmn:from><bpmn:to>newFrom</bpmn:to></bpmn:assignment>
            </bpmn:dataInputAssociation>
          </bpmn:serviceTask>
          <bpmn:serviceTask id="Persist" name="Persist">
            <bpmn:dataOutputAssociation>
              <bpmn:sourceRef>doNewFrom</bpmn:sourceRef>
              <bpmn:targetRef>refWrite</bpmn:targetRef>
            </bpmn:dataOutputAssociation>
          </bpmn:serviceTask>
          <bpmn:endEvent id="TxEnd"/>
          <bpmn:endEvent id="TxCancel"><bpmn:cancelEventDefinition/></bpmn:endEvent>
          <bpmn:sequenceFlow id="tf0" sourceRef="TxS" targetRef="LoadFrom"/>
          <bpmn:sequenceFlow id="tf1" sourceRef="LoadFrom" targetRef="Valid"/>
          <bpmn:sequenceFlow id="tf2" sourceRef="Valid" targetRef="Compute"/>
          <bpmn:sequenceFlow id="tf3" sourceRef="Valid" targetRef="TxCancel">
            <bpmn:conditionExpression>fromAccount.balance &lt; amount</bpmn:conditionExpression>
          </bpmn:sequenceFlow>
          <bpmn:sequenceFlow id="tf4" sourceRef="Compute" targetRef="Persist"/>
          <bpmn:sequenceFlow id="tf5" sourceRef="Persist" targetRef="TxEnd"/>
        </bpmn:transaction>
        <bpmn:boundaryEvent id="TxCancelBoundary" attachedToRef="Tx"><bpmn:cancelEventDefinition/></bpmn:boundaryEvent>
        <bpmn:endEvent id="Done"/>
        <bpmn:endEvent id="Rejected"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Tx"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Tx" targetRef="Done"/>
        <bpmn:sequenceFlow id="f3" sourceRef="TxCancelBoundary" targetRef="Rejected"/>
      </bpmn:process>
    </bpmn:definitions>"#;

async fn seeded_store() -> Rc<InMemoryDataStore> {
    let store = Rc::new(InMemoryDataStore::new("accounts"));
    store.put("alice", account(100, false)).await.unwrap();
    store
}

async fn balance_of(store: &InMemoryDataStore, key: &str) -> BigDecimal {
    match store.get(key).await.unwrap() {
        Some(FeelValue::Map(m)) => match m.get("balance") {
            Some(FeelValue::Number(n)) => n.clone(),
            other => panic!("expected number balance, got {other:?}"),
        },
        other => panic!("expected map account, got {other:?}"),
    }
}

async fn run_tx(store: Rc<InMemoryDataStore>, amount: i64) -> sutra_executor::ExecResult {
    let process = proc(TX_BPMN, "transfer");
    let lookup = Rc::clone(&store);
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "accounts").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .build()
        .execute_sync(
            &process,
            vars(&[("fromId", string("alice")), ("amount", num(amount))]),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn valid_transfer_commits_the_debit() {
    let store = seeded_store().await;
    let result = run_tx(Rc::clone(&store), 30).await;

    assert!(result.visited_nodes.contains("Done"));
    assert!(!result.visited_nodes.contains("Rejected"));
    assert_eq!(balance_of(&store, "alice").await, BigDecimal::from(70)); // committed
}

#[tokio::test]
async fn insufficient_funds_rolls_back_and_routes_cancel_boundary() {
    let store = seeded_store().await;
    let result = run_tx(Rc::clone(&store), 1000).await;

    assert!(result.visited_nodes.contains("Rejected"));
    assert!(!result.visited_nodes.contains("Done"));
    assert_eq!(balance_of(&store, "alice").await, BigDecimal::from(100)); // rolled back
}

// ---- TransactionCompensationTest --------------------------------------------------------

const TX_COMP_BPMN: &str = r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
      <bpmn:process id="txcomp">
        <bpmn:startEvent id="S"/>
        <bpmn:transaction id="Tx" name="Reserve">
          <bpmn:startEvent id="TxS"/>
          <bpmn:serviceTask id="Book" implementation="${book}"/>
          <bpmn:boundaryEvent id="BookComp" attachedToRef="Book">
            <bpmn:compensateEventDefinition/>
          </bpmn:boundaryEvent>
          <bpmn:serviceTask id="UndoBook" implementation="${undoBook}"/>
          <bpmn:exclusiveGateway id="Check" default="tf-ok"/>
          <bpmn:endEvent id="TxEnd"/>
          <bpmn:endEvent id="TxCancel"><bpmn:cancelEventDefinition/></bpmn:endEvent>
          <bpmn:sequenceFlow id="tf0" sourceRef="TxS" targetRef="Book"/>
          <bpmn:sequenceFlow id="tf1" sourceRef="Book" targetRef="Check"/>
          <bpmn:sequenceFlow id="tf-ok" sourceRef="Check" targetRef="TxEnd"/>
          <bpmn:sequenceFlow id="tf-cancel" sourceRef="Check" targetRef="TxCancel">
            <bpmn:conditionExpression>overLimit</bpmn:conditionExpression>
          </bpmn:sequenceFlow>
          <bpmn:sequenceFlow id="tf-comp" sourceRef="BookComp" targetRef="UndoBook"/>
        </bpmn:transaction>
        <bpmn:boundaryEvent id="TxCancelBoundary" attachedToRef="Tx"><bpmn:cancelEventDefinition/></bpmn:boundaryEvent>
        <bpmn:endEvent id="Done"/>
        <bpmn:endEvent id="Rejected"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Tx"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Tx" targetRef="Done"/>
        <bpmn:sequenceFlow id="f3" sourceRef="TxCancelBoundary" targetRef="Rejected"/>
      </bpmn:process>
    </bpmn:definitions>"#;

async fn run_comp(
    effects: Rc<RefCell<Vec<&'static str>>>,
    over_limit: bool,
) -> sutra_executor::ExecResult {
    let process = proc(TX_COMP_BPMN, "txcomp");
    let (eb, eu) = (Rc::clone(&effects), Rc::clone(&effects));
    let registry = TaskRegistry::new()
        .register("book", move |_, _| {
            eb.borrow_mut().push("book");
            ok_map(&[("booked", boolean(true))])
        })
        .register("undoBook", move |_, _| {
            eu.borrow_mut().push("undoBook");
            ok_map(&[])
        });
    TokenExecutor::builder(registry)
        .with_condition_evaluator(feel_condition_evaluator())
        .build()
        .execute_sync(&process, vars(&[("overLimit", boolean(over_limit))]))
        .await
        .unwrap()
}

#[tokio::test]
async fn cancel_compensates_the_completed_booking() {
    let effects = Rc::new(RefCell::new(Vec::new()));
    let result = run_comp(Rc::clone(&effects), true).await;

    assert!(result.visited_nodes.contains("Rejected"));
    assert!(!result.visited_nodes.contains("Done"));
    assert_eq!(effects.borrow().as_slice(), &["book", "undoBook"]);
}

#[tokio::test]
async fn commit_does_not_compensate() {
    let effects = Rc::new(RefCell::new(Vec::new()));
    let result = run_comp(Rc::clone(&effects), false).await;

    assert!(result.visited_nodes.contains("Done"));
    assert!(!result.visited_nodes.contains("Rejected"));
    assert_eq!(effects.borrow().as_slice(), &["book"]);
}
