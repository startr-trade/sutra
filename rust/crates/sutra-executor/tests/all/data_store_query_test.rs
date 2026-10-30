//! Data-store key queries (`<q:store query=…>`) and insert-if-absent writes
//! (`<q:store expect="absent">`), against the in-memory reference store — 0.6.10.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::str::FromStr;

use crate::common::*;
use bigdecimal::BigDecimal;
use sutra_bpmn::model::{StoreAggregateFunction, StoreQueryMode};
use sutra_bpmn::{BpmnModelLoader, Node};
use sutra_executor::{DataStore, InMemoryDataStore, TaskRegistry, TokenExecutor};
use sutra_feel::FeelValue;

/// One data task that runs `STORE` (a `<q:store>` element) into `report`.
fn query_process(store: &str) -> String {
    format!(
        r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:process id="q">
        <bpmn:dataObject id="doReport" name="report"/>
        <bpmn:dataStore id="events" name="events"/>
        <bpmn:dataStoreReference id="refQuery" dataStoreRef="events">
          <bpmn:extensionElements>{store}</bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Read">
          <bpmn:dataInputAssociation>
            <bpmn:sourceRef>refQuery</bpmn:sourceRef><bpmn:targetRef>doReport</bpmn:targetRef>
          </bpmn:dataInputAssociation>
        </bpmn:serviceTask>
        <bpmn:endEvent id="E"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Read"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Read" targetRef="E"/>
      </bpmn:process>
    </bpmn:definitions>"#
    )
}

fn dec(s: &str) -> FeelValue {
    FeelValue::Number(BigDecimal::from_str(s).unwrap())
}

fn entry(amount: FeelValue, side: &str, code: &str) -> FeelValue {
    fmap(&[
        ("amount", amount),
        ("dir", string(side)),
        ("tag", string(code)),
    ])
}

/// src-1 has three rows on 2026-10-01 and one on 2026-10-02; src-2 one row; plus a row whose
/// key sorts right after the `src-1|2026-10-01|` prefix range.
async fn events() -> Rc<InMemoryDataStore> {
    let store = Rc::new(InMemoryDataStore::new("events"));
    for (key, value) in [
        ("src-1|2026-10-01|0001", entry(dec("10.01"), "OUT", "OPEN")),
        ("src-1|2026-10-01|0002", entry(dec("20.04"), "IN", "CLOSE")),
        (
            "src-1|2026-10-01|0003",
            entry(string("10.01"), "IN", "CLOSE"),
        ),
        ("src-1|2026-10-02|0001", entry(dec("99"), "IN", "CLOSE")),
        ("src-1|2026-10-01}", entry(dec("1000"), "IN", "CLOSE")),
        ("src-2|2026-10-01|0001", entry(dec("5"), "OUT", "OPEN")),
    ] {
        store.put(key, value).await.unwrap();
    }
    store
}

async fn run(
    store: Rc<InMemoryDataStore>,
    q_store: &str,
    variables: &[(&str, FeelValue)],
) -> Result<FeelValue, sutra_executor::ExecError> {
    let process = proc(&query_process(q_store), "q");
    let lookup = Rc::clone(&store);
    let result = TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "events").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .build()
        .execute_sync(&process, vars(variables))
        .await?;
    Ok(result.output("report").cloned().expect("report set"))
}

fn field<'a>(value: &'a FeelValue, name: &str) -> &'a FeelValue {
    match value {
        FeelValue::Map(m) => m
            .get(name)
            .unwrap_or_else(|| panic!("no {name} in {value:?}")),
        other => panic!("not a map: {other:?}"),
    }
}

fn list(value: &FeelValue) -> &Vec<FeelValue> {
    match value {
        FeelValue::List(l) => l,
        other => panic!("not a list: {other:?}"),
    }
}

fn keys(report: &FeelValue) -> Vec<String> {
    list(field(report, "rows"))
        .iter()
        .map(|r| match field(r, "key") {
            FeelValue::String(s) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

const DAY: &str = r#"<q:store key="account + '|' + day + '|'" query="prefix">
  <q:aggregate fn="sum" field="amount" groupBy="dir"/>
  <q:aggregate fn="count" groupBy="tag"/>
  <q:aggregate fn="sum" field="amount"/>
</q:store>"#;

fn day_vars() -> Vec<(&'static str, FeelValue)> {
    vec![("account", string("src-1")), ("day", string("2026-10-01"))]
}

#[test]
fn the_loader_reads_the_query_and_its_aggregates() {
    let process = proc(&query_process(DAY), "q");
    let Node::DataTask { data_mapping, .. } = process.node("Read").unwrap() else {
        panic!("data task")
    };
    let query = data_mapping.store_reads[0].query.as_ref().expect("a query");
    assert_eq!(query.mode, StoreQueryMode::Prefix);
    assert!(query.rows);
    assert_eq!(query.limit, sutra_bpmn::STORE_QUERY_DEFAULT_LIMIT);
    assert_eq!(query.aggregates.len(), 3);
    assert_eq!(query.aggregates[0].function, StoreAggregateFunction::Sum);
    assert_eq!(query.aggregates[0].group_by.as_deref(), Some("dir"));
    assert_eq!(query.aggregates[1].function, StoreAggregateFunction::Count);
}

#[test]
fn a_malformed_query_is_refused_at_load() {
    for (store, needle) in [
        (r#"<q:store key="'a'" query="scan"/>"#, "not a query mode"),
        (r#"<q:store key="'a'" query="range"/>"#, "needs @to"),
        (
            r#"<q:store key="'a'" query="prefix" to="'b'"/>"#,
            "@to belongs",
        ),
        (r#"<q:store key="'a'" query="prefix" limit="0"/>"#, "@limit"),
        (
            r#"<q:store key="'a'" query="prefix" limit="10001"/>"#,
            "@limit",
        ),
        (
            r#"<q:store key="'a'" query="prefix"><q:aggregate fn="sum"/></q:store>"#,
            "needs @field",
        ),
        (
            r#"<q:store key="'a'" query="prefix"><q:aggregate fn="count" field="x"/></q:store>"#,
            "takes no @field",
        ),
        (
            r#"<q:store key="'a'" query="prefix"><q:aggregate fn="sum" field="a b"/></q:store>"#,
            "not a value path",
        ),
        (
            r#"<q:store key="'a'" query="prefix" rows="false"/>"#,
            "return nothing",
        ),
        (
            r#"<q:store key="'a'" query="prefix" forUpdate="true"/>"#,
            "@forUpdate",
        ),
    ] {
        let e = BpmnModelLoader::new()
            .load(query_process(store).as_bytes())
            .expect_err(store);
        assert_eq!(e.code, "SUTRA.PARSE.STORE.QUERY_INVALID", "{store}");
        assert!(e.message.contains(needle), "{store}: {}", e.message);
    }
}

#[tokio::test]
async fn a_prefix_query_returns_rows_in_key_order_and_exact_aggregates() {
    let report = run(events().await, DAY, &day_vars()).await.unwrap();
    assert_eq!(
        keys(&report),
        vec![
            "src-1|2026-10-01|0001",
            "src-1|2026-10-01|0002",
            "src-1|2026-10-01|0003"
        ],
        "the `}}` key just past the prefix range is not a match"
    );
    let aggregates = list(field(&report, "aggregates"));
    let by_dir = field(&aggregates[0], "groups");
    assert_eq!(field(field(by_dir, "IN"), "count"), &num(2));
    // A decimal string sums exactly with a JSON number: 20.04 + "10.01".
    assert_eq!(field(field(by_dir, "IN"), "sum"), &dec("30.05"));
    assert_eq!(field(field(by_dir, "OUT"), "sum"), &dec("10.01"));
    let by_tag = field(&aggregates[1], "groups");
    assert_eq!(field(field(by_tag, "CLOSE"), "count"), &num(2));
    assert!(
        !matches!(field(by_tag, "CLOSE"), FeelValue::Map(m) if m.contains_key("sum")),
        "a count aggregate carries no sum"
    );
    assert_eq!(field(&aggregates[2], "sum"), &dec("40.06"));
    assert_eq!(field(&aggregates[2], "count"), &num(3));
    assert_eq!(field(&report, "truncated"), &FeelValue::Boolean(false));
    assert_eq!(field(&report, "next"), &FeelValue::Null);
}

#[tokio::test]
async fn a_range_is_from_inclusive_to_exclusive() {
    let store =
        r#"<q:store key="'src-1|2026-10-01|0002'" query="range" to="'src-1|2026-10-02|0001'"/>"#;
    let report = run(events().await, store, &[]).await.unwrap();
    assert_eq!(
        keys(&report),
        vec![
            "src-1|2026-10-01|0002",
            "src-1|2026-10-01|0003",
            "src-1|2026-10-01}"
        ]
    );
}

#[tokio::test]
async fn paging_walks_every_row_once_while_aggregates_cover_them_all() {
    let store = r#"<q:store key="'src-1|'" query="prefix" limit="2" after="cursor">
      <q:aggregate fn="count"/>
    </q:store>"#;
    let events = events().await;
    let mut cursor = FeelValue::Null;
    let mut seen = Vec::new();
    loop {
        let report = run(Rc::clone(&events), store, &[("cursor", cursor.clone())])
            .await
            .unwrap();
        let aggregates = list(field(&report, "aggregates"));
        assert_eq!(
            field(&aggregates[0], "count"),
            &num(5),
            "independent of the page"
        );
        seen.extend(keys(&report));
        if field(&report, "truncated") == &FeelValue::Boolean(false) {
            assert_eq!(field(&report, "next"), &FeelValue::Null);
            break;
        }
        cursor = field(&report, "next").clone();
    }
    assert_eq!(seen.len(), 5);
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(seen, sorted);
}

#[tokio::test]
async fn aggregates_only_and_no_match_returns_zeros_not_null() {
    let store = r#"<q:store key="'nobody|'" query="prefix" rows="false">
      <q:aggregate fn="sum" field="amount"/>
      <q:aggregate fn="count" groupBy="dir"/>
    </q:store>"#;
    let report = run(events().await, store, &[]).await.unwrap();
    assert!(list(field(&report, "rows")).is_empty());
    let aggregates = list(field(&report, "aggregates"));
    assert_eq!(field(&aggregates[0], "count"), &num(0));
    assert_eq!(field(&aggregates[0], "sum"), &dec("0"));
    assert_eq!(
        field(&aggregates[1], "groups"),
        &FeelValue::Map(BTreeMap::new())
    );
}

#[tokio::test]
async fn a_missing_or_null_field_is_counted_but_not_summed() {
    let store = Rc::new(InMemoryDataStore::new("events"));
    store
        .put("k|1", fmap(&[("amount", dec("1.5"))]))
        .await
        .unwrap();
    store.put("k|2", fmap(&[("other", num(7))])).await.unwrap();
    store
        .put("k|3", fmap(&[("amount", FeelValue::Null)]))
        .await
        .unwrap();
    let q = r#"<q:store key="'k|'" query="prefix" rows="false">
      <q:aggregate fn="sum" field="amount" groupBy="dir"/>
      <q:aggregate fn="sum" field="amount"/>
    </q:store>"#;
    let report = run(store, q, &[]).await.unwrap();
    let aggregates = list(field(&report, "aggregates"));
    assert_eq!(field(&aggregates[1], "count"), &num(3));
    assert_eq!(field(&aggregates[1], "sum"), &dec("1.5"));
    // No `side` field anywhere: every row groups under "null".
    let groups = field(&aggregates[0], "groups");
    assert_eq!(field(field(groups, "null"), "count"), &num(3));
}

#[tokio::test]
async fn a_non_numeric_summed_field_fails_with_its_own_code() {
    let store = Rc::new(InMemoryDataStore::new("events"));
    store
        .put("k|1", fmap(&[("amount", dec("1"))]))
        .await
        .unwrap();
    store
        .put("k|2", fmap(&[("amount", string("ten"))]))
        .await
        .unwrap();
    store
        .put("k|3", fmap(&[("amount", boolean(true))]))
        .await
        .unwrap();
    let q =
        r#"<q:store key="'k|'" query="prefix"><q:aggregate fn="sum" field="amount"/></q:store>"#;
    let e = run(store, q, &[]).await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.DATASTORE.QUERY_NOT_NUMERIC");
    assert!(e.message().contains("2 matching row(s)"), "{}", e.message());
}

#[tokio::test]
async fn decimal_strings_follow_the_sql_grammar() {
    let store = Rc::new(InMemoryDataStore::new("events"));
    for (i, text) in [" 1.5 ", "+2", "-0.5", "5.", ".25", "1e2", "1.5E-1"]
        .iter()
        .enumerate()
    {
        store
            .put(&format!("k|{i}"), fmap(&[("amount", string(text))]))
            .await
            .unwrap();
    }
    let q = r#"<q:store key="'k|'" query="prefix" rows="false"><q:aggregate fn="sum" field="amount"/></q:store>"#;
    let report = run(Rc::clone(&store), q, &[]).await.unwrap();
    // 1.5 + 2 - 0.5 + 5 + 0.25 + 100 + 0.15
    assert_eq!(
        field(&list(field(&report, "aggregates"))[0], "sum"),
        &dec("108.40")
    );
    for bad in ["1.2.3", "e5", "1e", "--1", "0x10", ""] {
        let store = Rc::new(InMemoryDataStore::new("events"));
        store
            .put("k|1", fmap(&[("amount", string(bad))]))
            .await
            .unwrap();
        let e = run(store, q, &[]).await.unwrap_err();
        assert_eq!(
            e.code(),
            "SUTRA.RUNTIME.DATASTORE.QUERY_NOT_NUMERIC",
            "{bad:?}"
        );
    }
}

#[tokio::test]
async fn an_empty_prefix_is_refused() {
    let q = r#"<q:store key="''" query="prefix"/>"#;
    let e = run(events().await, q, &[]).await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.DATASTORE.QUERY_INVALID");
}

// ---- expect="absent": insert-if-absent ------------------------------------------------------

/// Claim `key` once: write `claim` with `expect="absent"`; a lost claim takes the error boundary
/// to `Lost` when `with_boundary`, else fails the instance.
fn claim_process(with_boundary: bool) -> String {
    let boundary = if with_boundary {
        r#"<bpmn:boundaryEvent id="AlreadyClaimed" attachedToRef="Claim">
          <bpmn:errorEventDefinition errorRef="conflict"/>
        </bpmn:boundaryEvent>
        <bpmn:endEvent id="Lost"/>
        <bpmn:sequenceFlow id="f3" sourceRef="AlreadyClaimed" targetRef="Lost"/>"#
    } else {
        ""
    };
    format!(
        r#"<?xml version="1.0"?>
    <bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                      xmlns:q="urn:sutra:q:1.0">
      <bpmn:error id="conflict" errorCode="SUTRA.RUNTIME.DATASTORE.CONFLICT"/>
      <bpmn:process id="claim">
        <bpmn:dataObject id="doClaim" name="claim"/>
        <bpmn:dataStore id="claims" name="claims"/>
        <bpmn:dataStoreReference id="refClaim" dataStoreRef="claims">
          <bpmn:extensionElements><q:store key="day" expect="absent"/></bpmn:extensionElements>
        </bpmn:dataStoreReference>
        <bpmn:startEvent id="S"/>
        <bpmn:serviceTask id="Claim">
          <bpmn:dataOutputAssociation>
            <bpmn:sourceRef>doClaim</bpmn:sourceRef><bpmn:targetRef>refClaim</bpmn:targetRef>
          </bpmn:dataOutputAssociation>
        </bpmn:serviceTask>
        <bpmn:endEvent id="Won"/>
        <bpmn:sequenceFlow id="f1" sourceRef="S" targetRef="Claim"/>
        <bpmn:sequenceFlow id="f2" sourceRef="Claim" targetRef="Won"/>
        {boundary}
      </bpmn:process>
    </bpmn:definitions>"#
    )
}

async fn claim(
    store: &Rc<InMemoryDataStore>,
    with_boundary: bool,
    who: &str,
) -> Result<sutra_executor::ExecResult, sutra_executor::ExecError> {
    let process = proc(&claim_process(with_boundary), "claim");
    let lookup = Rc::clone(store);
    TokenExecutor::builder(TaskRegistry::new())
        .with_feel()
        .with_data_stores(move |_, name| {
            (name == "claims").then(|| Rc::clone(&lookup) as Rc<dyn DataStore>)
        })
        .build()
        .execute_sync(
            &process,
            vars(&[("day", string("2026-10-30")), ("claim", string(who))]),
        )
        .await
}

#[test]
fn the_loader_reads_expect_absent() {
    let process = proc(&claim_process(false), "claim");
    let Node::DataTask { data_mapping, .. } = process.node("Claim").unwrap() else {
        panic!("data task")
    };
    assert!(data_mapping.store_writes[0].expect_absent);
    assert!(!data_mapping.store_writes[0].expect_unchanged);
}

#[tokio::test]
async fn the_first_claim_wins_and_a_second_conflicts() {
    let store = Rc::new(InMemoryDataStore::new("claims"));
    let first = claim(&store, false, "replica-a").await.unwrap();
    assert!(first.visited_nodes.contains("Won"));
    let e = claim(&store, false, "replica-b").await.unwrap_err();
    assert_eq!(e.code(), "SUTRA.RUNTIME.DATASTORE.CONFLICT");
    assert!(e.message().contains("already exists"), "{}", e.message());
    assert_eq!(
        store.get("2026-10-30").await.unwrap(),
        Some(string("replica-a")),
        "the losing write never landed"
    );
}

#[tokio::test]
async fn a_lost_claim_is_catchable_by_an_error_boundary() {
    let store = Rc::new(InMemoryDataStore::new("claims"));
    claim(&store, true, "replica-a").await.unwrap();
    let second = claim(&store, true, "replica-b").await.unwrap();
    assert!(
        second.visited_nodes.contains("Lost"),
        "{:?}",
        second.visited_nodes
    );
    assert!(!second.visited_nodes.contains("Won"));
}
