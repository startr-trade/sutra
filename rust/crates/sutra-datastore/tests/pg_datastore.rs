//! Integration tests against a real dockerised PostgreSQL (dynamic host port — never 8080):
//! 1. the `(store_name, store_key)` cutover table shape — full CRUD / revision / CAS /
//!    transaction semantics, pessimistic-lock serialisation, and concurrent-conflict detection;
//! 2. the REAL `examples/money-transfer` module: its `datastores.yaml` (env-indirected
//!    connection) + `migrations/accounts` run read-only from the examples tree, idempotently,
//!    including the module's fault-injection trigger — plus its `coverage` store, which ships NO
//!    migrations and is served the ENGINE's coverage schema on first use;
//! 3. the COVERAGE store end-to-end: engine-shipped DDL applied on first use to the connection
//!    the author declared, seed, first-covers-wins, and the SQL aggregate checked against a
//!    client-side recount over the same rows;
//! 4. the PROJECTED store shape (`structure:` → typed columns): round-trip fidelity against
//!    real column types, the same CRUD/CAS/lock semantics on the author's own table, the
//!    undeclared-field rejection, and first-use verification refusing a drifted table.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

use serde_json::json;
use sutra_datastore::{
    load_migrations, parse_datastores, CoverageStore, PostgresDataStore, ProjectedStore,
    StoreDefinition, StructureRef,
};

// ---- dockerised PostgreSQL (dynamic port) ---------------------------------

struct PgContainer {
    id: String,
    port: u16,
}

impl PgContainer {
    /// `docker run -d --rm -p 127.0.0.1:0:5432 postgres:16-alpine` — the ephemeral-port
    /// mapping keeps the host side dynamic (never a fixed port on this machine).
    fn start() -> PgContainer {
        let out = docker(&[
            "run",
            "-d",
            "--rm",
            "-e",
            "POSTGRES_PASSWORD=sutra",
            "-e",
            "POSTGRES_USER=sutra",
            "-e",
            "POSTGRES_DB=sutra",
            "-p",
            "127.0.0.1:0:5432",
            "postgres:16-alpine",
        ]);
        let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        assert!(!id.is_empty(), "docker run produced no container id");

        let port_out = docker(&["port", &id, "5432/tcp"]);
        let mapping = String::from_utf8_lossy(&port_out.stdout);
        let port: u16 = mapping
            .lines()
            .find_map(|l| l.rsplit(':').next()?.trim().parse().ok())
            .unwrap_or_else(|| panic!("no host port in docker port output: {mapping}"));

        // Readiness: pg_isready, then an actual query (postgres restarts once during init).
        let container = PgContainer { id, port };
        for attempt in 0..240 {
            let ready = Command::new("docker")
                .args([
                    "exec",
                    &container.id,
                    "psql",
                    "-U",
                    "sutra",
                    "-d",
                    "sutra",
                    "-c",
                    "SELECT 1",
                ])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            if ready {
                return container;
            }
            assert!(attempt < 239, "postgres container never became ready");
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        unreachable!()
    }

    fn url(&self) -> String {
        format!("postgres://127.0.0.1:{}/sutra", self.port)
    }
}

impl Drop for PgContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

fn docker(args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker CLI available (these tests require a local docker daemon)");
    assert!(
        out.status.success(),
        "docker {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

// ---- helpers ---------------------------------------------------------------

/// The cutover DDL a module ships: rows key on `(store_name, store_key)`
/// within the declared connection — the collapsed namespace columns do not exist.
const CUTOVER_DDL: &str = "CREATE TABLE IF NOT EXISTS data_store (\n\
  store_name  VARCHAR(128) NOT NULL,\n\
  store_key   VARCHAR(512) NOT NULL,\n\
  store_value TEXT         NOT NULL,\n\
  rev         BIGINT       NOT NULL DEFAULT 1,\n\
  updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,\n\
  PRIMARY KEY (store_name, store_key)\n\
);";

fn literal_def(name: &str, url: &str) -> StoreDefinition {
    let mut properties = std::collections::BTreeMap::new();
    properties.insert("sql.url".to_string(), url.to_string());
    properties.insert("sql.username".to_string(), "sutra".to_string());
    properties.insert("sql.password".to_string(), "sutra".to_string());
    StoreDefinition {
        name: name.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    }
}

// ---- the projected (typed-column) shape ------------------------------------

/// The declared structure a projected store's columns come from — one field of each
/// marshalling class, the last optional, with the facets the advisory type mapping (design
/// §4.4) turns into `VARCHAR(35)` / `NUMERIC(18,2)`.
const ACCOUNT_XSD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns="urn:sutra:test:accounts"
           targetNamespace="urn:sutra:test:accounts"
           elementFormDefault="qualified">
  <xs:complexType name="AccountRecord">
    <xs:sequence>
      <xs:element name="accountId" type="AccountId"/>
      <xs:element name="balance"   type="Amount"/>
      <xs:element name="seqNo"     type="xs:int"/>
      <xs:element name="frozen"    type="xs:boolean"/>
      <xs:element name="openedAt"  type="xs:date" minOccurs="0"/>
    </xs:sequence>
  </xs:complexType>
  <xs:simpleType name="AccountId">
    <xs:restriction base="xs:string"><xs:maxLength value="35"/></xs:restriction>
  </xs:simpleType>
  <xs:simpleType name="Amount">
    <xs:restriction base="xs:decimal">
      <xs:totalDigits value="18"/><xs:fractionDigits value="2"/>
    </xs:restriction>
  </xs:simpleType>
</xs:schema>
"#;

/// The DDL the author ships for that structure — the engine generates none (ruling 5). The
/// column types are the §4.4 mapping's PostgreSQL column, and `store_key`/`rev`/`updated_at`
/// are the engine's control columns. Note there is NO `store_name`: the table IS the store.
const PROJECTED_DDL: &str = "CREATE TABLE IF NOT EXISTS accounts (\n\
  store_key  VARCHAR(512)  NOT NULL PRIMARY KEY,\n\
  account_id VARCHAR(35)   NOT NULL,\n\
  balance    NUMERIC(18,2) NOT NULL,\n\
  seq_no     INTEGER       NOT NULL,\n\
  frozen     BOOLEAN       NOT NULL,\n\
  opened_at  DATE,\n\
  rev        BIGINT        NOT NULL DEFAULT 1,\n\
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP\n\
);";

/// A table that DRIFTED from the package's migrations: `balance` was dropped and the optional
/// `opened_at` was made mandatory by a hand-applied ALTER. Lint cannot see this — only the
/// live catalog can.
const DRIFTED_DDL: &str = "CREATE TABLE IF NOT EXISTS drifted (\n\
  store_key  VARCHAR(512) NOT NULL PRIMARY KEY,\n\
  account_id VARCHAR(35)  NOT NULL,\n\
  seq_no     INTEGER      NOT NULL,\n\
  frozen     BOOLEAN      NOT NULL,\n\
  opened_at  DATE         NOT NULL,\n\
  rev        BIGINT       NOT NULL DEFAULT 1,\n\
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP\n\
);";

/// Derive the projection exactly as the engine does at plan time: compile the package's own
/// XSD, enumerate the declared type's children, project them.
fn account_projection(store: &str, table: &str) -> ProjectedStore {
    let schema = sutra_xsd::Schema::compile(ACCOUNT_XSD.as_bytes()).expect("schema compiles");
    let fields = schema.fields_of("AccountRecord").expect("declared type");
    let structure = StructureRef {
        schema: "urn:accounts".to_string(),
        type_name: "AccountRecord".to_string(),
        columns: BTreeMap::new(),
    };
    let projection = structure.project(&fields).expect("the type is flat");
    ProjectedStore::new(store, table, projection).expect("projectable")
}

fn projected_store(name: &str, table: &str, url: &str, ddl: &str) -> PostgresDataStore {
    PostgresDataStore::from_definition_projected(
        &literal_def(name, url),
        vec![ddl.to_string()],
        None,
        Some(account_projection(name, table)),
    )
    .expect("store builds")
}

/// The record every fidelity assertion is written against. Parsed from TEXT so the decimal
/// keeps its WRITTEN scale ("100.50", not 100.5) — the property §4.5 requires survive the
/// round trip through a real `NUMERIC(18,2)`.
fn account_record() -> serde_json::Value {
    serde_json::from_str(r#"{"accountId":"ACC-000123","balance":100.50,"seqNo":7,"frozen":false}"#)
        .unwrap()
}

fn money_transfer_binding_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/money-transfer/deployments-src/default--money-transfer--1.0.0")
}

// ---- 1. cutover-shape semantics --------------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn cutover_model_crud_revision_cas_and_transactions() {
    let pg = PgContainer::start();
    let store = PostgresDataStore::from_definition(
        &literal_def("ledger", &pg.url()),
        vec![CUTOVER_DDL.to_string()],
    )
    .expect("store builds");

    // absent key: get None, revision 0, delete no-op
    assert_eq!(store.get("k1").await.unwrap(), None);
    assert_eq!(store.revision("k1").await.unwrap(), 0);
    store.delete("k1").await.unwrap();

    // insert-or-replace + revision bumps (insert = 1, each update +1)
    let precise = json!({"balance": 100.25_f64, "frozen": false});
    store.put("k1", &precise).await.unwrap();
    assert_eq!(store.revision("k1").await.unwrap(), 1);
    assert_eq!(store.get("k1").await.unwrap().unwrap(), precise);
    store.put("k1", &json!({"balance": 50})).await.unwrap();
    assert_eq!(store.revision("k1").await.unwrap(), 2);

    // arbitrary-precision round-trip: a value beyond f64 precision stays exact as text
    let exact: serde_json::Value =
        serde_json::from_str(r#"{"balance": 0.12345678901234567890123}"#).unwrap();
    store.put("exact", &exact).await.unwrap();
    assert_eq!(store.get("exact").await.unwrap().unwrap(), exact);

    // CAS: expect-absent (rev <= 0) inserts once, conflicts when the key exists
    assert!(store.put_if_revision("cas", &json!(1), 0).await.unwrap());
    assert!(
        !store.put_if_revision("cas", &json!(2), 0).await.unwrap(),
        "expect-absent conflict"
    );
    // expect-specific-rev: correct rev wins, stale rev is a detected conflict
    assert!(store.put_if_revision("cas", &json!(2), 1).await.unwrap());
    assert!(
        !store.put_if_revision("cas", &json!(3), 1).await.unwrap(),
        "stale rev conflict"
    );
    assert_eq!(store.get("cas").await.unwrap().unwrap(), json!(2));

    // store_name partitioning: same key, different store, same connection → independent
    let other = PostgresDataStore::from_definition(&literal_def("other", &pg.url()), Vec::new())
        .expect("store builds");
    assert_eq!(other.get("k1").await.unwrap(), None);
    other.put("k1", &json!("theirs")).await.unwrap();
    assert_eq!(
        store.get("k1").await.unwrap().unwrap(),
        json!({"balance": 50})
    );

    // transaction atomicity: rollback discards, drop-without-commit rolls back, commit publishes
    let mut tx = store.begin().await.unwrap();
    tx.put("t1", &json!("a")).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(store.get("t1").await.unwrap(), None);

    {
        let mut tx = store.begin().await.unwrap();
        tx.put("t1", &json!("b")).await.unwrap();
        // dropped here — implicit rollback (drop is a rollback)
    }
    assert_eq!(store.get("t1").await.unwrap(), None);

    let mut tx = store.begin().await.unwrap();
    tx.put("t1", &json!("c")).await.unwrap();
    tx.delete("k1").await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(store.get("t1").await.unwrap().unwrap(), json!("c"));
    assert_eq!(store.get("k1").await.unwrap(), None);
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn pessimistic_lock_serialises_concurrent_read_modify_write() {
    let pg = PgContainer::start();
    let store = PostgresDataStore::from_definition(
        &literal_def("accounts", &pg.url()),
        vec![CUTOVER_DDL.to_string()],
    )
    .expect("store builds");
    store.put("acct", &json!({"balance": 0})).await.unwrap();

    // 4 workers × 5 increments, each a get_for_update + put in its own transaction. The
    // rev-bumping lock UPDATE serialises them; a lost update would end below 20.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                let mut tx = store.begin().await.unwrap();
                let current = tx.get_for_update("acct").await.unwrap().unwrap();
                let balance = current["balance"].as_i64().unwrap();
                tx.put("acct", &json!({"balance": balance + 1}))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(
        store.get("acct").await.unwrap().unwrap(),
        json!({"balance": 20}),
        "pessimistic locking must not lose an update"
    );
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_cas_detects_conflicts_and_retries_converge() {
    let pg = PgContainer::start();
    let store = PostgresDataStore::from_definition(
        &literal_def("counters", &pg.url()),
        vec![CUTOVER_DDL.to_string()],
    )
    .expect("store builds");
    store.put("n", &json!(0)).await.unwrap();

    // 4 optimistic workers × 5 increments with a CAS-retry loop (the executor's
    // expect="unchanged" + DSO-retry shape): every conflict is detected, never silent.
    let mut handles = Vec::new();
    for _ in 0..4 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..5 {
                loop {
                    let rev = store.revision("n").await.unwrap();
                    let current = store.get("n").await.unwrap().unwrap().as_i64().unwrap();
                    if store
                        .put_if_revision("n", &json!(current + 1), rev)
                        .await
                        .unwrap()
                    {
                        break; // applied
                    }
                    // conflict — a concurrent committed write bumped rev; re-read and retry
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(store.get("n").await.unwrap().unwrap(), json!(20));
}

// ---- 1b. the projected (typed-column) shape --------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn projected_store_round_trips_upserts_cas_locks_and_deletes() {
    let pg = PgContainer::start();
    let store = projected_store("accounts", "accounts", &pg.url(), PROJECTED_DDL);

    // absent key: get None, revision 0, delete no-op — identical to the KV path
    assert_eq!(store.get("a1").await.unwrap(), None);
    assert_eq!(store.revision("a1").await.unwrap(), 0);
    store.delete("a1").await.unwrap();

    // ROUND-TRIP FIDELITY: put/get is byte-equal, including the decimal's written scale and
    // the absent optional field (which stays absent, not an explicit null).
    let written = account_record();
    store.put("a1", &written).await.unwrap();
    let read = store.get("a1").await.unwrap().expect("row");
    assert_eq!(read, written);
    assert_eq!(read.to_string(), written.to_string(), "byte-equal");
    assert_eq!(read["balance"].to_string(), "100.50", "the written scale");
    assert!(read.get("openedAt").is_none(), "absent stays absent");
    assert_eq!(store.revision("a1").await.unwrap(), 1);

    // every declared class, including the optional one, present
    let full: serde_json::Value = serde_json::from_str(
        r#"{"accountId":"ACC-000999","balance":0.05,"seqNo":-3,"frozen":true,
            "openedAt":"2026-08-04"}"#,
    )
    .unwrap();
    store.put("a2", &full).await.unwrap();
    let read = store.get("a2").await.unwrap().expect("row");
    assert_eq!(read.to_string(), full.to_string(), "byte-equal");

    // UPSERT overwrite: the same key replaces, bumping rev (no second row)
    let changed: serde_json::Value = serde_json::from_str(
        r#"{"accountId":"ACC-000123","balance":250.00,"seqNo":8,"frozen":true}"#,
    )
    .unwrap();
    store.put("a1", &changed).await.unwrap();
    assert_eq!(store.revision("a1").await.unwrap(), 2);
    assert_eq!(
        store.get("a1").await.unwrap().unwrap().to_string(),
        changed.to_string()
    );

    // UNDECLARED FIELD: fails closed, names the field, and leaves the row untouched.
    let mut extra = changed.as_object().unwrap().clone();
    extra.insert("nickname".into(), json!("rainy day"));
    let err = store
        .put("a1", &serde_json::Value::Object(extra))
        .await
        .expect_err("an undeclared field must never be silently dropped");
    let message = err.to_string();
    assert!(
        message.contains("SUTRA.RUNTIME.DATASTORE.UNDECLARED_FIELD"),
        "{message}"
    );
    assert!(message.contains("nickname"), "{message}");
    assert_eq!(store.revision("a1").await.unwrap(), 2, "no write happened");

    // a non-record value has nowhere to go in a projected row
    assert!(store.put("a1", &json!("scalar")).await.is_err());

    // CAS: stale rev conflicts, current rev applies, expect-absent inserts exactly once
    assert!(!store.put_if_revision("a1", &written, 1).await.unwrap());
    assert!(store.put_if_revision("a1", &written, 2).await.unwrap());
    assert_eq!(store.revision("a1").await.unwrap(), 3);
    assert!(store.put_if_revision("a3", &written, 0).await.unwrap());
    assert!(
        !store.put_if_revision("a3", &changed, 0).await.unwrap(),
        "expect-absent conflict"
    );

    // get_for_update inside a transaction: the read-modify-write, then rollback discards
    let mut tx = store.begin().await.unwrap();
    let current = tx.get_for_update("a1").await.unwrap().unwrap();
    assert_eq!(current["accountId"], json!("ACC-000123"));
    let mut next = current.as_object().unwrap().clone();
    next.insert("seqNo".into(), json!(99));
    tx.put("a1", &serde_json::Value::Object(next))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(store.get("a1").await.unwrap().unwrap()["seqNo"], json!(99));

    let mut tx = store.begin().await.unwrap();
    tx.put("a4", &written).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(store.get("a4").await.unwrap(), None);

    // delete
    store.delete("a1").await.unwrap();
    assert_eq!(store.get("a1").await.unwrap(), None);
    assert_eq!(store.revision("a1").await.unwrap(), 0);
    assert!(store.get("a2").await.unwrap().is_some(), "delete is scoped");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn first_use_verification_refuses_a_drifted_table() {
    let pg = PgContainer::start();
    // The table exists but has drifted: `balance` dropped, the optional `opened_at` made
    // mandatory. A silent partial write is far worse than a loud refusal (§4.5).
    let store = projected_store("drifted", "drifted", &pg.url(), DRIFTED_DDL);
    let err = store
        .get("a1")
        .await
        .expect_err("a drifted table must fail closed on first use");
    let message = err.to_string();
    assert!(
        message.contains("SUTRA.RUNTIME.DATASTORE.PROJECTION_UNSATISFIABLE"),
        "{message}"
    );
    assert!(message.contains("'balance'"), "{message}");
    assert!(message.contains("'opened_at' is NOT NULL"), "{message}");
    // Writes are refused too — the store is not served at all.
    assert!(store.put("a1", &account_record()).await.is_err());

    // And a projected store whose table is simply not there refuses just as loudly.
    let absent = projected_store("missing", "missing", &pg.url(), "SELECT 1");
    let err = absent.get("a1").await.expect_err("no table");
    assert!(err.to_string().contains("no columns visible"), "{err}");
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn a_constraint_failure_is_reported_not_retried_as_a_duplicate() {
    let pg = PgContainer::start();
    let store = projected_store("accounts", "accounts", &pg.url(), PROJECTED_DDL);

    // A projected table is the AUTHOR's, so its constraints are the author's too. Writing a
    // partial record to an absent key binds every unmentioned declared column NULL, which the
    // author's `NOT NULL` refuses — the case a `<q:store field="…">` create produces.
    //
    // The write must FAIL, loudly, naming the column. The regression this pins: the
    // duplicate-key predicate used to be the whole SQLSTATE class 23, which swept NOT NULL /
    // CHECK / foreign-key failures into the "someone else inserted first" retry path. There the
    // retried UPDATE matches no row — so the failure surfaced as a *conflict*, and on a dialect
    // with no aborted-transaction state to trip over, as SUCCESS having written nothing.
    let partial = json!({"accountId": "ACC-000123"});
    let err = store
        .put("a1", &partial)
        .await
        .expect_err("a NOT NULL violation is a fault, not a lost race");
    let message = err.to_string();
    assert!(
        message.contains("put failed for 'accounts'[a1]"),
        "the failure must name the store and key: {message}"
    );
    assert!(
        !message.contains("deleted concurrently"),
        "it must not be reported as the vanished-row race: {message}"
    );

    // Nothing was written, and the store still works afterwards.
    assert_eq!(store.get("a1").await.unwrap(), None);
    assert_eq!(store.revision("a1").await.unwrap(), 0);
    store.put("a1", &account_record()).await.unwrap();
    assert_eq!(store.revision("a1").await.unwrap(), 1);

    // Same discipline on the CAS create path: `expect="unchanged"` against an absent key does
    // not get to report "conflict" for a write that could never have applied.
    let err = store
        .put_if_revision("a2", &partial, 0)
        .await
        .expect_err("a constraint failure is not a CAS conflict");
    assert!(err.to_string().contains("compare-and-set failed"), "{err}");
    assert_eq!(store.get("a2").await.unwrap(), None);

    // A genuine duplicate still takes the retry path, and on PostgreSQL that path only exists
    // because the INSERT runs inside a SAVEPOINT: a failed statement aborts the whole
    // transaction here, so without it the retried UPDATE would fail with 25P02 and a lost race
    // would be reported as an error. Two writers, one absent key, both succeed — the same
    // convergence MySQL and SQL Server get for free.
    let a = projected_store("accounts", "accounts", &pg.url(), PROJECTED_DDL);
    let b = projected_store("accounts", "accounts", &pg.url(), PROJECTED_DDL);
    let full = account_record();
    let (ra, rb) = tokio::join!(a.put("race", &full), b.put("race", &full));
    ra.expect("writer a");
    rb.expect("writer b");
    let row = store.get("race").await.unwrap().expect("the row exists");
    assert_eq!(row.to_string(), full.to_string());

    // And the savepoint leaves the enclosing transaction USABLE after a rejected write: the
    // constraint failure above is undone, not poisoning, so the caller can carry on and commit.
    let mut tx = store.begin().await.unwrap();
    let err = tx.put("later", &partial).await.expect_err("still refused");
    assert!(err.to_string().contains("put failed"), "{err}");
    tx.put("later", &full)
        .await
        .expect("the transaction survives");
    tx.commit().await.expect("and commits");
    assert!(store.get("later").await.unwrap().is_some());

    // Same for the CAS create path: an expect-absent conflict is a verdict, not a poisoned
    // transaction — `Ok(false)` must leave the caller able to keep working.
    let mut tx = store.begin().await.unwrap();
    assert!(
        !tx.put_if_revision("later", &full, 0).await.unwrap(),
        "the key exists, so expect-absent conflicts"
    );
    tx.put("after-conflict", &full)
        .await
        .expect("the transaction survives the conflict");
    tx.commit().await.expect("and commits");
    assert!(store.get("after-conflict").await.unwrap().is_some());
}

// ---- 2. the REAL money-transfer module -------------------------------------

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn money_transfer_real_migrations_and_fault_trigger() {
    let pg = PgContainer::start();
    let binding_dir = money_transfer_binding_dir();

    // The env contract: the module's datastores.yaml points at env-indirected ACCOUNTS_DB_*
    // values; the URL is the native `postgresql://` form a module developer declares.
    std::env::set_var(
        "ACCOUNTS_DB_URL",
        format!("postgresql://127.0.0.1:{}/sutra", pg.port),
    );
    std::env::set_var("ACCOUNTS_DB_USER", "sutra");
    std::env::set_var("ACCOUNTS_DB_PASSWORD", "sutra");

    let yaml = std::fs::read_to_string(binding_dir.join("datastores.yaml")).unwrap();
    let defs = parse_datastores(&yaml).unwrap();
    assert_eq!(
        defs.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
        vec!["accounts", "coverage"]
    );

    let accounts_def = defs.iter().find(|d| d.name == "accounts").unwrap();
    let coverage_def = defs.iter().find(|d| d.name == "coverage").unwrap();
    let accounts_migrations = load_migrations(accounts_def, &binding_dir).unwrap();
    assert_eq!(
        accounts_migrations.len(),
        1,
        "migrations/accounts/V001__accounts.sql"
    );
    // The coverage store ships NOTHING: the engine owns that schema (§7), so the package has no
    // `migrations/coverage` and the declaration carries no `migrations:` key.
    assert!(
        load_migrations(coverage_def, &binding_dir)
            .unwrap()
            .is_empty(),
        "a deployment package carries no coverage SQL"
    );
    assert!(!binding_dir.join("migrations/coverage").exists());

    let accounts =
        PostgresDataStore::from_definition(accounts_def, accounts_migrations.clone()).unwrap();
    let coverage = CoverageStore::from_definition(coverage_def).unwrap();

    // First use runs the module's own migrations (DDL + seed + fault trigger). The seeded
    // ledger reads through the (store_name, store_key) predicate.
    let alice = accounts.get("alice").await.unwrap().expect("seeded row");
    assert_eq!(alice, json!({"balance": 100, "frozen": false}));
    assert_eq!(
        accounts.get("frozen-fred").await.unwrap().unwrap()["frozen"],
        json!(true)
    );
    assert_eq!(
        accounts.revision("alice").await.unwrap(),
        1,
        "seed inserts rev 1"
    );

    // Idempotency: a second store instance re-runs the same scripts (raw idempotent SQL,
    // not Flyway) — CREATE TABLE IF NOT EXISTS + ON CONFLICT DO NOTHING make it a no-op.
    let rerun = PostgresDataStore::from_definition(accounts_def, accounts_migrations).unwrap();
    assert_eq!(
        rerun.get("alice").await.unwrap().unwrap(),
        json!({"balance": 100, "frozen": false}),
        "re-seed must not clobber"
    );

    // The coverage store: first use applies the ENGINE's coverage DDL to the connection the
    // EXAMPLE declared (the same database `accounts` uses, by the author's choice), and the marks
    // land there. Nothing in the package created these tables.
    let dep = "dep-money-transfer-tier2";
    let urns = vec![
        "urn:sutra:coverage:transfer:accept".to_string(),
        "urn:sutra:coverage:transfer:reject".to_string(),
    ];
    assert_eq!(coverage.seed_declared(dep, &urns).await.unwrap(), 2);
    assert!(coverage.mark_path_covered(dep, &urns[0]).await.unwrap());
    let metrics = coverage.read_metrics(dep).await.unwrap();
    assert_eq!((metrics.total, metrics.covered), (2, 1));
    assert_eq!(metrics.uncovered, vec![urns[1].clone()]);
    assert_eq!(metrics.coverage_percentage(), 50.0);

    // CAS on the live seeded row: read rev, write with it (wins), stale rev conflicts.
    assert!(accounts
        .put_if_revision("alice", &json!({"balance": 60, "frozen": false}), 1)
        .await
        .unwrap());
    assert!(!accounts
        .put_if_revision("alice", &json!({"balance": 0, "frozen": false}), 1)
        .await
        .unwrap());
    assert_eq!(
        accounts.get("alice").await.unwrap().unwrap()["balance"],
        json!(60)
    );

    // The module's fault-injection trigger: the rev-only lock UPDATE must NOT trip it …
    let mut tx = accounts.begin().await.unwrap();
    let sentinel = tx
        .get_for_update("explode-on-credit")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sentinel["balance"], json!(100));
    tx.commit().await.unwrap();

    // … but a value-changing credit DOES — and the surrounding transaction rolls back
    // atomically (the debit is discarded with it).
    let mut tx = accounts.begin().await.unwrap();
    let debited = json!({"balance": 10, "frozen": false});
    tx.put("alice", &debited).await.unwrap();
    let boom = tx
        .put(
            "explode-on-credit",
            &json!({"balance": 200, "frozen": false}),
        )
        .await;
    assert!(boom.is_err(), "injected credit failure must surface");
    tx.rollback().await.unwrap();
    assert_eq!(
        accounts.get("alice").await.unwrap().unwrap()["balance"],
        json!(60),
        "rolled-back transaction must leave the debit unapplied"
    );
}

// ---- 3. the COVERAGE store (engine-owned schema, declared connection) -------

/// A coverage store on this container, declared exactly as an author would (one `sql.url`) —
/// with no migrations of its own, because the engine ships them.
fn coverage_store(url: &str) -> CoverageStore {
    let def = literal_def(sutra_datastore::COVERAGE_STORE_NAME, url);
    CoverageStore::from_definition(&def).expect("coverage store builds")
}

/// The coverage contract on the reference dialect: the engine's DDL applied on first use to a
/// connection the AUTHOR chose, an idempotent seed, durable first-covers-wins, deployment
/// isolation by the bound predicate, and — the property the portable aggregate had to preserve —
/// counts that agree with a client-side recount over the very same rows.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn coverage_store_marks_and_aggregates_on_the_declared_connection() {
    let pg = PgContainer::start();
    let store = coverage_store(&pg.url());
    let dep = "dep-coverage-1";
    let other = "dep-coverage-2";
    let urns: Vec<String> = ["a", "b", "c", "d"]
        .iter()
        .map(|p| format!("urn:sutra:coverage:file:{p}"))
        .collect();

    // First use applies the engine-shipped DDL (no table existed) and seeds every declared path.
    assert_eq!(store.seed_declared(dep, &urns).await.unwrap(), 4);
    // …and the seed is idempotent: a redeploy/replica boot inserts nothing more.
    assert_eq!(store.seed_declared(dep, &urns).await.unwrap(), 0);

    // First-covers-wins, carried by the write itself.
    assert!(store.mark_path_covered(dep, &urns[0]).await.unwrap());
    assert!(!store.mark_path_covered(dep, &urns[0]).await.unwrap());
    // A path that was never seeded is inserted already-covered — once.
    let unseeded = "urn:sutra:coverage:file:z".to_string();
    assert!(store.mark_path_covered(dep, &unseeded).await.unwrap());
    assert!(!store.mark_path_covered(dep, &unseeded).await.unwrap());
    assert!(store.mark_path_covered(dep, &urns[2]).await.unwrap());

    // Another deployment on the SAME store is invisible to the first — deployment_id is the
    // column and the predicate (no RLS on a user-owned connection).
    store.seed_declared(other, &urns).await.unwrap();
    store.mark_path_covered(other, &urns[1]).await.unwrap();

    // PARITY: the SQL aggregate against a client-side recount of the same rows.
    let mut all = urns.clone();
    all.push(unseeded.clone());
    let metrics = store.read_metrics(dep).await.unwrap();
    let covered_set = store.covered_among(dep, &all).await.unwrap();
    let recounted_uncovered: Vec<String> = all
        .iter()
        .filter(|u| !covered_set.contains(*u))
        .cloned()
        .collect();
    assert_eq!(metrics.total as usize, all.len());
    assert_eq!(metrics.covered as usize, covered_set.len());
    assert_eq!(metrics.uncovered, recounted_uncovered);
    assert_eq!(metrics.coverage_percentage(), 60.0);
    assert_eq!(store.count_metrics(dep).await.unwrap(), metrics.counts());
    assert_eq!(store.read_metrics(other).await.unwrap().covered, 1);

    // Scoped clear: only the named, actually-covered paths, and the count IS the answer.
    assert_eq!(store.clear_paths(dep, &all).await.unwrap(), 3);
    assert_eq!(store.clear_paths(dep, &all).await.unwrap(), 0);
    assert_eq!(store.read_metrics(dep).await.unwrap().covered, 0);

    // Reconstruction fragments round-trip in insertion order, and reset drops them.
    for (i, process) in ["hop-a", "hop-b"].iter().enumerate() {
        store
            .write_fragment(
                dep,
                &sutra_datastore::CoverageFragmentRow {
                    route_urn: urns[0].clone(),
                    segment_process: (*process).to_string(),
                    instance_id: format!("inst-{i}"),
                    business_key: Some(format!("bk-{i}")),
                    trace_id: None,
                },
            )
            .await
            .unwrap();
    }
    let fragments = store.read_fragments(dep).await.unwrap();
    assert_eq!(fragments.len(), 2);
    assert_eq!(fragments[0].segment_process, "hop-a");
    assert_eq!(fragments[1].business_key.as_deref(), Some("bk-1"));
    assert_eq!(fragments[0].trace_id, None);

    store.mark_path_covered(dep, &urns[3]).await.unwrap();
    store.reset(dep).await.unwrap();
    assert_eq!(store.read_metrics(dep).await.unwrap().covered, 0);
    assert!(store.read_fragments(dep).await.unwrap().is_empty());
    assert_eq!(
        store.read_metrics(other).await.unwrap().covered,
        1,
        "reset is deployment-scoped"
    );

    // Second store instance over the same connection: the DDL re-runs as a no-op.
    let again = coverage_store(&pg.url());
    assert_eq!(again.read_metrics(dep).await.unwrap().total, 5);
}
