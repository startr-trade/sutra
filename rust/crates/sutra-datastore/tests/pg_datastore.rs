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
    build_migration_scripts, load_migrations, parse_datastores, CoverageStore,
    MigrationPlaceholders, MigrationScript, PostgresDataStore, ProjectedStore, StoreDefinition,
    StructureRef,
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
    let other =
        PostgresDataStore::from_definition(&literal_def("other", &pg.url()), Vec::<String>::new())
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

// ---- key queries (`<q:store query=…>`, 0.6.10) ------------------------------

/// The cutover table with its key column in the bytewise "C" collation — what a store that runs
/// key queries ships, so the primary key serves a key range in bytewise order.
const QUERY_DDL: &str = "CREATE TABLE IF NOT EXISTS data_store (\n\
  store_name  VARCHAR(128) NOT NULL,\n\
  store_key   VARCHAR(512) COLLATE \"C\" NOT NULL,\n\
  store_value TEXT         NOT NULL,\n\
  rev         BIGINT       NOT NULL DEFAULT 1,\n\
  updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,\n\
  PRIMARY KEY (store_name, store_key)\n\
);";

const ACCOUNTS: i64 = 5;
const DAYS: i64 = 20;
const PER_DAY: i64 = 1000;

/// The amount (in cents) of row `seq`: deterministic, so the expected sums are computable.
fn cents(seq: i64) -> i64 {
    (seq * 7919) % 100_000 + 1
}

fn decimal_text(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

async fn admin_pool(pg: &PgContainer, db: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&format!(
            "postgres://sutra:sutra@127.0.0.1:{}/{db}",
            pg.port
        ))
        .await
        .expect("admin connect")
}

fn day_prefix(account: i64, day: i64) -> String {
    format!("src-{account}|2026-10-{day:02}|")
}

fn day_query(account: i64, day: i64, row_limit: Option<u32>) -> sutra_datastore::KeyRangeQuery {
    let lower = day_prefix(account, day);
    sutra_datastore::KeyRangeQuery {
        upper: sutra_datastore::keyquery::prefix_upper_bound(&lower),
        lower,
        after: None,
        row_limit,
        aggregates: vec![
            sutra_datastore::KeyQueryAggregate {
                sum_field: Some(vec!["amount".into()]),
                group_by: Some(vec!["dir".into()]),
            },
            sutra_datastore::KeyQueryAggregate {
                sum_field: None,
                group_by: Some(vec!["tag".into()]),
            },
            sutra_datastore::KeyQueryAggregate {
                sum_field: Some(vec!["amount".into()]),
                group_by: None,
            },
        ],
    }
}

/// 100 000 rows (5 accounts x 20 days x 1000), then: one day's prefix query with grouped and
/// ungrouped exact sums checked against a client-side recount, every page of a paged walk, and
/// EXPLAIN over the very statements the store ran — index range scans, never a sequential scan.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn key_query_pages_groups_and_sums_over_the_primary_key() {
    let pg = PgContainer::start();
    let store = PostgresDataStore::from_definition(
        &literal_def("events", &pg.url()),
        vec![QUERY_DDL.to_string()],
    )
    .expect("store builds");
    // First use runs the migration (an empty query is enough), then bulk-load the rows.
    let warm = store.query(&day_query(0, 1, Some(1))).await.unwrap();
    assert!(warm.rows.is_empty());
    let admin = admin_pool(&pg, "sutra").await;
    sqlx::query(&format!(
        "INSERT INTO data_store (store_name, store_key, store_value) \
         SELECT 'events', \
                'src-' || a || '|2026-10-' || lpad(d::text, 2, '0') || '|' || lpad(s::text, 6, '0'), \
                json_build_object( \
                  'amount', round(((s * 7919) % 100000 + 1) / 100.0, 2), \
                  'dir', CASE WHEN s % 3 = 0 THEN 'OUT' ELSE 'IN' END, \
                  'tag', CASE s % 4 WHEN 0 THEN 'OPEN' WHEN 1 THEN 'CLOSE' WHEN 2 THEN 'RETRY' ELSE 'SKIP' END \
                )::text \
         FROM generate_series(0, {}) a, generate_series(1, {DAYS}) d, generate_series(0, {}) s",
        ACCOUNTS - 1,
        PER_DAY - 1
    ))
    .execute(&admin)
    .await
    .expect("bulk load");
    // Rows of another store under the same keys must never leak into "events".
    sqlx::query(
        "INSERT INTO data_store (store_name, store_key, store_value) \
         VALUES ('other', 'src-2|2026-10-07|000001', '{\"amount\": 1000000}')",
    )
    .execute(&admin)
    .await
    .unwrap();
    sqlx::query("ANALYZE data_store")
        .execute(&admin)
        .await
        .unwrap();

    // ---- one day: rows + exact aggregates ------------------------------------
    let page = store.query(&day_query(2, 7, Some(300))).await.unwrap();
    assert_eq!(page.rows.len(), 300);
    assert!(page.truncated);
    assert_eq!(page.next.as_deref(), Some("src-2|2026-10-07|000299"));
    assert_eq!(page.rows[0].0, "src-2|2026-10-07|000000");
    let (mut inbound, mut outbound, mut in_n, mut out_n) = (0i64, 0i64, 0i64, 0i64);
    for s in 0..PER_DAY {
        if s % 3 == 0 {
            outbound += cents(s);
            out_n += 1;
        } else {
            inbound += cents(s);
            in_n += 1;
        }
    }
    let sutra_datastore::KeyAggregateResult::Grouped(by_dir) = &page.aggregates[0] else {
        panic!("grouped")
    };
    let as_cents = |text: &str| -> i64 {
        let (whole, frac) = text.split_once('.').unwrap_or((text, "0"));
        whole.parse::<i64>().unwrap() * 100 + format!("{frac:0<2}")[..2].parse::<i64>().unwrap()
    };
    assert_eq!(by_dir["IN"].count, in_n);
    assert_eq!(as_cents(by_dir["IN"].sum.as_deref().unwrap()), inbound);
    assert_eq!(by_dir["OUT"].count, out_n);
    assert_eq!(as_cents(by_dir["OUT"].sum.as_deref().unwrap()), outbound);
    let sutra_datastore::KeyAggregateResult::Grouped(by_tag) = &page.aggregates[1] else {
        panic!("grouped")
    };
    assert_eq!(by_tag.len(), 4);
    assert!(by_tag
        .values()
        .all(|c| c.count == PER_DAY / 4 && c.sum.is_none()));
    let sutra_datastore::KeyAggregateResult::Total(total) = &page.aggregates[2] else {
        panic!("total")
    };
    assert_eq!(total.count, PER_DAY, "aggregates ignore the row limit");
    assert_eq!(
        total.sum.as_deref(),
        Some(decimal_text(inbound + outbound).as_str())
    );
    assert_eq!(total.non_numeric, 0);

    // ---- paging: every row exactly once, in order, aggregates stable ---------
    let mut query = day_query(2, 7, Some(300));
    let mut seen: Vec<String> = Vec::new();
    loop {
        let page = store.query(&query).await.unwrap();
        assert_eq!(
            page.aggregates[2],
            sutra_datastore::KeyAggregateResult::Total(total.clone())
        );
        seen.extend(page.rows.iter().map(|(k, _)| k.clone()));
        match page.next {
            Some(next) => query.after = Some(next),
            None => {
                assert!(!page.truncated);
                break;
            }
        }
    }
    assert_eq!(seen.len() as i64, PER_DAY);
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "ascending, no repeats"
    );

    // ---- a range across days -------------------------------------------------
    let range = sutra_datastore::KeyRangeQuery {
        lower: "src-4|2026-10-03|".into(),
        upper: Some("src-4|2026-10-06|".into()),
        after: None,
        row_limit: None,
        aggregates: vec![sutra_datastore::KeyQueryAggregate {
            sum_field: None,
            group_by: None,
        }],
    };
    let page = store.query(&range).await.unwrap();
    assert!(page.rows.is_empty(), "rows=false");
    assert_eq!(
        page.aggregates[0],
        sutra_datastore::KeyAggregateResult::Total(sutra_datastore::KeyAggregateCell {
            count: 3 * PER_DAY,
            sum: None,
            non_numeric: 0,
        })
    );

    // ---- EXPLAIN the statements the store runs: the primary key, no seq scan --
    let mut explained = day_query(2, 7, Some(300));
    explained.after = Some("src-2|2026-10-07|000299".into());
    let mut statements = vec![(
        sutra_datastore::keyquery::rows_sql(sutra_datastore::SqlDialect::Postgres, &explained, 300),
        true,
    )];
    for agg in &explained.aggregates {
        statements.push((
            sutra_datastore::keyquery::aggregate_sql(
                sutra_datastore::SqlDialect::Postgres,
                &explained,
                agg,
            ),
            false,
        ));
    }
    for (sql, with_after) in statements {
        let explain_sql = format!("EXPLAIN {sql}");
        let mut explain = sqlx::query_scalar::<_, String>(&explain_sql).persistent(false);
        for value in sutra_datastore::keyquery::binds("events", &explained, with_after) {
            explain = explain.bind(value.to_string());
        }
        let plan = explain.fetch_all(&admin).await.unwrap().join("\n");
        assert!(
            !plan.contains("Seq Scan"),
            "sequential scan in:\n{plan}\nfor {sql}"
        );
        assert!(
            plan.contains("data_store_pkey"),
            "primary key not used:\n{plan}\nfor {sql}"
        );
    }

    // ---- the numeric rules on the database ---------------------------------
    for (key, value) in [
        ("edge|1", json!({"amount": 1.25, "dir": "A"})),
        ("edge|2", json!({"amount": "2.50", "dir": "A"})),
        ("edge|3", json!({"dir": "B"})),
        ("edge|4", json!({"amount": null})),
        ("edge|5", json!({"amount": "abc", "dir": "B"})),
        ("edge|6", json!({"amount": true, "dir": "B"})),
        ("edge|7", json!({"amount": {"v": 1}, "dir": "B"})),
    ] {
        store.put(key, &value).await.unwrap();
    }
    let edge = sutra_datastore::KeyRangeQuery {
        lower: "edge|".into(),
        upper: sutra_datastore::keyquery::prefix_upper_bound("edge|"),
        after: None,
        row_limit: Some(10),
        aggregates: vec![
            sutra_datastore::KeyQueryAggregate {
                sum_field: Some(vec!["amount".into()]),
                group_by: Some(vec!["dir".into()]),
            },
            sutra_datastore::KeyQueryAggregate {
                sum_field: Some(vec!["amount".into()]),
                group_by: None,
            },
        ],
    };
    let page = store.query(&edge).await.unwrap();
    assert_eq!(page.rows.len(), 7);
    let sutra_datastore::KeyAggregateResult::Total(cell) = &page.aggregates[1] else {
        panic!("total")
    };
    assert_eq!(cell.count, 7);
    assert_eq!(
        cell.sum.as_deref(),
        Some("3.75"),
        "1.25 + \"2.50\"; missing and null skipped"
    );
    assert_eq!(cell.non_numeric, 3, "text, boolean and object");
    let sutra_datastore::KeyAggregateResult::Grouped(groups) = &page.aggregates[0] else {
        panic!("grouped")
    };
    assert_eq!(groups["A"].sum.as_deref(), Some("3.75"));
    assert_eq!(groups["B"].non_numeric, 3);
    assert_eq!(
        groups["null"].count, 1,
        "a row without the group field groups under null"
    );
    assert_eq!(groups["null"].sum.as_deref(), Some("0"));

    // ---- no match: zeros, not null -----------------------------------------
    let mut none = day_query(9, 1, Some(10));
    none.lower = "nobody|".into();
    none.upper = sutra_datastore::keyquery::prefix_upper_bound("nobody|");
    let page = store.query(&none).await.unwrap();
    assert!(page.rows.is_empty() && !page.truncated && page.next.is_none());
    assert_eq!(
        page.aggregates[0],
        sutra_datastore::KeyAggregateResult::Grouped(BTreeMap::new())
    );
    assert_eq!(
        page.aggregates[2],
        sutra_datastore::KeyAggregateResult::Total(sutra_datastore::KeyAggregateCell {
            count: 0,
            sum: Some("0".into()),
            non_numeric: 0,
        })
    );
    admin.close().await;
}

/// A key column in a linguistic collation is refused before any query runs, naming the fix.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn key_query_refuses_a_key_column_that_does_not_sort_bytewise() {
    let pg = PgContainer::start();
    let admin = admin_pool(&pg, "sutra").await;
    sqlx::query("CREATE DATABASE linguistic TEMPLATE template0 LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'")
        .execute(&admin)
        .await
        .expect("create database");
    admin.close().await;
    let store = PostgresDataStore::from_definition(
        &literal_def(
            "events",
            &format!("postgres://127.0.0.1:{}/linguistic", pg.port),
        ),
        vec![CUTOVER_DDL.to_string()],
    )
    .expect("store builds");
    let e = store.query(&day_query(0, 1, Some(10))).await.unwrap_err();
    assert_eq!(
        e.code(),
        Some(sutra_datastore::keyquery::codes::QUERY_KEY_ORDER),
        "{e}"
    );
    assert!(e.to_string().contains("COLLATE \"C\""), "{e}");

    // A bytewise key column but a primary key led by namespace columns: no index serves the
    // range, so the query is refused instead of scanning the table.
    let legacy = PostgresDataStore::from_definition_with_namespace(
        &literal_def("events", &pg.url()),
        vec!["CREATE TABLE IF NOT EXISTS data_store (\
              tenant_id VARCHAR(64) NOT NULL, module_id VARCHAR(256) NOT NULL, \
              module_version VARCHAR(64) NOT NULL, store_name VARCHAR(128) NOT NULL, \
              store_key VARCHAR(512) COLLATE \"C\" NOT NULL, store_value TEXT NOT NULL, \
              rev BIGINT NOT NULL DEFAULT 1, \
              updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, \
              PRIMARY KEY (tenant_id, module_id, module_version, store_name, store_key))"
            .to_string()],
        Some(("t".into(), "m".into(), "1.0.0".into())),
    )
    .expect("store builds");
    let e = legacy.query(&day_query(0, 1, Some(10))).await.unwrap_err();
    assert_eq!(
        e.code(),
        Some(sutra_datastore::keyquery::codes::QUERY_KEY_INDEX),
        "{e}"
    );
}

/// Several stores on one database that share a table and its idempotent DDL, first used at the
/// same moment (two replicas, or one engine's lanes): every first use succeeds. The migration lock
/// used to be keyed by store name, so the stores ran `CREATE TABLE IF NOT EXISTS` at once and the
/// loser failed with a duplicate key on `pg_type_typname_nsp_index`.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stores_sharing_a_table_migrate_concurrently_without_racing() {
    let pg = PgContainer::start();
    let mut tasks = Vec::new();
    for round in 0..3 {
        // A fresh database each round, so every round races the very first CREATE TABLE.
        let db = format!("race_{round}");
        let status = Command::new("docker")
            .args([
                "exec",
                &pg.id,
                "psql",
                "-U",
                "sutra",
                "-d",
                "postgres",
                "-c",
                &format!("CREATE DATABASE {db}"),
            ])
            .output()
            .expect("docker exec");
        assert!(
            status.status.success(),
            "{}",
            String::from_utf8_lossy(&status.stderr)
        );
        let url = pg
            .url()
            .rsplit_once('/')
            .map(|(base, _)| format!("{base}/{db}"))
            .unwrap();
        for n in 0..8 {
            let store = PostgresDataStore::from_definition(
                &literal_def(&format!("store-{n}"), &url),
                vec![CUTOVER_DDL.to_string()],
            )
            .expect("store builds");
            tasks.push(tokio::spawn(async move {
                store.put("k", &json!({"n": n})).await
            }));
        }
    }
    for task in tasks {
        task.await
            .unwrap()
            .unwrap_or_else(|e| panic!("first use failed: {e}"));
    }
}

// ---- key queries on a typed (projected) store (0.6.13) -------------------------------------

/// The projected table of a typed store that runs key queries: `store_key` is the primary key in
/// the bytewise "C" collation, so the primary key serves a key range in bytewise order.
const TYPED_QUERY_DDL: &str = "CREATE TABLE IF NOT EXISTS ledger_entries (\n\
  store_key  VARCHAR(512)  COLLATE \"C\" NOT NULL PRIMARY KEY,\n\
  account_id VARCHAR(35)   NOT NULL,\n\
  balance    NUMERIC(18,2) NOT NULL,\n\
  seq_no     INTEGER       NOT NULL,\n\
  frozen     BOOLEAN       NOT NULL,\n\
  opened_at  DATE,\n\
  rev        BIGINT        NOT NULL DEFAULT 1,\n\
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP\n\
);";

const TYPED_ACCOUNTS: i64 = 4;
const TYPED_PER_ACCOUNT: i64 = 1500;

fn typed_agg(sum: Option<&str>, group: Option<&str>) -> sutra_datastore::KeyQueryAggregate {
    sutra_datastore::KeyQueryAggregate {
        sum_field: sum.map(|s| vec![s.to_string()]),
        group_by: group.map(|g| vec![g.to_string()]),
    }
}

fn typed_prefix_query(
    prefix: &str,
    row_limit: Option<u32>,
    aggregates: Vec<sutra_datastore::KeyQueryAggregate>,
) -> sutra_datastore::KeyRangeQuery {
    sutra_datastore::KeyRangeQuery {
        lower: prefix.to_string(),
        upper: sutra_datastore::keyquery::prefix_upper_bound(prefix),
        after: None,
        row_limit,
        aggregates,
    }
}

/// 6 000 typed rows, then: a prefix query whose rows are the typed records a get returns, exact
/// grouped and ungrouped sums on typed columns checked against a client-side recount, a paged
/// walk, a range, an empty match, the non-numeric rules, the path refusals, and EXPLAIN over the
/// statements the store ran — the primary key, never a sequential scan.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn typed_store_key_query_pages_groups_and_sums_typed_columns() {
    let pg = PgContainer::start();
    let store = projected_store("ledger", "ledger_entries", &pg.url(), TYPED_QUERY_DDL);
    let warm = store
        .query(&typed_prefix_query("acct-0|", Some(1), vec![]))
        .await
        .unwrap();
    assert!(warm.rows.is_empty());
    let admin = admin_pool(&pg, "sutra").await;
    sqlx::query(&format!(
        "INSERT INTO ledger_entries (store_key, account_id, balance, seq_no, frozen, opened_at) \
         SELECT 'acct-' || a || '|' || lpad(s::text, 6, '0'), 'ACC-' || a || '-' || s, \
                round(((s * 7919) % 100000 + 1) / 100.0, 2), s % 5, s % 3 = 0, \
                CASE WHEN s % 4 = 0 THEN NULL ELSE DATE '2026-10-01' + (s % 7) END \
         FROM generate_series(0, {}) a, generate_series(0, {}) s",
        TYPED_ACCOUNTS - 1,
        TYPED_PER_ACCOUNT - 1
    ))
    .execute(&admin)
    .await
    .expect("bulk load");
    sqlx::query("ANALYZE ledger_entries")
        .execute(&admin)
        .await
        .unwrap();

    // ---- one prefix: typed rows + exact aggregates ----------------------------
    let aggregates = vec![
        typed_agg(Some("balance"), Some("frozen")),
        typed_agg(None, Some("openedAt")),
        typed_agg(Some("balance"), None),
        typed_agg(Some("seqNo"), None),
    ];
    let query = typed_prefix_query("acct-2|", Some(400), aggregates);
    let page = store.query(&query).await.unwrap();
    assert_eq!(page.rows.len(), 400);
    assert!(page.truncated);
    assert_eq!(page.next.as_deref(), Some("acct-2|000399"));
    assert_eq!(page.rows[0].0, "acct-2|000000");
    for (key, value) in [&page.rows[0], &page.rows[1], &page.rows[399]] {
        let got = store.get(key).await.unwrap().expect("row exists");
        assert_eq!(value, &got, "a query row is the record a typed get returns");
    }
    assert_eq!(page.rows[1].1["balance"].to_string(), "79.20");
    assert!(
        page.rows[0].1.get("openedAt").is_none(),
        "NULL reads absent"
    );

    let (mut frozen_c, mut thawed_c, mut frozen_n, mut thawed_n, mut seq_sum) =
        (0i64, 0i64, 0i64, 0i64, 0i64);
    for s in 0..TYPED_PER_ACCOUNT {
        if s % 3 == 0 {
            frozen_c += cents(s);
            frozen_n += 1;
        } else {
            thawed_c += cents(s);
            thawed_n += 1;
        }
        seq_sum += s % 5;
    }
    let sutra_datastore::KeyAggregateResult::Grouped(by_frozen) = &page.aggregates[0] else {
        panic!("grouped")
    };
    assert_eq!(by_frozen.len(), 2);
    assert_eq!(by_frozen["true"].count, frozen_n);
    assert_eq!(
        by_frozen["true"].sum.as_deref(),
        Some(decimal_text(frozen_c).as_str())
    );
    assert_eq!(by_frozen["false"].count, thawed_n);
    assert_eq!(
        by_frozen["false"].sum.as_deref(),
        Some(decimal_text(thawed_c).as_str())
    );
    let sutra_datastore::KeyAggregateResult::Grouped(by_day) = &page.aggregates[1] else {
        panic!("grouped")
    };
    assert_eq!(by_day.len(), 8, "seven dates and null");
    assert_eq!(
        by_day["null"].count,
        TYPED_PER_ACCOUNT / 4,
        "an absent optional field groups under null"
    );
    assert!(by_day.contains_key("2026-10-01") && by_day.values().all(|c| c.sum.is_none()));
    let sutra_datastore::KeyAggregateResult::Total(total) = &page.aggregates[2] else {
        panic!("total")
    };
    assert_eq!(
        total.count, TYPED_PER_ACCOUNT,
        "aggregates ignore the row limit"
    );
    assert_eq!(
        total.sum.as_deref(),
        Some(decimal_text(frozen_c + thawed_c).as_str())
    );
    assert_eq!(total.non_numeric, 0);
    let sutra_datastore::KeyAggregateResult::Total(seq) = &page.aggregates[3] else {
        panic!("total")
    };
    assert_eq!(seq.sum.as_deref(), Some(seq_sum.to_string().as_str()));

    // ---- paging: every row exactly once, in order, aggregates stable ---------
    let mut walk = query.clone();
    let mut seen: Vec<String> = Vec::new();
    loop {
        let page = store.query(&walk).await.unwrap();
        assert_eq!(
            page.aggregates[2],
            sutra_datastore::KeyAggregateResult::Total(total.clone())
        );
        seen.extend(page.rows.iter().map(|(k, _)| k.clone()));
        match page.next {
            Some(next) => walk.after = Some(next),
            None => {
                assert!(!page.truncated);
                break;
            }
        }
    }
    assert_eq!(seen.len() as i64, TYPED_PER_ACCOUNT);
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "ascending, no repeats"
    );

    // ---- a range across accounts, aggregates only -----------------------------
    let range = sutra_datastore::KeyRangeQuery {
        lower: "acct-1|".into(),
        upper: Some("acct-3|".into()),
        after: None,
        row_limit: None,
        aggregates: vec![typed_agg(None, None)],
    };
    let page = store.query(&range).await.unwrap();
    assert!(page.rows.is_empty());
    assert_eq!(
        page.aggregates[0],
        sutra_datastore::KeyAggregateResult::Total(sutra_datastore::KeyAggregateCell {
            count: 2 * TYPED_PER_ACCOUNT,
            sum: None,
            non_numeric: 0,
        })
    );

    // ---- no match: zeros, not null ----------------------------------------------
    let none = typed_prefix_query(
        "nobody|",
        Some(10),
        vec![
            typed_agg(Some("balance"), Some("frozen")),
            typed_agg(Some("balance"), None),
        ],
    );
    let page = store.query(&none).await.unwrap();
    assert!(page.rows.is_empty() && !page.truncated && page.next.is_none());
    assert_eq!(
        page.aggregates[0],
        sutra_datastore::KeyAggregateResult::Grouped(BTreeMap::new())
    );
    assert_eq!(
        page.aggregates[1],
        sutra_datastore::KeyAggregateResult::Total(sutra_datastore::KeyAggregateCell {
            count: 0,
            sum: Some("0".into()),
            non_numeric: 0,
        })
    );

    // ---- a non-numeric column: every present value is counted, never summed --
    let odd = typed_prefix_query(
        "acct-3|",
        None,
        vec![
            typed_agg(Some("accountId"), None),
            typed_agg(Some("openedAt"), None),
        ],
    );
    let page = store.query(&odd).await.unwrap();
    let sutra_datastore::KeyAggregateResult::Total(text) = &page.aggregates[0] else {
        panic!("total")
    };
    assert_eq!(
        (text.count, text.sum.as_deref(), text.non_numeric),
        (TYPED_PER_ACCOUNT, Some("0"), TYPED_PER_ACCOUNT)
    );
    let sutra_datastore::KeyAggregateResult::Total(date) = &page.aggregates[1] else {
        panic!("total")
    };
    assert_eq!(
        date.non_numeric,
        TYPED_PER_ACCOUNT - TYPED_PER_ACCOUNT / 4,
        "NULL skipped"
    );

    // ---- a path that does not name one declared field is refused ----------------
    for agg in [
        sutra_datastore::KeyQueryAggregate {
            sum_field: Some(vec!["balance".into(), "value".into()]),
            group_by: None,
        },
        typed_agg(None, Some("missing")),
    ] {
        let e = store
            .query(&typed_prefix_query("acct-0|", Some(1), vec![agg]))
            .await
            .unwrap_err();
        assert_eq!(
            e.code(),
            Some(sutra_datastore::keyquery::codes::QUERY_UNSUPPORTED),
            "{e}"
        );
    }

    // ---- EXPLAIN the statements the store runs: the primary key, no seq scan --
    let projection = account_projection("ledger", "ledger_entries");
    let dialect = sutra_datastore::SqlDialect::Postgres;
    let mut explained = query.clone();
    explained.after = Some("acct-2|000399".into());
    let mut statements = vec![(
        sutra_datastore::keyquery::typed_rows_sql(dialect, &projection, &explained, 400),
        true,
    )];
    for agg in &explained.aggregates {
        statements.push((
            sutra_datastore::keyquery::typed_aggregate_sql(dialect, &projection, &explained, agg)
                .unwrap(),
            false,
        ));
    }
    for (sql, with_after) in statements {
        let explain_sql = format!("EXPLAIN {sql}");
        let mut explain = sqlx::query_scalar::<_, String>(&explain_sql).persistent(false);
        for value in sutra_datastore::keyquery::typed_binds(&explained, with_after) {
            explain = explain.bind(value.to_string());
        }
        let plan = explain.fetch_all(&admin).await.unwrap().join("\n");
        assert!(
            !plan.contains("Seq Scan"),
            "sequential scan in:\n{plan}\nfor {sql}"
        );
        assert!(
            plan.contains("ledger_entries_pkey"),
            "primary key not used:\n{plan}\nfor {sql}"
        );
    }
    admin.close().await;
}

/// A typed table whose key column is not bytewise, or whose indexes do not lead with
/// `store_key`, is refused before any query runs, naming the table and the fix.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn typed_store_key_query_refuses_a_linguistic_or_unindexed_key() {
    let pg = PgContainer::start();
    let admin = admin_pool(&pg, "sutra").await;
    sqlx::query("CREATE DATABASE linguistic TEMPLATE template0 LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'")
        .execute(&admin)
        .await
        .expect("create database");
    admin.close().await;
    let linguistic = projected_store(
        "ledger",
        "accounts",
        &format!("postgres://127.0.0.1:{}/linguistic", pg.port),
        PROJECTED_DDL,
    );
    let e = linguistic
        .query(&typed_prefix_query("acct-0|", Some(1), vec![]))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Some(sutra_datastore::keyquery::codes::QUERY_KEY_ORDER),
        "{e}"
    );
    assert!(e.to_string().contains("ALTER TABLE accounts"), "{e}");

    let unindexed = projected_store(
        "ledger",
        "unindexed",
        &pg.url(),
        "CREATE TABLE IF NOT EXISTS unindexed (\
         store_key VARCHAR(512) COLLATE \"C\" NOT NULL, account_id VARCHAR(35) NOT NULL, \
         balance NUMERIC(18,2) NOT NULL, seq_no INTEGER NOT NULL, frozen BOOLEAN NOT NULL, \
         opened_at DATE, rev BIGINT NOT NULL DEFAULT 1, \
         updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, \
         PRIMARY KEY (seq_no, store_key))",
    );
    let e = unindexed
        .query(&typed_prefix_query("acct-0|", Some(1), vec![]))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Some(sutra_datastore::keyquery::codes::QUERY_KEY_INDEX),
        "{e}"
    );
    assert!(e.to_string().contains("table unindexed"), "{e}");
}

// ---- the migration ledger (run once, checksums, adoption, placeholders) ------

/// The seed is deliberately NOT idempotent: a second run violates the primary key, so a store
/// that re-ran an applied script would fail its first use.
const LEDGER_SEED: &str = "INSERT INTO data_store (store_name, store_key, store_value) \
     VALUES ('${store.name}', 'seed-${package.tenant}', '{\"module\":\"${package.module}\"}');";
const LEDGER_V3: &str = "INSERT INTO data_store (store_name, store_key, store_value) \
     VALUES ('${store.name}', 'v3', '{\"version\":\"${package.version}\"}');";

fn ledger_scripts(store: &str, files: &[(&str, &str)]) -> Vec<MigrationScript> {
    let placeholders = MigrationPlaceholders {
        tenant: "acme".into(),
        module: "orders".into(),
        version: "1.0.0".into(),
        store_name: store.into(),
        store_table: "data_store".into(),
    };
    let files = files
        .iter()
        .map(|(f, sql)| (f.to_string(), sql.to_string()))
        .collect();
    build_migration_scripts(store, files, &placeholders).expect("scripts prepare")
}

async fn ledger_rows(url: &str, store: &str) -> Vec<(String, String, String)> {
    let pool = sqlx::PgPool::connect(&url.replace("postgres://", "postgres://sutra:sutra@"))
        .await
        .expect("direct connect");
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT version, script, checksum FROM sutra_store_migrations \
         WHERE store_name = $1 ORDER BY applied_at, version",
    )
    .bind(store)
    .fetch_all(&pool)
    .await
    .expect("ledger readable");
    pool.close().await;
    rows
}

#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn migration_ledger_runs_each_script_once_and_refuses_an_edited_one() {
    let pg = PgContainer::start();
    let url = pg.url();
    let def = literal_def("ledger", &url);
    let v1v2 = [
        ("V1__table.sql", CUTOVER_DDL),
        ("V2__seed.sql", LEDGER_SEED),
    ];

    // (1) + (6): first boot applies and records both; the seed carries the substituted labels.
    let first = PostgresDataStore::from_definition(&def, ledger_scripts("ledger", &v1v2)).unwrap();
    assert_eq!(
        first.get("seed-acme").await.unwrap(),
        Some(json!({"module": "orders"}))
    );
    let rows = ledger_rows(&url, "ledger").await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        (rows[0].0.as_str(), rows[0].1.as_str()),
        ("1", "V1__table.sql")
    );
    assert_eq!(rows[1].2, ledger_scripts("ledger", &v1v2)[1].checksum);

    // A re-boot (new instance, same database) runs nothing — the seed would collide.
    let reboot = PostgresDataStore::from_definition(&def, ledger_scripts("ledger", &v1v2)).unwrap();
    assert!(reboot.get("seed-acme").await.unwrap().is_some());
    assert_eq!(ledger_rows(&url, "ledger").await.len(), 2);

    // (2) adding V3 runs exactly V3, once.
    let v1v3 = [v1v2[0], v1v2[1], ("V3__more.sql", LEDGER_V3)];
    let upgraded =
        PostgresDataStore::from_definition(&def, ledger_scripts("ledger", &v1v3)).unwrap();
    assert_eq!(
        upgraded.get("v3").await.unwrap(),
        Some(json!({"version": "1.0.0"}))
    );
    let again = PostgresDataStore::from_definition(&def, ledger_scripts("ledger", &v1v3)).unwrap();
    assert!(again.get("v3").await.unwrap().is_some());
    assert_eq!(ledger_rows(&url, "ledger").await.len(), 3);

    // (3) an edited, already-applied script refuses the store with the checksum code.
    let edited = [v1v2[0], ("V2__seed.sql", "SELECT 1;")];
    let refused =
        PostgresDataStore::from_definition(&def, ledger_scripts("ledger", &edited)).unwrap();
    let err = refused.get("seed-acme").await.unwrap_err().to_string();
    assert!(
        err.contains("SUTRA.RUNTIME.DATASTORE.MIGRATION_CHECKSUM") && err.contains("V2__seed.sql"),
        "{err}"
    );

    // (4) two replicas racing first use apply each script exactly once.
    let race = literal_def("race", &url);
    let a = PostgresDataStore::from_definition(&race, ledger_scripts("race", &v1v2)).unwrap();
    let b = PostgresDataStore::from_definition(&race, ledger_scripts("race", &v1v2)).unwrap();
    let (ra, rb) = tokio::join!(a.get("seed-acme"), b.get("seed-acme"));
    assert!(ra.unwrap().is_some() && rb.unwrap().is_some());
    assert_eq!(ledger_rows(&url, "race").await.len(), 2);

    // (5) adoption: a table built before the ledger existed (no rows for the store) — its
    // idempotent scripts run once more and are recorded; existing rows survive.
    let adopt = literal_def("adopt", &url);
    let legacy = PostgresDataStore::from_definition(&adopt, vec![CUTOVER_DDL.to_string()]).unwrap();
    legacy.put("kept", &json!({"old": true})).await.unwrap();
    assert!(ledger_rows(&url, "adopt").await.is_empty());
    let adopted = PostgresDataStore::from_definition(
        &adopt,
        ledger_scripts("adopt", &[("V1__table.sql", CUTOVER_DDL)]),
    )
    .unwrap();
    assert_eq!(
        adopted.get("kept").await.unwrap(),
        Some(json!({"old": true}))
    );
    assert_eq!(ledger_rows(&url, "adopt").await.len(), 1);
}

/// A failed script records nothing (transactional DDL) — and a fixed package then applies.
#[ignore = "docker"]
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_migration_records_nothing() {
    let pg = PgContainer::start();
    let url = pg.url();
    let def = literal_def("broken", &url);
    let bad = [
        ("V1__table.sql", CUTOVER_DDL),
        (
            "V2__bad.sql",
            "CREATE TABLE half_done (x INT); SELECT * FROM no_such_table;",
        ),
    ];
    let store = PostgresDataStore::from_definition(&def, ledger_scripts("broken", &bad)).unwrap();
    assert!(store.get("k").await.is_err());
    let rows = ledger_rows(&url, "broken").await;
    assert_eq!(rows.len(), 1, "only V1 is recorded: {rows:?}");
    let good = [bad[0], ("V2__bad.sql", "CREATE TABLE half_done (x INT);")];
    let fixed = PostgresDataStore::from_definition(&def, ledger_scripts("broken", &good)).unwrap();
    assert_eq!(fixed.get("k").await.unwrap(), None);
    assert_eq!(ledger_rows(&url, "broken").await.len(), 2);
}
