//! Shared MySQL-dialect fixture: one container per test binary (the enclosing suite's
//! `start_db` decides the engine — mysql:8.0 or mariadb:11), a uniquely-named database per
//! test, and a [`MysqlDataStore`] built against it on the cutover table shape. Rows
//! partition by `store_name`, so a test that wants two independent stores just names them
//! differently on the same database.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sqlx::mysql::MySqlPoolOptions;
use sutra_datastore::{MysqlDataStore, ProjectedStore, StoreDefinition, StructureRef};

static CONTAINER: OnceLock<(Box<dyn Any + Send + Sync>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        // Start on a dedicated OS thread: the blocking testcontainers runner drives its
        // own runtime and must not be entered from inside a tokio worker.
        std::thread::spawn(crate::start_db)
            .join()
            .expect("container bootstrap thread")
    });
    *port
}

fn admin_url() -> String {
    // The container images run root with an empty password and ship a `mysql` system db.
    format!("mysql://root@127.0.0.1:{}/mysql", container_port())
}

fn db_url(db: &str) -> String {
    format!("mysql://root@127.0.0.1:{}/{db}", container_port())
}

/// The cutover DDL a Rust-era module ships on MySQL/MariaDB: rows key on
/// `(store_name, store_key)` within the declared connection. `store_value` is `LONGTEXT`
/// (the engine binds JSON as a plain string); `ascii` charset keeps the composite PK inside
/// InnoDB's index byte limit, per the shipped `datastore/mysql.sql` precedent.
pub const CUTOVER_DDL: &str = "CREATE TABLE IF NOT EXISTS data_store (\
  store_name  VARCHAR(128) NOT NULL, \
  store_key   VARCHAR(512) NOT NULL, \
  store_value LONGTEXT     NOT NULL, \
  rev         BIGINT       NOT NULL DEFAULT 1, \
  updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP, \
  PRIMARY KEY (store_name, store_key) \
) CHARACTER SET ascii;";

/// Creates a fresh, empty database on the suite container and returns its name.
pub async fn fresh_db() -> String {
    let admin = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url())
        .await
        .expect("admin connect");
    let db = format!("sutra_ds_test_{}", DB_SEQ.fetch_add(1, Ordering::Relaxed));
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create test database");
    admin.close().await;
    db
}

/// A store named `name` on database `db`, carrying the cutover DDL as its first-use
/// migration (idempotent — a second store on the same database re-runs it as a no-op).
pub fn store(db: &str, name: &str) -> MysqlDataStore {
    let mut properties = BTreeMap::new();
    properties.insert("sql.url".to_string(), db_url(db));
    let def = StoreDefinition {
        name: name.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    };
    MysqlDataStore::from_definition(&def, vec![CUTOVER_DDL.to_string()]).expect("store builds")
}

// ---- the projected (typed-column) shape ------------------------------------

/// The declared structure a projected store's columns come from — one field of each
/// marshalling class, the last optional, with the facets the advisory type mapping (design
/// §4.4) turns into `VARCHAR(35)` / `DECIMAL(18,2)`.
pub const ACCOUNT_XSD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
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

/// The DDL the author ships for that structure on MySQL/MariaDB — the engine generates none
/// (ruling 5). `TINYINT(1)` is the §4.4 truth column; `store_key`/`rev`/`updated_at` are the
/// engine's control columns. There is NO `store_name`: the table IS the store.
pub const PROJECTED_DDL: &str = "CREATE TABLE IF NOT EXISTS accounts ( \
  store_key  VARCHAR(191)  NOT NULL, \
  account_id VARCHAR(35)   NOT NULL, \
  balance    DECIMAL(18,2) NOT NULL, \
  seq_no     INT           NOT NULL, \
  frozen     TINYINT(1)    NOT NULL, \
  opened_at  DATE          NULL, \
  rev        BIGINT        NOT NULL DEFAULT 1, \
  updated_at DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP, \
  PRIMARY KEY (store_key) \
) CHARACTER SET ascii;";

/// A table that DRIFTED from the package's migrations: `balance` dropped and the optional
/// `opened_at` made mandatory by a hand-applied ALTER. Lint cannot see this — only the live
/// catalog can.
pub const DRIFTED_DDL: &str = "CREATE TABLE IF NOT EXISTS drifted ( \
  store_key  VARCHAR(191) NOT NULL, \
  account_id VARCHAR(35)  NOT NULL, \
  seq_no     INT          NOT NULL, \
  frozen     TINYINT(1)   NOT NULL, \
  opened_at  DATE         NOT NULL, \
  rev        BIGINT       NOT NULL DEFAULT 1, \
  updated_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP, \
  PRIMARY KEY (store_key) \
) CHARACTER SET ascii;";

/// Derive the projection exactly as the engine does at plan time: compile the package's own
/// XSD, enumerate the declared type's children, project them.
pub fn account_projection(store: &str, table: &str) -> ProjectedStore {
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

/// A projected store named `name` over table `table` on database `db`, carrying `ddl` as its
/// first-use migration.
pub fn projected_store(db: &str, name: &str, table: &str, ddl: &str) -> MysqlDataStore {
    let mut properties = BTreeMap::new();
    properties.insert("sql.url".to_string(), db_url(db));
    let def = StoreDefinition {
        name: name.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    };
    MysqlDataStore::from_definition_projected(
        &def,
        vec![ddl.to_string()],
        None,
        Some(account_projection(name, table)),
    )
    .expect("store builds")
}

/// The record every fidelity assertion is written against. Parsed from TEXT so the decimal
/// keeps its WRITTEN scale ("100.50", not 100.5) — the property §4.5 requires survive the
/// round trip through a real `DECIMAL(18,2)`.
pub fn account_record() -> serde_json::Value {
    serde_json::from_str(r#"{"accountId":"ACC-000123","balance":100.50,"seqNo":7,"frozen":false}"#)
        .unwrap()
}

/// A COVERAGE store on database `db`, declared exactly as an author would (one `sql.url`) — and
/// carrying no migrations of its own, because the engine ships the coverage schema and applies it
/// to this connection on first use.
pub fn coverage_store(db: &str) -> sutra_datastore::CoverageStore {
    let mut properties = BTreeMap::new();
    properties.insert("sql.url".to_string(), db_url(db));
    let def = StoreDefinition {
        name: sutra_datastore::COVERAGE_STORE_NAME.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    };
    sutra_datastore::CoverageStore::from_definition(&def).expect("coverage store builds")
}
