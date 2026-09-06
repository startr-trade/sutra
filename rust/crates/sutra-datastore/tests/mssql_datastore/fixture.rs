//! SQL Server fixture: one `mcr.microsoft.com/mssql/server:2022-latest` container per test
//! binary (reused), a uniquely-named database per test (created through a raw tiberius
//! connection — the datastore crate has no CREATE DATABASE surface), and a
//! [`MssqlDataStore`] built against it on the `(store_name, store_key)` cutover table shape.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use sutra_datastore::{MssqlDataStore, ProjectedStore, StoreDefinition, StructureRef};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ImageExt};
use testcontainers_modules::mssql_server::MssqlServer;
use tiberius::{AuthMethod, Config};
use tokio_util::compat::TokioAsyncWriteCompatExt;

static CONTAINER: OnceLock<(Container<MssqlServer>, u16)> = OnceLock::new();
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

fn container_port() -> u16 {
    let (_, port) = CONTAINER.get_or_init(|| {
        // Start on a dedicated OS thread: the blocking testcontainers runner drives its
        // own runtime and must not be entered from inside a tokio worker.
        std::thread::spawn(|| {
            let container = MssqlServer::default()
                .with_accept_eula()
                .with_tag("2022-latest")
                .start()
                .expect("start mssql/server:2022-latest (docker required, ~2 GB)");
            sutra_testkit::reap_on_exit(container.id());
            let port = container.get_host_port_ipv4(1433).expect("mapped 1433");
            (container, port)
        })
        .join()
        .expect("container bootstrap thread")
    });
    *port
}

/// The cutover DDL a Rust-era module ships on SQL Server: `VARCHAR` keys with a
/// `NONCLUSTERED` primary key keep the composite key inside SQL Server's index byte limit;
/// `store_value` is `VARCHAR(MAX)`. The `IF OBJECT_ID` guard makes the CREATE idempotent.
pub const CUTOVER_DDL: &str = "IF OBJECT_ID('data_store', 'U') IS NULL \
  CREATE TABLE data_store ( \
    store_name  VARCHAR(128) NOT NULL, \
    store_key   VARCHAR(512) NOT NULL, \
    store_value VARCHAR(MAX) NOT NULL, \
    rev         BIGINT       NOT NULL DEFAULT 1, \
    updated_at  DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(), \
    CONSTRAINT pk_data_store PRIMARY KEY NONCLUSTERED (store_name, store_key) \
  );";

fn base_config(database: &str) -> Config {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(container_port());
    config.database(database);
    config.authentication(AuthMethod::sql_server(
        "sa",
        MssqlServer::DEFAULT_SA_PASSWORD,
    ));
    config.trust_cert();
    config
}

/// Creates a fresh, empty database on the suite container and returns its name.
pub async fn fresh_db() -> String {
    let db = format!("sutra_ds_test_{}", DB_SEQ.fetch_add(1, Ordering::Relaxed));
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", container_port()))
        .await
        .expect("tcp connect");
    tcp.set_nodelay(true).unwrap();
    let mut client = tiberius::Client::connect(base_config("master"), tcp.compat_write())
        .await
        .expect("master connect");
    client
        .simple_query(format!("CREATE DATABASE [{db}]"))
        .await
        .expect("create test database")
        .into_results()
        .await
        .expect("drain create-database result");
    db
}

/// A store named `name` on database `db`, carrying the cutover DDL as its first-use
/// migration. The URL is the native `sqlserver://` form a module developer declares; credentials
/// come from the separate secret-ref properties, as in the money-transfer `datastores.yaml`.
pub fn store(db: &str, name: &str) -> MssqlDataStore {
    let mut properties = BTreeMap::new();
    properties.insert(
        "sql.url".to_string(),
        format!(
            "sqlserver://127.0.0.1:{};databaseName={db};trustServerCertificate=true",
            container_port()
        ),
    );
    properties.insert("sql.username".to_string(), "sa".to_string());
    properties.insert(
        "sql.password".to_string(),
        MssqlServer::DEFAULT_SA_PASSWORD.to_string(),
    );
    let def = StoreDefinition {
        name: name.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    };
    MssqlDataStore::from_definition(&def, vec![CUTOVER_DDL.to_string()]).expect("store builds")
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

/// The DDL the author ships for that structure on SQL Server — the engine generates none
/// (ruling 5). `BIT` is the §4.4 truth column; `store_key`/`rev`/`updated_at` are the engine's
/// control columns. There is NO `store_name`: the table IS the store.
pub const PROJECTED_DDL: &str = "IF OBJECT_ID('accounts', 'U') IS NULL \
  CREATE TABLE accounts ( \
    store_key  VARCHAR(512)  NOT NULL, \
    account_id VARCHAR(35)   NOT NULL, \
    balance    DECIMAL(18,2) NOT NULL, \
    seq_no     INT           NOT NULL, \
    frozen     BIT           NOT NULL, \
    opened_at  DATE          NULL, \
    rev        BIGINT        NOT NULL DEFAULT 1, \
    updated_at DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(), \
    CONSTRAINT pk_accounts PRIMARY KEY NONCLUSTERED (store_key) \
  );";

/// A table that DRIFTED from the package's migrations: `balance` dropped and the optional
/// `opened_at` made mandatory by a hand-applied ALTER. Lint cannot see this — only the live
/// catalog can.
pub const DRIFTED_DDL: &str = "IF OBJECT_ID('drifted', 'U') IS NULL \
  CREATE TABLE drifted ( \
    store_key  VARCHAR(512) NOT NULL, \
    account_id VARCHAR(35)  NOT NULL, \
    seq_no     INT          NOT NULL, \
    frozen     BIT          NOT NULL, \
    opened_at  DATE         NOT NULL, \
    rev        BIGINT       NOT NULL DEFAULT 1, \
    updated_at DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(), \
    CONSTRAINT pk_drifted PRIMARY KEY NONCLUSTERED (store_key) \
  );";

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
pub fn projected_store(db: &str, name: &str, table: &str, ddl: &str) -> MssqlDataStore {
    let mut properties = BTreeMap::new();
    properties.insert(
        "sql.url".to_string(),
        format!(
            "sqlserver://127.0.0.1:{};databaseName={db};trustServerCertificate=true",
            container_port()
        ),
    );
    properties.insert("sql.username".to_string(), "sa".to_string());
    properties.insert(
        "sql.password".to_string(),
        MssqlServer::DEFAULT_SA_PASSWORD.to_string(),
    );
    let def = StoreDefinition {
        name: name.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    };
    MssqlDataStore::from_definition_projected(
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

/// A COVERAGE store on database `db`, declared exactly as an author would — and carrying no
/// migrations of its own, because the engine ships the coverage schema and applies it to this
/// connection on first use.
pub fn coverage_store(db: &str) -> sutra_datastore::CoverageStore {
    let mut properties = BTreeMap::new();
    properties.insert(
        "sql.url".to_string(),
        format!(
            "sqlserver://127.0.0.1:{};databaseName={db};trustServerCertificate=true",
            container_port()
        ),
    );
    properties.insert("sql.username".to_string(), "sa".to_string());
    properties.insert(
        "sql.password".to_string(),
        MssqlServer::DEFAULT_SA_PASSWORD.to_string(),
    );
    let def = StoreDefinition {
        name: sutra_datastore::COVERAGE_STORE_NAME.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    };
    sutra_datastore::CoverageStore::from_definition(&def).expect("coverage store builds")
}
