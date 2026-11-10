//! Conformance: compliance coverage, the whole loop on a real engine and a real database.
//!
//! A database that predates cycles comes first, with rows in it. Then a package carrying a
//! requirements map is sealed and booted, cycles are declared and activated through the CLI
//! against the same database a host-side CI client would reach, real requests walk the declared
//! paths, and each step reads the outcome back through `sutra coverage check`.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::Value;
use sutra_cli::commands::coverage::{self, CheckArgs, CoverageAction, CoverageArgs, ResetArgs};
use sutra_cli::commands::coverage_cycle::{CycleAction, CycleArgs, CycleLabelArgs, CycleStoreArgs};
use sutra_cli::exit;
use sutra_cli::output::Io;
use sutra_cli::GlobalArgs;

use crate::support::engine::{self, EngineBuilder, PgFixture};
use crate::support::util;

const API_KEY: &str = "transfer-demo-key";

const REQUIREMENTS_MAP: &str = r#"
routes:
  accept:
    requirements: [REQ-HAPPY]
    weight: 3
    cases: [case-accept]
  reject:
    requirements: [REQ-REFUSAL]
    cases: [case-reject, case-reject-frozen]
"#;

/// The DDL of a coverage store before cycles existed, byte for byte what shipped.
const V901: &str =
    include_str!("../../../sutra-datastore/migrations/coverage/postgres/V901__coverage_metric.sql");
const V902: &str = include_str!(
    "../../../sutra-datastore/migrations/coverage/postgres/V902__coverage_fragment.sql"
);

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// money-transfer, plus the requirements map, plus a declared idempotency header on the intake so
/// that a client can state which run a request belongs to.
fn sealed_package() -> (PathBuf, PathBuf, String) {
    let scratch = util::world_readable_temp_dir("sutra-cycle-pkg");
    let package = scratch.join("default--money-transfer--1.0.0");
    copy_tree(
        &util::examples_dir().join("money-transfer/deployments-src/default--money-transfer--1.0.0"),
        &package,
    );
    std::fs::create_dir_all(package.join("coverage")).unwrap();
    std::fs::write(
        package
            .join("coverage")
            .join(sutra_loader::REQUIREMENTS_MAP_FILE),
        REQUIREMENTS_MAP,
    )
    .unwrap();
    let channels = std::fs::read_to_string(package.join("channels.yaml")).unwrap();
    let bind = "    bind: \"POST /channels/transfer-request\"\n";
    assert!(channels.contains(bind));
    std::fs::write(
        package.join("channels.yaml"),
        channels.replace(
            bind,
            &format!("{bind}    idempotency-key-header: X-Run-Key\n"),
        ),
    )
    .unwrap();

    let out = util::world_readable_temp_dir("sutra-cycle-archives");
    let outcome = sutra_loader::package::assemble_dir(
        &package,
        &out,
        &sutra_loader::PackageOptions::default(),
    )
    .expect("the package seals");
    util::make_world_readable(&out);
    let archive = outcome.archives[0].file_path.clone();
    let deployment = outcome.archives[0].id.value().to_string();
    (out, archive, deployment)
}

fn psql(pg: &PgFixture, sql: &str) -> String {
    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            &pg.container_name,
            "psql",
            "-U",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-tA",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("docker exec psql");
    use std::io::Write;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(sql.as_bytes())
        .unwrap();
    let done = child.wait_with_output().unwrap();
    assert!(done.status.success(), "psql failed on: {sql}");
    String::from_utf8(done.stdout).unwrap().trim().to_string()
}

/// A host-side CI client: the CLI against the store the engine writes.
struct Client {
    archive: PathBuf,
    url: String,
}

impl Client {
    fn store_args(&self) -> CycleStoreArgs {
        CycleStoreArgs {
            archive: self.archive.clone(),
            database_url: Some(self.url.clone()),
            db_user: Some("postgres".to_string()),
            db_password: Some("postgres".to_string()),
        }
    }

    fn run(&self, action: CoverageAction) -> (i32, Value) {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut input = std::io::Cursor::new(Vec::new());
        let code = {
            let mut io = Io {
                out: &mut out,
                err: &mut err,
                input: &mut input,
            };
            let global = GlobalArgs {
                format: Some("json".to_string()),
                ..GlobalArgs::default()
            };
            coverage::execute(CoverageArgs { action }, &global, &mut io)
        };
        let stdout = String::from_utf8(out).unwrap();
        let value = serde_json::from_str(stdout.trim()).unwrap_or_else(
            |_| serde_json::json!({ "stdout": stdout, "stderr": String::from_utf8_lossy(&err) }),
        );
        (code, value)
    }

    fn check(&self, cycle: Option<&str>, threshold: f64) -> (i32, Value) {
        self.run(CoverageAction::Check(CheckArgs {
            bpmn_file: None,
            process: None,
            max_paths: 256,
            archive: Some(self.archive.clone()),
            database_url: Some(self.url.clone()),
            db_user: Some("postgres".to_string()),
            db_password: Some("postgres".to_string()),
            threshold,
            cycle: cycle.map(str::to_string),
        }))
    }

    fn reset(&self, cycle: Option<&str>) -> (i32, Value) {
        self.run(CoverageAction::Reset(ResetArgs {
            archive: self.archive.clone(),
            database_url: Some(self.url.clone()),
            db_user: Some("postgres".to_string()),
            db_password: Some("postgres".to_string()),
            cycle: cycle.map(str::to_string),
        }))
    }

    fn cycle(&self, action: CycleAction) -> (i32, Value) {
        self.run(CoverageAction::Cycle(CycleArgs { action }))
    }

    fn label(&self, label: &str) -> CycleLabelArgs {
        CycleLabelArgs {
            label: label.to_string(),
            store: self.store_args(),
        }
    }

    fn declare(&self, label: &str) -> (i32, Value) {
        self.cycle(CycleAction::Declare(self.label(label)))
    }

    fn activate(&self, label: &str) -> (i32, Value) {
        self.cycle(CycleAction::Activate(self.label(label)))
    }

    fn deactivate(&self, label: &str) -> (i32, Value) {
        self.cycle(CycleAction::Deactivate(self.label(label)))
    }

    fn list(&self) -> Value {
        let (code, value) = self.cycle(CycleAction::List(self.store_args()));
        assert_eq!(code, exit::OK, "{value}");
        value
    }
}

/// Real requests, one at a time.
struct Traffic {
    runtime: tokio::runtime::Runtime,
    client: reqwest::Client,
    port: u16,
}

impl Traffic {
    fn post(&self, path: &str, body: &str, run_key: Option<&str>) -> (u16, String) {
        self.runtime.block_on(async {
            let mut request = self
                .client
                .post(format!("http://127.0.0.1:{}{path}", self.port))
                .header("Content-Type", "application/json")
                .header("X-Api-Key", API_KEY)
                .body(body.to_string());
            if let Some(key) = run_key {
                request = request.header("X-Run-Key", key);
            }
            let response = request.send().await.expect("engine request");
            let status = response.status().as_u16();
            (status, response.text().await.unwrap_or_default())
        })
    }

    fn transfer(&self, amount: u32, run_key: Option<&str>) -> (u16, String) {
        self.post(
            "/channels/transfer-request",
            &format!(
                "{{\"TransferRequest\":{{\"fromId\":\"carol\",\"toId\":\"bob\",\"amount\":{amount}}}}}"
            ),
            run_key,
        )
    }

    /// Walks the declared `accept` path.
    fn walk_accept(&self) {
        let (status, body) = self.transfer(1, None);
        assert_eq!(status, 200, "{body}");
        assert!(body.contains("<TransferAccepted"), "{body}");
    }

    /// Walks the declared `reject` path.
    fn walk_reject(&self) {
        let (_, body) = self.transfer(100_000, None);
        assert!(body.contains("<TransferRejected"), "{body}");
    }

    fn balance(&self, account: &str) -> String {
        let (status, body) = self.post(
            "/channels/balance",
            &format!("{{\"BalanceQuery\":{{\"accountId\":\"{account}\"}}}}"),
            None,
        );
        assert_eq!(status, 200, "{body}");
        let start = body.find("balance=\"").expect("balance attribute") + 9;
        body[start..][..body[start..].find('"').unwrap()].to_string()
    }

    fn coverage_query(&self) -> String {
        let (status, body) = self.post(
            "/channels/coverage-query",
            "{\"CoverageQuery\":{\"process\":\"transfer\"}}",
            None,
        );
        assert_eq!(status, 200, "{body}");
        body
    }
}

fn plain_percent(report: &Value) -> f64 {
    report["coveragePercentage"].as_f64().unwrap()
}

fn weighted_percent(report: &Value) -> f64 {
    report["compliance"]["weightedPercentage"].as_f64().unwrap()
}

fn requirement_percent(report: &Value, id: &str) -> f64 {
    report["compliance"]["requirements"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["requirement"] == id)
        .unwrap_or_else(|| panic!("no {id}: {report}"))["coveragePercentage"]
        .as_f64()
        .unwrap()
}

fn cycle_percent(list: &Value, label: &str) -> f64 {
    list["cycles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["cycle"] == label)
        .unwrap_or_else(|| panic!("no {label}: {list}"))["coveragePercentage"]
        .as_f64()
        .unwrap()
}

#[test]
#[ignore = "docker"]
fn tc_coverage_cycles_requirements_map_and_upgrade() {
    let _suite = sutra_testkit::hold_suite!("covcycles");
    let (archives, archive, deployment) = sealed_package();

    // ---- a database from before cycles, with coverage in it --------------------------------
    let pg = engine::start_postgres("cycles");
    let accounts = engine::start_postgres("cycles-accounts");
    psql(
        &accounts,
        &format!(
            "{V901}\n{V902}\nINSERT INTO coverage_metric (deployment_id, path_urn, covered) VALUES \
             ('{deployment}', 'accept', true), ('{deployment}', 'reject', false);"
        ),
    );
    assert_eq!(
        psql(
            &accounts,
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'coverage_cycle';"
        ),
        "0",
        "the database really does predate cycles"
    );

    let engine = EngineBuilder::new("cycles", &pg)
        .env(
            "ACCOUNTS_DB_URL",
            format!("postgresql://{}:5432/postgres", accounts.container_name),
        )
        .env("ACCOUNTS_DB_USER", "postgres")
        .env("ACCOUNTS_DB_PASSWORD", "postgres")
        .env("RABBITMQ_USERNAME", "mtransfer")
        .env("RABBITMQ_PASSWORD", "mtransfer-broker-pw")
        .expected_deployments(1)
        .start(&archives);
    let traffic = Traffic {
        runtime: tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
        client: reqwest::Client::new(),
        port: engine.http_port,
    };
    let client = Client {
        archive,
        url: format!("postgresql://127.0.0.1:{}/postgres", accounts.host_port),
    };

    // The upgrade: the boot added the cycle tables, and the old rows read as "no cycle".
    assert_eq!(
        psql(
            &accounts,
            "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'coverage_cycle%' \
             OR table_name = 'coverage_active_cycle';"
        ),
        "4"
    );
    let (code, report) = client.check(None, 100.0);
    assert_eq!(code, exit::FINDINGS, "{report}");
    assert_eq!(report["total"], 2, "{report}");
    assert_eq!(report["covered"], 1, "{report}");
    assert_eq!(report["uncovered"], serde_json::json!(["reject"]));
    assert_eq!(plain_percent(&report), 50.0);
    assert_eq!(weighted_percent(&report), 75.0, "{report}");
    assert!(report.get("cycle").is_none(), "{report}");
    assert_eq!(client.reset(None).0, exit::OK);

    // ---- declare and activate --------------------------------------------------------------
    let (code, refused) = client.activate("rel-1");
    assert_eq!(code, exit::FINDINGS, "{refused}");
    assert_eq!(refused["outcome"], "undeclared");
    assert_eq!(client.check(Some("rel-1"), 0.0).0, exit::USAGE);
    let (code, declared) = client.declare("rel-1");
    assert_eq!(code, exit::OK, "{declared}");
    assert_eq!(declared["total"], 2);
    assert_eq!(client.activate("rel-1").0, exit::OK);

    let (_, report) = client.check(Some("rel-1"), 0.0);
    assert_eq!(report["cycle"], "rel-1");
    assert_eq!(report["activeCycle"], "rel-1");
    assert_eq!(
        (report["total"].as_u64(), report["covered"].as_u64()),
        (Some(2), Some(0))
    );

    // ---- a real walk inside the cycle: per-requirement and weighted figures ----------------
    traffic.walk_accept();
    let (code, report) = client.check(Some("rel-1"), 100.0);
    assert_eq!(code, exit::FINDINGS, "below 100 fails closed: {report}");
    assert_eq!(plain_percent(&report), 50.0);
    assert_eq!(weighted_percent(&report), 75.0, "{report}");
    assert_eq!(requirement_percent(&report, "REQ-HAPPY"), 100.0);
    assert_eq!(requirement_percent(&report, "REQ-REFUSAL"), 0.0);
    // The threshold gates the weighted figure, which clears 75 while the plain one does not.
    assert_eq!(client.check(Some("rel-1"), 75.0).0, exit::OK);

    // An uncovered path lists the cases that would cover it.
    let routes = report["compliance"]["uncoveredRoutes"].as_array().unwrap();
    assert_eq!(routes.len(), 1, "{report}");
    assert_eq!(routes[0]["path"], "reject");
    assert_eq!(
        routes[0]["cases"],
        serde_json::json!(["case-reject", "case-reject-frozen"])
    );
    assert_eq!(
        report["compliance"]["casesToRun"],
        serde_json::json!(["case-reject", "case-reject-frozen"])
    );

    // The walk went to the cycle and nowhere else, and the in-flow report reads the cycle too.
    let (_, plain) = client.check(None, 0.0);
    assert_eq!(plain["covered"], 0, "{plain}");
    assert_eq!(plain["activeCycle"], "rel-1");
    let reply = traffic.coverage_query();
    assert!(reply.contains("percentage=\"50.0\""), "{reply}");
    assert!(reply.contains("<covered>accept</covered>"), "{reply}");

    // ---- deactivate: coverage records outside the cycle ------------------------------------
    assert_eq!(client.deactivate("rel-1").0, exit::OK);
    traffic.walk_reject();
    let (_, plain) = client.check(None, 0.0);
    assert_eq!(plain["covered"], 1, "{plain}");
    assert_eq!(plain["uncovered"], serde_json::json!(["accept"]));
    assert!(plain.get("activeCycle").is_none(), "{plain}");
    let (_, report) = client.check(Some("rel-1"), 0.0);
    assert_eq!(weighted_percent(&report), 75.0, "rel-1 did not move");

    // ---- a second cycle: the first is refused while it runs --------------------------------
    assert_eq!(client.declare("rel-2").0, exit::OK);
    assert_eq!(client.activate("rel-2").0, exit::OK);
    let (code, refused) = client.activate("rel-1");
    assert_eq!(code, exit::FINDINGS, "{refused}");
    assert_eq!(refused["outcome"], "refused");
    assert_eq!(refused["activeCycle"], "rel-2");
    assert_eq!(
        refused["code"],
        "SUTRA.RUNTIME.COVERAGE.CYCLE_ALREADY_ACTIVE"
    );
    traffic.walk_accept();
    let list = client.list();
    assert_eq!(list["activeCycle"], "rel-2");
    assert_eq!(cycle_percent(&list, "rel-2"), 50.0);
    assert_eq!(cycle_percent(&list, "rel-1"), 50.0);
    assert_eq!(list["noCycle"]["covered"], 1, "{list}");

    // Deactivating the wrong cycle says so and leaves the holder running.
    let (code, wrong) = client.deactivate("rel-1");
    assert_eq!(code, exit::FINDINGS, "{wrong}");
    assert_eq!(wrong["activeCycle"], "rel-2");
    assert_eq!(client.list()["activeCycle"], "rel-2");
    assert_eq!(client.deactivate("rel-2").0, exit::OK);

    // ---- reactivating the earlier cycle continues it ---------------------------------------
    assert_eq!(client.activate("rel-1").0, exit::OK);
    let (_, report) = client.check(Some("rel-1"), 0.0);
    assert_eq!(weighted_percent(&report), 75.0, "its marks are still there");
    traffic.walk_reject();
    let (code, report) = client.check(Some("rel-1"), 100.0);
    assert_eq!(code, exit::OK, "{report}");
    assert_eq!(weighted_percent(&report), 100.0);
    assert_eq!(report["compliance"]["casesToRun"], serde_json::json!([]));
    assert_eq!(requirement_percent(&report, "REQ-REFUSAL"), 100.0);

    // ---- a re-run is not a duplicate when the client's key says which attempt it is --------
    let before = traffic.balance("carol");
    let (status, body) = traffic.transfer(1, Some("rel-1:case-accept:1"));
    assert_eq!(status, 200, "first attempt runs: {body}");
    let after_first = traffic.balance("carol");
    assert_ne!(before, after_first, "the first attempt moved money");
    let (status, _) = traffic.transfer(1, Some("rel-1:case-accept:1"));
    assert_eq!(status, 202, "the same key is a duplicate");
    assert_eq!(traffic.balance("carol"), after_first, "and did nothing");
    let (status, body) = traffic.transfer(1, Some("rel-1:case-accept:2"));
    assert_eq!(status, 200, "a deliberate re-run runs: {body}");
    assert_ne!(traffic.balance("carol"), after_first, "and did the work");

    // ---- per-cycle reporting, and a reset of one cycle alone -------------------------------
    let list = client.list();
    assert_eq!(cycle_percent(&list, "rel-1"), 100.0, "{list}");
    assert_eq!(cycle_percent(&list, "rel-2"), 50.0);
    assert_eq!(client.reset(Some("rel-2")).0, exit::OK);
    let list = client.list();
    assert_eq!(cycle_percent(&list, "rel-2"), 0.0, "{list}");
    assert_eq!(cycle_percent(&list, "rel-1"), 100.0);
    assert_eq!(list["noCycle"]["covered"], 1);
    assert_eq!(client.deactivate("rel-1").0, exit::OK);
}
