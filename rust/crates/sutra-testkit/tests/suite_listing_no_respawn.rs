//! A binary that is not libtest cannot make the suite guard respawn. This file is such a binary
//! (`harness = false`), which is why it is its own test target and not a module of a `tests/all.rs`:
//! its probe, like a doc-test, ignores `--list` and runs its code again. The top-level process does
//! answer `--list`, so a runner that lists first, such as cargo-nextest, can find the test.
//!
//! The check cannot itself run away. Every process appends a line to a counter file before doing
//! anything else, and past [`PROCESS_LIMIT`] lines it exits at once, without taking a guard.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::{exit, Command};

const COUNTER_ENV: &str = "SUTRA_TESTKIT_RESPAWN_COUNTER";
const PROCESS_LIMIT: usize = 5;

fn main() {
    match std::env::var_os(COUNTER_ENV) {
        None if listing_requested() => list(),
        None => check(),
        Some(counter) => probe(PathBuf::from(counter)),
    }
}

/// A test runner that lists before it runs, such as cargo-nextest, asks with `--list`: answer the
/// way libtest's terse listing does, naming the one test, and none when only ignored tests are
/// wanted.
fn listing_requested() -> bool {
    std::env::args().skip(1).any(|arg| arg == "--list")
}

fn list() {
    if !std::env::args().skip(1).any(|arg| arg == "--ignored") {
        println!("suite_listing_no_respawn: test");
    }
}

/// The test: run a probe, and read how many processes it became.
fn check() {
    let counter = std::env::temp_dir().join(format!("sutra-respawn-{}", std::process::id()));
    let _ = std::fs::remove_file(&counter);
    let status = Command::new(std::env::current_exe().expect("own path"))
        .env(COUNTER_ENV, &counter)
        .status()
        .expect("run the probe");
    let processes = std::fs::read_to_string(&counter)
        .expect("the probe counted itself")
        .lines()
        .count();
    let _ = std::fs::remove_file(&counter);
    assert!(status.success(), "the probe: {status}");
    assert_eq!(
        processes, 2,
        "the probe and its one marked listing child, and nothing more"
    );
    println!("suite_listing_no_respawn: ok, {processes} processes");
}

/// What a doc-test using the guard would do, every time it is run.
fn probe(counter: PathBuf) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&counter)
        .expect("the counter file");
    writeln!(file, "{}", std::process::id()).expect("count this process");
    drop(file);
    let seen = std::fs::read_to_string(&counter)
        .expect("read the counter")
        .lines()
        .count();
    if seen > PROCESS_LIMIT {
        exit(3);
    }
    let guard = sutra_testkit::hold_suite!();
    if guard.expected_tests().is_some() {
        // Not libtest, so nothing may have been taken for a listing.
        exit(4);
    }
}
