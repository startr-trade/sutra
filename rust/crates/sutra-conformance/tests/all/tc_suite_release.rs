//! A suite's network goes when its last test ends, panicking or not, along with whatever is still
//! on it, and not a moment before. Against real docker.

use std::process::Command;

use crate::support::engine;
use crate::support::util;

const SUITE: &str = "netrelease";

fn exists(kind: &str, name: &str) -> bool {
    Command::new("docker")
        .args([kind, "inspect", name])
        .output()
        .expect("docker inspect")
        .status
        .success()
}

#[test]
#[ignore = "docker"]
fn tc_suite_release_the_last_test_takes_the_network_and_all_on_it_even_by_panic() {
    let _suite = sutra_testkit::hold_suite!();
    let running = engine::start_postgres(SUITE);
    let stopped = engine::start_postgres(SUITE);
    let network = util::network_for(SUITE);
    let stopped_id = stopped.container.id().to_string();
    let running_id = running.container.id().to_string();
    // The handles' own cleanup would race the release under test.
    std::mem::forget(running);
    std::mem::forget(stopped);
    let stop = Command::new("docker")
        .args(["stop", &stopped_id])
        .output()
        .expect("docker stop");
    assert!(stop.status.success());

    let first = sutra_testkit::conformance::suite::hold_counted(SUITE, Some(2), &[SUITE]);
    let second = sutra_testkit::conformance::suite::hold_counted(SUITE, Some(2), &[SUITE]);

    drop(first);
    assert!(exists("network", network), "one test is still running");
    assert!(exists("container", &running_id), "its fixtures stay too");

    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _second = second;
        panic!("the last test fails");
    }));
    assert!(failed.is_err());
    assert!(!exists("network", network), "the suite ended: {network}");
    assert!(
        !exists("container", &running_id),
        "the running fixture went"
    );
    assert!(
        !exists("container", &stopped_id),
        "the stopped fixture went"
    );
}
