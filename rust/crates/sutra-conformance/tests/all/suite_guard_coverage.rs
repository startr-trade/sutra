//! Every test in a docker suite holds its suite. A test without the guard would never count out,
//! so the suite's network would stay until the process exits: nothing fails, but a binary this
//! size runs out of docker address pools again. This check is what keeps that visible.

use std::path::Path;

#[test]
fn every_test_in_a_docker_suite_holds_its_suite() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/all");
    let mut suites = 0;
    for entry in std::fs::read_dir(&dir).expect("tests/all") {
        let path = entry.expect("a directory entry").path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        if !name.starts_with("tc_") || !name.ends_with(".rs") {
            continue;
        }
        suites += 1;
        let source = std::fs::read_to_string(&path).expect("a suite's source");
        let tests = source
            .lines()
            .filter(|line| {
                let line = line.trim_start();
                line.starts_with("#[test]") || line.starts_with("#[tokio::test")
            })
            .count();
        let holds = source.matches("hold_suite!(").count();
        assert_eq!(holds, tests, "{name}: each test needs one hold_suite! line");
    }
    assert!(suites > 10, "the suites were found: {suites}");
}
