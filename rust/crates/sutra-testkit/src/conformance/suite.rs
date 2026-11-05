//! Release a suite's docker network when the suite ends, not when the test process exits.
//!
//! Suites keep their containers in `static`s, so nothing tied them to the suite's end: every
//! suite in a test binary held its network until the binary exited. Docker's default address pools
//! run out at about 30 networks, and they are shared with everything else on the machine, so a
//! binary with many suites could fail partway through with "all predefined address pools have been
//! fully subnetted".
//!
//! Each test holds a [`SuiteGuard`] for its whole body:
//!
//! ```text
//! let _suite = sutra_testkit::hold_suite!("timers");
//! ```
//!
//! The listing re-runs this process's own executable, so it must never be able to recurse. A binary
//! that is not libtest, a doc-test for one, ignores `--list` and runs its code again, which would
//! take a guard and list again, without end. (That is also why the example above is `text`:
//! `--ignored` runs an `ignore` example.) So the child carries [`LISTING_CHILD_ENV`], which its own
//! children inherit, and a process that has it never spawns. A child that does not answer like
//! libtest, or does not answer within [`LISTING_DEADLINE`], counts as no listing at all.
//!
//! The guard counts the module's tests out. How many to expect comes from the test binary itself:
//! it is listed once with this run's own arguments, so a filtered run expects only the tests it
//! will run. When the last one ends, by returning or by panicking, the suite's containers and
//! network go. Tests of one suite may run in parallel and tests of different suites overlap freely:
//! nothing is released until every test in the module has finished, and network names carry the
//! process id, so another test process is never touched.
//!
//! The count errs towards keeping the network, which the atexit reaper then removes as before. A
//! test that dies before taking its guard, a test with no guard, or a listing that failed all leave
//! the count short, and the suite simply lasts until the process exits. It can never release early:
//! the listing is the most tests that can finish.

use std::collections::HashMap;
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::util;

/// Set on the process that lists the tests. A process with it set never lists in its turn.
const LISTING_CHILD_ENV: &str = "SUTRA_TESTKIT_SUITE_LISTING_CHILD";

/// How long the listing may take. libtest answers in milliseconds.
const LISTING_DEADLINE: Duration = Duration::from_secs(30);

/// Hold this module's suite open for the current test. The arguments name the suites (see
/// [`util::suite_of`]) whose networks go when the module's last test ends.
#[macro_export]
macro_rules! hold_suite {
    ($($suite:expr),* $(,)?) => {
        $crate::conformance::suite::hold(module_path!(), &[$($suite),*])
    };
}

/// One test's hold on its suite. Dropping the last one releases the suite's networks. Take it on
/// the test's first line, so it drops after everything else the test holds, and on a panic too.
pub struct SuiteGuard {
    key: String,
    expected: Option<usize>,
    suites: Vec<String>,
}

impl SuiteGuard {
    /// How many tests this guard's module counts out. `None` when the run could not be listed,
    /// which leaves the suite to the atexit reaper.
    pub fn expected_tests(&self) -> Option<usize> {
        self.expected
    }
}

/// Take a hold for the test in `module_path` (what `module_path!()` gives there). Use
/// [`hold_suite!`](crate::hold_suite), which passes it.
pub fn hold(module_path: &str, suites: &[&str]) -> SuiteGuard {
    let key = module_key(module_path).to_string();
    let expected = run_listing().map(|listing| tests_in_module(listing, &key));
    hold_counted(&key, expected, suites)
}

/// [`hold`] with the number of tests given rather than listed, for testing the release itself.
pub fn hold_counted(key: &str, expected: Option<usize>, suites: &[&str]) -> SuiteGuard {
    SuiteGuard {
        key: key.to_string(),
        expected,
        suites: suites.iter().map(|suite| suite.to_string()).collect(),
    }
}

impl Drop for SuiteGuard {
    fn drop(&mut self) {
        if !count_finished(&self.key, self.expected) {
            return;
        }
        for suite in &self.suites {
            let Some(network) = util::known_network(suite) else {
                continue;
            };
            if !crate::release_network(network) {
                eprintln!("suite {suite}: could not remove network {network}");
            }
        }
    }
}

/// Count one finished test. `true` when it was the last the run expects.
fn count_finished(key: &str, expected: Option<usize>) -> bool {
    static FINISHED: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    let finished = FINISHED.get_or_init(|| Mutex::new(HashMap::new()));
    let mut finished = match finished.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    let count = finished.entry(key.to_string()).or_insert(0);
    *count += 1;
    Some(*count) == expected
}

/// The test-name prefix of a module: `module_path!()` without the binary's crate name.
fn module_key(module_path: &str) -> &str {
    module_path
        .split_once("::")
        .map_or(module_path, |(_, rest)| rest)
}

/// This process's tests, as libtest lists them with the arguments this run was given. Once per
/// process. `None` when the listing cannot be had, which leaves every suite to the atexit reaper.
fn run_listing() -> Option<&'static str> {
    static LISTING: OnceLock<Option<String>> = OnceLock::new();
    LISTING
        .get_or_init(|| {
            if std::env::var_os(LISTING_CHILD_ENV).is_some() {
                return None;
            }
            let mut listing = Command::new(std::env::current_exe().ok()?);
            listing
                .args(std::env::args_os().skip(1))
                .arg("--list")
                .env(LISTING_CHILD_ENV, "1");
            let output = output_within(listing, LISTING_DEADLINE)?;
            is_libtest_listing(&output).then_some(output)
        })
        .as_deref()
}

/// A command's stdout if it exits successfully before `deadline`. Otherwise `None`, and it is killed.
fn output_within(mut command: Command, deadline: Duration) -> Option<String> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?;
    // Read on a thread: a listing bigger than the pipe's buffer would otherwise never exit.
    let reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = stdout.read_to_end(&mut bytes);
        bytes
    });
    let started = Instant::now();
    let succeeded = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.success(),
            Ok(None) if started.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(20))
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    let bytes = reader.join().ok()?;
    succeeded.then(|| String::from_utf8_lossy(&bytes).into_owned())
}

/// Whether output is what libtest's `--list` prints: it always ends with a count line.
fn is_libtest_listing(output: &str) -> bool {
    output
        .lines()
        .rev()
        .find(|line| !line.is_empty())
        .is_some_and(|last| {
            let mut words = last.split_whitespace();
            words.next().is_some_and(|n| n.parse::<usize>().is_ok())
                && matches!(words.next(), Some("test," | "tests,"))
        })
}

/// How many tests in a libtest `--list` output belong to the module `key`.
fn tests_in_module(listing: &str, key: &str) -> usize {
    let prefix = format!("{key}::");
    listing
        .lines()
        .filter(|line| line.ends_with(": test") && line.starts_with(&prefix))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LISTING: &str = "\
tc_a::tc_one: test
tc_a::tc_two: test
tc_ab::tc_other: test
tc_a::bench_it: bench

3 tests, 1 benchmarks
";

    #[test]
    fn a_listing_counts_only_the_tests_of_that_module() {
        assert_eq!(tests_in_module(LISTING, "tc_a"), 2);
        assert_eq!(tests_in_module(LISTING, "tc_ab"), 1);
        assert_eq!(tests_in_module(LISTING, "tc_none"), 0);
    }

    #[test]
    fn only_libtest_output_counts_as_a_listing() {
        assert!(is_libtest_listing(LISTING));
        assert!(is_libtest_listing("0 tests, 0 benchmarks\n"));
        assert!(is_libtest_listing("a::b: test\n\n1 test, 0 benchmarks\n"));
        assert!(!is_libtest_listing(""));
        assert!(!is_libtest_listing("running 1 test\ntest x ... ok\n"));
    }

    /// This test binary is libtest, so the real listing works and names this very test.
    #[test]
    fn a_real_test_binary_lists_itself() {
        let listing = run_listing().expect("a libtest binary answers --list");
        assert!(listing.contains("a_real_test_binary_lists_itself: test"));
    }

    #[test]
    fn a_child_past_the_deadline_is_killed_and_counts_as_no_listing() {
        let mut slow = Command::new("sleep");
        slow.arg("30");
        let started = Instant::now();
        assert_eq!(output_within(slow, Duration::from_millis(100)), None);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn a_failing_child_counts_as_no_listing() {
        assert_eq!(
            output_within(Command::new("false"), Duration::from_secs(10)),
            None
        );
    }

    #[test]
    fn a_module_key_drops_the_binary_name() {
        assert_eq!(module_key("all::tc_a"), "tc_a");
        assert_eq!(module_key("all::tc_a::inner"), "tc_a::inner");
        assert_eq!(module_key("all"), "all");
    }

    #[test]
    fn only_the_last_expected_test_finishes_a_suite() {
        assert!(!count_finished("count-two", Some(2)));
        assert!(count_finished("count-two", Some(2)));
        assert!(!count_finished("count-two", Some(2)), "only once");
    }

    #[test]
    fn a_count_that_cannot_be_reached_never_finishes_a_suite() {
        assert!(!count_finished("count-none", None));
        assert!(!count_finished("count-zero", Some(0)));
    }

    /// The guard has no docker work to do for a suite that never asked for a network, so this
    /// runs a panicking test's unwinding through it without a daemon.
    #[test]
    fn a_panic_counts_a_test_as_finished() {
        let panicked = std::panic::catch_unwind(|| {
            let _suite = hold_counted("count-panic", Some(2), &["never-asked"]);
            panic!("the test failed");
        });
        assert!(panicked.is_err());
        assert!(count_finished("count-panic", Some(2)));
    }
}
