//! Consolidated integration-test binary for sutra-bpmn (one link unit; modules preserve the original file names as filter paths).

#[path = "all/bpmn_support_doc.rs"]
mod bpmn_support_doc;
#[path = "all/contract_and_selection_test.rs"]
mod contract_and_selection_test;
#[path = "all/coverage_path_test.rs"]
mod coverage_path_test;
#[path = "all/loader_test.rs"]
mod loader_test;
#[path = "all/q_extensions_test.rs"]
mod q_extensions_test;
#[path = "all/retry_policy_test.rs"]
mod retry_policy_test;
#[path = "all/timer_channel_call_test.rs"]
mod timer_channel_call_test;
