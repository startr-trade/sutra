//! Consolidated integration-test binary for sutra-executor (one link unit; modules preserve the original file names as filter paths).

#[path = "all/common/mod.rs"]
mod common;

#[path = "all/channel_call_retry_test.rs"]
mod channel_call_retry_test;
#[path = "all/channel_call_timer_test.rs"]
mod channel_call_timer_test;
#[path = "all/continue_reply_test.rs"]
mod continue_reply_test;
#[path = "all/coverage_test.rs"]
mod coverage_test;
#[path = "all/data_tasks_test.rs"]
mod data_tasks_test;
#[path = "all/dispatch_and_params_test.rs"]
mod dispatch_and_params_test;
#[path = "all/emissions_test.rs"]
mod emissions_test;
#[path = "all/errors_and_scopes_test.rs"]
mod errors_and_scopes_test;
#[path = "all/gateways_test.rs"]
mod gateways_test;
#[path = "all/integration_examples_test.rs"]
mod integration_examples_test;
#[path = "all/retry_policy_test.rs"]
mod retry_policy_test;
#[path = "all/task_kinds_test.rs"]
mod task_kinds_test;
#[path = "all/time_skipping_retry_test.rs"]
mod time_skipping_retry_test;
#[path = "all/token_executor_test.rs"]
mod token_executor_test;
