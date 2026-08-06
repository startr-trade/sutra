//! Consolidated integration-test binary for sutra-channels (one link unit; modules preserve the original file names as filter paths).

use sutra_formats as _;

#[path = "all/support/mod.rs"]
mod support;

#[path = "all/async_lane_loop_test.rs"]
mod async_lane_loop_test;
#[path = "all/channel_call_retry_dispatch_test.rs"]
mod channel_call_retry_dispatch_test;
#[path = "all/config_test.rs"]
mod config_test;
#[path = "all/deferred_ack_test.rs"]
mod deferred_ack_test;
#[path = "all/dispatcher_policy_test.rs"]
mod dispatcher_policy_test;
#[path = "all/http_it_test.rs"]
mod http_it_test;
#[path = "all/intake_test.rs"]
mod intake_test;
#[path = "all/relay_resume_test.rs"]
mod relay_resume_test;
#[path = "all/reply_encode_test.rs"]
mod reply_encode_test;
#[path = "all/retry_redrive_test.rs"]
mod retry_redrive_test;
#[path = "all/shard_router_pin_test.rs"]
mod shard_router_pin_test;
#[path = "all/shard_scale_out_test.rs"]
mod shard_scale_out_test;
