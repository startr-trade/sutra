//! The N-lane rerun seam for this IT binary (execution scale-out §8, Phase 2).
//!
//! Every `serve()`-booting test in this binary takes its `engine_shards` from here.
//! With the env unset this is EXACTLY `EngineShardConfig::default()` — one lane,
//! unbounded queue, byte-identical to before the seam existed. Setting
//! `SUTRA_ENGINE_SHARDS=4` (and optionally `SUTRA_ENGINE_SHARD_QUEUE_CAPACITY`)
//! re-runs the SAME suites, unchanged expectations, on a four-lane router:
//!
//! ```text
//! SUTRA_ENGINE_SHARDS=4 cargo test -p sutra-engine --test all -- --ignored --skip k8s_
//! ```
//!
//! That is the Phase-2 tier-2 N=4 lane: the acceptance bar is the N=1 suites passing
//! VERBATIM at N=4 (the false-confidence trap §8 names is N=1-only greens); the
//! cross-shard-window ITs live in `sutra-channels/tests/all/shard_scale_out_test.rs`
//! and `shard_scale_out_it.rs` here.
#![allow(dead_code)]

pub fn engine_shards_from_env() -> sutra_engine::EngineShardConfig {
    let defaults = sutra_engine::EngineShardConfig::default();
    sutra_engine::EngineShardConfig {
        shards: std::env::var("SUTRA_ENGINE_SHARDS")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(defaults.shards),
        queue_capacity: std::env::var("SUTRA_ENGINE_SHARD_QUEUE_CAPACITY")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .or(defaults.queue_capacity),
    }
}
