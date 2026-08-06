//! Consolidated integration-test binary for sutra-dmn (one link unit; modules preserve the original file names as filter paths).

#[path = "all/dmn_decision_engine.rs"]
mod dmn_decision_engine;
#[path = "all/dmn_external_functions.rs"]
mod dmn_external_functions;
#[path = "all/dmn_full_hit_policy.rs"]
mod dmn_full_hit_policy;
#[path = "all/dmn_validator.rs"]
mod dmn_validator;
#[path = "all/drg_invocable_and_bkm_chain.rs"]
mod drg_invocable_and_bkm_chain;
