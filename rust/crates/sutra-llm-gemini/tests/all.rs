//! The native Gemini adapter against the REAL Gemini API. Every test here is
//! `#[ignore = "live"]`: it needs `GEMINI_API_KEY` and network access, costs a fraction of a cent,
//! and runs only when asked for (`examples/ai-advanced/live-check.sh` runs it):
//!
//!   GEMINI_API_KEY=... cargo test -p sutra-llm-gemini --test all -- --ignored --nocapture live_

#[path = "all/live_api.rs"]
mod live_api;
