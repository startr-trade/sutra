//! The `llm` transport suite: config parsing, wire shapes, routing, reply delivery and
//! observability, against a fake provider and a capturing reply port.

#[path = "all/support.rs"]
mod support;

#[path = "all/config.rs"]
mod config;
#[path = "all/inline.rs"]
mod inline;
#[path = "all/jobs.rs"]
mod jobs;
#[path = "all/jobs_db.rs"]
mod jobs_db;
#[path = "all/observability.rs"]
mod observability;
#[path = "all/reply.rs"]
mod reply;
#[path = "all/request.rs"]
mod request;
#[path = "all/routing.rs"]
mod routing;
