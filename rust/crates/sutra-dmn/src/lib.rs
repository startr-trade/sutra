//! DMN 1.5 decision-table core.
//!
//! Scope: [`DmnFileLoader`] (quick-xml two-phase load incl. the `bpm:code` extension-attribute
//! walk), [`model`] (the engine-internal decision model), [`unary_test`] (DMN unary-test →
//! full-FEEL translation), [`DmnRulesetValidator`] (all seven OMG DMN 1.5 § 8.2.10 hit
//! policies, the validate/evaluate duality, the `feelContext` payload projection with the
//! `{body: …}` envelope rule, and the injectable evaluation [`Clock`]), and
//! [`codes`] (the stable `SUTRA.VALIDATE.DMN.*` code strings).
//!
//! Deployment machinery (registry file watching, dependency-injection wiring, the
//! named-validator adapter) is out of scope for this crate — it is contract-dependent.
#![forbid(unsafe_code)]

pub mod clock;
pub mod codes;
pub mod drg;
pub mod engine;
pub mod error;
pub mod issue;
pub mod loader;
pub mod model;
pub mod unary_test;
pub mod validator;
mod xml;

pub use clock::{Clock, FixedClock, SystemClock};
pub use drg::{load_drg, load_drg_with_imports, Drg};
pub use engine::DmnDecisionEngine;
pub use error::DmnError;
pub use issue::{Severity, ValidationIssue};
pub use loader::DmnFileLoader;
pub use validator::{DmnPayload, DmnRulesetValidator};
