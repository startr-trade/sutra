//! FEEL subset — map/data-only (no host-object introspection). Evaluation is deterministic at
//! replay-bound sites: [`expressions::require_pure`] rejects the non-deterministic builtins
//! (`now`, `today`, `uuid`, `random`), and evaluation time enters as an injected input instead.
//!
//! Public entry point is the [`expressions`] module (the FEEL facade):
//! [`expressions::parse`], [`expressions::eval`], [`expressions::eval_boolean`],
//! [`expressions::paths`], [`expressions::require_pure`].
//!
//! # Numeric semantics
//!
//! Arithmetic uses DECIMAL64 semantics — 16 significant digits, `HALF_EVEN` rounding —
//! implemented in [`numeric`] on top of the `bigdecimal` crate (see that module for the exact
//! rounding contract and documented divergences). Integer-like inputs keep scale 0 by building
//! a scale-0 `BigDecimal` directly.
#![forbid(unsafe_code)]

pub mod ast;
pub mod codes;
pub mod determinism;
pub mod error;
pub mod evaluator;
pub mod expressions;
mod lexer;
pub mod numeric;
mod parser;
pub mod paths;
pub mod positions;
mod random;
pub mod temporal;
pub mod value;

pub use ast::{ArithOp, CompareOp, FeelExpr, LogicalOp};
pub use error::{FeelError, SourceLocation};
pub use value::{
    coerce_to_shape, ExternalFunctionBinding, FeelContext, FeelDuration, FeelFunction, FeelRange,
    FeelTypeShape, FeelValue, Invocable, TimeQualifier,
};
