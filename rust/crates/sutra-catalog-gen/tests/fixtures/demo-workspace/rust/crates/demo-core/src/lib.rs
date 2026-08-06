//! Fixture core crate root.
//!
//! Second paragraph — must NOT appear in the summary.

pub mod store;

/// A widget identifier.
pub struct WidgetId(pub u64);

/// Renders one widget.
pub trait Renderer {
    /// Draw it.
    fn draw(&self) -> String;
}

/// The protocol version.
pub const PROTOCOL_VERSION: u32 = 3;
