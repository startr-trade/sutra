//! Fixture app crate root — exercises cross-crate references.

use demo_core::store::Store;
use demo_core::WidgetId;

/// Runs the demo.
pub fn run(id: WidgetId) -> usize {
    let store = Store::new();
    let _ = id;
    store.len()
}
