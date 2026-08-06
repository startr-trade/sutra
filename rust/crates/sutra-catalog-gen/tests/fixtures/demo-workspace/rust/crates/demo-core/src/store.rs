//! Widget storage — the fixture's referenced module.

use crate::WidgetId;

/// Where widgets live.
pub struct Store {
    items: Vec<WidgetId>,
}

impl Store {
    /// A new, empty store.
    pub fn new() -> Store {
        Store { items: Vec::new() }
    }

    /// Number of stored widgets.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// True when nothing is stored.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl Default for Store {
    fn default() -> Self {
        Store::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test-only helper — must NOT appear in the catalog page.
    fn make() -> Store {
        Store::new()
    }

    #[test]
    fn empty_by_default() {
        assert!(make().is_empty());
    }
}
