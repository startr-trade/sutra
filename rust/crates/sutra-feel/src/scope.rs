//! The evaluator's name scope: a borrowed chain of frames over the caller's context.
//!
//! Every FEEL construct that binds a name (a filter's `item` and element fields, a quantifier or
//! `for` variable, a context literal's earlier entries, a function's parameters) needs the
//! caller's scope plus a few new bindings. Copying the caller's whole context for that would
//! copy every process variable, payload included, once per element or per call. A
//! [`FeelScope::Frame`] instead holds only the new bindings and borrows its parent, so a lookup
//! walks from the innermost frame outward and the first match wins: the same answer a copy
//! followed by inserts would give.

use crate::value::{FeelContext, FeelValue};

/// A name scope: the caller's context at the root, with zero or more binding frames on top.
#[derive(Clone, Copy)]
pub(crate) enum FeelScope<'a> {
    /// The context the evaluation was started with.
    Root(&'a FeelContext),
    /// New bindings layered over `parent`; a name bound here shadows the same name further out.
    Frame {
        parent: &'a FeelScope<'a>,
        bindings: &'a FeelContext,
    },
}

impl<'a> FeelScope<'a> {
    /// The value bound to `name`, looking in the innermost frame first.
    pub(crate) fn get(&self, name: &str) -> Option<&'a FeelValue> {
        let mut scope = *self;
        loop {
            match scope {
                FeelScope::Root(ctx) => return ctx.get(name),
                FeelScope::Frame { parent, bindings } => {
                    if let Some(v) = bindings.get(name) {
                        return Some(v);
                    }
                    scope = *parent;
                }
            }
        }
    }

    /// Whether `name` is bound in any frame, or at the root.
    pub(crate) fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// An owned copy of every binding in scope, inner bindings shadowing outer ones. Only a
    /// function literal's capture needs this.
    pub(crate) fn flatten(&self) -> FeelContext {
        match self {
            FeelScope::Root(ctx) => (*ctx).clone(),
            FeelScope::Frame { parent, bindings } => {
                let mut out = parent.flatten();
                for (k, v) in bindings.iter() {
                    out.insert(k.clone(), v.clone());
                }
                out
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(pairs: &[(&str, i64)]) -> FeelContext {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), FeelValue::from(*v)))
            .collect()
    }

    #[test]
    fn frame_shadows_parent() {
        let outer = ctx(&[("a", 1), ("b", 2)]);
        let inner = ctx(&[("a", 10), ("c", 30)]);
        let root = FeelScope::Root(&outer);
        let frame = FeelScope::Frame {
            parent: &root,
            bindings: &inner,
        };
        assert_eq!(frame.get("a"), Some(&FeelValue::from(10)));
        assert_eq!(frame.get("b"), Some(&FeelValue::from(2)));
        assert_eq!(frame.get("c"), Some(&FeelValue::from(30)));
        assert_eq!(frame.get("d"), None);
        assert!(frame.contains_key("b"));
        assert!(!frame.contains_key("d"));
        assert_eq!(root.get("a"), Some(&FeelValue::from(1)));
        assert!(!root.contains_key("c"));
    }

    #[test]
    fn flatten_keeps_inner_binding() {
        let outer = ctx(&[("a", 1), ("b", 2)]);
        let middle = ctx(&[("a", 10)]);
        let inner = ctx(&[("a", 100), ("c", 3)]);
        let root = FeelScope::Root(&outer);
        let mid = FeelScope::Frame {
            parent: &root,
            bindings: &middle,
        };
        let top = FeelScope::Frame {
            parent: &mid,
            bindings: &inner,
        };
        assert_eq!(top.flatten(), ctx(&[("a", 100), ("b", 2), ("c", 3)]));
        assert_eq!(mid.flatten(), ctx(&[("a", 10), ("b", 2)]));
    }
}
