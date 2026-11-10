//! The process variable map — an insertion-ordered `String → FeelValue` map (inserting an
//! existing key replaces the value in place; iteration follows first-insertion order), plus
//! the `FeelValue` ↔ `serde_json` conversions the template/script paths need.

use std::sync::Arc;

use bigdecimal::BigDecimal;
use serde_json::Value as Json;
use sutra_feel::{FeelContext, FeelValue};

/// Insertion-ordered variable map (`String → FeelValue`, first-insertion order).
///
/// The values live in a [`FeelContext`] so a FEEL expression evaluates against the map in place
/// ([`Variables::as_feel_context`]) instead of against a copy, and the whole map sits behind an
/// `Arc`: cloning a `Variables` is a reference-count bump, and the first mutation of a shared
/// map copies it (copy on write). A snapshot therefore costs nothing until the live map changes
/// while the snapshot is still held — exactly the copy an eager snapshot used to make up front.
#[derive(Clone, Default)]
pub struct Variables {
    inner: Arc<VariablesInner>,
}

/// The map itself: the values by key, and the keys in first-insertion order.
#[derive(Clone, Default)]
struct VariablesInner {
    values: FeelContext,
    order: Vec<String>,
}

impl Variables {
    pub fn new() -> Variables {
        Variables::default()
    }

    pub fn get(&self, name: &str) -> Option<&FeelValue> {
        self.inner.values.get(name)
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Inserts under `name`: replaces in place (position preserved) or appends.
    pub fn insert(&mut self, name: impl Into<String>, value: FeelValue) {
        let name = name.into();
        let inner = Arc::make_mut(&mut self.inner);
        match inner.values.get_mut(&name) {
            Some(slot) => *slot = value,
            None => {
                inner.order.push(name.clone());
                inner.values.insert(name, value);
            }
        }
    }

    pub fn remove(&mut self, name: &str) {
        if !self.inner.values.contains_key(name) {
            return;
        }
        let inner = Arc::make_mut(&mut self.inner);
        inner.values.remove(name);
        inner.order.retain(|k| k != name);
    }

    /// Merges every entry from `other`, applying insert semantics per key.
    pub fn merge(&mut self, other: &Variables) {
        for (k, v) in other.iter() {
            self.insert(k, v.clone());
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &FeelValue)> {
        let values = &self.inner.values;
        self.inner
            .order
            .iter()
            .filter_map(move |k| values.get(k).map(|v| (k.as_str(), v)))
    }

    pub fn len(&self) -> usize {
        self.inner.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.order.is_empty()
    }

    /// The map as a FEEL evaluation context, borrowed: no copy. Key order is not FEEL-relevant.
    pub fn as_feel_context(&self) -> &FeelContext {
        &self.inner.values
    }

    /// Snapshot as a FEEL evaluation context (`BTreeMap` — key order is not FEEL-relevant).
    /// [`Variables::as_feel_context`] gives the same map without the copy.
    pub fn to_feel_context(&self) -> FeelContext {
        self.inner.values.clone()
    }

    /// Snapshot as a JSON object (the template/script render model shape).
    pub fn to_json(&self) -> Json {
        let mut map = serde_json::Map::new();
        for (k, v) in self.iter() {
            map.insert(k.to_string(), feel_to_json(v));
        }
        Json::Object(map)
    }

    /// [`Variables::to_json`] with every null-valued object entry left out, at any depth (array
    /// elements stay, so index positions are stable): the same JSON as converting and then
    /// dropping the null entries, built in one pass.
    pub fn to_json_without_nulls(&self) -> Json {
        let mut map = serde_json::Map::new();
        for (k, v) in self.iter() {
            if !v.is_null() {
                map.insert(k.to_string(), feel_to_json_without_nulls(v));
            }
        }
        Json::Object(map)
    }
}

/// Equal when both hold the same keys, in the same insertion order, with equal values.
impl PartialEq for Variables {
    fn eq(&self, other: &Variables) -> bool {
        self.inner.order == other.inner.order && self.inner.values == other.inner.values
    }
}

/// The entries in insertion order, shaped as the `(key, value)` list they always printed as.
impl std::fmt::Debug for Variables {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct Entries<'a>(&'a Variables);
        impl std::fmt::Debug for Entries<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_list().entries(self.0.iter()).finish()
            }
        }
        f.debug_struct("Variables")
            .field("entries", &Entries(self))
            .finish()
    }
}

impl FromIterator<(String, FeelValue)> for Variables {
    fn from_iter<T: IntoIterator<Item = (String, FeelValue)>>(iter: T) -> Variables {
        let mut vars = Variables::new();
        for (k, v) in iter {
            vars.insert(k, v);
        }
        vars
    }
}

impl From<FeelContext> for Variables {
    fn from(ctx: FeelContext) -> Variables {
        ctx.into_iter().collect()
    }
}

/// `FeelValue` → JSON. Numbers ride serde_json's arbitrary-precision representation so the
/// exact decimal shape (`100.00`) survives into template renders, following the canonical
/// decimal string layout.
pub fn feel_to_json(v: &FeelValue) -> Json {
    match v {
        FeelValue::Null => Json::Null,
        FeelValue::Boolean(b) => Json::Bool(*b),
        FeelValue::Number(n) => {
            serde_json::Number::from_string_unchecked(canonical_decimal_string(n)).into()
        }
        FeelValue::String(s) => Json::String(s.clone()),
        FeelValue::Instant(..)
        | FeelValue::Date(_)
        | FeelValue::Time(..)
        | FeelValue::Duration(_)
        | FeelValue::Function(_)
        | FeelValue::Invocable(_)
        | FeelValue::Range(_) => Json::String(sutra_feel::value::canonical_string_of(v)),
        FeelValue::List(items) => Json::Array(items.iter().map(feel_to_json).collect()),
        FeelValue::Map(m) => Json::Object(
            m.iter()
                .map(|(k, val)| (k.clone(), feel_to_json(val)))
                .collect(),
        ),
    }
}

/// [`feel_to_json`] with every null-valued map entry left out, at any depth. List elements are
/// kept, nulls included, so index positions stay stable. Only `FeelValue::Null` converts to JSON
/// null, so this is exactly converting and then dropping the null object entries.
pub fn feel_to_json_without_nulls(v: &FeelValue) -> Json {
    match v {
        FeelValue::List(items) => {
            Json::Array(items.iter().map(feel_to_json_without_nulls).collect())
        }
        FeelValue::Map(m) => Json::Object(
            m.iter()
                .filter(|(_, val)| !val.is_null())
                .map(|(k, val)| (k.clone(), feel_to_json_without_nulls(val)))
                .collect(),
        ),
        other => feel_to_json(other),
    }
}

/// The canonical decimal string layout for the zero edge case: the `bigdecimal` crate
/// normalizes ZERO to `"0"` regardless of scale (`0.0` → `"0"`), while the canonical layout
/// preserves the scale (a scale-1 zero renders `"0.0"`) — visible on the wire wherever a
/// computed zero renders (e.g. a `coverage:report` percentage after a reset). Non-zero
/// values already print scale-faithfully.
fn canonical_decimal_string(n: &BigDecimal) -> String {
    let s = n.to_string();
    if s == "0" {
        let (_, exponent) = n.as_bigint_and_exponent();
        if exponent > 0 {
            return format!("0.{}", "0".repeat(exponent as usize));
        }
    }
    s
}

/// JSON → `FeelValue` (typed merge for script renders: numbers become `BigDecimal`s with the
/// exact literal scale, so the deserialized literal scale carries into the variable map).
pub fn json_to_feel(v: &Json) -> FeelValue {
    match v {
        Json::Null => FeelValue::Null,
        Json::Bool(b) => FeelValue::Boolean(*b),
        Json::Number(n) => n
            .to_string()
            .parse::<BigDecimal>()
            .map(FeelValue::Number)
            .unwrap_or(FeelValue::Null),
        Json::String(s) => FeelValue::String(s.clone()),
        Json::Array(items) => FeelValue::List(items.iter().map(json_to_feel).collect()),
        Json::Object(map) => FeelValue::Map(
            map.iter()
                .map(|(k, val)| (k.clone(), json_to_feel(val)))
                .collect(),
        ),
    }
}
