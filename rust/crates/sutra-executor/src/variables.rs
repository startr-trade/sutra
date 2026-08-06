//! The process variable map — an insertion-ordered `String → FeelValue` map (inserting an
//! existing key replaces the value in place; iteration follows first-insertion order), plus
//! the `FeelValue` ↔ `serde_json` conversions the template/script paths need.

use bigdecimal::BigDecimal;
use serde_json::Value as Json;
use sutra_feel::{FeelContext, FeelValue};

/// Insertion-ordered variable map (`String → FeelValue`, first-insertion order).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Variables {
    entries: Vec<(String, FeelValue)>,
}

impl Variables {
    pub fn new() -> Variables {
        Variables::default()
    }

    pub fn get(&self, name: &str) -> Option<&FeelValue> {
        self.entries.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Inserts under `name`: replaces in place (position preserved) or appends.
    pub fn insert(&mut self, name: impl Into<String>, value: FeelValue) {
        let name = name.into();
        match self.entries.iter_mut().find(|(k, _)| *k == name) {
            Some(slot) => slot.1 = value,
            None => self.entries.push((name, value)),
        }
    }

    pub fn remove(&mut self, name: &str) {
        self.entries.retain(|(k, _)| k != name);
    }

    /// Merges every entry from `other`, applying insert semantics per key.
    pub fn merge(&mut self, other: &Variables) {
        for (k, v) in &other.entries {
            self.insert(k.clone(), v.clone());
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &FeelValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Snapshot as a FEEL evaluation context (`BTreeMap` — key order is not FEEL-relevant).
    pub fn to_feel_context(&self) -> FeelContext {
        self.entries.iter().cloned().collect()
    }

    /// Snapshot as a JSON object (the template/script render model shape).
    pub fn to_json(&self) -> Json {
        let mut map = serde_json::Map::new();
        for (k, v) in &self.entries {
            map.insert(k.clone(), feel_to_json(v));
        }
        Json::Object(map)
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
