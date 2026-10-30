//! Durable key→value store SPI — the `DataStore` / `DataStoreTx` contract backing the BPMN
//! data store (`<q:store>`). The methods are `&self` (interior mutability, so a store can be
//! shared by reference across the executor and a test) and **async and `Result`-returning**:
//! a backend failure is a typed `Err(StoreError)` the executor handles at step boundaries (the instance
//! fails CLOSED — same conformance behaviour, now typed, replacing the old infallible-shaped
//! `last_error()` poison-slot surface). Object safety comes from `async_trait`'s boxed futures
//! (native RPITIT is not dyn-safe); the `?Send` form keeps the futures `!Send` (the executor is
//! `Rc`-based and single-threaded, driven on the actor's current-thread `LocalSet`).
//!
//! Includes an in-memory implementation with revision tracking and buffered transactions (the
//! durable-provider analog for tests); its async methods never error (`Ok(..)`).

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use async_trait::async_trait;
use sutra_feel::FeelValue;

use crate::deployment::DeploymentId;

/// A durable-store operation failure — the typed error the async SPI returns instead of the old
/// out-of-band `last_error()` poison slot. Carries a human-readable message; the executor maps it
/// to the step-boundary fail-closed diagnostics (`store_op_failed`), so a lost write NEVER
/// replies success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError {
    message: String,
}

impl StoreError {
    pub fn new(message: impl Into<String>) -> StoreError {
        StoreError {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StoreError {}

/// A key query (`<q:store query=…>`): the rows of one store whose key lies in
/// `[lower, upper)`, keys compared bytewise, plus aggregates over every matching row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreKeyQuery {
    /// Inclusive lower bound (a prefix query's prefix).
    pub lower: String,
    /// Exclusive upper bound; `None` = open-ended.
    pub upper: Option<String>,
    /// Return only rows with a key greater than this (the previous page's `next`).
    pub after: Option<String>,
    /// `Some(n)`: at most `n` rows; `None`: no rows, aggregates only.
    pub row_limit: Option<u32>,
    pub aggregates: Vec<StoreQueryAggregateSpec>,
}

/// One aggregate: a count, plus the sum of `sum_field` when set, grouped by `group_by` when set.
/// Paths are the segments of a dotted value path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreQueryAggregateSpec {
    pub sum_field: Option<Vec<String>>,
    pub group_by: Option<Vec<String>>,
}

/// What a [`StoreKeyQuery`] returns.
#[derive(Debug, Clone, PartialEq)]
pub struct StoreQueryResult {
    /// `(key, value)`, ascending by key.
    pub rows: Vec<(String, FeelValue)>,
    /// One per requested aggregate, in order.
    pub aggregates: Vec<StoreAggregateValue>,
    pub truncated: bool,
    pub next: Option<String>,
}

/// One aggregate's value: a total, or one cell per group (a row without the group field groups
/// under `"null"`).
#[derive(Debug, Clone, PartialEq)]
pub enum StoreAggregateValue {
    Total(StoreAggregateCell),
    Grouped(std::collections::BTreeMap<String, StoreAggregateCell>),
}

/// Count, exact sum (for a sum aggregate) and the number of rows whose summed field held a value
/// that is not a number.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StoreAggregateCell {
    pub count: i64,
    pub sum: Option<bigdecimal::BigDecimal>,
    pub non_numeric: i64,
}

/// A durable, cross-instance key→value store. Values are opaque `FeelValue`s. Async:
/// every data operation returns a `Result` (a backend failure fails the instance closed).
#[async_trait(?Send)]
pub trait DataStore {
    /// The store's declared name (from `datastores.yaml`).
    fn name(&self) -> &str;

    /// Autocommit read.
    async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError>;

    /// Autocommit write (insert-or-replace).
    async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError>;

    /// Autocommit delete; a no-op when the key is absent.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    /// The revision of the value at `key` (`0` when absent), or `-1` if this store does not
    /// track revisions. Backs optimistic concurrency (`<q:store expect="unchanged">`).
    async fn revision(&self, _key: &str) -> Result<i64, StoreError> {
        Ok(-1)
    }

    /// The value at `key` and its revision, as one read where the store can (a data task
    /// reads both). The default reads them one after the other.
    async fn get_with_revision(&self, key: &str) -> Result<(Option<FeelValue>, i64), StoreError> {
        let value = self.get(key).await?;
        let revision = self.revision(key).await?;
        Ok((value, revision))
    }

    /// Compare-and-set: write only if the current revision equals `expected_rev`, returning
    /// whether the write applied. A store that does not track revisions writes unconditionally
    /// and returns `Ok(true)`. A backend failure is `Err` (distinct from an `Ok(false)` conflict).
    async fn put_if_revision(
        &self,
        key: &str,
        value: FeelValue,
        _expected_rev: i64,
    ) -> Result<bool, StoreError> {
        self.put(key, value).await?;
        Ok(true)
    }

    /// Opens a caller-managed transaction; `Ok(None)` when this store does not support
    /// transactions (the executor then fails closed with `SUTRA.RUNTIME.UNEXPECTED`). An `Err`
    /// means the BEGIN itself failed.
    async fn begin(&self) -> Result<Option<Rc<dyn DataStoreTx>>, StoreError>;

    /// Key query (`<q:store query=…>`): committed rows, no locks, never served from a cache. A
    /// store that cannot run key queries refuses (the default).
    async fn query(&self, _query: &StoreKeyQuery) -> Result<StoreQueryResult, StoreError> {
        Err(StoreError::new(format!(
            "data store '{}' does not support key queries",
            self.name()
        )))
    }
}

/// Run a key query over in-memory `(key, value)` pairs — the reference semantics the SQL stores
/// implement: bytewise key order, aggregates over every matching row, a JSON-number-or-decimal-
/// string sum (missing or null skipped, anything else counted as non-numeric).
pub fn query_in_memory<'a>(
    entries: impl IntoIterator<Item = (&'a String, &'a FeelValue)>,
    query: &StoreKeyQuery,
) -> StoreQueryResult {
    let mut matching: Vec<(&String, &FeelValue)> = entries
        .into_iter()
        .filter(|(k, _)| {
            k.as_str() >= query.lower.as_str()
                && query.upper.as_deref().is_none_or(|u| k.as_str() < u)
        })
        .collect();
    matching.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let aggregates = query
        .aggregates
        .iter()
        .map(|agg| {
            let mut total = StoreAggregateCell::default();
            let mut groups: std::collections::BTreeMap<String, StoreAggregateCell> =
                std::collections::BTreeMap::new();
            for (_, value) in &matching {
                let cell = match &agg.group_by {
                    None => &mut total,
                    Some(path) => groups.entry(group_text(walk(value, path))).or_default(),
                };
                cell.count += 1;
                if let Some(path) = &agg.sum_field {
                    let sum = cell.sum.get_or_insert_with(bigdecimal::BigDecimal::default);
                    match numeric(walk(value, path)) {
                        Numeric::Number(n) => *sum += n,
                        Numeric::Skip => {}
                        Numeric::NotANumber => cell.non_numeric += 1,
                    }
                }
            }
            match &agg.group_by {
                None => {
                    if agg.sum_field.is_some() && total.sum.is_none() {
                        total.sum = Some(bigdecimal::BigDecimal::default());
                    }
                    StoreAggregateValue::Total(total)
                }
                Some(_) => StoreAggregateValue::Grouped(groups),
            }
        })
        .collect();
    let (rows, truncated, next) = match query.row_limit {
        None => (Vec::new(), false, None),
        Some(limit) => {
            let page: Vec<(String, FeelValue)> = matching
                .iter()
                .filter(|(k, _)| query.after.as_deref().is_none_or(|a| k.as_str() > a))
                .take(limit as usize + 1)
                .map(|(k, v)| ((*k).clone(), (*v).clone()))
                .collect();
            let truncated = page.len() > limit as usize;
            let mut page = page;
            page.truncate(limit as usize);
            let next = truncated
                .then(|| page.last().map(|(k, _)| k.clone()))
                .flatten();
            (page, truncated, next)
        }
    };
    StoreQueryResult {
        rows,
        aggregates,
        truncated,
        next,
    }
}

fn walk<'v>(value: &'v FeelValue, path: &[String]) -> Option<&'v FeelValue> {
    let mut current = value;
    for segment in path {
        match current {
            FeelValue::Map(map) => current = map.get(segment)?,
            _ => return None,
        }
    }
    Some(current)
}

fn group_text(value: Option<&FeelValue>) -> String {
    match value {
        None | Some(FeelValue::Null) => "null".to_string(),
        Some(FeelValue::String(s)) => s.clone(),
        Some(other) => sutra_feel::value::canonical_string_of(other),
    }
}

enum Numeric {
    Number(bigdecimal::BigDecimal),
    Skip,
    NotANumber,
}

fn numeric(value: Option<&FeelValue>) -> Numeric {
    use std::str::FromStr;
    match value {
        None | Some(FeelValue::Null) => Numeric::Skip,
        Some(FeelValue::Number(n)) => Numeric::Number(n.clone()),
        Some(FeelValue::String(s)) => {
            let t = s.trim();
            if !is_decimal_literal(t) {
                return Numeric::NotANumber;
            }
            // `5.` is a decimal literal the parser may not take; `5.0` means the same.
            let normalized = if t.ends_with('.') || t.contains(".e") || t.contains(".E") {
                t.replacen('.', ".0", 1)
            } else {
                t.to_string()
            };
            match bigdecimal::BigDecimal::from_str(normalized.trim_start_matches('+')) {
                Ok(n) => Numeric::Number(n),
                Err(_) => Numeric::NotANumber,
            }
        }
        Some(_) => Numeric::NotANumber,
    }
}

/// `[+-]?(digits(.digits?)?|.digits)([eE][+-]?digits)?` — the decimal literal a summed string
/// field may hold (the same grammar the SQL stores test with a regular expression).
fn is_decimal_literal(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        i += 1;
    }
    let int_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    let int_digits = i - int_start;
    let mut frac_digits = 0;
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        let frac_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        frac_digits = i - frac_start;
    }
    if int_digits == 0 && frac_digits == 0 {
        return false;
    }
    if i < bytes.len() && matches!(bytes[i], b'e' | b'E') {
        i += 1;
        if i < bytes.len() && matches!(bytes[i], b'+' | b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return false;
        }
    }
    i == bytes.len()
}

/// A caller-managed transaction over a single [`DataStore`] — the atomicity boundary the
/// engine threads across the data tasks enclosed in a `<bpmn:transaction>` scope.
#[async_trait(?Send)]
pub trait DataStoreTx {
    async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError>;

    /// Pessimistic-locking read (`SELECT … FOR UPDATE` on the `sql`-type provider).
    async fn get_for_update(&self, key: &str) -> Result<Option<FeelValue>, StoreError>;

    async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError>;

    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    async fn revision(&self, _key: &str) -> Result<i64, StoreError> {
        Ok(-1)
    }

    async fn put_if_revision(
        &self,
        key: &str,
        value: FeelValue,
        _expected_rev: i64,
    ) -> Result<bool, StoreError> {
        self.put(key, value).await?;
        Ok(true)
    }

    /// Commits and releases the transaction.
    async fn commit(&self) -> Result<(), StoreError>;

    /// Rolls back and releases the transaction.
    async fn rollback(&self) -> Result<(), StoreError>;
}

/// Resolves a [`DataStore`] by (deployment, store name).
pub type DataStoreRegistry = dyn Fn(&DeploymentId, &str) -> Option<Rc<dyn DataStore>>;

#[derive(Debug, Default)]
struct StoreCell {
    data: HashMap<String, FeelValue>,
    rev: i64,
}

/// In-memory [`DataStore`] with revision tracking and buffered transactions: a transaction
/// works on a copy and publishes on `commit` (a `rollback` discards), exactly like the durable
/// store's real transaction. Its async methods never error. A clone is another handle on the
/// SAME contents (the storage is shared), like a second handle on one pool.
#[derive(Debug, Default, Clone)]
pub struct InMemoryDataStore {
    name: String,
    cell: Rc<RefCell<StoreCell>>,
}

impl InMemoryDataStore {
    pub fn new(name: impl Into<String>) -> InMemoryDataStore {
        InMemoryDataStore {
            name: name.into(),
            cell: Rc::default(),
        }
    }

    /// Committed contents snapshot (test observation).
    pub fn snapshot(&self) -> Vec<(String, FeelValue)> {
        let cell = self.cell.borrow();
        let mut out: Vec<(String, FeelValue)> = cell
            .data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[async_trait(?Send)]
impl DataStore for InMemoryDataStore {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
        Ok(self.cell.borrow().data.get(key).cloned())
    }

    async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
        let mut cell = self.cell.borrow_mut();
        cell.data.insert(key.to_string(), value);
        cell.rev += 1;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.cell.borrow_mut().data.remove(key);
        Ok(())
    }

    async fn revision(&self, _key: &str) -> Result<i64, StoreError> {
        Ok(self.cell.borrow().rev)
    }

    async fn put_if_revision(
        &self,
        key: &str,
        value: FeelValue,
        expected_rev: i64,
    ) -> Result<bool, StoreError> {
        let mut cell = self.cell.borrow_mut();
        // `expected_rev <= 0` expects the key ABSENT (insert-only), as the SQL stores do.
        let applies = if expected_rev <= 0 {
            !cell.data.contains_key(key)
        } else {
            expected_rev == cell.rev
        };
        if !applies {
            return Ok(false);
        }
        cell.data.insert(key.to_string(), value);
        cell.rev += 1;
        Ok(true)
    }

    async fn query(&self, query: &StoreKeyQuery) -> Result<StoreQueryResult, StoreError> {
        Ok(query_in_memory(self.cell.borrow().data.iter(), query))
    }

    async fn begin(&self) -> Result<Option<Rc<dyn DataStoreTx>>, StoreError> {
        let pending = self.cell.borrow().data.clone();
        Ok(Some(Rc::new(InMemoryTx {
            store: Rc::clone(&self.cell),
            pending: RefCell::new(pending),
        })))
    }
}

struct InMemoryTx {
    store: Rc<RefCell<StoreCell>>,
    pending: RefCell<HashMap<String, FeelValue>>,
}

#[async_trait(?Send)]
impl DataStoreTx for InMemoryTx {
    async fn get(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
        Ok(self.pending.borrow().get(key).cloned())
    }

    async fn get_for_update(&self, key: &str) -> Result<Option<FeelValue>, StoreError> {
        // Single-threaded in-memory double — the pessimistic lock is a no-op.
        self.get(key).await
    }

    async fn put(&self, key: &str, value: FeelValue) -> Result<(), StoreError> {
        self.pending.borrow_mut().insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.pending.borrow_mut().remove(key);
        Ok(())
    }

    async fn revision(&self, _key: &str) -> Result<i64, StoreError> {
        Ok(self.store.borrow().rev)
    }

    async fn put_if_revision(
        &self,
        key: &str,
        value: FeelValue,
        expected_rev: i64,
    ) -> Result<bool, StoreError> {
        let applies = if expected_rev <= 0 {
            !self.pending.borrow().contains_key(key)
        } else {
            expected_rev == self.store.borrow().rev
        };
        if !applies {
            return Ok(false);
        }
        self.put(key, value).await?;
        Ok(true)
    }

    async fn commit(&self) -> Result<(), StoreError> {
        let mut cell = self.store.borrow_mut();
        cell.data = self.pending.borrow().clone();
        cell.rev += 1;
        Ok(())
    }

    async fn rollback(&self) -> Result<(), StoreError> {
        // discard pending
        Ok(())
    }
}
