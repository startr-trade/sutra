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
/// store's real transaction. Its async methods never error.
#[derive(Debug, Default)]
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
        if expected_rev != cell.rev {
            return Ok(false);
        }
        cell.data.insert(key.to_string(), value);
        cell.rev += 1;
        Ok(true)
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
        if expected_rev != self.store.borrow().rev {
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
